/**
 * CopilotSdkClient — scoped Effect wrapper around `@github/copilot-sdk`'s
 * `CopilotClient`.
 *
 * The client spawns and drives the installed `copilot` runtime binary over the
 * SDK's typed JSON-RPC protocol (`RuntimeConnection.forStdio`), replacing the
 * generic ACP transport the Copilot provider used previously. It is acquired as
 * a scoped resource: `start()` on acquire, `stop()` on release.
 *
 * @module provider/sdk/CopilotSdkClient
 */
import {
  CopilotClient,
  RuntimeConnection,
  type CopilotSession,
  type GetAuthStatusResponse,
  type GetStatusResponse,
  type ModelInfo,
  type ResumeSessionConfig,
  type SessionConfig,
} from "@github/copilot-sdk";
// Raw fs/path are needed to resolve the runtime binary to an absolute path at
// the SDK spawn boundary — a plain async callback outside any Effect context.
// @effect-diagnostics-next-line nodeBuiltinImport:off
import * as NodeFS from "node:fs";
// @effect-diagnostics-next-line nodeBuiltinImport:off
import * as NodePath from "node:path";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";

export class CopilotSdkError extends Schema.TaggedErrorClass<CopilotSdkError>()("CopilotSdkError", {
  operation: Schema.String,
  cause: Schema.optional(Schema.Defect()),
}) {
  override get message(): string {
    return `GitHub Copilot SDK operation '${this.operation}' failed.`;
  }
}

function toSdkError(operation: string) {
  return (cause: unknown): CopilotSdkError => new CopilotSdkError({ operation, cause });
}

/** Filters a `ProcessEnv` down to the `Record<string, string>` the SDK expects. */
function toStringEnv(
  environment: NodeJS.ProcessEnv | undefined,
): Record<string, string> | undefined {
  if (!environment) return undefined;
  const out: Record<string, string> = {};
  for (const [key, value] of Object.entries(environment)) {
    if (typeof value === "string") out[key] = value;
  }
  return out;
}

async function isExecutable(candidate: string): Promise<boolean> {
  try {
    // Must be a regular file, not a directory: a directory named `copilot`
    // earlier in PATH would otherwise shadow a real CLI later in PATH.
    const stat = await NodeFS.promises.stat(candidate);
    if (!stat.isFile()) return false;
    await NodeFS.promises.access(candidate, NodeFS.constants.X_OK);
    return true;
  } catch {
    return false;
  }
}

/**
 * Resolves the `copilot` runtime binary to an absolute path.
 *
 * The SDK spawns the path we give it and does NOT resolve a bare command name
 * against the connection env's PATH — and a GUI-launched app inherits a minimal
 * PATH that omits Homebrew/npm dirs. So an unresolved `"copilot"` fails with
 * "Copilot CLI not found at copilot". We resolve it ourselves against the
 * spawn env's PATH plus the usual install locations, and hand the SDK an
 * absolute path. An already-absolute/relative path (contains a separator) is
 * used verbatim; if nothing resolves we fall back to the bare name so the SDK
 * still surfaces its own diagnostic.
 */
export async function resolveCopilotBinaryPath(
  binary: string,
  env: Readonly<Record<string, string | undefined>> | undefined,
): Promise<string> {
  if (binary.includes(NodePath.sep) || binary.includes("/")) return binary;

  const home = env?.HOME ?? process.env.HOME ?? "";
  const pathValue = env?.PATH ?? process.env.PATH ?? "";
  const pathDirs = pathValue
    .split(NodePath.delimiter)
    .map((d) => d.trim())
    .filter(Boolean);

  // On Windows an npm-installed `copilot` is a `copilot.cmd` shim: the bare
  // name never resolves to a file, so probe each PATHEXT suffix (`.CMD`, …) in
  // addition to the exact name. `NodePath.sep` is the lint-safe platform probe.
  const isWindows = NodePath.sep === "\\";
  const pathext = isWindows
    ? (env?.PATHEXT ?? process.env.PATHEXT ?? ".COM;.EXE;.BAT;.CMD")
        .split(";")
        .map((ext) => ext.trim())
        .filter(Boolean)
    : [];
  const candidateNames = [
    binary,
    ...pathext
      .filter((ext) => !binary.toLowerCase().endsWith(ext.toLowerCase()))
      .map((ext) => `${binary}${ext}`),
  ];
  const commonDirs = [
    "/opt/homebrew/bin",
    "/usr/local/bin",
    "/usr/bin",
    "/bin",
    ...(home
      ? [
          NodePath.join(home, ".local/bin"),
          NodePath.join(home, ".bun/bin"),
          NodePath.join(home, ".volta/bin"),
          NodePath.join(home, ".npm-global/bin"),
        ]
      : []),
  ];

  const seen = new Set<string>();
  for (const dir of [...pathDirs, ...commonDirs]) {
    if (!dir || seen.has(dir)) continue;
    seen.add(dir);
    for (const name of candidateNames) {
      const candidate = NodePath.join(dir, name);
      if (await isExecutable(candidate)) return candidate;
    }
  }
  return binary;
}

export interface CopilotSdkClient {
  /** Underlying SDK client, for the rare call not wrapped below. */
  readonly raw: CopilotClient;
  readonly listModels: Effect.Effect<ReadonlyArray<ModelInfo>, CopilotSdkError>;
  readonly getAuthStatus: Effect.Effect<GetAuthStatusResponse, CopilotSdkError>;
  readonly getStatus: Effect.Effect<GetStatusResponse, CopilotSdkError>;
  readonly createSession: (config: SessionConfig) => Effect.Effect<CopilotSession, CopilotSdkError>;
  readonly resumeSession: (
    sessionId: string,
    config: ResumeSessionConfig,
  ) => Effect.Effect<CopilotSession, CopilotSdkError>;
}

export interface CopilotSdkClientInput {
  readonly binaryPath?: string | null;
  readonly environment?: NodeJS.ProcessEnv;
  readonly logLevel?: "none" | "error" | "warning" | "info" | "debug" | "all";
}

/**
 * Acquires a started `CopilotClient` as a scoped resource. The client is
 * stopped when the enclosing scope closes.
 */
export const makeCopilotSdkClient = (
  input: CopilotSdkClientInput,
): Effect.Effect<CopilotSdkClient, CopilotSdkError, Scope.Scope> => {
  const env = toStringEnv(input.environment);
  const binary = input.binaryPath?.trim() || "copilot";

  const acquire = Effect.tryPromise({
    try: async () => {
      const path = await resolveCopilotBinaryPath(binary, env);
      const client = new CopilotClient({
        // Env goes ONLY on the stdio connection — the SDK rejects setting it at
        // both the client level and the connection level, and prefers the
        // connection-level env for child-process transports.
        connection: RuntimeConnection.forStdio({ path, ...(env ? { env } : {}) }),
        ...(input.logLevel ? { logLevel: input.logLevel } : {}),
      });
      // `start()` spawns the runtime child process before it resolves; if it
      // rejects, `acquireRelease` never gets `client` to register its release,
      // so stop it here to avoid leaking the process.
      try {
        await client.start();
      } catch (error) {
        await client.stop().catch(() => client.forceStop());
        throw error;
      }
      return client;
    },
    catch: toSdkError("start"),
  });

  const release = (client: CopilotClient) =>
    // Force-stop if graceful shutdown rejects, so a failed `stop()` never leaves
    // the runtime child process alive after the scope closes (same fallback as
    // the acquisition path).
    Effect.promise(() => client.stop().catch(() => client.forceStop().catch(() => {}))).pipe(
      Effect.asVoid,
    );

  return Effect.acquireRelease(acquire, release).pipe(
    Effect.map(
      (client): CopilotSdkClient => ({
        raw: client,
        listModels: Effect.tryPromise({
          try: () => client.listModels(),
          catch: toSdkError("listModels"),
        }),
        getAuthStatus: Effect.tryPromise({
          try: () => client.getAuthStatus(),
          catch: toSdkError("getAuthStatus"),
        }),
        getStatus: Effect.tryPromise({
          try: () => client.getStatus(),
          catch: toSdkError("getStatus"),
        }),
        createSession: (config) =>
          Effect.tryPromise({
            try: () => client.createSession(config),
            catch: toSdkError("createSession"),
          }),
        resumeSession: (sessionId, config) =>
          Effect.tryPromise({
            try: () => client.resumeSession(sessionId, config),
            catch: toSdkError("resumeSession"),
          }),
      }),
    ),
  );
};
