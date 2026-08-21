import type { ServerProviderSlashCommand } from "@t3tools/contracts";

export interface CopilotCommandInfo {
  readonly name: string;
  readonly description?: string;
  readonly input?: {
    readonly hint?: string;
  };
}

export interface CopilotSlashCommandInvocation {
  readonly name: string;
  readonly input?: string;
}

const T3_OWNED_COMMANDS = new Set(["default", "model", "plan"]);

export function mapCopilotSlashCommands(
  commands: ReadonlyArray<CopilotCommandInfo>,
): ReadonlyArray<ServerProviderSlashCommand> {
  const seen = new Set<string>();
  const result: ServerProviderSlashCommand[] = [];

  for (const command of commands) {
    const name = command.name.trim();
    if (!name || T3_OWNED_COMMANDS.has(name) || seen.has(name)) {
      continue;
    }
    seen.add(name);

    const description = command.description?.trim();
    const hint = command.input?.hint?.trim();
    result.push({
      name,
      ...(description ? { description } : {}),
      ...(hint ? { input: { hint } } : {}),
    });
  }

  return result.toSorted((left, right) => left.name.localeCompare(right.name));
}

export function parseCopilotSlashCommand(
  prompt: string,
  commandNames: ReadonlySet<string>,
): CopilotSlashCommandInvocation | undefined {
  const match = /^\/([^\s/]+)(?:\s+([\s\S]*))?$/.exec(prompt.trim());
  const name = match?.[1]?.trim().toLowerCase();
  if (!name || !commandNames.has(name)) {
    return undefined;
  }

  const input = match?.[2]?.trim();
  return {
    name,
    ...(input ? { input } : {}),
  };
}
