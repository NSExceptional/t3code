import type { ServerProviderSkill } from "@t3tools/contracts";
import * as Exit from "effect/Exit";
import * as Schema from "effect/Schema";

const CopilotSkillSchema = Schema.Struct({
  name: Schema.String,
  description: Schema.optionalKey(Schema.String),
  enabled: Schema.Boolean,
  path: Schema.String,
  source: Schema.String,
});

const decodeCopilotSkills = Schema.decodeUnknownExit(
  Schema.fromJsonString(Schema.Array(CopilotSkillSchema)),
);

function normalizeSkillScope(source: string): string | undefined {
  switch (source.trim().toLowerCase()) {
    case "personal-agents":
    case "personal-copilot":
      return "personal";
    case "builtin":
      return "system";
    case "plugin":
      return "app";
    case "project":
      return "project";
    default:
      return undefined;
  }
}

export function parseCopilotSkillsCliOutput(stdout: string): ReadonlyArray<ServerProviderSkill> {
  const decoded = decodeCopilotSkills(stdout);
  if (Exit.isFailure(decoded)) {
    return [];
  }

  return decoded.value.flatMap((skill) => {
    const name = skill.name.trim();
    const path = skill.path.trim();
    if (!name || !path) {
      return [];
    }

    const description = skill.description?.trim();
    const scope = normalizeSkillScope(skill.source);
    return [
      {
        name,
        path,
        enabled: skill.enabled,
        ...(description ? { description, shortDescription: description } : {}),
        ...(scope ? { scope } : {}),
      },
    ];
  });
}
