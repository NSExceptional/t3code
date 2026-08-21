import * as NodeAssert from "node:assert/strict";
import { describe, it } from "vite-plus/test";

import { parseCopilotSkillsCliOutput } from "./CopilotSkills.ts";

describe("parseCopilotSkillsCliOutput", () => {
  it("maps Copilot skill metadata into provider skills", () => {
    const result = parseCopilotSkillsCliOutput(
      JSON.stringify([
        {
          name: "review-pr",
          description: "Review a pull request.",
          enabled: true,
          path: "/Users/test/.agents/skills/review-pr",
          source: "personal-agents",
        },
        {
          name: "builtin-skill",
          description: "A built-in skill.",
          enabled: true,
          path: "/opt/copilot/skills/builtin-skill",
          source: "builtin",
        },
      ]),
    );

    NodeAssert.deepEqual(result, [
      {
        name: "review-pr",
        description: "Review a pull request.",
        shortDescription: "Review a pull request.",
        enabled: true,
        path: "/Users/test/.agents/skills/review-pr",
        scope: "personal",
      },
      {
        name: "builtin-skill",
        description: "A built-in skill.",
        shortDescription: "A built-in skill.",
        enabled: true,
        path: "/opt/copilot/skills/builtin-skill",
        scope: "system",
      },
    ]);
  });

  it("returns no skills for malformed CLI output", () => {
    NodeAssert.deepEqual(parseCopilotSkillsCliOutput("not json"), []);
  });
});
