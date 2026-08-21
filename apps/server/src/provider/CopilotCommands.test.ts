import * as NodeAssert from "node:assert/strict";
import { describe, it } from "vite-plus/test";

import { mapCopilotSlashCommands, parseCopilotSlashCommand } from "./CopilotCommands.ts";

describe("mapCopilotSlashCommands", () => {
  it("maps Copilot command metadata and omits T3-owned commands", () => {
    const result = mapCopilotSlashCommands([
      {
        name: "model",
        description: "Select a Copilot model.",
      },
      {
        name: "mcp",
        description: "Manage MCP servers.",
        input: { hint: "command" },
      },
      {
        name: "mcp",
        description: "Duplicate.",
      },
      {
        name: "plan",
        description: "Plan a change.",
      },
    ]);

    NodeAssert.deepEqual(result, [
      {
        name: "mcp",
        description: "Manage MCP servers.",
        input: { hint: "command" },
      },
    ]);
  });

  it("skips commands without a name", () => {
    NodeAssert.deepEqual(mapCopilotSlashCommands([{ name: " " }]), []);
  });

  it("parses only known Copilot slash commands", () => {
    const commandNames = new Set(["skills", "mcp"]);

    NodeAssert.deepEqual(parseCopilotSlashCommand("/skills", commandNames), {
      name: "skills",
    });
    NodeAssert.deepEqual(parseCopilotSlashCommand("/mcp list", commandNames), {
      name: "mcp",
      input: "list",
    });
    NodeAssert.equal(parseCopilotSlashCommand("/unknown", commandNames), undefined);
  });
});
