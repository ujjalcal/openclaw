import { beforeEach, describe, expect, it, vi } from "vitest";
import { baseConfigSnapshot, createTestRuntime } from "./test-runtime-config-helpers.js";

const readConfigFileSnapshotMock = vi.hoisted(() => vi.fn());
const writeConfigFileMock = vi.hoisted(() => vi.fn().mockResolvedValue(undefined));

vi.mock("../config/config.js", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../config/config.js")>()),
  readConfigFileSnapshot: readConfigFileSnapshotMock,
  writeConfigFile: writeConfigFileMock,
}));

vi.mock("../channels/plugins/index.js", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../channels/plugins/index.js")>();
  return {
    ...actual,
    getChannelPlugin: (channel: string) => {
      if (channel === "matrix-js") {
        return {
          id: "matrix-js",
          setup: {
            resolveBindingAccountId: ({ agentId }: { agentId: string }) => agentId.toLowerCase(),
          },
        };
      }
      return actual.getChannelPlugin(channel);
    },
    normalizeChannelId: (channel: string) => {
      if (channel.trim().toLowerCase() === "matrix-js") {
        return "matrix-js";
      }
      return actual.normalizeChannelId(channel);
    },
  };
});

import { agentsBindCommand, agentsBindingsCommand, agentsUnbindCommand } from "./agents.js";

const runtime = createTestRuntime();

describe("agents bind/unbind commands", () => {
  beforeEach(() => {
    readConfigFileSnapshotMock.mockClear();
    writeConfigFileMock.mockClear();
    runtime.log.mockClear();
    runtime.error.mockClear();
    runtime.exit.mockClear();
  });

  it("lists all bindings by default", async () => {
    readConfigFileSnapshotMock.mockResolvedValue({
      ...baseConfigSnapshot,
      config: {
        bindings: [
          { agentId: "main", match: { channel: "matrix-js" } },
          { agentId: "ops", match: { channel: "telegram", accountId: "work" } },
        ],
      },
    });

    await agentsBindingsCommand({}, runtime);

    expect(runtime.log).toHaveBeenCalledWith(expect.stringContaining("main <- matrix-js"));
    expect(runtime.log).toHaveBeenCalledWith(
      expect.stringContaining("ops <- telegram accountId=work"),
    );
  });

  it("binds routes to default agent when --agent is omitted", async () => {
    readConfigFileSnapshotMock.mockResolvedValue({
      ...baseConfigSnapshot,
      config: {},
    });

    await agentsBindCommand({ bind: ["telegram"] }, runtime);

    expect(writeConfigFileMock).toHaveBeenCalledWith(
      expect.objectContaining({
        bindings: [{ agentId: "main", match: { channel: "telegram" } }],
      }),
    );
    expect(runtime.exit).not.toHaveBeenCalled();
  });

  it("defaults matrix-js accountId to the target agent id when omitted", async () => {
    readConfigFileSnapshotMock.mockResolvedValue({
      ...baseConfigSnapshot,
      config: {},
    });

    await agentsBindCommand({ agent: "main", bind: ["matrix-js"] }, runtime);

    expect(writeConfigFileMock).toHaveBeenCalledWith(
      expect.objectContaining({
        bindings: [{ agentId: "main", match: { channel: "matrix-js", accountId: "main" } }],
      }),
    );
    expect(runtime.exit).not.toHaveBeenCalled();
  });

  it("upgrades existing channel-only binding when accountId is later provided", async () => {
    readConfigFileSnapshotMock.mockResolvedValue({
      ...baseConfigSnapshot,
      config: {
        bindings: [{ agentId: "main", match: { channel: "telegram" } }],
      },
    });

    await agentsBindCommand({ bind: ["telegram:work"] }, runtime);

    expect(writeConfigFileMock).toHaveBeenCalledWith(
      expect.objectContaining({
        bindings: [{ agentId: "main", match: { channel: "telegram", accountId: "work" } }],
      }),
    );
    expect(runtime.log).toHaveBeenCalledWith("Updated bindings:");
    expect(runtime.exit).not.toHaveBeenCalled();
  });

  it("unbinds all routes for an agent", async () => {
    readConfigFileSnapshotMock.mockResolvedValue({
      ...baseConfigSnapshot,
      config: {
        agents: { list: [{ id: "ops", workspace: "/tmp/ops" }] },
        bindings: [
          { agentId: "main", match: { channel: "matrix-js" } },
          { agentId: "ops", match: { channel: "telegram", accountId: "work" } },
        ],
      },
    });

    await agentsUnbindCommand({ agent: "ops", all: true }, runtime);

    expect(writeConfigFileMock).toHaveBeenCalledWith(
      expect.objectContaining({
        bindings: [{ agentId: "main", match: { channel: "matrix-js" } }],
      }),
    );
    expect(runtime.exit).not.toHaveBeenCalled();
  });

  it("reports ownership conflicts during unbind and exits 1", async () => {
    readConfigFileSnapshotMock.mockResolvedValue({
      ...baseConfigSnapshot,
      config: {
        agents: { list: [{ id: "ops", workspace: "/tmp/ops" }] },
        bindings: [{ agentId: "main", match: { channel: "telegram", accountId: "ops" } }],
      },
    });

    await agentsUnbindCommand({ agent: "ops", bind: ["telegram:ops"] }, runtime);

    expect(writeConfigFileMock).not.toHaveBeenCalled();
    expect(runtime.error).toHaveBeenCalledWith("Bindings are owned by another agent:");
    expect(runtime.exit).toHaveBeenCalledWith(1);
  });

  it("binds with peer spec and no account id", async () => {
    readConfigFileSnapshotMock.mockResolvedValue({
      ...baseConfigSnapshot,
      config: {},
    });

    await agentsBindCommand({ bind: ["slack@channel:C0AJ0QAQ23T"] }, runtime);

    expect(writeConfigFileMock).toHaveBeenCalledWith(
      expect.objectContaining({
        bindings: [
          {
            agentId: "main",
            match: { channel: "slack", peer: { kind: "channel", id: "C0AJ0QAQ23T" } },
          },
        ],
      }),
    );
    expect(runtime.exit).not.toHaveBeenCalled();
  });

  it("binds with peer spec and account id", async () => {
    readConfigFileSnapshotMock.mockResolvedValue({
      ...baseConfigSnapshot,
      config: {},
    });

    await agentsBindCommand({ bind: ["slack:work@channel:C0AJ0QAQ23T"] }, runtime);

    expect(writeConfigFileMock).toHaveBeenCalledWith(
      expect.objectContaining({
        bindings: [
          {
            agentId: "main",
            match: {
              channel: "slack",
              accountId: "work",
              peer: { kind: "channel", id: "C0AJ0QAQ23T" },
            },
          },
        ],
      }),
    );
    expect(runtime.exit).not.toHaveBeenCalled();
  });

  it("rejects unknown peer kind", async () => {
    readConfigFileSnapshotMock.mockResolvedValue({
      ...baseConfigSnapshot,
      config: {},
    });

    await agentsBindCommand({ bind: ["slack@badkind:C123"] }, runtime);

    expect(writeConfigFileMock).not.toHaveBeenCalled();
    expect(runtime.error).toHaveBeenCalledWith(
      expect.stringContaining('Unknown peer kind "badkind"'),
    );
    expect(runtime.exit).toHaveBeenCalledWith(1);
  });

  it("rejects empty peer id", async () => {
    readConfigFileSnapshotMock.mockResolvedValue({
      ...baseConfigSnapshot,
      config: {},
    });

    await agentsBindCommand({ bind: ["slack@channel:"] }, runtime);

    expect(writeConfigFileMock).not.toHaveBeenCalled();
    expect(runtime.error).toHaveBeenCalledWith(expect.stringContaining("empty peer id"));
    expect(runtime.exit).toHaveBeenCalledWith(1);
  });

  it("peer binding does not conflict with channel-level binding for the same agent", async () => {
    readConfigFileSnapshotMock.mockResolvedValue({
      ...baseConfigSnapshot,
      config: {
        bindings: [{ agentId: "main", match: { channel: "slack" } }],
      },
    });

    await agentsBindCommand({ bind: ["slack@channel:C0AJ0QAQ23T"] }, runtime);

    expect(writeConfigFileMock).toHaveBeenCalledWith(
      expect.objectContaining({
        bindings: [
          { agentId: "main", match: { channel: "slack" } },
          {
            agentId: "main",
            match: { channel: "slack", peer: { kind: "channel", id: "C0AJ0QAQ23T" } },
          },
        ],
      }),
    );
    expect(runtime.exit).not.toHaveBeenCalled();
  });

  it("preserves peer IDs containing @ (e.g. WhatsApp 123@g.us)", async () => {
    readConfigFileSnapshotMock.mockResolvedValue({
      ...baseConfigSnapshot,
      config: {},
    });

    await agentsBindCommand({ bind: ["whatsapp@group:123456@g.us"] }, runtime);

    expect(writeConfigFileMock).toHaveBeenCalledWith(
      expect.objectContaining({
        bindings: [
          {
            agentId: "main",
            match: { channel: "whatsapp", peer: { kind: "group", id: "123456@g.us" } },
          },
        ],
      }),
    );
    expect(runtime.exit).not.toHaveBeenCalled();
  });

  it("unbinds only the peer-scoped binding", async () => {
    readConfigFileSnapshotMock.mockResolvedValue({
      ...baseConfigSnapshot,
      config: {
        bindings: [
          { agentId: "main", match: { channel: "slack" } },
          {
            agentId: "main",
            match: { channel: "slack", peer: { kind: "channel", id: "C0AJ0QAQ23T" } },
          },
        ],
      },
    });

    await agentsUnbindCommand({ bind: ["slack@channel:C0AJ0QAQ23T"] }, runtime);

    expect(writeConfigFileMock).toHaveBeenCalledWith(
      expect.objectContaining({
        bindings: [{ agentId: "main", match: { channel: "slack" } }],
      }),
    );
    expect(runtime.exit).not.toHaveBeenCalled();
  });

  it("keeps role-based bindings when removing channel-level discord binding", async () => {
    readConfigFileSnapshotMock.mockResolvedValue({
      ...baseConfigSnapshot,
      config: {
        bindings: [
          {
            agentId: "main",
            match: {
              channel: "discord",
              accountId: "guild-a",
              roles: ["111", "222"],
            },
          },
          {
            agentId: "main",
            match: {
              channel: "discord",
              accountId: "guild-a",
            },
          },
        ],
      },
    });

    await agentsUnbindCommand({ bind: ["discord:guild-a"] }, runtime);

    expect(writeConfigFileMock).toHaveBeenCalledWith(
      expect.objectContaining({
        bindings: [
          {
            agentId: "main",
            match: {
              channel: "discord",
              accountId: "guild-a",
              roles: ["111", "222"],
            },
          },
        ],
      }),
    );
    expect(runtime.exit).not.toHaveBeenCalled();
  });
});
