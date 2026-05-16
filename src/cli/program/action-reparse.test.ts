import { Command } from "commander";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { reparseProgramFromActionArgs } from "./action-reparse.js";

const buildParseArgvMock = vi.hoisted(() => vi.fn());
const resolveActionArgsMock = vi.hoisted(() => vi.fn());

vi.mock("../argv.js", () => ({
  buildParseArgv: buildParseArgvMock,
}));

vi.mock("./helpers.js", () => ({
  resolveActionArgs: resolveActionArgsMock,
}));

describe("reparseProgramFromActionArgs", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    buildParseArgvMock.mockReturnValue(["node", "openclaw", "status"]);
    resolveActionArgsMock.mockReturnValue([]);
  });

  it("uses action command name + args as fallback argv", async () => {
    const program = new Command().name("openclaw");
    const parseAsync = vi.spyOn(program, "parseAsync").mockResolvedValue(program);
    const actionCommand = {
      name: () => "status",
      parent: {
        rawArgs: ["node", "openclaw", "status", "--json"],
      },
    } as unknown as Command;
    resolveActionArgsMock.mockReturnValue(["--json"]);

    await reparseProgramFromActionArgs(program, [actionCommand]);

    expect(buildParseArgvMock).toHaveBeenCalledWith({
      programName: "openclaw",
      rawArgs: ["node", "openclaw", "status", "--json"],
      fallbackArgv: ["status", "--json"],
    });
    expect(parseAsync).toHaveBeenCalledWith(["node", "openclaw", "status"]);
  });

  it("reparses from the root program raw args when available", async () => {
    const program = new Command().name("openclaw") as Command & { rawArgs?: string[] };
    program.rawArgs = ["node", "openclaw", "browser", "--browser-profile", "titan-ig", "status"];
    const parseAsync = vi.spyOn(program, "parseAsync").mockResolvedValue(program);
    const browserCommand = new Command("browser");
    browserCommand.parent = program;
    const actionCommand = {
      name: () => "status",
      parent: browserCommand,
    } as unknown as Command;

    await reparseProgramFromActionArgs(browserCommand, [actionCommand]);

    expect(buildParseArgvMock).not.toHaveBeenCalled();
    expect(parseAsync).toHaveBeenCalledWith([
      "node",
      "openclaw",
      "browser",
      "--browser-profile",
      "titan-ig",
      "status",
    ]);
  });

  it("falls back to action args without command name when action has no name", async () => {
    const program = new Command().name("openclaw");
    const parseAsync = vi.spyOn(program, "parseAsync").mockResolvedValue(program);
    const actionCommand = {
      name: () => "",
      parent: {},
    } as unknown as Command;
    resolveActionArgsMock.mockReturnValue(["--json"]);

    await reparseProgramFromActionArgs(program, [actionCommand]);

    expect(buildParseArgvMock).toHaveBeenCalledWith({
      programName: "openclaw",
      rawArgs: undefined,
      fallbackArgv: ["--json"],
    });
    expect(parseAsync).toHaveBeenCalledWith(["node", "openclaw", "status"]);
  });

  it("uses program root when action command is missing", async () => {
    const program = new Command().name("openclaw");
    const parseAsync = vi.spyOn(program, "parseAsync").mockResolvedValue(program);

    await reparseProgramFromActionArgs(program, []);

    expect(resolveActionArgsMock).toHaveBeenCalledWith(undefined);
    expect(buildParseArgvMock).toHaveBeenCalledWith({
      programName: "openclaw",
      rawArgs: [],
      fallbackArgv: [],
    });
    expect(parseAsync).toHaveBeenCalledWith(["node", "openclaw", "status"]);
  });
});
