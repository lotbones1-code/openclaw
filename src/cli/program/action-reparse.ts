import type { Command } from "commander";
import { buildParseArgv } from "../argv.js";
import { resolveActionArgs } from "./helpers.js";

type CommandWithRawArgs = Command & { rawArgs?: string[] };

function resolveRootProgram(command: Command | undefined): CommandWithRawArgs | undefined {
  let current = command;
  while (current?.parent) {
    current = current.parent;
  }
  return (current ?? command) as CommandWithRawArgs | undefined;
}

function rawArgsLookLikeProcessArgv(rawArgs: readonly string[], programName: string): boolean {
  if (rawArgs.length >= 2 && rawArgs[1]?.includes(programName)) {
    return true;
  }
  return Boolean(rawArgs[0]?.includes("/") || rawArgs[0] === "node");
}

export async function reparseProgramFromActionArgs(
  program: Command,
  actionArgs: unknown[],
): Promise<void> {
  const actionCommand = actionArgs.at(-1) as Command | undefined;
  const rootProgram = resolveRootProgram(program);
  const rootRawArgs =
    rootProgram?.rawArgs && rootProgram.rawArgs.length > 0 ? rootProgram.rawArgs : null;
  if (rootProgram && rootRawArgs) {
    const parseOptions = rawArgsLookLikeProcessArgv(rootRawArgs, rootProgram.name())
      ? undefined
      : ({ from: "user" } as const);
    if (parseOptions) {
      await rootProgram.parseAsync(rootRawArgs, parseOptions);
    } else {
      await rootProgram.parseAsync(rootRawArgs);
    }
    return;
  }

  const programRawArgs = (program as CommandWithRawArgs).rawArgs;
  const rawArgs =
    programRawArgs && programRawArgs.length > 0
      ? programRawArgs
      : ((actionCommand?.parent ?? program) as CommandWithRawArgs).rawArgs;
  const actionArgsList = resolveActionArgs(actionCommand);
  const fallbackArgv = actionCommand?.name()
    ? [actionCommand.name(), ...actionArgsList]
    : actionArgsList;
  const parseArgv = buildParseArgv({
    programName: program.name(),
    rawArgs,
    fallbackArgv,
  });
  await program.parseAsync(parseArgv);
}
