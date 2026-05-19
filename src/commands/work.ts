import { getRuntimeConfig } from "../config/config.js";
import { loadCronStoreSync, resolveCronStorePath } from "../cron/store.js";
import {
  collectReliabilityHealthSnapshot,
  getLatestReliabilityHealthSnapshot,
} from "../reliability/supervisor.js";
import { type RuntimeEnv, writeRuntimeJson } from "../runtime.js";
import { listTaskRecords } from "../tasks/runtime-internal.js";
import { listTaskFlowRecords } from "../tasks/task-flow-runtime-internal.js";
import {
  buildWorkManagerSnapshot,
  summarizeWorkManagerStatus,
  toHumanWorkStatusLines,
  type WorkManagerMode,
} from "../work-manager/work-manager.js";

export type WorkStatusCommandOptions = {
  json?: boolean;
};

export async function workStatusCommand(
  opts: WorkStatusCommandOptions,
  runtime: RuntimeEnv,
): Promise<void> {
  const snapshot = await collectWorkManagerSnapshot("admission");

  if (opts.json) {
    writeRuntimeJson(runtime, {
      snapshot,
      summary: summarizeWorkManagerStatus(snapshot),
    });
    return;
  }

  for (const line of toHumanWorkStatusLines(snapshot)) {
    runtime.log(line);
  }
}

export async function workShadowCommand(
  opts: WorkStatusCommandOptions,
  runtime: RuntimeEnv,
): Promise<void> {
  const snapshot = await collectWorkManagerSnapshot("shadow");

  if (opts.json) {
    writeRuntimeJson(runtime, {
      snapshot,
      summary: summarizeWorkManagerStatus(snapshot),
    });
    return;
  }

  for (const line of toHumanWorkStatusLines(snapshot)) {
    runtime.log(line);
  }
}

async function collectWorkManagerSnapshot(mode: WorkManagerMode) {
  const cfg = getRuntimeConfig();
  const store = loadCronStoreSync(resolveCronStorePath(cfg.cron?.store));
  const reliability =
    getLatestReliabilityHealthSnapshot() ?? (await collectReliabilityHealthSnapshot());

  return buildWorkManagerSnapshot({
    mode,
    tasks: listTaskRecords(),
    taskFlows: listTaskFlowRecords(),
    cronJobs: store.jobs,
    reliability,
  });
}
