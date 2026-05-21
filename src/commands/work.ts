import fs from "node:fs/promises";
import path from "node:path";
import { getRuntimeConfig } from "../config/config.js";
import { loadCronStoreSync, resolveCronStorePath } from "../cron/store.js";
import {
  collectReliabilityHealthSnapshot,
  getLatestReliabilityHealthSnapshot,
} from "../reliability/supervisor.js";
import { type RuntimeEnv, writeRuntimeJson } from "../runtime.js";
import { listTaskRecords } from "../tasks/runtime-internal.js";
import { listTaskControlRecords } from "../tasks/task-control-registry.js";
import {
  createManagedTaskFlow,
  findLatestTaskFlowForOwnerKey,
  listTaskFlowRecords,
  updateFlowRecordByIdExpectedRevision,
} from "../tasks/task-flow-runtime-internal.js";
import {
  buildOpenClawMissionContract,
  buildWorkManagerSnapshot,
  summarizeWorkManagerStatus,
  toHumanWorkStatusLines,
  type WorkManagerMode,
} from "../work-manager/work-manager.js";

export type WorkStatusCommandOptions = {
  json?: boolean;
};

export type WorkMissionAcceptCommandOptions = {
  request: string;
  selectedOption: string;
  wakeTime?: string;
  proofPath?: string;
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

export async function workMissionAcceptCommand(
  opts: WorkMissionAcceptCommandOptions,
  runtime: RuntimeEnv,
): Promise<void> {
  const nowMs = Date.now();
  const missionId = `mission-${nowMs}`;
  const wakeTime = opts.wakeTime ?? new Date(nowMs + 8 * 60 * 60_000).toISOString();
  const proofPath = opts.proofPath ?? defaultMissionProofPath(nowMs, missionId);
  const mission = buildOpenClawMissionContract({
    missionId,
    userRequest: opts.request,
    selectedOption: opts.selectedOption,
    nowMs,
    wakeTime,
    proofPath,
  });
  const ownerKey = "work-manager:mission:current";
  const stateJson = {
    openclawMission: mission,
    openclawLastRecoveryAction: "mission_accepted",
  };
  const existing = findLatestTaskFlowForOwnerKey(ownerKey);
  if (existing && ["queued", "running", "waiting", "blocked"].includes(existing.status)) {
    updateFlowRecordByIdExpectedRevision({
      flowId: existing.flowId,
      expectedRevision: existing.revision,
      patch: {
        status: "running",
        goal: "OpenClaw Mission Contract",
        currentStep: "mission_active",
        stateJson,
        updatedAt: nowMs,
        endedAt: null,
      },
    });
  } else {
    createManagedTaskFlow({
      controllerId: "work-manager",
      ownerKey,
      notifyPolicy: "silent",
      status: "running",
      goal: "OpenClaw Mission Contract",
      currentStep: "mission_active",
      stateJson,
      createdAt: nowMs,
      updatedAt: nowMs,
    });
  }

  await writeMissionProof(proofPath, mission);

  if (opts.json) {
    writeRuntimeJson(runtime, { mission, proofPath });
    return;
  }
  runtime.log(`mission: active ${mission.selected_option}`);
  runtime.log(`priority: ${mission.priority}`);
  runtime.log(`wake: ${mission.wake_time}`);
  runtime.log(`report: ${proofPath}`);
}

async function collectWorkManagerSnapshot(mode: WorkManagerMode) {
  const cfg = getRuntimeConfig();
  const store = loadCronStoreSync(resolveCronStorePath(cfg.cron?.store));
  const reliability =
    getLatestReliabilityHealthSnapshot() ?? (await collectReliabilityHealthSnapshot());

  return buildWorkManagerSnapshot({
    mode,
    dispatchProofEnabled: cfg.executionKernel?.dispatchProof?.enabled === true,
    tasks: listTaskRecords(),
    taskControls: listTaskControlRecords({ activeOnly: true }),
    taskFlows: listTaskFlowRecords(),
    cronJobs: store.jobs,
    reliability,
  });
}

function defaultMissionProofPath(nowMs: number, missionId: string): string {
  const date = new Date(nowMs).toISOString().slice(0, 10);
  return path.join(
    "/Users/shamil/vault/Spaces/OpenClaw/Reports",
    date,
    `openclaw-mission-${missionId}.md`,
  );
}

async function writeMissionProof(proofPath: string, mission: unknown): Promise<void> {
  await fs.mkdir(path.dirname(proofPath), { recursive: true });
  const contract = mission as {
    mission_id: string;
    user_request: string;
    selected_option: string;
    objective: string;
    priority: string;
    wake_time: string;
  };
  await fs.writeFile(
    proofPath,
    [
      "# OpenClaw Mission Contract",
      "",
      `mission_id: ${contract.mission_id}`,
      `status: active`,
      `priority: ${contract.priority}`,
      `selected_option: ${contract.selected_option}`,
      `objective: ${contract.objective}`,
      `wake_time: ${contract.wake_time}`,
      "",
      "user_request:",
      contract.user_request,
      "",
      "runtime_behavior:",
      "- Selected option is authoritative unless an exact hard gate blocks it.",
      "- Safe revenue work stays autonomous through native cron/task/TaskFlow paths.",
      "- If idle, mission revenue floor promotes one existing safe revenue cron unit.",
      "",
    ].join("\n"),
    "utf8",
  );
}
