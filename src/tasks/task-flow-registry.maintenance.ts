import { listTasksForFlowId } from "./runtime-internal.js";
import {
  listTaskFlowAuditFindings,
  summarizeTaskFlowAuditFindings,
  type TaskFlowAuditSummary,
} from "./task-flow-registry.audit.js";
import {
  deleteTaskFlowRecordById,
  getTaskFlowById,
  listTaskFlowRecords,
  updateFlowRecordByIdExpectedRevision,
} from "./task-flow-registry.js";
import type { TaskFlowRecord } from "./task-flow-registry.types.js";

const TASK_FLOW_RETENTION_MS = 7 * 24 * 60 * 60_000;
const TASK_FLOW_STALE_RUNNING_MS = 30 * 60_000;

export type TaskFlowRegistryMaintenanceSummary = {
  reconciled: number;
  pruned: number;
};

function isTerminalFlow(flow: TaskFlowRecord): boolean {
  return (
    flow.status === "succeeded" ||
    flow.status === "failed" ||
    flow.status === "cancelled" ||
    flow.status === "lost"
  );
}

function hasActiveLinkedTasks(flowId: string): boolean {
  return listTasksForFlowId(flowId).some(
    (task) => task.status === "queued" || task.status === "running",
  );
}

function hasActiveRealLinkedTasks(flowId: string): boolean {
  return listTasksForFlowId(flowId).some(
    (task) =>
      task.taskKind !== "taskflow_repair" &&
      (task.status === "queued" || task.status === "running"),
  );
}

function resolveTerminalAt(flow: TaskFlowRecord): number {
  return flow.endedAt ?? flow.updatedAt ?? flow.createdAt;
}

function shouldPruneFlow(flow: TaskFlowRecord, now: number): boolean {
  if (!isTerminalFlow(flow)) {
    return false;
  }
  if (hasActiveLinkedTasks(flow.flowId)) {
    return false;
  }
  return now - resolveTerminalAt(flow) >= TASK_FLOW_RETENTION_MS;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isActiveOpenClawMissionFlow(flow: TaskFlowRecord): boolean {
  if (
    flow.goal !== "OpenClaw Mission Contract" &&
    flow.ownerKey !== "work-manager:mission:current"
  ) {
    return false;
  }
  const mission = isRecord(flow.stateJson) ? flow.stateJson.openclawMission : undefined;
  return isRecord(mission) && mission.status === "active";
}

function shouldNormalizeTerminalTimestamps(flow: TaskFlowRecord): boolean {
  return (
    isTerminalFlow(flow) &&
    typeof flow.endedAt === "number" &&
    (flow.endedAt < flow.createdAt || flow.endedAt < flow.updatedAt)
  );
}

function normalizeTerminalTimestamps(flow: TaskFlowRecord): boolean {
  let current = flow;
  for (let attempt = 0; attempt < 2; attempt += 1) {
    if (!shouldNormalizeTerminalTimestamps(current)) {
      return false;
    }
    const endedAt = Math.max(
      current.endedAt ?? current.updatedAt,
      current.updatedAt,
      current.createdAt,
    );
    const result = updateFlowRecordByIdExpectedRevision({
      flowId: current.flowId,
      expectedRevision: current.revision,
      patch: {
        endedAt,
        updatedAt: Math.max(current.updatedAt, endedAt),
      },
    });
    if (result.applied) {
      return true;
    }
    if (result.reason === "not_found" || !result.current) {
      return false;
    }
    current = result.current;
  }
  return false;
}

function shouldMarkStaleRunningManagedFlowLost(flow: TaskFlowRecord, now: number): boolean {
  if (flow.syncMode !== "managed" || flow.status !== "running") {
    return false;
  }
  if (isActiveOpenClawMissionFlow(flow)) {
    return false;
  }
  if (hasActiveRealLinkedTasks(flow.flowId)) {
    return false;
  }
  return now - flow.updatedAt >= TASK_FLOW_STALE_RUNNING_MS;
}

function markStaleRunningManagedFlowLost(flow: TaskFlowRecord, now: number): boolean {
  let current = flow;
  for (let attempt = 0; attempt < 2; attempt += 1) {
    if (!shouldMarkStaleRunningManagedFlowLost(current, now)) {
      return false;
    }
    const endedAt = Math.max(now, current.updatedAt, current.createdAt);
    const result = updateFlowRecordByIdExpectedRevision({
      flowId: current.flowId,
      expectedRevision: current.revision,
      patch: {
        status: "lost",
        blockedTaskId: null,
        blockedSummary: "stale running TaskFlow lost during native maintenance",
        waitJson: null,
        endedAt,
        updatedAt: endedAt,
      },
    });
    if (result.applied) {
      return true;
    }
    if (result.reason === "not_found" || !result.current) {
      return false;
    }
    current = result.current;
  }
  return false;
}

function shouldFinalizeCancelledFlow(flow: TaskFlowRecord): boolean {
  if (flow.syncMode !== "managed") {
    return false;
  }
  if (flow.cancelRequestedAt == null || isTerminalFlow(flow)) {
    return false;
  }
  return !hasActiveLinkedTasks(flow.flowId);
}

function finalizeCancelledFlow(flow: TaskFlowRecord, now: number): boolean {
  let current = flow;
  for (let attempt = 0; attempt < 2; attempt += 1) {
    const endedAt = Math.max(now, current.updatedAt, current.cancelRequestedAt ?? now);
    const result = updateFlowRecordByIdExpectedRevision({
      flowId: current.flowId,
      expectedRevision: current.revision,
      patch: {
        status: "cancelled",
        blockedTaskId: null,
        blockedSummary: null,
        waitJson: null,
        endedAt,
        updatedAt: endedAt,
      },
    });
    if (result.applied) {
      return true;
    }
    if (result.reason === "not_found" || !result.current) {
      return false;
    }
    current = result.current;
    if (!shouldFinalizeCancelledFlow(current)) {
      return false;
    }
  }
  return false;
}

export function getInspectableTaskFlowAuditSummary(): TaskFlowAuditSummary {
  return summarizeTaskFlowAuditFindings(listTaskFlowAuditFindings());
}

export function previewTaskFlowRegistryMaintenance(): TaskFlowRegistryMaintenanceSummary {
  const now = Date.now();
  let reconciled = 0;
  let pruned = 0;
  for (const flow of listTaskFlowRecords()) {
    if (shouldFinalizeCancelledFlow(flow)) {
      reconciled += 1;
      continue;
    }
    if (shouldNormalizeTerminalTimestamps(flow)) {
      reconciled += 1;
      continue;
    }
    if (shouldMarkStaleRunningManagedFlowLost(flow, now)) {
      reconciled += 1;
      continue;
    }
    if (shouldPruneFlow(flow, now)) {
      pruned += 1;
    }
  }
  return { reconciled, pruned };
}

export async function runTaskFlowRegistryMaintenance(): Promise<TaskFlowRegistryMaintenanceSummary> {
  const now = Date.now();
  let reconciled = 0;
  let pruned = 0;
  for (const flow of listTaskFlowRecords()) {
    const current = getTaskFlowById(flow.flowId);
    if (!current) {
      continue;
    }
    if (shouldFinalizeCancelledFlow(current)) {
      if (finalizeCancelledFlow(current, now)) {
        reconciled += 1;
      }
      continue;
    }
    if (shouldNormalizeTerminalTimestamps(current)) {
      if (normalizeTerminalTimestamps(current)) {
        reconciled += 1;
      }
      continue;
    }
    if (shouldMarkStaleRunningManagedFlowLost(current, now)) {
      if (markStaleRunningManagedFlowLost(current, now)) {
        reconciled += 1;
      }
      continue;
    }
    if (shouldPruneFlow(current, now) && deleteTaskFlowRecordById(current.flowId)) {
      pruned += 1;
    }
  }
  return { reconciled, pruned };
}
