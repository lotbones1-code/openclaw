import type { CronJob } from "../cron/types.js";
import type { ReliabilityHealthSnapshot } from "../reliability/supervisor.types.js";
import type { TaskFlowRecord, JsonValue } from "../tasks/task-flow-registry.types.js";
import type { TaskRecord } from "../tasks/task-registry.types.js";
import {
  POOL_CAPS,
  POOL_RANK,
  PRIORITY_RANK,
  WORK_POOLS,
  type BuildWorkManagerSnapshotInput,
  type ManagedWorkStatus,
  type WorkAdmissionDecision,
  type WorkManagerCandidate,
  type WorkManagerMode,
  type WorkManagerSnapshot,
  type WorkManagerStatusSummary,
  type WorkPool,
  type WorkPriority,
  type WorkResourceLock,
} from "./work-manager.types.js";

export type {
  BuildWorkManagerSnapshotInput,
  ManagedWorkStatus,
  WorkAdmissionDecision,
  WorkManagerCandidate,
  WorkManagerMode,
  WorkManagerSnapshot,
  WorkManagerStatusSummary,
  WorkPool,
  WorkPriority,
  WorkResourceLock,
} from "./work-manager.types.js";

const DEFAULT_LEASE_MS = 30 * 60_000;
const MAX_HANDOFF_DEPTH_V1 = 1;

export function buildWorkManagerSnapshot(
  input: BuildWorkManagerSnapshotInput,
): WorkManagerSnapshot {
  const nowMs = input.nowMs ?? Date.now();
  const mode = input.mode ?? "shadow";
  const candidates = [
    ...(input.tasks ?? []).map((task) => candidateFromTask(task)),
    ...(input.taskFlows ?? []).map((flow) => candidateFromTaskFlow(flow)),
    ...(input.cronJobs ?? []).flatMap((job) => candidateFromCronJob(job)),
  ].filter((candidate): candidate is WorkManagerCandidate => Boolean(candidate));

  const runningByPool = emptyPoolCounts();
  const queuedByPool = emptyPoolCounts();
  const blockedByPool = emptyPoolCounts();
  const locks: WorkResourceLock[] = [];
  const deadLetteredWork: WorkManagerCandidate[] = [];
  const completedWork: WorkManagerCandidate[] = [];

  for (const candidate of candidates) {
    const status = candidate.status ?? "queued";
    if (status === "running") {
      runningByPool[candidate.pool] += 1;
      for (const resource of candidate.requestedResources) {
        const leaseUntil = candidate.leaseUntil ?? candidate.createdAt + DEFAULT_LEASE_MS;
        if (leaseUntil <= nowMs) {
          continue;
        }
        locks.push({
          lockId: `${resource}:${candidate.workId}`,
          ownerWorkId: candidate.workId,
          resource,
          pool: candidate.pool,
          priority: candidate.priority,
          acquiredAt: candidate.createdAt,
          leaseUntil,
          enforced: mode !== "shadow",
        });
      }
    } else if (status === "queued") {
      queuedByPool[candidate.pool] += 1;
    } else if (status === "blocked") {
      blockedByPool[candidate.pool] += 1;
    } else if (status === "dead_lettered") {
      blockedByPool[candidate.pool] += 1;
      deadLetteredWork.push(candidate);
    } else if (isTerminalWorkStatus(status)) {
      completedWork.push(candidate);
    }
  }

  const pressure = summarizeReliabilityPressure(input.reliability);
  const runnableCandidates = candidates.filter((candidate) => {
    const status = candidate.status ?? "queued";
    return status === "queued" || status === "blocked";
  });
  const rankedRunnable = rankWorkCandidates(runnableCandidates).filter(
    (candidate) => evaluateWorkAdmissionFromLocks(mode, locks, candidate).decision !== "queue",
  );

  return {
    version: 1,
    generatedAt: nowMs,
    mode,
    status: pressure.reliabilityStatus,
    runningByPool,
    queuedByPool,
    blockedByPool,
    locks,
    blockedLocks: locks,
    deadLetteredWork,
    p0p1RevenueWork: candidates.filter(
      (candidate) =>
        candidate.pool === "revenue" &&
        (candidate.priority === "P0" || candidate.priority === "P1") &&
        !isTerminalWorkStatus(candidate.status ?? "queued"),
    ),
    nextBestSafeWork: rankedRunnable[0],
    lastCompletedWork: completedWork.sort((a, b) => b.createdAt - a.createdAt)[0],
    pressure,
    candidates,
  };
}

export function evaluateWorkAdmission(
  snapshot: WorkManagerSnapshot,
  candidate: WorkManagerCandidate,
): WorkAdmissionDecision {
  if ((candidate.handoffDepth ?? 0) > MAX_HANDOFF_DEPTH_V1) {
    return { decision: "block", reason: "handoff_depth_exceeded" };
  }
  if (snapshot.mode === "shadow") {
    return { decision: "allow", reason: "shadow_only" };
  }

  const lockDecision = evaluateWorkAdmissionFromLocks(snapshot.mode, snapshot.locks, candidate);
  if (lockDecision.decision !== "allow") {
    return lockDecision;
  }

  const cap = POOL_CAPS[candidate.pool];
  if (typeof cap === "number" && snapshot.runningByPool[candidate.pool] >= cap) {
    return { decision: "queue", reason: `pool_cap:${candidate.pool}` };
  }

  const heavyModelConflict =
    candidate.requestedResources.includes("model:heavy") &&
    snapshot.locks.some((lock) => lock.resource === "model:heavy");
  if (heavyModelConflict) {
    return { decision: "queue", reason: "model_heavy_cap", blockedResources: ["model:heavy"] };
  }

  if (
    (snapshot.status === "yellow" || snapshot.status === "red") &&
    (candidate.priority === "P4" || candidate.priority === "P5")
  ) {
    return { decision: "queue", reason: "backpressure" };
  }

  return { decision: "allow", reason: "available" };
}

export function rankWorkCandidates(candidates: WorkManagerCandidate[]): WorkManagerCandidate[] {
  return [...candidates].sort((a, b) => {
    const priorityDelta = PRIORITY_RANK[a.priority] - PRIORITY_RANK[b.priority];
    if (priorityDelta !== 0) {
      return priorityDelta;
    }
    const poolDelta = POOL_RANK[a.pool] - POOL_RANK[b.pool];
    if (poolDelta !== 0) {
      return poolDelta;
    }
    return a.createdAt - b.createdAt;
  });
}

export function summarizeWorkManagerStatus(
  snapshot: WorkManagerSnapshot,
): WorkManagerStatusSummary {
  return {
    mode: snapshot.mode,
    status: snapshot.status,
    runningByPool: snapshot.runningByPool,
    queuedByPool: snapshot.queuedByPool,
    blockedLocks: snapshot.blockedLocks,
    deadLetteredWork: snapshot.deadLetteredWork,
    p0p1RevenueWork: snapshot.p0p1RevenueWork,
    nextBestSafeWork: snapshot.nextBestSafeWork,
    lastCompletedWork: snapshot.lastCompletedWork,
  };
}

export function toHumanWorkStatusLines(snapshot: WorkManagerSnapshot): string[] {
  const summary = summarizeWorkManagerStatus(snapshot);
  const running = compactPoolCounts(summary.runningByPool);
  const queued = compactPoolCounts(summary.queuedByPool);
  const p0p1 = summary.p0p1RevenueWork.length;
  const next = summary.nextBestSafeWork
    ? `${summary.nextBestSafeWork.priority} ${summary.nextBestSafeWork.lane}`
    : "none";
  const blocked = summary.blockedLocks.length;
  const dead = summary.deadLetteredWork.length;
  return [
    `Work Manager ${summary.mode}: ${summary.status}`,
    `running: ${running || "none"} | queued: ${queued || "none"}`,
    `blocked locks: ${blocked} | dead-lettered: ${dead} | P0/P1 revenue: ${p0p1}`,
    `next best safe work: ${next}`,
    "report: openclaw work status --json",
  ];
}

function evaluateWorkAdmissionFromLocks(
  mode: WorkManagerMode,
  locks: WorkResourceLock[],
  candidate: WorkManagerCandidate,
): WorkAdmissionDecision {
  if (mode === "shadow") {
    return { decision: "allow", reason: "shadow_only" };
  }
  const blockedResources = candidate.requestedResources.filter((resource) =>
    locks.some((lock) => lock.resource === resource && lock.ownerWorkId !== candidate.workId),
  );
  if (blockedResources.length > 0) {
    return {
      decision: "queue",
      reason: "resource_locked",
      resource: blockedResources[0],
      blockedResources,
    };
  }
  return { decision: "allow", reason: "available" };
}

function emptyPoolCounts(): Record<WorkPool, number> {
  return Object.fromEntries(WORK_POOLS.map((pool) => [pool, 0])) as Record<WorkPool, number>;
}

function compactPoolCounts(counts: Record<WorkPool, number>): string {
  return WORK_POOLS.filter((pool) => counts[pool] > 0)
    .map((pool) => `${pool} ${counts[pool]}`)
    .join(", ");
}

function isTerminalWorkStatus(status: ManagedWorkStatus): boolean {
  return (
    status === "succeeded" || status === "failed" || status === "cancelled" || status === "lost"
  );
}

function candidateFromTask(task: TaskRecord): WorkManagerCandidate {
  const inferred = inferWork(task.label, task.task, task.sourceId);
  return {
    workId: task.taskId,
    lane: task.label ?? task.sourceId ?? task.taskKind ?? "task",
    pool: inferred.pool,
    priority: inferred.priority,
    requestedResources: inferred.resources,
    expectedOutput: task.task,
    proofPath: task.childSessionKey ?? task.runId ?? "",
    timeoutMs: 30 * 60_000,
    owner: task.ownerKey,
    status: taskStatusToWorkStatus(task.status),
    createdAt: task.startedAt ?? task.createdAt,
    leaseUntil: task.cleanupAfter,
    parentWorkId: task.parentTaskId ?? task.parentFlowId,
  };
}

function candidateFromTaskFlow(flow: TaskFlowRecord): WorkManagerCandidate {
  const metadata = readWorkManagerMetadata(flow.stateJson);
  const inferred = inferWork(flow.goal, flow.currentStep, flow.ownerKey);
  return {
    workId: metadata.workId ?? flow.flowId,
    lane: metadata.lane ?? flow.goal,
    pool: metadata.pool ?? inferred.pool,
    priority: metadata.priority ?? inferred.priority,
    requestedResources: metadata.requestedResources ?? inferred.resources,
    expectedOutput: metadata.expectedOutput ?? flow.goal,
    proofPath: metadata.proofPath ?? "",
    timeoutMs: metadata.timeoutMs ?? 30 * 60_000,
    owner: metadata.owner ?? flow.ownerKey,
    status: metadata.status ?? flowStatusToWorkStatus(flow.status),
    createdAt: metadata.createdAt ?? flow.createdAt,
    leaseUntil: metadata.leaseUntil,
    parentWorkId: metadata.parentWorkId,
    handoffDepth: metadata.handoffDepth,
  };
}

function candidateFromCronJob(job: CronJob): WorkManagerCandidate[] {
  if (typeof job.state.runningAtMs !== "number") {
    return [];
  }
  const payloadText = job.payload.kind === "agentTurn" ? job.payload.message : job.payload.text;
  const inferred = inferWork(job.name, payloadText, job.id);
  return [
    {
      workId: `cron:${job.id}:${job.state.runningAtMs}`,
      lane: job.name,
      pool: inferred.pool,
      priority: inferred.priority,
      requestedResources: inferred.resources,
      expectedOutput: payloadText,
      proofPath: job.id,
      timeoutMs: 30 * 60_000,
      owner: "cron",
      status: "running",
      createdAt: job.state.runningAtMs,
      leaseUntil: job.state.runningAtMs + 30 * 60_000,
    },
  ];
}

function taskStatusToWorkStatus(status: TaskRecord["status"]): ManagedWorkStatus {
  if (status === "timed_out") {
    return "failed";
  }
  return status;
}

function flowStatusToWorkStatus(status: TaskFlowRecord["status"]): ManagedWorkStatus {
  if (status === "waiting") {
    return "running";
  }
  if (status === "cancelled" || status === "lost") {
    return status;
  }
  return status === "blocked" ? "blocked" : status;
}

function readWorkManagerMetadata(value: JsonValue | undefined): Partial<WorkManagerCandidate> {
  const root = isRecord(value) ? (value.openclawWorkManager ?? value.workManager) : undefined;
  if (!isRecord(root)) {
    return {};
  }
  return {
    workId: stringValue(root.workId),
    lane: stringValue(root.lane),
    pool: workPoolValue(root.pool),
    priority: workPriorityValue(root.priority),
    requestedResources: stringArrayValue(root.requestedResources),
    expectedOutput: stringValue(root.expectedOutput),
    proofPath: stringValue(root.proofPath),
    timeoutMs: numberValue(root.timeoutMs),
    owner: stringValue(root.owner),
    status: workStatusValue(root.workStatus),
    createdAt: numberValue(root.createdAt),
    leaseUntil: numberValue(root.leaseUntil),
    parentWorkId: stringValue(root.parentWorkId),
    handoffDepth: numberValue(root.handoffDepth),
  } as Partial<WorkManagerCandidate>;
}

function inferWork(...parts: Array<string | undefined>): {
  pool: WorkPool;
  priority: WorkPriority;
  resources: string[];
} {
  const text = parts.filter(Boolean).join(" ").toLowerCase();
  const resources = new Set<string>();
  let pool: WorkPool = "unknown";
  let priority: WorkPriority = "P4";

  if (
    /\b(payment|wallet|order|checkout|buyer|sale|sales|revenue|attribution|inbox|dm triage|warm lead)\b/.test(
      text,
    )
  ) {
    pool = "revenue";
    priority = /\b(payment|wallet|order|checkout broken|buyer signal|warm lead)\b/.test(text)
      ? "P0"
      : "P1";
  }
  if (/\b(instagram|ig|titan-ig|shamil-ig|comment|social|post|reel|x reply|twitter)\b/.test(text)) {
    pool = pool === "revenue" ? "revenue" : "social";
    priority = priority === "P0" || priority === "P1" ? priority : "P2";
  }
  if (/\b(build|test|source|runtime|openclawv2|openclaw-runtime-src|repo|commit)\b/.test(text)) {
    pool = POOL_RANK[pool] >= POOL_RANK.maintenance ? "build" : pool;
    priority = PRIORITY_RANK[priority] >= PRIORITY_RANK.P4 ? "P3" : priority;
  }
  if (/\b(research|scrape|target enrichment|seo|cro|crawl)\b/.test(text)) {
    pool = pool === "unknown" ? "research" : pool;
    priority = priority === "P4" ? "P2" : priority;
  }
  if (/\b(personal|assistant|calendar|email|reminder|shamil-support)\b/.test(text)) {
    pool = pool === "unknown" ? "personal" : pool;
    priority = priority === "P4" ? "P2" : priority;
  }
  if (/\b(cleanup|debrief|maintenance|compact|archive)\b/.test(text) && pool === "unknown") {
    pool = "maintenance";
    priority = "P5";
  }

  if (
    text.includes("titan-ig") ||
    text.includes("titan brand") ||
    text.includes("@titan.peptidelab")
  ) {
    resources.add("browser_profile:titan-ig");
    resources.add("social_account:titan-ig");
  }
  if (text.includes("shamil-ig") || text.includes("shamilkch17")) {
    resources.add("browser_profile:shamil-ig");
    resources.add("social_account:shamilkch17");
  }
  if (text.includes("openclawv2")) {
    resources.add("repo:openclawv2");
  }
  if (text.includes("openclaw-runtime-src") || text.includes("runtime")) {
    resources.add("repo:openclaw-runtime-src");
  }
  if (text.includes("project_state")) {
    resources.add("vault:PROJECT_STATE");
  }
  if (text.includes("ops board")) {
    resources.add("vault:OpsBoard");
  }
  if (text.includes("checkout")) {
    resources.add("checkout:titan");
  }
  if (text.includes("payment") || text.includes("wallet")) {
    resources.add("payment/wallet");
  }
  if (text.includes("heavy model") || text.includes("claude") || text.includes("gpt-5.5")) {
    resources.add("model:heavy");
  }

  return { pool, priority, resources: [...resources] };
}

function summarizeReliabilityPressure(snapshot: ReliabilityHealthSnapshot | undefined) {
  return {
    reliabilityStatus: snapshot?.status ?? "green",
    staleTasks: findingCount(snapshot, "tasks"),
    dirtyCronMarkers: findingCount(snapshot, "cron"),
    modelIssues: findingCount(snapshot, "models"),
    mcpIssues: findingCount(snapshot, "mcp"),
    deliveryIssues: findingCount(snapshot, "delivery"),
    sessionPressure: findingCount(snapshot, "sessions"),
  };
}

function findingCount(
  snapshot: ReliabilityHealthSnapshot | undefined,
  subsystem: keyof ReliabilityHealthSnapshot["subsystems"],
): number {
  return snapshot?.subsystems[subsystem]?.findings.length ?? 0;
}

function isRecord(value: unknown): value is Record<string, JsonValue> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function stringValue(value: JsonValue | undefined): string | undefined {
  return typeof value === "string" && value.length > 0 ? value : undefined;
}

function numberValue(value: JsonValue | undefined): number | undefined {
  return typeof value === "number" && Number.isFinite(value) ? value : undefined;
}

function stringArrayValue(value: JsonValue | undefined): string[] | undefined {
  if (!Array.isArray(value)) {
    return undefined;
  }
  const strings = value.filter(
    (item): item is string => typeof item === "string" && item.length > 0,
  );
  return strings.length > 0 ? strings : undefined;
}

function workPoolValue(value: JsonValue | undefined): WorkPool | undefined {
  return typeof value === "string" && WORK_POOLS.includes(value as WorkPool)
    ? (value as WorkPool)
    : undefined;
}

function workPriorityValue(value: JsonValue | undefined): WorkPriority | undefined {
  return typeof value === "string" && value in PRIORITY_RANK ? (value as WorkPriority) : undefined;
}

function workStatusValue(value: JsonValue | undefined): ManagedWorkStatus | undefined {
  const statuses: ManagedWorkStatus[] = [
    "queued",
    "running",
    "blocked",
    "dead_lettered",
    "succeeded",
    "failed",
    "cancelled",
    "lost",
  ];
  return typeof value === "string" && statuses.includes(value as ManagedWorkStatus)
    ? (value as ManagedWorkStatus)
    : undefined;
}
