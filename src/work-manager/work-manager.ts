import type { CronJob } from "../cron/types.js";
import {
  validateOpenClawDirectiveContract,
  type OpenClawDirectiveContract,
} from "../execution-kernel/execution-kernel.js";
import type { ReliabilityHealthSnapshot } from "../reliability/supervisor.types.js";
import type { TaskControlRecord } from "../tasks/task-control-registry.js";
import type { TaskFlowRecord, JsonValue } from "../tasks/task-flow-registry.types.js";
import type { TaskRecord } from "../tasks/task-registry.types.js";
import {
  POOL_CAPS,
  POOL_RANK,
  PRIORITY_RANK,
  WORK_POOLS,
  type BuildWorkManagerSnapshotInput,
  type ManagedWorkStatus,
  type MissionRevenueFloorDecision,
  type MissedWorkLedgerRow,
  type MissionValidationResult,
  type OpenClawMissionContract,
  type WorkAdmissionDecision,
  type WorkDispatchEffect,
  type WorkDispatchProof,
  type WorkLivenessHandoffDecision,
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
  MissionRevenueFloorDecision,
  MissedWorkLedgerRow,
  WorkAdmissionDecision,
  WorkDispatchEffect,
  WorkDispatchProof,
  WorkLivenessHandoffDecision,
  MissionValidationResult,
  OpenClawMissionContract,
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
const CRON_RUN_ID_DRIFT_MS = 5_000;
const MISSION_REVENUE_FLOOR_COOLDOWN_MS = 10 * 60_000;

export function buildWorkManagerSnapshot(
  input: BuildWorkManagerSnapshotInput,
): WorkManagerSnapshot {
  const nowMs = input.nowMs ?? Date.now();
  const mode = input.mode ?? "shadow";
  const tasks = input.tasks ?? [];
  const taskControls = input.taskControls ?? [];
  const taskRunIds = new Set(
    tasks
      .map((task) => task.runId)
      .filter((runId): runId is string => typeof runId === "string" && runId.length > 0),
  );
  const activeCronJobIds = new Set(
    tasks
      .filter((task) => task.status === "running" || task.status === "queued")
      .map((task) => task.sourceId)
      .filter(
        (sourceId): sourceId is string => typeof sourceId === "string" && sourceId.length > 0,
      ),
  );
  const candidates = [
    ...tasks.map((task) => candidateFromTask(task, taskControls)),
    ...(input.taskFlows ?? []).map((flow) => candidateFromTaskFlow(flow)),
    ...(input.cronJobs ?? []).flatMap((job) => candidateFromCronJob(job, taskRunIds, tasks)),
  ].filter(
    (candidate): candidate is WorkManagerCandidate =>
      Boolean(candidate) && !isQueuedCronCandidateShadowedByActiveRun(candidate, activeCronJobIds),
  );

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
  const activeMission = resolveActiveMission(input.taskFlows ?? []);
  const activeDirective = resolveActiveDirective(input.taskFlows ?? []);
  const queuedP0P1Work = candidates.filter((candidate) => {
    const status = candidate.status ?? "queued";
    return status === "queued" && isP0P1OrDirective(candidate.priority);
  });
  const runnableCandidates = candidates.filter((candidate) => {
    const status = candidate.status ?? "queued";
    return status === "queued" || status === "blocked";
  });
  const rankedRunnable = rankWorkCandidates(runnableCandidates).filter(
    (candidate) => evaluateWorkAdmissionFromLocks(mode, locks, candidate).decision !== "queue",
  );

  const snapshot: WorkManagerSnapshot = {
    version: 1,
    generatedAt: nowMs,
    mode,
    status: pressure.reliabilityStatus,
    activeMission,
    activeDirective,
    runningByPool,
    queuedByPool,
    blockedByPool,
    locks,
    blockedLocks: locks,
    deadLetteredWork,
    p0p1RevenueWork: candidates.filter(
      (candidate) =>
        candidate.pool === "revenue" &&
        isP0P1OrDirective(candidate.priority) &&
        !isTerminalWorkStatus(candidate.status ?? "queued"),
    ),
    queuedP0P1Work,
    queueDrain: { decision: "none", reason: "no_queued_p0_p1" },
    missedWorkCount: resolveMissedWorkCount(input.taskFlows ?? []),
    lastRecoveryAction: resolveLastRecoveryAction(input.taskFlows ?? []),
    nextBestSafeWork: rankedRunnable[0],
    lastCompletedWork: completedWork.sort((a, b) => b.createdAt - a.createdAt)[0],
    pressure,
    candidates,
  };

  const selectedQueueDrain = selectQueuedWorkForDispatch(snapshot);
  snapshot.queueDrain =
    input.dispatchProofEnabled &&
    selectedQueueDrain.decision === "dispatch" &&
    validateQueueDrainDispatchProof(selectedQueueDrain).valid === false
      ? {
          decision: "blocked",
          reason: "DISPATCH_PROOF_MISSING",
          candidate: selectedQueueDrain.candidate,
          nextDispatchCheckAt: nowMs + 60_000,
        }
      : selectedQueueDrain;
  return snapshot;
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

export function validateMissionContract(value: unknown): MissionValidationResult {
  const requiredStringFields = [
    "mission_id",
    "user_request",
    "selected_option",
    "objective",
    "start_time",
    "wake_time",
    "status",
    "proof_path",
  ];
  const requiredArrayFields = ["allowed_lanes", "hard_gates", "success_criteria"];
  const missing: string[] = [];
  if (!isRecord(value)) {
    return {
      valid: false,
      missing: [...requiredStringFields, "priority", ...requiredArrayFields, "minimum_work_floor"],
    };
  }
  for (const field of requiredStringFields) {
    if (typeof value[field] !== "string" || value[field].length === 0) {
      missing.push(field);
    }
  }
  if (!workPriorityValue(value.priority)) {
    missing.push("priority");
  }
  for (const field of requiredArrayFields) {
    if (!Array.isArray(value[field])) {
      missing.push(field);
    }
  }
  if (typeof value.minimum_work_floor !== "number" || !Number.isFinite(value.minimum_work_floor)) {
    missing.push("minimum_work_floor");
  }
  return { valid: missing.length === 0, missing };
}

export function buildOpenClawMissionContract(params: {
  missionId: string;
  userRequest: string;
  selectedOption: string;
  nowMs: number;
  wakeTime: string;
  proofPath: string;
  objective?: string;
  minimumWorkFloor?: number;
}): OpenClawMissionContract {
  return {
    mission_id: params.missionId,
    user_request: params.userRequest,
    selected_option: params.selectedOption,
    objective:
      params.objective ??
      "Keep safe revenue work moving until sale, buyer signal, or wake report proof exists.",
    priority: "P0_USER_DIRECTIVE",
    allowed_lanes: [
      "revenue",
      "buyer_signals",
      "inbox_dm",
      "attribution",
      "checkout_cro",
      "content_packets",
      "target_sourcing",
    ],
    hard_gates: [
      "public_social_action_requires_existing_platform_gates",
      "payment_mutation_requires_existing_payment_gate",
      "dns_mutation_requires_existing_dns_gate",
      "account_security_change_requires_existing_account_gate",
      "customer_record_mutation_requires_existing_customer_record_gate",
    ],
    minimum_work_floor: params.minimumWorkFloor ?? 1,
    success_criteria: [
      "sale/payment_seen",
      "qualified_buyer_signal",
      "one_safe_revenue_unit_per_hour_until_wake_time",
    ],
    start_time: new Date(params.nowMs).toISOString(),
    wake_time: params.wakeTime,
    status: "active",
    proof_path: params.proofPath,
  };
}

export function selectMissionRevenueFloorCronJob(params: {
  snapshot: WorkManagerSnapshot;
  cronJobs: CronJob[];
  nowMs: number;
}): MissionRevenueFloorDecision {
  const mission = params.snapshot.activeMission;
  if (!mission) {
    return { decision: "none", reason: "no_active_mission" };
  }
  if (!isRevenueMission(mission)) {
    return { decision: "none", reason: "mission_not_revenue" };
  }
  const activeP0P1 = params.snapshot.p0p1RevenueWork.filter((candidate) =>
    isMissionBlockingP0P1(candidate),
  );
  const queuedP0P1 = params.snapshot.queuedP0P1Work.filter((candidate) =>
    isMissionBlockingP0P1(candidate),
  );
  if (activeP0P1.length > 0 || queuedP0P1.length > 0) {
    return { decision: "none", reason: "p0_p1_already_active" };
  }

  const candidates = params.cronJobs
    .filter((job) => isMissionRevenueFloorJob(job, params.nowMs))
    .map((job) => ({
      job,
      candidate: createCronWorkCandidate({ job, nowMs: params.nowMs, status: "queued" }),
    }))
    .filter(({ candidate }) => isMissionRevenueFloorCandidate(candidate));
  const ranked = rankWorkCandidates(candidates.map(({ candidate }) => candidate));
  const selected = ranked[0];
  if (!selected) {
    return { decision: "none", reason: "no_candidate" };
  }
  const job = candidates.find((entry) => entry.candidate.workId === selected.workId)?.job;
  if (!job) {
    return { decision: "none", reason: "no_candidate" };
  }
  return { decision: "promote", reason: "revenue_floor", jobId: job.id, candidate: selected };
}

export function selectQueuedWorkForDispatch(snapshot: WorkManagerSnapshot) {
  const ranked = rankWorkCandidates(snapshot.queuedP0P1Work);
  const productive = ranked.filter((candidate) => !isReadOnlySensorOrStatusCandidate(candidate));
  const candidates = productive.length > 0 ? productive : ranked;
  if (candidates.length === 0) {
    return { decision: "none" as const, reason: "no_queued_p0_p1" as const };
  }
  let firstBlocked:
    | {
        candidate: WorkManagerCandidate;
        admission: Exclude<WorkAdmissionDecision, { decision: "allow" }>;
      }
    | undefined;
  for (const candidate of candidates) {
    const admission = evaluateWorkAdmission(snapshot, candidate);
    if (admission.decision === "allow") {
      return { decision: "dispatch" as const, reason: "available" as const, candidate };
    }
    firstBlocked ??= { candidate, admission };
  }
  const blocked = firstBlocked!;
  return {
    decision: "blocked" as const,
    reason: blocked.admission.reason,
    candidate: blocked.candidate,
    resource: blocked.admission.resource,
    blockedResources: blocked.admission.blockedResources,
    nextDispatchCheckAt: blocked.candidate.leaseUntil,
  };
}

export function applyQueuedWorkDispatchProof(
  decision: WorkManagerSnapshot["queueDrain"],
  proof: Omit<WorkDispatchProof, "workId" | "proofPath" | "expectedOutput"> & {
    workId?: string;
    proofPath?: string;
    expectedOutput?: string;
  },
): WorkManagerSnapshot["queueDrain"] {
  if (decision.decision !== "dispatch") {
    return decision;
  }
  return {
    ...decision,
    dispatchEffect: proof.dispatchEffect,
    workId: proof.workId ?? decision.candidate.workId,
    ...(proof.taskId !== undefined ? { taskId: proof.taskId } : {}),
    ...(proof.cronJobId !== undefined ? { cronJobId: proof.cronJobId } : {}),
    ...(proof.flowId !== undefined ? { flowId: proof.flowId } : {}),
    owner: proof.owner,
    startedAt: proof.startedAt,
    proofPath: proof.proofPath ?? decision.candidate.proofPath,
    expectedOutput: proof.expectedOutput ?? decision.candidate.expectedOutput,
    firstStatusCheck: proof.firstStatusCheck,
  };
}

export function validateQueueDrainDispatchProof(
  decision: WorkManagerSnapshot["queueDrain"],
): { valid: true } | { valid: false; reason: "dispatch_proof_missing" } {
  if (decision.decision !== "dispatch") {
    return { valid: true };
  }
  if (
    decision.dispatchEffect &&
    decision.workId &&
    decision.owner &&
    typeof decision.startedAt === "number" &&
    decision.proofPath &&
    decision.expectedOutput &&
    typeof decision.firstStatusCheck === "number" &&
    (decision.dispatchEffect === "exact_gate" ||
      Boolean(decision.taskId || decision.cronJobId || decision.flowId))
  ) {
    return { valid: true };
  }
  return { valid: false, reason: "dispatch_proof_missing" };
}

export function evaluateLivenessHandoff(params: {
  snapshot: WorkManagerSnapshot;
  completedWork: WorkManagerCandidate;
  proposedNextWork?: WorkManagerCandidate;
}): WorkLivenessHandoffDecision {
  if (!["revenue", "social", "build"].includes(params.completedWork.pool)) {
    return { decision: "none_available", reason: "non_handoff_pool" };
  }
  if ((params.completedWork.handoffDepth ?? 0) >= MAX_HANDOFF_DEPTH_V1) {
    return { decision: "none_available", reason: "handoff_depth_exceeded" };
  }
  if (!params.proposedNextWork) {
    return { decision: "none_available", reason: "no_candidate" };
  }
  const candidate: WorkManagerCandidate = {
    ...params.proposedNextWork,
    handoffDepth: (params.completedWork.handoffDepth ?? 0) + 1,
    parentWorkId: params.completedWork.workId,
  };
  const admission = evaluateWorkAdmission(params.snapshot, candidate);
  if (admission.decision === "allow") {
    return { decision: "selected", reason: "available", candidate };
  }
  return {
    decision: "queued",
    reason: admission.reason,
    candidate,
    resource: admission.resource,
    blockedResources: admission.blockedResources,
  };
}

export function buildMissedWorkLedger(params: {
  promisedWork: string[];
  shippedWork: string[];
  recoveryCandidates?: WorkManagerCandidate[];
  defaultReason?: string;
  proofPath?: string;
}): MissedWorkLedgerRow[] {
  const shipped = new Set(params.shippedWork);
  const candidates = rankWorkCandidates(params.recoveryCandidates ?? []);
  return params.promisedWork
    .filter((promised) => !shipped.has(promised))
    .map((promised) => {
      const recovery = candidates.find((candidate) =>
        [candidate.workId, candidate.lane, candidate.expectedOutput].includes(promised),
      );
      const fallback = candidates[0];
      const next = recovery ?? fallback;
      return {
        promised_work: promised,
        shipped_work: "not_shipped",
        missed_work: promised,
        reason: params.defaultReason ?? "not_shipped",
        recover_now: Boolean(next),
        next_safe_recovery_unit: next?.lane ?? "none_available",
        owner_lane: next?.owner ?? "unknown",
        proof_path: next?.proofPath ?? params.proofPath ?? "",
      };
    });
}

export function createCronWorkCandidate(params: {
  job: CronJob;
  nowMs: number;
  status?: ManagedWorkStatus;
}): WorkManagerCandidate {
  const payloadText =
    params.job.payload.kind === "agentTurn" ? params.job.payload.message : params.job.payload.text;
  const inferred = inferWork(params.job.name, payloadText, params.job.id);
  return {
    workId: `cron:${params.job.id}:${params.nowMs}`,
    lane: params.job.name,
    pool: inferred.pool,
    priority: inferred.priority,
    requestedResources: inferred.resources,
    expectedOutput: payloadText,
    proofPath: params.job.id,
    timeoutMs: 30 * 60_000,
    owner: "cron",
    status: params.status ?? "queued",
    createdAt: params.nowMs,
    leaseUntil: params.nowMs + 30 * 60_000,
  };
}

export function summarizeWorkManagerStatus(
  snapshot: WorkManagerSnapshot,
): WorkManagerStatusSummary {
  return {
    mode: snapshot.mode,
    status: snapshot.status,
    activeMission: snapshot.activeMission,
    activeDirective: snapshot.activeDirective,
    runningByPool: snapshot.runningByPool,
    queuedByPool: snapshot.queuedByPool,
    blockedLocks: snapshot.blockedLocks,
    deadLetteredWork: snapshot.deadLetteredWork,
    p0p1RevenueWork: snapshot.p0p1RevenueWork,
    queuedP0P1Work: snapshot.queuedP0P1Work,
    queueDrain: snapshot.queueDrain,
    missedWorkCount: snapshot.missedWorkCount,
    lastRecoveryAction: snapshot.lastRecoveryAction,
    nextBestSafeWork: snapshot.nextBestSafeWork,
    lastCompletedWork: snapshot.lastCompletedWork,
  };
}

export function toHumanWorkStatusLines(snapshot: WorkManagerSnapshot): string[] {
  const summary = summarizeWorkManagerStatus(snapshot);
  const mission = summary.activeMission
    ? `${summary.activeMission.status} ${summary.activeMission.selected_option}`
    : "none";
  const directive = summary.activeDirective
    ? `${summary.activeDirective.current_state} ${summary.activeDirective.selected_option}`
    : "none";
  const running = compactPoolCounts(summary.runningByPool);
  const queued = compactPoolCounts(summary.queuedByPool);
  const p0p1 = summary.p0p1RevenueWork.length;
  const next = summary.nextBestSafeWork
    ? `${summary.nextBestSafeWork.priority} ${summary.nextBestSafeWork.lane}`
    : "none";
  const blocked = summary.blockedLocks.length;
  const dead = summary.deadLetteredWork.length;
  return [
    `Work Manager ${summary.mode}: ${summary.status} | mission: ${mission} | directive: ${directive}`,
    `running: ${running || "none"} | queued: ${queued || "none"}`,
    `blocked locks: ${blocked} | dead-lettered: ${dead} | P0/P1 revenue: ${p0p1} | queued P0/P1: ${summary.queuedP0P1Work.length}`,
    `next best safe work: ${next}`,
    "report: openclaw work status --json",
  ];
}

function isP0P1OrDirective(priority: WorkPriority): boolean {
  return priority === "P0_USER_DIRECTIVE" || priority === "P0" || priority === "P1";
}

function isP0P1P2OrDirective(priority: WorkPriority): boolean {
  return (
    priority === "P0_USER_DIRECTIVE" || priority === "P0" || priority === "P1" || priority === "P2"
  );
}

function isRevenueMission(mission: OpenClawMissionContract): boolean {
  const text = [
    mission.user_request,
    mission.selected_option,
    mission.objective,
    ...mission.allowed_lanes,
    ...mission.success_criteria,
  ]
    .join(" ")
    .toLowerCase();
  return /\b(revenue|sale|sales|buyer|inbox|dm|attribution|checkout|cro|content|target)\b/.test(
    text,
  );
}

function isMissionContractCandidate(candidate: WorkManagerCandidate): boolean {
  return (
    candidate.owner.startsWith("work-manager:mission:") || /mission contract/i.test(candidate.lane)
  );
}

function isMissionRevenueFloorJob(job: CronJob, nowMs: number): boolean {
  if (!job.enabled || typeof job.state.runningAtMs === "number") {
    return false;
  }
  const lastRunAt = job.state.lastRunAtMs;
  if (typeof lastRunAt === "number" && nowMs - lastRunAt < MISSION_REVENUE_FLOOR_COOLDOWN_MS) {
    return false;
  }
  const text = cronJobText(job);
  if (
    isNativeHealthCheckText(text.toLowerCase()) ||
    isStatusDigestText(text.toLowerCase()) ||
    isRevenueDebriefText(text.toLowerCase()) ||
    /\bwallet watcher\b/i.test(text)
  ) {
    return false;
  }
  return /\b(revenue|sale|sales|buyer|inbox|dm|attribution|checkout|cro|content|brand|comment|target enrichment|social|x reply|ig|titan)\b/i.test(
    text,
  );
}

function isMissionRevenueFloorCandidate(candidate: WorkManagerCandidate): boolean {
  if (!["revenue", "social", "research"].includes(candidate.pool)) {
    return false;
  }
  return isP0P1P2OrDirective(candidate.priority);
}

function isMissionBlockingP0P1(candidate: WorkManagerCandidate): boolean {
  return (
    isP0P1OrDirective(candidate.priority) &&
    !isMissionContractCandidate(candidate) &&
    !isReadOnlySensorOrStatusCandidate(candidate)
  );
}

function isReadOnlySensorOrStatusCandidate(candidate: WorkManagerCandidate): boolean {
  const text = [candidate.lane, candidate.expectedOutput].join(" ").toLowerCase();
  return (
    /\b(status digest|shift status|status-only|operator status|work status)\b/.test(text) ||
    /\b(wallet watcher|watch wallet|poll payment wallet|pending_sale_alerts|tick ok)\b/.test(text)
  );
}

function cronJobText(job: CronJob): string {
  const payloadText = job.payload.kind === "agentTurn" ? job.payload.message : job.payload.text;
  return [job.id, job.name, payloadText].filter(Boolean).join(" ");
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

function candidateFromTask(
  task: TaskRecord,
  taskControls: readonly TaskControlRecord[],
): WorkManagerCandidate {
  const inferred = inferWork(task.label, task.task, task.sourceId);
  const stopControl = findMatchingStopControl(task, taskControls);
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
    status: stopControl ? "cancelled" : taskStatusToWorkStatus(task.status),
    createdAt: task.startedAt ?? task.createdAt,
    leaseUntil: task.cleanupAfter,
    parentWorkId: task.parentTaskId ?? task.parentFlowId,
  };
}

function findMatchingStopControl(
  task: TaskRecord,
  taskControls: readonly TaskControlRecord[],
): TaskControlRecord | undefined {
  return taskControls.find((record) => {
    if (record.state !== "requested") {
      return false;
    }
    if (
      record.command !== "stop" &&
      record.command !== "pause" &&
      record.command !== "cancel" &&
      record.command !== "stop_browser" &&
      record.command !== "stop_social" &&
      record.command !== "stop_personal_assistant" &&
      record.command !== "red_stop_all"
    ) {
      return false;
    }
    if (record.scope === "global" || record.command === "red_stop_all") {
      return true;
    }
    return Boolean(
      (record.taskId && record.taskId === task.taskId) ||
      (record.runId && record.runId === task.runId) ||
      (record.sessionKey &&
        (record.sessionKey === task.childSessionKey ||
          record.sessionKey === task.requesterSessionKey ||
          record.sessionKey === task.ownerKey)),
    );
  });
}

function candidateFromTaskFlow(flow: TaskFlowRecord): WorkManagerCandidate {
  const metadata = readWorkManagerMetadata(flow.stateJson);
  const inferred = inferWork(flow.goal, flow.currentStep, flow.ownerKey);
  return normalizeReadOnlySpecialCandidate({
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
  });
}

function candidateFromCronJob(
  job: CronJob,
  taskRunIds: ReadonlySet<string>,
  tasks: readonly TaskRecord[],
): WorkManagerCandidate[] {
  if (typeof job.state.runningAtMs !== "number") {
    return [];
  }
  if (taskRunIds.has(`cron:${job.id}:${job.state.runningAtMs}`)) {
    return [];
  }
  if (hasTaskForCronRunningMarker(job, tasks)) {
    return [];
  }
  return [
    createCronWorkCandidate({
      job,
      nowMs: job.state.runningAtMs,
      status: "running",
    }),
  ];
}

function hasTaskForCronRunningMarker(job: CronJob, tasks: readonly TaskRecord[]): boolean {
  const runningAtMs = job.state.runningAtMs;
  if (typeof runningAtMs !== "number" || !Number.isFinite(runningAtMs)) {
    return false;
  }
  return tasks.some((task) => {
    const parsed = parseCronRunId(task.runId);
    if (parsed && parsed.jobId === job.id) {
      return Math.abs(parsed.startedAtMs - runningAtMs) <= CRON_RUN_ID_DRIFT_MS;
    }
    if (task.sourceId === job.id && typeof task.startedAt === "number") {
      return Math.abs(task.startedAt - runningAtMs) <= CRON_RUN_ID_DRIFT_MS;
    }
    return false;
  });
}

function parseCronRunId(runId: string | undefined): { jobId: string; startedAtMs: number } | null {
  if (!runId?.startsWith("cron:")) {
    return null;
  }
  const lastColon = runId.lastIndexOf(":");
  if (lastColon <= "cron:".length) {
    return null;
  }
  const startedAtMs = Number(runId.slice(lastColon + 1));
  if (!Number.isFinite(startedAtMs)) {
    return null;
  }
  return {
    jobId: runId.slice("cron:".length, lastColon),
    startedAtMs,
  };
}

function isQueuedCronCandidateShadowedByActiveRun(
  candidate: WorkManagerCandidate,
  activeCronJobIds: ReadonlySet<string>,
): boolean {
  if ((candidate.status ?? "queued") !== "queued") {
    return false;
  }
  const parsed = parseCronRunId(candidate.workId);
  return Boolean(parsed && activeCronJobIds.has(parsed.jobId));
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

function isTerminalTaskFlowStatus(status: TaskFlowRecord["status"]): boolean {
  return (
    status === "succeeded" || status === "failed" || status === "cancelled" || status === "lost"
  );
}

function resolveActiveMission(flows: TaskFlowRecord[]): OpenClawMissionContract | undefined {
  return flows
    .map((flow) => ({
      mission: readMissionContract(flow.stateJson),
      flowStatus: flow.status,
      updatedAt: flow.updatedAt,
    }))
    .filter(
      (
        entry,
      ): entry is {
        mission: OpenClawMissionContract;
        flowStatus: TaskFlowRecord["status"];
        updatedAt: number;
      } =>
        Boolean(entry.mission) &&
        !isTerminalTaskFlowStatus(entry.flowStatus) &&
        (entry.mission?.status === "active" ||
          entry.mission?.status === "queued" ||
          entry.mission?.status === "blocked"),
    )
    .sort((a, b) => {
      const activeDelta =
        Number(b.mission.status === "active") - Number(a.mission.status === "active");
      if (activeDelta !== 0) {
        return activeDelta;
      }
      return b.updatedAt - a.updatedAt;
    })[0]?.mission;
}

function resolveActiveDirective(flows: TaskFlowRecord[]): OpenClawDirectiveContract | undefined {
  return flows
    .map((flow) => ({
      directive: readDirectiveContract(flow.stateJson),
      flowStatus: flow.status,
      updatedAt: flow.updatedAt,
    }))
    .filter(
      (
        entry,
      ): entry is {
        directive: OpenClawDirectiveContract;
        flowStatus: TaskFlowRecord["status"];
        updatedAt: number;
      } => {
        const directive = entry.directive;
        if (!directive) {
          return false;
        }
        if (isTerminalTaskFlowStatus(entry.flowStatus)) {
          return false;
        }
        return !["completed", "succeeded", "failed", "cancelled", "stopped"].includes(
          directive.current_state,
        );
      },
    )
    .sort((a, b) => b.updatedAt - a.updatedAt)[0]?.directive;
}

function readDirectiveContract(
  value: JsonValue | undefined,
): OpenClawDirectiveContract | undefined {
  const directive = isRecord(value) ? value.openclawDirective : undefined;
  if (!isRecord(directive) || !validateOpenClawDirectiveContract(directive).valid) {
    return undefined;
  }
  return directive as unknown as OpenClawDirectiveContract;
}

function readMissionContract(value: JsonValue | undefined): OpenClawMissionContract | undefined {
  const mission = isRecord(value) ? value.openclawMission : undefined;
  if (!isRecord(mission) || !validateMissionContract(mission).valid) {
    return undefined;
  }
  return {
    mission_id: mission.mission_id as string,
    user_request: mission.user_request as string,
    selected_option: mission.selected_option as string,
    objective: mission.objective as string,
    priority: mission.priority as WorkPriority,
    allowed_lanes: stringArrayValue(mission.allowed_lanes) ?? [],
    hard_gates: stringArrayValue(mission.hard_gates) ?? [],
    minimum_work_floor: mission.minimum_work_floor as number,
    success_criteria: stringArrayValue(mission.success_criteria) ?? [],
    start_time: mission.start_time as string,
    wake_time: mission.wake_time as string,
    status: mission.status as OpenClawMissionContract["status"],
    proof_path: mission.proof_path as string,
  };
}

function resolveMissedWorkCount(flows: TaskFlowRecord[]): number {
  return flows.reduce((count, flow) => {
    const ledger = isRecord(flow.stateJson) ? flow.stateJson.openclawMissedWorkLedger : undefined;
    return count + (Array.isArray(ledger) ? ledger.length : 0);
  }, 0);
}

function resolveLastRecoveryAction(flows: TaskFlowRecord[]): string | undefined {
  return flows
    .map((flow) => ({
      action: stringValue(
        isRecord(flow.stateJson) ? flow.stateJson.openclawLastRecoveryAction : undefined,
      ),
      updatedAt: flow.updatedAt,
    }))
    .filter((entry): entry is { action: string; updatedAt: number } => Boolean(entry.action))
    .sort((a, b) => b.updatedAt - a.updatedAt)[0]?.action;
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
  const isStatusDigest = isStatusDigestText(text);
  const isWalletWatcher = isWalletWatcherText(text);
  const isRevenueDebrief = isRevenueDebriefText(text);
  const isNativeHealthCheck = isNativeHealthCheckText(text);
  const isSocialSteward = isSocialStewardText(text);
  const isReadOnlyRevenueSurface =
    isStatusDigest || isWalletWatcher || isRevenueDebrief || isNativeHealthCheck;
  const isPaymentForbiddenOnlySurface =
    hasForbiddenPaymentLanguage(text) && !hasPaymentExecutionDirective(text);
  const isReadOnlyPaymentSensor =
    isWalletWatcher ||
    isRevenueDebrief ||
    (!isNativeHealthCheck && /\b(read-only|no payment alert)\b/.test(text));

  if (isStatusDigest) {
    pool = "conversation";
    priority = "P1";
  } else if (isNativeHealthCheck) {
    pool = "maintenance";
    priority = "P5";
  }

  if (
    !isStatusDigest &&
    !isNativeHealthCheck &&
    /\b(payment|wallet|order|checkout|buyer|sale|sales|revenue|attribution|inbox|dm triage|warm lead)\b/.test(
      text,
    )
  ) {
    pool = "revenue";
    priority = /\b(payment|wallet|order|checkout broken|buyer signal|warm lead)\b/.test(text)
      ? "P0"
      : "P1";
  }
  if (
    !isNativeHealthCheck &&
    /\b(instagram|ig|titan-ig|shamil-ig|comment|social|post|reel|x reply|twitter)\b/.test(text)
  ) {
    pool = pool === "revenue" ? "revenue" : "social";
    priority = priority === "P0" || priority === "P1" ? priority : "P2";
  }
  if (
    !isNativeHealthCheck &&
    /\b(build|test|source|runtime|openclawv2|openclaw-runtime-src|repo|commit)\b/.test(text)
  ) {
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
  if (!isWalletWatcher && text.includes("openclawv2")) {
    resources.add("repo:openclawv2");
  }
  if (!isNativeHealthCheck && (text.includes("openclaw-runtime-src") || text.includes("runtime"))) {
    resources.add("repo:openclaw-runtime-src");
  }
  if (text.includes("project_state")) {
    resources.add("vault:PROJECT_STATE");
  }
  if (text.includes("ops board")) {
    resources.add("vault:OpsBoard");
  }
  if (!isReadOnlyRevenueSurface && !isPaymentForbiddenOnlySurface && text.includes("checkout")) {
    resources.add("checkout:titan");
  }
  if (
    !isReadOnlyRevenueSurface &&
    !isPaymentForbiddenOnlySurface &&
    (text.includes("payment") || text.includes("wallet"))
  ) {
    resources.add(isReadOnlyPaymentSensor ? "payment/wallet:read" : "payment/wallet");
  } else if (isReadOnlyPaymentSensor && (text.includes("payment") || text.includes("wallet"))) {
    resources.add("payment/wallet:read");
  }
  if (text.includes("heavy model") || text.includes("claude") || text.includes("gpt-5.5")) {
    resources.add("model:heavy");
  }

  return { pool, priority, resources: [...resources] };
}

function normalizeReadOnlySpecialCandidate(candidate: WorkManagerCandidate): WorkManagerCandidate {
  const text = [candidate.lane, candidate.expectedOutput, candidate.owner, candidate.workId]
    .filter(Boolean)
    .join(" ")
    .toLowerCase();
  if (
    !isStatusDigestText(text) &&
    !isWalletWatcherText(text) &&
    !isRevenueDebriefText(text) &&
    !isNativeHealthCheckText(text)
  ) {
    return candidate;
  }
  const inferred = inferWork(
    candidate.lane,
    candidate.expectedOutput,
    candidate.owner,
    candidate.workId,
  );
  const isStatusDigest = isStatusDigestText(text);
  const isRevenueDebrief = isRevenueDebriefText(text);
  const isNativeHealthCheck = isNativeHealthCheckText(text);
  return {
    ...candidate,
    pool: inferred.pool,
    priority: isStatusDigest
      ? "P1"
      : isRevenueDebrief
        ? "P2"
        : isNativeHealthCheck
          ? "P5"
          : candidate.priority,
    requestedResources: inferred.resources,
  };
}

function isStatusDigestText(text: string): boolean {
  return (
    /\b(status digest|shift status|status-only|operator status|work status)\b/.test(text) ||
    /\b(no change since last digest|≤60 words|no menus)\b/.test(text)
  );
}

function isWalletWatcherText(text: string): boolean {
  return (
    /\btitan wallet watcher\s*(?:—|-|:|\bevery\b)/.test(text) ||
    /\btitan_wallet_watcher\.py\b/.test(text) ||
    /\brun exactly one shell command\b[\s\S]*\btitan_wallet_watcher\.py\b/.test(text) ||
    /\bpolls titan receive addresses\b/.test(text)
  );
}

function isRevenueDebriefText(text: string): boolean {
  return /\b(daily revenue debrief|revenue debrief)\b/.test(text);
}

function isNativeHealthCheckText(text: string): boolean {
  return (
    /\b(openclaw native health check|native health check|health check)\b/.test(text) &&
    /\b(config validate|gateway status|tasks audit|cron list|sessions)\b/.test(text)
  );
}

function isSocialStewardText(text: string): boolean {
  return /\b(social steward|social_sales_growth|ig\/titan social steward)\b/.test(text);
}

function hasForbiddenPaymentLanguage(text: string): boolean {
  return (
    /\bforbidden:[\s\S]{0,600}\b(payment|refund|order|checkout|dns|account)\b/.test(text) ||
    /\bdo\s+not\s+[^.]{0,180}\b(payment|wallet|checkout|refund|order|buy|purchase|upgrade|annual|plus)\b/.test(
      text,
    ) ||
    /\bno\s+(?:browser,\s+source edits,\s+social\/account\/email\/)?payment\b/.test(text) ||
    /\bno\s+(?:extra\s+)?(?:purchases?|upgrades?|annual|plus)\b/.test(text) ||
    /\bno\s+(?:payment|refund|order|checkout|dns)\b/.test(text)
  );
}

function hasPaymentExecutionDirective(text: string): boolean {
  return (
    /\b(start paid service|pay now|submit payment|payment submission|checkout start)\b/.test(
      text,
    ) ||
    /\bsubscribe\s+to\b/.test(text) ||
    /\b(?:buy|purchase)\s+(?:the\s+)?(?:starter|subscription|plan|workspace|business starter|\$|[0-9])\b/.test(
      text,
    )
  );
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
