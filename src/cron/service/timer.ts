import { resolveFailoverReasonFromError } from "../../agents/failover-error.js";
import type { CronConfig, CronRetryOn } from "../../config/types.cron.js";
import type { HeartbeatRunResult } from "../../infra/heartbeat-wake.js";
import {
  isMissionRuntimeAutonomousCronJob,
  selectMissionRuntimeDispatchPlan,
} from "../../mission-runtime/mission-runtime.js";
import type {
  MissionRuntimeDispatchPlan,
  MissionRuntimeDispatchUnit,
} from "../../mission-runtime/mission-runtime.types.js";
import {
  getLatestReliabilityHealthSnapshot,
  recordReliabilityEvent,
} from "../../reliability/supervisor.js";
import { DEFAULT_AGENT_ID } from "../../routing/session-key.js";
import { normalizeOptionalLowercaseString } from "../../shared/string-coerce.js";
import {
  completeTaskRunByRunId,
  createRunningTaskRun,
  failTaskRunByRunId,
} from "../../tasks/detached-task-runtime.js";
import { findTaskByRunId, listTaskRecords } from "../../tasks/runtime-internal.js";
import { listTaskControlRecords } from "../../tasks/task-control-registry.js";
import type { JsonValue } from "../../tasks/task-flow-registry.types.js";
import {
  createManagedTaskFlow,
  findLatestTaskFlowForOwnerKey,
  finishFlow,
  listTaskFlowRecords,
  updateFlowRecordByIdExpectedRevision,
} from "../../tasks/task-flow-runtime-internal.js";
import type { TaskRecord } from "../../tasks/task-registry.types.js";
import {
  buildWorkManagerSnapshot,
  applyQueuedWorkDispatchProof,
  createCronWorkCandidate,
  evaluateWorkAdmission,
  selectMissionRevenueFloorCronJob,
  type WorkAdmissionDecision,
  type WorkManagerCandidate,
  type WorkResourceLock,
} from "../../work-manager/work-manager.js";
import { clearCronJobActive, isCronJobActive, markCronJobActive } from "../active-jobs.js";
import { resolveCronDeliveryPlan } from "../delivery-plan.js";
import { createCronExecutionId } from "../run-id.js";
import { sweepCronRunSessions } from "../session-reaper.js";
import type {
  CronDeliveryStatus,
  CronDeliveryTrace,
  CronJob,
  CronMessageChannel,
  CronRunOutcome,
  CronRunStatus,
  CronRunTelemetry,
} from "../types.js";
import {
  DEFAULT_ERROR_BACKOFF_SCHEDULE_MS,
  computeJobPreviousRunAtMs,
  computeJobNextRunAtMs,
  errorBackoffMs,
  hasScheduledNextRunAtMs,
  isJobEnabled,
  nextWakeAtMs,
  recomputeNextRunsForMaintenance,
  recordScheduleComputeError,
  resolveJobPayloadTextForMain,
} from "./jobs.js";
import { locked } from "./locked.js";
import type { CronEvent, CronServiceState } from "./state.js";
import { ensureLoaded, persist } from "./store.js";
import { DEFAULT_JOB_TIMEOUT_MS, resolveCronJobTimeoutMs } from "./timeout-policy.js";

export { DEFAULT_JOB_TIMEOUT_MS } from "./timeout-policy.js";

const MAX_TIMER_DELAY_MS = 60_000;

/**
 * Minimum gap between consecutive fires of the same cron job.  This is a
 * safety net that prevents spin-loops when `computeJobNextRunAtMs` returns
 * a value within the same second as the just-completed run.  The guard
 * is intentionally generous (2 s) so it never masks a legitimate schedule
 * but always breaks an infinite re-trigger cycle.  (See #17821)
 */
const MIN_REFIRE_GAP_MS = 2_000;

const DEFAULT_MISSED_JOB_STAGGER_MS = 5_000;
const DEFAULT_MAX_MISSED_JOBS_PER_RESTART = 5;
const DEFAULT_FAILURE_ALERT_AFTER = 2;
const DEFAULT_FAILURE_ALERT_COOLDOWN_MS = 60 * 60_000; // 1 hour
const RECURRING_TIMEOUT_QUARANTINE_AFTER = 3;
const RUNNING_MARKER_TIMEOUT_RECONCILE_GRACE_MS = 2_000;
const RUNNING_MARKER_RUN_ID_DRIFT_MS = 5_000;
const WORK_MANAGER_CONTROLLER_ID = "work-manager";
const MISSION_RUNTIME_CONTROLLER_ID = "mission-runtime";
const WORK_MANAGER_ADMISSION_RETRY_MS = 60_000;
const WORK_MANAGER_TIMEOUT_SPLIT_AFTER = 2;

type ResolvedFailureAlert = {
  after: number;
  cooldownMs: number;
  channel: CronMessageChannel;
  to?: string;
  mode?: "announce" | "webhook";
  accountId?: string;
  includeSkipped: boolean;
};

type TimedCronRunOutcome = CronRunOutcome &
  CronRunTelemetry & {
    jobId: string;
    taskRunId?: string;
    delivered?: boolean;
    deliveryAttempted?: boolean;
    startedAt: number;
    endedAt: number;
  };

type StartupCatchupCandidate = {
  jobId: string;
  job: CronJob;
};

type StartupCatchupPlan = {
  candidates: StartupCatchupCandidate[];
  deferredJobIds: string[];
};

export async function executeJobCoreWithTimeout(
  state: CronServiceState,
  job: CronJob,
): Promise<Awaited<ReturnType<typeof executeJobCore>>> {
  const jobTimeoutMs = resolveCronJobTimeoutMs(job);
  if (typeof jobTimeoutMs !== "number") {
    return await executeJobCore(state, job);
  }

  const runAbortController = new AbortController();
  let timeoutId: NodeJS.Timeout | undefined;
  let rejectTimeout: ((reason?: unknown) => void) | undefined;
  const timeoutPromise = new Promise<never>((_, reject) => {
    rejectTimeout = reject;
  });
  const startTimeout = () => {
    if (timeoutId) {
      return;
    }
    timeoutId = setTimeout(() => {
      runAbortController.abort(timeoutErrorMessage());
      rejectTimeout?.(new Error(timeoutErrorMessage()));
    }, jobTimeoutMs);
  };
  const deferTimeoutUntilExecutionStart =
    job.sessionTarget !== "main" && job.payload.kind === "agentTurn";
  if (!deferTimeoutUntilExecutionStart) {
    startTimeout();
  }
  try {
    return await Promise.race([
      executeJobCore(state, job, runAbortController.signal, {
        onExecutionStarted: deferTimeoutUntilExecutionStart ? startTimeout : undefined,
      }),
      timeoutPromise,
    ]);
  } finally {
    if (timeoutId) {
      clearTimeout(timeoutId);
    }
  }
}

function resolveRunConcurrency(state: CronServiceState): number {
  const raw =
    state.deps.cronConfig?.maxConcurrentRuns ??
    (state.deps.executionKernel?.missionRuntime?.enabled === true
      ? state.deps.executionKernel.missionRuntime.maxParallelDispatch
      : undefined);
  if (typeof raw !== "number" || !Number.isFinite(raw)) {
    return 1;
  }
  return Math.max(1, Math.floor(raw));
}
function timeoutErrorMessage(): string {
  return "cron: job execution timed out";
}

function isTimeoutRunError(error: unknown): boolean {
  return normalizeCronRunErrorText(error) === timeoutErrorMessage();
}

function isTimeoutLikeCronError(error: unknown): boolean {
  const normalized = normalizeCronRunErrorText(error).toLowerCase();
  return (
    normalized === timeoutErrorMessage() ||
    /timeout|timed out|interrupted by gateway restart|gateway restart|stale_running_marker|backing session missing|lost/.test(
      normalized,
    )
  );
}

function shouldQuarantineRecurringTimeoutLoop(
  job: CronJob,
  result: { status: CronRunStatus; error?: string },
) {
  return (
    result.status === "error" &&
    job.schedule.kind !== "at" &&
    isTimeoutRunError(result.error) &&
    (job.state.consecutiveErrors ?? 0) >= RECURRING_TIMEOUT_QUARANTINE_AFTER
  );
}

function shouldQuarantineStoredRecurringTimeoutLoop(job: CronJob) {
  return (
    isJobEnabled(job) &&
    job.schedule.kind !== "at" &&
    typeof job.state.runningAtMs !== "number" &&
    (job.state.lastStatus === "error" || job.state.lastRunStatus === "error") &&
    isTimeoutLikeCronError(job.state.lastError) &&
    (job.state.consecutiveErrors ?? 0) >= RECURRING_TIMEOUT_QUARANTINE_AFTER
  );
}

function quarantineRecurringTimeoutLoop(state: CronServiceState, job: CronJob) {
  job.enabled = false;
  job.state.nextRunAtMs = undefined;
  job.state.lastError = `cron: job auto-quarantined after ${job.state.consecutiveErrors ?? RECURRING_TIMEOUT_QUARANTINE_AFTER} consecutive timeouts`;
  recordReliabilityEvent({
    subsystem: "cron",
    code: "cron_timeout_loop_quarantined",
    severity: "warn",
    subject: job.id,
    message: job.state.lastError,
    recoverable: false,
    createdAt: Date.now(),
    metadata: {
      jobName: job.name,
      consecutiveErrors: job.state.consecutiveErrors,
    },
  });
  state.deps.log.warn(
    {
      jobId: job.id,
      jobName: job.name,
      consecutiveErrors: job.state.consecutiveErrors,
    },
    "cron: auto-quarantined recurring job after repeated timeouts",
  );
}

function quarantineStoredRecurringTimeoutLoops(state: CronServiceState): boolean {
  let changed = false;
  for (const job of state.store?.jobs ?? []) {
    if (!shouldQuarantineStoredRecurringTimeoutLoop(job)) {
      continue;
    }
    quarantineRecurringTimeoutLoop(state, job);
    changed = true;
  }
  return changed;
}

function resolveTimeoutExpiredRunningOutcome(
  job: CronJob,
  nowMs: number,
): TimedCronRunOutcome | undefined {
  if (!isJobEnabled(job)) {
    return undefined;
  }
  const runningAtMs = job.state.runningAtMs;
  if (typeof runningAtMs !== "number" || !Number.isFinite(runningAtMs)) {
    return undefined;
  }
  const timeoutMs = resolveCronJobTimeoutMs(job);
  if (typeof timeoutMs !== "number" || !Number.isFinite(timeoutMs)) {
    return undefined;
  }
  if (nowMs < runningAtMs + timeoutMs + RUNNING_MARKER_TIMEOUT_RECONCILE_GRACE_MS) {
    return undefined;
  }
  return {
    jobId: job.id,
    taskRunId: createCronExecutionId(job.id, runningAtMs),
    status: "error",
    error: timeoutErrorMessage(),
    startedAt: runningAtMs,
    endedAt: nowMs,
  };
}

function reconcileRunningMarkers(state: CronServiceState, nowMs: number): boolean {
  const outcomes = (state.store?.jobs ?? [])
    .map((job) => resolveRunningMarkerOutcome(state, job, nowMs))
    .filter((outcome): outcome is TimedCronRunOutcome => outcome !== undefined);
  for (const outcome of outcomes) {
    state.deps.log.warn(
      { jobId: outcome.jobId, startedAt: outcome.startedAt, endedAt: outcome.endedAt },
      "cron: reconciling stale running marker",
    );
    recordReliabilityEvent({
      subsystem: "cron",
      code: "cron_running_marker_reconciled",
      severity: outcome.status === "ok" ? "info" : "warn",
      subject: outcome.jobId,
      message:
        outcome.error ?? `cron: reconciled running marker with terminal outcome ${outcome.status}`,
      recoverable: false,
      createdAt: outcome.endedAt,
      metadata: {
        taskRunId: outcome.taskRunId,
        startedAt: outcome.startedAt,
        status: outcome.status,
      },
    });
    applyOutcomeToStoredJob(state, outcome);
  }
  return outcomes.length > 0;
}

function isActiveTaskStatus(status: TaskRecord["status"]): boolean {
  return status === "queued" || status === "running";
}

function resolveTerminalTaskOutcome(
  job: CronJob,
  task: TaskRecord,
  runningAtMs: number,
  nowMs: number,
  taskRunId = createCronExecutionId(job.id, runningAtMs),
): TimedCronRunOutcome | undefined {
  if (isActiveTaskStatus(task.status)) {
    return undefined;
  }
  const startedAt = task.startedAt ?? runningAtMs;
  const endedAt = task.endedAt ?? task.lastEventAt ?? nowMs;
  if (task.status === "succeeded") {
    return {
      jobId: job.id,
      taskRunId,
      status: "ok",
      summary: task.terminalSummary ?? task.progressSummary,
      startedAt,
      endedAt,
    };
  }
  if (task.status === "timed_out") {
    return {
      jobId: job.id,
      taskRunId,
      status: "error",
      error: timeoutErrorMessage(),
      summary: task.terminalSummary ?? task.progressSummary,
      startedAt,
      endedAt,
    };
  }
  return {
    jobId: job.id,
    taskRunId,
    status: "error",
    error:
      task.error ??
      (task.status === "cancelled"
        ? "cron: job cancelled"
        : `cron: task registry marked run ${task.status}`),
    summary: task.terminalSummary ?? task.progressSummary,
    startedAt,
    endedAt,
  };
}

function findCronTaskForRunningMarker(
  job: CronJob,
  runningAtMs: number,
):
  | {
      task: TaskRecord;
      taskRunId: string;
      driftMs: number;
    }
  | undefined {
  const exactRunId = createCronExecutionId(job.id, runningAtMs);
  const exactTask = findTaskByRunId(exactRunId);
  if (exactTask) {
    return { task: exactTask, taskRunId: exactRunId, driftMs: 0 };
  }

  const prefix = `cron:${job.id}:`;
  let best:
    | {
        task: TaskRecord;
        taskRunId: string;
        driftMs: number;
      }
    | undefined;

  for (const task of listTaskRecords()) {
    if (
      !task.runId ||
      task.runtime !== "cron" ||
      task.sourceId !== job.id ||
      !task.runId.startsWith(prefix)
    ) {
      continue;
    }
    const startedAtFromRunId = Number(task.runId.slice(prefix.length));
    if (!Number.isFinite(startedAtFromRunId)) {
      continue;
    }
    const driftMs = Math.abs(startedAtFromRunId - runningAtMs);
    if (driftMs > RUNNING_MARKER_RUN_ID_DRIFT_MS) {
      continue;
    }
    if (!best || driftMs < best.driftMs) {
      best = { task, taskRunId: task.runId, driftMs };
    }
  }

  return best;
}

function resolveRunningMarkerOutcome(
  state: CronServiceState,
  job: CronJob,
  nowMs: number,
): TimedCronRunOutcome | undefined {
  const runningAtMs = job.state.runningAtMs;
  if (typeof runningAtMs !== "number" || !Number.isFinite(runningAtMs)) {
    return undefined;
  }
  const runId = createCronExecutionId(job.id, runningAtMs);
  const matchedRun = findCronTaskForRunningMarker(job, runningAtMs);
  if (matchedRun) {
    const terminalOutcome = resolveTerminalTaskOutcome(
      job,
      matchedRun.task,
      runningAtMs,
      nowMs,
      matchedRun.taskRunId,
    );
    if (!terminalOutcome) {
      return undefined;
    }
    state.deps.log.warn(
      {
        jobId: job.id,
        runId: matchedRun.taskRunId,
        expectedRunId: runId,
        runIdDriftMs: matchedRun.driftMs,
        taskId: matchedRun.task.taskId,
        taskStatus: matchedRun.task.status,
      },
      "cron: reconciling running marker from task registry",
    );
    return terminalOutcome;
  }

  const timeoutOutcome = resolveTimeoutExpiredRunningOutcome(job, nowMs);
  if (timeoutOutcome) {
    return timeoutOutcome;
  }

  state.deps.log.debug(
    { jobId: job.id, runId, active: isCronJobActive(job.id), runningAtMs },
    "cron: running marker has no task registry match yet",
  );
  return undefined;
}

function isAbortError(err: unknown): boolean {
  if (!(err instanceof Error)) {
    return false;
  }
  return err.name === "AbortError" || err.message === timeoutErrorMessage();
}

export function normalizeCronRunErrorText(err: unknown): string {
  if (isAbortError(err)) {
    return timeoutErrorMessage();
  }
  if (typeof err === "string") {
    return err === `Error: ${timeoutErrorMessage()}` ? timeoutErrorMessage() : err;
  }
  return String(err);
}

function tryCreateCronTaskRun(params: {
  state: CronServiceState;
  job: CronJob;
  startedAt: number;
}): string | undefined {
  const runId = createCronExecutionId(params.job.id, params.startedAt);
  try {
    createRunningTaskRun({
      runtime: "cron",
      sourceId: params.job.id,
      ownerKey: "",
      scopeKind: "system",
      childSessionKey: params.job.sessionKey,
      agentId: params.job.agentId,
      runId,
      label: params.job.name,
      task: params.job.name || params.job.id,
      deliveryStatus: "not_applicable",
      notifyPolicy: "silent",
      startedAt: params.startedAt,
      lastEventAt: params.startedAt,
    });
    return runId;
  } catch (error) {
    params.state.deps.log.warn(
      { jobId: params.job.id, error },
      "cron: failed to create task ledger record",
    );
    return undefined;
  }
}

function tryFinishCronTaskRun(
  state: CronServiceState,
  result: Pick<TimedCronRunOutcome, "taskRunId" | "status" | "error" | "endedAt" | "summary">,
): void {
  if (!result.taskRunId) {
    return;
  }
  try {
    if (result.status === "ok" || result.status === "skipped") {
      completeTaskRunByRunId({
        runId: result.taskRunId,
        runtime: "cron",
        endedAt: result.endedAt,
        lastEventAt: result.endedAt,
        terminalSummary: result.summary ?? undefined,
      });
      return;
    }
    failTaskRunByRunId({
      runId: result.taskRunId,
      runtime: "cron",
      status:
        normalizeCronRunErrorText(result.error) === timeoutErrorMessage() ? "timed_out" : "failed",
      endedAt: result.endedAt,
      lastEventAt: result.endedAt,
      error: result.status === "error" ? normalizeCronRunErrorText(result.error) : undefined,
      terminalSummary: result.summary ?? undefined,
    });
  } catch (error) {
    state.deps.log.warn(
      { runId: result.taskRunId, jobStatus: result.status, error },
      "cron: failed to update task ledger record",
    );
  }
}
/** Default max retries for one-shot jobs on transient errors (#24355). */
const DEFAULT_MAX_TRANSIENT_RETRIES = 3;

const TRANSIENT_PATTERNS: Record<string, RegExp> = {
  rate_limit:
    /(rate[_ ]limit|too many requests|429|resource has been exhausted|cloudflare|tokens per day)/i,
  overloaded:
    /\b529\b|\boverloaded(?:_error)?\b|high demand|temporar(?:ily|y) overloaded|capacity exceeded/i,
  network: /(network|econnreset|econnrefused|fetch failed|socket)/i,
  timeout: /(timeout|etimedout)/i,
  server_error: /\b5\d{2}\b/,
};

function isTransientCronError(error: string | undefined, retryOn?: CronRetryOn[]): boolean {
  if (!error || typeof error !== "string") {
    return false;
  }
  const keys = retryOn?.length ? retryOn : (Object.keys(TRANSIENT_PATTERNS) as CronRetryOn[]);
  return keys.some((k) => TRANSIENT_PATTERNS[k]?.test(error));
}

function resolveCronNextRunWithLowerBound(params: {
  state: CronServiceState;
  job: CronJob;
  naturalNext: number | undefined;
  lowerBoundMs: number;
  context: "completion" | "error_backoff";
}): number | undefined {
  if (params.naturalNext === undefined) {
    params.state.deps.log.warn(
      {
        jobId: params.job.id,
        jobName: params.job.name,
        context: params.context,
      },
      "cron: next run unresolved; clearing schedule to avoid a refire loop",
    );
    return undefined;
  }
  return Math.max(params.naturalNext, params.lowerBoundMs);
}

function resolveRetryConfig(cronConfig?: CronConfig) {
  const retry = cronConfig?.retry;
  return {
    maxAttempts:
      typeof retry?.maxAttempts === "number" ? retry.maxAttempts : DEFAULT_MAX_TRANSIENT_RETRIES,
    backoffMs:
      Array.isArray(retry?.backoffMs) && retry.backoffMs.length > 0
        ? retry.backoffMs
        : DEFAULT_ERROR_BACKOFF_SCHEDULE_MS.slice(0, 3),
    retryOn: Array.isArray(retry?.retryOn) && retry.retryOn.length > 0 ? retry.retryOn : undefined,
  };
}

function resolveDeliveryState(params: { job: CronJob; delivered?: boolean }): {
  delivered?: boolean;
  status: CronDeliveryStatus;
} {
  if (!resolveCronDeliveryPlan(params.job).requested) {
    return { status: "not-requested" };
  }
  if (params.delivered === true) {
    return { delivered: true, status: "delivered" };
  }
  if (params.delivered === false) {
    return { delivered: false, status: "not-delivered" };
  }
  return { status: "unknown" };
}

function normalizeCronMessageChannel(input: unknown): CronMessageChannel | undefined {
  const channel = normalizeOptionalLowercaseString(input);
  return channel ? (channel as CronMessageChannel) : undefined;
}

function normalizeTo(input: unknown): string | undefined {
  if (typeof input !== "string") {
    return undefined;
  }
  const to = input.trim();
  return to ? to : undefined;
}

function clampPositiveInt(value: unknown, fallback: number): number {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    return fallback;
  }
  const floored = Math.floor(value);
  return floored >= 1 ? floored : fallback;
}

function clampNonNegativeInt(value: unknown, fallback: number): number {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    return fallback;
  }
  const floored = Math.floor(value);
  return floored >= 0 ? floored : fallback;
}

function resolveFailureAlert(state: CronServiceState, job: CronJob): ResolvedFailureAlert | null {
  const globalConfig = state.deps.cronConfig?.failureAlert;
  const jobConfig = job.failureAlert === false ? undefined : job.failureAlert;

  if (job.failureAlert === false) {
    return null;
  }
  if (!jobConfig && globalConfig?.enabled !== true) {
    return null;
  }

  const mode = jobConfig?.mode ?? globalConfig?.mode;
  const explicitTo = normalizeTo(jobConfig?.to);

  return {
    after: clampPositiveInt(jobConfig?.after ?? globalConfig?.after, DEFAULT_FAILURE_ALERT_AFTER),
    cooldownMs: clampNonNegativeInt(
      jobConfig?.cooldownMs ?? globalConfig?.cooldownMs,
      DEFAULT_FAILURE_ALERT_COOLDOWN_MS,
    ),
    channel:
      normalizeCronMessageChannel(jobConfig?.channel) ??
      normalizeCronMessageChannel(job.delivery?.channel) ??
      "last",
    to: mode === "webhook" ? explicitTo : (explicitTo ?? normalizeTo(job.delivery?.to)),
    mode,
    accountId: jobConfig?.accountId ?? globalConfig?.accountId,
    includeSkipped: jobConfig?.includeSkipped ?? globalConfig?.includeSkipped ?? false,
  };
}

function emitFailureAlert(
  state: CronServiceState,
  params: {
    job: CronJob;
    error?: string;
    consecutiveErrors: number;
    channel: CronMessageChannel;
    to?: string;
    mode?: "announce" | "webhook";
    accountId?: string;
    status: "error" | "skipped";
  },
) {
  const safeJobName = params.job.name || params.job.id;
  const truncatedError = (params.error?.trim() || "unknown reason").slice(0, 200);
  const statusVerb = params.status === "skipped" ? "skipped" : "failed";
  const detailLabel = params.status === "skipped" ? "Skip reason" : "Last error";
  const text = [
    `Cron job "${safeJobName}" ${statusVerb} ${params.consecutiveErrors} times`,
    `${detailLabel}: ${truncatedError}`,
  ].join("\n");

  if (state.deps.sendCronFailureAlert) {
    void state.deps
      .sendCronFailureAlert({
        job: params.job,
        text,
        channel: params.channel,
        to: params.to,
        mode: params.mode,
        accountId: params.accountId,
      })
      .catch((err) => {
        state.deps.log.warn(
          { jobId: params.job.id, err: String(err) },
          "cron: failure alert delivery failed",
        );
      });
    return;
  }

  state.deps.enqueueSystemEvent(text, { agentId: params.job.agentId });
  if (params.job.wakeMode === "now") {
    state.deps.requestHeartbeatNow({ reason: `cron:${params.job.id}:failure-alert` });
  }
}

function maybeEmitFailureAlert(
  state: CronServiceState,
  params: {
    job: CronJob;
    alertConfig: ResolvedFailureAlert | null;
    status: "error" | "skipped";
    error?: string;
    consecutiveCount: number;
  },
) {
  if (!params.alertConfig || params.consecutiveCount < params.alertConfig.after) {
    return;
  }
  const isBestEffort = params.job.delivery?.bestEffort === true;
  if (isBestEffort) {
    return;
  }
  const now = state.deps.nowMs();
  const lastAlert = params.job.state.lastFailureAlertAtMs;
  const inCooldown =
    typeof lastAlert === "number" && now - lastAlert < Math.max(0, params.alertConfig.cooldownMs);
  if (inCooldown) {
    return;
  }
  emitFailureAlert(state, {
    job: params.job,
    error: params.error,
    consecutiveErrors: params.consecutiveCount,
    channel: params.alertConfig.channel,
    to: params.alertConfig.to,
    mode: params.alertConfig.mode,
    accountId: params.alertConfig.accountId,
    status: params.status,
  });
  params.job.state.lastFailureAlertAtMs = now;
}

/**
 * Apply the result of a job execution to the job's state.
 * Handles consecutive error tracking, exponential backoff, one-shot disable,
 * and nextRunAtMs computation. Returns `true` if the job should be deleted.
 */
export function applyJobResult(
  state: CronServiceState,
  job: CronJob,
  result: {
    status: CronRunStatus;
    error?: string;
    delivered?: boolean;
    startedAt: number;
    endedAt: number;
  },
  opts?: {
    // Preserve recurring "every" anchors for manual force runs.
    preserveSchedule?: boolean;
  },
): boolean {
  const prevLastRunAtMs = job.state.lastRunAtMs;
  const computeNextWithPreservedLastRun = (nowMs: number) => {
    const saved = job.state.lastRunAtMs;
    job.state.lastRunAtMs = prevLastRunAtMs;
    try {
      return computeJobNextRunAtMs(job, nowMs);
    } finally {
      job.state.lastRunAtMs = saved;
    }
  };
  job.state.runningAtMs = undefined;
  job.state.lastRunAtMs = result.startedAt;
  job.state.lastRunStatus = result.status;
  job.state.lastStatus = result.status;
  job.state.lastDurationMs = Math.max(0, result.endedAt - result.startedAt);
  job.state.lastError = result.error;
  job.state.lastErrorReason =
    result.status === "error" && typeof result.error === "string"
      ? (resolveFailoverReasonFromError(result.error) ?? undefined)
      : undefined;
  const deliveryState = resolveDeliveryState({ job, delivered: result.delivered });
  job.state.lastDelivered = deliveryState.delivered;
  job.state.lastDeliveryStatus = deliveryState.status;
  job.state.lastDeliveryError =
    deliveryState.status === "not-delivered" && result.error ? result.error : undefined;
  job.updatedAtMs = result.endedAt;

  // Track consecutive errors for backoff / auto-disable; skipped runs use a
  // separate counter so opt-in skip alerts do not affect retry behavior.
  const alertConfig = resolveFailureAlert(state, job);
  if (result.status === "error") {
    job.state.consecutiveErrors = (job.state.consecutiveErrors ?? 0) + 1;
    job.state.consecutiveSkipped = 0;
    maybeEmitFailureAlert(state, {
      job,
      alertConfig,
      status: "error",
      error: result.error,
      consecutiveCount: job.state.consecutiveErrors,
    });
    if (shouldQuarantineRecurringTimeoutLoop(job, result)) {
      quarantineRecurringTimeoutLoop(state, job);
    }
  } else if (result.status === "skipped") {
    job.state.consecutiveErrors = 0;
    job.state.consecutiveSkipped = (job.state.consecutiveSkipped ?? 0) + 1;
    if (alertConfig?.includeSkipped) {
      maybeEmitFailureAlert(state, {
        job,
        alertConfig,
        status: "skipped",
        error: result.error,
        consecutiveCount: job.state.consecutiveSkipped,
      });
    } else {
      job.state.lastFailureAlertAtMs = undefined;
    }
  } else {
    job.state.consecutiveErrors = 0;
    job.state.consecutiveSkipped = 0;
    job.state.lastFailureAlertAtMs = undefined;
  }

  const shouldDelete =
    job.schedule.kind === "at" && job.deleteAfterRun === true && result.status === "ok";

  if (!shouldDelete) {
    if (job.schedule.kind === "at") {
      if (result.status === "ok" || result.status === "skipped") {
        // One-shot done or skipped: disable to prevent tight-loop (#11452).
        job.enabled = false;
        job.state.nextRunAtMs = undefined;
      } else if (result.status === "error") {
        const retryConfig = resolveRetryConfig(state.deps.cronConfig);
        const transient = isTransientCronError(result.error, retryConfig.retryOn);
        // consecutiveErrors is always set to ≥1 by the increment block above.
        const consecutive = job.state.consecutiveErrors;
        if (transient && consecutive <= retryConfig.maxAttempts) {
          // Schedule retry with backoff (#24355).
          const backoff = errorBackoffMs(consecutive, retryConfig.backoffMs);
          job.state.nextRunAtMs = result.endedAt + backoff;
          state.deps.log.info(
            {
              jobId: job.id,
              jobName: job.name,
              consecutiveErrors: consecutive,
              backoffMs: backoff,
              nextRunAtMs: job.state.nextRunAtMs,
            },
            "cron: scheduling one-shot retry after transient error",
          );
        } else {
          // Permanent error or max retries exhausted: disable.
          // Note: deleteAfterRun:true only triggers on ok (see shouldDelete above),
          // so exhausted-retry jobs are disabled but intentionally kept in the store
          // to preserve the error state for inspection.
          job.enabled = false;
          job.state.nextRunAtMs = undefined;
          state.deps.log.warn(
            {
              jobId: job.id,
              jobName: job.name,
              consecutiveErrors: consecutive,
              error: result.error,
              reason: transient ? "max retries exhausted" : "permanent error",
            },
            "cron: disabling one-shot job after error",
          );
        }
      }
    } else if (result.status === "error" && isJobEnabled(job)) {
      // Apply exponential backoff for errored jobs to prevent retry storms.
      const backoff = errorBackoffMs(job.state.consecutiveErrors ?? 1);
      let normalNext: number | undefined;
      try {
        normalNext =
          opts?.preserveSchedule && job.schedule.kind === "every"
            ? computeNextWithPreservedLastRun(result.endedAt)
            : computeJobNextRunAtMs(job, result.endedAt);
      } catch (err) {
        // If the schedule expression/timezone throws (croner edge cases),
        // record the schedule error (auto-disables after repeated failures)
        // and fall back to backoff-only schedule so the state update is not lost.
        recordScheduleComputeError({ state, job, err });
      }
      const backoffNext = result.endedAt + backoff;
      // Use whichever is later: the natural next run or the backoff delay.
      job.state.nextRunAtMs =
        job.schedule.kind === "cron"
          ? resolveCronNextRunWithLowerBound({
              state,
              job,
              naturalNext: normalNext,
              lowerBoundMs: backoffNext,
              context: "error_backoff",
            })
          : normalNext !== undefined
            ? Math.max(normalNext, backoffNext)
            : backoffNext;
      state.deps.log.info(
        {
          jobId: job.id,
          consecutiveErrors: job.state.consecutiveErrors,
          backoffMs: backoff,
          nextRunAtMs: job.state.nextRunAtMs,
        },
        "cron: applying error backoff",
      );
    } else if (isJobEnabled(job)) {
      let naturalNext: number | undefined;
      try {
        naturalNext =
          opts?.preserveSchedule && job.schedule.kind === "every"
            ? computeNextWithPreservedLastRun(result.endedAt)
            : computeJobNextRunAtMs(job, result.endedAt);
      } catch (err) {
        // If the schedule expression/timezone throws (croner edge cases),
        // record the schedule error (auto-disables after repeated failures)
        // so a persistent throw doesn't cause a MIN_REFIRE_GAP_MS hot loop.
        recordScheduleComputeError({ state, job, err });
      }
      if (job.schedule.kind === "cron") {
        // Safety net: ensure the next fire is at least MIN_REFIRE_GAP_MS
        // after the current run ended.  Prevents spin-loops when the
        // schedule computation lands in the same second due to
        // timezone/croner edge cases (see #17821).
        const minNext = result.endedAt + MIN_REFIRE_GAP_MS;
        job.state.nextRunAtMs = resolveCronNextRunWithLowerBound({
          state,
          job,
          naturalNext,
          lowerBoundMs: minNext,
          context: "completion",
        });
      } else {
        job.state.nextRunAtMs = naturalNext;
      }
    } else {
      job.state.nextRunAtMs = undefined;
    }
  }

  return shouldDelete;
}

function applyOutcomeToStoredJob(state: CronServiceState, result: TimedCronRunOutcome): void {
  clearCronJobActive(result.jobId);
  tryFinishCronTaskRun(state, result);
  const store = state.store;
  if (!store) {
    return;
  }
  const jobs = store.jobs;
  const job = jobs.find((entry) => entry.id === result.jobId);
  if (!job) {
    state.deps.log.warn(
      { jobId: result.jobId },
      "cron: applyOutcomeToStoredJob — job not found after forceReload, result discarded",
    );
    return;
  }

  const shouldDelete = applyJobResult(state, job, {
    status: result.status,
    error: result.error,
    delivered: result.delivered,
    startedAt: result.startedAt,
    endedAt: result.endedAt,
  });
  upsertTimeoutAutoSplitWorkFlow({ state, job, result });

  emitJobFinished(state, job, result, result.startedAt);

  if (shouldDelete) {
    store.jobs = jobs.filter((entry) => entry.id !== job.id);
    emit(state, { jobId: job.id, action: "removed" });
  }
}

export function armTimer(state: CronServiceState) {
  if (state.timer) {
    clearTimeout(state.timer);
  }
  state.timer = null;
  if (!state.deps.cronEnabled) {
    state.deps.log.debug({}, "cron: armTimer skipped - scheduler disabled");
    return;
  }
  const nextAt = nextWakeAtMs(state);
  if (!nextAt) {
    const jobCount = state.store?.jobs.length ?? 0;
    const enabledCount = state.store?.jobs.filter((j) => j.enabled).length ?? 0;
    const withNextRun =
      state.store?.jobs.filter((j) => j.enabled && hasScheduledNextRunAtMs(j.state.nextRunAtMs))
        .length ?? 0;
    if (enabledCount > 0) {
      armRunningRecheckTimer(state);
      state.deps.log.debug(
        { jobCount, enabledCount, withNextRun, delayMs: MAX_TIMER_DELAY_MS },
        "cron: timer armed for maintenance recheck",
      );
      return;
    }
    state.deps.log.debug(
      { jobCount, enabledCount, withNextRun },
      "cron: armTimer skipped - no jobs with nextRunAtMs",
    );
    return;
  }
  const now = state.deps.nowMs();
  const delay = Math.max(nextAt - now, 0);
  // Floor: when the next wake time is in the past (delay === 0), enforce a
  // minimum delay to prevent a tight setTimeout(0) loop.  This can happen
  // when a job has a stuck runningAtMs marker and a past-due nextRunAtMs:
  // findDueJobs skips the job (blocked by runningAtMs), while
  // recomputeNextRunsForMaintenance intentionally does not advance the
  // past-due nextRunAtMs (per #13992).  The finally block in onTimer then
  // re-invokes armTimer with delay === 0, creating an infinite hot-loop
  // that saturates the event loop and fills the log file to its size cap.
  const flooredDelay = delay === 0 ? MIN_REFIRE_GAP_MS : delay;
  // Wake at least once a minute to avoid schedule drift and recover quickly
  // when the process was paused or wall-clock time jumps.
  const clampedDelay = Math.min(flooredDelay, MAX_TIMER_DELAY_MS);
  // Intentionally avoid an `async` timer callback:
  // Vitest's fake-timer helpers can await async callbacks, which would block
  // tests that simulate long-running jobs. Runtime behavior is unchanged.
  state.timer = setTimeout(() => {
    void onTimer(state).catch((err) => {
      state.deps.log.error({ err: String(err) }, "cron: timer tick failed");
    });
  }, clampedDelay);
  state.deps.log.debug(
    { nextAt, delayMs: clampedDelay, clamped: delay > MAX_TIMER_DELAY_MS },
    "cron: timer armed",
  );
}

function armRunningRecheckTimer(state: CronServiceState) {
  if (state.timer) {
    clearTimeout(state.timer);
  }
  state.timer = setTimeout(() => {
    void onTimer(state).catch((err) => {
      state.deps.log.error({ err: String(err) }, "cron: timer tick failed");
    });
  }, MAX_TIMER_DELAY_MS);
}

export async function onTimer(state: CronServiceState) {
  if (state.running) {
    // Re-arm the timer so the scheduler keeps ticking even when a job is
    // still executing.  Without this, a long-running job (e.g. an agentTurn
    // exceeding MAX_TIMER_DELAY_MS) causes the clamped 60 s timer to fire
    // while `running` is true.  The early return then leaves no timer set,
    // silently killing the scheduler until the next gateway restart.
    //
    // We use MAX_TIMER_DELAY_MS as a fixed re-check interval to avoid a
    // zero-delay hot-loop when past-due jobs are waiting for the current
    // execution to finish.
    // See: https://github.com/openclaw/openclaw/issues/12025
    armRunningRecheckTimer(state);
    return;
  }
  state.running = true;
  // Keep a watchdog timer armed while a tick is executing. If execution hangs
  // (for example in a provider call), the scheduler still wakes to re-check.
  armRunningRecheckTimer(state);
  try {
    const dueJobs = await locked(state, async () => {
      await ensureLoaded(state, { forceReload: true, skipRecompute: true });
      const dueCheckNow = state.deps.nowMs();
      const reconciledRunningMarkers = reconcileRunningMarkers(state, dueCheckNow);
      const quarantinedStoredTimeoutLoops = quarantineStoredRecurringTimeoutLoops(state);
      const missionRuntimeTick = applyMissionRuntimeTick(state, dueCheckNow);
      const promotedMissionRevenueFloor = missionRuntimeTick.enabled
        ? false
        : promoteMissionRevenueFloorJob(state, dueCheckNow);
      const appliedQueuedDispatchProof = missionRuntimeTick.enabled
        ? false
        : applyAlwaysOnQueuedDispatchProof(state, dueCheckNow);
      const due = collectRunnableJobs(state, dueCheckNow, {
        skipJobIds: missionRuntimeTick.skipJobIds,
      });
      const admitted = filterRunnableJobsByWorkAdmission(state, due, dueCheckNow);
      const admissionDeferred = admitted.length !== due.length;

      if (admitted.length === 0) {
        // Use maintenance-only recompute to avoid advancing past-due nextRunAtMs
        // values without execution. This prevents jobs from being silently skipped
        // when the timer wakes up but findDueJobs returns empty (see #13992).
        const changed = recomputeNextRunsForMaintenance(state, {
          recomputeExpired: true,
          nowMs: dueCheckNow,
        });
        if (
          reconciledRunningMarkers ||
          quarantinedStoredTimeoutLoops ||
          missionRuntimeTick.changed ||
          promotedMissionRevenueFloor ||
          appliedQueuedDispatchProof ||
          changed ||
          admissionDeferred
        ) {
          await persist(state);
        }
        return [];
      }

      const now = state.deps.nowMs();
      for (const job of admitted) {
        job.state.runningAtMs = now;
        job.state.lastError = undefined;
      }
      await persist(state);

      return admitted.map((j) => ({
        id: j.id,
        job: j,
      }));
    });

    const runDueJob = async (params: {
      id: string;
      job: CronJob;
    }): Promise<TimedCronRunOutcome> => {
      const { id, job } = params;
      const startedAt = state.deps.nowMs();
      job.state.runningAtMs = startedAt;
      markCronJobActive(job.id);
      emit(state, { jobId: job.id, action: "started", runAtMs: startedAt });
      const jobTimeoutMs = resolveCronJobTimeoutMs(job);
      const taskRunId = tryCreateCronTaskRun({ state, job, startedAt });

      try {
        const result = await executeJobCoreWithTimeout(state, job);
        return {
          jobId: id,
          taskRunId,
          ...result,
          startedAt,
          endedAt: state.deps.nowMs(),
        };
      } catch (err) {
        const errorText = normalizeCronRunErrorText(err);
        state.deps.log.warn(
          { jobId: id, jobName: job.name, timeoutMs: jobTimeoutMs ?? null },
          `cron: job failed: ${errorText}`,
        );
        return {
          jobId: id,
          taskRunId,
          status: "error",
          error: errorText,
          startedAt,
          endedAt: state.deps.nowMs(),
        };
      }
    };

    const concurrency = Math.min(resolveRunConcurrency(state), Math.max(1, dueJobs.length));
    const results: (TimedCronRunOutcome | undefined)[] = Array.from({ length: dueJobs.length });
    let cursor = 0;
    const workers = Array.from({ length: concurrency }, async () => {
      for (;;) {
        const index = cursor++;
        if (index >= dueJobs.length) {
          return;
        }
        const due = dueJobs[index];
        if (!due) {
          return;
        }
        results[index] = await runDueJob(due);
      }
    });
    await Promise.all(workers);

    const completedResults: TimedCronRunOutcome[] = results.filter(
      (entry): entry is TimedCronRunOutcome => entry !== undefined,
    );

    if (completedResults.length > 0) {
      await locked(state, async () => {
        await ensureLoaded(state, { forceReload: true, skipRecompute: true });
        for (const result of completedResults) {
          applyOutcomeToStoredJob(state, result);
        }

        // Use maintenance-only recompute to avoid advancing past-due
        // nextRunAtMs values that became due between findDueJobs and this
        // locked block.  The full recomputeNextRuns would silently skip
        // those jobs (advancing nextRunAtMs without execution), causing
        // daily cron schedules to jump 48 h instead of 24 h (#17852).
        recomputeNextRunsForMaintenance(state);
        await persist(state);
      });
    }
  } finally {
    // Piggyback session reaper on timer tick (self-throttled to every 5 min).
    // Placed in `finally` so the reaper runs even when a long-running job keeps
    // `state.running` true across multiple timer ticks — the early return at the
    // top of onTimer would otherwise skip the reaper indefinitely.
    const storePaths = new Set<string>();
    if (state.deps.resolveSessionStorePath) {
      const defaultAgentId = state.deps.defaultAgentId ?? DEFAULT_AGENT_ID;
      if (state.store?.jobs?.length) {
        for (const job of state.store.jobs) {
          const agentId =
            typeof job.agentId === "string" && job.agentId.trim() ? job.agentId : defaultAgentId;
          storePaths.add(state.deps.resolveSessionStorePath(agentId));
        }
      } else {
        storePaths.add(state.deps.resolveSessionStorePath(defaultAgentId));
      }
    } else if (state.deps.sessionStorePath) {
      storePaths.add(state.deps.sessionStorePath);
    }

    if (storePaths.size > 0) {
      const nowMs = state.deps.nowMs();
      for (const storePath of storePaths) {
        try {
          await sweepCronRunSessions({
            cronConfig: state.deps.cronConfig,
            sessionStorePath: storePath,
            nowMs,
            log: state.deps.log,
          });
        } catch (err) {
          state.deps.log.warn({ err: String(err), storePath }, "cron: session reaper sweep failed");
        }
      }
    }

    state.running = false;
    armTimer(state);
  }
}

function isRunnableJob(params: {
  job: CronJob;
  nowMs: number;
  skipJobIds?: ReadonlySet<string>;
  skipAtIfAlreadyRan?: boolean;
  allowCronMissedRunByLastRun?: boolean;
}): boolean {
  const { job, nowMs } = params;
  if (!job.state) {
    job.state = {};
  }
  if (!isJobEnabled(job)) {
    return false;
  }
  if (params.skipJobIds?.has(job.id)) {
    return false;
  }
  if (typeof job.state.runningAtMs === "number") {
    return false;
  }
  if (params.skipAtIfAlreadyRan && job.schedule.kind === "at" && job.state.lastStatus) {
    // One-shot with terminal status: skip unless it's a transient-error retry.
    // Retries have nextRunAtMs > lastRunAtMs (scheduled after the failed run) (#24355).
    // ok/skipped or error-without-retry always skip (#13845).
    const lastRun = job.state.lastRunAtMs;
    const nextRun = job.state.nextRunAtMs;
    if (
      job.state.lastStatus === "error" &&
      isJobEnabled(job) &&
      typeof nextRun === "number" &&
      typeof lastRun === "number" &&
      nextRun > lastRun
    ) {
      return nowMs >= nextRun;
    }
    return false;
  }
  const next = job.state.nextRunAtMs;
  if (hasScheduledNextRunAtMs(next) && nowMs >= next) {
    return true;
  }
  if (hasScheduledNextRunAtMs(next) && next > nowMs && isErrorBackoffPending(job, nowMs)) {
    // Respect active retry backoff windows on restart, but allow missed-slot
    // replay once the backoff window has elapsed.
    return false;
  }
  if (!params.allowCronMissedRunByLastRun || job.schedule.kind !== "cron") {
    return false;
  }
  let previousRunAtMs: number | undefined;
  try {
    previousRunAtMs = computeJobPreviousRunAtMs(job, nowMs);
  } catch {
    return false;
  }
  if (typeof previousRunAtMs !== "number" || !Number.isFinite(previousRunAtMs)) {
    return false;
  }
  const lastRunAtMs = job.state.lastRunAtMs;
  if (typeof lastRunAtMs !== "number" || !Number.isFinite(lastRunAtMs)) {
    // Only replay a "missed slot" when there is concrete run history.
    return false;
  }
  return previousRunAtMs > lastRunAtMs;
}

function isErrorBackoffPending(job: CronJob, nowMs: number): boolean {
  if (job.schedule.kind === "at" || job.state.lastStatus !== "error") {
    return false;
  }
  const lastRunAtMs = job.state.lastRunAtMs;
  if (typeof lastRunAtMs !== "number" || !Number.isFinite(lastRunAtMs)) {
    return false;
  }
  const consecutiveErrorsRaw = job.state.consecutiveErrors;
  const consecutiveErrors =
    typeof consecutiveErrorsRaw === "number" && Number.isFinite(consecutiveErrorsRaw)
      ? Math.max(1, Math.floor(consecutiveErrorsRaw))
      : 1;
  return nowMs < lastRunAtMs + errorBackoffMs(consecutiveErrors);
}

function collectRunnableJobs(
  state: CronServiceState,
  nowMs: number,
  opts?: {
    skipJobIds?: ReadonlySet<string>;
    skipAtIfAlreadyRan?: boolean;
    allowCronMissedRunByLastRun?: boolean;
  },
): CronJob[] {
  if (!state.store) {
    return [];
  }
  return state.store.jobs.filter((job) =>
    isRunnableJob({
      job,
      nowMs,
      skipJobIds: opts?.skipJobIds,
      skipAtIfAlreadyRan: opts?.skipAtIfAlreadyRan,
      allowCronMissedRunByLastRun: opts?.allowCronMissedRunByLastRun,
    }),
  );
}

function applyMissionRuntimeTick(
  state: CronServiceState,
  nowMs: number,
): { enabled: boolean; changed: boolean; skipJobIds: ReadonlySet<string> } {
  if (!state.store || state.deps.executionKernel?.missionRuntime?.enabled !== true) {
    return { enabled: false, changed: false, skipJobIds: new Set<string>() };
  }

  const mode = state.deps.executionKernel.missionRuntime.mode ?? "shadow";
  const standingCompanyDirective =
    state.deps.executionKernel.missionRuntime.standingCompanyDirective !== false;
  const suppressLaneAutonomy =
    state.deps.executionKernel.missionRuntime.suppressLaneAutonomy !== false;
  const maxParallelDispatch = state.deps.executionKernel.missionRuntime.maxParallelDispatch;
  const snapshot = buildWorkManagerSnapshot({
    nowMs,
    mode: "admission",
    tasks: listTaskRecords(),
    taskControls: listTaskControlRecords({ activeOnly: true }),
    taskFlows: listTaskFlowRecords(),
    cronJobs: state.store.jobs,
    reliability: getLatestReliabilityHealthSnapshot(),
  });
  const queuedCandidates =
    snapshot.queueDrain.decision === "dispatch" ? [snapshot.queueDrain.candidate] : [];
  const plan = selectMissionRuntimeDispatchPlan({
    nowMs,
    snapshot,
    cronJobs: state.store.jobs,
    queuedCandidates,
    standingCompanyDirective,
    maxParallelDispatch,
    novelAgentEnabled: state.deps.executionKernel.missionRuntime.novelAgentEnabled,
    selfImprovementEnabled: state.deps.executionKernel.missionRuntime.selfImprovementEnabled,
    personalAssistantEnabled: state.deps.executionKernel.missionRuntime.personalAssistantEnabled,
    agentModel: state.deps.executionKernel.missionRuntime.agentModel,
  });

  const skipJobIds =
    mode === "active" && suppressLaneAutonomy
      ? new Set(plan.suppressedCronJobIds)
      : new Set<string>();
  let changed = upsertMissionRuntimeDecisionFlow({ plan, nowMs, mode });
  if (mode !== "active") {
    return { enabled: true, changed, skipJobIds: new Set<string>() };
  }

  if (suppressLaneAutonomy) {
    changed =
      suppressAutonomousLaneCronJobs({
        state,
        nowMs,
        skipJobIds,
        selectedJobIds: new Set(
          plan.decision === "dispatch"
            ? plan.units
                .filter((unit) => unit.action === "promote_cron")
                .map((unit) => unit.cronJobId)
            : [],
        ),
      }) || changed;
  }

  if (plan.decision === "dispatch") {
    for (const unit of plan.units) {
      changed =
        applyMissionRuntimeDispatchUnit({
          state,
          unit,
          missionId: plan.mission.mission_id,
          nowMs,
          suppressedCronJobIds: plan.suppressedCronJobIds,
        }) || changed;
    }
  }

  return { enabled: true, changed, skipJobIds };
}

function suppressAutonomousLaneCronJobs(params: {
  state: CronServiceState;
  nowMs: number;
  skipJobIds: ReadonlySet<string>;
  selectedJobIds?: ReadonlySet<string>;
}): boolean {
  if (!params.state.store || params.skipJobIds.size === 0) {
    return false;
  }
  let changed = false;
  for (const job of params.state.store.jobs) {
    if (!params.skipJobIds.has(job.id) || params.selectedJobIds?.has(job.id)) {
      continue;
    }
    if (!isMissionRuntimeAutonomousCronJob(job)) {
      continue;
    }
    const nextRunAtMs = params.nowMs + WORK_MANAGER_ADMISSION_RETRY_MS;
    if (job.state.nextRunAtMs !== nextRunAtMs) {
      job.state.nextRunAtMs = nextRunAtMs;
      job.updatedAtMs = params.nowMs;
      changed = true;
    }
    upsertMissionRuntimeSuppressedFlow({ job, nowMs: params.nowMs, nextRunAtMs });
  }
  return changed;
}

function promoteMissionRevenueFloorJob(state: CronServiceState, nowMs: number): boolean {
  if (!state.store) {
    return false;
  }
  const snapshot = buildWorkManagerSnapshot({
    nowMs,
    mode: "overnight",
    tasks: listTaskRecords(),
    taskFlows: listTaskFlowRecords(),
    cronJobs: state.store.jobs,
    reliability: getLatestReliabilityHealthSnapshot(),
  });
  const decision = selectMissionRevenueFloorCronJob({
    snapshot,
    cronJobs: state.store.jobs,
    nowMs,
  });
  if (decision.decision !== "promote") {
    return false;
  }
  const job = state.store.jobs.find((entry) => entry.id === decision.jobId);
  if (!job || !isJobEnabled(job) || typeof job.state.runningAtMs === "number") {
    return false;
  }
  job.state.nextRunAtMs = nowMs;
  job.updatedAtMs = nowMs;
  upsertMissionRevenueFloorWorkFlow({
    job,
    candidate: decision.candidate,
    missionId: snapshot.activeMission?.mission_id ?? "unknown",
    nowMs,
  });
  state.deps.log.info(
    {
      jobId: job.id,
      missionId: snapshot.activeMission?.mission_id,
    },
    "cron: mission revenue floor promoted existing native cron job",
  );
  recordReliabilityEvent({
    subsystem: "cron",
    code: "mission_revenue_floor_promoted",
    severity: "info",
    subject: job.id,
    message: `cron: mission revenue floor promoted ${job.name}`,
    recoverable: true,
    quiet: true,
    createdAt: nowMs,
    metadata: {
      missionId: snapshot.activeMission?.mission_id,
      jobId: job.id,
    },
  });
  return true;
}

function applyAlwaysOnQueuedDispatchProof(state: CronServiceState, nowMs: number): boolean {
  if (!state.store) {
    return false;
  }
  const snapshot = buildWorkManagerSnapshot({
    nowMs,
    mode: "admission",
    tasks: listTaskRecords(),
    taskControls: listTaskControlRecords({ activeOnly: true }),
    taskFlows: listTaskFlowRecords(),
    cronJobs: state.store.jobs,
    reliability: getLatestReliabilityHealthSnapshot(),
  });
  const decision = snapshot.queueDrain;
  if (decision.decision !== "dispatch") {
    return false;
  }

  const parsedCronRun = parseQueueDrainCronWorkId(decision.candidate.workId);
  if (parsedCronRun) {
    const job = state.store.jobs.find((entry) => entry.id === parsedCronRun.jobId);
    if (job && isJobEnabled(job) && typeof job.state.runningAtMs !== "number") {
      job.state.nextRunAtMs = nowMs;
      job.updatedAtMs = nowMs;
      upsertQueueDrainDispatchFlow({
        candidate: decision.candidate,
        dispatchEffect: "cron_promoted",
        cronJobId: job.id,
        nowMs,
        reason: "always_on_directive_drain",
      });
      state.deps.log.info(
        { jobId: job.id, workId: decision.candidate.workId },
        "cron: always-on directive drain promoted native cron job",
      );
      recordReliabilityEvent({
        subsystem: "cron",
        code: "queue_drain_cron_promoted",
        severity: "info",
        subject: job.id,
        message: `cron: always-on directive drain promoted ${job.name}`,
        recoverable: true,
        quiet: true,
        createdAt: nowMs,
        metadata: {
          jobId: job.id,
          workId: decision.candidate.workId,
        },
      });
      return true;
    }
  }

  const flow = listTaskFlowRecords()
    .filter((record) => ["queued", "blocked", "waiting"].includes(record.status))
    .find((record) => {
      const metadata = readObject(record.stateJson, "openclawWorkManager");
      return metadata.workId === decision.candidate.workId;
    });
  if (!flow) {
    return false;
  }
  const dispatchProof = applyQueuedWorkDispatchProof(decision, {
    dispatchEffect: "taskflow_started",
    flowId: flow.flowId,
    owner: decision.candidate.owner,
    startedAt: nowMs,
    firstStatusCheck: nowMs + 60_000,
  });
  const stateJson = mergeStateJson(flow.stateJson, {
    openclawWorkManager: {
      ...readObject(flow.stateJson, "openclawWorkManager"),
      workStatus: "running",
      dispatchedAt: nowMs,
      leaseUntil: nowMs + DEFAULT_JOB_TIMEOUT_MS,
    },
    openclawQueueDrain: {
      ...dispatchProof,
      reason: "always_on_directive_drain",
      dispatchedAt: nowMs,
    },
  });
  updateFlowRecordByIdExpectedRevision({
    flowId: flow.flowId,
    expectedRevision: flow.revision,
    patch: {
      status: "running",
      currentStep: "dispatched_by_work_manager",
      stateJson,
      updatedAt: nowMs,
      endedAt: null,
    },
  });
  state.deps.log.info(
    { flowId: flow.flowId, workId: decision.candidate.workId },
    "cron: always-on directive drain started native TaskFlow work",
  );
  recordReliabilityEvent({
    subsystem: "cron",
    code: "queue_drain_taskflow_started",
    severity: "info",
    subject: flow.flowId,
    message: `cron: always-on directive drain started TaskFlow ${flow.flowId}`,
    recoverable: true,
    quiet: true,
    createdAt: nowMs,
    metadata: {
      flowId: flow.flowId,
      workId: decision.candidate.workId,
    },
  });
  return true;
}

function parseQueueDrainCronWorkId(workId: string): { jobId: string; startedAtMs: number } | null {
  if (!workId.startsWith("cron:")) {
    return null;
  }
  const lastColon = workId.lastIndexOf(":");
  if (lastColon <= "cron:".length) {
    return null;
  }
  const startedAtMs = Number(workId.slice(lastColon + 1));
  if (!Number.isFinite(startedAtMs)) {
    return null;
  }
  return { jobId: workId.slice("cron:".length, lastColon), startedAtMs };
}

function upsertQueueDrainDispatchFlow(params: {
  candidate: WorkManagerCandidate;
  dispatchEffect: "cron_promoted";
  cronJobId: string;
  nowMs: number;
  reason: string;
}) {
  const ownerKey = `work-manager:queue-drain:${params.candidate.workId}`;
  const dispatchProof = applyQueuedWorkDispatchProof(
    { decision: "dispatch", reason: "available", candidate: params.candidate },
    {
      dispatchEffect: params.dispatchEffect,
      cronJobId: params.cronJobId,
      owner: params.candidate.owner,
      startedAt: params.nowMs,
      firstStatusCheck: params.nowMs + 60_000,
    },
  );
  createManagedTaskFlow({
    controllerId: WORK_MANAGER_CONTROLLER_ID,
    ownerKey,
    notifyPolicy: "silent",
    status: "succeeded",
    goal: `Queue drain dispatched ${params.candidate.lane}`,
    currentStep: "queue_drain_dispatched",
    stateJson: {
      openclawWorkManager: {
        version: 1,
        workId: params.candidate.workId,
        lane: params.candidate.lane,
        pool: params.candidate.pool,
        priority: params.candidate.priority,
        requestedResources: params.candidate.requestedResources,
        expectedOutput: params.candidate.expectedOutput,
        proofPath: params.candidate.proofPath,
        timeoutMs: params.candidate.timeoutMs,
        owner: params.candidate.owner,
        workStatus: "succeeded",
        createdAt: params.nowMs,
        leaseUntil: params.nowMs,
        handoffDepth: params.candidate.handoffDepth ?? 0,
      },
      openclawQueueDrain: {
        ...dispatchProof,
        reason: params.reason,
        dispatchedAt: params.nowMs,
      },
      openclawLastRecoveryAction: `queue_drain:${params.dispatchEffect}:${params.cronJobId}`,
    },
    createdAt: params.nowMs,
    updatedAt: params.nowMs,
    endedAt: params.nowMs,
  });
}

function filterRunnableJobsByWorkAdmission(
  state: CronServiceState,
  due: CronJob[],
  nowMs: number,
): CronJob[] {
  if (!state.store || due.length === 0) {
    return due;
  }

  const snapshot = buildWorkManagerSnapshot({
    nowMs,
    mode: "admission",
    tasks: listTaskRecords(),
    taskControls: listTaskControlRecords({ activeOnly: true }),
    taskFlows: listTaskFlowRecords(),
    cronJobs: state.store.jobs,
    reliability: getLatestReliabilityHealthSnapshot(),
  });
  const admitted: CronJob[] = [];
  const reservedLocks: WorkResourceLock[] = [];

  for (const job of due) {
    const candidate = createCronWorkCandidate({ job, nowMs, status: "queued" });
    const decision = evaluateWorkAdmission(
      {
        ...snapshot,
        locks: [...snapshot.locks, ...reservedLocks],
      },
      candidate,
    );

    if (decision.decision === "allow") {
      finishAdmissionDeferredWorkFlow(job, candidate, nowMs);
      admitted.push(job);
      for (const resource of candidate.requestedResources) {
        reservedLocks.push({
          lockId: `${resource}:${candidate.workId}`,
          ownerWorkId: candidate.workId,
          resource,
          pool: candidate.pool,
          priority: candidate.priority,
          acquiredAt: nowMs,
          leaseUntil: nowMs + 30 * 60_000,
          enforced: true,
        });
      }
      continue;
    }

    job.state.nextRunAtMs = nowMs + WORK_MANAGER_ADMISSION_RETRY_MS;
    upsertAdmissionDeferredWorkFlow({
      job,
      candidate,
      decision,
      nowMs,
      nextRunAtMs: job.state.nextRunAtMs,
    });
    state.deps.log.info(
      {
        jobId: job.id,
        reason: decision.reason,
        blockedResources: decision.blockedResources,
        deferredUntilMs: job.state.nextRunAtMs,
      },
      "cron: work manager admission deferred job",
    );
    recordReliabilityEvent({
      subsystem: "cron",
      code: "work_admission_deferred",
      severity: "info",
      subject: job.id,
      message: `cron: work manager deferred ${job.name} (${decision.reason})`,
      recoverable: false,
      quiet: true,
      createdAt: nowMs,
      metadata: {
        reason: decision.reason,
        blockedResources: decision.blockedResources,
      },
    });
  }

  return admitted;
}

function missionRevenueFloorOwnerKey(params: { missionId: string; jobId: string }): string {
  return `work-manager:mission-floor:${params.missionId}:${params.jobId}`;
}

function upsertMissionRevenueFloorWorkFlow(params: {
  job: CronJob;
  candidate: WorkManagerCandidate;
  missionId: string;
  nowMs: number;
}) {
  const ownerKey = missionRevenueFloorOwnerKey({
    missionId: params.missionId,
    jobId: params.job.id,
  });
  const dispatchProof = applyQueuedWorkDispatchProof(
    { decision: "dispatch", reason: "available", candidate: params.candidate },
    {
      dispatchEffect: "cron_promoted",
      cronJobId: params.job.id,
      owner: params.candidate.owner,
      startedAt: params.nowMs,
      firstStatusCheck: params.nowMs + 60_000,
    },
  );
  const stateJson = {
    openclawWorkManager: {
      version: 1,
      workId: params.candidate.workId,
      lane: params.candidate.lane,
      pool: params.candidate.pool,
      priority: params.candidate.priority,
      requestedResources: params.candidate.requestedResources,
      expectedOutput: params.candidate.expectedOutput,
      proofPath: params.candidate.proofPath,
      timeoutMs: params.candidate.timeoutMs,
      owner: params.candidate.owner,
      workStatus: "succeeded",
      createdAt: params.nowMs,
      leaseUntil: params.nowMs,
      handoffDepth: params.candidate.handoffDepth ?? 0,
    },
    openclawQueueDrain: {
      ...dispatchProof,
      reason: "mission_revenue_floor",
      dispatchedAt: params.nowMs,
    },
    openclawLastRecoveryAction: `promoted:${params.job.id}`,
  } satisfies Record<string, JsonValue>;
  const existing = findLatestTaskFlowForOwnerKey(ownerKey);
  if (existing && ["queued", "running", "waiting", "blocked"].includes(existing.status)) {
    finishFlow({
      flowId: existing.flowId,
      expectedRevision: existing.revision,
      currentStep: "mission_revenue_floor_promoted",
      stateJson,
      updatedAt: params.nowMs,
      endedAt: params.nowMs,
    });
    return;
  }
  createManagedTaskFlow({
    controllerId: WORK_MANAGER_CONTROLLER_ID,
    ownerKey,
    notifyPolicy: "silent",
    status: "succeeded",
    goal: `Mission revenue floor promoted ${params.job.name}`,
    currentStep: "mission_revenue_floor_promoted",
    stateJson,
    createdAt: params.nowMs,
    updatedAt: params.nowMs,
    endedAt: params.nowMs,
  });
}

function upsertMissionRuntimeDecisionFlow(params: {
  plan: MissionRuntimeDispatchPlan;
  nowMs: number;
  mode: "shadow" | "active";
}): boolean {
  const ownerKey = "mission-runtime:decision:current";
  const runtimeState: Record<string, JsonValue> = {
    version: 1,
    mode: params.mode,
    decision: params.plan.decision,
    reason: params.plan.reason,
    suppressedCronJobIds: params.plan.suppressedCronJobIds,
    exactGates: params.plan.exactGates,
    nextCheckAt: params.plan.nextCheckAt,
    parallelLimit: params.plan.parallelLimit,
    decidedAt: params.nowMs,
  };
  if (params.plan.decision === "dispatch") {
    runtimeState.units = params.plan.units.map((unit) => ({
      action: unit.action,
      unitKind: unit.unitKind,
      workId:
        unit.action === "promote_cron"
          ? unit.candidate.workId
          : unit.action === "start_taskflow"
            ? unit.workId
            : unit.workId,
      ...(unit.action === "promote_cron" ? { cronJobId: unit.cronJobId } : {}),
      proofPath: unit.proofPath,
      expectedOutput: unit.expectedOutput,
      pool: unit.pool,
      priority: unit.priority,
    })) as JsonValue;
  }
  const stateJson: Record<string, JsonValue> = {
    openclawMissionRuntime: runtimeState,
  };
  if (params.plan.mission) {
    stateJson.openclawMission = params.plan.mission as unknown as JsonValue;
  }
  const existing = findLatestTaskFlowForOwnerKey(ownerKey);
  if (existing && ["queued", "running", "waiting", "blocked"].includes(existing.status)) {
    updateFlowRecordByIdExpectedRevision({
      flowId: existing.flowId,
      expectedRevision: existing.revision,
      patch: {
        status: "running",
        currentStep: "mission_runtime_decided",
        stateJson,
        updatedAt: params.nowMs,
        endedAt: null,
      },
    });
    return true;
  }
  createManagedTaskFlow({
    controllerId: MISSION_RUNTIME_CONTROLLER_ID,
    ownerKey,
    notifyPolicy: "silent",
    status: "running",
    goal: "Mission Runtime owns the next safe company unit",
    currentStep: "mission_runtime_decided",
    stateJson,
    createdAt: params.nowMs,
    updatedAt: params.nowMs,
    endedAt: null,
  });
  return true;
}

function applyMissionRuntimeDispatchUnit(params: {
  state: CronServiceState;
  unit: MissionRuntimeDispatchUnit;
  missionId: string;
  nowMs: number;
  suppressedCronJobIds: string[];
}): boolean {
  const unit = params.unit;
  if (unit.action === "promote_cron") {
    const job = params.state.store?.jobs.find((entry) => entry.id === unit.cronJobId);
    if (!job || !isJobEnabled(job) || typeof job.state.runningAtMs === "number") {
      return false;
    }
    job.state.nextRunAtMs = params.nowMs;
    job.updatedAtMs = params.nowMs;
    upsertMissionRuntimeDispatchFlow({
      job,
      candidate: unit.candidate,
      missionId: params.missionId,
      nowMs: params.nowMs,
    });
    params.state.deps.log.info(
      { jobId: job.id, missionId: params.missionId },
      "cron: mission runtime promoted selected native capability",
    );
    recordReliabilityEvent({
      subsystem: "cron",
      code: "mission_runtime_cron_promoted",
      severity: "info",
      subject: job.id,
      message: `cron: mission runtime promoted ${job.name}`,
      recoverable: true,
      quiet: true,
      createdAt: params.nowMs,
      metadata: {
        jobId: job.id,
        missionId: params.missionId,
        suppressedCronJobIds: params.suppressedCronJobIds,
      },
    });
    return true;
  }
  if (unit.action === "start_taskflow") {
    return startMissionRuntimeTaskFlow({
      candidate: unit.candidate,
      missionId: params.missionId,
      nowMs: params.nowMs,
    });
  }
  return upsertMissionRuntimeAgentCronJob({
    state: params.state,
    unit,
    missionId: params.missionId,
    nowMs: params.nowMs,
  });
}

function upsertMissionRuntimeDispatchFlow(params: {
  job: CronJob;
  candidate: WorkManagerCandidate;
  missionId: string;
  nowMs: number;
}) {
  const ownerKey = `mission-runtime:dispatch:${params.missionId}:${params.job.id}`;
  const dispatchProof = applyQueuedWorkDispatchProof(
    { decision: "dispatch", reason: "available", candidate: params.candidate },
    {
      dispatchEffect: "cron_promoted",
      cronJobId: params.job.id,
      owner: params.candidate.owner,
      startedAt: params.nowMs,
      firstStatusCheck: params.nowMs + 60_000,
    },
  );
  const stateJson = {
    openclawWorkManager: {
      version: 1,
      workId: params.candidate.workId,
      lane: params.candidate.lane,
      pool: params.candidate.pool,
      priority: params.candidate.priority,
      requestedResources: params.candidate.requestedResources,
      expectedOutput: params.candidate.expectedOutput,
      proofPath: params.candidate.proofPath,
      timeoutMs: params.candidate.timeoutMs,
      owner: params.candidate.owner,
      workStatus: "succeeded",
      createdAt: params.nowMs,
      leaseUntil: params.nowMs,
      handoffDepth: params.candidate.handoffDepth ?? 0,
    },
    openclawQueueDrain: {
      ...dispatchProof,
      reason: "mission_runtime_selected",
      dispatchedAt: params.nowMs,
    },
    openclawMissionRuntime: {
      version: 1,
      missionId: params.missionId,
      decision: "promote_cron",
      selectedCronJobId: params.job.id,
      proofPath: params.candidate.proofPath,
      expectedOutput: params.candidate.expectedOutput,
    },
    openclawLastRecoveryAction: `mission_runtime:promoted:${params.job.id}`,
  } satisfies Record<string, JsonValue>;
  const existing = findLatestTaskFlowForOwnerKey(ownerKey);
  if (existing && ["queued", "running", "waiting", "blocked"].includes(existing.status)) {
    finishFlow({
      flowId: existing.flowId,
      expectedRevision: existing.revision,
      currentStep: "mission_runtime_promoted",
      stateJson,
      updatedAt: params.nowMs,
      endedAt: params.nowMs,
    });
    return;
  }
  createManagedTaskFlow({
    controllerId: MISSION_RUNTIME_CONTROLLER_ID,
    ownerKey,
    notifyPolicy: "silent",
    status: "succeeded",
    goal: `Mission Runtime promoted ${params.job.name}`,
    currentStep: "mission_runtime_promoted",
    stateJson,
    createdAt: params.nowMs,
    updatedAt: params.nowMs,
    endedAt: params.nowMs,
  });
}

function upsertMissionRuntimeAgentCronJob(params: {
  state: CronServiceState;
  unit: Extract<MissionRuntimeDispatchUnit, { action: "spawn_agent" }>;
  missionId: string;
  nowMs: number;
}): boolean {
  if (!params.state.store) {
    return false;
  }
  const jobId = missionAgentCronJobId(params.unit.workId);
  const existing = params.state.store.jobs.find((job) => job.id === jobId);
  if (existing && typeof existing.state.runningAtMs === "number") {
    return false;
  }
  const job: CronJob =
    existing ??
    ({
      id: jobId,
      agentId: params.state.deps.defaultAgentId ?? DEFAULT_AGENT_ID,
      name: `Mission Agent — ${params.unit.unitKind.replaceAll("_", " ")}`,
      description: `Mission Runtime spawned native agent work for ${params.missionId}`,
      enabled: true,
      deleteAfterRun: false,
      createdAtMs: params.nowMs,
      updatedAtMs: params.nowMs,
      schedule: { kind: "at", at: new Date(params.nowMs).toISOString() },
      sessionTarget: "isolated",
      wakeMode: "now",
      payload: {
        kind: "agentTurn",
        message: params.unit.agentPacket.prompt,
        model: params.unit.agentPacket.model,
        timeoutSeconds: Math.ceil(params.unit.agentPacket.timeoutMs / 1000),
      },
      delivery: { mode: "none" },
      state: {},
    } satisfies CronJob);
  job.enabled = true;
  job.updatedAtMs = params.nowMs;
  job.schedule = { kind: "at", at: new Date(params.nowMs).toISOString() };
  job.sessionTarget = "isolated";
  job.wakeMode = "now";
  job.payload = {
    kind: "agentTurn",
    message: params.unit.agentPacket.prompt,
    model: params.unit.agentPacket.model,
    timeoutSeconds: Math.ceil(params.unit.agentPacket.timeoutMs / 1000),
  };
  job.delivery = { mode: "none" };
  job.state.nextRunAtMs = params.nowMs;
  job.state.lastError = undefined;
  if (!existing) {
    params.state.store.jobs.push(job);
  }
  upsertMissionRuntimeAgentDispatchFlow({
    job,
    unit: params.unit,
    missionId: params.missionId,
    nowMs: params.nowMs,
  });
  params.state.deps.log.info(
    {
      jobId,
      missionId: params.missionId,
      unitKind: params.unit.unitKind,
      workId: params.unit.workId,
    },
    "cron: mission runtime spawned native agent work",
  );
  recordReliabilityEvent({
    subsystem: "cron",
    code: "mission_runtime_agent_spawned",
    severity: "info",
    subject: jobId,
    message: `cron: mission runtime spawned ${params.unit.unitKind} agent`,
    recoverable: true,
    quiet: true,
    createdAt: params.nowMs,
    metadata: {
      jobId,
      missionId: params.missionId,
      workId: params.unit.workId,
      unitKind: params.unit.unitKind,
    },
  });
  return true;
}

function upsertMissionRuntimeAgentDispatchFlow(params: {
  job: CronJob;
  unit: Extract<MissionRuntimeDispatchUnit, { action: "spawn_agent" }>;
  missionId: string;
  nowMs: number;
}) {
  const ownerKey = `mission-runtime:agent-spawn:${params.missionId}:${params.unit.workId}`;
  const stateJson = {
    openclawWorkManager: {
      version: 1,
      workId: params.unit.workId,
      lane: `Mission Agent ${params.unit.unitKind}`,
      pool: params.unit.pool,
      priority: params.unit.priority,
      requestedResources: params.unit.requestedResources,
      expectedOutput: params.unit.expectedOutput,
      proofPath: params.unit.proofPath,
      timeoutMs: params.unit.agentPacket.timeoutMs,
      owner: "mission-runtime",
      workStatus: "queued",
      createdAt: params.nowMs,
      leaseUntil: params.nowMs + params.unit.agentPacket.timeoutMs,
      handoffDepth: 0,
    },
    openclawMissionRuntime: {
      version: 2,
      missionId: params.missionId,
      decision: "spawn_agent",
      unitKind: params.unit.unitKind,
      workId: params.unit.workId,
      cronJobId: params.job.id,
      proofPath: params.unit.proofPath,
      expectedOutput: params.unit.expectedOutput,
      agentModel: params.unit.agentPacket.model,
      skillHints: params.unit.agentPacket.skillHints,
      toolHints: params.unit.agentPacket.toolHints,
      spawnedAt: params.nowMs,
    },
    openclawLastRecoveryAction: `mission_runtime:agent_spawned:${params.job.id}`,
  } satisfies Record<string, JsonValue>;
  const existing = findLatestTaskFlowForOwnerKey(ownerKey);
  if (existing && ["queued", "running", "waiting", "blocked"].includes(existing.status)) {
    updateFlowRecordByIdExpectedRevision({
      flowId: existing.flowId,
      expectedRevision: existing.revision,
      patch: {
        status: "queued",
        currentStep: "mission_runtime_agent_spawned",
        stateJson,
        updatedAt: params.nowMs,
        endedAt: null,
      },
    });
    return;
  }
  createManagedTaskFlow({
    controllerId: MISSION_RUNTIME_CONTROLLER_ID,
    ownerKey,
    notifyPolicy: "silent",
    status: "queued",
    goal: `Mission Runtime spawned ${params.unit.unitKind} agent`,
    currentStep: "mission_runtime_agent_spawned",
    stateJson,
    createdAt: params.nowMs,
    updatedAt: params.nowMs,
    endedAt: null,
  });
}

function missionAgentCronJobId(workId: string): string {
  return `mission-agent-${stableShortHash(workId)}`;
}

function stableShortHash(value: string): string {
  let hash = 2166136261;
  for (let i = 0; i < value.length; i += 1) {
    hash ^= value.charCodeAt(i);
    hash = Math.imul(hash, 16777619);
  }
  return (hash >>> 0).toString(36).padStart(7, "0").slice(0, 10);
}

function startMissionRuntimeTaskFlow(params: {
  candidate: WorkManagerCandidate;
  missionId: string;
  nowMs: number;
}): boolean {
  const flow = listTaskFlowRecords()
    .filter((record) => ["queued", "blocked", "waiting"].includes(record.status))
    .find((record) => {
      const metadata = readObject(record.stateJson, "openclawWorkManager");
      return metadata.workId === params.candidate.workId;
    });
  if (!flow) {
    upsertMissionRuntimeExactGateFlow({
      candidate: params.candidate,
      missionId: params.missionId,
      nowMs: params.nowMs,
      gate: "DISPATCH_PROOF_MISSING",
    });
    return true;
  }
  const dispatchProof = applyQueuedWorkDispatchProof(
    { decision: "dispatch", reason: "available", candidate: params.candidate },
    {
      dispatchEffect: "taskflow_started",
      flowId: flow.flowId,
      owner: params.candidate.owner,
      startedAt: params.nowMs,
      firstStatusCheck: params.nowMs + 60_000,
    },
  );
  const stateJson = mergeStateJson(flow.stateJson, {
    openclawWorkManager: {
      ...readObject(flow.stateJson, "openclawWorkManager"),
      workStatus: "running",
      dispatchedAt: params.nowMs,
      leaseUntil: params.nowMs + DEFAULT_JOB_TIMEOUT_MS,
    },
    openclawQueueDrain: {
      ...dispatchProof,
      reason: "mission_runtime_selected",
      dispatchedAt: params.nowMs,
    },
    openclawMissionRuntime: {
      version: 1,
      missionId: params.missionId,
      decision: "start_taskflow",
      selectedWorkId: params.candidate.workId,
      proofPath: params.candidate.proofPath,
      expectedOutput: params.candidate.expectedOutput,
    },
  });
  updateFlowRecordByIdExpectedRevision({
    flowId: flow.flowId,
    expectedRevision: flow.revision,
    patch: {
      status: "running",
      currentStep: "dispatched_by_mission_runtime",
      stateJson,
      updatedAt: params.nowMs,
      endedAt: null,
    },
  });
  return true;
}

function upsertMissionRuntimeSuppressedFlow(params: {
  job: CronJob;
  nowMs: number;
  nextRunAtMs: number;
}) {
  const ownerKey = `mission-runtime:suppressed:${params.job.id}`;
  const stateJson = {
    openclawMissionRuntime: {
      version: 1,
      decision: "suppressed_autonomous_lane",
      jobId: params.job.id,
      jobName: params.job.name,
      reason: "mission_runtime_owns_company_work",
      nextRunAtMs: params.nextRunAtMs,
      decidedAt: params.nowMs,
    },
  } satisfies Record<string, JsonValue>;
  const existing = findLatestTaskFlowForOwnerKey(ownerKey);
  if (existing && ["queued", "running", "waiting", "blocked"].includes(existing.status)) {
    updateFlowRecordByIdExpectedRevision({
      flowId: existing.flowId,
      expectedRevision: existing.revision,
      patch: {
        status: "blocked",
        currentStep: "mission_runtime_suppressed_autonomy",
        stateJson,
        updatedAt: params.nowMs,
        endedAt: null,
      },
    });
    return;
  }
  createManagedTaskFlow({
    controllerId: MISSION_RUNTIME_CONTROLLER_ID,
    ownerKey,
    notifyPolicy: "silent",
    status: "blocked",
    goal: `Mission Runtime suppresses autonomous lane ${params.job.name}`,
    currentStep: "mission_runtime_suppressed_autonomy",
    stateJson,
    createdAt: params.nowMs,
    updatedAt: params.nowMs,
    endedAt: null,
  });
}

function upsertMissionRuntimeExactGateFlow(params: {
  candidate: WorkManagerCandidate;
  missionId: string;
  nowMs: number;
  gate: string;
}) {
  createManagedTaskFlow({
    controllerId: MISSION_RUNTIME_CONTROLLER_ID,
    ownerKey: `mission-runtime:exact-gate:${params.missionId}:${params.candidate.workId}`,
    notifyPolicy: "silent",
    status: "blocked",
    goal: `Mission Runtime exact gate for ${params.candidate.lane}`,
    currentStep: "mission_runtime_exact_gate",
    stateJson: {
      openclawMissionRuntime: {
        version: 1,
        missionId: params.missionId,
        decision: "exact_gate",
        gate: params.gate,
        workId: params.candidate.workId,
        proofPath: params.candidate.proofPath,
        expectedOutput: params.candidate.expectedOutput,
        decidedAt: params.nowMs,
      },
    },
    createdAt: params.nowMs,
    updatedAt: params.nowMs,
    endedAt: null,
  });
}

function workManagerCronOwnerKey(job: CronJob): string {
  return `work-manager:cron:${job.id}`;
}

function upsertAdmissionDeferredWorkFlow(params: {
  job: CronJob;
  candidate: WorkManagerCandidate;
  decision: WorkAdmissionDecision;
  nowMs: number;
  nextRunAtMs: number;
}) {
  if (params.decision.decision === "allow" || !shouldTrackAdmissionDeferredWork(params.candidate)) {
    return;
  }

  const ownerKey = workManagerCronOwnerKey(params.job);
  const existing = findLatestTaskFlowForOwnerKey(ownerKey);
  const stateJson = buildAdmissionDeferredStateJson({
    ...params,
    decision: params.decision,
  });
  const activeStatuses = new Set(["queued", "running", "waiting", "blocked"]);
  if (existing && activeStatuses.has(existing.status)) {
    updateFlowRecordByIdExpectedRevision({
      flowId: existing.flowId,
      expectedRevision: existing.revision,
      patch: {
        status: "queued",
        goal: params.job.name,
        currentStep: "admission_deferred",
        blockedTaskId: params.job.id,
        blockedSummary: `work manager deferred cron job: ${params.decision.reason}`,
        stateJson,
        updatedAt: params.nowMs,
        endedAt: null,
      },
    });
    return;
  }

  createManagedTaskFlow({
    controllerId: WORK_MANAGER_CONTROLLER_ID,
    ownerKey,
    notifyPolicy: "silent",
    status: "queued",
    goal: params.job.name,
    currentStep: "admission_deferred",
    blockedTaskId: params.job.id,
    blockedSummary: `work manager deferred cron job: ${params.decision.reason}`,
    stateJson,
    createdAt: params.nowMs,
    updatedAt: params.nowMs,
  });
}

function finishAdmissionDeferredWorkFlow(
  job: CronJob,
  candidate: WorkManagerCandidate,
  nowMs: number,
) {
  if (!shouldTrackAdmissionDeferredWork(candidate)) {
    return;
  }
  const existing = findLatestTaskFlowForOwnerKey(workManagerCronOwnerKey(job));
  if (!existing || !["queued", "running", "waiting", "blocked"].includes(existing.status)) {
    return;
  }
  const dispatchProof = applyQueuedWorkDispatchProof(
    { decision: "dispatch", reason: "available", candidate },
    {
      dispatchEffect: "cron_promoted",
      cronJobId: job.id,
      owner: candidate.owner,
      startedAt: nowMs,
      firstStatusCheck: nowMs + 60_000,
    },
  );
  const stateJson = mergeStateJson(existing.stateJson, {
    openclawWorkManager: {
      ...readObject(existing.stateJson, "openclawWorkManager"),
      workStatus: "succeeded",
      dispatchedAt: nowMs,
    },
    openclawQueueDrain: {
      ...dispatchProof,
      dispatchedAt: nowMs,
    },
  });
  finishFlow({
    flowId: existing.flowId,
    expectedRevision: existing.revision,
    currentStep: "dispatched_by_work_manager",
    stateJson,
    updatedAt: nowMs,
    endedAt: nowMs,
  });
}

function shouldTrackAdmissionDeferredWork(candidate: WorkManagerCandidate): boolean {
  return (
    candidate.priority === "P0_USER_DIRECTIVE" ||
    candidate.priority === "P0" ||
    candidate.priority === "P1"
  );
}

function buildAdmissionDeferredStateJson(params: {
  candidate: WorkManagerCandidate;
  decision: Exclude<WorkAdmissionDecision, { decision: "allow" }>;
  nowMs: number;
  nextRunAtMs: number;
}): JsonValue {
  return {
    openclawWorkManager: {
      version: 1,
      workId: params.candidate.workId,
      lane: params.candidate.lane,
      pool: params.candidate.pool,
      priority: params.candidate.priority,
      requestedResources: params.candidate.requestedResources,
      expectedOutput: params.candidate.expectedOutput,
      proofPath: params.candidate.proofPath,
      timeoutMs: params.candidate.timeoutMs,
      owner: params.candidate.owner,
      workStatus: "queued",
      createdAt: params.nowMs,
      leaseUntil: params.nextRunAtMs,
      handoffDepth: params.candidate.handoffDepth ?? 0,
      blockedReason: params.decision.reason,
      blockedResource: params.decision.resource ?? "",
      blockedResources: params.decision.blockedResources ?? [],
      nextDispatchCheckAt: params.nextRunAtMs,
    },
    openclawQueueDrain: {
      decision: "blocked",
      reason: params.decision.reason,
      blockedResource: params.decision.resource ?? "",
      blockedResources: params.decision.blockedResources ?? [],
      nextDispatchCheckAt: params.nextRunAtMs,
    },
  };
}

function timeoutSplitOwnerKey(job: CronJob): string {
  return `work-manager:timeout-split:${job.id}`;
}

function upsertTimeoutAutoSplitWorkFlow(params: {
  state: CronServiceState;
  job: CronJob;
  result: Pick<TimedCronRunOutcome, "status" | "error" | "endedAt">;
}) {
  if (
    params.result.status !== "error" ||
    params.job.schedule.kind === "at" ||
    !isTimeoutLikeError(params.result.error) ||
    (params.job.state.consecutiveErrors ?? 0) < WORK_MANAGER_TIMEOUT_SPLIT_AFTER
  ) {
    return;
  }
  const nowMs = params.result.endedAt;
  const baseCandidate = createCronWorkCandidate({
    job: params.job,
    nowMs,
    status: "queued",
  });
  const candidate: WorkManagerCandidate = {
    ...baseCandidate,
    workId: `timeout-split:${params.job.id}:${nowMs}`,
    lane: `Narrowed recovery for ${params.job.name}`,
    expectedOutput: `Shrink the timed-out native cron scope and finish one bounded safe unit for ${params.job.name}.`,
    proofPath: params.job.id,
    owner: "work-manager:timeout-split",
    leaseUntil: nowMs + WORK_MANAGER_ADMISSION_RETRY_MS,
  };
  const stateJson = {
    openclawWorkManager: {
      version: 1,
      workId: candidate.workId,
      lane: candidate.lane,
      pool: candidate.pool,
      priority: candidate.priority,
      requestedResources: candidate.requestedResources,
      expectedOutput: candidate.expectedOutput,
      proofPath: candidate.proofPath,
      timeoutMs: candidate.timeoutMs,
      owner: candidate.owner,
      workStatus: "queued",
      createdAt: nowMs,
      leaseUntil: candidate.leaseUntil ?? nowMs + WORK_MANAGER_ADMISSION_RETRY_MS,
      handoffDepth: candidate.handoffDepth ?? 0,
      blockedReason: "timeout_auto_split",
      nextDispatchCheckAt: nowMs + WORK_MANAGER_ADMISSION_RETRY_MS,
    },
    openclawQueueDrain: {
      decision: "blocked",
      reason: "timeout_auto_split",
      nextDispatchCheckAt: nowMs + WORK_MANAGER_ADMISSION_RETRY_MS,
    },
  } satisfies Record<string, JsonValue>;
  const ownerKey = timeoutSplitOwnerKey(params.job);
  const existing = findLatestTaskFlowForOwnerKey(ownerKey);
  const activeStatuses = new Set(["queued", "running", "waiting", "blocked"]);
  if (existing && activeStatuses.has(existing.status)) {
    updateFlowRecordByIdExpectedRevision({
      flowId: existing.flowId,
      expectedRevision: existing.revision,
      patch: {
        status: "queued",
        goal: candidate.lane,
        currentStep: "timeout_auto_split_queued",
        blockedTaskId: params.job.id,
        blockedSummary: "timed-out cron needs narrowed native recovery unit",
        stateJson,
        updatedAt: nowMs,
        endedAt: null,
      },
    });
    return;
  }
  createManagedTaskFlow({
    controllerId: WORK_MANAGER_CONTROLLER_ID,
    ownerKey,
    notifyPolicy: "silent",
    status: "queued",
    goal: candidate.lane,
    currentStep: "timeout_auto_split_queued",
    blockedTaskId: params.job.id,
    blockedSummary: "timed-out cron needs narrowed native recovery unit",
    stateJson,
    createdAt: nowMs,
    updatedAt: nowMs,
  });
  params.state.deps.log.info(
    {
      jobId: params.job.id,
      consecutiveErrors: params.job.state.consecutiveErrors,
    },
    "cron: work manager queued timeout auto-split recovery",
  );
  recordReliabilityEvent({
    subsystem: "cron",
    code: "timeout_auto_split_queued",
    severity: "info",
    subject: params.job.id,
    message: `cron: queued narrowed recovery for timed-out job ${params.job.name}`,
    recoverable: true,
    quiet: true,
    createdAt: nowMs,
    metadata: {
      jobId: params.job.id,
      consecutiveErrors: params.job.state.consecutiveErrors,
    },
  });
}

function isTimeoutLikeError(error: string | undefined): boolean {
  return /\b(timeout|timed out|deadline|abort)\b/i.test(error ?? "");
}

function mergeStateJson(
  existing: JsonValue | undefined,
  patch: Record<string, JsonValue>,
): JsonValue {
  return {
    ...(isPlainJsonObject(existing) ? existing : {}),
    ...patch,
  };
}

function readObject(value: JsonValue | undefined, key: string): Record<string, JsonValue> {
  if (!isPlainJsonObject(value)) {
    return {};
  }
  const child = value[key];
  return isPlainJsonObject(child) ? child : {};
}

function isPlainJsonObject(value: unknown): value is Record<string, JsonValue> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

export async function runMissedJobs(
  state: CronServiceState,
  opts?: { skipJobIds?: ReadonlySet<string> },
) {
  const plan = await planStartupCatchup(state, opts);
  if (plan.candidates.length === 0 && plan.deferredJobIds.length === 0) {
    return;
  }

  const outcomes = await executeStartupCatchupPlan(state, plan);
  await applyStartupCatchupOutcomes(state, plan, outcomes);
}

async function planStartupCatchup(
  state: CronServiceState,
  opts?: { skipJobIds?: ReadonlySet<string> },
): Promise<StartupCatchupPlan> {
  const maxImmediate = Math.max(
    0,
    state.deps.maxMissedJobsPerRestart ?? DEFAULT_MAX_MISSED_JOBS_PER_RESTART,
  );
  return locked(state, async () => {
    await ensureLoaded(state, { skipRecompute: true });
    if (!state.store) {
      return { candidates: [], deferredJobIds: [] };
    }

    const now = state.deps.nowMs();
    const reconciledRunningMarkers = reconcileRunningMarkers(state, now);
    const quarantinedStoredTimeoutLoops = quarantineStoredRecurringTimeoutLoops(state);
    const missionRuntimeTick = applyMissionRuntimeTick(state, now);
    const skipJobIds =
      missionRuntimeTick.skipJobIds.size > 0 || opts?.skipJobIds
        ? new Set([...(opts?.skipJobIds ?? []), ...missionRuntimeTick.skipJobIds])
        : undefined;
    const missed = collectRunnableJobs(state, now, {
      skipJobIds,
      skipAtIfAlreadyRan: true,
      allowCronMissedRunByLastRun: true,
    });
    if (missed.length === 0) {
      if (reconciledRunningMarkers || quarantinedStoredTimeoutLoops || missionRuntimeTick.changed) {
        await persist(state);
      }
      return { candidates: [], deferredJobIds: [] };
    }
    const sorted = missed.toSorted(
      (a, b) => (a.state.nextRunAtMs ?? 0) - (b.state.nextRunAtMs ?? 0),
    );
    const startupCandidates = sorted.slice(0, maxImmediate);
    const deferred = sorted.slice(maxImmediate);
    if (deferred.length > 0) {
      state.deps.log.info(
        {
          immediateCount: startupCandidates.length,
          deferredCount: deferred.length,
          totalMissed: missed.length,
        },
        "cron: staggering missed jobs to prevent gateway overload",
      );
    }
    if (startupCandidates.length > 0) {
      state.deps.log.info(
        { count: startupCandidates.length, jobIds: startupCandidates.map((j) => j.id) },
        "cron: running missed jobs after restart",
      );
    }
    for (const job of startupCandidates) {
      job.state.runningAtMs = now;
      job.state.lastError = undefined;
    }
    await persist(state);

    return {
      candidates: startupCandidates.map((job) => ({ jobId: job.id, job })),
      deferredJobIds: deferred.map((job) => job.id),
    };
  });
}

async function executeStartupCatchupPlan(
  state: CronServiceState,
  plan: StartupCatchupPlan,
): Promise<TimedCronRunOutcome[]> {
  const outcomes: TimedCronRunOutcome[] = [];
  for (const candidate of plan.candidates) {
    outcomes.push(await runStartupCatchupCandidate(state, candidate));
  }
  return outcomes;
}

async function runStartupCatchupCandidate(
  state: CronServiceState,
  candidate: StartupCatchupCandidate,
): Promise<TimedCronRunOutcome> {
  const startedAt = state.deps.nowMs();
  const taskRunId = tryCreateCronTaskRun({
    state,
    job: candidate.job,
    startedAt,
  });
  emit(state, { jobId: candidate.job.id, action: "started", runAtMs: startedAt });
  try {
    const result = await executeJobCoreWithTimeout(state, candidate.job);
    return {
      jobId: candidate.jobId,
      taskRunId,
      status: result.status,
      error: result.error,
      summary: result.summary,
      delivered: result.delivered,
      sessionId: result.sessionId,
      sessionKey: result.sessionKey,
      model: result.model,
      provider: result.provider,
      usage: result.usage,
      startedAt,
      endedAt: state.deps.nowMs(),
    };
  } catch (err) {
    return {
      jobId: candidate.jobId,
      taskRunId,
      status: "error",
      error: normalizeCronRunErrorText(err),
      startedAt,
      endedAt: state.deps.nowMs(),
    };
  }
}

async function applyStartupCatchupOutcomes(
  state: CronServiceState,
  plan: StartupCatchupPlan,
  outcomes: TimedCronRunOutcome[],
): Promise<void> {
  const staggerMs = Math.max(0, state.deps.missedJobStaggerMs ?? DEFAULT_MISSED_JOB_STAGGER_MS);
  await locked(state, async () => {
    // Startup catch-up runs during service bootstrap, before the timer loop is
    // armed. Reuse the in-memory store instead of forcing a second reload.
    await ensureLoaded(state, { skipRecompute: true });
    if (!state.store) {
      return;
    }

    for (const result of outcomes) {
      applyOutcomeToStoredJob(state, result);
    }

    if (plan.deferredJobIds.length > 0) {
      const baseNow = state.deps.nowMs();
      let offset = staggerMs;
      for (const jobId of plan.deferredJobIds) {
        const job = state.store.jobs.find((entry) => entry.id === jobId);
        if (!job || !isJobEnabled(job)) {
          continue;
        }
        job.state.nextRunAtMs = baseNow + offset;
        offset += staggerMs;
      }
    }

    // Preserve any new past-due nextRunAtMs values that became due while
    // startup catch-up was running. They should execute on a future tick
    // instead of being silently advanced.
    recomputeNextRunsForMaintenance(state);
    await persist(state);
  });
}

export async function runDueJobs(state: CronServiceState) {
  if (!state.store) {
    return;
  }
  const now = state.deps.nowMs();
  const reconciledRunningMarkers = reconcileRunningMarkers(state, now);
  const missionRuntimeTick = applyMissionRuntimeTick(state, now);
  const due = collectRunnableJobs(state, now, { skipJobIds: missionRuntimeTick.skipJobIds });
  if (reconciledRunningMarkers || missionRuntimeTick.changed) {
    await persist(state);
  }
  for (const job of due) {
    await executeJob(state, job, now, { forced: false });
  }
}

export async function executeJobCore(
  state: CronServiceState,
  job: CronJob,
  abortSignal?: AbortSignal,
  options?: {
    onExecutionStarted?: () => void;
  },
): Promise<
  CronRunOutcome &
    CronRunTelemetry & {
      delivered?: boolean;
      deliveryAttempted?: boolean;
      delivery?: CronDeliveryTrace;
    }
> {
  const resolveAbortError = () => ({
    status: "error" as const,
    error: timeoutErrorMessage(),
  });
  const waitWithAbort = async (ms: number) => {
    if (!abortSignal) {
      await new Promise<void>((resolve) => setTimeout(resolve, ms));
      return;
    }
    if (abortSignal.aborted) {
      return;
    }
    await new Promise<void>((resolve) => {
      const timer = setTimeout(() => {
        abortSignal.removeEventListener("abort", onAbort);
        resolve();
      }, ms);
      const onAbort = () => {
        clearTimeout(timer);
        abortSignal.removeEventListener("abort", onAbort);
        resolve();
      };
      abortSignal.addEventListener("abort", onAbort, { once: true });
    });
  };

  if (abortSignal?.aborted) {
    return resolveAbortError();
  }
  if (job.sessionTarget === "main") {
    return await executeMainSessionCronJob(state, job, abortSignal, waitWithAbort);
  }

  return await executeDetachedCronJob(state, job, abortSignal, resolveAbortError, options);
}

async function executeMainSessionCronJob(
  state: CronServiceState,
  job: CronJob,
  abortSignal: AbortSignal | undefined,
  waitWithAbort: (ms: number) => Promise<void>,
): Promise<
  CronRunOutcome &
    CronRunTelemetry & {
      delivered?: boolean;
      deliveryAttempted?: boolean;
      delivery?: CronDeliveryTrace;
    }
> {
  const text = resolveJobPayloadTextForMain(job);
  if (!text) {
    const kind = job.payload.kind;
    return {
      status: "skipped",
      error:
        kind === "systemEvent"
          ? "main job requires non-empty systemEvent text"
          : 'main job requires payload.kind="systemEvent"',
    };
  }
  const targetMainSessionKey = job.sessionKey;
  state.deps.enqueueSystemEvent(text, {
    agentId: job.agentId,
    sessionKey: targetMainSessionKey,
    contextKey: `cron:${job.id}`,
  });
  if (job.wakeMode === "now" && state.deps.runHeartbeatOnce) {
    const reason = `cron:${job.id}`;
    const isRecurringJob = job.schedule.kind !== "at";
    const maxWaitMs = state.deps.wakeNowHeartbeatBusyMaxWaitMs ?? 2 * 60_000;
    const retryDelayMs = state.deps.wakeNowHeartbeatBusyRetryDelayMs ?? 250;
    const waitStartedAt = state.deps.nowMs();

    let heartbeatResult: HeartbeatRunResult;
    for (;;) {
      if (abortSignal?.aborted) {
        return { status: "error", error: timeoutErrorMessage() };
      }
      heartbeatResult = await state.deps.runHeartbeatOnce({
        reason,
        agentId: job.agentId,
        sessionKey: targetMainSessionKey,
        heartbeat: { target: "last" },
      });
      if (heartbeatResult.status !== "skipped" || heartbeatResult.reason !== "requests-in-flight") {
        break;
      }
      if (isRecurringJob) {
        // Recurring main-session cron jobs should not hold the cron lane open
        // while the main lane is busy, or their measured duration starts to
        // reflect queue wait instead of cron bookkeeping (#58833).
        state.deps.requestHeartbeatNow({
          reason,
          agentId: job.agentId,
          sessionKey: targetMainSessionKey,
          heartbeat: { target: "last" },
        });
        return { status: "ok", summary: text };
      }
      if (abortSignal?.aborted) {
        return { status: "error", error: timeoutErrorMessage() };
      }
      if (state.deps.nowMs() - waitStartedAt > maxWaitMs) {
        if (abortSignal?.aborted) {
          return { status: "error", error: timeoutErrorMessage() };
        }
        state.deps.requestHeartbeatNow({
          reason,
          agentId: job.agentId,
          sessionKey: targetMainSessionKey,
          heartbeat: { target: "last" },
        });
        return { status: "ok", summary: text };
      }
      await waitWithAbort(retryDelayMs);
    }

    if (heartbeatResult.status === "ran") {
      return { status: "ok", summary: text };
    }
    if (heartbeatResult.status === "skipped") {
      return { status: "skipped", error: heartbeatResult.reason, summary: text };
    }
    return { status: "error", error: heartbeatResult.reason, summary: text };
  }

  if (abortSignal?.aborted) {
    return { status: "error", error: timeoutErrorMessage() };
  }
  state.deps.requestHeartbeatNow({
    reason: `cron:${job.id}`,
    agentId: job.agentId,
    sessionKey: targetMainSessionKey,
    heartbeat: { target: "last" },
  });
  return { status: "ok", summary: text };
}

async function executeDetachedCronJob(
  state: CronServiceState,
  job: CronJob,
  abortSignal: AbortSignal | undefined,
  resolveAbortError: () => { status: "error"; error: string },
  options?: {
    onExecutionStarted?: () => void;
  },
): Promise<
  CronRunOutcome &
    CronRunTelemetry & {
      delivered?: boolean;
      deliveryAttempted?: boolean;
      delivery?: CronDeliveryTrace;
    }
> {
  if (job.payload.kind !== "agentTurn") {
    return { status: "skipped", error: "isolated job requires payload.kind=agentTurn" };
  }
  if (abortSignal?.aborted) {
    return resolveAbortError();
  }

  const res = await state.deps.runIsolatedAgentJob({
    job,
    message: job.payload.message,
    abortSignal,
    onExecutionStarted: options?.onExecutionStarted,
  });

  if (abortSignal?.aborted) {
    return { status: "error", error: timeoutErrorMessage() };
  }

  const normalized = normalizeMissionAgentProofOutcome(job, res);

  return {
    status: normalized.status,
    error: normalized.error,
    summary: normalized.summary,
    delivered: normalized.delivered,
    deliveryAttempted: normalized.deliveryAttempted,
    delivery: normalized.delivery,
    sessionId: normalized.sessionId,
    sessionKey: normalized.sessionKey,
    model: normalized.model,
    provider: normalized.provider,
    usage: normalized.usage,
  };
}

function isMissionRuntimeAgentCronJob(job: CronJob): boolean {
  return job.id.startsWith("mission-agent-") || /^Mission Agent\b/.test(job.name);
}

function normalizeMissionAgentProofOutcome<T extends CronRunOutcome & CronRunTelemetry>(
  job: CronJob,
  result: T & {
    delivered?: boolean;
    deliveryAttempted?: boolean;
    delivery?: CronDeliveryTrace;
  },
): T & {
  delivered?: boolean;
  deliveryAttempted?: boolean;
  delivery?: CronDeliveryTrace;
} {
  if (
    !isMissionRuntimeAgentCronJob(job) ||
    result.status !== "ok" ||
    (typeof result.summary === "string" && result.summary.trim())
  ) {
    return result;
  }
  return {
    ...result,
    status: "error",
    error: "NO_OUTPUT: mission agent exited without mandatory terminal proof summary",
    summary: "NO_OUTPUT: mission agent exited without mandatory terminal proof summary",
  };
}

/**
 * Execute a job. This version is used by the `run` command and other
 * places that need the full execution with state updates.
 */
export async function executeJob(
  state: CronServiceState,
  job: CronJob,
  _nowMs: number,
  _opts: { forced: boolean },
) {
  if (!job.state) {
    job.state = {};
  }
  const startedAt = state.deps.nowMs();
  job.state.runningAtMs = startedAt;
  job.state.lastError = undefined;
  markCronJobActive(job.id);
  emit(state, { jobId: job.id, action: "started", runAtMs: startedAt });

  let coreResult: {
    status: CronRunStatus;
    delivered?: boolean;
    delivery?: CronDeliveryTrace;
  } & CronRunOutcome &
    CronRunTelemetry;
  try {
    coreResult = await executeJobCoreWithTimeout(state, job);
  } catch (err) {
    coreResult = { status: "error", error: String(err) };
  }

  const endedAt = state.deps.nowMs();
  const shouldDelete = applyJobResult(state, job, {
    status: coreResult.status,
    error: coreResult.error,
    delivered: coreResult.delivered,
    startedAt,
    endedAt,
  });

  emitJobFinished(state, job, coreResult, startedAt);

  if (shouldDelete && state.store) {
    state.store.jobs = state.store.jobs.filter((j) => j.id !== job.id);
    emit(state, { jobId: job.id, action: "removed" });
  }
  clearCronJobActive(job.id);
}

function emitJobFinished(
  state: CronServiceState,
  job: CronJob,
  result: {
    status: CronRunStatus;
    delivered?: boolean;
    delivery?: CronDeliveryTrace;
  } & CronRunOutcome &
    CronRunTelemetry,
  runAtMs: number,
) {
  emit(state, {
    jobId: job.id,
    action: "finished",
    status: result.status,
    error: result.error,
    summary: result.summary,
    delivered: result.delivered,
    deliveryStatus: job.state.lastDeliveryStatus,
    deliveryError: job.state.lastDeliveryError,
    delivery: result.delivery,
    sessionId: result.sessionId,
    sessionKey: result.sessionKey,
    runAtMs,
    durationMs: job.state.lastDurationMs,
    nextRunAtMs: job.state.nextRunAtMs,
    model: result.model,
    provider: result.provider,
    usage: result.usage,
  });
}

export function wake(
  state: CronServiceState,
  opts: { mode: "now" | "next-heartbeat"; text: string },
) {
  const text = opts.text.trim();
  if (!text) {
    return { ok: false } as const;
  }
  state.deps.enqueueSystemEvent(text);
  if (opts.mode === "now") {
    state.deps.requestHeartbeatNow({ reason: "wake" });
  }
  return { ok: true } as const;
}

export function stopTimer(state: CronServiceState) {
  if (state.timer) {
    clearTimeout(state.timer);
  }
  state.timer = null;
}

export function emit(state: CronServiceState, evt: CronEvent) {
  try {
    state.deps.onEvent?.(evt);
  } catch {
    /* ignore */
  }
}
