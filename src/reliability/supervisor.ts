import { getRuntimeConfig } from "../config/config.js";
import { resolveCronJobTimeoutMs } from "../cron/service/timeout-policy.js";
import { loadCronStoreSync, resolveCronStorePath } from "../cron/store.js";
import { loadPendingDeliveries } from "../infra/outbound/delivery-queue-storage.js";
import { createSubsystemLogger } from "../logging/subsystem.js";
import { listTaskRecords } from "../tasks/runtime-internal.js";
import { listTaskAuditFindings, summarizeTaskAuditFindings } from "../tasks/task-registry.audit.js";
import {
  previewTaskRegistryMaintenance,
  runTaskRegistryMaintenance,
} from "../tasks/task-registry.maintenance.js";
import type {
  ReliabilityCronJobInput,
  ReliabilityDeliveryInput,
  ReliabilityEvent,
  ReliabilityHealthSnapshot,
  ReliabilityRecoveryAction,
  ReliabilitySessionInput,
  ReliabilityStatus,
  ReliabilitySubsystem,
  ReliabilitySubsystemHealth,
  ReliabilitySupervisorInput,
  ReliabilityTaskAuditSummary,
  ReliabilityTaskMaintenancePreview,
  ReliabilityTaskRunInput,
} from "./supervisor.types.js";

const MAX_EVENTS = 200;
const MCP_TIMEOUT_CIRCUIT_BREAK_AFTER = 3;
const CRON_TIMEOUT_QUARANTINE_AFTER = 3;
const SESSION_TOKEN_PRESSURE_RATIO = 0.9;
const SUPERVISOR_TICK_MS = 60_000;

const log = createSubsystemLogger("reliability-supervisor");

let eventSeq = 0;
let events: ReliabilityEvent[] = [];
let latestSnapshot: ReliabilityHealthSnapshot | undefined;
let supervisorTimer: NodeJS.Timeout | undefined;
let latestRecoveryAction: ReliabilityRecoveryAction | undefined;

function createEventId(createdAt: number): string {
  eventSeq += 1;
  return `rel-${createdAt}-${eventSeq}`;
}

function createEmptySubsystemHealth(): Record<ReliabilitySubsystem, ReliabilitySubsystemHealth> {
  return {
    tasks: { status: "green", findings: [] },
    cron: { status: "green", findings: [] },
    delivery: { status: "green", findings: [] },
    models: { status: "green", findings: [] },
    sessions: { status: "green", findings: [] },
    mcp: { status: "green", findings: [] },
  };
}

function maxStatus(left: ReliabilityStatus, right: ReliabilityStatus): ReliabilityStatus {
  if (left === "red" || right === "red") {
    return "red";
  }
  if (left === "yellow" || right === "yellow") {
    return "yellow";
  }
  return "green";
}

function statusFromSeverity(severity: ReliabilityEvent["severity"]): ReliabilityStatus {
  if (severity === "error") {
    return "red";
  }
  if (severity === "warn") {
    return "yellow";
  }
  return "green";
}

function addFinding(
  subsystems: Record<ReliabilitySubsystem, ReliabilitySubsystemHealth>,
  finding: ReliabilityEvent,
): void {
  const subsystem = subsystems[finding.subsystem];
  subsystem.findings.push(finding);
  subsystem.status = maxStatus(subsystem.status, statusFromSeverity(finding.severity));
}

function createSyntheticEvent(params: Omit<ReliabilityEvent, "id">): ReliabilityEvent {
  return {
    ...params,
    id: createEventId(params.createdAt),
  };
}

function getCodeCount(summary: ReliabilityTaskAuditSummary | undefined, code: string): number {
  return summary?.byCode?.[code] ?? 0;
}

function createTaskFindings(params: {
  nowMs: number;
  taskAudit?: ReliabilityTaskAuditSummary;
  preview?: ReliabilityTaskMaintenancePreview;
}): ReliabilityEvent[] {
  const findings: ReliabilityEvent[] = [];
  const staleRunning = getCodeCount(params.taskAudit, "stale_running");
  const lost = getCodeCount(params.taskAudit, "lost");
  if (staleRunning > 0) {
    findings.push(
      createSyntheticEvent({
        subsystem: "tasks",
        code: "stale_running",
        severity: "error",
        message: `${staleRunning} running task${staleRunning === 1 ? "" : "s"} appear stuck`,
        recoverable: (params.preview?.reconciled ?? 0) > 0 || (params.preview?.recovered ?? 0) > 0,
        createdAt: params.nowMs,
      }),
    );
  }
  if (lost > 0) {
    findings.push(
      createSyntheticEvent({
        subsystem: "tasks",
        code: "lost",
        severity: "info",
        message: `${lost} task${lost === 1 ? "" : "s"} lost backing session proof`,
        recoverable: false,
        createdAt: params.nowMs,
      }),
    );
  }
  return findings;
}

function isTerminalTaskStatus(status: ReliabilityTaskRunInput["status"]): boolean {
  return status !== "queued" && status !== "running";
}

function createCronFindings(params: {
  nowMs: number;
  jobs?: ReliabilityCronJobInput[];
  taskRuns?: ReliabilityTaskRunInput[];
}): ReliabilityEvent[] {
  const findings: ReliabilityEvent[] = [];
  const taskRunsByRunId = new Map(
    (params.taskRuns ?? [])
      .filter((task): task is ReliabilityTaskRunInput & { runId: string } => Boolean(task.runId))
      .map((task) => [task.runId, task]),
  );

  for (const job of params.jobs ?? []) {
    if (!job.enabled || typeof job.runningAtMs !== "number") {
      continue;
    }
    const runId = `cron:${job.id}:${job.runningAtMs}`;
    const task = taskRunsByRunId.get(runId);
    if (task && isTerminalTaskStatus(task.status)) {
      findings.push(
        createSyntheticEvent({
          subsystem: "cron",
          code: "dirty_running_marker_terminal_task",
          severity: "warn",
          subject: job.id,
          message: `${job.name ?? job.id} still has runningAtMs but task registry says ${task.status}`,
          recoverable: true,
          createdAt: params.nowMs,
          metadata: { runId, taskStatus: task.status },
        }),
      );
      continue;
    }

    const timeoutMs = job.timeoutMs;
    if (
      typeof timeoutMs === "number" &&
      Number.isFinite(timeoutMs) &&
      params.nowMs >= job.runningAtMs + timeoutMs
    ) {
      findings.push(
        createSyntheticEvent({
          subsystem: "cron",
          code: "dirty_running_marker_timeout",
          severity: "warn",
          subject: job.id,
          message: `${job.name ?? job.id} running marker exceeded timeout`,
          recoverable: true,
          createdAt: params.nowMs,
          metadata: { runId },
        }),
      );
    }
  }

  for (const job of params.jobs ?? []) {
    if (
      job.enabled &&
      (job.consecutiveErrors ?? 0) >= CRON_TIMEOUT_QUARANTINE_AFTER &&
      /timeout|timed out|interrupted by gateway restart|stale_running_marker|lost/i.test(
        job.lastError ?? "",
      )
    ) {
      findings.push(
        createSyntheticEvent({
          subsystem: "cron",
          code: "recurring_timeout_loop",
          severity: "error",
          subject: job.id,
          message: `${job.name ?? job.id} has ${job.consecutiveErrors} timeout-like consecutive errors`,
          recoverable: true,
          createdAt: params.nowMs,
        }),
      );
    }
  }

  return findings;
}

function createDeliveryFindings(params: {
  nowMs: number;
  delivery?: ReliabilityDeliveryInput;
}): ReliabilityEvent[] {
  const delivery = params.delivery;
  if (!delivery) {
    return [];
  }
  const findings: ReliabilityEvent[] = [];
  if (delivery.failed > 0 || delivery.permanentFailures > 0) {
    findings.push(
      createSyntheticEvent({
        subsystem: "delivery",
        code: "delivery_failed",
        severity: delivery.permanentFailures > 0 ? "error" : "warn",
        message: `${delivery.failed} queued delivery failure${delivery.failed === 1 ? "" : "s"}`,
        recoverable: delivery.permanentFailures === 0,
        createdAt: params.nowMs,
      }),
    );
  }
  if (delivery.pending >= 20) {
    findings.push(
      createSyntheticEvent({
        subsystem: "delivery",
        code: "delivery_backlog",
        severity: "warn",
        message: `${delivery.pending} pending delivery queue entr${delivery.pending === 1 ? "y" : "ies"}`,
        recoverable: true,
        createdAt: params.nowMs,
      }),
    );
  }
  return findings;
}

function createSessionFindings(params: {
  nowMs: number;
  sessions?: ReliabilitySessionInput[];
}): ReliabilityEvent[] {
  const findings: ReliabilityEvent[] = [];
  for (const session of params.sessions ?? []) {
    if (
      typeof session.tokenUsageRatio === "number" &&
      Number.isFinite(session.tokenUsageRatio) &&
      session.tokenUsageRatio >= SESSION_TOKEN_PRESSURE_RATIO
    ) {
      findings.push(
        createSyntheticEvent({
          subsystem: "sessions",
          code: "context_pressure",
          severity: "warn",
          subject: session.key,
          message: `${session.key} is at ${Math.round(session.tokenUsageRatio * 100)}% context usage`,
          recoverable: true,
          createdAt: params.nowMs,
          metadata: { totalTokens: session.totalTokens },
        }),
      );
    }
  }
  return findings;
}

function resolveMcpCircuitBreakActions(findings: ReliabilityEvent[]): ReliabilityRecoveryAction[] {
  const timeoutCounts = new Map<string, number>();
  for (const finding of findings) {
    if (finding.subsystem !== "mcp" || finding.code !== "mcp_startup_timeout") {
      continue;
    }
    const subject = finding.subject ?? "unknown-mcp";
    timeoutCounts.set(subject, (timeoutCounts.get(subject) ?? 0) + 1);
  }
  return Array.from(timeoutCounts.entries())
    .filter(([, count]) => count >= MCP_TIMEOUT_CIRCUIT_BREAK_AFTER)
    .map(([subject]) => ({
      kind: "mcp_circuit_break" as const,
      subsystem: "mcp" as const,
      subject,
      reason: `${subject} exceeded startup timeout threshold; mark degraded and route around it`,
      critical: false,
    }));
}

function buildActions(findings: ReliabilityEvent[]): ReliabilityRecoveryAction[] {
  const actions: ReliabilityRecoveryAction[] = [];
  if (
    findings.some(
      (finding) =>
        finding.subsystem === "tasks" &&
        (finding.code === "stale_running" || (finding.code === "lost" && finding.recoverable)),
    )
  ) {
    const hasStaleRunning = findings.some(
      (finding) => finding.subsystem === "tasks" && finding.code === "stale_running",
    );
    actions.push({
      kind: "task_maintenance",
      subsystem: "tasks",
      reason: hasStaleRunning
        ? "stale running tasks require native task-registry maintenance"
        : "lost task records require native task-registry maintenance",
      critical: hasStaleRunning,
    });
  }
  for (const finding of findings) {
    if (
      finding.subsystem === "cron" &&
      (finding.code === "dirty_running_marker_terminal_task" ||
        finding.code === "dirty_running_marker_timeout" ||
        finding.code === "recurring_timeout_loop")
    ) {
      actions.push({
        kind: "cron_reconcile",
        subsystem: "cron",
        reason: finding.message,
        subject: finding.subject,
        critical: finding.severity === "error",
      });
    }
    if (
      finding.subsystem === "delivery" &&
      (finding.code === "delivery_failed" || finding.code === "delivery_backlog")
    ) {
      actions.push({
        kind: "delivery_recover",
        subsystem: "delivery",
        reason: finding.message,
        critical: finding.severity === "error",
      });
    }
    if (finding.subsystem === "delivery" && finding.severity === "error" && !finding.quiet) {
      actions.push({
        kind: "critical_alert",
        subsystem: "delivery",
        reason: finding.message,
        subject: finding.subject,
        critical: true,
      });
    }
    if (finding.subsystem === "models" && finding.code === "model_fallback_decision") {
      const reason = String(finding.metadata?.reason ?? "");
      const nextCandidate = String(finding.metadata?.nextCandidate ?? "");
      if (/billing|rate_limit|overloaded|timeout/.test(reason) && nextCandidate) {
        actions.push({
          kind: "model_fallback_route",
          subsystem: "models",
          reason: `${finding.subject ?? "model"} unavailable (${reason}); route to ${nextCandidate}`,
          subject: finding.subject,
          critical: false,
        });
      }
    }
    if (finding.subsystem === "sessions" && finding.code === "context_pressure") {
      actions.push({
        kind: "session_rotate",
        subsystem: "sessions",
        reason: finding.message,
        subject: finding.subject,
        critical: false,
      });
    }
  }
  actions.push(...resolveMcpCircuitBreakActions(findings));
  return actions;
}

function pruneEvents(nowMs: number): void {
  events = events.filter((event) => nowMs - event.createdAt <= 24 * 60 * 60_000).slice(-MAX_EVENTS);
}

export function recordReliabilityEvent(
  event: Omit<ReliabilityEvent, "id"> & { id?: string },
): ReliabilityEvent {
  const createdAt = event.createdAt;
  const recorded: ReliabilityEvent = {
    ...event,
    id: event.id ?? createEventId(createdAt),
  };
  events.push(recorded);
  pruneEvents(createdAt);
  return recorded;
}

export function recordModelFallbackReliabilityDecision(params: {
  decision: string;
  requestedProvider: string;
  requestedModel: string;
  candidateProvider: string;
  candidateModel: string;
  reason?: string | null;
  nextCandidateProvider?: string;
  nextCandidateModel?: string;
}): void {
  const nextCandidate =
    params.nextCandidateProvider && params.nextCandidateModel
      ? `${params.nextCandidateProvider}/${params.nextCandidateModel}`
      : undefined;
  recordReliabilityEvent({
    subsystem: "models",
    code: "model_fallback_decision",
    severity:
      params.decision === "candidate_succeeded" || params.decision === "probe_cooldown_candidate"
        ? "info"
        : "warn",
    subject: `${params.candidateProvider}/${params.candidateModel}`,
    message: `model fallback ${params.decision}: ${params.candidateProvider}/${params.candidateModel}`,
    recoverable: Boolean(nextCandidate) || params.decision === "candidate_succeeded",
    createdAt: Date.now(),
    metadata: {
      decision: params.decision,
      requested: `${params.requestedProvider}/${params.requestedModel}`,
      reason: params.reason ?? undefined,
      nextCandidate,
    },
  });
}

export function recordMcpStartupFailure(params: {
  serverName: string;
  message: string;
  createdAt?: number;
}): void {
  recordReliabilityEvent({
    subsystem: "mcp",
    code: /timeout|timed out/i.test(params.message) ? "mcp_startup_timeout" : "mcp_startup_failed",
    severity: "warn",
    subject: params.serverName,
    message: params.message,
    recoverable: true,
    createdAt: params.createdAt ?? Date.now(),
  });
}

export function buildReliabilityHealthSnapshot(
  input: ReliabilitySupervisorInput = {},
): ReliabilityHealthSnapshot {
  const nowMs = input.nowMs ?? Date.now();
  pruneEvents(nowMs);
  const findings = [
    ...events,
    ...(input.events ?? []),
    ...createTaskFindings({
      nowMs,
      taskAudit: input.taskAudit,
      preview: input.taskMaintenancePreview,
    }),
    ...createCronFindings({
      nowMs,
      jobs: input.cronJobs,
      taskRuns: input.taskRuns,
    }),
    ...createDeliveryFindings({
      nowMs,
      delivery: input.delivery,
    }),
    ...createSessionFindings({
      nowMs,
      sessions: input.sessions,
    }),
  ];
  const subsystems = createEmptySubsystemHealth();
  for (const finding of findings) {
    addFinding(subsystems, finding);
  }
  const actions = buildActions(findings);
  const status = (Object.values(subsystems) as ReliabilitySubsystemHealth[]).reduce(
    (acc, subsystem) => maxStatus(acc, subsystem.status),
    "green" as ReliabilityStatus,
  );
  return {
    version: 1,
    generatedAt: nowMs,
    status,
    subsystems,
    actions,
    ...(latestRecoveryAction ? { lastRecoveryAction: latestRecoveryAction } : {}),
  };
}

async function collectDeliveryInput(): Promise<ReliabilityDeliveryInput> {
  const pending = await loadPendingDeliveries();
  return {
    pending: pending.length,
    failed: pending.filter((entry) => entry.retryCount > 0).length,
    permanentFailures: 0,
  };
}

function collectTaskAuditSummary(): ReliabilityTaskAuditSummary {
  const findings = listTaskAuditFindings();
  const summary = summarizeTaskAuditFindings(findings);
  return {
    total: summary.total,
    errors: summary.errors,
    warnings: summary.warnings,
    byCode: summary.byCode as unknown as Record<string, number>,
  };
}

function collectTaskRuns(): ReliabilityTaskRunInput[] {
  return listTaskRecords().map((task) => ({
    runId: task.runId,
    status: task.status,
  }));
}

function collectCronJobs(): ReliabilityCronJobInput[] {
  try {
    const cfg = getRuntimeConfig();
    const store = loadCronStoreSync(resolveCronStorePath(cfg.cron?.store));
    return store.jobs.map((job) => ({
      id: job.id,
      name: job.name,
      enabled: job.enabled !== false,
      runningAtMs: job.state.runningAtMs,
      timeoutMs: resolveCronJobTimeoutMs(job),
      consecutiveErrors: job.state.consecutiveErrors,
      lastError: job.state.lastError,
    }));
  } catch (error) {
    recordReliabilityEvent({
      subsystem: "cron",
      code: "cron_status_collect_failed",
      severity: "warn",
      message: `cron status collection failed: ${String(error)}`,
      recoverable: true,
      createdAt: Date.now(),
    });
    return [];
  }
}

export async function collectReliabilityHealthSnapshot(): Promise<ReliabilityHealthSnapshot> {
  const [delivery] = await Promise.all([collectDeliveryInput()]);
  latestSnapshot = buildReliabilityHealthSnapshot({
    taskAudit: collectTaskAuditSummary(),
    taskMaintenancePreview: previewTaskRegistryMaintenance(),
    cronJobs: collectCronJobs(),
    taskRuns: collectTaskRuns(),
    delivery,
  });
  return latestSnapshot;
}

export async function runAutonomicReliabilitySweep(): Promise<ReliabilityHealthSnapshot> {
  try {
    const maintenance = await runTaskRegistryMaintenance();
    if (maintenance.reconciled > 0 || maintenance.recovered > 0) {
      latestRecoveryAction = {
        kind: "task_maintenance",
        subsystem: "tasks",
        reason: `task maintenance reconciled=${maintenance.reconciled} recovered=${maintenance.recovered}`,
        critical: maintenance.reconciled > 0 || maintenance.recovered > 0,
      };
      recordReliabilityEvent({
        subsystem: "tasks",
        code: "task_maintenance_recovery",
        severity: latestRecoveryAction.critical ? "warn" : "info",
        message: latestRecoveryAction.reason,
        recoverable: false,
        createdAt: Date.now(),
        metadata: maintenance as unknown as Record<string, unknown>,
      });
    }
  } catch (error) {
    log.warn("reliability supervisor: task maintenance sweep failed", { error });
    recordReliabilityEvent({
      subsystem: "tasks",
      code: "task_maintenance_failed",
      severity: "error",
      message: `task maintenance sweep failed: ${String(error)}`,
      recoverable: true,
      createdAt: Date.now(),
    });
  }
  latestSnapshot = await collectReliabilityHealthSnapshot();
  return latestSnapshot;
}

export function getLatestReliabilityHealthSnapshot(): ReliabilityHealthSnapshot | undefined {
  return latestSnapshot;
}

export function startAutonomicReliabilitySupervisor(): void {
  if (supervisorTimer) {
    return;
  }
  void runAutonomicReliabilitySweep();
  supervisorTimer = setInterval(() => {
    void runAutonomicReliabilitySweep();
  }, SUPERVISOR_TICK_MS);
  supervisorTimer.unref?.();
}

export function stopAutonomicReliabilitySupervisor(): void {
  if (!supervisorTimer) {
    return;
  }
  clearInterval(supervisorTimer);
  supervisorTimer = undefined;
}

export function resetReliabilitySupervisorForTests(): void {
  events = [];
  latestSnapshot = undefined;
  latestRecoveryAction = undefined;
  eventSeq = 0;
  stopAutonomicReliabilitySupervisor();
}
