export type ReliabilitySubsystem = "tasks" | "cron" | "delivery" | "models" | "sessions" | "mcp";

export type ReliabilitySeverity = "info" | "warn" | "error";

export type ReliabilityStatus = "green" | "yellow" | "red";

export type ReliabilityRecoveryKind =
  | "task_maintenance"
  | "cron_reconcile"
  | "delivery_recover"
  | "model_fallback_route"
  | "session_rotate"
  | "mcp_circuit_break"
  | "critical_alert";

export type ReliabilityEvent = {
  id: string;
  subsystem: ReliabilitySubsystem;
  code: string;
  severity: ReliabilitySeverity;
  message: string;
  subject?: string;
  recoverable: boolean;
  createdAt: number;
  quiet?: boolean;
  metadata?: Record<string, unknown>;
};

export type ReliabilitySubsystemHealth = {
  status: ReliabilityStatus;
  findings: ReliabilityEvent[];
};

export type ReliabilityRecoveryAction = {
  kind: ReliabilityRecoveryKind;
  subsystem: ReliabilitySubsystem;
  reason: string;
  subject?: string;
  critical: boolean;
};

export type ReliabilityHealthSnapshot = {
  version: 1;
  generatedAt: number;
  status: ReliabilityStatus;
  subsystems: Record<ReliabilitySubsystem, ReliabilitySubsystemHealth>;
  actions: ReliabilityRecoveryAction[];
  lastRecoveryAction?: ReliabilityRecoveryAction;
};

export type ReliabilityTaskAuditSummary = {
  total: number;
  errors: number;
  warnings: number;
  byCode?: Record<string, number>;
};

export type ReliabilityTaskMaintenancePreview = {
  reconciled: number;
  recovered: number;
  cleanupStamped: number;
  pruned: number;
};

export type ReliabilityCronJobInput = {
  id: string;
  name?: string;
  enabled: boolean;
  runningAtMs?: number;
  timeoutMs?: number;
  consecutiveErrors?: number;
  lastError?: string;
};

export type ReliabilityTaskRunInput = {
  runId?: string;
  status: "queued" | "running" | "succeeded" | "failed" | "timed_out" | "cancelled" | "lost";
};

export type ReliabilityDeliveryInput = {
  pending: number;
  failed: number;
  permanentFailures: number;
};

export type ReliabilitySessionInput = {
  key: string;
  tokenUsageRatio?: number;
  totalTokens?: number;
};

export type ReliabilitySupervisorInput = {
  nowMs?: number;
  taskAudit?: ReliabilityTaskAuditSummary;
  taskMaintenancePreview?: ReliabilityTaskMaintenancePreview;
  cronJobs?: ReliabilityCronJobInput[];
  taskRuns?: ReliabilityTaskRunInput[];
  delivery?: ReliabilityDeliveryInput;
  sessions?: ReliabilitySessionInput[];
  events?: ReliabilityEvent[];
};
