import type { CronJob } from "../cron/types.js";
import type {
  ReliabilityHealthSnapshot,
  ReliabilityStatus,
} from "../reliability/supervisor.types.js";
import type { TaskControlRecord } from "../tasks/task-control-registry.js";
import type { TaskFlowRecord } from "../tasks/task-flow-registry.types.js";
import type { TaskRecord } from "../tasks/task-registry.types.js";

export type WorkManagerMode = "shadow" | "admission" | "overnight";

export type WorkPool =
  | "conversation"
  | "revenue"
  | "social"
  | "build"
  | "research"
  | "personal"
  | "maintenance"
  | "unknown";

export type WorkPriority = "P0_USER_DIRECTIVE" | "P0" | "P1" | "P2" | "P3" | "P4" | "P5";

export type ManagedWorkStatus =
  | "queued"
  | "running"
  | "blocked"
  | "dead_lettered"
  | "succeeded"
  | "failed"
  | "cancelled"
  | "lost";

export type WorkManagerCandidate = {
  workId: string;
  lane: string;
  pool: WorkPool;
  priority: WorkPriority;
  requestedResources: string[];
  expectedOutput: string;
  proofPath: string;
  timeoutMs: number;
  owner: string;
  status?: ManagedWorkStatus;
  createdAt: number;
  leaseUntil?: number;
  parentWorkId?: string;
  handoffDepth?: number;
};

export type WorkResourceLock = {
  lockId: string;
  ownerWorkId: string;
  resource: string;
  pool: WorkPool;
  priority: WorkPriority;
  acquiredAt: number;
  leaseUntil?: number;
  releaseReason?: string;
  enforced: boolean;
};

export type WorkAdmissionDecision =
  | { decision: "allow"; reason: "available" | "shadow_only" }
  | { decision: "queue"; reason: string; resource?: string; blockedResources?: string[] }
  | { decision: "block"; reason: string; resource?: string; blockedResources?: string[] };

export type OpenClawMissionStatus =
  | "queued"
  | "active"
  | "blocked"
  | "succeeded"
  | "failed"
  | "cancelled";

export type OpenClawMissionContract = {
  mission_id: string;
  user_request: string;
  selected_option: string;
  objective: string;
  priority: WorkPriority;
  allowed_lanes: string[];
  hard_gates: string[];
  minimum_work_floor: number;
  success_criteria: string[];
  start_time: string;
  wake_time: string;
  status: OpenClawMissionStatus;
  proof_path: string;
};

export type MissionValidationResult = {
  valid: boolean;
  missing: string[];
};

export type WorkQueueDrainDecision =
  | { decision: "none"; reason: "no_queued_p0_p1" }
  | {
      decision: "dispatch";
      reason: "available";
      candidate: WorkManagerCandidate;
      nextDispatchCheckAt?: number;
    }
  | {
      decision: "blocked";
      reason: string;
      candidate: WorkManagerCandidate;
      resource?: string;
      blockedResources?: string[];
      nextDispatchCheckAt?: number;
    };

export type MissionRevenueFloorDecision =
  | {
      decision: "none";
      reason: "no_active_mission" | "p0_p1_already_active" | "no_candidate" | "mission_not_revenue";
    }
  | {
      decision: "promote";
      reason: "revenue_floor";
      jobId: string;
      candidate: WorkManagerCandidate;
    };

export type WorkLivenessHandoffDecision =
  | { decision: "none_available"; reason: string }
  | { decision: "selected"; reason: "available"; candidate: WorkManagerCandidate }
  | {
      decision: "queued";
      reason: string;
      candidate: WorkManagerCandidate;
      resource?: string;
      blockedResources?: string[];
    };

export type MissedWorkLedgerRow = {
  promised_work: string;
  shipped_work: string;
  missed_work: string;
  reason: string;
  recover_now: boolean;
  next_safe_recovery_unit: string;
  owner_lane: string;
  proof_path: string;
};

export type WorkManagerSnapshot = {
  version: 1;
  generatedAt: number;
  mode: WorkManagerMode;
  status: ReliabilityStatus;
  activeMission?: OpenClawMissionContract;
  runningByPool: Record<WorkPool, number>;
  queuedByPool: Record<WorkPool, number>;
  blockedByPool: Record<WorkPool, number>;
  locks: WorkResourceLock[];
  blockedLocks: WorkResourceLock[];
  deadLetteredWork: WorkManagerCandidate[];
  p0p1RevenueWork: WorkManagerCandidate[];
  queuedP0P1Work: WorkManagerCandidate[];
  queueDrain: WorkQueueDrainDecision;
  missedWorkCount: number;
  lastRecoveryAction?: string;
  nextBestSafeWork?: WorkManagerCandidate;
  lastCompletedWork?: WorkManagerCandidate;
  pressure: {
    reliabilityStatus: ReliabilityStatus;
    staleTasks: number;
    dirtyCronMarkers: number;
    modelIssues: number;
    mcpIssues: number;
    deliveryIssues: number;
    sessionPressure: number;
  };
  candidates: WorkManagerCandidate[];
};

export type BuildWorkManagerSnapshotInput = {
  nowMs?: number;
  mode?: WorkManagerMode;
  tasks?: TaskRecord[];
  taskControls?: TaskControlRecord[];
  taskFlows?: TaskFlowRecord[];
  cronJobs?: CronJob[];
  reliability?: ReliabilityHealthSnapshot;
};

export type WorkManagerStatusSummary = {
  mode: WorkManagerMode;
  status: ReliabilityStatus;
  activeMission?: OpenClawMissionContract;
  runningByPool: Record<WorkPool, number>;
  queuedByPool: Record<WorkPool, number>;
  blockedLocks: WorkResourceLock[];
  deadLetteredWork: WorkManagerCandidate[];
  p0p1RevenueWork: WorkManagerCandidate[];
  queuedP0P1Work: WorkManagerCandidate[];
  queueDrain: WorkQueueDrainDecision;
  missedWorkCount: number;
  lastRecoveryAction?: string;
  nextBestSafeWork?: WorkManagerCandidate;
  lastCompletedWork?: WorkManagerCandidate;
};

export const WORK_POOLS: WorkPool[] = [
  "conversation",
  "revenue",
  "social",
  "build",
  "research",
  "personal",
  "maintenance",
  "unknown",
];

export const PRIORITY_RANK: Record<WorkPriority, number> = {
  P0_USER_DIRECTIVE: 0,
  P0: 1,
  P1: 2,
  P2: 3,
  P3: 4,
  P4: 5,
  P5: 6,
};

export const POOL_RANK: Record<WorkPool, number> = {
  conversation: 0,
  revenue: 1,
  social: 2,
  build: 3,
  research: 4,
  personal: 5,
  maintenance: 6,
  unknown: 7,
};

export const POOL_CAPS: Partial<Record<WorkPool, number>> = {
  revenue: 2,
  research: 2,
  personal: 1,
};
