import type { CronJob } from "../cron/types.js";
import type {
  ReliabilityHealthSnapshot,
  ReliabilityStatus,
} from "../reliability/supervisor.types.js";
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

export type WorkPriority = "P0" | "P1" | "P2" | "P3" | "P4" | "P5";

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

export type WorkManagerSnapshot = {
  version: 1;
  generatedAt: number;
  mode: WorkManagerMode;
  status: ReliabilityStatus;
  runningByPool: Record<WorkPool, number>;
  queuedByPool: Record<WorkPool, number>;
  blockedByPool: Record<WorkPool, number>;
  locks: WorkResourceLock[];
  blockedLocks: WorkResourceLock[];
  deadLetteredWork: WorkManagerCandidate[];
  p0p1RevenueWork: WorkManagerCandidate[];
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
  taskFlows?: TaskFlowRecord[];
  cronJobs?: CronJob[];
  reliability?: ReliabilityHealthSnapshot;
};

export type WorkManagerStatusSummary = {
  mode: WorkManagerMode;
  status: ReliabilityStatus;
  runningByPool: Record<WorkPool, number>;
  queuedByPool: Record<WorkPool, number>;
  blockedLocks: WorkResourceLock[];
  deadLetteredWork: WorkManagerCandidate[];
  p0p1RevenueWork: WorkManagerCandidate[];
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
  P0: 0,
  P1: 1,
  P2: 2,
  P3: 3,
  P4: 4,
  P5: 5,
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
