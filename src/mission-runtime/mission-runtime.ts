import { isJobEnabled } from "../cron/service/jobs.js";
import type { CronJob } from "../cron/types.js";
import {
  buildOpenClawMissionContract,
  createCronWorkCandidate,
  type OpenClawMissionContract,
  type WorkManagerCandidate,
  type WorkManagerSnapshot,
} from "../work-manager/work-manager.js";
import type { MissionRuntimeDecision } from "./mission-runtime.types.js";

const MISSION_RUNTIME_RECHECK_MS = 60_000;
const DEGRADED_CAPABILITY_ERROR_THRESHOLD = 3;

const DEFAULT_STANDING_COMPANY_OBJECTIVE =
  "Keep building OpenClaw/Titan and moving safe company/revenue work forward.";

const SAFE_COMPANY_POOLS = new Set(["revenue", "social", "build", "research", "personal"]);

export function buildStandingCompanyMissionContract(params: {
  nowMs: number;
}): OpenClawMissionContract {
  const base = buildOpenClawMissionContract({
    missionId: "standing-company-directive",
    nowMs: params.nowMs,
    userRequest: DEFAULT_STANDING_COMPANY_OBJECTIVE,
    selectedOption: "standing_company_directive",
    objective: DEFAULT_STANDING_COMPANY_OBJECTIVE,
    wakeTime: new Date(params.nowMs + 60 * 60_000).toISOString(),
    minimumWorkFloor: 1,
    proofPath:
      "/Users/shamil/vault/Spaces/OpenClaw/Reports/mission-runtime/standing-company-directive.md",
  });
  return {
    ...base,
    allowed_lanes: [
      "revenue/buyer_signals",
      "attribution",
      "content_packets",
      "target_sourcing",
      "checkout_cro",
      "safe_build",
      "tool_setup_proof",
      "personal_admin_prep",
    ],
    hard_gates: [
      "PAYMENT_MUTATION_REQUIRES_EXACT_DIRECTIVE",
      "DNS_MUTATION_REQUIRES_EXACT_DIRECTIVE",
      "PUBLIC_ACTION_REQUIRES_PRE_ACTION_VERIFIER",
      "ACCOUNT_SECURITY_MUTATION_REQUIRES_EXACT_DIRECTIVE",
    ],
    success_criteria: [
      "one proof-backed safe company unit ships or exact typed gate recorded",
      "next three company actions stay current",
      "risky surfaces are not mutated without exact directive and verifier pass",
    ],
  };
}

export function isMissionRuntimeSensorCronJob(job: CronJob): boolean {
  const text = cronJobText(job);
  return /\b(wallet watcher|health check|metrics|memory steward|status|morning sales brief|watcher)\b/i.test(
    text,
  );
}

export function isMissionRuntimeAutonomousCronJob(job: CronJob): boolean {
  if (!isJobEnabled(job)) {
    return false;
  }
  if (isMissionRuntimeSensorCronJob(job)) {
    return false;
  }
  const text = cronJobText(job);
  return /\b(brand comment|brand cadence|social steward|content factory|target enrichment|cro|inbox|dm triage|research steward|golden revenue|daily revenue|daily debrief|revenue debrief|always-on team|company|buyer|sales|outreach|comment lane|reel|post|publish|higgsfield)\b/i.test(
    text,
  );
}

export function selectMissionRuntimeUnit(params: {
  nowMs: number;
  snapshot: WorkManagerSnapshot;
  cronJobs: CronJob[];
  queuedCandidates?: WorkManagerCandidate[];
  standingCompanyDirective?: boolean;
}): MissionRuntimeDecision {
  const mission =
    params.snapshot.activeMission ??
    (params.standingCompanyDirective
      ? buildStandingCompanyMissionContract({ nowMs: params.nowMs })
      : undefined);
  const nextCheckAt = params.nowMs + MISSION_RUNTIME_RECHECK_MS;
  const autonomousJobs = params.cronJobs.filter(isMissionRuntimeAutonomousCronJob);
  const exactGates = gatedCapabilityCodes(autonomousJobs);

  if (!mission) {
    return {
      decision: "none",
      reason: "no_active_mission",
      suppressedCronJobIds: [],
      exactGates,
      nextCheckAt,
    };
  }

  if (
    mission.status === "cancelled" ||
    mission.status === "failed" ||
    mission.status === "succeeded"
  ) {
    return {
      decision: "none",
      reason: "mission_paused",
      mission,
      suppressedCronJobIds: [],
      exactGates,
      nextCheckAt,
    };
  }

  if (hasActiveCompanyWork(params.snapshot)) {
    return {
      decision: "blocked",
      reason: "p0_p1_company_work_already_active",
      mission,
      suppressedCronJobIds: autonomousJobs.map((job) => job.id),
      exactGates,
      nextCheckAt,
    };
  }

  const queuedCandidate = bestQueuedCompanyCandidate(params.queuedCandidates ?? []);
  if (queuedCandidate) {
    return {
      decision: "start_taskflow",
      reason: "queued_company_work",
      mission,
      workId: queuedCandidate.workId,
      candidate: queuedCandidate,
      proofPath: queuedCandidate.proofPath,
      expectedOutput: queuedCandidate.expectedOutput,
      suppressedCronJobIds: autonomousJobs.map((job) => job.id),
      exactGates,
      nextCheckAt,
    };
  }

  const selectableJobs = autonomousJobs
    .filter((job) => typeof job.state.runningAtMs !== "number")
    .filter((job) => !isCapabilityDegraded(job))
    .map((job) => ({
      job,
      candidate: createCronWorkCandidate({ job, nowMs: params.nowMs, status: "queued" }),
      score: missionRuntimeCronScore(job),
    }))
    .filter((entry) => Number.isFinite(entry.score))
    .sort((a, b) => b.score - a.score || a.job.createdAtMs - b.job.createdAtMs);

  const selected = selectableJobs[0];
  if (!selected) {
    return {
      decision: exactGates.length > 0 ? "blocked" : "none",
      reason: exactGates.length > 0 ? "all_capabilities_gated" : "no_safe_company_unit",
      mission,
      suppressedCronJobIds: autonomousJobs.map((job) => job.id),
      exactGates,
      nextCheckAt,
    } as MissionRuntimeDecision;
  }

  return {
    decision: "promote_cron",
    reason: "best_next_company_unit",
    mission,
    cronJobId: selected.job.id,
    job: selected.job,
    candidate: selected.candidate,
    proofPath: selected.candidate.proofPath,
    expectedOutput: selected.candidate.expectedOutput,
    suppressedCronJobIds: autonomousJobs
      .filter((job) => job.id !== selected.job.id)
      .map((job) => job.id),
    exactGates,
    nextCheckAt,
  };
}

function bestQueuedCompanyCandidate(
  candidates: WorkManagerCandidate[],
): WorkManagerCandidate | undefined {
  return candidates
    .filter((candidate) => isSafeCompanyCandidate(candidate))
    .sort((a, b) => candidateScore(b) - candidateScore(a) || a.createdAt - b.createdAt)[0];
}

function isSafeCompanyCandidate(candidate: WorkManagerCandidate): boolean {
  if (!SAFE_COMPANY_POOLS.has(candidate.pool)) {
    return false;
  }
  if (candidate.status && !["queued", "blocked"].includes(candidate.status)) {
    return false;
  }
  const text = `${candidate.lane} ${candidate.expectedOutput}`.toLowerCase();
  if (/\b(purchase|top[- ]?up|billing change|dns mutation|send now|post now|dm now)\b/.test(text)) {
    return false;
  }
  return true;
}

function hasActiveCompanyWork(snapshot: WorkManagerSnapshot): boolean {
  const activeCompanyLock = snapshot.locks.some((lock) =>
    ["revenue", "social", "build"].includes(lock.pool),
  );
  if (!activeCompanyLock) {
    return false;
  }
  return (
    (snapshot.runningByPool.revenue ?? 0) > 0 ||
    (snapshot.runningByPool.social ?? 0) > 0 ||
    (snapshot.runningByPool.build ?? 0) > 0
  );
}

function candidateScore(candidate: WorkManagerCandidate): number {
  let score = priorityScore(candidate.priority) + poolScore(candidate.pool);
  const text = `${candidate.lane} ${candidate.expectedOutput}`.toLowerCase();
  if (/\b(order|buyer|lead|checkout|sale|wallet|payment detection|attribution)\b/.test(text)) {
    score += 40;
  }
  if (/\b(higgsfield|asset|content|post|comment|social)\b/.test(text)) {
    score += 20;
  }
  return score;
}

function missionRuntimeCronScore(job: CronJob): number {
  const candidate = createCronWorkCandidate({ job, nowMs: Date.now(), status: "queued" });
  let score = candidateScore(candidate);
  const text = cronJobText(job).toLowerCase();
  if (/\b(debrief|brief|status|report)\b/.test(text)) {
    score -= 60;
  }
  if (/\b(wallet watcher|health check|memory steward|metrics)\b/.test(text)) {
    return Number.NEGATIVE_INFINITY;
  }
  if (/\b(buyer|order|checkout|wallet|attribution|target|cro)\b/.test(text)) {
    score += 35;
  }
  if (/\b(comment|social|post|reel|content|higgsfield)\b/.test(text)) {
    score += 20;
  }
  return score;
}

function poolScore(pool: WorkManagerCandidate["pool"]): number {
  switch (pool) {
    case "revenue":
      return 50;
    case "social":
      return 35;
    case "build":
      return 25;
    case "research":
      return 20;
    case "personal":
      return 10;
    default:
      return 0;
  }
}

function priorityScore(priority: WorkManagerCandidate["priority"]): number {
  switch (priority) {
    case "P0_USER_DIRECTIVE":
      return 100;
    case "P0":
      return 90;
    case "P1":
      return 80;
    case "P2":
      return 60;
    case "P3":
      return 40;
    case "P4":
      return 20;
    default:
      return 0;
  }
}

function gatedCapabilityCodes(jobs: CronJob[]): string[] {
  return jobs.filter(isCapabilityDegraded).map((job) => `CAPABILITY_DEGRADED:${job.id}`);
}

function isCapabilityDegraded(job: CronJob): boolean {
  const consecutiveErrors = job.state.consecutiveErrors ?? 0;
  return (
    consecutiveErrors >= DEGRADED_CAPABILITY_ERROR_THRESHOLD ||
    (job.state.lastRunStatus === "error" &&
      typeof job.state.lastError === "string" &&
      /timeout|context overflow|no output|delivery_failed|gateway restart/i.test(
        job.state.lastError,
      ))
  );
}

function cronJobText(job: CronJob): string {
  const payloadText =
    job.payload.kind === "agentTurn"
      ? job.payload.message
      : job.payload.kind === "systemEvent"
        ? job.payload.text
        : "";
  return `${job.name} ${payloadText}`;
}
