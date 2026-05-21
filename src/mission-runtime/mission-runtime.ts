import { isJobEnabled } from "../cron/service/jobs.js";
import type { CronJob } from "../cron/types.js";
import type { OpenClawDirectiveContract } from "../execution-kernel/execution-kernel.js";
import {
  buildOpenClawMissionContract,
  createCronWorkCandidate,
  type OpenClawMissionContract,
  type WorkManagerCandidate,
  type WorkManagerSnapshot,
} from "../work-manager/work-manager.js";
import type {
  MissionAgentPacket,
  MissionRuntimeDecision,
  MissionRuntimeDecisionReason,
  MissionRuntimeDispatchPlan,
  MissionRuntimeDispatchUnit,
  MissionRuntimeUnitKind,
} from "./mission-runtime.types.js";

const MISSION_RUNTIME_RECHECK_MS = 60_000;
const DEGRADED_CAPABILITY_ERROR_THRESHOLD = 3;
const DEFAULT_MISSION_AGENT_MODEL = "claude-cli/claude-opus-4-7";

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
  const plan = selectMissionRuntimeDispatchPlan({
    ...params,
    maxParallelDispatch: 1,
  });
  if (plan.decision === "none") {
    return {
      decision: "none",
      reason: plan.reason,
      mission: plan.mission,
      suppressedCronJobIds: plan.suppressedCronJobIds,
      exactGates: plan.exactGates,
      nextCheckAt: plan.nextCheckAt,
    };
  }
  if (plan.decision === "blocked") {
    return {
      decision: "blocked",
      reason: plan.reason,
      mission: plan.mission,
      suppressedCronJobIds: plan.suppressedCronJobIds,
      exactGates: plan.exactGates,
      nextCheckAt: plan.nextCheckAt,
      ...(plan.resource ? { resource: plan.resource } : {}),
      ...(plan.blockedResources ? { blockedResources: plan.blockedResources } : {}),
    };
  }
  const unit = plan.units[0];
  if (!unit) {
    return {
      decision: "none",
      reason: "no_safe_company_unit",
      mission: plan.mission,
      suppressedCronJobIds: plan.suppressedCronJobIds,
      exactGates: plan.exactGates,
      nextCheckAt: plan.nextCheckAt,
    };
  }
  if (unit.action === "promote_cron") {
    return {
      decision: "promote_cron",
      reason: "best_next_company_unit",
      mission: plan.mission,
      cronJobId: unit.cronJobId,
      job: unit.job,
      candidate: unit.candidate,
      proofPath: unit.proofPath,
      expectedOutput: unit.expectedOutput,
      suppressedCronJobIds: plan.suppressedCronJobIds,
      exactGates: plan.exactGates,
      nextCheckAt: plan.nextCheckAt,
    };
  }
  if (unit.action === "start_taskflow") {
    return {
      decision: "start_taskflow",
      reason: "queued_company_work",
      mission: plan.mission,
      workId: unit.workId,
      candidate: unit.candidate,
      proofPath: unit.proofPath,
      expectedOutput: unit.expectedOutput,
      suppressedCronJobIds: plan.suppressedCronJobIds,
      exactGates: plan.exactGates,
      nextCheckAt: plan.nextCheckAt,
    };
  }
  return {
    decision: "none",
    reason: "no_safe_company_unit",
    mission: plan.mission,
    suppressedCronJobIds: plan.suppressedCronJobIds,
    exactGates: plan.exactGates,
    nextCheckAt: plan.nextCheckAt,
  };
}

export function selectMissionRuntimeDispatchPlan(params: {
  nowMs: number;
  snapshot: WorkManagerSnapshot;
  cronJobs: CronJob[];
  queuedCandidates?: WorkManagerCandidate[];
  standingCompanyDirective?: boolean;
  maxParallelDispatch?: number;
  novelAgentEnabled?: boolean;
  selfImprovementEnabled?: boolean;
  personalAssistantEnabled?: boolean;
  agentModel?: string;
}): MissionRuntimeDispatchPlan {
  const mission =
    params.snapshot.activeMission ??
    missionFromDirective({ directive: params.snapshot.activeDirective, nowMs: params.nowMs }) ??
    (params.standingCompanyDirective
      ? buildStandingCompanyMissionContract({ nowMs: params.nowMs })
      : undefined);
  const nextCheckAt = params.nowMs + MISSION_RUNTIME_RECHECK_MS;
  const autonomousJobs = params.cronJobs.filter(isMissionRuntimeAutonomousCronJob);
  const exactGates = gatedCapabilityCodes(autonomousJobs);
  const parallelLimit = clampParallelLimit(params.maxParallelDispatch);

  if (!mission) {
    return {
      decision: "none",
      reason: "no_active_mission",
      suppressedCronJobIds: [],
      exactGates,
      nextCheckAt,
      units: [],
      parallelLimit,
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
      units: [],
      parallelLimit,
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
      units: [],
      parallelLimit,
    };
  }

  const units: MissionRuntimeDispatchUnit[] = [];
  const selectedCronJobIds = new Set<string>();
  const reservedResources = new Set<string>();

  for (const queuedCandidate of rankedQueuedCompanyCandidates(params.queuedCandidates ?? [])) {
    if (!canReserveResources(queuedCandidate.requestedResources, reservedResources)) {
      continue;
    }
    reserveResources(queuedCandidate.requestedResources, reservedResources);
    units.push({
      action: "start_taskflow",
      unitKind: "known_capability",
      workId: queuedCandidate.workId,
      candidate: queuedCandidate,
      proofPath: queuedCandidate.proofPath,
      expectedOutput: queuedCandidate.expectedOutput,
      requestedResources: queuedCandidate.requestedResources,
      priority: queuedCandidate.priority,
      pool: queuedCandidate.pool,
    });
    if (units.length >= parallelLimit) {
      break;
    }
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

  for (const selected of selectableJobs) {
    if (units.length >= parallelLimit) {
      break;
    }
    if (!canReserveResources(selected.candidate.requestedResources, reservedResources)) {
      continue;
    }
    reserveResources(selected.candidate.requestedResources, reservedResources);
    selectedCronJobIds.add(selected.job.id);
    units.push({
      action: "promote_cron",
      unitKind: "known_capability",
      cronJobId: selected.job.id,
      job: selected.job,
      candidate: selected.candidate,
      proofPath: selected.candidate.proofPath,
      expectedOutput: selected.candidate.expectedOutput,
      requestedResources: selected.candidate.requestedResources,
      priority: selected.candidate.priority,
      pool: selected.candidate.pool,
    });
  }

  appendGeneratedMissionAgentUnits({
    units,
    mission,
    exactGates,
    nowMs: params.nowMs,
    parallelLimit,
    reservedResources,
    novelAgentEnabled: params.novelAgentEnabled !== false,
    selfImprovementEnabled: params.selfImprovementEnabled !== false,
    personalAssistantEnabled: params.personalAssistantEnabled !== false,
    agentModel: params.agentModel ?? DEFAULT_MISSION_AGENT_MODEL,
  });

  const suppressedCronJobIds = autonomousJobs
    .filter((job) => !selectedCronJobIds.has(job.id))
    .map((job) => job.id);

  if (units.length === 0) {
    return {
      decision: exactGates.length > 0 ? "blocked" : "none",
      reason: exactGates.length > 0 ? "all_capabilities_gated" : "no_safe_company_unit",
      mission,
      suppressedCronJobIds,
      exactGates,
      nextCheckAt,
      units: [],
      parallelLimit,
    } as MissionRuntimeDispatchPlan;
  }

  return {
    decision: "dispatch",
    reason: dispatchReasonForUnits(units),
    mission,
    units,
    suppressedCronJobIds,
    exactGates,
    nextCheckAt,
    parallelLimit,
  };
}

function rankedQueuedCompanyCandidates(candidates: WorkManagerCandidate[]): WorkManagerCandidate[] {
  return candidates
    .filter((candidate) => isSafeCompanyCandidate(candidate))
    .sort((a, b) => candidateScore(b) - candidateScore(a) || a.createdAt - b.createdAt);
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
  const activeCompanyCandidates = snapshot.candidates.filter((candidate) => {
    const status = candidate.status ?? "queued";
    return (
      status === "running" &&
      ["revenue", "social", "build"].includes(candidate.pool) &&
      !isMissionRuntimeControlCandidate(candidate)
    );
  });
  if (activeCompanyCandidates.length > 0) {
    return true;
  }
  const missionRuntimeOnlyActive = snapshot.candidates.some((candidate) => {
    const status = candidate.status ?? "queued";
    return (
      status === "running" &&
      ["revenue", "social", "build"].includes(candidate.pool) &&
      isMissionRuntimeControlCandidate(candidate)
    );
  });
  if (missionRuntimeOnlyActive) {
    return false;
  }
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

function isMissionRuntimeControlCandidate(candidate: WorkManagerCandidate): boolean {
  return (
    candidate.owner.startsWith("mission-runtime") ||
    candidate.workId.startsWith("mission-runtime") ||
    candidate.lane.toLowerCase().includes("mission runtime owns")
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

function clampParallelLimit(value: number | undefined): number {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    return 1;
  }
  return Math.max(1, Math.min(12, Math.floor(value)));
}

function canReserveResources(resources: string[], reserved: ReadonlySet<string>): boolean {
  return resources.every((resource) => !reserved.has(resource));
}

function reserveResources(resources: string[], reserved: Set<string>): void {
  for (const resource of resources) {
    reserved.add(resource);
  }
}

function missionFromDirective(params: {
  directive: OpenClawDirectiveContract | undefined;
  nowMs: number;
}): OpenClawMissionContract | undefined {
  if (!params.directive) {
    return undefined;
  }
  return buildOpenClawMissionContract({
    missionId: `directive:${params.directive.directive_id}`,
    nowMs: params.nowMs,
    userRequest: params.directive.user_request,
    selectedOption: params.directive.selected_option,
    objective: params.directive.goal,
    wakeTime:
      params.directive.expires_or_review_at ?? new Date(params.nowMs + 60 * 60_000).toISOString(),
    minimumWorkFloor: 1,
    proofPath: params.directive.proof_path,
  });
}

function appendGeneratedMissionAgentUnits(params: {
  units: MissionRuntimeDispatchUnit[];
  mission: OpenClawMissionContract;
  exactGates: string[];
  nowMs: number;
  parallelLimit: number;
  reservedResources: Set<string>;
  novelAgentEnabled: boolean;
  selfImprovementEnabled: boolean;
  personalAssistantEnabled: boolean;
  agentModel: string;
}): void {
  const objectiveText = missionText(params.mission);
  if (
    params.units.length < params.parallelLimit &&
    params.personalAssistantEnabled &&
    isPersonalAssistantMission(objectiveText)
  ) {
    appendAgentUnit(params, "personal_assistant");
  }
  if (
    params.units.length < params.parallelLimit &&
    params.selfImprovementEnabled &&
    params.exactGates.length > 0
  ) {
    appendAgentUnit(params, "self_improvement");
  }
  if (
    params.units.length < params.parallelLimit &&
    params.novelAgentEnabled &&
    shouldSpawnNovelAgent(objectiveText, params.units)
  ) {
    appendAgentUnit(params, "novel_agent");
  }
}

function appendAgentUnit(
  params: {
    units: MissionRuntimeDispatchUnit[];
    mission: OpenClawMissionContract;
    exactGates: string[];
    nowMs: number;
    parallelLimit: number;
    reservedResources: Set<string>;
    agentModel: string;
  },
  unitKind: Exclude<MissionRuntimeUnitKind, "known_capability">,
): void {
  if (params.units.length >= params.parallelLimit) {
    return;
  }
  const requestedResources = agentUnitResources(unitKind);
  if (!canReserveResources(requestedResources, params.reservedResources)) {
    return;
  }
  reserveResources(requestedResources, params.reservedResources);
  const packet = buildMissionAgentPacket({
    mission: params.mission,
    unitKind,
    exactGates: params.exactGates,
    nowMs: params.nowMs,
    agentModel: params.agentModel,
  });
  params.units.push({
    action: "spawn_agent",
    unitKind,
    workId: packet.workId,
    proofPath: packet.proofPath,
    expectedOutput: packet.expectedOutput,
    requestedResources,
    priority: agentUnitPriority(unitKind),
    pool: agentUnitPool(unitKind),
    agentPacket: packet,
  });
}

function buildMissionAgentPacket(params: {
  mission: OpenClawMissionContract;
  unitKind: Exclude<MissionRuntimeUnitKind, "known_capability">;
  exactGates: string[];
  nowMs: number;
  agentModel: string;
}): MissionAgentPacket {
  const taskClass = params.unitKind;
  const objective = missionAgentObjective(params.mission, params.unitKind, params.exactGates);
  const workId = `mission-agent:${params.unitKind}:${params.mission.mission_id}:${stableShortHash(
    `${params.unitKind}:${params.mission.mission_id}:${objective}`,
  )}`;
  const proofPath = `${params.mission.proof_path}#${params.unitKind}`;
  const skillHints = [
    "execution-steward",
    "account-automation",
    "social-content",
    "revenue-operator",
    "self-builder-contract",
  ];
  const toolHints = ["full native MCP/tools registry", "native browser profiles", "TaskFlow"];
  return {
    workId,
    taskClass,
    objective,
    model: params.agentModel,
    proofPath,
    expectedOutput: missionAgentExpectedOutput(params.unitKind),
    timeoutMs: 30 * 60_000,
    skillHints,
    toolHints,
    prompt: buildMissionAgentPrompt({
      mission: params.mission,
      unitKind: params.unitKind,
      objective,
      exactGates: params.exactGates,
      proofPath,
      skillHints,
      toolHints,
    }),
  };
}

function buildMissionAgentPrompt(params: {
  mission: OpenClawMissionContract;
  unitKind: Exclude<MissionRuntimeUnitKind, "known_capability">;
  objective: string;
  exactGates: string[];
  proofPath: string;
  skillHints: string[];
  toolHints: string[];
}): string {
  const intro =
    params.unitKind === "self_improvement"
      ? "Run this under the Self-Builder Contract."
      : params.unitKind === "personal_assistant"
        ? "Run this as a personal assistant mission for Shamil."
        : "Run this as a novel mission agent task.";
  return [
    intro,
    "",
    `Mission id: ${params.mission.mission_id}`,
    `Mission objective: ${params.mission.objective}`,
    `Selected option: ${params.mission.selected_option}`,
    `Task objective: ${params.objective}`,
    `Proof path: ${params.proofPath}`,
    `Exact gates already visible: ${params.exactGates.length ? params.exactGates.join(", ") : "none"}`,
    "",
    "Use the available MCP tools and native OpenClaw tool registry. Discover the right tool instead of assuming a hardcoded path.",
    "Use the available OpenClaw skills when relevant. Skill hints: " + params.skillHints.join(", "),
    "Tool hints: " + params.toolHints.join(", "),
    "",
    "Native constraints: use TaskFlow, Work Manager, native cron/task/session state, native browser profiles, vault proof logs, Learning OS, and native secrets/config only.",
    "Do not add wrappers, host cron, side daemons, fake browser paths, direct provider shortcuts, or a parallel DB.",
    "Do not mutate payment, DNS, billing, account security, public social, customer records, browser state, purchases, credits, or top-ups unless an exact directive and pre-action verifier allow it.",
    "If blocked, return one exact typed gate and a next safe fallback. End with proof path, counters, lessons applied, learning event, and next safe unit.",
  ].join("\n");
}

function missionAgentObjective(
  mission: OpenClawMissionContract,
  unitKind: Exclude<MissionRuntimeUnitKind, "known_capability">,
  exactGates: string[],
): string {
  if (unitKind === "self_improvement") {
    return `Fix the native capability gap causing ${exactGates.join(", ") || "mission degradation"} with the smallest tested OpenClaw source change.`;
  }
  if (unitKind === "personal_assistant") {
    return `Execute the personal assistant request safely: ${mission.objective}`;
  }
  return `Figure out and execute the next safe bounded unit for novel mission work: ${mission.objective}`;
}

function missionAgentExpectedOutput(
  unitKind: Exclude<MissionRuntimeUnitKind, "known_capability">,
): string {
  switch (unitKind) {
    case "self_improvement":
      return "tested native source fix or exact source/test gate";
    case "personal_assistant":
      return "completed personal assistant unit or exact typed gate";
    case "novel_agent":
      return "completed novel bounded unit or exact typed gate";
  }
}

function agentUnitResources(
  unitKind: Exclude<MissionRuntimeUnitKind, "known_capability">,
): string[] {
  switch (unitKind) {
    case "self_improvement":
      return ["repo:openclaw-runtime-src", "mission_agent:self_improvement"];
    case "personal_assistant":
      return ["mission_agent:personal_assistant"];
    case "novel_agent":
      return ["mission_agent:novel_agent"];
  }
}

function agentUnitPriority(
  unitKind: Exclude<MissionRuntimeUnitKind, "known_capability">,
): WorkManagerCandidate["priority"] {
  return unitKind === "personal_assistant" ? "P0_USER_DIRECTIVE" : "P1";
}

function agentUnitPool(
  unitKind: Exclude<MissionRuntimeUnitKind, "known_capability">,
): WorkManagerCandidate["pool"] {
  if (unitKind === "self_improvement") {
    return "build";
  }
  if (unitKind === "personal_assistant") {
    return "personal";
  }
  return "research";
}

function missionText(mission: OpenClawMissionContract): string {
  return [
    mission.user_request,
    mission.selected_option,
    mission.objective,
    ...mission.allowed_lanes,
    ...mission.success_criteria,
  ]
    .join(" ")
    .toLowerCase();
}

function isPersonalAssistantMission(text: string): boolean {
  return /\b(personal|assistant|calendar|schedule|reminder|file|organize|admin|life|appointment|email)\b/.test(
    text,
  );
}

function shouldSpawnNovelAgent(
  text: string,
  selectedUnits: readonly MissionRuntimeDispatchUnit[],
): boolean {
  return (
    selectedUnits.length === 0 ||
    /\b(novel|new|unknown|figure out|supplier|portal|integration|tool|research|source|evaluate|first time|not coded)\b/.test(
      text,
    )
  );
}

function dispatchReasonForUnits(
  units: readonly MissionRuntimeDispatchUnit[],
): Extract<
  MissionRuntimeDecisionReason,
  | "best_next_company_unit"
  | "queued_company_work"
  | "parallel_company_work"
  | "novel_agent_required"
  | "self_improvement_required"
  | "personal_assistant_required"
> {
  if (units.some((unit) => unit.unitKind === "personal_assistant")) {
    return "personal_assistant_required";
  }
  if (units.some((unit) => unit.unitKind === "self_improvement")) {
    return "self_improvement_required";
  }
  if (units.some((unit) => unit.unitKind === "novel_agent")) {
    return "novel_agent_required";
  }
  if (units.length > 1) {
    return "parallel_company_work";
  }
  return units[0]?.action === "start_taskflow" ? "queued_company_work" : "best_next_company_unit";
}

function stableShortHash(value: string): string {
  let hash = 2166136261;
  for (let i = 0; i < value.length; i += 1) {
    hash ^= value.charCodeAt(i);
    hash = Math.imul(hash, 16777619);
  }
  return (hash >>> 0).toString(36).padStart(7, "0").slice(0, 10);
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
