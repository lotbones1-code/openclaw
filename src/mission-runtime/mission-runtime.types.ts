import type { CronJob } from "../cron/types.js";
import type {
  OpenClawMissionContract,
  WorkManagerCandidate,
} from "../work-manager/work-manager.js";

export type MissionRuntimeMode = "shadow" | "active";

export type MissionRuntimeDecisionReason =
  | "best_next_company_unit"
  | "queued_company_work"
  | "p0_p1_company_work_already_active"
  | "no_active_mission"
  | "no_safe_company_unit"
  | "mission_paused"
  | "all_capabilities_gated"
  | "parallel_company_work"
  | "novel_agent_required"
  | "self_acquire_required"
  | "self_improvement_required"
  | "personal_assistant_required";

export type MissionRuntimeUnitKind =
  | "known_capability"
  | "novel_agent"
  | "self_acquire"
  | "self_improvement"
  | "personal_assistant";

export type MissionAgentPacket = {
  workId: string;
  taskClass: string;
  objective: string;
  prompt: string;
  model: string;
  proofPath: string;
  expectedOutput: string;
  timeoutMs: number;
  skillHints: string[];
  toolHints: string[];
};

export type MissionRuntimeDispatchUnit =
  | {
      action: "promote_cron";
      unitKind: "known_capability";
      cronJobId: string;
      job: CronJob;
      candidate: WorkManagerCandidate;
      proofPath: string;
      expectedOutput: string;
      requestedResources: string[];
      priority: WorkManagerCandidate["priority"];
      pool: WorkManagerCandidate["pool"];
    }
  | {
      action: "start_taskflow";
      unitKind: "known_capability";
      workId: string;
      candidate: WorkManagerCandidate;
      proofPath: string;
      expectedOutput: string;
      requestedResources: string[];
      priority: WorkManagerCandidate["priority"];
      pool: WorkManagerCandidate["pool"];
    }
  | {
      action: "spawn_agent";
      unitKind: MissionRuntimeUnitKind;
      workId: string;
      proofPath: string;
      expectedOutput: string;
      requestedResources: string[];
      priority: WorkManagerCandidate["priority"];
      pool: WorkManagerCandidate["pool"];
      agentPacket: MissionAgentPacket;
    };

export type MissionRuntimeDispatchPlan =
  | (MissionRuntimeDecisionBase & {
      decision: "dispatch";
      reason: Extract<
        MissionRuntimeDecisionReason,
        | "best_next_company_unit"
        | "queued_company_work"
        | "parallel_company_work"
        | "novel_agent_required"
        | "self_acquire_required"
        | "self_improvement_required"
        | "personal_assistant_required"
      >;
      mission: OpenClawMissionContract;
      units: MissionRuntimeDispatchUnit[];
      parallelLimit: number;
    })
  | (MissionRuntimeDecisionBase & {
      decision: "none";
      reason: Extract<
        MissionRuntimeDecisionReason,
        "no_active_mission" | "no_safe_company_unit" | "mission_paused"
      >;
      units: [];
      parallelLimit: number;
    })
  | (MissionRuntimeDecisionBase & {
      decision: "blocked";
      reason: Extract<
        MissionRuntimeDecisionReason,
        "p0_p1_company_work_already_active" | "all_capabilities_gated"
      >;
      mission: OpenClawMissionContract;
      resource?: string;
      blockedResources?: string[];
      units: [];
      parallelLimit: number;
    });

export type MissionRuntimeDecisionBase = {
  mission?: OpenClawMissionContract;
  suppressedCronJobIds: string[];
  exactGates: string[];
  nextCheckAt: number;
};

export type MissionRuntimeDecision =
  | (MissionRuntimeDecisionBase & {
      decision: "none";
      reason: Extract<
        MissionRuntimeDecisionReason,
        "no_active_mission" | "no_safe_company_unit" | "mission_paused"
      >;
    })
  | (MissionRuntimeDecisionBase & {
      decision: "blocked";
      reason: Extract<
        MissionRuntimeDecisionReason,
        "p0_p1_company_work_already_active" | "all_capabilities_gated"
      >;
      resource?: string;
      blockedResources?: string[];
    })
  | (MissionRuntimeDecisionBase & {
      decision: "start_taskflow";
      reason: "queued_company_work";
      workId: string;
      candidate: WorkManagerCandidate;
      proofPath: string;
      expectedOutput: string;
    })
  | (MissionRuntimeDecisionBase & {
      decision: "promote_cron";
      reason: "best_next_company_unit";
      cronJobId: string;
      job: CronJob;
      candidate: WorkManagerCandidate;
      proofPath: string;
      expectedOutput: string;
    });
