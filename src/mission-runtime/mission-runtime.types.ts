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
  | "all_capabilities_gated";

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
