import { describe, expect, it } from "vitest";
import type { CronJob } from "../cron/types.js";
import {
  buildOpenClawMissionContract,
  buildWorkManagerSnapshot,
  type WorkManagerCandidate,
} from "../work-manager/work-manager.js";
import { selectMissionRuntimeDispatchPlan, selectMissionRuntimeUnit } from "./mission-runtime.js";

const now = Date.parse("2026-05-21T12:00:00.000Z");

function cronJob(overrides: Partial<CronJob> & { id: string; name: string }): CronJob {
  return {
    id: overrides.id,
    name: overrides.name,
    enabled: true,
    createdAtMs: now - 60_000,
    updatedAtMs: now - 60_000,
    schedule: { kind: "every", everyMs: 300_000 },
    sessionTarget: "isolated",
    wakeMode: "now",
    payload: { kind: "agentTurn", message: overrides.name },
    delivery: { mode: "none" },
    state: { nextRunAtMs: now + 300_000 },
    ...overrides,
  } as CronJob;
}

function candidate(overrides: Partial<WorkManagerCandidate> = {}): WorkManagerCandidate {
  return {
    workId: "queued-safe-revenue",
    lane: "Target Enrichment + CRO",
    pool: "revenue",
    priority: "P1",
    requestedResources: [],
    expectedOutput: "10 buyer targets and one CRO fix",
    proofPath: "/tmp/target-enrichment.md",
    timeoutMs: 300_000,
    owner: "work-manager",
    status: "queued",
    createdAt: now - 10_000,
    ...overrides,
  };
}

describe("mission runtime", () => {
  it("creates a standing company mission when no explicit active mission exists", () => {
    const snapshot = buildWorkManagerSnapshot({
      nowMs: now,
      mode: "admission",
      taskFlows: [],
      tasks: [],
      cronJobs: [],
    });

    const decision = selectMissionRuntimeUnit({
      nowMs: now,
      snapshot,
      cronJobs: [],
      queuedCandidates: [candidate()],
      standingCompanyDirective: true,
    });

    expect(decision.mission?.objective).toContain("moving safe company/revenue work forward");
    expect(decision.decision).toBe("start_taskflow");
    expect(decision.workId).toBe("queued-safe-revenue");
  });

  it("selects one bounded company unit and suppresses other autonomous lanes", () => {
    const social = cronJob({ id: "social", name: "Titan Brand Comment Lane" });
    const content = cronJob({ id: "content", name: "Content Factory" });
    const wallet = cronJob({ id: "wallet", name: "Titan Wallet Watcher" });
    const snapshot = buildWorkManagerSnapshot({
      nowMs: now,
      mode: "admission",
      taskFlows: [],
      tasks: [],
      cronJobs: [social, content, wallet],
    });

    const decision = selectMissionRuntimeUnit({
      nowMs: now,
      snapshot,
      cronJobs: [social, content, wallet],
      queuedCandidates: [],
      standingCompanyDirective: true,
    });

    expect(decision.decision).toBe("promote_cron");
    expect(decision.cronJobId).toBe("social");
    expect(decision.suppressedCronJobIds).toContain("content");
    expect(decision.suppressedCronJobIds).not.toContain("wallet");
  });

  it("does not keep hammering red autonomous lanes when safer candidates exist", () => {
    const redSocial = cronJob({
      id: "social",
      name: "Titan Brand Comment Lane",
      state: { nextRunAtMs: now, consecutiveErrors: 4, lastRunStatus: "error" },
    });
    const target = cronJob({ id: "target", name: "Target Enrichment + CRO" });
    const snapshot = buildWorkManagerSnapshot({
      nowMs: now,
      mode: "admission",
      taskFlows: [],
      tasks: [],
      cronJobs: [redSocial, target],
    });

    const decision = selectMissionRuntimeUnit({
      nowMs: now,
      snapshot,
      cronJobs: [redSocial, target],
      queuedCandidates: [],
      standingCompanyDirective: true,
    });

    expect(decision.decision).toBe("promote_cron");
    expect(decision.cronJobId).toBe("target");
    expect(decision.exactGates).toContain("CAPABILITY_DEGRADED:social");
  });

  it("blocks dispatch when P0/P1 company work is already running", () => {
    const snapshot = buildWorkManagerSnapshot({
      nowMs: now,
      mode: "admission",
      taskFlows: [],
      tasks: [],
      cronJobs: [],
    });

    const decision = selectMissionRuntimeUnit({
      nowMs: now,
      snapshot: {
        ...snapshot,
        runningByPool: { ...snapshot.runningByPool, revenue: 1 },
        locks: [
          {
            lockId: "payment/wallet:read:active",
            ownerWorkId: "active-revenue",
            resource: "payment/wallet:read",
            pool: "revenue",
            priority: "P0",
            acquiredAt: now - 10_000,
            leaseUntil: now + 60_000,
            enforced: true,
          },
        ],
      },
      cronJobs: [],
      queuedCandidates: [candidate()],
      standingCompanyDirective: true,
    });

    expect(decision).toMatchObject({
      decision: "blocked",
      reason: "p0_p1_company_work_already_active",
    });
  });

  it("plans multiple bounded units when parallel dispatch capacity is available", () => {
    const target = cronJob({ id: "target", name: "Target Enrichment + CRO" });
    const content = cronJob({ id: "content", name: "Content Factory" });
    const social = cronJob({ id: "social", name: "Titan Brand Comment Lane" });
    const snapshot = buildWorkManagerSnapshot({
      nowMs: now,
      mode: "admission",
      taskFlows: [],
      tasks: [],
      cronJobs: [target, content, social],
    });

    const plan = selectMissionRuntimeDispatchPlan({
      nowMs: now,
      snapshot,
      cronJobs: [target, content, social],
      queuedCandidates: [],
      standingCompanyDirective: true,
      maxParallelDispatch: 3,
    });

    expect(plan.decision).toBe("dispatch");
    expect(plan.units.map((unit) => unit.action)).toEqual([
      "promote_cron",
      "promote_cron",
      "promote_cron",
    ]);
    expect(plan.units.map((unit) => unit.unitKind)).toEqual([
      "known_capability",
      "known_capability",
      "known_capability",
    ]);
    expect(plan.suppressedCronJobIds).toEqual([]);
  });

  it("creates a novel agent packet when no coded capability fits the mission", () => {
    const snapshot = buildWorkManagerSnapshot({
      nowMs: now,
      mode: "admission",
      taskFlows: [
        missionFlow({
          objective: "Find and evaluate a new supplier portal Shamil mentioned in Telegram",
          selectedOption: "new_supplier_portal",
        }),
      ],
      tasks: [],
      cronJobs: [],
    });

    const plan = selectMissionRuntimeDispatchPlan({
      nowMs: now,
      snapshot,
      cronJobs: [],
      queuedCandidates: [],
      maxParallelDispatch: 2,
    });

    expect(plan.decision).toBe("dispatch");
    expect(plan.units).toHaveLength(1);
    expect(plan.units[0]).toMatchObject({
      action: "spawn_agent",
      unitKind: "novel_agent",
      pool: "research",
      priority: "P1",
    });
    expect(plan.units[0]?.agentPacket?.prompt).toContain("available MCP tools");
    expect(plan.units[0]?.agentPacket?.prompt).toContain("available OpenClaw skills");
  });

  it("creates a self-builder packet for repeated capability gaps", () => {
    const redSocial = cronJob({
      id: "social-red",
      name: "Titan Brand Comment Lane",
      state: { nextRunAtMs: now, consecutiveErrors: 4, lastRunStatus: "error" },
    });
    const snapshot = buildWorkManagerSnapshot({
      nowMs: now,
      mode: "admission",
      taskFlows: [],
      tasks: [],
      cronJobs: [redSocial],
    });

    const plan = selectMissionRuntimeDispatchPlan({
      nowMs: now,
      snapshot,
      cronJobs: [redSocial],
      queuedCandidates: [],
      standingCompanyDirective: true,
      maxParallelDispatch: 2,
    });

    expect(plan.units.some((unit) => unit.unitKind === "self_improvement")).toBe(true);
    const selfBuild = plan.units.find((unit) => unit.unitKind === "self_improvement");
    expect(selfBuild?.agentPacket?.prompt).toContain("Self-Builder Contract");
    expect(selfBuild?.agentPacket?.prompt).toContain("CAPABILITY_DEGRADED:social-red");
  });

  it("routes personal directives into personal-assistant agent packets", () => {
    const snapshot = buildWorkManagerSnapshot({
      nowMs: now,
      mode: "admission",
      taskFlows: [
        missionFlow({
          objective: "Help Shamil organize personal files and schedule reminders",
          selectedOption: "personal_assistant",
        }),
      ],
      tasks: [],
      cronJobs: [],
    });

    const plan = selectMissionRuntimeDispatchPlan({
      nowMs: now,
      snapshot,
      cronJobs: [],
      queuedCandidates: [],
      maxParallelDispatch: 2,
    });

    expect(plan.units[0]).toMatchObject({
      action: "spawn_agent",
      unitKind: "personal_assistant",
      pool: "personal",
      priority: "P0_USER_DIRECTIVE",
    });
    expect(plan.units[0]?.agentPacket?.prompt).toContain("personal assistant");
  });
});

function missionFlow(params: { objective: string; selectedOption: string }) {
  const mission = buildOpenClawMissionContract({
    missionId: `mission-${params.selectedOption}`,
    userRequest: params.objective,
    selectedOption: params.selectedOption,
    objective: params.objective,
    nowMs: now,
    wakeTime: new Date(now + 60 * 60_000).toISOString(),
    proofPath: `/tmp/${params.selectedOption}.md`,
  });
  return {
    flowId: `flow-${params.selectedOption}`,
    syncMode: "managed" as const,
    ownerKey: `mission:${params.selectedOption}`,
    controllerId: "mission-runtime",
    revision: 0,
    status: "running" as const,
    notifyPolicy: "silent" as const,
    goal: params.objective,
    currentStep: "active",
    stateJson: { openclawMission: mission },
    createdAt: now - 10_000,
    updatedAt: now - 1_000,
  };
}
