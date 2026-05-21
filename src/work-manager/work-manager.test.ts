import { describe, expect, it } from "vitest";
import type { CronJob } from "../cron/types.js";
import { buildOpenClawDirectiveContract } from "../execution-kernel/execution-kernel.js";
import type { ReliabilityHealthSnapshot } from "../reliability/supervisor.types.js";
import type { TaskFlowRecord } from "../tasks/task-flow-registry.types.js";
import type { TaskRecord } from "../tasks/task-registry.types.js";
import {
  buildWorkManagerSnapshot,
  buildMissedWorkLedger,
  buildOpenClawMissionContract,
  applyQueuedWorkDispatchProof,
  evaluateWorkAdmission,
  evaluateLivenessHandoff,
  selectQueuedWorkForDispatch,
  validateQueueDrainDispatchProof,
  selectMissionRevenueFloorCronJob,
  rankWorkCandidates,
  summarizeWorkManagerStatus,
  validateMissionContract,
  type WorkManagerCandidate,
} from "./work-manager.js";

const now = Date.parse("2026-05-19T07:00:00.000Z");

function task(overrides: Partial<TaskRecord> = {}): TaskRecord {
  return {
    taskId: "task-1",
    runtime: "cron",
    sourceId: "cron-1",
    requesterSessionKey: "",
    ownerKey: "system:cron:cron-1",
    scopeKind: "system",
    runId: "cron:cron-1:1",
    label: "Titan Brand Comment Lane",
    task: "Use titan-ig to ship one safe comment",
    status: "running",
    deliveryStatus: "not_applicable",
    notifyPolicy: "silent",
    createdAt: now - 10_000,
    startedAt: now - 10_000,
    lastEventAt: now - 5_000,
    ...overrides,
  };
}

function flow(overrides: Partial<TaskFlowRecord> = {}): TaskFlowRecord {
  return {
    flowId: "flow-1",
    syncMode: "managed",
    ownerKey: "work-manager",
    controllerId: "work-manager",
    revision: 0,
    status: "queued",
    notifyPolicy: "silent",
    goal: "Queued Titan revenue work",
    createdAt: now - 20_000,
    updatedAt: now - 20_000,
    stateJson: {
      openclawWorkManager: {
        version: 1,
        pool: "revenue",
        priority: "P1",
        requestedResources: ["browser_profile:titan-ig"],
        proofPath: "/tmp/proof.md",
        timeoutMs: 300_000,
        leaseUntil: now + 300_000,
        handoffDepth: 0,
        workStatus: "queued",
      },
    },
    ...overrides,
  };
}

function reliability(status: ReliabilityHealthSnapshot["status"]): ReliabilityHealthSnapshot {
  return {
    version: 1,
    generatedAt: now,
    status,
    subsystems: {
      tasks: { status: "green", findings: [] },
      cron: { status: "green", findings: [] },
      delivery: { status: "green", findings: [] },
      models: { status: "green", findings: [] },
      sessions: { status: "green", findings: [] },
      mcp: { status: "green", findings: [] },
    },
    actions: [],
  };
}

describe("work manager", () => {
  it("shadow mode infers locks and pool counts without blocking existing work", () => {
    const snapshot = buildWorkManagerSnapshot({
      nowMs: now,
      tasks: [task()],
      taskFlows: [],
      cronJobs: [],
      reliability: reliability("green"),
      mode: "shadow",
    });

    expect(snapshot.mode).toBe("shadow");
    expect(snapshot.runningByPool.social).toBe(1);
    expect(snapshot.locks.map((lock) => lock.resource)).toContain("browser_profile:titan-ig");
    expect(snapshot.locks[0]?.enforced).toBe(false);
  });

  it("queues new work when an active task already owns the requested browser profile", () => {
    const snapshot = buildWorkManagerSnapshot({
      nowMs: now,
      tasks: [task()],
      taskFlows: [],
      cronJobs: [],
      reliability: reliability("green"),
      mode: "admission",
    });
    const candidate: WorkManagerCandidate = {
      workId: "candidate-1",
      lane: "Titan Brand Comment Lane",
      pool: "social",
      priority: "P1",
      requestedResources: ["browser_profile:titan-ig"],
      expectedOutput: "one safe comment or exact gate",
      proofPath: "/tmp/comment.md",
      timeoutMs: 300_000,
      owner: "cron:test",
      createdAt: now,
    };

    expect(evaluateWorkAdmission(snapshot, candidate)).toMatchObject({
      decision: "queue",
      reason: "resource_locked",
      resource: "browser_profile:titan-ig",
    });
  });

  it("does not keep resource locks for tasks with active native stop controls", () => {
    const stoppedTask = task({
      taskId: "stopped-task",
      childSessionKey: "agent:main:subagent:stopped",
    });
    const snapshot = buildWorkManagerSnapshot({
      nowMs: now,
      tasks: [stoppedTask],
      taskControls: [
        {
          controlId: "stop-1",
          command: "stop",
          scope: "task:stopped-task",
          taskId: "stopped-task",
          sessionKey: "agent:main:subagent:stopped",
          requestedAt: now,
          state: "requested",
        },
      ],
      taskFlows: [],
      cronJobs: [],
      reliability: reliability("green"),
      mode: "admission",
    });

    expect(snapshot.runningByPool.social).toBe(0);
    expect(snapshot.locks.map((lock) => lock.resource)).not.toContain("browser_profile:titan-ig");
    expect(snapshot.lastCompletedWork?.status).toBe("cancelled");
  });

  it("does not double-count a running cron row when task registry has the same run id", () => {
    const walletRunId = `cron:wallet:${now}`;
    const walletTask = task({
      taskId: "wallet-task",
      sourceId: "wallet",
      runId: walletRunId,
      label: "Titan Wallet Watcher — every 2m",
      task: "Poll payment wallet",
      status: "running",
      createdAt: now,
      startedAt: now,
    });
    const walletCron = {
      id: "wallet",
      name: "Titan Wallet Watcher — every 2m",
      enabled: true,
      createdAtMs: now,
      updatedAtMs: now,
      schedule: { kind: "every", everyMs: 120_000 },
      sessionTarget: "isolated",
      wakeMode: "now",
      payload: { kind: "agentTurn", message: "Poll payment wallet" },
      delivery: { mode: "none" },
      state: { runningAtMs: now },
    } as CronJob;

    const snapshot = buildWorkManagerSnapshot({
      nowMs: now,
      tasks: [walletTask],
      cronJobs: [walletCron],
      reliability: reliability("green"),
      mode: "admission",
    });

    expect(snapshot.runningByPool.revenue).toBe(1);
    expect(snapshot.locks.filter((lock) => lock.resource === "payment/wallet:read")).toHaveLength(
      1,
    );
  });

  it("does not double-count a running cron row when task run id has small timestamp drift", () => {
    const driftedRunId = `cron:wallet:${now + 72}`;
    const walletTask = task({
      taskId: "wallet-task",
      sourceId: "wallet",
      runId: driftedRunId,
      label: "Titan Wallet Watcher — every 2m",
      task: "Poll payment wallet",
      status: "running",
      createdAt: now,
      startedAt: now + 72,
    });
    const walletCron = {
      id: "wallet",
      name: "Titan Wallet Watcher — every 2m",
      enabled: true,
      createdAtMs: now,
      updatedAtMs: now,
      schedule: { kind: "every", everyMs: 120_000 },
      sessionTarget: "isolated",
      wakeMode: "now",
      payload: { kind: "agentTurn", message: "Poll payment wallet" },
      delivery: { mode: "none" },
      state: { runningAtMs: now },
    } as CronJob;

    const snapshot = buildWorkManagerSnapshot({
      nowMs: now,
      tasks: [walletTask],
      cronJobs: [walletCron],
      reliability: reliability("green"),
      mode: "admission",
    });

    expect(snapshot.runningByPool.revenue).toBe(1);
    expect(snapshot.locks.filter((lock) => lock.resource === "payment/wallet:read")).toHaveLength(
      1,
    );
  });

  it("prioritizes revenue work over cleanup work", () => {
    const ranked = rankWorkCandidates([
      {
        workId: "cleanup",
        lane: "cleanup",
        pool: "maintenance",
        priority: "P4",
        requestedResources: [],
        expectedOutput: "cleanup report",
        proofPath: "/tmp/cleanup.md",
        timeoutMs: 300_000,
        owner: "cron:cleanup",
        createdAt: now - 100,
      },
      {
        workId: "buyer",
        lane: "buyer reply",
        pool: "revenue",
        priority: "P0",
        requestedResources: ["social_account:titan-ig"],
        expectedOutput: "buyer signal handled",
        proofPath: "/tmp/buyer.md",
        timeoutMs: 300_000,
        owner: "cron:revenue",
        createdAt: now,
      },
    ]);

    expect(ranked[0]?.workId).toBe("buyer");
  });

  it("prioritizes Shamil-selected directives above normal P0 work", () => {
    const ranked = rankWorkCandidates([
      {
        workId: "wallet",
        lane: "wallet watch",
        pool: "revenue",
        priority: "P0",
        requestedResources: ["payment/wallet"],
        expectedOutput: "watch wallet",
        proofPath: "/tmp/wallet.md",
        timeoutMs: 300_000,
        owner: "cron:wallet",
        createdAt: now - 100,
      },
      {
        workId: "selected",
        lane: "Shamil selected checkout copy",
        pool: "revenue",
        priority: "P0_USER_DIRECTIVE",
        requestedResources: ["checkout:titan"],
        expectedOutput: "execute selected option",
        proofPath: "/tmp/selected.md",
        timeoutMs: 300_000,
        owner: "mission:overnight",
        createdAt: now,
      },
    ]);

    expect(ranked[0]?.workId).toBe("selected");
  });

  it("surfaces a valid active mission contract from TaskFlow state", () => {
    const mission = {
      mission_id: "mission-1",
      user_request: "work until morning",
      selected_option: "checkout reassurance copy",
      objective: "recover missed revenue work",
      priority: "P0_USER_DIRECTIVE",
      allowed_lanes: ["revenue", "build"],
      hard_gates: ["payment_mutation", "dns_change"],
      minimum_work_floor: 3,
      success_criteria: ["one safe revenue unit ships"],
      start_time: "2026-05-19T23:00:00.000Z",
      wake_time: "2026-05-20T13:00:00.000Z",
      status: "active",
      proof_path: "/tmp/mission.md",
    };

    const snapshot = buildWorkManagerSnapshot({
      nowMs: now,
      tasks: [],
      taskFlows: [
        flow({
          stateJson: {
            openclawMission: mission,
            openclawWorkManager: {
              version: 1,
              pool: "revenue",
              priority: "P0_USER_DIRECTIVE",
              requestedResources: [],
              proofPath: "/tmp/mission.md",
              timeoutMs: 300_000,
              workStatus: "queued",
            },
          },
        }),
      ],
      reliability: reliability("green"),
      mode: "overnight",
    });

    expect(validateMissionContract(mission)).toEqual({ valid: true, missing: [] });
    expect(snapshot.activeMission?.mission_id).toBe("mission-1");
    expect(snapshot.queuedP0P1Work.map((candidate) => candidate.workId)).toContain("flow-1");
  });

  it("surfaces a valid active directive contract from TaskFlow state", () => {
    const directive = buildOpenClawDirectiveContract({
      directiveId: "directive-higgsfield-starter",
      userRequest: "buy and connect Higgsfield Starter monthly",
      selectedOption: "Starter monthly",
      goal: "complete Higgsfield Starter setup without selecting Pro or annual",
      taskClass: "saas_subscription",
      vendor: "Higgsfield",
      account: "shamilbones1@gmail.com",
      workspace: "Shamil Workspace",
      allowedActions: ["select_plan", "submit_checkout", "connect_cli"],
      forbiddenActions: ["select_pro", "select_annual", "add_credits"],
      constraints: ["Starter only", "monthly only"],
      budgetOrPriceCap: 25,
      billingPeriod: "monthly",
      exactPlanName: "Starter",
      successCriteria: ["Starter workspace verified", "CLI connected"],
      proofRequired: ["plan proof", "workspace proof"],
      hardGates: ["MISSING_CVV", "MISSING_2FA", "CAPTCHA_REQUIRED"],
      fallbackPolicy: "closest safe setup verification",
      stopInstruction: "stop Higgsfield halts this directive",
      rollbackInstruction: "do not repeat purchase; verify existing subscription first",
      proofPath: "/tmp/higgsfield-directive.md",
      createdFromMessage: "Shamil authorized Starter around $20 monthly",
      nowIso: new Date(now).toISOString(),
      ownerDirectApproval: true,
    });

    const snapshot = buildWorkManagerSnapshot({
      nowMs: now,
      tasks: [],
      taskFlows: [
        flow({
          stateJson: {
            openclawDirective: directive,
            openclawWorkManager: {
              version: 1,
              pool: "personal",
              priority: "P0_USER_DIRECTIVE",
              requestedResources: ["subscription:higgsfield"],
              proofPath: "/tmp/higgsfield-directive.md",
              timeoutMs: 300_000,
              workStatus: "queued",
            },
          },
        }),
      ],
      reliability: reliability("green"),
      mode: "admission",
    });

    expect(snapshot.activeDirective?.directive_id).toBe("directive-higgsfield-starter");
    expect(snapshot.activeDirective?.selected_option).toBe("Starter monthly");
    expect(snapshot.queuedP0P1Work.map((candidate) => candidate.priority)).toContain(
      "P0_USER_DIRECTIVE",
    );
  });

  it("does not report directives from terminal TaskFlows as active", () => {
    const directive = buildOpenClawDirectiveContract({
      directiveId: "directive-higgsfield-starter",
      userRequest: "connect Higgsfield Starter",
      selectedOption: "Starter monthly",
      goal: "verify Higgsfield Starter workspace",
      taskClass: "tool_integration",
      vendor: "Higgsfield",
      allowedActions: ["verify_cli_account"],
      forbiddenActions: ["repeat_purchase"],
      constraints: ["do not repeat purchase"],
      budgetOrPriceCap: 20,
      billingPeriod: "monthly",
      exactPlanName: "Starter",
      successCriteria: ["CLI connected"],
      proofRequired: ["status proof"],
      hardGates: ["WRONG_PLAN"],
      fallbackPolicy: "stop at exact gate",
      stopInstruction: "stop Higgsfield",
      rollbackInstruction: "retire stale directive flow",
      proofPath: "/tmp/higgsfield-directive.md",
      createdFromMessage: "Shamil authorized Starter around $20 monthly",
      nowIso: new Date(now).toISOString(),
      ownerDirectApproval: true,
    });
    directive.current_state = "blocked";

    const snapshot = buildWorkManagerSnapshot({
      nowMs: now,
      tasks: [],
      taskFlows: [
        flow({
          status: "cancelled",
          updatedAt: now,
          stateJson: { openclawDirective: directive },
        }),
      ],
      reliability: reliability("green"),
      mode: "admission",
    });

    expect(snapshot.activeDirective).toBeUndefined();
  });

  it("builds a valid revenue mission contract from a selected user objective", () => {
    const mission = buildOpenClawMissionContract({
      missionId: "mission-revenue-1",
      userRequest: "keep working until it gets a sale",
      selectedOption: "recover missed revenue work",
      nowMs: now,
      wakeTime: "2026-05-20T13:00:00.000Z",
      proofPath: "/tmp/mission-revenue-1.md",
    });

    expect(validateMissionContract(mission)).toEqual({ valid: true, missing: [] });
    expect(mission.priority).toBe("P0_USER_DIRECTIVE");
    expect(mission.objective).toContain("safe revenue");
    expect(mission.success_criteria).toContain("sale/payment_seen");
  });

  it("promotes an existing safe revenue cron for an active mission when no P0/P1 work is active", () => {
    const mission = buildOpenClawMissionContract({
      missionId: "mission-revenue-floor",
      userRequest: "work until sale",
      selectedOption: "run the next safe revenue unit",
      nowMs: now,
      wakeTime: "2026-05-20T13:00:00.000Z",
      proofPath: "/tmp/mission-revenue-floor.md",
    });
    const snapshot = buildWorkManagerSnapshot({
      nowMs: now,
      tasks: [],
      taskFlows: [
        flow({
          goal: "OpenClaw Mission Contract",
          status: "running",
          stateJson: { openclawMission: mission },
        }),
      ],
      reliability: reliability("green"),
      mode: "overnight",
    });
    const walletJob = {
      id: "wallet",
      name: "Titan Wallet Watcher — every 2m",
      enabled: true,
      createdAtMs: now,
      updatedAtMs: now,
      schedule: { kind: "every", everyMs: 120_000 },
      sessionTarget: "isolated",
      wakeMode: "now",
      payload: { kind: "agentTurn", message: "Poll payment wallet" },
      delivery: { mode: "none" },
      state: { nextRunAtMs: now + 120_000 },
    } as CronJob;
    const revenueJob = {
      id: "inbox",
      name: "Native Inbox + DM Triage — revenue buyer signals",
      enabled: true,
      createdAtMs: now,
      updatedAtMs: now,
      schedule: { kind: "every", everyMs: 30 * 60_000 },
      sessionTarget: "isolated",
      wakeMode: "now",
      payload: {
        kind: "agentTurn",
        message: "Check inbound buyer signals, DMs, attribution, and next safe sales unit",
      },
      delivery: { mode: "none" },
      state: { nextRunAtMs: now + 20 * 60_000 },
    } as CronJob;

    expect(
      selectMissionRevenueFloorCronJob({
        snapshot,
        cronJobs: [walletJob, revenueJob],
        nowMs: now,
      }),
    ).toMatchObject({
      decision: "promote",
      jobId: "inbox",
    });
  });

  it("does not promote mission revenue-floor work when P0/P1 work is already queued", () => {
    const mission = buildOpenClawMissionContract({
      missionId: "mission-revenue-busy",
      userRequest: "work until sale",
      selectedOption: "run the next safe revenue unit",
      nowMs: now,
      wakeTime: "2026-05-20T13:00:00.000Z",
      proofPath: "/tmp/mission-revenue-busy.md",
    });
    const snapshot = buildWorkManagerSnapshot({
      nowMs: now,
      tasks: [],
      taskFlows: [
        flow({
          goal: "OpenClaw Mission Contract",
          status: "running",
          stateJson: { openclawMission: mission },
        }),
        flow({
          flowId: "queued-revenue",
          stateJson: {
            openclawWorkManager: {
              version: 1,
              pool: "revenue",
              priority: "P1",
              requestedResources: [],
              proofPath: "/tmp/revenue.md",
              timeoutMs: 300_000,
              workStatus: "queued",
            },
          },
        }),
      ],
      reliability: reliability("green"),
      mode: "overnight",
    });

    expect(
      selectMissionRevenueFloorCronJob({
        snapshot,
        cronJobs: [],
        nowMs: now,
      }),
    ).toMatchObject({
      decision: "none",
      reason: "p0_p1_already_active",
    });
  });

  it("reports blocked proof when queued P0/P1 work cannot dispatch", () => {
    const snapshot = buildWorkManagerSnapshot({
      nowMs: now,
      tasks: [task()],
      taskFlows: [flow()],
      cronJobs: [],
      reliability: reliability("green"),
      mode: "admission",
    });

    expect(selectQueuedWorkForDispatch(snapshot)).toMatchObject({
      decision: "blocked",
      reason: "resource_locked",
      blockedResources: ["browser_profile:titan-ig"],
    });
  });

  it("does not let a blocked queued item hide a later runnable P0/P1 item", () => {
    const snapshot = buildWorkManagerSnapshot({
      nowMs: now,
      tasks: [task()],
      taskFlows: [
        flow({
          flowId: "blocked-revenue",
          stateJson: {
            openclawWorkManager: {
              version: 1,
              pool: "revenue",
              priority: "P0",
              requestedResources: ["browser_profile:titan-ig"],
              proofPath: "/tmp/blocked.md",
              timeoutMs: 300_000,
              workStatus: "queued",
              createdAt: now - 2_000,
            },
          },
        }),
        flow({
          flowId: "runnable-revenue",
          stateJson: {
            openclawWorkManager: {
              version: 1,
              pool: "revenue",
              priority: "P1",
              requestedResources: [],
              proofPath: "/tmp/runnable.md",
              timeoutMs: 300_000,
              workStatus: "queued",
              createdAt: now - 1_000,
            },
          },
        }),
      ],
      cronJobs: [],
      reliability: reliability("green"),
      mode: "admission",
    });

    expect(selectQueuedWorkForDispatch(snapshot)).toMatchObject({
      decision: "dispatch",
      candidate: { workId: "runnable-revenue" },
    });
  });

  it("prefers productive company work over queued status digest work", () => {
    const snapshot = buildWorkManagerSnapshot({
      nowMs: now,
      tasks: [],
      taskFlows: [
        flow({
          flowId: "status-digest",
          goal: "Shamil shift status digest — every 5m",
          stateJson: {
            openclawWorkManager: {
              version: 1,
              workId: "status-digest-work",
              lane: "Shamil shift status digest — every 5m",
              pool: "conversation",
              priority: "P1",
              requestedResources: [],
              expectedOutput: "STATUS DIGEST for Shamil. ≤60 words. NO menus.",
              proofPath: "/tmp/status.md",
              timeoutMs: 300_000,
              workStatus: "queued",
              createdAt: now - 2_000,
            },
          },
        }),
        flow({
          flowId: "research-revenue",
          goal: "Target sourcing revenue recovery",
          stateJson: {
            openclawWorkManager: {
              version: 1,
              workId: "research-revenue-work",
              lane: "Target sourcing revenue recovery",
              pool: "revenue",
              priority: "P1",
              requestedResources: ["vault:PROJECT_STATE"],
              expectedOutput: "Source 10 safe Titan targets and write proof.",
              proofPath: "/tmp/research.md",
              timeoutMs: 300_000,
              workStatus: "queued",
              createdAt: now - 1_000,
            },
          },
        }),
      ],
      cronJobs: [],
      reliability: reliability("green"),
      mode: "admission",
    });

    expect(selectQueuedWorkForDispatch(snapshot)).toMatchObject({
      decision: "dispatch",
      candidate: { workId: "research-revenue-work" },
    });
  });

  it("does not validate queueDrain dispatch without native start proof", () => {
    const snapshot = buildWorkManagerSnapshot({
      nowMs: now,
      tasks: [],
      taskFlows: [
        flow({
          flowId: "runnable-revenue",
          stateJson: {
            openclawWorkManager: {
              version: 1,
              pool: "revenue",
              priority: "P1",
              requestedResources: [],
              proofPath: "/tmp/runnable.md",
              timeoutMs: 300_000,
              workStatus: "queued",
            },
          },
        }),
      ],
      cronJobs: [],
      reliability: reliability("green"),
      mode: "admission",
    });

    expect(validateQueueDrainDispatchProof(snapshot.queueDrain)).toEqual({
      valid: false,
      reason: "dispatch_proof_missing",
    });
  });

  it("records dispatch proof for a promoted native cron job", () => {
    const snapshot = buildWorkManagerSnapshot({
      nowMs: now,
      tasks: [],
      taskFlows: [
        flow({
          flowId: "runnable-revenue",
          stateJson: {
            openclawWorkManager: {
              version: 1,
              pool: "revenue",
              priority: "P1",
              requestedResources: [],
              proofPath: "/tmp/runnable.md",
              timeoutMs: 300_000,
              workStatus: "queued",
            },
          },
        }),
      ],
      cronJobs: [],
      reliability: reliability("green"),
      mode: "admission",
    });

    const proofed = applyQueuedWorkDispatchProof(snapshot.queueDrain, {
      dispatchEffect: "cron_promoted",
      cronJobId: "cron-revenue",
      owner: "cron",
      startedAt: now,
      firstStatusCheck: now + 60_000,
    });

    expect(validateQueueDrainDispatchProof(proofed)).toEqual({ valid: true });
    expect(proofed).toMatchObject({
      decision: "dispatch",
      dispatchEffect: "cron_promoted",
      workId: "runnable-revenue",
      cronJobId: "cron-revenue",
      proofPath: "/tmp/runnable.md",
    });
  });

  it("keeps Shamil status digests in the conversation lane without checkout locks", () => {
    const digestCron = {
      id: "status-digest",
      name: "Shamil shift status digest — every 5m",
      enabled: true,
      createdAtMs: now,
      updatedAtMs: now,
      schedule: { kind: "every", everyMs: 300_000 },
      sessionTarget: "isolated",
      wakeMode: "now",
      payload: {
        kind: "agentTurn",
        message:
          "STATUS DIGEST for Shamil. Read Higgsfield checkout proof and pending_sale_alerts from Titan Wallet Watcher terminalSummary. ≤60 words. NO menus.",
      },
      delivery: { mode: "telegram" },
      state: { runningAtMs: now },
    } as CronJob;

    const snapshot = buildWorkManagerSnapshot({
      nowMs: now,
      tasks: [],
      cronJobs: [digestCron],
      reliability: reliability("green"),
      mode: "admission",
    });

    expect(snapshot.runningByPool.conversation).toBe(1);
    expect(snapshot.locks.map((lock) => lock.resource)).toEqual([]);
  });

  it("does not treat Social Steward forbidden payment text as payment ownership", () => {
    const socialCron = {
      id: "social-steward",
      name: "IG/Titan Social Steward — every 45m",
      enabled: true,
      createdAtMs: now,
      updatedAtMs: now,
      schedule: { kind: "every", everyMs: 2_700_000 },
      sessionTarget: "isolated",
      wakeMode: "now",
      payload: {
        kind: "agentTurn",
        message:
          "OpenClaw Social Steward social_sales_growth tick. Forbidden: No public posts, comments, follows, likes, profile edits, payment/refund/order/checkout/DNS/account actions. Read PROJECT_STATE and titan-ig only.",
      },
      delivery: { mode: "none" },
      state: { runningAtMs: now },
    } as CronJob;

    const snapshot = buildWorkManagerSnapshot({
      nowMs: now,
      tasks: [],
      cronJobs: [socialCron],
      reliability: reliability("green"),
      mode: "admission",
    });

    expect(snapshot.runningByPool.revenue + snapshot.runningByPool.social).toBeGreaterThan(0);
    expect(snapshot.locks.map((lock) => lock.resource)).toEqual(
      expect.arrayContaining(["browser_profile:titan-ig", "social_account:titan-ig"]),
    );
    expect(snapshot.locks.map((lock) => lock.resource)).not.toContain("checkout:titan");
    expect(snapshot.locks.map((lock) => lock.resource)).not.toContain("payment/wallet");
  });

  it("does not treat no-purchase/no-payment instructions as payment ownership", () => {
    const snapshot = buildWorkManagerSnapshot({
      nowMs: now,
      tasks: [
        task({
          taskId: "creative-task",
          label: "titan-higgsfield-creative-sales-20260520-0021",
          task: "Higgsfield creative-to-sales sprint. Produce buyer-facing assets if the subscription is already active. No public posting, no DMs/comments/emails, no extra purchases/upgrades/annual/Plus, no payment/account-security/DNS.",
          status: "running",
        }),
      ],
      reliability: reliability("green"),
      mode: "admission",
    });

    expect(snapshot.runningByPool.revenue).toBe(1);
    expect(snapshot.locks.map((lock) => lock.resource)).not.toContain("payment/wallet");
  });

  it("does not treat account-capability tasks with do-not-buy wording as payment ownership", () => {
    const snapshot = buildWorkManagerSnapshot({
      nowMs: now,
      tasks: [
        task({
          taskId: "api-connect-task",
          label: "higgsfield-api-mcp-account-connect-20260520-0026",
          task: "Higgsfield account capability lane. Connect the API/MCP/CLI route to the account just purchased. Do not buy/upgrade/annual/add-ons, do not change DNS/payment/social. If API key creation is available and free/included, save credentials only through approved secret storage.",
          status: "running",
        }),
      ],
      reliability: reliability("green"),
      mode: "admission",
    });

    expect(snapshot.runningByPool.revenue).toBe(1);
    expect(snapshot.locks.map((lock) => lock.resource)).not.toContain("payment/wallet");
  });

  it("normalizes stale queued status digest metadata instead of preserving old payment locks", () => {
    const snapshot = buildWorkManagerSnapshot({
      nowMs: now,
      tasks: [],
      taskFlows: [
        flow({
          flowId: "stale-status-flow",
          goal: "Shamil shift status digest — every 5m",
          stateJson: {
            openclawWorkManager: {
              version: 1,
              workId: "cron:status:old",
              lane: "Shamil shift status digest — every 5m",
              pool: "revenue",
              priority: "P0",
              requestedResources: ["checkout:titan", "payment/wallet"],
              expectedOutput:
                "STATUS DIGEST for Shamil. Check Higgsfield checkout proof and Sales. ≤60 words. NO menus.",
              proofPath: "/tmp/status.md",
              timeoutMs: 300_000,
              owner: "cron",
              workStatus: "queued",
              createdAt: now - 60_000,
            },
          },
        }),
      ],
      cronJobs: [],
      reliability: reliability("green"),
      mode: "admission",
    });

    expect(snapshot.queuedByPool.conversation).toBe(1);
    expect(snapshot.queuedP0P1Work[0]?.requestedResources).toEqual([]);
    expect(snapshot.queueDrain).toMatchObject({
      decision: "dispatch",
      candidate: { workId: "cron:status:old" },
    });
  });

  it("normalizes stale queued Wallet Watcher metadata to read-only payment sensing", () => {
    const snapshot = buildWorkManagerSnapshot({
      nowMs: now,
      tasks: [],
      taskFlows: [
        flow({
          flowId: "stale-wallet-flow",
          goal: "Titan Wallet Watcher — every 2m",
          stateJson: {
            openclawWorkManager: {
              version: 1,
              workId: "cron:wallet:old",
              lane: "Titan Wallet Watcher — every 2m",
              pool: "revenue",
              priority: "P0",
              requestedResources: ["repo:openclawv2", "payment/wallet"],
              expectedOutput:
                "Run the Titan Wallet Watcher tick OK only; poll payment wallet and exit.",
              proofPath: "/tmp/wallet.md",
              timeoutMs: 300_000,
              owner: "cron",
              workStatus: "queued",
              createdAt: now - 60_000,
            },
          },
        }),
      ],
      cronJobs: [],
      reliability: reliability("green"),
      mode: "admission",
    });

    expect(snapshot.queuedByPool.revenue).toBe(1);
    expect(snapshot.queuedP0P1Work[0]?.requestedResources).toEqual(["payment/wallet:read"]);
  });

  it("ignores stale queued cron TaskFlow metadata when the same cron job is already running", () => {
    const snapshot = buildWorkManagerSnapshot({
      nowMs: now,
      tasks: [
        task({
          taskId: "active-debrief",
          sourceId: "daily-revenue-debrief",
          runId: `cron:daily-revenue-debrief:${now}`,
          label: "Daily Revenue Debrief — 22:19 MDT",
          task: "Daily Revenue Debrief revenue attribution and next actions",
          status: "running",
          createdAt: now,
          startedAt: now,
        }),
      ],
      taskFlows: [
        flow({
          flowId: "old-debrief-queue-row",
          goal: "Daily Revenue Debrief — 22:19 MDT",
          stateJson: {
            openclawWorkManager: {
              version: 1,
              workId: `cron:daily-revenue-debrief:${now - 60_000}`,
              lane: "Daily Revenue Debrief — 22:19 MDT",
              pool: "revenue",
              priority: "P0",
              requestedResources: ["vault:PROJECT_STATE", "payment/wallet:read"],
              expectedOutput: "Daily Revenue Debrief",
              proofPath: "/tmp/debrief.md",
              timeoutMs: 900_000,
              owner: "cron",
              workStatus: "queued",
              createdAt: now - 60_000,
            },
          },
        }),
      ],
      cronJobs: [],
      reliability: reliability("green"),
      mode: "admission",
    });

    expect(snapshot.runningByPool.revenue).toBe(1);
    expect(snapshot.queuedP0P1Work.map((candidate) => candidate.workId)).not.toContain(
      `cron:daily-revenue-debrief:${now - 60_000}`,
    );
    expect(snapshot.queueDrain).toEqual({ decision: "none", reason: "no_queued_p0_p1" });
  });

  it("lets mission revenue floor ignore queued status and wallet sensor work", () => {
    const mission = buildOpenClawMissionContract({
      missionId: "mission-revenue-sensors",
      userRequest: "work until sale",
      selectedOption: "run the next safe revenue unit",
      nowMs: now,
      wakeTime: "2026-05-20T13:00:00.000Z",
      proofPath: "/tmp/mission-revenue-sensors.md",
    });
    const snapshot = buildWorkManagerSnapshot({
      nowMs: now,
      tasks: [],
      taskFlows: [
        flow({
          flowId: "mission-flow",
          goal: "OpenClaw Mission Contract",
          status: "running",
          stateJson: { openclawMission: mission },
        }),
        flow({
          flowId: "queued-status",
          goal: "Shamil shift status digest",
          stateJson: {
            openclawWorkManager: {
              version: 1,
              pool: "conversation",
              priority: "P1",
              requestedResources: [],
              proofPath: "/tmp/status.md",
              timeoutMs: 300_000,
              workStatus: "queued",
              expectedOutput: "status digest",
            },
          },
        }),
        flow({
          flowId: "queued-wallet",
          goal: "Titan Wallet Watcher — every 2m",
          stateJson: {
            openclawWorkManager: {
              version: 1,
              pool: "revenue",
              priority: "P0",
              requestedResources: ["payment/wallet:read"],
              proofPath: "/tmp/wallet.md",
              timeoutMs: 300_000,
              workStatus: "queued",
              expectedOutput: "poll payment wallet; tick OK",
            },
          },
        }),
      ],
      reliability: reliability("green"),
      mode: "overnight",
    });
    const revenueJob = {
      id: "inbox",
      name: "Native Inbox + DM Triage — revenue buyer signals",
      enabled: true,
      createdAtMs: now,
      updatedAtMs: now,
      schedule: { kind: "every", everyMs: 30 * 60_000 },
      sessionTarget: "isolated",
      wakeMode: "now",
      payload: {
        kind: "agentTurn",
        message: "Check inbound buyer signals, DMs, attribution, and next safe sales unit",
      },
      delivery: { mode: "none" },
      state: { nextRunAtMs: now + 20 * 60_000 },
    } as CronJob;

    expect(
      selectMissionRevenueFloorCronJob({
        snapshot,
        cronJobs: [revenueJob],
        nowMs: now,
      }),
    ).toMatchObject({
      decision: "promote",
      jobId: "inbox",
    });
  });

  it("does not promote native health checks or debriefs as mission revenue-floor work", () => {
    const mission = buildOpenClawMissionContract({
      missionId: "mission-revenue-no-report-only",
      userRequest: "work until sale",
      selectedOption: "run the next safe revenue unit",
      nowMs: now,
      wakeTime: "2026-05-20T13:00:00.000Z",
      proofPath: "/tmp/mission-revenue-no-report-only.md",
    });
    const snapshot = buildWorkManagerSnapshot({
      nowMs: now,
      tasks: [],
      taskFlows: [
        flow({
          flowId: "mission-flow",
          goal: "OpenClaw Mission Contract",
          status: "running",
          stateJson: { openclawMission: mission },
        }),
      ],
      reliability: reliability("green"),
      mode: "overnight",
    });
    const healthJob = {
      id: "health",
      name: "OpenClaw Native Health Check - 8am MDT",
      enabled: true,
      createdAtMs: now,
      updatedAtMs: now,
      schedule: { kind: "every", everyMs: 24 * 60 * 60_000 },
      sessionTarget: "isolated",
      wakeMode: "now",
      payload: {
        kind: "agentTurn",
        message:
          "OpenClaw Native Health Check. Read/run only: openclaw config validate, openclaw gateway status, openclaw tasks audit --json, openclaw cron list --json, openclaw sessions --active 180 --json. No source/social/account/payment/DNS/email/browser mutation.",
      },
      delivery: { mode: "none" },
      state: { nextRunAtMs: now + 60_000 },
    } as CronJob;
    const debriefJob = {
      id: "revenue-debrief",
      name: "Daily Revenue Debrief — 22:19 MDT",
      enabled: true,
      createdAtMs: now,
      updatedAtMs: now,
      schedule: { kind: "every", everyMs: 24 * 60 * 60_000 },
      sessionTarget: "isolated",
      wakeMode: "now",
      payload: {
        kind: "agentTurn",
        message: "Daily Revenue Debrief. Summarize sales, attribution, and next actions.",
      },
      delivery: { mode: "none" },
      state: { nextRunAtMs: now + 60_000 },
    } as CronJob;
    const contentJob = {
      id: "content",
      name: "Content Factory — Titan creative supply",
      enabled: true,
      createdAtMs: now,
      updatedAtMs: now,
      schedule: { kind: "every", everyMs: 4 * 60 * 60_000 },
      sessionTarget: "isolated",
      wakeMode: "now",
      payload: {
        kind: "agentTurn",
        message: "Create Titan buyer content packets for Instagram and target sourcing.",
      },
      delivery: { mode: "none" },
      state: { nextRunAtMs: now + 60_000 },
    } as CronJob;

    expect(
      selectMissionRevenueFloorCronJob({
        snapshot,
        cronJobs: [healthJob, debriefJob, contentJob],
        nowMs: now,
      }),
    ).toMatchObject({
      decision: "promote",
      jobId: "content",
    });
  });

  it("keeps native health checks in maintenance without payment or repo locks", () => {
    const healthCron = {
      id: "health",
      name: "OpenClaw Native Health Check - 8am MDT",
      enabled: true,
      createdAtMs: now,
      updatedAtMs: now,
      schedule: { kind: "every", everyMs: 24 * 60 * 60_000 },
      sessionTarget: "isolated",
      wakeMode: "now",
      payload: {
        kind: "agentTurn",
        message:
          "OpenClaw Native Health Check. Read/run only: openclaw config validate, openclaw gateway status, openclaw tasks audit --json, openclaw cron list --json, openclaw sessions --active 180 --json. No source/social/account/payment/DNS/email/browser mutation.",
      },
      delivery: { mode: "none" },
      state: { runningAtMs: now },
    } as CronJob;

    const snapshot = buildWorkManagerSnapshot({
      nowMs: now,
      tasks: [],
      cronJobs: [healthCron],
      reliability: reliability("green"),
      mode: "admission",
    });

    expect(snapshot.runningByPool.maintenance).toBe(1);
    expect(snapshot.runningByPool.revenue).toBe(0);
    expect(snapshot.locks.map((lock) => lock.resource)).toEqual([]);
  });

  it("selects at most one bounded liveness handoff from completed revenue work", () => {
    const snapshot = buildWorkManagerSnapshot({
      nowMs: now,
      tasks: [],
      taskFlows: [],
      cronJobs: [],
      reliability: reliability("green"),
      mode: "admission",
    });
    const completed: WorkManagerCandidate = {
      workId: "done",
      lane: "Revenue worker",
      pool: "revenue",
      priority: "P1",
      requestedResources: [],
      expectedOutput: "shipped",
      proofPath: "/tmp/done.md",
      timeoutMs: 300_000,
      owner: "cron:revenue",
      status: "succeeded",
      createdAt: now - 1_000,
      handoffDepth: 0,
    };
    const proposed: WorkManagerCandidate = {
      workId: "next",
      lane: "Attribution missing-proof action",
      pool: "revenue",
      priority: "P1",
      requestedResources: [],
      expectedOutput: "close attribution proof",
      proofPath: "/tmp/next.md",
      timeoutMs: 300_000,
      owner: "work-manager",
      status: "queued",
      createdAt: now,
    };

    expect(
      evaluateLivenessHandoff({
        snapshot,
        completedWork: completed,
        proposedNextWork: proposed,
      }),
    ).toMatchObject({
      decision: "selected",
      candidate: {
        workId: "next",
        parentWorkId: "done",
        handoffDepth: 1,
      },
    });

    expect(
      evaluateLivenessHandoff({
        snapshot,
        completedWork: { ...completed, handoffDepth: 1 },
        proposedNextWork: proposed,
      }),
    ).toMatchObject({
      decision: "none_available",
      reason: "handoff_depth_exceeded",
    });
  });

  it("builds compact missed-work recovery rows without claiming unshipped work", () => {
    const recovery: WorkManagerCandidate = {
      workId: "recover-checkout",
      lane: "Checkout CRO check",
      pool: "revenue",
      priority: "P1",
      requestedResources: ["checkout:titan"],
      expectedOutput: "checkout-copy",
      proofPath: "/tmp/checkout.md",
      timeoutMs: 300_000,
      owner: "revenue",
      status: "queued",
      createdAt: now,
    };

    expect(
      buildMissedWorkLedger({
        promisedWork: ["wallet-watch", "checkout-copy"],
        shippedWork: ["wallet-watch"],
        recoveryCandidates: [recovery],
        defaultReason: "mission_under_delivered",
      }),
    ).toEqual([
      {
        promised_work: "checkout-copy",
        shipped_work: "not_shipped",
        missed_work: "checkout-copy",
        reason: "mission_under_delivered",
        recover_now: true,
        next_safe_recovery_unit: "Checkout CRO check",
        owner_lane: "revenue",
        proof_path: "/tmp/checkout.md",
      },
    ]);
  });

  it("ignores expired locks from terminal owner work", () => {
    const snapshot = buildWorkManagerSnapshot({
      nowMs: now,
      tasks: [
        task({
          status: "succeeded",
          endedAt: now - 1_000,
          label: "Finished Titan work",
        }),
      ],
      taskFlows: [
        flow({
          status: "succeeded",
          endedAt: now - 1_000,
        }),
      ],
      cronJobs: [],
      reliability: reliability("green"),
      mode: "admission",
    });

    expect(snapshot.locks).toEqual([]);
  });

  it("bounds overnight handoff chains at one nested handoff", () => {
    const snapshot = buildWorkManagerSnapshot({
      nowMs: now,
      tasks: [],
      taskFlows: [
        flow({
          stateJson: {
            openclawWorkManager: {
              version: 1,
              pool: "revenue",
              priority: "P1",
              requestedResources: [],
              proofPath: "/tmp/proof.md",
              timeoutMs: 300_000,
              leaseUntil: now + 100_000,
              parentWorkId: "parent",
              handoffDepth: 1,
              workStatus: "queued",
            },
          },
        }),
      ],
      cronJobs: [],
      reliability: reliability("green"),
      mode: "overnight",
    });
    const candidate: WorkManagerCandidate = {
      workId: "child",
      lane: "next revenue unit",
      pool: "revenue",
      priority: "P1",
      requestedResources: [],
      expectedOutput: "next safe unit",
      proofPath: "/tmp/child.md",
      timeoutMs: 300_000,
      owner: "handoff",
      createdAt: now,
      parentWorkId: "flow-1",
      handoffDepth: 2,
    };

    expect(evaluateWorkAdmission(snapshot, candidate)).toMatchObject({
      decision: "block",
      reason: "handoff_depth_exceeded",
    });
  });

  it("summarizes status metrics for fast operator output", () => {
    const snapshot = buildWorkManagerSnapshot({
      nowMs: now,
      tasks: [task()],
      taskFlows: [flow()],
      cronJobs: [],
      reliability: reliability("yellow"),
      mode: "shadow",
    });

    expect(summarizeWorkManagerStatus(snapshot)).toMatchObject({
      mode: "shadow",
      status: "yellow",
      runningByPool: expect.objectContaining({ social: 1 }),
      queuedByPool: expect.objectContaining({ revenue: 1 }),
      blockedLocks: expect.any(Array),
      p0p1RevenueWork: expect.any(Array),
    });
  });
});
