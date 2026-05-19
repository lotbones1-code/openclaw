import { describe, expect, it } from "vitest";
import type { ReliabilityHealthSnapshot } from "../reliability/supervisor.types.js";
import type { TaskFlowRecord } from "../tasks/task-flow-registry.types.js";
import type { TaskRecord } from "../tasks/task-registry.types.js";
import {
  buildWorkManagerSnapshot,
  evaluateWorkAdmission,
  rankWorkCandidates,
  summarizeWorkManagerStatus,
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
