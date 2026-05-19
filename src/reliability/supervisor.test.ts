import { beforeEach, describe, expect, it } from "vitest";
import {
  buildReliabilityHealthSnapshot,
  recordReliabilityEvent,
  resetReliabilitySupervisorForTests,
} from "./supervisor.js";

describe("autonomic reliability supervisor", () => {
  beforeEach(() => {
    resetReliabilitySupervisorForTests();
  });

  it("routes stale running tasks to native task maintenance", () => {
    const snapshot = buildReliabilityHealthSnapshot({
      nowMs: 10 * 60_000,
      taskAudit: {
        total: 1,
        errors: 1,
        warnings: 0,
        byCode: { stale_running: 1 },
      },
      taskMaintenancePreview: {
        reconciled: 1,
        recovered: 0,
        cleanupStamped: 0,
        pruned: 0,
      },
    });

    expect(snapshot.status).toBe("red");
    expect(snapshot.subsystems.tasks.status).toBe("red");
    expect(snapshot.actions).toContainEqual(
      expect.objectContaining({
        kind: "task_maintenance",
        subsystem: "tasks",
        critical: true,
      }),
    );
  });

  it("routes dirty cron running markers to native cron reconciliation", () => {
    const runningAtMs = 1_000;
    const snapshot = buildReliabilityHealthSnapshot({
      nowMs: runningAtMs + 30 * 60_000,
      cronJobs: [
        {
          id: "cron-a",
          name: "Cron A",
          enabled: true,
          runningAtMs,
          timeoutMs: 10 * 60_000,
          consecutiveErrors: 0,
        },
      ],
      taskRuns: [
        {
          runId: `cron:cron-a:${runningAtMs}`,
          status: "succeeded",
        },
      ],
    });

    expect(snapshot.subsystems.cron.status).toBe("yellow");
    expect(snapshot.actions).toContainEqual(
      expect.objectContaining({
        kind: "cron_reconcile",
        subsystem: "cron",
        subject: "cron-a",
      }),
    );
  });

  it("does not keep health yellow for old lost task history that maintenance cannot recover", () => {
    const snapshot = buildReliabilityHealthSnapshot({
      nowMs: 10 * 60_000,
      taskAudit: {
        total: 2,
        errors: 0,
        warnings: 2,
        byCode: { lost: 2 },
      },
      taskMaintenancePreview: {
        reconciled: 0,
        recovered: 0,
        cleanupStamped: 0,
        pruned: 0,
      },
    });

    expect(snapshot.subsystems.tasks.status).toBe("green");
    expect(snapshot.actions).not.toContainEqual(
      expect.objectContaining({
        kind: "task_maintenance",
        subsystem: "tasks",
      }),
    );
  });

  it("turns repeated MCP startup timeouts into a degraded circuit breaker", () => {
    recordReliabilityEvent({
      subsystem: "mcp",
      code: "mcp_startup_timeout",
      severity: "warn",
      subject: "browsermcp",
      message: "bundle-mcp: failed to start server browsermcp: timed out",
      recoverable: true,
      createdAt: 1_000,
    });
    recordReliabilityEvent({
      subsystem: "mcp",
      code: "mcp_startup_timeout",
      severity: "warn",
      subject: "browsermcp",
      message: "bundle-mcp: failed to start server browsermcp: timed out",
      recoverable: true,
      createdAt: 2_000,
    });
    recordReliabilityEvent({
      subsystem: "mcp",
      code: "mcp_startup_timeout",
      severity: "warn",
      subject: "browsermcp",
      message: "bundle-mcp: failed to start server browsermcp: timed out",
      recoverable: true,
      createdAt: 3_000,
    });

    const snapshot = buildReliabilityHealthSnapshot({ nowMs: 4_000 });

    expect(snapshot.subsystems.mcp.status).toBe("yellow");
    expect(snapshot.actions).toContainEqual(
      expect.objectContaining({
        kind: "mcp_circuit_break",
        subsystem: "mcp",
        subject: "browsermcp",
      }),
    );
  });

  it("routes billing/rate primary failures to configured fallback before treating the run as dead", () => {
    recordReliabilityEvent({
      subsystem: "models",
      code: "model_fallback_decision",
      severity: "warn",
      subject: "anthropic/claude-opus-4-7",
      message: "primary model billing cooldown; next=openai-codex/gpt-5.5",
      recoverable: true,
      createdAt: 1_000,
      metadata: {
        decision: "skip_candidate",
        reason: "billing",
        nextCandidate: "openai-codex/gpt-5.5",
      },
    });

    const snapshot = buildReliabilityHealthSnapshot({ nowMs: 2_000 });

    expect(snapshot.subsystems.models.status).toBe("yellow");
    expect(snapshot.actions).toContainEqual(
      expect.objectContaining({
        kind: "model_fallback_route",
        subsystem: "models",
        subject: "anthropic/claude-opus-4-7",
        critical: false,
      }),
    );
  });

  it("alerts on critical delivery failures even when normal skips stay quiet", () => {
    const snapshot = buildReliabilityHealthSnapshot({
      nowMs: 2_000,
      delivery: {
        pending: 4,
        failed: 1,
        permanentFailures: 0,
      },
      events: [
        {
          id: "evt-normal",
          subsystem: "delivery",
          code: "NO_SAFE_UNIT",
          severity: "info",
          message: "no fresh target",
          recoverable: false,
          createdAt: 1_000,
          quiet: true,
        },
        {
          id: "evt-critical",
          subsystem: "delivery",
          code: "critical_delivery_failed",
          severity: "error",
          message: "Telegram delivery target missing for critical failure",
          recoverable: true,
          createdAt: 1_500,
        },
      ],
    });

    expect(snapshot.subsystems.delivery.status).toBe("red");
    expect(snapshot.actions).toContainEqual(
      expect.objectContaining({
        kind: "critical_alert",
        subsystem: "delivery",
        critical: true,
      }),
    );
    expect(snapshot.actions).not.toContainEqual(
      expect.objectContaining({
        reason: expect.stringContaining("no fresh target"),
      }),
    );
  });
});
