import { afterEach, describe, expect, it, vi } from "vitest";
import { withOpenClawTestState } from "../test-utils/openclaw-test-state.js";
import {
  acknowledgeTaskControl,
  containUnacknowledgedStopControls,
  findActiveStopControl,
  listTaskControlRecords,
  requestTaskControlStop,
  resetTaskControlRegistryForTests,
} from "./task-control-registry.js";

describe("task-control registry", () => {
  afterEach(() => {
    resetTaskControlRegistryForTests();
  });

  it("persists stop records and acknowledges them by control id", async () => {
    await withOpenClawTestState(
      {
        label: "task-control-ack",
        applyEnv: true,
      },
      async () => {
        const record = requestTaskControlStop({
          controlId: "control-stop-1",
          sessionKey: "agent:main:telegram:personal",
          source: "test",
          reason: "stop-command",
          now: 100,
        });

        expect(record).toMatchObject({
          controlId: "control-stop-1",
          scope: "session:agent:main:telegram:personal",
          state: "requested",
        });
        expect(
          findActiveStopControl({ sessionKey: "agent:main:telegram:personal" })?.controlId,
        ).toBe("control-stop-1");

        const acknowledged = acknowledgeTaskControl({
          controlId: "control-stop-1",
          now: 125,
        });

        expect(acknowledged).toMatchObject({
          controlId: "control-stop-1",
          state: "acknowledged",
          acknowledgedAt: 125,
          detailCode: "stopped_by_user",
        });
        expect(listTaskControlRecords({ activeOnly: true })).toEqual([]);
      },
    );
  });

  it("contains unacknowledged stop records by cancelling only their scope", async () => {
    await withOpenClawTestState(
      {
        label: "task-control-containment",
        applyEnv: true,
      },
      async () => {
        const cancelScope = vi.fn();

        requestTaskControlStop({
          controlId: "control-stop-2",
          scope: "session:agent:main:telegram:personal",
          sessionKey: "agent:main:telegram:personal",
          source: "test",
          now: 100,
        });

        const contained = containUnacknowledgedStopControls({
          now: 10_101,
          unacknowledgedMs: 5_000,
          cancelScope,
        });

        expect(cancelScope).toHaveBeenCalledWith("session:agent:main:telegram:personal");
        expect(contained).toHaveLength(1);
        expect(contained[0]).toMatchObject({
          controlId: "control-stop-2",
          state: "contained",
          detailCode: "STOP_UNACKNOWLEDGED",
        });
      },
    );
  });

  it("does not acknowledge sensitive stopped work with broad resume detail codes", async () => {
    await withOpenClawTestState(
      {
        label: "task-control-sensitive-resume-deny",
        applyEnv: true,
      },
      async () => {
        requestTaskControlStop({
          controlId: "control-b2b-stop",
          scope: "task:b2b-email-sends-20260515",
          taskId: "b2b-email-sends-20260515",
          source: "test",
          reason: "B2B Gmail SMTP send worker stopped",
          now: 100,
        });

        const denied = acknowledgeTaskControl({
          controlId: "control-b2b-stop",
          now: 150,
          detailCode: "explicit_resume_current_boss_directive_36090",
        });

        expect(denied).toMatchObject({
          controlId: "control-b2b-stop",
          state: "requested",
          detailCode: "SENSITIVE_RESUME_DENIED",
        });
        expect(listTaskControlRecords({ activeOnly: true })).toHaveLength(1);
      },
    );
  });
});
