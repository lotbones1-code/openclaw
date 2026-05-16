import { afterEach, describe, expect, it } from "vitest";
import { withOpenClawTestState } from "../test-utils/openclaw-test-state.js";
import {
  appendPolicyLockAudit,
  consumeMatchingPolicyUnlock,
  createPolicyUnlock,
  getPolicyLock,
  listPolicyLockAudit,
  listPolicyLocks,
  resetPolicyLockRegistryForTests,
  setPolicyLock,
} from "./policy-lock-registry.js";

describe("policy-lock registry", () => {
  afterEach(() => {
    resetPolicyLockRegistryForTests();
  });

  it("creates default sensitive-action locks as locked", async () => {
    await withOpenClawTestState(
      {
        label: "policy-lock-defaults",
        applyEnv: true,
      },
      async () => {
        const locks = listPolicyLocks();

        expect(locks.some((lock) => lock.lockId === "b2b:send" && lock.state === "LOCKED")).toBe(
          true,
        );
        expect(
          locks.some((lock) => lock.lockId === "payment:refund" && lock.state === "LOCKED"),
        ).toBe(true);
      },
    );
  });

  it("records single-use unlocks and consumes a matching unlock only once", async () => {
    await withOpenClawTestState(
      {
        label: "policy-lock-single-use",
        applyEnv: true,
      },
      async () => {
        setPolicyLock({
          lockId: "b2b:send",
          state: "LOCKED",
          now: 100,
          source: "test",
        });
        createPolicyUnlock({
          unlockId: "unlock-1",
          lockId: "b2b:send",
          taskId: "task-1",
          lane: "b2b_send",
          action: "b2b_send",
          account: "gmail",
          targetClass: "clinic",
          approvalText: "send exactly one clinic email now",
          proofPath: "/tmp/proof.md",
          stopInstruction: "stop b2b",
          rollbackInstruction: "do not retry",
          now: 200,
          expiresAt: 1_000,
          source: "test",
        });

        const first = consumeMatchingPolicyUnlock({
          lockId: "b2b:send",
          taskId: "task-1",
          lane: "b2b_send",
          action: "b2b_send",
          account: "gmail",
          targetClass: "clinic",
          now: 250,
        });
        const second = consumeMatchingPolicyUnlock({
          lockId: "b2b:send",
          taskId: "task-1",
          lane: "b2b_send",
          action: "b2b_send",
          account: "gmail",
          targetClass: "clinic",
          now: 260,
        });

        expect(first).toMatchObject({
          unlockId: "unlock-1",
          state: "USED",
          usedAt: 250,
        });
        expect(second).toBeNull();
        expect(getPolicyLock("b2b:send")).toMatchObject({ state: "LOCKED" });
        expect(listPolicyLockAudit().some((row) => row.reasonCode === "POLICY_UNLOCK_USED")).toBe(
          true,
        );
      },
    );
  });

  it("persists deny audit records", async () => {
    await withOpenClawTestState(
      {
        label: "policy-lock-audit",
        applyEnv: true,
      },
      async () => {
        appendPolicyLockAudit({
          auditId: "audit-1",
          timestamp: 300,
          lockId: "b2b:send",
          taskId: "task-1",
          lane: "b2b",
          action: "send",
          decision: "DENIED",
          reasonCode: "SEND_LOCKED",
        });

        expect(listPolicyLockAudit()[0]).toMatchObject({
          auditId: "audit-1",
          decision: "DENIED",
          reasonCode: "SEND_LOCKED",
        });
      },
    );
  });
});
