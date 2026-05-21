import { describe, expect, it } from "vitest";
import {
  buildCompactSuccessPatternNote,
  buildCapabilityStatusRecord,
  buildOpenClawDirectiveContract,
  markCapabilityFirstUseExactGate,
  markCapabilityUsed,
  resolveAuthorityPrecedence,
  splitVagueWorkerObjective,
  validateDirectiveCapsule,
  validateFinalGateCode,
  validateOpenClawDirectiveContract,
  validateProofCounters,
  validateSelfBuilderCloseout,
  verifyPreAction,
} from "./execution-kernel.js";

const starterDirective = buildOpenClawDirectiveContract({
  directiveId: "directive-higgsfield-starter",
  userRequest: "buy the starter subscription the $20 one",
  selectedOption: "Higgsfield Starter monthly",
  goal: "Subscribe to Higgsfield Starter monthly and connect the workspace",
  taskClass: "saas_subscription",
  vendor: "Higgsfield",
  account: "titanpeptidelab@proton.me",
  workspace: "Titan Peptide Lab",
  allowedActions: ["select_plan", "submit_checkout", "verify_workspace", "connect_cli"],
  forbiddenActions: ["select_pro", "annual_billing", "addon_purchase", "repeat_purchase"],
  constraints: ["starter_monthly_only"],
  budgetOrPriceCap: 20,
  billingPeriod: "monthly",
  exactPlanName: "Starter",
  successCriteria: ["Starter monthly active", "workspace verified"],
  proofRequired: ["checkout_success_url", "workspace_status"],
  hardGates: ["MISSING_CVV", "MISSING_2FA", "CAPTCHA_REQUIRED"],
  fallbackPolicy: "closest_safe_setup_step",
  stopInstruction: "stop Higgsfield",
  rollbackInstruction: "cancel or downgrade only with explicit Shamil approval",
  proofPath: "/tmp/higgsfield.md",
  createdFromMessage: "telegram:6032869886:2026-05-20T00:01:00-06:00",
  nowIso: "2026-05-20T06:01:00.000Z",
  ownerDirectApproval: true,
});

describe("execution kernel directive contract", () => {
  it("validates durable directives with selected option and success criteria", () => {
    expect(validateOpenClawDirectiveContract(starterDirective)).toEqual({
      valid: true,
      missing: [],
    });
  });

  it("treats latest explicit Shamil instruction as highest authority", () => {
    expect(
      resolveAuthorityPrecedence({
        latestExplicitInstruction: "buy Starter monthly only",
        activeDirective: starterDirective,
        runtimeRule: "payment category is gated",
        skillGuidance: "ask again",
        modelJudgment: "try Pro annual",
      }),
    ).toMatchObject({
      source: "latest_explicit_shamil_instruction",
      instruction: "buy Starter monthly only",
    });
  });

  it("requires directive capsules for sensitive workers", () => {
    expect(
      validateDirectiveCapsule({
        directive_id: starterDirective.directive_id,
        goal: starterDirective.goal,
        selected_option: starterDirective.selected_option,
        allowed_actions: starterDirective.allowed_actions,
        forbidden_actions: starterDirective.forbidden_actions,
        constraints: starterDirective.constraints,
        exact_gates: starterDirective.hard_gates,
        success_criteria: starterDirective.success_criteria,
        proof_path: starterDirective.proof_path,
        current_checkpoint: "pricing page observed",
      }),
    ).toEqual({ valid: true, missing: [] });

    expect(validateDirectiveCapsule(undefined)).toMatchObject({
      valid: false,
      missing: expect.arrayContaining(["directive_id", "selected_option"]),
    });
  });
});

describe("execution kernel typed gates and pre-action verifier", () => {
  it.each(["human_gate", "payment_gate", "subscription_blocked", "manual_required", "ask_user"])(
    "rejects broad final gate label %s",
    (gate) => {
      expect(validateFinalGateCode(gate)).toMatchObject({
        valid: false,
        code: "BROAD_GATE_LABEL",
      });
    },
  );

  it("accepts concrete typed final gates for exact blockers", () => {
    expect(validateFinalGateCode("MISSING_CVV")).toEqual({ valid: true, code: "MISSING_CVV" });
    expect(validateFinalGateCode("INSUFFICIENT_CREDITS")).toEqual({
      valid: true,
      code: "INSUFFICIENT_CREDITS",
    });
  });

  it("allows the exact selected Starter monthly checkout action", () => {
    expect(
      verifyPreAction({
        directive: starterDirective,
        candidateAction: {
          action: "submit_checkout",
          vendor: "Higgsfield",
          visiblePlan: "Starter",
          visiblePrice: 15,
          billingPeriod: "monthly",
          selectedAddOns: [],
          account: "titanpeptidelab@proton.me",
          workspace: "Titan Peptide Lab",
          browserProfile: "higgsfield",
          browserProfileProven: true,
          predictedEffect: "Subscribe to Higgsfield Starter - monthly",
        },
      }),
    ).toEqual({ decision: "allow", reason: "verified" });
  });

  it("blocks wrong plan, annual billing, add-ons, and price cap drift with typed gates", () => {
    expect(
      verifyPreAction({
        directive: starterDirective,
        candidateAction: {
          action: "submit_checkout",
          vendor: "Higgsfield",
          visiblePlan: "Pro",
          visiblePrice: 49,
          billingPeriod: "annual",
          selectedAddOns: ["extra credits"],
          account: "titanpeptidelab@proton.me",
          workspace: "Titan Peptide Lab",
          browserProfile: "higgsfield",
          browserProfileProven: true,
          predictedEffect: "Subscribe to Higgsfield Pro annual with credits",
        },
      }),
    ).toMatchObject({
      decision: "block",
      gate: "WRONG_PLAN",
    });
  });
});

describe("execution kernel worker closeout", () => {
  it("does not let prep packets count as shipped execution", () => {
    expect(
      validateProofCounters({
        outcome: "SHIPPED_UNIT",
        counters: {
          sent_count: 0,
          public_actions_count: 0,
          rendered_assets_count: 0,
          sales_count: 0,
          payments_seen: 0,
          accounts_created: 0,
          subscriptions_completed: 0,
          checkout_attempts: 0,
          buyer_signals: 0,
          drafts_created: 2,
          prep_packets_created: 3,
        },
      }),
    ).toMatchObject({
      valid: false,
      code: "PREP_ONLY_NOT_SHIPPED",
    });
  });

  it("splits vague giant missions into bounded worker packets", () => {
    expect(splitVagueWorkerObjective("make Titan sales and fix everything")).toMatchObject({
      acceptedAsSingleWorker: false,
      packets: expect.arrayContaining([
        expect.objectContaining({ objective: "check wallet/orders" }),
        expect.objectContaining({ objective: "check buyer DMs" }),
        expect.objectContaining({ objective: "source 10 targets" }),
      ]),
    });
  });

  it("requires self-builder work to have source discipline and native proof", () => {
    expect(
      validateSelfBuilderCloseout({
        directiveId: "directive-self-builder",
        defectFixed: "selected option was not enforced",
        sourceAuthority: "src/execution-kernel",
        nativeSourceChange: true,
        regressionTest: "execution-kernel selected option regression",
        testsPassed: true,
        stagedSecretJunkScan: true,
        commitHash: "abc1234",
        reportPath: "/tmp/self-builder.md",
        rollbackOrDisablePath: "executionKernel.enabled=false",
        learningFooter: {
          lessons_applied: ["none_relevant"],
          learning_event: "recorded",
          next_action: "completed",
        },
        addedNonNativeSurfaces: [],
      }),
    ).toEqual({ valid: true, missing: [] });

    expect(
      validateSelfBuilderCloseout({
        directiveId: "directive-self-builder",
        defectFixed: "selected option was not enforced",
        sourceAuthority: "src/execution-kernel",
        nativeSourceChange: true,
        testsPassed: true,
        stagedSecretJunkScan: true,
        reportPath: "/tmp/self-builder.md",
        rollbackOrDisablePath: "executionKernel.enabled=false",
        learningFooter: {
          lessons_applied: ["none_relevant"],
          learning_event: "recorded",
          next_action: "completed",
        },
        addedNonNativeSurfaces: ["wrapper_script"],
      }),
    ).toMatchObject({
      valid: false,
      missing: expect.arrayContaining(["regressionTest_or_exactTestGate"]),
      blockedSurfaces: ["wrapper_script"],
    });

    expect(
      validateSelfBuilderCloseout({
        directiveId: "directive-self-builder",
        defectFixed: "old cron referenced non-native openclawd env",
        sourceAuthority: "native compliance",
        nativeSourceChange: true,
        regressionTest: "native compliance old openclawd cron residue",
        testsPassed: true,
        stagedSecretJunkScan: true,
        commitHash: "abc1234",
        reportPath: "/tmp/self-builder.md",
        rollbackOrDisablePath: "native openclaw cron disable <job>",
        learningFooter: {
          lessons_applied: ["none_relevant"],
          learning_event: "recorded",
          next_action: "completed",
        },
        addedNonNativeSurfaces: ["openclawd_env_cron_reference"],
      }),
    ).toMatchObject({
      valid: false,
      blockedSurfaces: ["openclawd_env_cron_reference"],
    });
  });

  it("writes compact non-secret success pattern notes", () => {
    const note = buildCompactSuccessPatternNote({
      task_class: "tool_setup",
      service_vendor: "Higgsfield",
      what_worked: "device flow login in OpenClaw higgsfield browser profile",
      exact_gates_encountered: ["MISSING_CVV"],
      proof_path: "/tmp/higgsfield.md",
      next_time_shortcut: "reuse higgsfield profile and run account status first",
      what_not_to_repeat: "do not retry checkout after subscription proof",
      secrets: {
        privateField: "redacted-example-value",
        billingField: "000",
      },
    });

    expect(JSON.stringify(note)).not.toContain("redacted-example-value");
    expect(JSON.stringify(note)).not.toContain("000");
    expect(note.omitted_secret_fields).toEqual(["privateField", "billingField"]);
  });
});

describe("execution kernel capability status", () => {
  it("does not count a connected tool as used before first-use proof", () => {
    const connected = buildCapabilityStatusRecord({
      capabilityId: "higgsfield:titan",
      service: "Higgsfield",
      status: "connected",
      proofPath: "/tmp/higgsfield-connected.md",
      lastVerifiedAt: "2026-05-21T10:00:00.000Z",
      hardGates: [],
    });

    expect(connected.firstUseStatus).toBe("not_used");
    expect(connected.status).toBe("connected");

    const used = markCapabilityUsed(connected, {
      proofPath: "/tmp/higgsfield-first-use.md",
      usedAt: "2026-05-21T10:05:00.000Z",
    });

    expect(used.firstUseStatus).toBe("used");
    expect(used.proofPath).toBe("/tmp/higgsfield-first-use.md");
  });

  it("records exact typed gate for gated capability first use", () => {
    const connected = buildCapabilityStatusRecord({
      capabilityId: "higgsfield:titan",
      service: "Higgsfield",
      status: "connected",
      proofPath: "/tmp/higgsfield-connected.md",
      lastVerifiedAt: "2026-05-21T10:00:00.000Z",
      hardGates: [],
    });

    const gated = markCapabilityFirstUseExactGate(connected, {
      gate: "INSUFFICIENT_CREDITS",
      proofPath: "/tmp/higgsfield-gated.md",
      gatedAt: "2026-05-21T10:05:00.000Z",
    });

    expect(gated.status).toBe("gated");
    expect(gated.firstUseStatus).toBe("exact_gate");
    expect(gated.hardGates).toContain("INSUFFICIENT_CREDITS");
  });
});
