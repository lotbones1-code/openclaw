import { describe, expect, it } from "vitest";
import { buildOpenClawDirectiveContract } from "../execution-kernel/execution-kernel.js";
import { evaluateBrowserSurfaceGuard } from "./browser-surface-guard.js";

const directive = buildOpenClawDirectiveContract({
  directiveId: "directive-higgsfield-starter",
  userRequest: "buy the starter subscription the $20 one",
  selectedOption: "Higgsfield Starter monthly",
  goal: "Subscribe to Higgsfield Starter monthly",
  taskClass: "saas_subscription",
  vendor: "Higgsfield",
  account: "titanpeptidelab@proton.me",
  workspace: "Titan Peptide Lab",
  allowedActions: ["submit_checkout"],
  forbiddenActions: ["select_pro", "annual_billing", "addon_purchase"],
  constraints: ["starter_monthly_only"],
  budgetOrPriceCap: 20,
  billingPeriod: "monthly",
  exactPlanName: "Starter",
  successCriteria: ["Starter monthly active"],
  proofRequired: ["checkout_success_url"],
  hardGates: ["MISSING_CVV", "MISSING_2FA"],
  fallbackPolicy: "closest_safe_setup_step",
  stopInstruction: "stop Higgsfield",
  rollbackInstruction: "cancel only with explicit approval",
  proofPath: "/tmp/higgsfield.md",
  createdFromMessage: "telegram:6032869886",
  nowIso: "2026-05-20T06:01:00.000Z",
  ownerDirectApproval: true,
});

describe("browser surface guard execution-kernel verifier", () => {
  it("allows an owner-approved exact Starter monthly checkout action", () => {
    expect(
      evaluateBrowserSurfaceGuard({
        toolName: "browser.click",
        toolParams: {
          browserProfile: "higgsfield",
          cdpTargetId: "target-1",
          url: "https://higgsfield.ai/pricing?plan=starter",
          openclawDirective: directive,
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
        },
      }),
    ).toEqual({ blocked: false });
  });

  it("blocks wrong plan drift with exact typed gate instead of broad human gate", () => {
    expect(
      evaluateBrowserSurfaceGuard({
        toolName: "browser.click",
        toolParams: {
          browserProfile: "higgsfield",
          cdpTargetId: "target-1",
          url: "https://higgsfield.ai/checkout?plan=pro",
          openclawDirective: directive,
          candidateAction: {
            action: "submit_checkout",
            vendor: "Higgsfield",
            visiblePlan: "Pro",
            visiblePrice: 49,
            billingPeriod: "monthly",
            selectedAddOns: [],
            account: "titanpeptidelab@proton.me",
            workspace: "Titan Peptide Lab",
            browserProfile: "higgsfield",
            browserProfileProven: true,
            predictedEffect: "Subscribe to Higgsfield Pro - monthly",
          },
        },
      }),
    ).toMatchObject({
      blocked: true,
      code: "WRONG_PLAN",
      reason: expect.stringContaining("WRONG_PLAN"),
    });
  });
});
