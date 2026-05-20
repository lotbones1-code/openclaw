import {
  AUTHORITY_PRECEDENCE,
  BROAD_GATE_LABELS,
  TYPED_GATE_CODE_SET,
  TYPED_GATE_CODES,
  type AuthoritySource,
  type OpenClawDirectiveContract,
  type PreActionCandidate,
  type ProofCounters,
  type TypedGateCode,
} from "./execution-kernel.types.js";

export { AUTHORITY_PRECEDENCE, TYPED_GATE_CODES };
export {
  buildCompactSuccessPatternNote,
  validateSelfBuilderCloseout,
} from "./execution-kernel-self-builder.js";
export type {
  AuthoritySource,
  DirectiveCapsule,
  LearningFooter,
  OpenClawDirectiveContract,
  PreActionCandidate,
  ProofCounters,
  SideEffectCheckpoint,
  TypedGateCode,
} from "./execution-kernel.types.js";

export function buildOpenClawDirectiveContract(params: {
  directiveId: string;
  userRequest: string;
  selectedOption: string;
  goal: string;
  taskClass: string;
  vendor?: string;
  service?: string;
  tool?: string;
  account?: string;
  workspace?: string;
  allowedActions: string[];
  forbiddenActions: string[];
  constraints: string[];
  budgetOrPriceCap?: number;
  billingPeriod?: string;
  exactPlanName?: string;
  successCriteria: string[];
  proofRequired: string[];
  hardGates: string[];
  fallbackPolicy: string;
  stopInstruction: string;
  rollbackInstruction: string;
  proofPath: string;
  createdFromMessage: string;
  nowIso: string;
  ownerDirectApproval: boolean;
  sourceAuthorityLoaded?: string[];
  expiresOrReviewAt?: string;
}): OpenClawDirectiveContract {
  return {
    directive_id: params.directiveId,
    user_request: params.userRequest,
    selected_option: params.selectedOption,
    goal: params.goal,
    task_class: params.taskClass,
    ...(params.vendor !== undefined ? { vendor: params.vendor } : {}),
    ...(params.service !== undefined ? { service: params.service } : {}),
    ...(params.tool !== undefined ? { tool: params.tool } : {}),
    ...(params.account !== undefined ? { account: params.account } : {}),
    ...(params.workspace !== undefined ? { workspace: params.workspace } : {}),
    allowed_actions: [...params.allowedActions],
    forbidden_actions: [...params.forbiddenActions],
    constraints: [...params.constraints],
    ...(params.budgetOrPriceCap !== undefined
      ? { budget_or_price_cap: params.budgetOrPriceCap }
      : {}),
    ...(params.billingPeriod !== undefined ? { billing_period: params.billingPeriod } : {}),
    ...(params.exactPlanName !== undefined ? { exact_plan_name: params.exactPlanName } : {}),
    success_criteria: [...params.successCriteria],
    proof_required: [...params.proofRequired],
    hard_gates: [...params.hardGates],
    fallback_policy: params.fallbackPolicy,
    stop_instruction: params.stopInstruction,
    rollback_instruction: params.rollbackInstruction,
    ...(params.expiresOrReviewAt !== undefined
      ? { expires_or_review_at: params.expiresOrReviewAt }
      : {}),
    created_from_message: params.createdFromMessage,
    owner_direct_approval: params.ownerDirectApproval,
    source_authority_loaded: params.sourceAuthorityLoaded ?? [],
    current_state: "created",
    last_checkpoint: params.nowIso,
    side_effects_completed: [],
    proof_path: params.proofPath,
  };
}

export function validateOpenClawDirectiveContract(value: unknown): {
  valid: boolean;
  missing: string[];
} {
  const requiredStringFields = [
    "directive_id",
    "user_request",
    "selected_option",
    "goal",
    "task_class",
    "fallback_policy",
    "stop_instruction",
    "rollback_instruction",
    "created_from_message",
    "current_state",
    "last_checkpoint",
    "proof_path",
  ];
  const requiredArrayFields = [
    "allowed_actions",
    "forbidden_actions",
    "constraints",
    "success_criteria",
    "proof_required",
    "hard_gates",
    "source_authority_loaded",
    "side_effects_completed",
  ];
  const missing: string[] = [];
  if (!isRecord(value)) {
    return {
      valid: false,
      missing: [...requiredStringFields, ...requiredArrayFields, "owner_direct_approval"],
    };
  }
  for (const field of requiredStringFields) {
    if (!stringValue(value[field])) {
      missing.push(field);
    }
  }
  for (const field of requiredArrayFields) {
    if (!Array.isArray(value[field])) {
      missing.push(field);
    }
  }
  if (typeof value.owner_direct_approval !== "boolean") {
    missing.push("owner_direct_approval");
  }
  return { valid: missing.length === 0, missing };
}

export function validateDirectiveCapsule(value: unknown): {
  valid: boolean;
  missing: string[];
} {
  const requiredStringFields = [
    "directive_id",
    "goal",
    "selected_option",
    "proof_path",
    "current_checkpoint",
  ];
  const requiredArrayFields = [
    "allowed_actions",
    "forbidden_actions",
    "constraints",
    "exact_gates",
    "success_criteria",
  ];
  const missing: string[] = [];
  if (!isRecord(value)) {
    return { valid: false, missing: [...requiredStringFields, ...requiredArrayFields] };
  }
  for (const field of requiredStringFields) {
    if (!stringValue(value[field])) {
      missing.push(field);
    }
  }
  for (const field of requiredArrayFields) {
    if (!Array.isArray(value[field])) {
      missing.push(field);
    }
  }
  return { valid: missing.length === 0, missing };
}

export function resolveAuthorityPrecedence(params: {
  latestExplicitInstruction?: string;
  activeDirective?: OpenClawDirectiveContract;
  runtimeRule?: string;
  skillGuidance?: string;
  modelJudgment?: string;
}): { source: AuthoritySource; instruction: string } | undefined {
  const latest = stringValue(params.latestExplicitInstruction);
  if (latest) {
    return { source: "latest_explicit_shamil_instruction", instruction: latest };
  }
  if (params.activeDirective) {
    return {
      source: "active_directive_contract",
      instruction: params.activeDirective.selected_option || params.activeDirective.user_request,
    };
  }
  const runtimeRule = stringValue(params.runtimeRule);
  if (runtimeRule) {
    return { source: "runtime_source_rules", instruction: runtimeRule };
  }
  const skillGuidance = stringValue(params.skillGuidance);
  if (skillGuidance) {
    return { source: "skill_runbook_guidance", instruction: skillGuidance };
  }
  const modelJudgment = stringValue(params.modelJudgment);
  if (modelJudgment) {
    return { source: "model_judgment", instruction: modelJudgment };
  }
  return undefined;
}

export function validateFinalGateCode(
  gate: string,
):
  | { valid: true; code: TypedGateCode }
  | { valid: false; code: "BROAD_GATE_LABEL" | "UNKNOWN_TYPED_GATE"; gate: string } {
  const normalized = normalize(gate).replaceAll("-", "_");
  if (BROAD_GATE_LABELS.has(normalized) || BROAD_GATE_LABELS.has(normalized.replaceAll("_", " "))) {
    return { valid: false, code: "BROAD_GATE_LABEL", gate };
  }
  const upper = gate.trim().toUpperCase();
  if (TYPED_GATE_CODE_SET.has(upper)) {
    return { valid: true, code: upper as TypedGateCode };
  }
  return { valid: false, code: "UNKNOWN_TYPED_GATE", gate };
}

export function verifyPreAction(params: {
  directive: OpenClawDirectiveContract;
  candidateAction: PreActionCandidate;
}):
  | { decision: "allow"; reason: "verified" }
  | { decision: "block"; gate: TypedGateCode; reason: string } {
  const directive = params.directive;
  const action = params.candidateAction;
  const hardGate = action.visibleHardGates?.find((gate) => validateFinalGateCode(gate).valid);
  if (hardGate) {
    return {
      decision: "block",
      gate: validateFinalGateCode(hardGate).code as TypedGateCode,
      reason: `${hardGate}: visible exact hard gate is present.`,
    };
  }
  const expectedVendor = directive.vendor ?? directive.service ?? directive.tool;
  if (expectedVendor && !sameLoose(action.vendor, expectedVendor)) {
    return { decision: "block", gate: "WRONG_VENDOR", reason: "WRONG_VENDOR: vendor mismatch." };
  }
  if (!directive.allowed_actions.includes(action.action)) {
    return {
      decision: "block",
      gate: "UNKNOWN_ACTION_EFFECT",
      reason: "UNKNOWN_ACTION_EFFECT: action is not in directive allowed_actions.",
    };
  }
  if (directive.exact_plan_name && !sameLoose(action.visiblePlan, directive.exact_plan_name)) {
    return { decision: "block", gate: "WRONG_PLAN", reason: "WRONG_PLAN: selected plan drift." };
  }
  if (
    typeof directive.budget_or_price_cap === "number" &&
    typeof action.visiblePrice === "number" &&
    action.visiblePrice > directive.budget_or_price_cap
  ) {
    return {
      decision: "block",
      gate: "PRICE_EXCEEDS_CAP",
      reason: "PRICE_EXCEEDS_CAP: visible price exceeds directive cap.",
    };
  }
  if (directive.billing_period && !sameLoose(action.billingPeriod, directive.billing_period)) {
    return {
      decision: "block",
      gate: "BILLING_PERIOD_MISMATCH",
      reason: "BILLING_PERIOD_MISMATCH: billing period drift.",
    };
  }
  if ((action.selectedAddOns ?? []).length > 0) {
    return {
      decision: "block",
      gate: "ADDON_OR_UPSELL_SELECTED",
      reason: "ADDON_OR_UPSELL_SELECTED: add-on or upsell is selected.",
    };
  }
  if (directive.account && !sameLoose(action.account, directive.account)) {
    return {
      decision: "block",
      gate: "ACCOUNT_IDENTITY_MISMATCH",
      reason: "ACCOUNT_IDENTITY_MISMATCH: account mismatch.",
    };
  }
  if (directive.workspace && !sameLoose(action.workspace, directive.workspace)) {
    return {
      decision: "block",
      gate: "ACCOUNT_IDENTITY_MISMATCH",
      reason: "ACCOUNT_IDENTITY_MISMATCH: workspace mismatch.",
    };
  }
  if (action.browserProfile && !action.browserProfileProven) {
    return {
      decision: "block",
      gate: "ACCOUNT_IDENTITY_MISMATCH",
      reason: "ACCOUNT_IDENTITY_MISMATCH: browser profile was not proven.",
    };
  }
  if (!stringValue(action.predictedEffect)) {
    return {
      decision: "block",
      gate: "UNKNOWN_ACTION_EFFECT",
      reason: "UNKNOWN_ACTION_EFFECT: predicted effect is missing.",
    };
  }
  return { decision: "allow", reason: "verified" };
}

export function validateProofCounters(params: {
  outcome: "SHIPPED_UNIT" | "EXACT_GATE_LANE_SWITCH";
  counters?: Partial<ProofCounters>;
}): { valid: true } | { valid: false; code: string; missing?: string[] } {
  const required: Array<keyof ProofCounters> = [
    "sent_count",
    "public_actions_count",
    "rendered_assets_count",
    "sales_count",
    "payments_seen",
    "accounts_created",
    "subscriptions_completed",
    "checkout_attempts",
    "buyer_signals",
    "drafts_created",
    "prep_packets_created",
  ];
  const missing = required.filter((field) => typeof params.counters?.[field] !== "number");
  if (missing.length > 0) {
    return { valid: false, code: "MISSING_PROOF_COUNTERS", missing };
  }
  const counters = params.counters as ProofCounters;
  const negative = required.filter((field) => counters[field] < 0);
  if (negative.length > 0) {
    return { valid: false, code: "INVALID_PROOF_COUNTERS", missing: negative };
  }
  const executionCount =
    counters.sent_count +
    counters.public_actions_count +
    counters.rendered_assets_count +
    counters.sales_count +
    counters.payments_seen +
    counters.accounts_created +
    counters.subscriptions_completed +
    counters.checkout_attempts +
    counters.buyer_signals;
  if (
    params.outcome === "SHIPPED_UNIT" &&
    executionCount === 0 &&
    counters.prep_packets_created > 0
  ) {
    return { valid: false, code: "PREP_ONLY_NOT_SHIPPED" };
  }
  return { valid: true };
}

export function splitVagueWorkerObjective(objective: string): {
  acceptedAsSingleWorker: boolean;
  packets: Array<{ objective: string; expected_artifact: string; timeout_ms: number }>;
  reason?: string;
} {
  const text = normalize(objective);
  const vague =
    /\b(make sales|fix everything|do everything|run (the )?company|whole company|all of it)\b/.test(
      text,
    );
  if (!vague) {
    return { acceptedAsSingleWorker: true, packets: [] };
  }
  const titanSales = /\b(titan|sales|revenue|company)\b/.test(text);
  if (!titanSales) {
    return {
      acceptedAsSingleWorker: false,
      reason: "vague_worker_objective",
      packets: [
        {
          objective: "recover current authority and active lanes",
          expected_artifact: "authority map",
          timeout_ms: 10 * 60_000,
        },
      ],
    };
  }
  return {
    acceptedAsSingleWorker: false,
    reason: "vague_worker_objective",
    packets: [
      {
        objective: "check wallet/orders",
        expected_artifact: "payment/order status",
        timeout_ms: 5 * 60_000,
      },
      {
        objective: "check buyer DMs",
        expected_artifact: "buyer signal status",
        timeout_ms: 10 * 60_000,
      },
      {
        objective: "publish one approved content packet",
        expected_artifact: "permalink or exact gate",
        timeout_ms: 15 * 60_000,
      },
      { objective: "source 10 targets", expected_artifact: "target list", timeout_ms: 20 * 60_000 },
      {
        objective: "repair checkout friction",
        expected_artifact: "CRO proof",
        timeout_ms: 30 * 60_000,
      },
      {
        objective: "verify paid-start/email sendability",
        expected_artifact: "sendability proof or exact gate",
        timeout_ms: 20 * 60_000,
      },
      {
        objective: "render one Higgsfield asset",
        expected_artifact: "rendered asset or exact gate",
        timeout_ms: 20 * 60_000,
      },
      {
        objective: "update attribution",
        expected_artifact: "attribution ledger update",
        timeout_ms: 10 * 60_000,
      },
    ],
  };
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function stringValue(value: unknown): string | undefined {
  return typeof value === "string" && value.trim().length > 0 ? value.trim() : undefined;
}

function normalize(value: unknown): string {
  return typeof value === "string" ? value.trim().toLowerCase() : "";
}

function sameLoose(actual: unknown, expected: string): boolean {
  const actualText = normalize(actual);
  const expectedText = normalize(expected);
  return Boolean(actualText && expectedText && actualText.includes(expectedText));
}
