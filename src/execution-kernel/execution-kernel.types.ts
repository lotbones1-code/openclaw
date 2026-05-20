export const AUTHORITY_PRECEDENCE = [
  "latest_explicit_shamil_instruction",
  "active_directive_contract",
  "runtime_source_rules",
  "skill_runbook_guidance",
  "model_judgment",
] as const;

export type AuthoritySource = (typeof AUTHORITY_PRECEDENCE)[number];

export const TYPED_GATE_CODES = [
  "WRONG_VENDOR",
  "WRONG_PLAN",
  "PRICE_EXCEEDS_CAP",
  "BILLING_PERIOD_MISMATCH",
  "ADDON_OR_UPSELL_SELECTED",
  "MISSING_PASSWORD",
  "MISSING_CVV",
  "MISSING_2FA",
  "CAPTCHA_REQUIRED",
  "PLATFORM_WARNING",
  "LEGAL_OR_COMPLIANCE_WARNING",
  "DNS_MUTATION",
  "DESTRUCTIVE_ACTION",
  "ACCOUNT_IDENTITY_MISMATCH",
  "MISSING_ACCOUNT_ACCESS",
  "INSUFFICIENT_CREDITS",
  "UNKNOWN_ACTION_EFFECT",
  "SEND_LOCKED",
  "PUBLIC_ACTION_CAP",
  "RESOURCE_LOCKED",
  "USER_STOPPED",
] as const;

export type TypedGateCode = (typeof TYPED_GATE_CODES)[number];

export const TYPED_GATE_CODE_SET = new Set<string>(TYPED_GATE_CODES);

export const BROAD_GATE_LABELS = new Set([
  "human_gate",
  "human gate",
  "payment_gate",
  "payment gate",
  "payment blocked",
  "subscription_gate",
  "subscription gate",
  "subscription_blocked",
  "subscription blocked",
  "manual_required",
  "manual required",
  "ask_user",
  "ask user",
  "ask shamil",
  "wait_for_user",
  "wait for user",
  "blocked",
  "unclear",
]);

export type OpenClawDirectiveContract = {
  directive_id: string;
  user_request: string;
  selected_option: string;
  goal: string;
  task_class: string;
  vendor?: string;
  service?: string;
  tool?: string;
  account?: string;
  workspace?: string;
  allowed_actions: string[];
  forbidden_actions: string[];
  constraints: string[];
  budget_or_price_cap?: number;
  billing_period?: string;
  exact_plan_name?: string;
  success_criteria: string[];
  proof_required: string[];
  hard_gates: string[];
  fallback_policy: string;
  stop_instruction: string;
  rollback_instruction: string;
  expires_or_review_at?: string;
  created_from_message: string;
  owner_direct_approval: boolean;
  source_authority_loaded: string[];
  current_state: string;
  last_checkpoint: string;
  side_effects_completed: SideEffectCheckpoint[];
  proof_path: string;
};

export type DirectiveCapsule = {
  directive_id: string;
  goal: string;
  selected_option: string;
  allowed_actions: string[];
  forbidden_actions: string[];
  constraints: string[];
  exact_gates: string[];
  success_criteria: string[];
  proof_path: string;
  current_checkpoint: string;
};

export type SideEffectCheckpoint = {
  side_effect_id: string;
  action: string;
  checkpointed_at: string;
  verifier_result: "allow" | "block";
  observed_result?: string;
  proof_path?: string;
  unsafe_to_repeat: boolean;
};

export type PreActionCandidate = {
  action: string;
  vendor?: string;
  visiblePlan?: string;
  visiblePrice?: number;
  billingPeriod?: string;
  selectedAddOns?: string[];
  account?: string;
  workspace?: string;
  browserProfile?: string;
  browserProfileProven?: boolean;
  predictedEffect?: string;
  visibleHardGates?: string[];
};

export type ProofCounters = {
  sent_count: number;
  public_actions_count: number;
  rendered_assets_count: number;
  sales_count: number;
  payments_seen: number;
  accounts_created: number;
  subscriptions_completed: number;
  checkout_attempts: number;
  buyer_signals: number;
  drafts_created: number;
  prep_packets_created: number;
};

export type LearningFooter = {
  lessons_applied: string[];
  learning_event: string;
  next_action: string;
};
