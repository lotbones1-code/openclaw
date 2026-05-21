import type { WorkManagerCandidate } from "../work-manager/work-manager.js";

export type MissionCapabilityId =
  | "social-media-execution"
  | "trading-operations"
  | "content-creation"
  | "revenue-operations"
  | "market-intelligence"
  | "memory-learning"
  | "system-health"
  | "self-acquire"
  | "dns-email-setup";

export type MissionCapabilityStatus = "available" | "gated" | "degraded" | "missing";

export type MissionCapabilityDefinition = {
  capabilityId: MissionCapabilityId;
  label: string;
  description: string;
  pool: WorkManagerCandidate["pool"];
  priorityHint: WorkManagerCandidate["priority"];
  frequencyHint: string;
  requiredTools: string[];
  requiredSkills: string[];
  proofRequired: string[];
  resourceRequirements: string[];
  allowedActions: string[];
  forbiddenActions: string[];
  hardGates: string[];
  highStakes: boolean;
  requiresModelCouncil: boolean;
  status: MissionCapabilityStatus;
};

const CORE_MISSION_CAPABILITIES: MissionCapabilityDefinition[] = [
  {
    capabilityId: "social-media-execution",
    label: "Social Media Execution",
    description: "IG/X posting, commenting, DM engagement, and proof-backed social actions.",
    pool: "social",
    priorityHint: "P1",
    frequencyHint: "when buyer/content signal is fresh and public gates pass",
    requiredTools: ["native-browser-profiles", "browser-mcp", "outbound-log"],
    requiredSkills: ["social-content", "execution-steward"],
    proofRequired: ["screenshot", "outbound_log_entry", "mandatory_terminal_summary"],
    resourceRequirements: ["browser_profile:titan-ig", "social_account:titan-ig"],
    allowedActions: ["observe", "draft", "approved_public_action"],
    forbiddenActions: ["spam", "duplicate_public_reply", "public_action_without_verifier"],
    hardGates: ["PUBLIC_ACTION_REQUIRES_PRE_ACTION_VERIFIER", "SEND_LOCKED", "AUTH_EXPIRED"],
    highStakes: true,
    requiresModelCouncil: true,
    status: "available",
  },
  {
    capabilityId: "trading-operations",
    label: "Trading Operations",
    description: "Hyperliquid market analysis, position management, and gated trade execution.",
    pool: "revenue",
    priorityHint: "P1",
    frequencyHint: "only when trading directive and risk gates are explicit",
    requiredTools: ["hyperliquid-mcp", "market-data"],
    requiredSkills: ["trading-strategies", "risk-management"],
    proofRequired: ["model_council_decision", "order_confirmation", "position_state"],
    resourceRequirements: ["trading:hyperliquid", "model:council"],
    allowedActions: ["analyze", "prepare_order", "execute_if_exact_directive"],
    forbiddenActions: ["trade_without_model_council", "trade_without_risk_gate"],
    hardGates: ["TRADING_REQUIRES_EXACT_DIRECTIVE", "MODEL_COUNCIL_REQUIRED", "AUTH_EXPIRED"],
    highStakes: true,
    requiresModelCouncil: true,
    status: "gated",
  },
  {
    capabilityId: "content-creation",
    label: "Content Creation",
    description: "Blog posts, social content, marketing materials, and publish-ready packets.",
    pool: "revenue",
    priorityHint: "P1",
    frequencyHint: "daily or when mission needs fresh content",
    requiredTools: ["vault-reports", "native-files"],
    requiredSkills: ["social-content", "direct-response-copy", "content-strategy"],
    proofRequired: ["content_file_created", "proof_path"],
    resourceRequirements: ["vault:content"],
    allowedActions: ["research", "draft", "package_content"],
    forbiddenActions: ["publish_without_public_gate"],
    hardGates: ["PUBLIC_ACTION_REQUIRES_PRE_ACTION_VERIFIER"],
    highStakes: false,
    requiresModelCouncil: false,
    status: "available",
  },
  {
    capabilityId: "revenue-operations",
    label: "Revenue Operations",
    description: "Lead generation, outreach, sales pipeline, and response tracking.",
    pool: "revenue",
    priorityHint: "P0",
    frequencyHint: "every mission cycle when sales proof is missing",
    requiredTools: ["outbound-log", "titan-attribution-ledger", "native-browser-profiles"],
    requiredSkills: ["revenue-operator", "direct-response-copy"],
    proofRequired: ["outreach_sent_or_exact_gate", "response_tracking_entry", "attribution_entry"],
    resourceRequirements: ["vault:revenue", "mailbox:outbound"],
    allowedActions: ["source_targets", "draft_outreach", "send_if_verifier_passes"],
    forbiddenActions: [
      "send_without_SEND_LOCKED_clear",
      "customer_record_mutation_without_directive",
    ],
    hardGates: ["SEND_LOCKED", "PUBLIC_ACTION_REQUIRES_PRE_ACTION_VERIFIER"],
    highStakes: true,
    requiresModelCouncil: true,
    status: "available",
  },
  {
    capabilityId: "market-intelligence",
    label: "Market Intelligence",
    description: "Market research, customer feedback, competitor analysis, and buyer signals.",
    pool: "research",
    priorityHint: "P1",
    frequencyHint: "when revenue action lacks evidence",
    requiredTools: ["web-research", "vault-reports"],
    requiredSkills: ["content-strategy", "seo-audit"],
    proofRequired: ["research_report", "source_links", "next_sales_action"],
    resourceRequirements: ["vault:research"],
    allowedActions: ["research", "summarize", "rank_opportunities"],
    forbiddenActions: ["fabricated_claims"],
    hardGates: ["SOURCE_UNVERIFIED"],
    highStakes: false,
    requiresModelCouncil: false,
    status: "available",
  },
  {
    capabilityId: "memory-learning",
    label: "Memory And Learning",
    description: "System state, decision history, lesson extraction, and Obsidian proof updates.",
    pool: "build",
    priorityHint: "P2",
    frequencyHint: "after meaningful shipped work or failure pattern",
    requiredTools: ["obsidian-vault", "memory-lancedb-pro"],
    requiredSkills: ["execution-steward"],
    proofRequired: ["vault_entry_updated", "lesson_or_none_relevant"],
    resourceRequirements: ["vault:Learning OS"],
    allowedActions: ["record_lesson", "update_state", "compact_success_note"],
    forbiddenActions: ["store_secrets", "store_raw_private_content"],
    hardGates: ["VAULT_WRITE_BLOCKED"],
    highStakes: false,
    requiresModelCouncil: false,
    status: "available",
  },
  {
    capabilityId: "system-health",
    label: "System Health",
    description: "Disk monitoring, session cleanup, config validation, cron/task audits.",
    pool: "maintenance",
    priorityHint: "P1",
    frequencyHint: "when reliability is red or before overnight/company mode",
    requiredTools: ["openclaw-cli", "task-registry", "work-manager"],
    requiredSkills: ["execution-steward"],
    proofRequired: ["health_report", "config_validate_result", "task_audit_result"],
    resourceRequirements: ["repo:openclaw-runtime-src", "state:openclaw"],
    allowedActions: ["status", "validate", "native_maintenance"],
    forbiddenActions: ["manual_state_db_edits", "host_cron", "side_daemon"],
    hardGates: ["NATIVE_MAINTENANCE_COMMAND_MISSING"],
    highStakes: false,
    requiresModelCouncil: false,
    status: "available",
  },
  {
    capabilityId: "self-acquire",
    label: "Self Acquire",
    description: "Research, install, register, and test missing MCP tools or skills natively.",
    pool: "build",
    priorityHint: "P1",
    frequencyHint: "when a mission needs a capability OpenClaw does not have",
    requiredTools: ["openclaw-cli", "tool-registry", "skills-registry"],
    requiredSkills: ["execution-steward", "self-builder-contract"],
    proofRequired: ["option_comparison", "native_install_or_exact_gate", "capability_registration"],
    resourceRequirements: ["repo:openclaw-runtime-src", "native_tool_registry"],
    allowedActions: ["research", "compare", "install_via_native_openclaw", "register", "test"],
    forbiddenActions: ["wrapper_script", "host_cron", "side_daemon", "parallel_db"],
    hardGates: ["NATIVE_INSTALL_PATH_MISSING", "EXTERNAL_TOOL_UNVERIFIED"],
    highStakes: false,
    requiresModelCouncil: false,
    status: "available",
  },
  {
    capabilityId: "dns-email-setup",
    label: "DNS And Email Setup",
    description: "DKIM, DMARC, SPF, and Workspace billing readiness as exact human-input gates.",
    pool: "personal",
    priorityHint: "P2",
    frequencyHint: "when outbound email capability is blocked",
    requiredTools: ["account-registry", "dns-provider", "google-workspace"],
    requiredSkills: ["account-automation"],
    proofRequired: ["required_records", "human_input_gate", "workspace_status"],
    resourceRequirements: ["dns:workspace-domain", "account_setup:google-workspace"],
    allowedActions: ["observe", "prepare_records", "report_exact_gate"],
    forbiddenActions: ["dns_mutation_without_directive", "billing_activation_without_directive"],
    hardGates: ["DNS_MUTATION_REQUIRES_EXACT_DIRECTIVE", "BILLING_REQUIRES_EXACT_DIRECTIVE"],
    highStakes: true,
    requiresModelCouncil: false,
    status: "gated",
  },
];

export function listMissionCapabilityDefinitions(): MissionCapabilityDefinition[] {
  return CORE_MISSION_CAPABILITIES.map((capability) => ({
    ...capability,
    requiredTools: [...capability.requiredTools],
    requiredSkills: [...capability.requiredSkills],
    proofRequired: [...capability.proofRequired],
    resourceRequirements: [...capability.resourceRequirements],
    allowedActions: [...capability.allowedActions],
    forbiddenActions: [...capability.forbiddenActions],
    hardGates: [...capability.hardGates],
  }));
}

export function getMissionCapabilityDefinition(
  capabilityId: MissionCapabilityId,
): MissionCapabilityDefinition | undefined {
  return listMissionCapabilityDefinitions().find(
    (capability) => capability.capabilityId === capabilityId,
  );
}
