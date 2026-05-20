export type ClaudeCliPrimaryRouteValidation =
  | { valid: true }
  | {
      valid: false;
      code: "CLAUDE_PRIMARY_ROUTE_NOT_CLI_CANONICAL";
      reason: string;
    };

export function validateClaudeCliPrimaryRoute(params: {
  strictCliOnly?: boolean;
  primaryModelRef: string;
  actualBackend?: string;
  authProfileProvider?: string;
  authProfileMode?: string;
}): ClaudeCliPrimaryRouteValidation {
  if (params.strictCliOnly !== true) {
    return { valid: true };
  }
  const primary = normalize(params.primaryModelRef);
  const backend = normalize(params.actualBackend);
  const authProvider = normalize(params.authProfileProvider);
  const authMode = normalize(params.authProfileMode);
  const canonicalPrimary = primary.startsWith("claude-cli/claude-");
  const anthropicWithCliOauthPrimary = primary.startsWith("anthropic/claude-");
  const cliBackend = backend === "claude-cli" || !backend;
  const anthropicBackend = backend === "anthropic";
  const cliOauth = authProvider === "claude-cli" && authMode === "oauth";
  if (canonicalPrimary && cliBackend && cliOauth) {
    return { valid: true };
  }
  if (anthropicWithCliOauthPrimary && (anthropicBackend || !backend) && cliOauth) {
    return { valid: true };
  }
  return {
    valid: false,
    code: "CLAUDE_PRIMARY_ROUTE_NOT_CLI_CANONICAL",
    reason:
      "CLAUDE_PRIMARY_ROUTE_NOT_CLI_CANONICAL: CLI-only Claude requires claude-cli/<model> plus claude-cli OAuth backend proof.",
  };
}

function normalize(value: unknown): string {
  return typeof value === "string" ? value.trim().toLowerCase() : "";
}
