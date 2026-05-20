import { describe, expect, it } from "vitest";
import { validateClaudeCliPrimaryRoute } from "./model-route-canonicalization.js";

describe("Claude CLI primary route canonicalization", () => {
  it("allows the canonical CLI route for CLI-only Claude", () => {
    expect(
      validateClaudeCliPrimaryRoute({
        strictCliOnly: true,
        primaryModelRef: "claude-cli/claude-opus-4-7",
        actualBackend: "claude-cli",
        authProfileProvider: "claude-cli",
        authProfileMode: "oauth",
      }),
    ).toEqual({ valid: true });
  });

  it("allows Anthropic-labeled Claude only when it proves claude-cli OAuth backing", () => {
    expect(
      validateClaudeCliPrimaryRoute({
        strictCliOnly: true,
        primaryModelRef: "anthropic/claude-opus-4-7",
        actualBackend: "anthropic",
        authProfileProvider: "claude-cli",
        authProfileMode: "oauth",
      }),
    ).toEqual({ valid: true });
  });

  it("blocks ambiguous Anthropic/API route drift when CLI-only Claude is required", () => {
    expect(
      validateClaudeCliPrimaryRoute({
        strictCliOnly: true,
        primaryModelRef: "anthropic/claude-opus-4-7",
        actualBackend: "anthropic",
        authProfileProvider: "anthropic",
        authProfileMode: "token",
      }),
    ).toMatchObject({
      valid: false,
      code: "CLAUDE_PRIMARY_ROUTE_NOT_CLI_CANONICAL",
    });
  });
});
