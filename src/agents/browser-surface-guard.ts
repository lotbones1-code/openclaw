import { normalizeOptionalString } from "../shared/string-coerce.js";
import { isPlainObject } from "../utils.js";

export type BrowserSurfaceGuardDecision =
  | { blocked: false }
  | {
      blocked: true;
      code: "WRONG_SURFACE_DETECTED" | "SENSITIVE_ACCOUNT_SURFACE_GATE";
      reason: string;
    };

const BROWSER_TOOL_PATTERNS = [
  "browser",
  "chrome",
  "computer",
  "cdp",
  "screenshot",
  "navigate",
  "click",
  "type",
  "upload",
  "tab",
] as const;

const HUMAN_SURFACE_PATTERNS = ["user", "chatgpt", "chat.openai.com", "netflix", "comet"] as const;

const SENSITIVE_SURFACE_PATTERNS = [
  "amazon.",
  "/orders",
  "order-history",
  "your-orders",
  "refund",
  "return",
  "checkout",
  "payment",
  "subscription",
  "billing",
  "account/recovery",
  "account/security",
  "security settings",
  "security center",
  "password reset",
  "reset password",
  "password change",
  "change password",
  "two-factor",
  "two factor",
  "2fa",
  "mfa",
  "passkey",
] as const;

const APPROVAL_KEYS = [
  "sensitiveAccountSurfaceApproved",
  "sensitiveSurfaceApproved",
  "scopedApproval",
  "exactScopedApproval",
] as const;

function includesPattern(value: string, patterns: readonly string[]): boolean {
  const lower = value.toLowerCase();
  return patterns.some((pattern) => lower.includes(pattern));
}

function collectStrings(value: unknown, into: string[] = [], keyPath = ""): string[] {
  if (typeof value === "string") {
    into.push(keyPath ? `${keyPath}=${value}` : value);
    return into;
  }
  if (Array.isArray(value)) {
    for (const [index, item] of value.entries()) {
      collectStrings(item, into, keyPath ? `${keyPath}.${index}` : String(index));
    }
    return into;
  }
  if (!isPlainObject(value)) {
    return into;
  }
  let entries: Array<[string, unknown]>;
  try {
    entries = Object.entries(value);
  } catch {
    return into;
  }
  for (const [key, nested] of entries) {
    try {
      const nestedKey = keyPath ? `${keyPath}.${key}` : key;
      if (typeof nested === "string") {
        into.push(`${nestedKey}=${nested}`);
        continue;
      }
      collectStrings(nested, into, nestedKey);
    } catch {
      continue;
    }
  }
  return into;
}

function hasTruthyApproval(value: unknown): boolean {
  if (!isPlainObject(value)) {
    return false;
  }
  for (const key of APPROVAL_KEYS) {
    if (value[key] === true) {
      return true;
    }
  }
  const allowedActions = value.allowedActions;
  if (Array.isArray(allowedActions)) {
    return allowedActions.some(
      (item) =>
        typeof item === "string" &&
        (item === "sensitive_account_surface" || item === "account_order_refund_surface"),
    );
  }
  return false;
}

function isBrowserOrAccountTool(toolName: string, strings: string[]): boolean {
  if (includesPattern(toolName, BROWSER_TOOL_PATTERNS)) {
    return true;
  }
  if (
    strings.some((entry) =>
      /(^|\.)(browserProfile|cdpTargetId|cdpSessionId|targetId|url|tabId)=/i.test(entry),
    )
  ) {
    return true;
  }
  return false;
}

function hasHumanSurface(strings: string[]): boolean {
  return strings.some((entry) => includesPattern(entry, HUMAN_SURFACE_PATTERNS));
}

function hasSensitiveSurface(strings: string[]): boolean {
  return strings.some((entry) => includesPattern(entry, SENSITIVE_SURFACE_PATTERNS));
}

function hasExplicitOpenClawOwner(value: unknown): boolean {
  if (!isPlainObject(value)) {
    return false;
  }
  const browserProfile = normalizeOptionalString(
    typeof value.browserProfile === "string" ? value.browserProfile : undefined,
  );
  const cdpTargetId = normalizeOptionalString(
    typeof value.cdpTargetId === "string" ? value.cdpTargetId : undefined,
  );
  const ownerTaskId = normalizeOptionalString(
    typeof value.ownerTaskId === "string" ? value.ownerTaskId : undefined,
  );
  const ownerSessionKey = normalizeOptionalString(
    typeof value.ownerSessionKey === "string" ? value.ownerSessionKey : undefined,
  );
  return Boolean(browserProfile && (cdpTargetId || ownerTaskId || ownerSessionKey));
}

export function evaluateBrowserSurfaceGuard(params: {
  toolName: string;
  toolParams: unknown;
  sessionKey?: string;
  runId?: string;
}): BrowserSurfaceGuardDecision {
  const strings = collectStrings(params.toolParams);
  if (!isBrowserOrAccountTool(params.toolName, strings)) {
    return { blocked: false };
  }

  if (hasHumanSurface(strings) && !hasExplicitOpenClawOwner(params.toolParams)) {
    return {
      blocked: true,
      code: "WRONG_SURFACE_DETECTED",
      reason:
        "WRONG_SURFACE_DETECTED: browser/account action targeted a human or unassigned foreground surface instead of an OpenClaw-owned browser profile.",
    };
  }

  if (hasSensitiveSurface(strings) && !hasTruthyApproval(params.toolParams)) {
    return {
      blocked: true,
      code: "SENSITIVE_ACCOUNT_SURFACE_GATE",
      reason:
        "SENSITIVE_ACCOUNT_SURFACE_GATE: order/refund/checkout/payment/account-security surface requires exact scoped approval before browser action.",
    };
  }

  return { blocked: false };
}
