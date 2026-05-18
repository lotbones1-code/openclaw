import { normalizeOptionalString } from "../shared/string-coerce.js";
import {
  appendPolicyLockAudit,
  consumeMatchingPolicyUnlock,
  getPolicyLock,
} from "../tasks/policy-lock-registry.js";
import { isPlainObject } from "../utils.js";

export type PolicyLockGuardDecision =
  | { blocked: false }
  | {
      blocked: true;
      code:
        | "SEND_LOCKED"
        | "NON_NATIVE_TOOLING_BLOCKED"
        | "WRONG_PROFILE_DEFAULT_BLOCK"
        | "USER_PROFILE_AUTONOMY_BLOCK"
        | "SENSITIVE_ACCOUNT_SURFACE_GATE";
      reason: string;
      lockId?: string;
    };

type SensitiveAction = {
  lockId: string;
  action: string;
  code: PolicyLockGuardDecision extends infer T
    ? T extends { blocked: true; code: infer C }
      ? C
      : never
    : never;
  reason: string;
  alwaysBlock?: boolean;
};

const SHELL_TOOL_PATTERNS = ["bash", "shell", "exec", "command", "terminal", "run"] as const;
const BROWSER_TOOL_PATTERNS = ["browser", "chrome", "computer", "cdp"] as const;
const WRITE_TOOL_PATTERNS = ["write", "edit", "patch"] as const;

const B2B_SEND_PATTERNS = [
  "send_clinic_batch.py",
  "send_clinic_batch_",
  "send_clinic_followups.py",
  "smtp.gmail.com",
  "smtplib",
  "gmail smtp",
  "GMAIL_APP_PASSWORD",
  "GMAIL_SENDER",
  "clinic-outreach-batch",
] as const;

const DIRECT_PROVIDER_API_PATTERNS = [
  "brand_publisher.py",
  "graph.facebook.com",
  "content_publish",
  "instagram graph",
  "meta graph api",
  "oauth/access_token",
  "access_token?grant_type",
] as const;

const PUBLIC_SOCIAL_MUTATION_PATTERNS = [
  "publish",
  "post",
  "comment",
  "dm",
  "direct message",
  "follow",
  "like",
  "share",
  "upload",
] as const;

const MONEY_ADMIN_PATTERNS = [
  "refund",
  "payment",
  "checkout",
  "subscription",
  "billing",
  "dns",
  "dkim",
  "dmarc",
  "mailbox",
] as const;

const ACCOUNT_SECURITY_ADMIN_PATTERNS = [
  "account recovery",
  "account security",
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
  "api key",
  "credential rotation",
  "credential deletion",
] as const;

function includesAny(value: string, patterns: readonly string[]): boolean {
  const lower = value.toLowerCase();
  return patterns.some((pattern) => lower.includes(pattern.toLowerCase()));
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
  let entries: [string, unknown][];
  try {
    entries = Object.entries(value);
  } catch {
    return into;
  }
  for (const [key, nested] of entries) {
    const nestedKey = keyPath ? `${keyPath}.${key}` : key;
    if (typeof nested === "string") {
      into.push(`${nestedKey}=${nested}`);
      continue;
    }
    collectStrings(nested, into, nestedKey);
  }
  return into;
}

function toolNameMatches(toolName: string, patterns: readonly string[]): boolean {
  return includesAny(toolName, patterns);
}

function isShellActionTool(toolName: string): boolean {
  return toolNameMatches(toolName, SHELL_TOOL_PATTERNS);
}

function isBrowserActionTool(toolName: string): boolean {
  return toolNameMatches(toolName, BROWSER_TOOL_PATTERNS);
}

function isWriteActionTool(toolName: string): boolean {
  return toolNameMatches(toolName, WRITE_TOOL_PATTERNS);
}

function paramsPath(value: unknown): string {
  if (!isPlainObject(value)) {
    return "";
  }
  return (
    normalizeOptionalString(
      typeof value.path === "string"
        ? value.path
        : typeof value.file === "string"
          ? value.file
          : undefined,
    ) ?? ""
  );
}

function hasTruthyExactUnlock(value: unknown): boolean {
  if (!isPlainObject(value)) {
    return false;
  }
  return Boolean(
    value.exactPolicyUnlock === true ||
    value.policyUnlockApproved === true ||
    normalizeOptionalString(
      typeof value.policyUnlockId === "string" ? value.policyUnlockId : undefined,
    ),
  );
}

function inferSensitiveAction(params: {
  toolName: string;
  toolParams: unknown;
  joinedStrings: string;
}): SensitiveAction | null {
  const { toolName, toolParams, joinedStrings } = params;
  const path = paramsPath(toolParams);

  if (isShellActionTool(toolName) && includesAny(joinedStrings, B2B_SEND_PATTERNS)) {
    return {
      lockId: "b2b:send",
      action: "b2b_send",
      code: "SEND_LOCKED",
      reason:
        "SEND_LOCKED: direct B2B/Gmail/SMTP sender execution is blocked outside a native OpenClaw send lane.",
      alwaysBlock: true,
    };
  }

  if (isWriteActionTool(toolName) && includesAny(path, B2B_SEND_PATTERNS)) {
    return {
      lockId: "b2b:send",
      action: "write_sender",
      code: "NON_NATIVE_TOOLING_BLOCKED",
      reason:
        "NON_NATIVE_TOOLING_BLOCKED: generated SMTP sender files must be quarantined or native-gated, not rewritten as executable senders.",
      alwaysBlock: true,
    };
  }

  if (
    isShellActionTool(toolName) &&
    /python\d*(\.\d+)?\s+.*send_.*(clinic|b2b|email)/i.test(joinedStrings)
  ) {
    return {
      lockId: "b2b:send",
      action: "direct_send_script",
      code: "NON_NATIVE_TOOLING_BLOCKED",
      reason:
        "NON_NATIVE_TOOLING_BLOCKED: direct Python/email sender execution is blocked outside a native OpenClaw send lane.",
      alwaysBlock: true,
    };
  }

  if (isShellActionTool(toolName) && includesAny(joinedStrings, DIRECT_PROVIDER_API_PATTERNS)) {
    return {
      lockId: "api:provider_mutation",
      action: "direct_provider_api",
      code: "NON_NATIVE_TOOLING_BLOCKED",
      reason:
        "NON_NATIVE_TOOLING_BLOCKED: direct provider/API mutation scripts must be converted to native OpenClaw adapters with policy-lock checks.",
      alwaysBlock: true,
    };
  }

  if (
    isBrowserActionTool(toolName) &&
    includesAny(joinedStrings, PUBLIC_SOCIAL_MUTATION_PATTERNS) &&
    includesAny(joinedStrings, ["instagram.com", "threads", "x.com", "twitter.com", "facebook.com"])
  ) {
    return {
      lockId: includesAny(joinedStrings, ["dm", "direct message"])
        ? "public_social:dm_cold"
        : includesAny(joinedStrings, ["comment"])
          ? "public_social:comment"
          : "public_social:post",
      action: "public_social_mutation",
      code: "SEND_LOCKED",
      reason:
        "SEND_LOCKED: public social mutation requires exact account/path authorization and green platform proof.",
    };
  }

  if (
    (isShellActionTool(toolName) || isBrowserActionTool(toolName)) &&
    (includesAny(joinedStrings, MONEY_ADMIN_PATTERNS) ||
      includesAny(joinedStrings, ACCOUNT_SECURITY_ADMIN_PATTERNS)) &&
    includesAny(joinedStrings, [
      "post ",
      "submit",
      "mutation",
      "change",
      "update",
      "delete",
      "create",
    ])
  ) {
    return {
      lockId: includesAny(joinedStrings, ["dns", "dkim", "dmarc", "mailbox"])
        ? "dns:mutation"
        : includesAny(joinedStrings, ["refund"])
          ? "payment:refund"
          : includesAny(joinedStrings, ["checkout"])
            ? "checkout:mutation"
            : "payment:order",
      action: "sensitive_mutation",
      code: "SENSITIVE_ACCOUNT_SURFACE_GATE",
      reason:
        "SENSITIVE_ACCOUNT_SURFACE_GATE: payment/order/checkout/DNS/mailbox/account-security mutation requires exact scoped approval.",
    };
  }

  return null;
}

export function evaluatePolicyLockGuard(params: {
  toolName: string;
  toolParams: unknown;
  sessionKey?: string;
  runId?: string;
}): PolicyLockGuardDecision {
  const strings = collectStrings(params.toolParams);
  const joinedStrings = strings.join("\n");
  const sensitive = inferSensitiveAction({
    toolName: params.toolName,
    toolParams: params.toolParams,
    joinedStrings,
  });
  if (!sensitive) {
    return { blocked: false };
  }

  if (sensitive.alwaysBlock) {
    appendPolicyLockAudit({
      lockId: sensitive.lockId,
      sessionKey: params.sessionKey,
      runId: params.runId,
      action: sensitive.action,
      source: "before-tool-policy-guard",
      channel: params.toolName,
      decision: "DENIED",
      reasonCode: sensitive.code,
      detail: sensitive.reason,
    });

    return {
      blocked: true,
      code: sensitive.code,
      lockId: sensitive.lockId,
      reason: sensitive.reason,
    };
  }

  const lock = getPolicyLock(sensitive.lockId);
  if (lock?.state !== "LOCKED") {
    return { blocked: false };
  }

  if (hasTruthyExactUnlock(params.toolParams)) {
    const unlock = consumeMatchingPolicyUnlock({
      lockId: sensitive.lockId,
      taskId: normalizeOptionalString(
        isPlainObject(params.toolParams) && typeof params.toolParams.taskId === "string"
          ? params.toolParams.taskId
          : undefined,
      ),
      lane: normalizeOptionalString(
        isPlainObject(params.toolParams) && typeof params.toolParams.lane === "string"
          ? params.toolParams.lane
          : undefined,
      ),
      action: sensitive.action,
      account: normalizeOptionalString(
        isPlainObject(params.toolParams) && typeof params.toolParams.account === "string"
          ? params.toolParams.account
          : undefined,
      ),
      targetClass: normalizeOptionalString(
        isPlainObject(params.toolParams) && typeof params.toolParams.targetClass === "string"
          ? params.toolParams.targetClass
          : undefined,
      ),
    });
    if (unlock) {
      return { blocked: false };
    }
  }

  appendPolicyLockAudit({
    lockId: sensitive.lockId,
    sessionKey: params.sessionKey,
    runId: params.runId,
    action: sensitive.action,
    source: "before-tool-policy-guard",
    channel: params.toolName,
    decision: "DENIED",
    reasonCode: sensitive.code,
    detail: sensitive.reason,
  });

  return {
    blocked: true,
    code: sensitive.code,
    lockId: sensitive.lockId,
    reason: sensitive.reason,
  };
}
