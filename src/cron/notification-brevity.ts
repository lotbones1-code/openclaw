import type { ReplyPayload } from "../auto-reply/reply-payload.js";
import type { OpenClawConfig } from "../config/types.openclaw.js";
import { truncateUtf16Safe } from "../utils.js";
import type { CronJob } from "./types.js";

type CronNotificationBrevityConfig = {
  style?: "default" | "brief";
  maxLines?: number;
  maxChars?: number;
  includeReportPaths?: boolean;
};

const DEFAULT_MAX_LINES = 6;
const DEFAULT_MAX_CHARS = 900;
const REPORT_PATH_RE = /\/Users\/[^\s)'"`<>]+(?:\.md|\.json|\.txt|\.log|\.png|\.jpe?g|\.webp)\b/g;

const LOUD_SIGNAL_RE =
  /\b(SALE|SALE_CAPTURED|BUYER|BUYER_SIGNAL|ORDER|PAYMENT|MONEY|REFUND|CHECKOUT|TRUE_HUMAN_GATE|HUMAN_GATE|EXACT_GATE_LANE_SWITCH|FAIL(?:ED|URE)?|ERROR|WARNING|SECURITY|ACCOUNT|CHECKPOINT|2FA|PASSKEY|CAPTCHA|SEND_LOCKED|SENSITIVE_|WRONG_SURFACE|WRONG_PROFILE|DENIED)\b/i;

const MARKER_RE =
  /\b(SHIPPED_UNIT|WORK_UNIT_SHIPPED|REAL_OUTBOUND_SENT|SOCIAL_ACTION_SENT|NO_SAFE_UNIT|EXACT_GATE_LANE_SWITCH|HUMAN_GATE|TRUE_HUMAN_GATE|SEND_LOCKED|SENSITIVE_RESUME_DENIED|NON_NATIVE_TOOLING_BLOCKED|WRONG_SURFACE_DETECTED|WRONG_PROFILE_DEFAULT_BLOCK|USER_PROFILE_AUTONOMY_BLOCK|LATEST_INSTRUCTION_OVERRIDES_MEMORY)\b(?::[^\n]+)?/i;

function resolveBrevityConfig(cfg: OpenClawConfig): CronNotificationBrevityConfig {
  const raw = cfg.cron?.notifications?.telegram;
  return {
    style: raw?.style === "default" ? "default" : "brief",
    maxLines: normalizePositiveInt(raw?.maxLines) ?? DEFAULT_MAX_LINES,
    maxChars: normalizePositiveInt(raw?.maxChars) ?? DEFAULT_MAX_CHARS,
    includeReportPaths: raw?.includeReportPaths !== false,
  };
}

function normalizePositiveInt(value: unknown): number | undefined {
  return typeof value === "number" && Number.isFinite(value) && value > 0
    ? Math.floor(value)
    : undefined;
}

function hasStructuredPayload(payload: ReplyPayload): boolean {
  return (
    payload.mediaUrl !== undefined ||
    (payload.mediaUrls?.length ?? 0) > 0 ||
    (payload.interactive?.blocks?.length ?? 0) > 0 ||
    Object.keys(payload.channelData ?? {}).length > 0
  );
}

function normalizeLines(text: string): string[] {
  return text
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => line.replace(/^[-*]\s+/, "").trim())
    .filter(Boolean);
}

function uniqueValues(values: Iterable<string>): string[] {
  const seen = new Set<string>();
  const result: string[] = [];
  for (const value of values) {
    const trimmed = value.trim();
    if (!trimmed || seen.has(trimmed)) {
      continue;
    }
    seen.add(trimmed);
    result.push(trimmed);
  }
  return result;
}

function extractReportPaths(text: string): string[] {
  return uniqueValues(text.match(REPORT_PATH_RE) ?? []).slice(0, 3);
}

function isLoudNotification(text: string, payload: ReplyPayload): boolean {
  return payload.isError === true || LOUD_SIGNAL_RE.test(text);
}

function isPathHeavy(line: string): boolean {
  return REPORT_PATH_RE.test(line);
}

function chooseDetailLines(lines: string[], marker: string | undefined, maxDetails: number) {
  const picked: string[] = [];
  const skip = new Set<string>();
  if (marker) {
    skip.add(marker);
  }
  for (const line of lines) {
    if (picked.length >= maxDetails) {
      break;
    }
    if (skip.has(line) || isPathHeavy(line) || line.length > 220) {
      continue;
    }
    if (/^(proof|report|artifact|screenshots?)\s*:/i.test(line)) {
      continue;
    }
    picked.push(line);
  }
  return picked;
}

export function formatCronTelegramBrief(params: {
  cfg: OpenClawConfig;
  job: Pick<CronJob, "name">;
  payload: ReplyPayload;
}): ReplyPayload {
  const config = resolveBrevityConfig(params.cfg);
  if (config.style !== "brief" || hasStructuredPayload(params.payload)) {
    return params.payload;
  }
  const rawText = params.payload.text?.trim();
  if (!rawText) {
    return params.payload;
  }
  const maxLines = Math.max(3, config.maxLines ?? DEFAULT_MAX_LINES);
  const maxChars = Math.max(240, config.maxChars ?? DEFAULT_MAX_CHARS);
  const lines = normalizeLines(rawText);
  if (lines.length <= maxLines && rawText.length <= maxChars) {
    return params.payload;
  }

  const marker = rawText.match(MARKER_RE)?.[0]?.trim();
  const reportPaths = config.includeReportPaths ? extractReportPaths(rawText) : [];
  const loud = isLoudNotification(rawText, params.payload);
  const detailBudget = Math.max(1, maxLines - 3 - Math.min(reportPaths.length, 2));
  const detailLines = chooseDetailLines(lines, marker, detailBudget);
  const nextLines = [
    `${loud ? "Alert" : "OpenClaw"}: ${params.job.name}`,
    marker ? `Result: ${marker}` : `Result: ${params.payload.isError ? "ERROR" : "OK"}`,
    ...detailLines.map((line) => `- ${line}`),
    ...reportPaths.slice(0, 2).map((path) => `Report: ${path}`),
    reportPaths.length > 2 ? `More proof: ${reportPaths.length - 2} more path(s) in report` : "",
  ].filter(Boolean);

  let nextText = nextLines.slice(0, maxLines).join("\n");
  if (nextText.length > maxChars) {
    nextText = `${truncateUtf16Safe(nextText, maxChars - 3)}...`;
  }
  return {
    ...params.payload,
    text: nextText,
  };
}

export function applyCronTelegramBrevity(params: {
  cfg: OpenClawConfig;
  job: Pick<CronJob, "name">;
  channel?: string;
  payloads: ReplyPayload[];
}): ReplyPayload[] {
  if (params.channel !== "telegram" || params.payloads.length === 0) {
    return params.payloads;
  }
  return params.payloads.map((payload) =>
    formatCronTelegramBrief({
      cfg: params.cfg,
      job: params.job,
      payload,
    }),
  );
}
