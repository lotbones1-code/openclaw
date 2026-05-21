import fs from "node:fs/promises";
import path from "node:path";
import type { AgentContextInjection } from "../config/types.agent-defaults.js";
import type { OpenClawConfig } from "../config/types.openclaw.js";
import { normalizeOptionalString } from "../shared/string-coerce.js";
import { resolveSessionAgentIds } from "./agent-scope.js";
import { getOrLoadBootstrapFiles } from "./bootstrap-cache.js";
import { applyBootstrapHookOverrides } from "./bootstrap-hooks.js";
import { shouldIncludeHeartbeatGuidanceForSystemPrompt } from "./heartbeat-system-prompt.js";
import type { EmbeddedContextFile } from "./pi-embedded-helpers.js";
import {
  buildBootstrapContextFiles,
  resolveBootstrapMaxChars,
  resolveBootstrapTotalMaxChars,
} from "./pi-embedded-helpers.js";
import {
  DEFAULT_AGENTS_FILENAME,
  DEFAULT_CLAUDE_FILENAME,
  DEFAULT_HEARTBEAT_FILENAME,
  DEFAULT_IDENTITY_FILENAME,
  DEFAULT_SOUL_FILENAME,
  filterBootstrapFilesForSession,
  isWorkspaceBootstrapPending,
  loadWorkspaceBootstrapFiles,
  type WorkspaceBootstrapFile,
} from "./workspace.js";

export type BootstrapContextMode = "full" | "lightweight";
export type BootstrapContextRunKind = "default" | "heartbeat" | "cron";

const CONTINUATION_SCAN_MAX_TAIL_BYTES = 256 * 1024;
const CONTINUATION_SCAN_MAX_RECORDS = 500;
export const FULL_BOOTSTRAP_COMPLETED_CUSTOM_TYPE = "openclaw:bootstrap-context:full";
const BOOTSTRAP_WARNING_DEDUPE_LIMIT = 1024;
const seenBootstrapWarnings = new Set<string>();
const bootstrapWarningOrder: string[] = [];
const CRITICAL_AUTHORITY_REQUIRED_SECTIONS = [
  "Authority Order",
  "Native Only",
  "Stop And Interrupt",
  "Proof And Reporting",
  "Selected Option Fidelity",
  "Typed Gates",
  "Safe Work Continues",
  "Learning OS",
  "Self-Builder Discipline",
] as const;
const CRITICAL_AUTHORITY_SECTION_ALIASES: Record<
  (typeof CRITICAL_AUTHORITY_REQUIRED_SECTIONS)[number],
  string[]
> = {
  "Authority Order": ["Authority Order"],
  "Native Only": ["Native Only"],
  "Stop And Interrupt": ["Stop And Interrupt"],
  "Proof And Reporting": ["Proof And Reporting"],
  "Selected Option Fidelity": ["Selected Option Fidelity", "Execution Kernel"],
  "Typed Gates": ["Typed Gates", "Execution Kernel"],
  "Safe Work Continues": ["Safe Work Continues", "Always-On Directive Drain"],
  "Learning OS": ["Learning OS", "Learning And Success Notes"],
  "Self-Builder Discipline": ["Self-Builder Discipline", "Self-Builder Contract"],
};
const CRITICAL_AUTHORITY_FILE_NAMES = new Set([
  DEFAULT_AGENTS_FILENAME,
  DEFAULT_IDENTITY_FILENAME,
  DEFAULT_CLAUDE_FILENAME,
  DEFAULT_SOUL_FILENAME,
]);

function rememberBootstrapWarning(key: string): boolean {
  if (seenBootstrapWarnings.has(key)) {
    return false;
  }
  if (seenBootstrapWarnings.size >= BOOTSTRAP_WARNING_DEDUPE_LIMIT) {
    const oldest = bootstrapWarningOrder.shift();
    if (oldest) {
      seenBootstrapWarnings.delete(oldest);
    }
  }
  seenBootstrapWarnings.add(key);
  bootstrapWarningOrder.push(key);
  return true;
}

export function _resetBootstrapWarningCacheForTest(): void {
  seenBootstrapWarnings.clear();
  bootstrapWarningOrder.length = 0;
}

export function resolveContextInjectionMode(config?: OpenClawConfig): AgentContextInjection {
  return config?.agents?.defaults?.contextInjection ?? "always";
}

export async function hasCompletedBootstrapTurn(sessionFile: string): Promise<boolean> {
  try {
    const stat = await fs.lstat(sessionFile);
    if (stat.isSymbolicLink()) {
      return false;
    }

    const fh = await fs.open(sessionFile, "r");
    try {
      const bytesToRead = Math.min(stat.size, CONTINUATION_SCAN_MAX_TAIL_BYTES);
      if (bytesToRead <= 0) {
        return false;
      }
      const start = stat.size - bytesToRead;
      const buffer = Buffer.allocUnsafe(bytesToRead);
      const { bytesRead } = await fh.read(buffer, 0, bytesToRead, start);
      let text = buffer.toString("utf-8", 0, bytesRead);
      if (start > 0) {
        const firstNewline = text.indexOf("\n");
        if (firstNewline === -1) {
          return false;
        }
        text = text.slice(firstNewline + 1);
      }

      const records = text
        .split(/\r?\n/u)
        .filter((line) => line.trim().length > 0)
        .slice(-CONTINUATION_SCAN_MAX_RECORDS);
      let compactedAfterLatestAssistant = false;

      for (let i = records.length - 1; i >= 0; i--) {
        const line = records[i];
        if (!line) {
          continue;
        }
        let entry: unknown;
        try {
          entry = JSON.parse(line);
        } catch {
          continue;
        }
        const record = entry as
          | {
              type?: string;
              customType?: string;
              message?: { role?: string };
            }
          | null
          | undefined;
        if (record?.type === "compaction") {
          compactedAfterLatestAssistant = true;
          continue;
        }
        if (
          record?.type === "custom" &&
          record.customType === FULL_BOOTSTRAP_COMPLETED_CUSTOM_TYPE
        ) {
          return !compactedAfterLatestAssistant;
        }
      }

      return false;
    } finally {
      await fh.close();
    }
  } catch {
    return false;
  }
}

export function makeBootstrapWarn(params: {
  sessionLabel: string;
  workspaceDir?: string;
  warn?: (message: string) => void;
}): ((message: string) => void) | undefined {
  const warn = params.warn;
  if (!warn) {
    return undefined;
  }
  const workspacePrefix = params.workspaceDir ?? "";
  return (message: string) => {
    const key = `${workspacePrefix}\u0000${params.sessionLabel}\u0000${message}`;
    if (!rememberBootstrapWarning(key)) {
      return;
    }
    warn(`${message} (sessionKey=${params.sessionLabel})`);
  };
}

function sanitizeBootstrapFiles(
  files: WorkspaceBootstrapFile[],
  workspaceDir: string,
  warn?: (message: string) => void,
): WorkspaceBootstrapFile[] {
  const workspaceRoot = path.resolve(workspaceDir);
  const seenPaths = new Set<string>();
  const sanitized: WorkspaceBootstrapFile[] = [];
  for (const file of files) {
    const pathValue = normalizeOptionalString(file.path) ?? "";
    if (!pathValue) {
      warn?.(
        `skipping bootstrap file "${file.name}" — missing or invalid "path" field (hook may have used "filePath" instead)`,
      );
      continue;
    }
    const resolvedPath = path.isAbsolute(pathValue)
      ? path.resolve(pathValue)
      : path.resolve(workspaceRoot, pathValue);
    const dedupeKey = path.normalize(path.relative(workspaceRoot, resolvedPath));
    if (seenPaths.has(dedupeKey)) {
      continue;
    }
    seenPaths.add(dedupeKey);
    sanitized.push({ ...file, path: resolvedPath });
  }
  return sanitized;
}

function applyContextModeFilter(params: {
  files: WorkspaceBootstrapFile[];
  contextMode?: BootstrapContextMode;
  runKind?: BootstrapContextRunKind;
}): WorkspaceBootstrapFile[] {
  const contextMode = params.contextMode ?? "full";
  const runKind = params.runKind ?? "default";
  if (contextMode !== "lightweight") {
    return params.files;
  }
  if (runKind === "heartbeat") {
    return params.files.filter((file) => file.name === "HEARTBEAT.md");
  }
  // cron/default lightweight mode keeps bootstrap context empty on purpose.
  return [];
}

function shouldExcludeHeartbeatBootstrapFile(params: {
  config?: OpenClawConfig;
  sessionKey?: string;
  sessionId?: string;
  agentId?: string;
  runKind?: BootstrapContextRunKind;
}): boolean {
  if (!params.config || params.runKind === "heartbeat") {
    return false;
  }
  const { defaultAgentId, sessionAgentId } = resolveSessionAgentIds({
    sessionKey: params.sessionKey ?? params.sessionId,
    config: params.config,
    agentId: params.agentId,
  });
  if (sessionAgentId !== defaultAgentId) {
    return false;
  }
  return !shouldIncludeHeartbeatGuidanceForSystemPrompt({
    config: params.config,
    agentId: sessionAgentId,
    defaultAgentId,
  });
}

function filterHeartbeatBootstrapFile(
  files: WorkspaceBootstrapFile[],
  excludeHeartbeatBootstrapFile: boolean,
): WorkspaceBootstrapFile[] {
  if (!excludeHeartbeatBootstrapFile) {
    return files;
  }
  return files.filter((file) => file.name !== DEFAULT_HEARTBEAT_FILENAME);
}

function isExecutionKernelEnabled(config?: OpenClawConfig): boolean {
  const candidate = config as
    | (OpenClawConfig & { executionKernel?: { enabled?: boolean } })
    | undefined;
  return candidate?.executionKernel?.enabled === true;
}

function isContextAuthorityEnabled(config?: OpenClawConfig): boolean {
  const candidate = config as
    | (OpenClawConfig & {
        executionKernel?: { contextAuthority?: { enabled?: boolean } };
      })
    | undefined;
  return candidate?.executionKernel?.contextAuthority?.enabled === true;
}

function normalizeHeading(value: string): string {
  return value.trim().toLowerCase().replace(/\s+/g, " ");
}

function extractMarkdownHeadings(content: string): Set<string> {
  const headings = new Set<string>();
  let inCodeBlock = false;
  for (const line of content.split("\n")) {
    if (line.trimStart().startsWith("```")) {
      inCodeBlock = !inCodeBlock;
      continue;
    }
    if (inCodeBlock) {
      continue;
    }
    const match = line.match(/^#{2,3}\s+(.+?)\s*$/);
    if (match?.[1]) {
      headings.add(normalizeHeading(match[1]));
    }
  }
  return headings;
}

function resolveMissingCriticalAuthoritySections(files: WorkspaceBootstrapFile[]): string[] {
  const agents = files.find((file) => file.name === DEFAULT_AGENTS_FILENAME && !file.missing);
  if (!agents?.content) {
    // Lightweight cron/default runs intentionally inject no bootstrap files.
    // Context authority validation should fail incomplete authority when it is
    // present, not turn intentionally empty lightweight context into a hard gate.
    return [];
  }
  const headings = extractMarkdownHeadings(agents.content);
  return CRITICAL_AUTHORITY_REQUIRED_SECTIONS.filter((section) => {
    const aliases = CRITICAL_AUTHORITY_SECTION_ALIASES[section] ?? [section];
    return !aliases.some((alias) => headings.has(normalizeHeading(alias)));
  });
}

function assertCriticalBootstrapAuthorityFits(params: {
  files: WorkspaceBootstrapFile[];
  config?: OpenClawConfig;
}): void {
  if (!isExecutionKernelEnabled(params.config) && !isContextAuthorityEnabled(params.config)) {
    return;
  }
  const maxChars = resolveBootstrapMaxChars(params.config);
  const offenders = params.files
    .filter((file) => CRITICAL_AUTHORITY_FILE_NAMES.has(file.name) && !file.missing)
    .filter((file) => (file.content ?? "").length > maxChars)
    .map((file) => `${file.name}:${(file.content ?? "").length}/${maxChars}`);
  const missingSections = isContextAuthorityEnabled(params.config)
    ? resolveMissingCriticalAuthoritySections(params.files)
    : [];
  if (offenders.length === 0 && missingSections.length === 0) {
    return;
  }
  if (isContextAuthorityEnabled(params.config)) {
    const details = [
      offenders.length > 0 ? `over budget: ${offenders.join(", ")}` : "",
      missingSections.length > 0 ? `missing critical sections: ${missingSections.join(", ")}` : "",
    ]
      .filter(Boolean)
      .join("; ");
    throw new Error(
      `BOOTSTRAP_CRITICAL_AUTHORITY_INVALID: critical bootstrap authority would be incomplete (${details}). Compact or split authority before injection.`,
    );
  }
  throw new Error(
    `BOOTSTRAP_CRITICAL_TRUNCATED: critical bootstrap authority exceeds bootstrapMaxChars (${offenders.join(", ")}). Compact or split authority before injection.`,
  );
}

export async function resolveBootstrapFilesForRun(params: {
  workspaceDir: string;
  config?: OpenClawConfig;
  sessionKey?: string;
  sessionId?: string;
  agentId?: string;
  warn?: (message: string) => void;
  contextMode?: BootstrapContextMode;
  runKind?: BootstrapContextRunKind;
}): Promise<WorkspaceBootstrapFile[]> {
  const excludeHeartbeatBootstrapFile = shouldExcludeHeartbeatBootstrapFile(params);
  const sessionKey = params.sessionKey ?? params.sessionId;
  const rawFiles = params.sessionKey
    ? await getOrLoadBootstrapFiles({
        workspaceDir: params.workspaceDir,
        sessionKey: params.sessionKey,
      })
    : await loadWorkspaceBootstrapFiles(params.workspaceDir);
  const bootstrapFiles = applyContextModeFilter({
    files: filterBootstrapFilesForSession(rawFiles, sessionKey),
    contextMode: params.contextMode,
    runKind: params.runKind,
  });

  const updated = await applyBootstrapHookOverrides({
    files: bootstrapFiles,
    workspaceDir: params.workspaceDir,
    config: params.config,
    sessionKey: params.sessionKey,
    sessionId: params.sessionId,
    agentId: params.agentId,
  });
  return sanitizeBootstrapFiles(
    filterHeartbeatBootstrapFile(updated, excludeHeartbeatBootstrapFile),
    params.workspaceDir,
    params.warn,
  );
}

export async function resolveBootstrapContextForRun(params: {
  workspaceDir: string;
  config?: OpenClawConfig;
  sessionKey?: string;
  sessionId?: string;
  agentId?: string;
  warn?: (message: string) => void;
  contextMode?: BootstrapContextMode;
  runKind?: BootstrapContextRunKind;
}): Promise<{
  bootstrapFiles: WorkspaceBootstrapFile[];
  contextFiles: EmbeddedContextFile[];
}> {
  const bootstrapFiles = await resolveBootstrapFilesForRun(params);
  assertCriticalBootstrapAuthorityFits({ files: bootstrapFiles, config: params.config });
  const contextFiles = buildBootstrapContextFiles(bootstrapFiles, {
    maxChars: resolveBootstrapMaxChars(params.config),
    totalMaxChars: resolveBootstrapTotalMaxChars(params.config),
    warn: params.warn,
  });
  return { bootstrapFiles, contextFiles };
}

export { isWorkspaceBootstrapPending };
