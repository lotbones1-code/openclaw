import {
  resolveAgentConfig,
  resolveAgentDir,
  resolveAgentWorkspaceDir,
  resolveSessionAgentId,
} from "../agents/agent-scope.js";
import { DEFAULT_MODEL, DEFAULT_PROVIDER } from "../agents/defaults.js";
import { compactEmbeddedPiSession } from "../agents/pi-embedded.js";
import { incrementCompactionCount } from "../auto-reply/reply/session-updates.js";
import { getRuntimeConfig } from "../config/config.js";
import { resolveAgentModelPrimaryValue } from "../config/model-input.js";
import {
  loadSessionStore,
  resolveSessionFilePath,
  resolveSessionFilePathOptions,
  resolveSessionTotalTokens,
  type SessionEntry,
} from "../config/sessions.js";
import type { OpenClawConfig } from "../config/types.openclaw.js";
import { type RuntimeEnv, writeRuntimeJson } from "../runtime.js";
import { normalizeOptionalString } from "../shared/string-coerce.js";
import { resolveSessionStoreTargetsOrExit } from "./session-store-targets.js";

export type SessionsCompactOptions = {
  store?: string;
  agent?: string;
  allAgents?: boolean;
  key?: string;
  minTokens?: string;
  inactiveMinutes?: string;
  max?: string;
  dryRun?: boolean;
  force?: boolean;
  json?: boolean;
};

type CompactCandidate = {
  agentId: string;
  storePath: string;
  sessionKey: string;
  sessionId: string;
  sessionFile: string;
  totalTokens: number;
  updatedAt: number;
  contextTokens?: number;
  provider: string;
  model: string;
  selected: boolean;
  reason?: string;
};

type CompactResult = CompactCandidate & {
  ok?: boolean;
  compacted?: boolean;
  tokensBefore?: number;
  tokensAfter?: number;
  failure?: string;
};

const DEFAULT_MIN_TOKENS = 1_000_000;
const DEFAULT_INACTIVE_MINUTES = 30;
const DEFAULT_MAX_COMPACTIONS = 4;

function parsePositiveInteger(raw: string | undefined, fallback: number): number {
  if (raw === undefined) {
    return fallback;
  }
  const parsed = Number.parseInt(raw, 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

function parseModelRef(
  raw: string | undefined,
  defaultProvider: string,
): {
  provider: string;
  model: string;
} {
  const trimmed = normalizeOptionalString(raw);
  if (!trimmed) {
    return { provider: defaultProvider, model: DEFAULT_MODEL };
  }
  const slashIndex = trimmed.indexOf("/");
  if (slashIndex <= 0 || slashIndex === trimmed.length - 1) {
    return { provider: defaultProvider, model: trimmed };
  }
  return {
    provider: trimmed.slice(0, slashIndex).trim() || defaultProvider,
    model: trimmed.slice(slashIndex + 1).trim() || DEFAULT_MODEL,
  };
}

function resolveSessionRuntimeModel(params: {
  cfg: OpenClawConfig;
  agentId: string;
  entry: SessionEntry;
}): { provider: string; model: string } {
  const agentPrimary =
    resolveAgentModelPrimaryValue(resolveAgentConfig(params.cfg, params.agentId)?.model) ??
    resolveAgentModelPrimaryValue(params.cfg.agents?.defaults?.model);
  const fallback = parseModelRef(agentPrimary, DEFAULT_PROVIDER);
  const override = normalizeOptionalString(params.entry.modelOverride);
  if (override) {
    return parseModelRef(
      override,
      normalizeOptionalString(params.entry.providerOverride) ?? fallback.provider,
    );
  }
  const storedModel = normalizeOptionalString(params.entry.model);
  if (storedModel) {
    return parseModelRef(
      storedModel,
      normalizeOptionalString(params.entry.modelProvider) ?? fallback.provider,
    );
  }
  return fallback;
}

function collectCompactCandidates(params: {
  cfg: OpenClawConfig;
  opts: SessionsCompactOptions;
  now: number;
  runtime: RuntimeEnv;
}): Array<{ candidate: CompactCandidate; entry: SessionEntry }> | null {
  const targets = resolveSessionStoreTargetsOrExit({
    cfg: params.cfg,
    opts: {
      store: params.opts.store,
      agent: params.opts.agent,
      allAgents: params.opts.allAgents,
    },
    runtime: params.runtime,
  });
  return (
    targets?.flatMap((target) => {
      const store = loadSessionStore(target.storePath, { skipCache: true });
      const sessionPathOptions = resolveSessionFilePathOptions({
        agentId: target.agentId,
        storePath: target.storePath,
      });
      return Object.entries(store).flatMap(([sessionKey, entry]) => {
        if (params.opts.key && sessionKey !== params.opts.key) {
          return [];
        }
        const sessionId = normalizeOptionalString(entry.sessionId);
        if (!sessionId) {
          return [];
        }
        const totalTokens = resolveSessionTotalTokens(entry);
        if (totalTokens === undefined) {
          return [];
        }
        const sessionAgentId = resolveSessionAgentId({ sessionKey, config: params.cfg });
        const runtimeModel = resolveSessionRuntimeModel({
          cfg: params.cfg,
          agentId: sessionAgentId,
          entry,
        });
        const updatedAt = typeof entry.updatedAt === "number" ? entry.updatedAt : 0;
        const candidate: CompactCandidate = {
          agentId: sessionAgentId,
          storePath: target.storePath,
          sessionKey,
          sessionId,
          sessionFile: resolveSessionFilePath(sessionId, entry, sessionPathOptions),
          totalTokens,
          updatedAt,
          ...(typeof entry.contextTokens === "number"
            ? { contextTokens: entry.contextTokens }
            : {}),
          provider: runtimeModel.provider,
          model: runtimeModel.model,
          selected: false,
        };
        return [{ candidate, entry }];
      });
    }) ?? null
  );
}

function selectCandidates(params: {
  candidates: Array<{ candidate: CompactCandidate; entry: SessionEntry }>;
  minTokens: number;
  inactiveMinutes: number;
  max: number;
  force: boolean;
  now: number;
}): Array<{ candidate: CompactCandidate; entry: SessionEntry }> {
  let selected = 0;
  return params.candidates
    .toSorted((left, right) => right.candidate.totalTokens - left.candidate.totalTokens)
    .map((item) => {
      const inactiveMs = Math.max(0, params.now - item.candidate.updatedAt);
      const activeRecently = inactiveMs < params.inactiveMinutes * 60_000;
      const next = { ...item.candidate };
      if (item.candidate.totalTokens < params.minTokens) {
        next.reason = "below_min_tokens";
      } else if (!params.force && activeRecently) {
        next.reason = "active_recently";
      } else if (selected >= params.max) {
        next.reason = "max_compactions_reached";
      } else {
        next.selected = true;
        selected += 1;
      }
      return { candidate: next, entry: item.entry };
    });
}

export async function sessionsCompactCommand(opts: SessionsCompactOptions, runtime: RuntimeEnv) {
  const cfg = getRuntimeConfig();
  const minTokens = parsePositiveInteger(opts.minTokens, DEFAULT_MIN_TOKENS);
  const inactiveMinutes = parsePositiveInteger(opts.inactiveMinutes, DEFAULT_INACTIVE_MINUTES);
  const max = parsePositiveInteger(opts.max, DEFAULT_MAX_COMPACTIONS);
  const now = Date.now();
  const collected = collectCompactCandidates({
    cfg,
    opts,
    now,
    runtime,
  });
  if (!collected) {
    return;
  }
  const planned = selectCandidates({
    candidates: collected,
    minTokens,
    inactiveMinutes,
    max,
    force: Boolean(opts.force),
    now,
  });

  if (opts.dryRun) {
    const candidates = planned.map(({ candidate }) => candidate);
    if (opts.json) {
      writeRuntimeJson(runtime, {
        dryRun: true,
        minTokens,
        inactiveMinutes,
        max,
        selected: candidates.filter((candidate) => candidate.selected).length,
        candidates,
      });
      return;
    }
    runtime.log(
      `Session compaction dry-run: ${candidates.filter((candidate) => candidate.selected).length} selected.`,
    );
    for (const candidate of candidates) {
      runtime.log(
        `${candidate.selected ? "select" : "skip"} ${candidate.sessionKey} ${candidate.totalTokens} tokens${candidate.reason ? ` (${candidate.reason})` : ""}`,
      );
    }
    return;
  }

  const results: CompactResult[] = [];
  for (const { candidate, entry } of planned.filter((item) => item.candidate.selected)) {
    const sessionStore = loadSessionStore(candidate.storePath, { skipCache: true });
    const latestEntry = sessionStore[candidate.sessionKey] ?? entry;
    const result = await compactEmbeddedPiSession({
      sessionId: latestEntry.sessionId,
      sessionKey: candidate.sessionKey,
      allowGatewaySubagentBinding: true,
      sessionFile: candidate.sessionFile,
      workspaceDir: resolveAgentWorkspaceDir(cfg, candidate.agentId),
      agentDir: resolveAgentDir(cfg, candidate.agentId),
      config: cfg,
      skillsSnapshot: latestEntry.skillsSnapshot,
      provider: candidate.provider,
      model: candidate.model,
      agentHarnessId: latestEntry.agentHarnessId,
      thinkLevel: "off",
      bashElevated: {
        enabled: false,
        allowed: false,
        defaultLevel: "off",
      },
      trigger: "manual",
      customInstructions:
        "Native OpenClaw session compaction maintenance. Preserve decisions, active directives, exact gates, proof paths, and next safe actions.",
      senderIsOwner: true,
    });
    if (result.ok && result.compacted) {
      await incrementCompactionCount({
        cfg,
        sessionEntry: latestEntry,
        sessionStore,
        sessionKey: candidate.sessionKey,
        storePath: candidate.storePath,
        tokensAfter: result.result?.tokensAfter,
        newSessionId: result.result?.sessionId,
        newSessionFile: result.result?.sessionFile,
      });
    }
    results.push({
      ...candidate,
      ok: result.ok,
      compacted: result.compacted,
      tokensBefore: result.result?.tokensBefore,
      tokensAfter: result.result?.tokensAfter,
      ...(result.ok ? {} : { failure: result.reason ?? "compaction_failed" }),
    });
  }

  if (opts.json) {
    writeRuntimeJson(runtime, {
      dryRun: false,
      minTokens,
      inactiveMinutes,
      max,
      results,
      skipped: planned.filter((item) => !item.candidate.selected).map((item) => item.candidate),
    });
    return;
  }
  runtime.log(`Session compaction applied: ${results.length} attempted.`);
  for (const result of results) {
    runtime.log(
      `${result.compacted ? "compacted" : "not_compacted"} ${result.sessionKey}${result.tokensBefore ? ` ${result.tokensBefore} -> ${result.tokensAfter ?? "?"}` : ""}${result.failure ? ` (${result.failure})` : ""}`,
    );
  }
}
