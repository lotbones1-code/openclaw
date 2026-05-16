import crypto from "node:crypto";
import { chmodSync, existsSync, mkdirSync } from "node:fs";
import type { DatabaseSync, StatementSync } from "node:sqlite";
import { requireNodeSqlite } from "../infra/node-sqlite.js";
import { configureSqliteWalMaintenance, type SqliteWalMaintenance } from "../infra/sqlite-wal.js";
import { normalizeOptionalString } from "../shared/string-coerce.js";
import { resolveTaskRegistryDir, resolveTaskRegistrySqlitePath } from "./task-registry.paths.js";

export type PolicyLockState = "LOCKED" | "UNLOCKED";
export type PolicyUnlockState = "ACTIVE" | "USED" | "EXPIRED";
export type PolicyLockDecision = "LOCKED" | "UNLOCKED" | "DENIED" | "EXPIRED" | "USED";

export type PolicyLockRecord = {
  lockId: string;
  state: PolicyLockState;
  updatedAt: number;
  source?: string;
  reason?: string;
};

export type PolicyUnlockRecord = {
  unlockId: string;
  lockId: string;
  taskId?: string;
  lane?: string;
  action?: string;
  account?: string;
  targetClass?: string;
  recipientHash?: string;
  templateId?: string;
  maxCount?: string;
  cadence?: string;
  approvalText?: string;
  proofPath?: string;
  stopInstruction?: string;
  rollbackInstruction?: string;
  expiresAt: number;
  usedAt?: number;
  state: PolicyUnlockState;
  createdAt: number;
  source?: string;
};

export type PolicyLockAuditRecord = {
  auditId: string;
  timestamp: number;
  lockId?: string;
  unlockId?: string;
  taskId?: string;
  sessionKey?: string;
  runId?: string;
  lane?: string;
  action?: string;
  source?: string;
  channel?: string;
  decision: PolicyLockDecision;
  reasonCode: string;
  proofPath?: string;
  detail?: string;
};

type LockRow = {
  lock_id: string;
  state: PolicyLockState;
  updated_at: number | bigint;
  source: string | null;
  reason: string | null;
};

type UnlockRow = {
  unlock_id: string;
  lock_id: string;
  task_id: string | null;
  lane: string | null;
  action: string | null;
  account: string | null;
  target_class: string | null;
  recipient_hash: string | null;
  template_id: string | null;
  max_count: string | null;
  cadence: string | null;
  approval_text: string | null;
  proof_path: string | null;
  stop_instruction: string | null;
  rollback_instruction: string | null;
  expires_at: number | bigint;
  used_at: number | bigint | null;
  state: PolicyUnlockState;
  created_at: number | bigint;
  source: string | null;
};

type AuditRow = {
  audit_id: string;
  timestamp: number | bigint;
  lock_id: string | null;
  unlock_id: string | null;
  task_id: string | null;
  session_key: string | null;
  run_id: string | null;
  lane: string | null;
  action: string | null;
  source: string | null;
  channel: string | null;
  decision: PolicyLockDecision;
  reason_code: string;
  proof_path: string | null;
  detail: string | null;
};

type PolicyLockStatements = {
  upsertLock: StatementSync;
  selectLocks: StatementSync;
  selectLockById: StatementSync;
  insertUnlock: StatementSync;
  selectUnlocks: StatementSync;
  selectActiveUnlocksForLock: StatementSync;
  useUnlock: StatementSync;
  expireUnlock: StatementSync;
  insertAudit: StatementSync;
  selectAudit: StatementSync;
};

type PolicyLockDatabase = {
  db: DatabaseSync;
  path: string;
  statements: PolicyLockStatements;
  walMaintenance: SqliteWalMaintenance;
};

export const DEFAULT_POLICY_LOCKS = [
  "b2b:send",
  "public_social:post",
  "public_social:comment",
  "public_social:dm_cold",
  "payment:refund",
  "payment:order",
  "checkout:mutation",
  "dns:mutation",
  "mailbox:admin",
  "account:security",
  "api:provider_mutation",
] as const;

const TASK_REGISTRY_DIR_MODE = 0o700;
const TASK_REGISTRY_FILE_MODE = 0o600;
const TASK_REGISTRY_SIDECAR_SUFFIXES = ["", "-shm", "-wal"] as const;
const DEFAULT_UNLOCK_TTL_MS = 60 * 60 * 1000;

let cachedDatabase: PolicyLockDatabase | null = null;

function normalizeNumber(value: number | bigint | null): number | undefined {
  if (typeof value === "bigint") {
    return Number(value);
  }
  return typeof value === "number" ? value : undefined;
}

function ensureTaskRegistryPermissions(pathname: string) {
  const dir = resolveTaskRegistryDir(process.env);
  mkdirSync(dir, { recursive: true, mode: TASK_REGISTRY_DIR_MODE });
  chmodSync(dir, TASK_REGISTRY_DIR_MODE);
  for (const suffix of TASK_REGISTRY_SIDECAR_SUFFIXES) {
    const candidate = `${pathname}${suffix}`;
    if (!existsSync(candidate)) {
      continue;
    }
    chmodSync(candidate, TASK_REGISTRY_FILE_MODE);
  }
}

function ensureSchema(db: DatabaseSync) {
  db.exec(`
    CREATE TABLE IF NOT EXISTS policy_locks (
      lock_id TEXT PRIMARY KEY,
      state TEXT NOT NULL,
      updated_at INTEGER NOT NULL,
      source TEXT,
      reason TEXT
    );
  `);
  db.exec(`
    CREATE TABLE IF NOT EXISTS policy_lock_unlocks (
      unlock_id TEXT PRIMARY KEY,
      lock_id TEXT NOT NULL,
      task_id TEXT,
      lane TEXT,
      action TEXT,
      account TEXT,
      target_class TEXT,
      recipient_hash TEXT,
      template_id TEXT,
      max_count TEXT,
      cadence TEXT,
      approval_text TEXT,
      proof_path TEXT,
      stop_instruction TEXT,
      rollback_instruction TEXT,
      expires_at INTEGER NOT NULL,
      used_at INTEGER,
      state TEXT NOT NULL,
      created_at INTEGER NOT NULL,
      source TEXT
    );
  `);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_policy_unlock_lock ON policy_lock_unlocks(lock_id);`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_policy_unlock_state ON policy_lock_unlocks(state);`);
  db.exec(`
    CREATE TABLE IF NOT EXISTS policy_lock_audit (
      audit_id TEXT PRIMARY KEY,
      timestamp INTEGER NOT NULL,
      lock_id TEXT,
      unlock_id TEXT,
      task_id TEXT,
      session_key TEXT,
      run_id TEXT,
      lane TEXT,
      action TEXT,
      source TEXT,
      channel TEXT,
      decision TEXT NOT NULL,
      reason_code TEXT NOT NULL,
      proof_path TEXT,
      detail TEXT
    );
  `);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_policy_audit_timestamp ON policy_lock_audit(timestamp);`);
}

function createStatements(db: DatabaseSync): PolicyLockStatements {
  return {
    upsertLock: db.prepare(`
      INSERT INTO policy_locks (lock_id, state, updated_at, source, reason)
      VALUES (@lock_id, @state, @updated_at, @source, @reason)
      ON CONFLICT(lock_id) DO UPDATE SET
        state = excluded.state,
        updated_at = excluded.updated_at,
        source = excluded.source,
        reason = excluded.reason
    `),
    selectLocks: db.prepare(`
      SELECT lock_id, state, updated_at, source, reason
      FROM policy_locks
      ORDER BY lock_id ASC
    `),
    selectLockById: db.prepare(`
      SELECT lock_id, state, updated_at, source, reason
      FROM policy_locks
      WHERE lock_id = ?
    `),
    insertUnlock: db.prepare(`
      INSERT INTO policy_lock_unlocks (
        unlock_id,
        lock_id,
        task_id,
        lane,
        action,
        account,
        target_class,
        recipient_hash,
        template_id,
        max_count,
        cadence,
        approval_text,
        proof_path,
        stop_instruction,
        rollback_instruction,
        expires_at,
        used_at,
        state,
        created_at,
        source
      ) VALUES (
        @unlock_id,
        @lock_id,
        @task_id,
        @lane,
        @action,
        @account,
        @target_class,
        @recipient_hash,
        @template_id,
        @max_count,
        @cadence,
        @approval_text,
        @proof_path,
        @stop_instruction,
        @rollback_instruction,
        @expires_at,
        @used_at,
        @state,
        @created_at,
        @source
      )
    `),
    selectUnlocks: db.prepare(`
      SELECT
        unlock_id,
        lock_id,
        task_id,
        lane,
        action,
        account,
        target_class,
        recipient_hash,
        template_id,
        max_count,
        cadence,
        approval_text,
        proof_path,
        stop_instruction,
        rollback_instruction,
        expires_at,
        used_at,
        state,
        created_at,
        source
      FROM policy_lock_unlocks
      ORDER BY created_at DESC, unlock_id DESC
    `),
    selectActiveUnlocksForLock: db.prepare(`
      SELECT
        unlock_id,
        lock_id,
        task_id,
        lane,
        action,
        account,
        target_class,
        recipient_hash,
        template_id,
        max_count,
        cadence,
        approval_text,
        proof_path,
        stop_instruction,
        rollback_instruction,
        expires_at,
        used_at,
        state,
        created_at,
        source
      FROM policy_lock_unlocks
      WHERE lock_id = ? AND state = 'ACTIVE'
      ORDER BY created_at ASC, unlock_id ASC
    `),
    useUnlock: db.prepare(`
      UPDATE policy_lock_unlocks
      SET state = 'USED', used_at = ?
      WHERE unlock_id = ?
    `),
    expireUnlock: db.prepare(`
      UPDATE policy_lock_unlocks
      SET state = 'EXPIRED'
      WHERE unlock_id = ?
    `),
    insertAudit: db.prepare(`
      INSERT INTO policy_lock_audit (
        audit_id,
        timestamp,
        lock_id,
        unlock_id,
        task_id,
        session_key,
        run_id,
        lane,
        action,
        source,
        channel,
        decision,
        reason_code,
        proof_path,
        detail
      ) VALUES (
        @audit_id,
        @timestamp,
        @lock_id,
        @unlock_id,
        @task_id,
        @session_key,
        @run_id,
        @lane,
        @action,
        @source,
        @channel,
        @decision,
        @reason_code,
        @proof_path,
        @detail
      )
    `),
    selectAudit: db.prepare(`
      SELECT
        audit_id,
        timestamp,
        lock_id,
        unlock_id,
        task_id,
        session_key,
        run_id,
        lane,
        action,
        source,
        channel,
        decision,
        reason_code,
        proof_path,
        detail
      FROM policy_lock_audit
      ORDER BY timestamp DESC, audit_id DESC
    `),
  };
}

function openPolicyLockDatabase(): PolicyLockDatabase {
  const pathname = resolveTaskRegistrySqlitePath(process.env);
  if (cachedDatabase && cachedDatabase.path === pathname) {
    return cachedDatabase;
  }
  if (cachedDatabase) {
    cachedDatabase.walMaintenance.close();
    cachedDatabase.db.close();
    cachedDatabase = null;
  }
  ensureTaskRegistryPermissions(pathname);
  const { DatabaseSync } = requireNodeSqlite();
  const db = new DatabaseSync(pathname);
  const walMaintenance = configureSqliteWalMaintenance(db);
  db.exec(`PRAGMA synchronous = NORMAL;`);
  db.exec(`PRAGMA busy_timeout = 5000;`);
  ensureSchema(db);
  ensureTaskRegistryPermissions(pathname);
  cachedDatabase = {
    db,
    path: pathname,
    statements: createStatements(db),
    walMaintenance,
  };
  ensureDefaultPolicyLocks();
  return cachedDatabase;
}

function lockRowToRecord(row: LockRow): PolicyLockRecord {
  return {
    lockId: row.lock_id,
    state: row.state,
    updatedAt: normalizeNumber(row.updated_at) ?? 0,
    ...(row.source ? { source: row.source } : {}),
    ...(row.reason ? { reason: row.reason } : {}),
  };
}

function unlockRowToRecord(row: UnlockRow): PolicyUnlockRecord {
  const usedAt = normalizeNumber(row.used_at);
  return {
    unlockId: row.unlock_id,
    lockId: row.lock_id,
    ...(row.task_id ? { taskId: row.task_id } : {}),
    ...(row.lane ? { lane: row.lane } : {}),
    ...(row.action ? { action: row.action } : {}),
    ...(row.account ? { account: row.account } : {}),
    ...(row.target_class ? { targetClass: row.target_class } : {}),
    ...(row.recipient_hash ? { recipientHash: row.recipient_hash } : {}),
    ...(row.template_id ? { templateId: row.template_id } : {}),
    ...(row.max_count ? { maxCount: row.max_count } : {}),
    ...(row.cadence ? { cadence: row.cadence } : {}),
    ...(row.approval_text ? { approvalText: row.approval_text } : {}),
    ...(row.proof_path ? { proofPath: row.proof_path } : {}),
    ...(row.stop_instruction ? { stopInstruction: row.stop_instruction } : {}),
    ...(row.rollback_instruction ? { rollbackInstruction: row.rollback_instruction } : {}),
    expiresAt: normalizeNumber(row.expires_at) ?? 0,
    ...(usedAt != null ? { usedAt } : {}),
    state: row.state,
    createdAt: normalizeNumber(row.created_at) ?? 0,
    ...(row.source ? { source: row.source } : {}),
  };
}

function auditRowToRecord(row: AuditRow): PolicyLockAuditRecord {
  return {
    auditId: row.audit_id,
    timestamp: normalizeNumber(row.timestamp) ?? 0,
    ...(row.lock_id ? { lockId: row.lock_id } : {}),
    ...(row.unlock_id ? { unlockId: row.unlock_id } : {}),
    ...(row.task_id ? { taskId: row.task_id } : {}),
    ...(row.session_key ? { sessionKey: row.session_key } : {}),
    ...(row.run_id ? { runId: row.run_id } : {}),
    ...(row.lane ? { lane: row.lane } : {}),
    ...(row.action ? { action: row.action } : {}),
    ...(row.source ? { source: row.source } : {}),
    ...(row.channel ? { channel: row.channel } : {}),
    decision: row.decision,
    reasonCode: row.reason_code,
    ...(row.proof_path ? { proofPath: row.proof_path } : {}),
    ...(row.detail ? { detail: row.detail } : {}),
  };
}

function optional(value: string | undefined): string | null {
  return normalizeOptionalString(value) ?? null;
}

export function ensureDefaultPolicyLocks(now = Date.now()): void {
  const db = cachedDatabase;
  if (!db) {
    return;
  }
  for (const lockId of DEFAULT_POLICY_LOCKS) {
    const row = db.statements.selectLockById.get(lockId) as LockRow | undefined;
    if (row) {
      continue;
    }
    db.statements.upsertLock.run({
      lock_id: lockId,
      state: "LOCKED",
      updated_at: now,
      source: "openclaw-default",
      reason: "default-sensitive-action-lock",
    });
  }
}

export function setPolicyLock(params: {
  lockId: string;
  state?: PolicyLockState;
  source?: string;
  reason?: string;
  now?: number;
}): PolicyLockRecord {
  const lockId = normalizeOptionalString(params.lockId);
  if (!lockId) {
    throw new Error("lockId is required");
  }
  const state = params.state ?? "LOCKED";
  openPolicyLockDatabase().statements.upsertLock.run({
    lock_id: lockId,
    state,
    updated_at: params.now ?? Date.now(),
    source: optional(params.source) ?? "unknown",
    reason: optional(params.reason) ?? "policy-lock-set",
  });
  const lock = getPolicyLock(lockId);
  if (!lock) {
    throw new Error(`policy lock was not persisted: ${lockId}`);
  }
  appendPolicyLockAudit({
    lockId,
    decision: state,
    reasonCode: state === "LOCKED" ? "POLICY_LOCK_SET" : "POLICY_LOCK_UNLOCKED",
    source: params.source,
    detail: params.reason,
    timestamp: params.now,
  });
  return lock;
}

export function getPolicyLock(lockId: string): PolicyLockRecord | null {
  const row = openPolicyLockDatabase().statements.selectLockById.get(lockId) as LockRow | undefined;
  return row ? lockRowToRecord(row) : null;
}

export function listPolicyLocks(): PolicyLockRecord[] {
  const rows = openPolicyLockDatabase().statements.selectLocks.all() as LockRow[];
  return rows.map(lockRowToRecord);
}

export function createPolicyUnlock(params: {
  lockId: string;
  taskId?: string;
  lane?: string;
  action?: string;
  account?: string;
  targetClass?: string;
  recipientHash?: string;
  templateId?: string;
  maxCount?: string;
  cadence?: string;
  approvalText?: string;
  proofPath?: string;
  stopInstruction?: string;
  rollbackInstruction?: string;
  expiresAt?: number;
  ttlMs?: number;
  unlockId?: string;
  source?: string;
  now?: number;
}): PolicyUnlockRecord {
  const lockId = normalizeOptionalString(params.lockId);
  if (!lockId) {
    throw new Error("lockId is required");
  }
  const now = params.now ?? Date.now();
  const unlockId = normalizeOptionalString(params.unlockId) ?? crypto.randomUUID();
  const required = [
    params.taskId,
    params.lane,
    params.action,
    params.account,
    params.targetClass,
    params.approvalText,
    params.proofPath,
    params.stopInstruction,
    params.rollbackInstruction,
  ];
  if (required.some((item) => !normalizeOptionalString(item))) {
    throw new Error(
      "exact unlock requires taskId, lane, action, account, targetClass, approvalText, proofPath, stopInstruction, and rollbackInstruction",
    );
  }
  openPolicyLockDatabase().statements.insertUnlock.run({
    unlock_id: unlockId,
    lock_id: lockId,
    task_id: optional(params.taskId),
    lane: optional(params.lane),
    action: optional(params.action),
    account: optional(params.account),
    target_class: optional(params.targetClass),
    recipient_hash: optional(params.recipientHash),
    template_id: optional(params.templateId),
    max_count: optional(params.maxCount),
    cadence: optional(params.cadence),
    approval_text: optional(params.approvalText),
    proof_path: optional(params.proofPath),
    stop_instruction: optional(params.stopInstruction),
    rollback_instruction: optional(params.rollbackInstruction),
    expires_at: params.expiresAt ?? now + Math.max(1, params.ttlMs ?? DEFAULT_UNLOCK_TTL_MS),
    used_at: null,
    state: "ACTIVE",
    created_at: now,
    source: optional(params.source) ?? "unknown",
  });
  appendPolicyLockAudit({
    lockId,
    unlockId,
    taskId: params.taskId,
    lane: params.lane,
    action: params.action,
    source: params.source,
    decision: "UNLOCKED",
    reasonCode: "POLICY_UNLOCK_CREATED",
    proofPath: params.proofPath,
    timestamp: now,
  });
  return listPolicyUnlocks().find((unlock) => unlock.unlockId === unlockId)!;
}

export function listPolicyUnlocks(): PolicyUnlockRecord[] {
  const rows = openPolicyLockDatabase().statements.selectUnlocks.all() as UnlockRow[];
  return rows.map(unlockRowToRecord);
}

export function listPolicyLockAudit(): PolicyLockAuditRecord[] {
  const rows = openPolicyLockDatabase().statements.selectAudit.all() as AuditRow[];
  return rows.map(auditRowToRecord);
}

export function appendPolicyLockAudit(params: {
  auditId?: string;
  timestamp?: number;
  lockId?: string;
  unlockId?: string;
  taskId?: string;
  sessionKey?: string;
  runId?: string;
  lane?: string;
  action?: string;
  source?: string;
  channel?: string;
  decision: PolicyLockDecision;
  reasonCode: string;
  proofPath?: string;
  detail?: string;
}): PolicyLockAuditRecord {
  const audit: PolicyLockAuditRecord = {
    auditId: normalizeOptionalString(params.auditId) ?? crypto.randomUUID(),
    timestamp: params.timestamp ?? Date.now(),
    ...(normalizeOptionalString(params.lockId)
      ? { lockId: normalizeOptionalString(params.lockId) }
      : {}),
    ...(normalizeOptionalString(params.unlockId)
      ? { unlockId: normalizeOptionalString(params.unlockId) }
      : {}),
    ...(normalizeOptionalString(params.taskId)
      ? { taskId: normalizeOptionalString(params.taskId) }
      : {}),
    ...(normalizeOptionalString(params.sessionKey)
      ? { sessionKey: normalizeOptionalString(params.sessionKey) }
      : {}),
    ...(normalizeOptionalString(params.runId)
      ? { runId: normalizeOptionalString(params.runId) }
      : {}),
    ...(normalizeOptionalString(params.lane) ? { lane: normalizeOptionalString(params.lane) } : {}),
    ...(normalizeOptionalString(params.action)
      ? { action: normalizeOptionalString(params.action) }
      : {}),
    ...(normalizeOptionalString(params.source)
      ? { source: normalizeOptionalString(params.source) }
      : {}),
    ...(normalizeOptionalString(params.channel)
      ? { channel: normalizeOptionalString(params.channel) }
      : {}),
    decision: params.decision,
    reasonCode: params.reasonCode,
    ...(normalizeOptionalString(params.proofPath)
      ? { proofPath: normalizeOptionalString(params.proofPath) }
      : {}),
    ...(normalizeOptionalString(params.detail)
      ? { detail: normalizeOptionalString(params.detail) }
      : {}),
  };
  openPolicyLockDatabase().statements.insertAudit.run({
    audit_id: audit.auditId,
    timestamp: audit.timestamp,
    lock_id: audit.lockId ?? null,
    unlock_id: audit.unlockId ?? null,
    task_id: audit.taskId ?? null,
    session_key: audit.sessionKey ?? null,
    run_id: audit.runId ?? null,
    lane: audit.lane ?? null,
    action: audit.action ?? null,
    source: audit.source ?? null,
    channel: audit.channel ?? null,
    decision: audit.decision,
    reason_code: audit.reasonCode,
    proof_path: audit.proofPath ?? null,
    detail: audit.detail ?? null,
  });
  return audit;
}

function matchesOptional(expected: string | undefined, actual: string | undefined): boolean {
  const normalizedExpected = normalizeOptionalString(expected);
  if (!normalizedExpected) {
    return true;
  }
  const normalizedActual = normalizeOptionalString(actual);
  return normalizedExpected === normalizedActual;
}

export function consumeMatchingPolicyUnlock(params: {
  lockId: string;
  taskId?: string;
  lane?: string;
  action?: string;
  account?: string;
  targetClass?: string;
  now?: number;
}): PolicyUnlockRecord | null {
  const now = params.now ?? Date.now();
  const rows = openPolicyLockDatabase().statements.selectActiveUnlocksForLock.all(
    params.lockId,
  ) as UnlockRow[];
  for (const row of rows) {
    const unlock = unlockRowToRecord(row);
    if (unlock.expiresAt <= now) {
      openPolicyLockDatabase().statements.expireUnlock.run(unlock.unlockId);
      appendPolicyLockAudit({
        lockId: unlock.lockId,
        unlockId: unlock.unlockId,
        taskId: unlock.taskId,
        lane: unlock.lane,
        action: unlock.action,
        source: "policy-lock-registry",
        decision: "EXPIRED",
        reasonCode: "POLICY_UNLOCK_EXPIRED",
        proofPath: unlock.proofPath,
        timestamp: now,
      });
      continue;
    }
    if (
      !matchesOptional(unlock.taskId, params.taskId) ||
      !matchesOptional(unlock.lane, params.lane) ||
      !matchesOptional(unlock.action, params.action) ||
      !matchesOptional(unlock.account, params.account) ||
      !matchesOptional(unlock.targetClass, params.targetClass)
    ) {
      continue;
    }
    openPolicyLockDatabase().statements.useUnlock.run(now, unlock.unlockId);
    appendPolicyLockAudit({
      lockId: unlock.lockId,
      unlockId: unlock.unlockId,
      taskId: unlock.taskId,
      lane: unlock.lane,
      action: unlock.action,
      source: "before-tool-policy-guard",
      decision: "USED",
      reasonCode: "POLICY_UNLOCK_USED",
      proofPath: unlock.proofPath,
      timestamp: now,
    });
    return {
      ...unlock,
      state: "USED",
      usedAt: now,
    };
  }
  return null;
}

export function resetPolicyLockRegistryForTests() {
  if (!cachedDatabase) {
    return;
  }
  cachedDatabase.walMaintenance.close();
  cachedDatabase.db.close();
  cachedDatabase = null;
}
