import crypto from "node:crypto";
import { chmodSync, existsSync, mkdirSync } from "node:fs";
import type { DatabaseSync, StatementSync } from "node:sqlite";
import { requireNodeSqlite } from "../infra/node-sqlite.js";
import { configureSqliteWalMaintenance, type SqliteWalMaintenance } from "../infra/sqlite-wal.js";
import { normalizeOptionalString } from "../shared/string-coerce.js";
import { resolveTaskRegistryDir, resolveTaskRegistrySqlitePath } from "./task-registry.paths.js";

export type TaskControlCommand =
  | "stop"
  | "pause"
  | "cancel"
  | "stop_browser"
  | "stop_social"
  | "stop_personal_assistant"
  | "red_stop_all";

export type TaskControlState = "requested" | "acknowledged" | "contained";

export type TaskControlRecord = {
  controlId: string;
  command: TaskControlCommand;
  scope: string;
  taskId?: string;
  sessionKey?: string;
  runId?: string;
  source?: string;
  reason?: string;
  requestedAt: number;
  acknowledgedAt?: number;
  containedAt?: number;
  state: TaskControlState;
  resumeCondition?: string;
  detailCode?: string;
};

type TaskControlRow = {
  control_id: string;
  command: TaskControlCommand;
  scope: string;
  task_id: string | null;
  session_key: string | null;
  run_id: string | null;
  source: string | null;
  reason: string | null;
  requested_at: number | bigint;
  acknowledged_at: number | bigint | null;
  contained_at: number | bigint | null;
  state: TaskControlState;
  resume_condition: string | null;
  detail_code: string | null;
};

type TaskControlStatements = {
  selectAll: StatementSync;
  selectActive: StatementSync;
  upsertRecord: StatementSync;
  acknowledgeRecord: StatementSync;
  containRecord: StatementSync;
};

type TaskControlDatabase = {
  db: DatabaseSync;
  path: string;
  statements: TaskControlStatements;
  walMaintenance: SqliteWalMaintenance;
};

const TASK_REGISTRY_DIR_MODE = 0o700;
const TASK_REGISTRY_FILE_MODE = 0o600;
const TASK_REGISTRY_SIDECAR_SUFFIXES = ["", "-shm", "-wal"] as const;
const DEFAULT_STOP_UNACKNOWLEDGED_MS = 5_000;

let cachedDatabase: TaskControlDatabase | null = null;

function normalizeNumber(value: number | bigint | null): number | undefined {
  if (typeof value === "bigint") {
    return Number(value);
  }
  return typeof value === "number" ? value : undefined;
}

function normalizeRequiredString(value: string | undefined | null, fallback: string): string {
  return normalizeOptionalString(value) ?? fallback;
}

function normalizeScope(params: {
  scope?: string;
  taskId?: string;
  sessionKey?: string;
  runId?: string;
  command?: TaskControlCommand;
}): string {
  const explicit = normalizeOptionalString(params.scope);
  if (explicit) {
    return explicit;
  }
  const taskId = normalizeOptionalString(params.taskId);
  if (taskId) {
    return `task:${taskId}`;
  }
  const runId = normalizeOptionalString(params.runId);
  if (runId) {
    return `run:${runId}`;
  }
  const sessionKey = normalizeOptionalString(params.sessionKey);
  if (sessionKey) {
    return `session:${sessionKey}`;
  }
  return params.command === "red_stop_all" ? "global" : "scope:unknown";
}

function rowToTaskControlRecord(row: TaskControlRow): TaskControlRecord {
  const acknowledgedAt = normalizeNumber(row.acknowledged_at);
  const containedAt = normalizeNumber(row.contained_at);
  return {
    controlId: row.control_id,
    command: row.command,
    scope: row.scope,
    ...(row.task_id ? { taskId: row.task_id } : {}),
    ...(row.session_key ? { sessionKey: row.session_key } : {}),
    ...(row.run_id ? { runId: row.run_id } : {}),
    ...(row.source ? { source: row.source } : {}),
    ...(row.reason ? { reason: row.reason } : {}),
    requestedAt: normalizeNumber(row.requested_at) ?? 0,
    ...(acknowledgedAt != null ? { acknowledgedAt } : {}),
    ...(containedAt != null ? { containedAt } : {}),
    state: row.state,
    ...(row.resume_condition ? { resumeCondition: row.resume_condition } : {}),
    ...(row.detail_code ? { detailCode: row.detail_code } : {}),
  };
}

function bindTaskControlRecord(record: TaskControlRecord) {
  return {
    control_id: record.controlId,
    command: record.command,
    scope: record.scope,
    task_id: record.taskId ?? null,
    session_key: record.sessionKey ?? null,
    run_id: record.runId ?? null,
    source: record.source ?? null,
    reason: record.reason ?? null,
    requested_at: record.requestedAt,
    acknowledged_at: record.acknowledgedAt ?? null,
    contained_at: record.containedAt ?? null,
    state: record.state,
    resume_condition: record.resumeCondition ?? null,
    detail_code: record.detailCode ?? null,
  };
}

function createStatements(db: DatabaseSync): TaskControlStatements {
  return {
    selectAll: db.prepare(`
      SELECT
        control_id,
        command,
        scope,
        task_id,
        session_key,
        run_id,
        source,
        reason,
        requested_at,
        acknowledged_at,
        contained_at,
        state,
        resume_condition,
        detail_code
      FROM task_control_records
      ORDER BY requested_at DESC, control_id DESC
    `),
    selectActive: db.prepare(`
      SELECT
        control_id,
        command,
        scope,
        task_id,
        session_key,
        run_id,
        source,
        reason,
        requested_at,
        acknowledged_at,
        contained_at,
        state,
        resume_condition,
        detail_code
      FROM task_control_records
      WHERE state = 'requested'
      ORDER BY requested_at DESC, control_id DESC
    `),
    upsertRecord: db.prepare(`
      INSERT INTO task_control_records (
        control_id,
        command,
        scope,
        task_id,
        session_key,
        run_id,
        source,
        reason,
        requested_at,
        acknowledged_at,
        contained_at,
        state,
        resume_condition,
        detail_code
      ) VALUES (
        @control_id,
        @command,
        @scope,
        @task_id,
        @session_key,
        @run_id,
        @source,
        @reason,
        @requested_at,
        @acknowledged_at,
        @contained_at,
        @state,
        @resume_condition,
        @detail_code
      )
      ON CONFLICT(control_id) DO UPDATE SET
        command = excluded.command,
        scope = excluded.scope,
        task_id = excluded.task_id,
        session_key = excluded.session_key,
        run_id = excluded.run_id,
        source = excluded.source,
        reason = excluded.reason,
        requested_at = excluded.requested_at,
        acknowledged_at = excluded.acknowledged_at,
        contained_at = excluded.contained_at,
        state = excluded.state,
        resume_condition = excluded.resume_condition,
        detail_code = excluded.detail_code
    `),
    acknowledgeRecord: db.prepare(`
      UPDATE task_control_records
      SET
        state = 'acknowledged',
        acknowledged_at = ?,
        detail_code = COALESCE(?, detail_code)
      WHERE control_id = ?
    `),
    containRecord: db.prepare(`
      UPDATE task_control_records
      SET
        state = 'contained',
        contained_at = ?,
        detail_code = COALESCE(?, detail_code)
      WHERE control_id = ?
    `),
  };
}

function ensureSchema(db: DatabaseSync) {
  db.exec(`
    CREATE TABLE IF NOT EXISTS task_control_records (
      control_id TEXT PRIMARY KEY,
      command TEXT NOT NULL,
      scope TEXT NOT NULL,
      task_id TEXT,
      session_key TEXT,
      run_id TEXT,
      source TEXT,
      reason TEXT,
      requested_at INTEGER NOT NULL,
      acknowledged_at INTEGER,
      contained_at INTEGER,
      state TEXT NOT NULL,
      resume_condition TEXT,
      detail_code TEXT
    );
  `);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_task_control_state ON task_control_records(state);`);
  db.exec(
    `CREATE INDEX IF NOT EXISTS idx_task_control_session ON task_control_records(session_key);`,
  );
  db.exec(`CREATE INDEX IF NOT EXISTS idx_task_control_run ON task_control_records(run_id);`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_task_control_task ON task_control_records(task_id);`);
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

function openTaskControlDatabase(): TaskControlDatabase {
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
  return cachedDatabase;
}

function upsertTaskControlRecord(record: TaskControlRecord) {
  openTaskControlDatabase().statements.upsertRecord.run(bindTaskControlRecord(record));
  ensureTaskRegistryPermissions(openTaskControlDatabase().path);
}

export function requestTaskControlStop(params: {
  command?: TaskControlCommand;
  scope?: string;
  taskId?: string;
  sessionKey?: string;
  runId?: string;
  source?: string;
  reason?: string;
  resumeCondition?: string;
  now?: number;
  controlId?: string;
}): TaskControlRecord {
  const command = params.command ?? "stop";
  const record: TaskControlRecord = {
    controlId: params.controlId ?? crypto.randomUUID(),
    command,
    scope: normalizeScope(params),
    ...(normalizeOptionalString(params.taskId)
      ? { taskId: normalizeOptionalString(params.taskId) }
      : {}),
    ...(normalizeOptionalString(params.sessionKey)
      ? { sessionKey: normalizeOptionalString(params.sessionKey) }
      : {}),
    ...(normalizeOptionalString(params.runId)
      ? { runId: normalizeOptionalString(params.runId) }
      : {}),
    source: normalizeRequiredString(params.source, "unknown"),
    reason: normalizeRequiredString(params.reason, "stop_requested"),
    requestedAt: params.now ?? Date.now(),
    state: "requested",
    resumeCondition: normalizeRequiredString(params.resumeCondition, "explicit_resume"),
  };
  upsertTaskControlRecord(record);
  return record;
}

export function listTaskControlRecords(
  opts: {
    activeOnly?: boolean;
    state?: TaskControlState;
  } = {},
): TaskControlRecord[] {
  const rows = (
    opts.activeOnly
      ? openTaskControlDatabase().statements.selectActive.all()
      : openTaskControlDatabase().statements.selectAll.all()
  ) as TaskControlRow[];
  const records = rows.map(rowToTaskControlRecord);
  if (!opts.state) {
    return records;
  }
  return records.filter((record) => record.state === opts.state);
}

function commandIsStopLike(command: TaskControlCommand): boolean {
  return (
    command === "stop" ||
    command === "pause" ||
    command === "cancel" ||
    command === "stop_browser" ||
    command === "stop_social" ||
    command === "stop_personal_assistant" ||
    command === "red_stop_all"
  );
}

function recordMatchesContext(
  record: TaskControlRecord,
  params: {
    scope?: string;
    taskId?: string;
    sessionKey?: string;
    runId?: string;
  },
): boolean {
  if (record.command === "red_stop_all" || record.scope === "global") {
    return true;
  }
  const scope = normalizeOptionalString(params.scope);
  if (scope && record.scope === scope) {
    return true;
  }
  const taskId = normalizeOptionalString(params.taskId);
  if (taskId && record.taskId === taskId) {
    return true;
  }
  const runId = normalizeOptionalString(params.runId);
  if (runId && record.runId === runId) {
    return true;
  }
  const sessionKey = normalizeOptionalString(params.sessionKey);
  if (sessionKey && record.sessionKey === sessionKey) {
    return true;
  }
  return false;
}

export function findActiveStopControl(params: {
  scope?: string;
  taskId?: string;
  sessionKey?: string;
  runId?: string;
}): TaskControlRecord | null {
  const controls = listTaskControlRecords({ activeOnly: true });
  return (
    controls.find(
      (record) => commandIsStopLike(record.command) && recordMatchesContext(record, params),
    ) ?? null
  );
}

export function acknowledgeTaskControl(params: {
  controlId: string;
  now?: number;
  detailCode?: string;
}): TaskControlRecord | null {
  const controlId = normalizeOptionalString(params.controlId);
  if (!controlId) {
    return null;
  }
  openTaskControlDatabase().statements.acknowledgeRecord.run(
    params.now ?? Date.now(),
    normalizeOptionalString(params.detailCode) ?? "stopped_by_user",
    controlId,
  );
  return listTaskControlRecords().find((record) => record.controlId === controlId) ?? null;
}

export function containTaskControl(params: {
  controlId: string;
  now?: number;
  detailCode?: string;
}): TaskControlRecord | null {
  const controlId = normalizeOptionalString(params.controlId);
  if (!controlId) {
    return null;
  }
  openTaskControlDatabase().statements.containRecord.run(
    params.now ?? Date.now(),
    normalizeOptionalString(params.detailCode) ?? "STOP_UNACKNOWLEDGED",
    controlId,
  );
  return listTaskControlRecords().find((record) => record.controlId === controlId) ?? null;
}

export function containUnacknowledgedStopControls(
  params: {
    now?: number;
    unacknowledgedMs?: number;
    cancelScope?: (scopeKey: string) => void;
  } = {},
): TaskControlRecord[] {
  const now = params.now ?? Date.now();
  const unacknowledgedMs = Math.max(
    1,
    Math.floor(params.unacknowledgedMs ?? DEFAULT_STOP_UNACKNOWLEDGED_MS),
  );
  const contained: TaskControlRecord[] = [];
  for (const record of listTaskControlRecords({ activeOnly: true })) {
    if (!commandIsStopLike(record.command)) {
      continue;
    }
    if (now - record.requestedAt < unacknowledgedMs) {
      continue;
    }
    if (record.scope && record.scope !== "global") {
      params.cancelScope?.(record.scope);
    }
    const updated = containTaskControl({
      controlId: record.controlId,
      now,
      detailCode: "STOP_UNACKNOWLEDGED",
    });
    if (updated) {
      contained.push(updated);
    }
  }
  return contained;
}

export function resetTaskControlRegistryForTests() {
  if (!cachedDatabase) {
    return;
  }
  cachedDatabase.walMaintenance.close();
  cachedDatabase.db.close();
  cachedDatabase = null;
}
