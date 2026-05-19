import {
  collectReliabilityHealthSnapshot,
  getLatestReliabilityHealthSnapshot,
} from "../reliability/supervisor.js";
import type { ReliabilityHealthSnapshot } from "../reliability/supervisor.types.js";
import { type RuntimeEnv, writeRuntimeJson } from "../runtime.js";
import { theme } from "../terminal/theme.js";

export type ReliabilityStatusCommandOptions = {
  json?: boolean;
};

function formatSubsystemLine(
  label: string,
  subsystem: ReliabilityHealthSnapshot["subsystems"][keyof ReliabilityHealthSnapshot["subsystems"]],
): string {
  const eventCount = subsystem.findings.length;
  const suffix = eventCount === 0 ? "clean" : `${eventCount} finding(s)`;
  return `${label.padEnd(9)} ${subsystem.status.toUpperCase().padEnd(6)} ${suffix}`;
}

function formatReliabilityStatus(snapshot: ReliabilityHealthSnapshot): string[] {
  const heading = `Reliability ${snapshot.status.toUpperCase()}`;
  const lines = [theme.heading(heading)];
  lines.push(formatSubsystemLine("tasks", snapshot.subsystems.tasks));
  lines.push(formatSubsystemLine("cron", snapshot.subsystems.cron));
  lines.push(formatSubsystemLine("delivery", snapshot.subsystems.delivery));
  lines.push(formatSubsystemLine("models", snapshot.subsystems.models));
  lines.push(formatSubsystemLine("sessions", snapshot.subsystems.sessions));
  lines.push(formatSubsystemLine("mcp", snapshot.subsystems.mcp));

  if (snapshot.actions.length > 0) {
    const latest = snapshot.actions[0];
    lines.push("");
    lines.push(
      `Latest action: ${latest.kind} (${latest.critical ? "critical" : "routine"}) — ${latest.reason}`,
    );
  }

  return lines;
}

export async function reliabilityStatusCommand(
  opts: ReliabilityStatusCommandOptions,
  runtime: RuntimeEnv,
): Promise<void> {
  const snapshot =
    getLatestReliabilityHealthSnapshot() ?? (await collectReliabilityHealthSnapshot());

  if (opts.json) {
    writeRuntimeJson(runtime, snapshot);
    return;
  }

  for (const line of formatReliabilityStatus(snapshot)) {
    runtime.log(line);
  }
}
