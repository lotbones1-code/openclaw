import type { LearningFooter } from "./execution-kernel.types.js";

export function validateSelfBuilderCloseout(params: {
  directiveId?: string;
  defectFixed?: string;
  sourceAuthority?: string;
  nativeSourceChange?: boolean;
  regressionTest?: string;
  exactTestGate?: string;
  testsPassed?: boolean;
  stagedSecretJunkScan?: boolean;
  commitHash?: string;
  exactSourceGate?: string;
  reportPath?: string;
  rollbackOrDisablePath?: string;
  learningFooter?: LearningFooter;
  addedNonNativeSurfaces?: string[];
}): { valid: true; missing: [] } | { valid: false; missing: string[]; blockedSurfaces: string[] } {
  const missing: string[] = [];
  if (!stringValue(params.directiveId)) {
    missing.push("directiveId");
  }
  if (!stringValue(params.defectFixed)) {
    missing.push("defectFixed");
  }
  if (!stringValue(params.sourceAuthority)) {
    missing.push("sourceAuthority");
  }
  if (params.nativeSourceChange !== true) {
    missing.push("nativeSourceChange");
  }
  if (!stringValue(params.regressionTest) && !stringValue(params.exactTestGate)) {
    missing.push("regressionTest_or_exactTestGate");
  }
  if (params.testsPassed !== true && !stringValue(params.exactTestGate)) {
    missing.push("testsPassed_or_exactTestGate");
  }
  if (params.stagedSecretJunkScan !== true) {
    missing.push("stagedSecretJunkScan");
  }
  if (!stringValue(params.commitHash) && !stringValue(params.exactSourceGate)) {
    missing.push("commitHash_or_exactSourceGate");
  }
  if (!stringValue(params.reportPath)) {
    missing.push("reportPath");
  }
  if (!stringValue(params.rollbackOrDisablePath)) {
    missing.push("rollbackOrDisablePath");
  }
  if (!params.learningFooter?.lessons_applied?.length) {
    missing.push("learningFooter.lessons_applied");
  }
  if (!stringValue(params.learningFooter?.learning_event)) {
    missing.push("learningFooter.learning_event");
  }
  if (!stringValue(params.learningFooter?.next_action)) {
    missing.push("learningFooter.next_action");
  }
  const blockedSurfaces = (params.addedNonNativeSurfaces ?? []).filter((surface) =>
    /wrapper|host_cron|launchagent|daemon|fake_browser|provider_shortcut|parallel_db|openclawd/i.test(
      surface,
    ),
  );
  if (blockedSurfaces.length > 0) {
    missing.push("native_only_compliance");
  }
  if (missing.length > 0) {
    return { valid: false, missing, blockedSurfaces };
  }
  return { valid: true, missing: [] };
}

export function buildCompactSuccessPatternNote(params: {
  task_class: string;
  service_vendor: string;
  what_worked: string;
  exact_gates_encountered: string[];
  proof_path: string;
  next_time_shortcut: string;
  what_not_to_repeat: string;
  secrets?: Record<string, unknown>;
}): {
  task_class: string;
  service_vendor: string;
  what_worked: string;
  exact_gates_encountered: string[];
  proof_path: string;
  next_time_shortcut: string;
  what_not_to_repeat: string;
  omitted_secret_fields: string[];
} {
  return {
    task_class: params.task_class,
    service_vendor: params.service_vendor,
    what_worked: params.what_worked,
    exact_gates_encountered: params.exact_gates_encountered,
    proof_path: params.proof_path,
    next_time_shortcut: params.next_time_shortcut,
    what_not_to_repeat: params.what_not_to_repeat,
    omitted_secret_fields: Object.keys(params.secrets ?? {}),
  };
}

function stringValue(value: unknown): string | undefined {
  return typeof value === "string" && value.trim().length > 0 ? value.trim() : undefined;
}
