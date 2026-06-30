import { mkdir, writeFile } from "node:fs/promises";
import { dirname } from "node:path";

export type ManualResearchRepairOutcome =
  | "needs_repair"
  | "confirmed_null"
  | "confirmed_neutral"
  | "blocked_by_contract";

export type ManualResearchRepairGap = {
  id: string;
  stage: "candidate_profile" | "candidate_records" | "candidate_record_labels";
  objectType: "candidate_profile" | "candidate_record" | "candidate_record_label" | "candidate_record_set";
  outcome: ManualResearchRepairOutcome;
  reason: string;
  focusedResearchPass: string;
  promptFile?: string;
  field?: string;
  recordIndex?: number;
  labelIndex?: number;
  sourceUrl?: string;
  eventDate?: string;
  description?: string;
  failureKind?: "schema" | "source_url" | "label_validation" | "quality_gap";
  failureType?: "transient" | "permanent";
};

export type ManualResearchRepairReport = {
  schemaVersion: "manual_research_repair_report.v1";
  generatedAt: string;
  command: string;
  manualKey: string;
  status: "needs_repair" | "blocked_by_contract_only" | "confirmed_only";
  target: Record<string, string | number | boolean | null>;
  gaps: ManualResearchRepairGap[];
};

export function buildManualResearchRepairReport(input: {
  command: string;
  manualKey: string;
  target: Record<string, string | number | boolean | null>;
  gaps: ManualResearchRepairGap[];
}): ManualResearchRepairReport {
  const hasRepairableGap = input.gaps.some((gap) => gap.outcome === "needs_repair");
  const hasContractBlockedGap = input.gaps.some((gap) => gap.outcome === "blocked_by_contract");
  return {
    schemaVersion: "manual_research_repair_report.v1",
    generatedAt: new Date().toISOString(),
    command: input.command,
    manualKey: input.manualKey,
    status: hasRepairableGap
      ? "needs_repair"
      : hasContractBlockedGap
        ? "blocked_by_contract_only"
        : "confirmed_only",
    target: input.target,
    gaps: input.gaps,
  };
}

export async function writeManualResearchRepairReport(
  filePath: string | null,
  report: ManualResearchRepairReport
): Promise<void> {
  if (!filePath) {
    return;
  }
  await mkdir(dirname(filePath), { recursive: true });
  await writeFile(filePath, `${JSON.stringify(report, null, 2)}\n`, "utf8");
}

export function summarizeManualResearchGaps(gaps: readonly ManualResearchRepairGap[]): string {
  const preview = gaps
    .slice(0, 5)
    .map((gap) => `${gap.id}: ${gap.reason}`)
    .join("; ");
  const extra = gaps.length > 5 ? `; +${gaps.length - 5} more` : "";
  return `${preview}${extra}`;
}
