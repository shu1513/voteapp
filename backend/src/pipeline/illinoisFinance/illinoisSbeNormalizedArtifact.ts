import { readFile } from "node:fs/promises";

export const ILLINOIS_SBE_NORMALIZED_ARTIFACT_SCHEMA_VERSION = 1;

export type IllinoisSbeNormalizedArtifactSource = "illinois_sbe" | "illinois_sunshine";

export type IllinoisSbeCandidateCommitteeRelation = {
  candidateId: string;
  candidateName: string;
  electionYear: number;
  districtType: string;
  district: string;
  office: string;
  isAtLarge: boolean;
  committeeId: string;
  committeeName: string;
  committeeStatus: "active" | "final" | "inactive" | "unknown";
  sourceUrl: string;
};

export type IllinoisSbeD2ReportSummary = {
  reportId: string;
  committeeId: string;
  periodStart: string;
  periodEnd: string;
  filedAt: string;
  totalReceipts: number | null;
  totalDisbursements: number | null;
  cashOnHand: number | null;
  debtsOwed: number | null;
  sourceUrl: string;
};

export type IllinoisSbeNormalizedArtifact = {
  schemaVersion: 1;
  complete: true;
  source: IllinoisSbeNormalizedArtifactSource;
  acquiredAt: string;
  sourceUrl: string;
  candidateCommitteeRelations: IllinoisSbeCandidateCommitteeRelation[];
  d2ReportSummaries: IllinoisSbeD2ReportSummary[];
};

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function requireString(record: Record<string, unknown>, key: string, context: string): string {
  const value = record[key];
  if (typeof value !== "string" || value.replace(/\0/g, "").trim().length === 0) {
    throw new Error(`Illinois SBE normalized artifact ${context}.${key} must be a non-empty string`);
  }
  return value.replace(/\0/g, "").trim().replace(/\s+/g, " ");
}

function requireUrl(record: Record<string, unknown>, key: string, context: string): string {
  const value = requireString(record, key, context);
  let parsed: URL;
  try {
    parsed = new URL(value);
  } catch {
    throw new Error(`Illinois SBE normalized artifact ${context}.${key} must be an http(s) URL`);
  }
  if (parsed.protocol !== "http:" && parsed.protocol !== "https:") {
    throw new Error(`Illinois SBE normalized artifact ${context}.${key} must be an http(s) URL`);
  }
  return value;
}

function requireTimestamp(record: Record<string, unknown>, key: string, context: string): string {
  const value = requireString(record, key, context);
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    throw new Error(`Illinois SBE normalized artifact ${context}.${key} must be a timestamp`);
  }
  return parsed.toISOString();
}

function requireDate(record: Record<string, unknown>, key: string, context: string): string {
  const value = requireString(record, key, context);
  if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) {
    throw new Error(`Illinois SBE normalized artifact ${context}.${key} must use YYYY-MM-DD`);
  }
  const parsed = new Date(`${value}T00:00:00.000Z`);
  if (Number.isNaN(parsed.getTime()) || parsed.toISOString().slice(0, 10) !== value) {
    throw new Error(`Illinois SBE normalized artifact ${context}.${key} must be a valid date`);
  }
  return value;
}

function requireElectionYear(record: Record<string, unknown>, context: string): number {
  const value = record.electionYear;
  if (!Number.isInteger(value) || Number(value) < 2000 || Number(value) > 2100) {
    throw new Error(`Illinois SBE normalized artifact ${context}.electionYear must be an integer from 2000 to 2100`);
  }
  return Number(value);
}

function nullableNonnegativeAmount(
  record: Record<string, unknown>,
  key: string,
  context: string
): number | null {
  const value = record[key];
  if (value === null) {
    return null;
  }
  if (typeof value !== "number" || !Number.isFinite(value) || value < 0) {
    throw new Error(`Illinois SBE normalized artifact ${context}.${key} must be null or a nonnegative number`);
  }
  return Math.round(value * 100) / 100;
}

function parseRelation(value: unknown, index: number): IllinoisSbeCandidateCommitteeRelation {
  const context = `candidateCommitteeRelations[${index}]`;
  if (!isRecord(value)) {
    throw new Error(`Illinois SBE normalized artifact ${context} must be an object`);
  }
  if (typeof value.isAtLarge !== "boolean") {
    throw new Error(`Illinois SBE normalized artifact ${context}.isAtLarge must be boolean`);
  }
  const committeeStatus = requireString(value, "committeeStatus", context).toLowerCase();
  if (!["active", "final", "inactive", "unknown"].includes(committeeStatus)) {
    throw new Error(`Illinois SBE normalized artifact ${context}.committeeStatus is invalid`);
  }
  return {
    candidateId: requireString(value, "candidateId", context),
    candidateName: requireString(value, "candidateName", context),
    electionYear: requireElectionYear(value, context),
    districtType: requireString(value, "districtType", context),
    district: requireString(value, "district", context),
    office: requireString(value, "office", context),
    isAtLarge: value.isAtLarge,
    committeeId: requireString(value, "committeeId", context),
    committeeName: requireString(value, "committeeName", context),
    committeeStatus: committeeStatus as IllinoisSbeCandidateCommitteeRelation["committeeStatus"],
    sourceUrl: requireUrl(value, "sourceUrl", context),
  };
}

function parseD2Report(value: unknown, index: number): IllinoisSbeD2ReportSummary {
  const context = `d2ReportSummaries[${index}]`;
  if (!isRecord(value)) {
    throw new Error(`Illinois SBE normalized artifact ${context} must be an object`);
  }
  const periodStart = requireDate(value, "periodStart", context);
  const periodEnd = requireDate(value, "periodEnd", context);
  if (periodStart > periodEnd) {
    throw new Error(`Illinois SBE normalized artifact ${context} has periodStart after periodEnd`);
  }
  const report: IllinoisSbeD2ReportSummary = {
    reportId: requireString(value, "reportId", context),
    committeeId: requireString(value, "committeeId", context),
    periodStart,
    periodEnd,
    filedAt: requireTimestamp(value, "filedAt", context),
    totalReceipts: nullableNonnegativeAmount(value, "totalReceipts", context),
    totalDisbursements: nullableNonnegativeAmount(value, "totalDisbursements", context),
    cashOnHand: nullableNonnegativeAmount(value, "cashOnHand", context),
    debtsOwed: nullableNonnegativeAmount(value, "debtsOwed", context),
    sourceUrl: requireUrl(value, "sourceUrl", context),
  };
  if (
    report.totalReceipts === null &&
    report.totalDisbursements === null &&
    report.cashOnHand === null &&
    report.debtsOwed === null
  ) {
    throw new Error(`Illinois SBE normalized artifact ${context} has no financial values`);
  }
  return report;
}

function assertUnique(values: readonly string[], label: string): void {
  const seen = new Set<string>();
  for (const value of values) {
    if (seen.has(value)) {
      throw new Error(`Illinois SBE normalized artifact contains duplicate ${label}: ${value}`);
    }
    seen.add(value);
  }
}

export function parseIllinoisSbeNormalizedArtifact(text: string): IllinoisSbeNormalizedArtifact {
  let parsed: unknown;
  try {
    parsed = JSON.parse(text.replace(/^\uFEFF/, ""));
  } catch {
    throw new Error("Illinois SBE normalized artifact must be valid JSON");
  }
  if (!isRecord(parsed)) {
    throw new Error("Illinois SBE normalized artifact must be an object");
  }
  if (parsed.schemaVersion !== ILLINOIS_SBE_NORMALIZED_ARTIFACT_SCHEMA_VERSION) {
    throw new Error(
      `Illinois SBE normalized artifact schemaVersion must be ${ILLINOIS_SBE_NORMALIZED_ARTIFACT_SCHEMA_VERSION}`
    );
  }
  if (parsed.complete !== true) {
    throw new Error("Illinois SBE normalized artifact must declare complete=true");
  }
  if (parsed.source !== "illinois_sbe" && parsed.source !== "illinois_sunshine") {
    throw new Error("Illinois SBE normalized artifact source is invalid");
  }
  if (!Array.isArray(parsed.candidateCommitteeRelations) || !Array.isArray(parsed.d2ReportSummaries)) {
    throw new Error("Illinois SBE normalized artifact relation and D-2 collections must be arrays");
  }

  const candidateCommitteeRelations = parsed.candidateCommitteeRelations.map(parseRelation);
  const d2ReportSummaries = parsed.d2ReportSummaries.map(parseD2Report);
  assertUnique(
    candidateCommitteeRelations.map((relation) =>
      [
        relation.candidateId,
        relation.electionYear,
        relation.districtType,
        relation.district,
        relation.office,
        relation.committeeId,
      ].join("\u0000")
    ),
    "candidate/committee relation"
  );
  assertUnique(d2ReportSummaries.map((report) => report.reportId), "D-2 report ID");

  return {
    schemaVersion: ILLINOIS_SBE_NORMALIZED_ARTIFACT_SCHEMA_VERSION,
    complete: true,
    source: parsed.source,
    acquiredAt: requireTimestamp(parsed, "acquiredAt", "root"),
    sourceUrl: requireUrl(parsed, "sourceUrl", "root"),
    candidateCommitteeRelations,
    d2ReportSummaries,
  };
}

export async function loadIllinoisSbeNormalizedArtifact(path: string): Promise<IllinoisSbeNormalizedArtifact> {
  const normalizedPath = path.trim();
  if (!normalizedPath) {
    throw new Error("Illinois SBE normalized artifact path is required");
  }
  return parseIllinoisSbeNormalizedArtifact(await readFile(normalizedPath, "utf8"));
}
