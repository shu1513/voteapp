import {
  hasMiddleNameConflict,
  personNamesMatchWithMiddleEvidence,
} from "../finance/personNameMiddleEvidence.js";
import type { NewHampshireReceiptCsvRow } from "./newHampshireCfsCsv.js";
import { normalizeNewHampshireCandidateAlias } from "./newHampshireOutsideSpendingAggregator.js";

export type NewHampshireCandidateFilerResolverInput = {
  candidateName: string;
  electionYear: number;
  receiptRows: readonly NewHampshireReceiptCsvRow[];
  sourceUrl?: string | null;
};

export type NewHampshireCandidateFilerMatch = {
  filingEntityId: number;
  filerName: string;
  candidateAliases: string[];
  confidence: "exact";
  source: "cfs_bulk";
  sourceUrl: string | null;
  matchedReceiptRowCount: number;
};

export type NewHampshireCandidateFilerResolution =
  | ({ status: "matched" } & NewHampshireCandidateFilerMatch)
  | {
      status: "unmatched";
      reason: "missing_candidate_name" | "no_candidate_filer_match";
      candidateNameNormalized: string;
    }
  | {
      status: "ambiguous";
      reason: "multiple_matching_filers";
      candidateNameNormalized: string;
      matches: NewHampshireCandidateFilerMatch[];
    };

type CandidateFilerAccumulator = {
  filingEntityId: number;
  filerName: string;
  filerNamePriority: number;
  filerNameDateKey: number;
  candidateAliases: Map<string, string>;
  rows: NewHampshireReceiptCsvRow[];
};

function normalizeElectionYear(value: number): number {
  if (!Number.isInteger(value) || value < 2016 || value > 2100) {
    throw new Error(`Invalid New Hampshire candidate filer election year: ${value}`);
  }
  return value;
}

function normalizeTextKey(value: string): string {
  return value
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toUpperCase()
    .replace(/[^A-Z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function normalizePersonName(value: string): string {
  return normalizeTextKey(value)
    .replace(/\b(JR|SR|II|III|IV)\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

export function normalizeNewHampshireCandidateNameKeys(value: string): Set<string> {
  const trimmed = value.trim();
  const keys = new Set<string>();
  const normalized = normalizePersonName(trimmed);
  if (normalized) keys.add(normalized);

  const commaParts = trimmed
    .split(",")
    .map(normalizePersonName)
    .filter(Boolean);
  if (commaParts.length >= 2) {
    const lastName = commaParts[0] ?? "";
    const givenNames = commaParts.slice(1).join(" ");
    const flipped = normalizePersonName(`${givenNames} ${lastName}`);
    if (flipped) keys.add(flipped);
  }
  return keys;
}

export function normalizeNewHampshireCandidateNameForStorage(value: string): string {
  const trimmed = value.trim();
  const commaParts = trimmed
    .split(",")
    .map(normalizePersonName)
    .filter(Boolean);
  if (commaParts.length >= 2) {
    return normalizePersonName(`${commaParts.slice(1).join(" ")} ${commaParts[0] ?? ""}`);
  }
  return normalizePersonName(trimmed);
}

function candidateNamesMatch(candidateName: string, rowCandidateName: string): boolean {
  const candidateKeys = normalizeNewHampshireCandidateNameKeys(candidateName);
  const rowKeys = normalizeNewHampshireCandidateNameKeys(rowCandidateName);
  for (const key of rowKeys) {
    if (candidateKeys.has(key)) {
      return !hasMiddleNameConflict({
        candidateName,
        rowNames: [rowCandidateName],
        normalizePersonName,
      });
    }
  }
  return personNamesMatchWithMiddleEvidence({
    candidateName,
    rowNames: [rowCandidateName],
    normalizePersonName,
  });
}

function parseFilingEntityId(value: string): number | null {
  const trimmed = value.trim();
  if (!/^\d+$/.test(trimmed)) return null;
  const parsed = Number(trimmed);
  return Number.isSafeInteger(parsed) && parsed > 0 ? parsed : null;
}

function isExactElectionCycle(row: NewHampshireReceiptCsvRow, electionYear: number): boolean {
  return row["Election year"].trim() === String(electionYear);
}

function receiptDateKey(value: string): number {
  const match = /^(\d{1,2})\/(\d{1,2})\/(\d{4})$/.exec(value.trim());
  if (!match) return 0;
  return Number(match[3]) * 10_000 + Number(match[1]) * 100 + Number(match[2]);
}

function rememberFilerName(
  accumulator: CandidateFilerAccumulator,
  row: NewHampshireReceiptCsvRow
): void {
  const committeeName = row["Committee Name"].trim();
  const filerName = committeeName || row["Candidate Name"].trim();
  if (!filerName) return;
  const priority = committeeName ? 1 : 0;
  const dateKey = receiptDateKey(row["Date of Receipt"]);
  if (
    priority < accumulator.filerNamePriority ||
    (priority === accumulator.filerNamePriority &&
      (dateKey < accumulator.filerNameDateKey ||
        (dateKey === accumulator.filerNameDateKey &&
          accumulator.filerName &&
          filerName.localeCompare(accumulator.filerName) <= 0)))
  ) {
    return;
  }
  accumulator.filerName = filerName;
  accumulator.filerNamePriority = priority;
  accumulator.filerNameDateKey = dateKey;
}

function rememberCandidateAlias(accumulator: CandidateFilerAccumulator, value: string): void {
  const alias = value.trim();
  const key = normalizeNewHampshireCandidateAlias(alias);
  if (key && !accumulator.candidateAliases.has(key)) {
    accumulator.candidateAliases.set(key, alias);
  }
}

function toMatch(
  accumulator: CandidateFilerAccumulator,
  sourceUrl: string | null
): NewHampshireCandidateFilerMatch {
  return {
    filingEntityId: accumulator.filingEntityId,
    filerName: accumulator.filerName,
    candidateAliases: [...accumulator.candidateAliases.values()].sort((left, right) =>
      left.localeCompare(right)
    ),
    confidence: "exact",
    source: "cfs_bulk",
    sourceUrl,
    matchedReceiptRowCount: accumulator.rows.length,
  };
}

export function resolveNewHampshireCandidateFiler(
  input: NewHampshireCandidateFilerResolverInput
): NewHampshireCandidateFilerResolution {
  const electionYear = normalizeElectionYear(input.electionYear);
  const candidateNameNormalized = normalizeNewHampshireCandidateNameForStorage(input.candidateName);
  if (!candidateNameNormalized) {
    return {
      status: "unmatched",
      reason: "missing_candidate_name",
      candidateNameNormalized,
    };
  }

  const rowsByFiler = new Map<number, CandidateFilerAccumulator>();
  for (const row of input.receiptRows) {
    // NH registers direct candidates and candidate committees separately.
    // Candidate Name is the relationship evidence; Committee Subtype is not.
    if (!isExactElectionCycle(row, electionYear)) continue;
    const filingEntityId = parseFilingEntityId(row["Filing Entity ID"]);
    if (filingEntityId === null) continue;
    const rowCandidateName = row["Candidate Name"].trim();
    if (!rowCandidateName || !candidateNamesMatch(input.candidateName, rowCandidateName)) continue;

    const accumulator = rowsByFiler.get(filingEntityId) ?? {
      filingEntityId,
      filerName: "",
      filerNamePriority: -1,
      filerNameDateKey: 0,
      candidateAliases: new Map<string, string>(),
      rows: [],
    };
    accumulator.rows.push(row);
    rememberFilerName(accumulator, row);
    rememberCandidateAlias(accumulator, input.candidateName);
    rememberCandidateAlias(accumulator, rowCandidateName);
    rowsByFiler.set(filingEntityId, accumulator);
  }

  const matches = [...rowsByFiler.values()]
    .filter((accumulator) => Boolean(accumulator.filerName))
    .map((accumulator) => toMatch(accumulator, input.sourceUrl ?? null))
    .sort((left, right) => left.filingEntityId - right.filingEntityId);

  if (matches.length === 0) {
    return {
      status: "unmatched",
      reason: "no_candidate_filer_match",
      candidateNameNormalized,
    };
  }
  if (matches.length > 1) {
    return {
      status: "ambiguous",
      reason: "multiple_matching_filers",
      candidateNameNormalized,
      matches,
    };
  }
  return { status: "matched", ...matches[0]! };
}
