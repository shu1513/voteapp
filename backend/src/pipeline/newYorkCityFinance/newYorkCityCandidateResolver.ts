import { hasMiddleNameConflict } from "../finance/personNameMiddleEvidence.js";
import type { NewYorkCityCfbFinancialAnalysisRow } from "./newYorkCityCfbCsv.js";
import { toNewYorkCityCfbOfficeSearchInput } from "./newYorkCityFinanceEligibleOffices.js";

export type NewYorkCityCandidateFinanceResolution =
  | {
      status: "matched";
      cfbCandidateId: string;
      cfbCandidateName: string;
      officeCode: "1" | "2" | "3" | "4";
      boroughCode: "X" | "K" | "M" | "Q" | "S" | null;
      summary: NewYorkCityCfbFinancialAnalysisRow;
    }
  | { status: "unmatched"; reason: "unsupported_office" | "missing_candidate_name" | "no_exact_match" }
  | { status: "ambiguous"; reason: "multiple_exact_matches"; matches: string[] };

function normalizePerson(value: string): string {
  return value
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toUpperCase()
    .replace(/[^A-Z0-9,]+/g, " ")
    .replace(/\b(JR|SR|II|III|IV|V)\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}
export function newYorkCityCandidateNameKeys(value: string): Set<string> {
  const normalized = normalizePerson(value);
  const keys = new Set<string>();
  if (!normalized) return keys;
  keys.add(normalized.replace(/,/g, "").replace(/\s+/g, " ").trim());
  const comma = normalized.split(",").map((part) => part.trim()).filter(Boolean);
  if (comma.length >= 2) {
    const flipped = `${comma.slice(1).join(" ")} ${comma[0]}`.replace(/\s+/g, " ").trim();
    keys.add(flipped);
    const parts = flipped.split(" ");
    if (parts.length >= 2) keys.add(`${parts[0]} ${parts[parts.length - 1]}`);
  } else {
    const parts = [...keys][0]!.split(" ");
    if (parts.length >= 2) keys.add(`${parts[0]} ${parts[parts.length - 1]}`);
  }
  return keys;
}

function latestSummary(rows: readonly NewYorkCityCfbFinancialAnalysisRow[]): NewYorkCityCfbFinancialAnalysisRow {
  return [...rows].sort(
    (left, right) => right.toStatement - left.toStatement || right.fromStatement - left.fromStatement
  )[0]!;
}

export function resolveNewYorkCityCandidate(input: {
  candidateName: string;
  electionYear: number;
  officeScope: string;
  officeCanonicalName: string;
  districtGeoid: string;
  analysisRows: readonly NewYorkCityCfbFinancialAnalysisRow[];
}): NewYorkCityCandidateFinanceResolution {
  const expectedOffice = toNewYorkCityCfbOfficeSearchInput(input);
  if (!expectedOffice) return { status: "unmatched", reason: "unsupported_office" };
  const candidateKeys = newYorkCityCandidateNameKeys(input.candidateName);
  if (candidateKeys.size === 0) return { status: "unmatched", reason: "missing_candidate_name" };

  const rowsByCandidate = new Map<string, NewYorkCityCfbFinancialAnalysisRow[]>();
  for (const row of input.analysisRows) {
    if (
      row.electionYear !== input.electionYear ||
      row.officeCode !== expectedOffice.officeCode ||
      row.boroughCode !== expectedOffice.boroughCode
    ) continue;
    const rowKeys = newYorkCityCandidateNameKeys(row.candidateName);
    if (![...candidateKeys].some((key) => rowKeys.has(key))) continue;
    // The keys collapse to first+last, so "Jane Q. Doe" would take
    // "DOE, JANE R."'s CFB filings whenever office, borough, and year agree.
    if (
      hasMiddleNameConflict({
        candidateName: input.candidateName,
        rowNames: [row.candidateName],
        normalizePersonName: normalizePerson,
      })
    ) continue;
    const rows = rowsByCandidate.get(row.candidateId) ?? [];
    rows.push(row);
    rowsByCandidate.set(row.candidateId, rows);
  }
  if (rowsByCandidate.size === 0) return { status: "unmatched", reason: "no_exact_match" };
  if (rowsByCandidate.size > 1) {
    return { status: "ambiguous", reason: "multiple_exact_matches", matches: [...rowsByCandidate.keys()].sort() };
  }
  const [cfbCandidateId, rows] = [...rowsByCandidate.entries()][0]!;
  const summary = latestSummary(rows);
  return {
    status: "matched",
    cfbCandidateId,
    cfbCandidateName: summary.candidateName,
    officeCode: summary.officeCode,
    boroughCode: summary.boroughCode,
    summary,
  };
}
