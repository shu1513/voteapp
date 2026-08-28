// Candidate → ethics-filer resolution for South Carolina campaign finance.
//
// Identity evidence is name + election-cycle filings ONLY. The filer-search
// row's office fields are never consulted: the live API serves the office of a
// filer's PRIOR registration (Alan Wilson's search row says "Attorney General"
// while he runs for governor) and the 2026-cycle statewide label is the
// literal string "4". Office fit is the caller's job via the eligibility
// predicate and the roster row being resolved.
//
// Pure functions — the Phase 5 auto-link fetches search rows and per-filer
// report indexes, then calls these.

import {
  parsePersonNameCandidates,
  personNamesMatchWithMiddleEvidence,
} from "../finance/personNameMiddleEvidence.js";
import {
  SOUTH_CAROLINA_ETHICS_PUBLIC_REPORTING_URL,
  type SouthCarolinaCandidateReportRow,
  type SouthCarolinaFilerSearchRow,
} from "./southCarolinaEthicsClient.js";

export type SouthCarolinaFilerReportSet = {
  filer: SouthCarolinaFilerSearchRow;
  reports: readonly SouthCarolinaCandidateReportRow[];
};

export type SouthCarolinaCandidateFilerMatch = {
  candidateFilerId: number;
  filerName: string;
  // Distinct report electionDates (M/D/YYYY) in the linked election's year —
  // the cycle evidence the match rests on.
  matchedElectionDates: string[];
  cycleReportCount: number;
  confidence: "exact";
  source: "ethics_filer_search";
  sourceUrl: string;
};

export type SouthCarolinaCandidateFilerResolution =
  | ({ status: "matched" } & SouthCarolinaCandidateFilerMatch)
  | {
      // Surname and cycle filings line up but the given names do not
      // (Alan Wilson files as "Wilson, Michael A"). Never auto-link;
      // a human confirms and stores the link with the evidence URL.
      status: "manual_confirm_required";
      candidates: SouthCarolinaCandidateFilerMatch[];
    }
  | {
      status: "ambiguous";
      reason: "multiple_matching_filers";
      matches: SouthCarolinaCandidateFilerMatch[];
    }
  | {
      status: "unmatched";
      reason: "missing_candidate_name" | "no_matching_filer" | "no_cycle_filings";
    };

const GENERATIONAL_SUFFIXES = new Set(["JR", "SR", "II", "III", "IV"]);

function normalizePersonName(value: string): string {
  return value
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toUpperCase()
    .replace(/[^A-Z0-9]+/g, " ")
    .replace(/\b(JR|SR|II|III|IV)\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

// The search text the auto-link sends to the filer-name endpoint: the final
// surname word. Matching is contains/fuzzy server-side, so the final word of a
// compound or hyphenated surname still hits the full stored name; results are
// then narrowed locally with filterSouthCarolinaFilersByExactSurname.
export function southCarolinaFilerSearchTerm(candidateName: string): string | null {
  const withoutParens = candidateName.replace(/\([^()]*\)/g, " ");
  const commaIndex = withoutParens.indexOf(",");
  const surnameSegment = commaIndex > 0 ? withoutParens.slice(0, commaIndex) : withoutParens;
  const tokens = surnameSegment
    .split(/\s+/)
    .map((token) => token.replace(/^[^A-Za-z0-9]+|[^A-Za-z0-9.'-]+$/g, ""))
    .filter(Boolean);
  while (tokens.length > 0) {
    const last = tokens[tokens.length - 1]!;
    if (!GENERATIONAL_SUFFIXES.has(last.replace(/\./g, "").toUpperCase())) {
      break;
    }
    tokens.pop();
  }
  const term = tokens[tokens.length - 1]?.replace(/\.+$/, "") ?? "";
  return term.length >= 2 ? term : null;
}

// The roster name's surname, read from its EXPLICIT boundary: the pre-comma
// segment of a comma form, else the final whitespace token (a hyphenated token
// stays one unit and normalizes to a multi-word key, so "Mary Johnson-Wilson"
// is JOHNSON WILSON, never bare WILSON). Ambiguous space-form splits are
// deliberately not enumerated here — a wrong split would surname-match another
// person's filer. An unhyphenated compound surname can miss and stay unlinked,
// which is the conservative direction.
function rosterSurnameKey(candidateName: string): string {
  const withoutParens = candidateName.replace(/\([^()]*\)/g, " ");
  const commaIndex = withoutParens.indexOf(",");
  if (commaIndex > 0) {
    return normalizePersonName(withoutParens.slice(0, commaIndex));
  }
  const tokens = withoutParens.trim().split(/\s+/);
  for (let index = tokens.length - 1; index >= 0; index -= 1) {
    const key = normalizePersonName(tokens[index]!);
    if (key) {
      return key;
    }
  }
  return "";
}

// Local exact-surname filter over the fuzzy/contains server results
// ("Wilson" also returns "Johnson-Wilson"). SEI-only rows (candidateFilerId
// 0) have no candidate account and no reports, so they can never carry cycle
// evidence — dropped here.
export function filterSouthCarolinaFilersByExactSurname(
  candidateName: string,
  rows: readonly SouthCarolinaFilerSearchRow[]
): SouthCarolinaFilerSearchRow[] {
  const surnameKey = rosterSurnameKey(candidateName);
  if (!surnameKey) {
    return [];
  }
  return rows.filter((row) => {
    if (row.candidateFilerId <= 0) {
      return false;
    }
    return parsePersonNameCandidates(row.candidate, normalizePersonName).some(
      (parse) => parse.last === surnameKey
    );
  });
}

function reportElectionYear(electionDate: string): number {
  // Client-validated M/D/YYYY.
  return Number.parseInt(electionDate.slice(-4), 10);
}

function toMatch(input: {
  filer: SouthCarolinaFilerSearchRow;
  cycleReports: readonly SouthCarolinaCandidateReportRow[];
}): SouthCarolinaCandidateFilerMatch {
  const matchedElectionDates = [...new Set(input.cycleReports.map((row) => row.electionDate))].sort();
  return {
    candidateFilerId: input.filer.candidateFilerId,
    filerName: input.filer.candidate,
    matchedElectionDates,
    cycleReportCount: input.cycleReports.length,
    confidence: "exact",
    source: "ethics_filer_search",
    sourceUrl: SOUTH_CAROLINA_ETHICS_PUBLIC_REPORTING_URL,
  };
}

export function resolveSouthCarolinaCandidateFiler(input: {
  candidateName: string;
  // ISO YYYY-MM-DD from the linked election row.
  electionDate: string;
  filerReportSets: readonly SouthCarolinaFilerReportSet[];
}): SouthCarolinaCandidateFilerResolution {
  const candidateName = input.candidateName.trim();
  if (!candidateName) {
    return { status: "unmatched", reason: "missing_candidate_name" };
  }
  const electionYear = Number.parseInt(input.electionDate.slice(0, 4), 10);
  if (!/^\d{4}-\d{2}-\d{2}$/.test(input.electionDate) || !Number.isInteger(electionYear)) {
    throw new Error(`Invalid South Carolina resolver election date: ${input.electionDate}`);
  }

  const surnameKey = rosterSurnameKey(candidateName);
  const fullMatches: SouthCarolinaCandidateFilerMatch[] = [];
  const surnameOnlyMatches: SouthCarolinaCandidateFilerMatch[] = [];
  const seenFilerIds = new Set<number>();
  let sawNameCandidate = false;

  for (const set of input.filerReportSets) {
    const filerId = set.filer.candidateFilerId;
    if (filerId <= 0 || seenFilerIds.has(filerId)) {
      continue;
    }
    const parses = parsePersonNameCandidates(set.filer.candidate, normalizePersonName);
    if (!surnameKey || !parses.some((parse) => parse.last === surnameKey)) {
      continue;
    }
    seenFilerIds.add(filerId);
    sawNameCandidate = true;

    const cycleReports = set.reports.filter(
      (row) => reportElectionYear(row.electionDate) === electionYear
    );
    if (cycleReports.length === 0) {
      continue;
    }
    const match = toMatch({ filer: set.filer, cycleReports });
    if (
      personNamesMatchWithMiddleEvidence({
        candidateName,
        rowNames: [set.filer.candidate],
        normalizePersonName,
      })
    ) {
      fullMatches.push(match);
    } else {
      surnameOnlyMatches.push(match);
    }
  }

  if (fullMatches.length === 1) {
    return { status: "matched", ...fullMatches[0]! };
  }
  if (fullMatches.length > 1) {
    return {
      status: "ambiguous",
      reason: "multiple_matching_filers",
      matches: fullMatches.sort((left, right) => left.candidateFilerId - right.candidateFilerId),
    };
  }
  if (surnameOnlyMatches.length > 0) {
    return {
      status: "manual_confirm_required",
      candidates: surnameOnlyMatches.sort((left, right) => left.candidateFilerId - right.candidateFilerId),
    };
  }
  return {
    status: "unmatched",
    reason: sawNameCandidate ? "no_cycle_filings" : "no_matching_filer",
  };
}
