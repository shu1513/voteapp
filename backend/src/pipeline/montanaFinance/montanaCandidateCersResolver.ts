// Candidate -> CERS-candidate resolution for Montana campaign finance
// (docs/plans/montana-finance.md, Phase 2a).
//
// We link FROM the VoteApp roster, never from the CERS registration list
// (it contains test data — "TEST, Acct" — plus exploratory and off-ballot
// registrations). Identity evidence is full name + election year + an exact
// office-title match; fuzzy-name-only matching is forbidden (Phase 0
// observed LYN BENNET / LYN BENNETT drift in IE targets).
//
// Office titles verified live 2026-08-28 against the full 1,089-row 2026
// registration list:
//   "Senate District No. 43" / "House District No. 12" (legislative),
//   "Supreme Court Justice No. 03"/"No. 04" (zero-padded seat numbers),
//   "Public Service Commission District No. 1".

import {
  personNamesMatchWithMiddleEvidence,
} from "../finance/personNameMiddleEvidence.js";
import {
  buildMontanaCersDataTablesQuery,
  buildMontanaCersUrl,
  createMontanaCersSession,
  MONTANA_CERS_DASHBOARD_URL,
  type MontanaCersSessionOptions,
} from "./montanaCersClient.js";
import {
  parseMontanaCersCandidateSearchResults,
  type MontanaCersCandidateSearchRow,
} from "./montanaCersParsers.js";

export type MontanaCersOfficeExpectation =
  | { kind: "legislative_upper" | "legislative_lower" | "psc"; districtNumber: number }
  | { kind: "supreme_court" };

export type MontanaCersResolverInput = {
  candidateName: string;
  electionYear: number;
  officeScope: string;
  officeName: string;
  districtName: string | null;
  legislativeDistrict?: string | null;
  rows: readonly MontanaCersCandidateSearchRow[];
};

export type MontanaCersCandidateMatch = {
  /** Numeric CERS candidateId (per candidate per election cycle). */
  cersCandidateId: number;
  /** CERS display name, stored as the link's committee_name. */
  cersCandidateName: string;
  officeTitle: string | null;
  confidence: "name_office_year_exact";
  source: "cers_portal";
  sourceUrl: string;
};

export type MontanaCersCandidateResolution =
  | ({ status: "matched" } & MontanaCersCandidateMatch)
  | {
      status: "unmatched";
      reason:
        | "missing_candidate_name"
        | "unsupported_office"
        | "missing_district_number"
        | "no_matching_cers_candidate";
    }
  | {
      status: "ambiguous";
      reason: "multiple_matching_cers_candidates";
      matches: MontanaCersCandidateMatch[];
    };

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

// Stored-name normalization (the Delaware/South Carolina convention).
export function normalizeMontanaCandidateNameForStorage(value: string): string {
  return value
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toUpperCase()
    .replace(/[^A-Z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function parseDistrictNumber(value: string | null | undefined): number | null {
  if (typeof value !== "string") {
    return null;
  }
  const match = /(\d+)/.exec(value);
  if (match?.[1] === undefined) {
    return null;
  }
  const parsed = Number.parseInt(match[1], 10);
  return Number.isSafeInteger(parsed) && parsed > 0 ? parsed : null;
}

export function toMontanaCersOfficeExpectation(input: {
  officeScope: string;
  officeName: string;
  districtName: string | null;
  legislativeDistrict?: string | null;
}): MontanaCersOfficeExpectation | { unmatchedReason: "unsupported_office" | "missing_district_number" } {
  if (input.officeScope === "state_upper" && input.officeName === "State Senator") {
    const districtNumber = parseDistrictNumber(input.legislativeDistrict);
    return districtNumber === null
      ? { unmatchedReason: "missing_district_number" }
      : { kind: "legislative_upper", districtNumber };
  }
  if (input.officeScope === "state_lower" && input.officeName === "State Lower Chamber Legislator") {
    const districtNumber = parseDistrictNumber(input.legislativeDistrict);
    return districtNumber === null
      ? { unmatchedReason: "missing_district_number" }
      : { kind: "legislative_lower", districtNumber };
  }
  if (input.officeScope === "statewide" && input.officeName === "State Level Judge") {
    // 2026: Supreme Court Justice seats. Seat numbers are not modeled on the
    // roster side, so a single full-name + year match within the judicial
    // class is the evidence; two same-named candidates on different seats
    // would surface as ambiguous and stay unlinked.
    return { kind: "supreme_court" };
  }
  if (input.officeScope === "statewide" && input.officeName === "Public Service Commissioner") {
    const districtNumber = parseDistrictNumber(input.districtName);
    return districtNumber === null
      ? { unmatchedReason: "missing_district_number" }
      : { kind: "psc", districtNumber };
  }
  return { unmatchedReason: "unsupported_office" };
}

function normalizeOfficeTitle(value: string): string {
  return value.toUpperCase().replace(/\s+/g, " ").trim();
}

export function montanaCersOfficeTitleMatches(
  expectation: MontanaCersOfficeExpectation,
  officeTitle: string | null
): boolean {
  if (officeTitle === null) {
    return false;
  }
  const title = normalizeOfficeTitle(officeTitle);
  switch (expectation.kind) {
    case "legislative_upper": {
      const match = /^SENATE DISTRICT NO\.? 0*(\d+)$/.exec(title);
      return match !== null && Number.parseInt(match[1]!, 10) === expectation.districtNumber;
    }
    case "legislative_lower": {
      const match = /^HOUSE DISTRICT NO\.? 0*(\d+)$/.exec(title);
      return match !== null && Number.parseInt(match[1]!, 10) === expectation.districtNumber;
    }
    case "supreme_court":
      return /^SUPREME COURT (?:CHIEF )?JUSTICE(?: NO\.? 0*\d+)?$/.test(title);
    case "psc": {
      const match = /^PUBLIC SERVICE COMMISSION(?:ER)? DISTRICT NO\.? 0*(\d+)$/.exec(title);
      return match !== null && Number.parseInt(match[1]!, 10) === expectation.districtNumber;
    }
  }
}

/** CERS display name for storage: "Bedey, David F." */
export function montanaCersCandidateDisplayName(row: MontanaCersCandidateSearchRow): string {
  const given = [row.firstName, row.middleInitial].filter(Boolean).join(" ");
  return given ? `${row.lastName}, ${given}` : row.lastName;
}

function toMatch(row: MontanaCersCandidateSearchRow): MontanaCersCandidateMatch {
  return {
    cersCandidateId: row.candidateId,
    cersCandidateName: montanaCersCandidateDisplayName(row),
    officeTitle: row.officeTitle,
    confidence: "name_office_year_exact",
    source: "cers_portal",
    sourceUrl: MONTANA_CERS_DASHBOARD_URL,
  };
}

export function resolveMontanaCersCandidate(input: MontanaCersResolverInput): MontanaCersCandidateResolution {
  const candidateName = input.candidateName.trim();
  if (!candidateName) {
    return { status: "unmatched", reason: "missing_candidate_name" };
  }
  const expectation = toMontanaCersOfficeExpectation(input);
  if ("unmatchedReason" in expectation) {
    return { status: "unmatched", reason: expectation.unmatchedReason };
  }

  const matchesById = new Map<number, MontanaCersCandidateSearchRow>();
  for (const row of input.rows) {
    if (row.electionYear !== input.electionYear) {
      continue;
    }
    if (!montanaCersOfficeTitleMatches(expectation, row.officeTitle)) {
      continue;
    }
    const cersFullName = [row.firstName, row.middleInitial, row.lastName].filter(Boolean).join(" ");
    if (
      !personNamesMatchWithMiddleEvidence({
        candidateName,
        rowNames: [cersFullName],
        normalizePersonName,
      })
    ) {
      continue;
    }
    matchesById.set(row.candidateId, row);
  }

  const matches = [...matchesById.values()]
    .map(toMatch)
    .sort((left, right) => left.cersCandidateId - right.cersCandidateId);
  if (matches.length === 1) {
    return { status: "matched", ...matches[0]! };
  }
  if (matches.length > 1) {
    return { status: "ambiguous", reason: "multiple_matching_cers_candidates", matches };
  }
  return { status: "unmatched", reason: "no_matching_cers_candidate" };
}

/**
 * Fetches the full CERS registration list for one election year (a single
 * DataTables page; 1,089 rows for 2026, verified live 2026-08-28). One fetch
 * per year serves a whole auto-link batch — resolution then happens locally,
 * which avoids depending on the portal's name-search semantics entirely.
 */
export async function searchMontanaCersCandidatesByYear(
  electionYear: number,
  options: MontanaCersSessionOptions = {}
): Promise<MontanaCersCandidateSearchRow[]> {
  if (!Number.isSafeInteger(electionYear) || electionYear < 2020 || electionYear > 2100) {
    throw new Error(`Invalid Montana CERS election year: ${electionYear}`);
  }
  const session = createMontanaCersSession(options);
  await session.get(buildMontanaCersUrl("search/candidateSearch"));
  await session.postForm(buildMontanaCersUrl("searchResults/searchCandidates"), {
    lastName: "",
    firstName: "",
    middleInitial: "",
    candidateTypeCode: "",
    officeCode: "",
    countyCode: "",
    partyCode: "",
    electionYear: String(electionYear),
  });
  const response = await session.get(
    buildMontanaCersUrl("searchResults/listCandidateResults", buildMontanaCersDataTablesQuery(5_000))
  );
  return parseMontanaCersCandidateSearchResults(response.text());
}
