// Candidate → FCPA race-row resolution for Alabama campaign finance.
//
// Identity evidence is the candidate name within the office's race rows for
// the linked election cycle, plus — for legislative offices — the district
// carried by the committee-search join (race rows have no district column;
// plan-alabama-finance.md, gotcha 14). Pure functions: the auto-link fetches
// race rows and the office's committee-search rows, then calls these.
//
// Fail-closed rules (plan Phase 3): never link on a bare surname, never link
// an ambiguous name, and for districted offices never link a row whose
// jurisdiction is missing or contradicts the roster district.

import { firstNameVariants } from "../finance/personFirstNameNicknames.js";
import { personNamesMatchWithMiddleEvidence } from "../finance/personNameMiddleEvidence.js";
import type { AlabamaCommitteeSearchRow, AlabamaRaceRow } from "./alabamaFcpaClient.js";

export type AlabamaDistrictChamber = "HOUSE" | "SENATE";

export type AlabamaCandidateRaceMatch = {
  /** Race-row COMMITTEEID — the portal's internal id (filings, covers). */
  internalCommitteeId: number;
  /** Committee-search committeeId — the FCPA number the extracts carry. */
  fcpaCommitteeNumber: string | null;
  raceCandidateName: string;
  candidateStatus: string | null;
  committeeName: string | null;
  jurisdiction: string | null;
};

export type AlabamaCandidateRaceResolution =
  | ({ status: "matched" } & AlabamaCandidateRaceMatch)
  | { status: "ambiguous"; reason: "multiple_matching_race_rows"; matches: AlabamaCandidateRaceMatch[] }
  | {
      status: "manual_confirm_required";
      reason: "missing_committee_row" | "missing_jurisdiction" | "district_mismatch";
      candidates: AlabamaCandidateRaceMatch[];
    }
  | { status: "unmatched"; reason: "missing_candidate_name" | "no_matching_race_row" };

// Stored-name normalization (the Delaware convention): diacritics stripped,
// uppercased, non-alphanumerics collapsed to single spaces.
export function normalizeAlabamaCandidateNameForStorage(value: string): string {
  return value
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toUpperCase()
    .replace(/[^A-Z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

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

/**
 * District number from a VoteApp district name ("State House District 68
 * (2024); Alabama" → 68); null when the name carries no district number.
 */
export function alabamaDistrictNumberFromDistrictName(name: string | null | undefined): number | null {
  if (!name) {
    return null;
  }
  const match = /\bDistrict\s+(\d+)\b/i.exec(name);
  return match ? Number.parseInt(match[1]!, 10) : null;
}

/**
 * Chamber + district number from a committee-search jurisdiction
 * ("HOUSE DISTRICT 68"); null when the field is absent or another shape
 * (statewide registrations carry places or nothing).
 */
export function alabamaJurisdictionDistrict(
  jurisdiction: string | null | undefined
): { chamber: AlabamaDistrictChamber; district: number } | null {
  const match = /^\s*(HOUSE|SENATE)\s+DISTRICT\s+(\d+)\s*$/i.exec(jurisdiction ?? "");
  if (!match) {
    return null;
  }
  return {
    chamber: match[1]!.toUpperCase() as AlabamaDistrictChamber,
    district: Number.parseInt(match[2]!, 10),
  };
}

// "Julius Walker, Jr." — the comma marks the suffix, not a Last, First
// boundary, and it derails the shared parser's comma handling. Rewriting it
// to "Julius Walker Jr." keeps the suffix (so Sr-vs-Jr conflicts still
// reject) while restoring the natural word order.
function stripSuffixComma(value: string): string {
  return value.replace(/,\s*((?:JR|SR|II|III|IV)\.?)\s*$/i, " $1");
}

// Roster names in natural order get their first token expanded through the
// shared nickname table: FCPA race rows carry legal first names ("TUBERVILLE,
// THOMAS H" for roster "Tommy Tuberville" — live-observed 2026-08-28). A
// comma-form roster name is left alone (its first token is a surname).
function candidateNameVariants(candidateName: string): string[] {
  const cleaned = stripSuffixComma(candidateName).trim();
  const variants = [cleaned];
  const tokens = cleaned.split(/\s+/);
  if (tokens.length >= 2 && !cleaned.includes(",")) {
    for (const variant of firstNameVariants(normalizePersonName(tokens[0]!))) {
      variants.push([variant, ...tokens.slice(1)].join(" "));
    }
  }
  return variants;
}

function toMatch(
  row: AlabamaRaceRow,
  committee: AlabamaCommitteeSearchRow | undefined
): AlabamaCandidateRaceMatch {
  const committeeName = [
    committee?.candidateFirstName,
    committee?.candidateMiddleName,
    committee?.candidateLastName,
  ]
    .map((part) => part?.trim())
    .filter(Boolean)
    .join(" ");
  return {
    internalCommitteeId: row.COMMITTEEID,
    fcpaCommitteeNumber: committee?.committeeId?.trim() || null,
    raceCandidateName: row.CANDIDATE,
    candidateStatus: row.CANDIDATESTATUS?.trim() || null,
    committeeName: committeeName || null,
    jurisdiction: committee?.jurisdiction?.trim() || null,
  };
}

// Same-name rows within one office are usually ONE person's committee
// history, not two people: Alabama candidates re-register, and the race
// query keeps the predecessor (live 2026: Brad Mendheim appears Active AND
// Dissolved under Supreme Court Associate Justice). Auto-link may pick the
// Active row ONLY when that cannot drop money: exactly one row is Active
// and every other row carries zero cycle money (a dead registration). A
// predecessor with real money — Mendheim's dissolved committee raised
// $23,500 and spent $81,660.96 this cycle — goes to manual review instead,
// because linking the Active row alone would silently undercount and no
// summation policy exists yet (transfers between the committees would
// double-count naively).
function zeroMoney(row: AlabamaRaceRow): boolean {
  return (
    row.MONETARYCONTRIB === 0 &&
    row.NONMONETARYCONTRIB === 0 &&
    row.OTHERSOURCES === 0 &&
    row.MONETARYEXP === 0 &&
    row.BEGINNINGFUNDS === 0 &&
    row.ENDINGFUNDS === 0
  );
}

function activeStatusTieBreak(
  entries: readonly { row: AlabamaRaceRow; match: AlabamaCandidateRaceMatch }[]
): AlabamaCandidateRaceMatch | null {
  const active = entries.filter(
    (entry) => (entry.row.CANDIDATESTATUS ?? "").trim().toUpperCase() === "ACTIVE"
  );
  if (active.length !== 1) {
    return null;
  }
  const rest = entries.filter((entry) => entry !== active[0]);
  return rest.every((entry) => zeroMoney(entry.row)) ? active[0]!.match : null;
}

export function resolveAlabamaCandidateRace(input: {
  candidateName: string;
  /** Race rows already scoped to the FCPA office + election cycle. */
  raceRows: readonly AlabamaRaceRow[];
  /** Office-scoped committee-search rows, keyed by internal id (`id`). */
  committeeRowsByInternalId: ReadonlyMap<number, AlabamaCommitteeSearchRow>;
  /** Required for legislative offices; null/undefined for statewide. */
  district?: { chamber: AlabamaDistrictChamber; district: number } | null;
}): AlabamaCandidateRaceResolution {
  const candidateName = input.candidateName.trim();
  if (!candidateName) {
    return { status: "unmatched", reason: "missing_candidate_name" };
  }

  const nameMatches: { row: AlabamaRaceRow; match: AlabamaCandidateRaceMatch }[] = [];
  const seenCommitteeIds = new Set<number>();
  for (const row of input.raceRows) {
    if (seenCommitteeIds.has(row.COMMITTEEID)) {
      continue;
    }
    const rowName = stripSuffixComma(row.CANDIDATE);
    if (
      !candidateNameVariants(candidateName).some((variant) =>
        personNamesMatchWithMiddleEvidence({
          candidateName: variant,
          rowNames: [rowName],
          normalizePersonName,
        })
      )
    ) {
      continue;
    }
    seenCommitteeIds.add(row.COMMITTEEID);
    nameMatches.push({
      row,
      match: toMatch(row, input.committeeRowsByInternalId.get(row.COMMITTEEID)),
    });
  }
  if (nameMatches.length === 0) {
    return { status: "unmatched", reason: "no_matching_race_row" };
  }

  const district = input.district ?? null;
  if (district === null) {
    if (nameMatches.length > 1) {
      const tieBreak = activeStatusTieBreak(nameMatches);
      if (tieBreak !== null) {
        return { status: "matched", ...tieBreak };
      }
      return {
        status: "ambiguous",
        reason: "multiple_matching_race_rows",
        matches: nameMatches.map((entry) => entry.match),
      };
    }
    return { status: "matched", ...nameMatches[0]!.match };
  }

  // Districted office: the jurisdiction join is the only district evidence
  // (a race query returns every district's candidates flat), so a name match
  // with no committee row or no parseable jurisdiction can never be
  // auto-confirmed — same-named candidates in different districts are routine
  // in a 105-district chamber.
  const confirmed: { row: AlabamaRaceRow; match: AlabamaCandidateRaceMatch }[] = [];
  const unconfirmed: { reason: "missing_committee_row" | "missing_jurisdiction"; match: AlabamaCandidateRaceMatch }[] = [];
  let sawDistrictMismatch = false;
  for (const entry of nameMatches) {
    const committee = input.committeeRowsByInternalId.get(entry.row.COMMITTEEID);
    if (committee === undefined) {
      unconfirmed.push({ reason: "missing_committee_row", match: entry.match });
      continue;
    }
    const jurisdiction = alabamaJurisdictionDistrict(committee.jurisdiction);
    if (jurisdiction === null) {
      unconfirmed.push({ reason: "missing_jurisdiction", match: entry.match });
      continue;
    }
    if (jurisdiction.chamber === district.chamber && jurisdiction.district === district.district) {
      confirmed.push(entry);
    } else {
      sawDistrictMismatch = true;
    }
  }
  if (confirmed.length === 1) {
    // A same-named row that could not be district-checked may be the real
    // one — the confirmed row's uniqueness is not trustworthy.
    if (unconfirmed.length > 0) {
      return {
        status: "manual_confirm_required",
        reason: unconfirmed[0]!.reason,
        candidates: [confirmed[0]!.match, ...unconfirmed.map((entry) => entry.match)],
      };
    }
    return { status: "matched", ...confirmed[0]!.match };
  }
  if (confirmed.length > 1) {
    if (unconfirmed.length === 0) {
      const tieBreak = activeStatusTieBreak(confirmed);
      if (tieBreak !== null) {
        return { status: "matched", ...tieBreak };
      }
    }
    return {
      status: "ambiguous",
      reason: "multiple_matching_race_rows",
      matches: confirmed.map((entry) => entry.match),
    };
  }
  if (unconfirmed.length > 0) {
    return {
      status: "manual_confirm_required",
      reason: unconfirmed[0]!.reason,
      candidates: unconfirmed.map((entry) => entry.match),
    };
  }
  if (sawDistrictMismatch) {
    // Every name match sits in a different district: a same-named different
    // person, reported for review rather than silently unmatched.
    return {
      status: "manual_confirm_required",
      reason: "district_mismatch",
      candidates: nameMatches.map((entry) => entry.match),
    };
  }
  return { status: "unmatched", reason: "no_matching_race_row" };
}
