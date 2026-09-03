// Candidate -> CFRS committee resolution for West Virginia (plan hard fact
// 6: registry-only, exact-evidence, roster-authoritative).
//
// The committee registry (getPublicCandidatesCommitteeDataList) carries
// office, district, election and candidateName ("Last, First M.", suffix
// after the given names: "Oliverio, Michael Angelo II"). Evidence for a
// link is ALL of: State Candidate org type, the election-year cycle
// ("2026 Election"), the exact registry office label, the exact seat number,
// and a full-name match through the shared middle-evidence matcher (the
// roster's first name may be a nickname; the registry holds legal names).
// Pure functions — the auto-link fetches the registry once per batch and
// calls these.
//
// Fail-closed rules: never link on a surname alone, never link an ambiguous
// name, and never pick between two committees registered for the same seat
// (candidates re-register — 3 such pairs live in the 2026 population — and
// no summation policy exists, so the pair goes to manual review). A committee
// still registered under office "Undeclared" counts as the same seat, so a
// declared + undeclared pair for one person is ambiguous like any other.

import { firstNameVariants } from "../finance/personFirstNameNicknames.js";
import { personNamesMatchWithMiddleEvidence } from "../finance/personNameMiddleEvidence.js";
import type { WestVirginiaCommitteeRow } from "./westVirginiaCfrsClient.js";
import type { WestVirginiaRegistryOffice } from "./westVirginiaFinanceEligibleOffices.js";

const STATE_CANDIDATE_ORG_TYPE = "State Candidate";

// Registry office for a committee registered before the candidate declared
// a race (21 of the 429 "2026 Election" State Candidate rows, live 2026-09-01
// and again 2026-09-03). The seat number is still filled in, and the row
// stays "Undeclared" after the candidate is certified, so it is the only
// 2026 committee those candidates have. Accepted as office evidence only
// together with the exact seat + full-name match every other row needs.
const UNDECLARED_REGISTRY_OFFICE = "Undeclared";

export type WestVirginiaCommitteeMatch = {
  entityId: string;
  orgID: number;
  /** Registry orgName, or the candidate name when the registration has none. */
  committeeName: string;
  registryCandidateName: string;
  orgStatus: string;
  orgSubType: string | null;
  registrationYear: string | null;
};

export type WestVirginiaCommitteeResolution =
  | ({ status: "matched" } & WestVirginiaCommitteeMatch)
  | { status: "ambiguous"; reason: "multiple_matching_committees"; matches: WestVirginiaCommitteeMatch[] }
  | { status: "unmatched"; reason: "missing_candidate_name" | "no_matching_committee" };

// Stored-name normalization (the Delaware convention): diacritics stripped,
// uppercased, non-alphanumerics collapsed to single spaces.
export function normalizeWestVirginiaCandidateNameForStorage(value: string): string {
  return value
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toUpperCase()
    .replace(/[^A-Z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

// Match-time form: storage normalization plus generational-suffix stripping
// (the shared matcher reads suffixes from the RAW string and expects the
// normalizer to have removed them).
function normalizePersonName(value: string): string {
  return normalizeWestVirginiaCandidateNameForStorage(value)
    .replace(/\b(JR|SR|II|III|IV)\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

// "Julius Walker, Jr." on the roster: the comma marks a suffix, not a
// "Last, First" boundary. Rewriting it keeps the suffix (so Sr-vs-Jr
// conflicts still reject) while restoring the natural word order.
function stripSuffixComma(value: string): string {
  return value.replace(/,\s*((?:JR|SR|II|III|IV)\.?)\s*$/i, " $1");
}

// Both the registry (`Jeffries, Warren "Dean"`) and rosters (`Carl "Robbie"
// Martin`) write call names in double quotes; the shared matcher only reads
// the parenthesized convention, where a call name is an alias for the first
// name rather than a middle name. Rewriting keeps "Robbie" from contradicting
// "Robert" and lets a roster "Dean" align with the registry's "Warren".
function quotedCallNamesToParenthetical(value: string): string {
  return value.replace(/"([^"]+)"/g, "($1)");
}

// One-sided nickname expansion on the roster side only: the registry holds
// legal first names ("Michael") while rosters carry campaign names ("Mike").
// A comma-form roster name is left alone (its first token is a surname).
function candidateNameVariants(candidateName: string): string[] {
  const cleaned = quotedCallNamesToParenthetical(stripSuffixComma(candidateName)).trim();
  const variants = [cleaned];
  const tokens = cleaned.split(/\s+/);
  if (tokens.length >= 2 && !cleaned.includes(",")) {
    for (const variant of firstNameVariants(normalizePersonName(tokens[0]!))) {
      variants.push([variant, ...tokens.slice(1)].join(" "));
    }
  }
  return variants;
}

export function westVirginiaRegistryElectionLabel(electionYear: number): string {
  return `${electionYear} Election`;
}

function toMatch(row: WestVirginiaCommitteeRow, registryCandidateName: string): WestVirginiaCommitteeMatch {
  return {
    entityId: row.entityId,
    orgID: row.orgID,
    committeeName: row.orgName ?? registryCandidateName,
    registryCandidateName,
    orgStatus: row.orgStatus,
    orgSubType: row.orgSubType,
    registrationYear: row.registrationYear,
  };
}

export function resolveWestVirginiaCandidateCommittee(input: {
  candidateName: string;
  electionYear: number;
  registryOffice: WestVirginiaRegistryOffice;
  districtNumber: number;
  /** The full committee registry (all org types, all cycles). */
  committees: readonly WestVirginiaCommitteeRow[];
}): WestVirginiaCommitteeResolution {
  const candidateName = input.candidateName.trim();
  if (!candidateName) {
    return { status: "unmatched", reason: "missing_candidate_name" };
  }
  const election = westVirginiaRegistryElectionLabel(input.electionYear);
  const district = String(input.districtNumber);
  const variants = candidateNameVariants(candidateName);

  const matches: WestVirginiaCommitteeMatch[] = [];
  const seenEntityIds = new Set<string>();
  for (const row of input.committees) {
    if (
      row.orgType !== STATE_CANDIDATE_ORG_TYPE ||
      row.election !== election ||
      (row.office !== input.registryOffice && row.office !== UNDECLARED_REGISTRY_OFFICE) ||
      row.district !== district ||
      row.candidateName === null ||
      seenEntityIds.has(row.entityId)
    ) {
      continue;
    }
    const registryCandidateName = row.candidateName;
    if (
      !variants.some((variant) =>
        personNamesMatchWithMiddleEvidence({
          candidateName: variant,
          rowNames: [quotedCallNamesToParenthetical(registryCandidateName)],
          normalizePersonName,
        })
      )
    ) {
      continue;
    }
    seenEntityIds.add(row.entityId);
    matches.push(toMatch(row, registryCandidateName));
  }

  if (matches.length === 0) {
    return { status: "unmatched", reason: "no_matching_committee" };
  }
  if (matches.length > 1) {
    return {
      status: "ambiguous",
      reason: "multiple_matching_committees",
      matches: matches.sort((left, right) => left.entityId.localeCompare(right.entityId)),
    };
  }
  return { status: "matched", ...matches[0]! };
}
