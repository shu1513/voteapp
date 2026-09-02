// Candidate -> CFRS committee resolution for North Dakota (plan
// "northDakotaCandidateCommitteeResolver.ts: registry office/district/party/
// election evidence; never name-alone").
//
// The committee registry (getPublicCandidatesCommitteeDataList) carries
// office, district, election and candidateName in "Last, First M." form,
// sometimes with an honorific ("Mr. Sharbono, Doug", "Dr. O'Riley,
// Christine Ann", "Hon. ...") and a generational suffix on either side of the
// comma ("Lippert, Donald Jr.", "Johnston Sr, Daniel") — shapes pinned live
// 2026-09-01. Evidence for a link is ALL of: the candidate org type, the
// election-cycle label ("2026 Election - Statewide"), the exact registry
// office label, the exact seat ("District N") when the office is districted,
// and a full-name match through the shared middle-evidence matcher (the
// roster's first name may be a nickname; the registry holds legal names).
// Party is display data on both sides and is not evidence — a same-seat,
// same-name party disagreement is a data defect to surface, not a tiebreak.
// Pure functions — the auto-link fetches the registry once per batch.
//
// Fail-closed rules: never link on a surname alone, never link across a
// middle-name or suffix conflict, and never pick between two committees that
// both fit (re-registrations) — the pair goes to manual review.

import { firstNameVariants } from "../finance/personFirstNameNicknames.js";
import { personNamesMatchWithMiddleEvidence } from "../finance/personNameMiddleEvidence.js";
import {
  NORTH_DAKOTA_CANDIDATE_COMMITTEE_ORG_TYPE,
  type NorthDakotaCommitteeRow,
} from "./northDakotaCfrsClient.js";
import { northDakotaRegistryDistrictLabel, type NorthDakotaEligibleOffice } from "./northDakotaFinanceEligibleOffices.js";

export type NorthDakotaCommitteeMatch = {
  entityId: string;
  orgID: number;
  /** Registry orgName, or the candidate name (honorific dropped) for the 234 filers registered without a committee. */
  committeeName: string;
  registryCandidateName: string;
  orgStatus: string;
  orgSubType: string | null;
  party: string | null;
  registrationYear: string | null;
};

export type NorthDakotaCommitteeResolution =
  | ({ status: "matched" } & NorthDakotaCommitteeMatch)
  | { status: "ambiguous"; reason: "multiple_matching_committees"; matches: NorthDakotaCommitteeMatch[] }
  | { status: "unmatched"; reason: "missing_candidate_name" | "no_matching_committee" };

// Stored-name normalization (the Delaware convention): diacritics stripped,
// uppercased, non-alphanumerics collapsed to single spaces.
export function normalizeNorthDakotaCandidateNameForStorage(value: string): string {
  return value
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toUpperCase()
    .replace(/[^A-Z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

const HONORIFIC_PREFIX = /^(?:MR|MRS|MS|DR|HON)\s+/;

/** Registry candidate name without the honorific ("Mr. Sharbono, Doug" -> "Sharbono, Doug"). */
export function stripNorthDakotaHonorific(value: string): string {
  return value.replace(/^\s*(?:Mr|Mrs|Ms|Dr|Hon)\.?\s+/i, "").trim();
}

// Match-time form: storage normalization plus honorific and generational-
// suffix stripping (the shared matcher reads suffixes from the RAW string
// and expects the normalizer to have removed them). The honorific is
// stripped after normalization so it is caught on either side of the comma
// split ("Mr. Sharbono" is the surname half of a comma-form name).
function normalizePersonName(value: string): string {
  return normalizeNorthDakotaCandidateNameForStorage(value)
    .replace(HONORIFIC_PREFIX, "")
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

// One-sided nickname expansion on the roster side only: the registry holds
// legal first names ("Michael") while rosters carry campaign names ("Mike").
// A comma-form roster name is left alone (its first token is a surname).
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

/** Registry `election` label for a cycle ("2026 Election - Statewide"). */
export function northDakotaRegistryElectionLabel(electionYear: number): string {
  return `${electionYear} Election - Statewide`;
}

function toMatch(row: NorthDakotaCommitteeRow, registryCandidateName: string): NorthDakotaCommitteeMatch {
  return {
    entityId: row.entityId,
    orgID: row.orgID,
    committeeName: row.orgName ?? stripNorthDakotaHonorific(registryCandidateName),
    registryCandidateName,
    orgStatus: row.orgStatus,
    orgSubType: row.orgSubType,
    party: row.party,
    registrationYear: row.registrationYear,
  };
}

export function resolveNorthDakotaCandidateCommittee(input: {
  candidateName: string;
  electionYear: number;
  office: NorthDakotaEligibleOffice;
  /** Required when the office is districted; ignored otherwise. */
  districtNumber: number | null;
  /** The full committee registry (all org types, all cycles). */
  committees: readonly NorthDakotaCommitteeRow[];
}): NorthDakotaCommitteeResolution {
  const candidateName = input.candidateName.trim();
  if (!candidateName) {
    return { status: "unmatched", reason: "missing_candidate_name" };
  }
  if (input.office.districted && input.districtNumber === null) {
    throw new Error(`North Dakota ${input.office.registryOffice} resolution requires a district number`);
  }
  const election = northDakotaRegistryElectionLabel(input.electionYear);
  const district = input.office.districted ? northDakotaRegistryDistrictLabel(input.districtNumber!) : null;
  const variants = candidateNameVariants(candidateName);

  const matches: NorthDakotaCommitteeMatch[] = [];
  const seenEntityIds = new Set<string>();
  for (const row of input.committees) {
    if (
      row.orgType !== NORTH_DAKOTA_CANDIDATE_COMMITTEE_ORG_TYPE ||
      row.election !== election ||
      row.office !== input.office.registryOffice ||
      (district !== null && row.district !== district) ||
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
          rowNames: [registryCandidateName],
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
