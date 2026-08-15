import { firstNamesConflict, firstNameVariants } from "../finance/personFirstNameNicknames.js";
import { personNamesMatchWithMiddleEvidence } from "../finance/personNameMiddleEvidence.js";
import {
  searchErtsOrganizations,
  selectErtsOrganizationOrgId,
  type ErtsTransport,
} from "./rhodeIslandErtsClient.js";
import type { ErtsOrganizationSearchRow } from "./rhodeIslandErtsParsers.js";
import { isRhodeIslandFinanceEligibleOffice } from "./rhodeIslandFinanceEligibleOffices.js";

// Committee resolver for Rhode Island (rhode_island_plan.md PR 5). ERTS has
// no bulk registry export: the only committee evidence is the portal's own
// organization search, whose result grid carries a person-style organization
// name ("DANIEL J MCKEE"), an address, and the Board's Active/Inactive status
// — no office, no district, no election year. Resolution is therefore a
// person-name match under the shared normalization, nickname, middle-name,
// and generational-suffix gates (denver pattern), gated to Active
// registrations, and fail-closed on anything that cannot be proven from the
// one page in hand:
//   - a paginated result grid refuses to resolve (rows are an incomplete
//     slice and the pager mechanics were never probed);
//   - two Active matches are ambiguous — the Board lets registrations share
//     a name, and row order is not identity evidence (spike review fix d);
//   - an Inactive-only match refuses rather than linking a closed
//     registration to a live 2026 candidacy (the manual-link escape hatch
//     covers the genuine exceptions).
// The numeric Board key (OrgID) never appears in the grid; the live wrapper
// reads it off the dated search's redirect after selecting the matched row.

export type RhodeIslandCandidateCommitteeResolverInput = {
  candidateName: string;
  officeScope: string;
  officeName: string;
  searchRows: readonly ErtsOrganizationSearchRow[];
  searchHasMorePages: boolean;
};

export type RhodeIslandCandidateCommitteeRowMatch = {
  organizationName: string;
  postbackTarget: string;
  status: string;
};

export type RhodeIslandCandidateCommitteeUnmatchedReason =
  | "missing_candidate_name"
  | "unsupported_office"
  | "paginated_search_results"
  | "no_organization_match"
  | "unknown_registration_status"
  | "no_active_organization_match";

export type RhodeIslandCandidateCommitteeRowResolution =
  | {
      status: "matched";
      match: RhodeIslandCandidateCommitteeRowMatch;
      // Name-matching rows the Active gate excluded — surfaced so a run
      // report can show why a candidate resolved past an old registration.
      inactiveMatchCount: number;
    }
  | {
      status: "unmatched";
      reason: RhodeIslandCandidateCommitteeUnmatchedReason;
      candidateNameNormalized: string;
    }
  | {
      status: "ambiguous";
      reason: "multiple_active_organization_matches";
      candidateNameNormalized: string;
      matches: RhodeIslandCandidateCommitteeRowMatch[];
    };

export function normalizeRhodeIslandTextKey(value: string | null | undefined): string {
  return (value ?? "")
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toUpperCase()
    .replace(/&/g, " AND ")
    .replace(/[^A-Z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function normalizeRhodeIslandPersonName(value: string): string {
  return normalizeRhodeIslandTextKey(value)
    .replace(/\b(JR|SR|II|III|IV|V)\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

export function normalizeRhodeIslandCandidateNameForStorage(value: string): string {
  return normalizeRhodeIslandPersonName(value.replace(/\([^()]+\)/g, " "));
}

// Search token for the portal's "Organization Last Name" field: the final
// token of the normalized display name (comma forms flip). Georgia's
// surname-token derivation (#629), verbatim semantics.
export function rhodeIslandLastNameSearchToken(candidateName: string): string {
  const trimmed = candidateName.replace(/\([^()]+\)/g, " ").trim();
  if (trimmed.includes(",")) {
    const commaFirst = normalizeRhodeIslandPersonName(trimmed.split(",", 1)[0]!);
    if (commaFirst) {
      return commaFirst;
    }
  }
  const normalized = normalizeRhodeIslandPersonName(trimmed);
  return normalized.split(/\s+/).filter(Boolean).at(-1) ?? trimmed;
}

// Expand nicknames on the VoteApp side ONLY (the personFirstNameNicknames
// design rule): two distinct registered names can never meet at a shared key.
//
// That module's shared-nickname tradeoff ("Pat Smith" may link the one
// registered PATRICIA SMITH) leans on office/district/year agreement to break
// ties — corroboration the ERTS grid does not carry. So a bridge that could
// also reach a DIFFERENT formal family (PAT → PATRICK or PATRICIA, SAM →
// SAMUEL or SAMANTHA, TED → EDWARD or THEODORE) is not usable evidence here
// and refuses; the manual-link escape hatch covers the genuine cases.
// Unambiguous bridges (MIKE → MICHAEL) and formal spelling variants of one
// name (STEVE → STEPHEN/STEVEN) still match.
function firstNamesEquivalent(candidateFirst: string, rowFirst: string): boolean {
  if (candidateFirst === rowFirst) {
    return true;
  }
  if (!firstNameVariants(candidateFirst).includes(rowFirst)) {
    return false;
  }
  // Checked from both ends: PAT → PATRICIA is vetoed because PAT also
  // reaches PATRICK, and PATRICK → PAT is vetoed because the registered PAT
  // could be a Patricia.
  const bridgesConflictingFamily =
    firstNameVariants(candidateFirst).some((variant) => firstNamesConflict(variant, rowFirst)) ||
    firstNameVariants(rowFirst).some((variant) => firstNamesConflict(variant, candidateFirst));
  return !bridgesConflictingFamily;
}

/**
 * Person-name comparison between an ERTS organization name and a VoteApp
 * display name under the shared normalization, nickname, middle-evidence,
 * and generational-suffix gates — token-based, never substring. Quoted call
 * names ('Daniel "Dan" McKee') are rewritten to the parenthetical form
 * personNameParseVariants understands.
 */
export function rhodeIslandOrganizationNameMatchesCandidate(
  organizationName: string,
  candidateDisplayName: string
): boolean {
  return personNamesMatchWithMiddleEvidence({
    candidateName: candidateDisplayName.replace(/"([^"]+)"/g, " ($1) "),
    rowNames: [organizationName],
    normalizePersonName: normalizeRhodeIslandPersonName,
    firstNamesEquivalent,
  });
}

const ACTIVE_STATUS = "ACTIVE";
// The Board's full status vocabulary as proven live (2026-08-13 spike).
const KNOWN_STATUSES = new Set([ACTIVE_STATUS, "INACTIVE"]);

/** Pure resolution over one search page's rows; see the module header. */
export function resolveRhodeIslandCandidateCommitteeRows(
  input: RhodeIslandCandidateCommitteeResolverInput
): RhodeIslandCandidateCommitteeRowResolution {
  const candidateNameNormalized = normalizeRhodeIslandCandidateNameForStorage(input.candidateName);
  if (!candidateNameNormalized) {
    return { status: "unmatched", reason: "missing_candidate_name", candidateNameNormalized };
  }
  if (
    !isRhodeIslandFinanceEligibleOffice({
      officeScope: input.officeScope,
      officeCanonicalName: input.officeName,
    })
  ) {
    return { status: "unmatched", reason: "unsupported_office", candidateNameNormalized };
  }
  if (input.searchHasMorePages) {
    return { status: "unmatched", reason: "paginated_search_results", candidateNameNormalized };
  }

  const nameMatches = input.searchRows.filter((row) =>
    rhodeIslandOrganizationNameMatchesCandidate(row.organizationName, input.candidateName)
  );
  if (nameMatches.length === 0) {
    return { status: "unmatched", reason: "no_organization_match", candidateNameNormalized };
  }

  const toMatch = (row: ErtsOrganizationSearchRow): RhodeIslandCandidateCommitteeRowMatch => ({
    organizationName: row.organizationName,
    postbackTarget: row.postbackTarget,
    status: row.status,
  });
  // A name-matching row with a status outside the pinned vocabulary refuses
  // outright: a drifted column must neither widen the gate NOR narrow it —
  // a renamed active status ("Current") silently dropped here could hide a
  // second candidate that should have made the resolution ambiguous.
  if (nameMatches.some((row) => !KNOWN_STATUSES.has(row.status.toUpperCase()))) {
    return { status: "unmatched", reason: "unknown_registration_status", candidateNameNormalized };
  }
  const activeMatches = nameMatches.filter((row) => row.status.toUpperCase() === ACTIVE_STATUS);
  if (activeMatches.length === 0) {
    return { status: "unmatched", reason: "no_active_organization_match", candidateNameNormalized };
  }
  if (activeMatches.length > 1) {
    return {
      status: "ambiguous",
      reason: "multiple_active_organization_matches",
      candidateNameNormalized,
      matches: activeMatches.map(toMatch),
    };
  }
  return {
    status: "matched",
    match: toMatch(activeMatches[0]!),
    inactiveMatchCount: nameMatches.length - activeMatches.length,
  };
}

export type RhodeIslandCandidateCommitteeSearchInput = {
  candidateName: string;
  officeScope: string;
  officeName: string;
  // Portal-format US dates for the dated search that surfaces the OrgID
  // (the RI cycle window; any valid window works — the redirect carries the
  // key regardless of what the window contains).
  cycleBeginUs: string;
  cycleEndUs: string;
};

export type RhodeIslandCandidateCommitteeResolution =
  | {
      status: "matched";
      orgId: string;
      organizationName: string;
      searchLastName: string;
      confidence: "exact";
      source: "erts_organization_search";
      sourceUrl: string;
      inactiveMatchCount: number;
    }
  | {
      status: "unmatched";
      reason: RhodeIslandCandidateCommitteeUnmatchedReason;
      candidateNameNormalized: string;
    }
  | {
      status: "ambiguous";
      reason: "multiple_active_organization_matches";
      candidateNameNormalized: string;
      matches: RhodeIslandCandidateCommitteeRowMatch[];
    };

/**
 * Live resolution: search the portal by the candidate's surname token,
 * resolve the rows, and — on the one Active match — select that exact row
 * (its own postback target, no name-string round trip) and read the numeric
 * Board key off the dated search's redirect. Five paced requests when a
 * match exists, three otherwise.
 */
export async function searchAndResolveRhodeIslandCandidateCommittee(
  input: RhodeIslandCandidateCommitteeSearchInput,
  transport: ErtsTransport
): Promise<RhodeIslandCandidateCommitteeResolution> {
  const searchLastName = rhodeIslandLastNameSearchToken(input.candidateName);
  const search = await searchErtsOrganizations(transport, { lastName: searchLastName });
  const resolution = resolveRhodeIslandCandidateCommitteeRows({
    candidateName: input.candidateName,
    officeScope: input.officeScope,
    officeName: input.officeName,
    searchRows: search.rows,
    searchHasMorePages: search.hasMorePages,
  });
  if (resolution.status !== "matched") {
    return resolution;
  }
  const orgId = await selectErtsOrganizationOrgId(transport, {
    searchResultsHtml: search.html,
    postbackTarget: resolution.match.postbackTarget,
    begin: input.cycleBeginUs,
    end: input.cycleEndUs,
  });
  return {
    status: "matched",
    orgId,
    organizationName: resolution.match.organizationName,
    searchLastName,
    confidence: "exact",
    source: "erts_organization_search",
    sourceUrl: search.url,
    inactiveMatchCount: resolution.inactiveMatchCount,
  };
}
