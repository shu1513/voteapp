// Candidate → controlled-committee resolution for Denver (plan Phase 2).
// Much smaller than the CAL/Georgia name-parsing class: SearchLight's
// GetCandidatesByElectionCycle is an OFFICIAL candidate → committee mapping
// (fullName, officeSought, committeeId, filerId per row), so resolution is
// roster candidate ↔ registrant by normalized person name plus the at-large
// seat gate — the filerId then follows from the mapping. There is no
// committee-name-parsing evidence tier.
//
// Every registration anomaly the Phase 0 probe documented fails closed here
// (all verified live 2026-08-12):
// - Duplicate registrant names in one cycle ("Monica Martinez", filers
//   1322/1328): a candidate matching more than one registrant is ambiguous.
// - Registrants whose getElectionCyclesByFiler list omits the cycle (filers
//   1329/1330): blocked — the registration is inconsistent.
// - GetCommitteeDetailsByFiler reflects the committee's LATEST registration,
//   not the requested cycle (Johnston queried at cycle 26 answers cycle 33):
//   a details record whose cycle echo differs from the requested cycle is
//   blocked, since its committeeName/office cannot be trusted for this cycle.
// - Terminated filers, filer/committee echo mismatches, non-candidate
//   committee types, and missing committee names: blocked.
// - A registrant with NO details record at all (the endpoint answers HTTP 204
//   for filer 1328 at cycle 36): blocked, and deliberately still counted in
//   the duplicate-name check — dropping it would make its live sibling 1322
//   look unique and auto-link the wrong committee.
// A blocked registrant still blocks the candidate it name-matches (surfaced
// in the reason) — never silently dropped.

import { firstNameVariants } from "../finance/personFirstNameNicknames.js";
import { personNamesMatchWithMiddleEvidence } from "../finance/personNameMiddleEvidence.js";
import { parseDenverAtLargeSeatLetter } from "./denverFinanceEligibleOffices.js";
import {
  DENVER_SEARCHLIGHT_CANDIDATE_COMMITTEE_TYPE_ID,
  type DenverCommitteeDetails,
  type DenverCycleCandidate,
  type DenverFiler,
  type DenverFilerCycle,
} from "./denverSearchlightClient.js";

export type DenverAppCandidate = {
  candidateId: string;
  displayName: string;
  electionYear: number;
  /** Seat letter parsed from the roster ballot title; null fails closed. */
  atLargeSeatLetter: string | null;
};

/** One cycle registrant with its identity records, prefetched by the caller. */
export type DenverRegistrantRecord = {
  registrant: DenverCycleCandidate;
  filer: DenverFiler;
  cycles: readonly DenverFilerCycle[];
  /** null when SearchLight has no detail record for this filer/cycle (204). */
  details: DenverCommitteeDetails | null;
};

export type DenverCandidateCommitteeResolution = {
  candidate: DenverAppCandidate;
} & (
  | {
      status: "matched";
      filerId: number;
      committeeName: string;
      committeeEntityIds: number[];
    }
  | { status: "unmatched"; reason: string }
  | { status: "ambiguous"; reason: string }
);

export function normalizeDenverTextKey(value: string | null | undefined): string {
  return (value ?? "")
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toUpperCase()
    .replace(/&/g, " AND ")
    .replace(/[^A-Z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function normalizeDenverPersonName(value: string): string {
  return normalizeDenverTextKey(value)
    .replace(/\b(JR|SR|II|III|IV|V)\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

// Expand nicknames on the VoteApp side ONLY (the personFirstNameNicknames
// design rule): two distinct filed names can never meet at a shared key.
function firstNamesEquivalent(candidateFirst: string, rowFirst: string): boolean {
  return (
    candidateFirst === rowFirst ||
    firstNameVariants(candidateFirst).includes(rowFirst)
  );
}

/**
 * Person-name comparison between a SearchLight name and a roster display
 * name under the shared normalization, nickname, middle-evidence, and
 * generational-suffix gates — token-based, never substring. Roster display
 * names carry quoted call names ('Emanuel "Manny" Yekutiel'); rewritten to
 * the parenthetical form personNameParseVariants understands.
 */
export function denverPersonNameMatchesCandidate(
  rowPersonName: string,
  candidateDisplayName: string,
): boolean {
  return personNamesMatchWithMiddleEvidence({
    candidateName: candidateDisplayName.replace(/"([^"]+)"/g, " ($1) "),
    rowNames: [rowPersonName],
    normalizePersonName: normalizeDenverPersonName,
    firstNamesEquivalent,
  });
}

// Identity-record validation for one registrant (probe gate-11 rules as
// resolver law). Returns the blocking reason, or null when linkable.
function registrantBlockReason(
  electionCycleId: number,
  record: DenverRegistrantRecord,
): string | null {
  const { registrant, filer, cycles, details } = record;
  if (filer.filerId !== registrant.filerId)
    return `filer endpoint echoes ${filer.filerId} for registrant filer ${registrant.filerId}`;
  if (!filer.committeeIds.includes(registrant.committeeId))
    return `registration committee ${registrant.committeeId} is not on filer ${registrant.filerId}'s committee list [${filer.committeeIds.join(", ")}]`;
  if (filer.isTerminated) return `filer ${registrant.filerId} is terminated`;
  if (!cycles.some((cycle) => cycle.electionCycleId === electionCycleId))
    return `filer ${registrant.filerId}'s cycle list [${cycles.map((cycle) => cycle.electionCycleId).join(", ")}] omits cycle ${electionCycleId} — inconsistent registration`;
  if (!details)
    return `SearchLight has no committee details for filer ${registrant.filerId} in cycle ${electionCycleId}`;
  if (details.filerId !== registrant.filerId)
    return `committee details echo filer ${details.filerId} for registrant filer ${registrant.filerId}`;
  if (details.electionCycleId !== electionCycleId)
    return `committee details reflect cycle ${details.electionCycleId}, not the requested cycle ${electionCycleId} — latest-registration drift`;
  if (details.committeeId !== registrant.committeeId)
    return `committee details name committee ${details.committeeId}, registration names ${registrant.committeeId}`;
  if (details.committeeTypeId !== DENVER_SEARCHLIGHT_CANDIDATE_COMMITTEE_TYPE_ID)
    return `committee ${registrant.committeeId} has committeeTypeId ${details.committeeTypeId ?? "null"} (${details.committeeType ?? "unknown"}), not a candidate committee`;
  if (!details.committeeName)
    return `committee ${registrant.committeeId} has no committee name`;
  if (normalizeDenverTextKey(details.office) !== normalizeDenverTextKey(registrant.officeSought))
    return `committee details office "${details.office ?? ""}" disagrees with registration office "${registrant.officeSought ?? ""}"`;
  return null;
}

/**
 * Resolves each roster candidate against the cycle's registrant records.
 * Candidates without an at-large seat letter fail closed (the office gate
 * would be unverifiable). Anything not a clean one-to-one match — no
 * registrant, several registrants, a blocked registrant, one registrant
 * claimed by two candidates — fails closed for manual review.
 */
export function resolveDenverCandidateCommittees(input: {
  electionCycleId: number;
  candidates: readonly DenverAppCandidate[];
  registrants: readonly DenverRegistrantRecord[];
}): DenverCandidateCommitteeResolution[] {
  const resolutions: DenverCandidateCommitteeResolution[] = input.candidates.map(
    (candidate) => {
      if (!candidate.atLargeSeatLetter) {
        return {
          candidate,
          status: "unmatched",
          reason:
            "candidate's ballot title names no at-large seat letter; cannot verify the office gate",
        };
      }
      // Office gate first: a same-named registrant for a different contest is
      // a different race, not a match. Then the person-name gates.
      const nameMatches = input.registrants.filter(
        (record) =>
          parseDenverAtLargeSeatLetter(record.registrant.officeSought) ===
            candidate.atLargeSeatLetter &&
          denverPersonNameMatchesCandidate(
            record.registrant.fullName,
            candidate.displayName,
          ),
      );
      if (nameMatches.length > 1) {
        return {
          candidate,
          status: "ambiguous",
          reason: `${nameMatches.length} cycle registrants name-match (filers ${nameMatches
            .map((record) => record.registrant.filerId)
            .join(", ")}); link manually`,
        };
      }
      if (nameMatches.length === 0) {
        return {
          candidate,
          status: "unmatched",
          reason: "no cycle registrant name-matches",
        };
      }
      const record = nameMatches[0]!;
      const blocked = registrantBlockReason(input.electionCycleId, record);
      if (blocked) {
        return {
          candidate,
          status: "unmatched",
          reason: `cannot auto-link: ${blocked}`,
        };
      }
      return {
        candidate,
        status: "matched",
        filerId: record.registrant.filerId,
        // Non-null past registrantBlockReason: it blocks null details and a
        // details record with no committee name.
        committeeName: record.details!.committeeName!,
        committeeEntityIds: [...record.filer.committeeIds],
      };
    },
  );

  // One registrant resolving to two candidates would attach the same money to
  // both — fail all of its links closed instead of letting order decide.
  const candidatesByFilerId = new Map<number, number>();
  for (const resolution of resolutions) {
    if (resolution.status === "matched") {
      candidatesByFilerId.set(
        resolution.filerId,
        (candidatesByFilerId.get(resolution.filerId) ?? 0) + 1,
      );
    }
  }
  return resolutions.map((resolution) => {
    if (
      resolution.status === "matched" &&
      (candidatesByFilerId.get(resolution.filerId) ?? 0) > 1
    ) {
      return {
        candidate: resolution.candidate,
        status: "ambiguous",
        reason: `registrant filer ${resolution.filerId} resolves to multiple roster candidates; link manually`,
      };
    }
    return resolution;
  });
}
