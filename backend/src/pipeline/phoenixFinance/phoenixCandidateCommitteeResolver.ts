// Candidate → controlled-committee resolution for Phoenix (plan Phase 2).
// Unlike the CAL-export cities (SJ/SD), the Phoenix registration index
// carries a first-class CandidateName, a committee-type display string, and
// the election year sought — so committee identity never rests on parsing a
// committee name. The evidence chain (plan "Architecture", re-verified live
// 2026-08-12):
//
//   1. Certified-roster COP IDs: roster research reads the Clerk's certified
//      candidate list and stores each candidate's COP ID in
//      candidates.state_filing_ids. A COP-shaped stored id is curated Phoenix
//      evidence (the SD clerk-log precedent): the registration must confirm
//      it — approved, Candidate Committee, not terminated, CandidateName
//      matches, cycle matches — and any contradiction FAILS CLOSED for manual
//      review instead of falling through to weaker evidence.
//   2. Registration evidence: canonical registration (latest approved
//      version per COP ID, phoenixEfilingClient), CandidateName matched via
//      the shared middle-evidence / nickname / generational-suffix gates,
//      and OfficeSoughtElectionCycle ("2026") landing in the same portal
//      candidate cycle as the election date. The cycle gate deliberately
//      compares CYCLES, not years, so a March runoff (2027) still matches a
//      "2026" registration, while a stale "2022" one never does. A 2023-cycle
//      registration legitimately targets 2026 (Jimenez CAN-23-5) — the
//      ElectionCycle display string is never evidence.
//   3. Report-cover "Office Sought" ("Council Member District 4", verified
//      machine-readable) corroborates office + district for the name tier.
//      Candidates WITHOUT a stored COP ID resolve by name only when a parsed
//      cover corroborates the district — a committee with no report and
//      name-only evidence fails closed (plan rule). Cover data is supplied by
//      the caller (the Phase 3 report parser); until it exists the name tier
//      simply fails closed, which is safe because every rostered candidate
//      carries a curated COP ID.
//   4. Committee-name evidence (district digits, MAYOR/COUNCIL words) is a
//      confirming veto when present, never a match requirement.
//
// Anything else — no match, two registrations matching one candidate, one
// committee matching two candidates — fails closed for manual review.

import { firstNameVariants } from "../finance/personFirstNameNicknames.js";
import { personNamesMatchWithMiddleEvidence } from "../finance/personNameMiddleEvidence.js";
import {
  PHOENIX_TEST_COMMITTEE_PATTERN,
  phoenixCandidateCycleForDate,
  type PhoenixRegistrationRow,
} from "./phoenixEfilingClient.js";
import { isPhoenixCityCouncilDistrictNumber } from "./phoenixFinanceEligibleOffices.js";

/** Registration CommitteeType display string for candidate committees
 * (verified live; PACs read "Political Action Committee"). */
export const PHOENIX_CANDIDATE_COMMITTEE_TYPE = "Candidate Committee";

/** COP-shaped filing ids (CAN-25-4). state_filing_ids accumulates ids from
 * every race the person runs, so only COP-shaped entries count as Phoenix
 * evidence; anything else (an AZ SOS committee id, a CA FPPC id) is ignored
 * here rather than misread. */
const COP_ID_PATTERN = /^(?:CAN|PAC|IE|MC)-\d{2}-\d+$/;

export type PhoenixAppCandidate = {
  candidateId: string;
  displayName: string;
  officeName: "Mayor" | "City Council Member";
  /** Council district (1–8); null for Mayor. */
  districtNumber: number | null;
  electionYear: number;
  /** ISO election date; anchors the portal-cycle gate. */
  electionDate: string;
  /** Filing ids from candidates.state_filing_ids ([] when none). */
  stateFilingIds: readonly string[];
};

export type PhoenixCandidateCommitteeResolution = {
  candidate: PhoenixAppCandidate;
} & (
  | {
      status: "matched";
      copId: string;
      committeeName: string;
      /** Registration ElectionCycle display string, stored verbatim on the
       * link row as portal_cycle_name. */
      portalCycleName: string;
      matchedBy: "cop_id" | "name";
    }
  | { status: "unmatched"; reason: string }
  | { status: "ambiguous"; reason: string }
);

export function normalizePhoenixTextKey(
  value: string | null | undefined,
): string {
  return (value ?? "")
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toUpperCase()
    .replace(/&/g, " AND ")
    .replace(/[^A-Z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function normalizePhoenixPersonName(value: string): string {
  return normalizePhoenixTextKey(value)
    .replace(/\b(JR|SR|II|III|IV|V)\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

// Expand nicknames on the VoteApp side ONLY (the personFirstNameNicknames
// design rule): two distinct filed names can never meet at a shared key.
function firstNamesEquivalent(candidateFirst: string, rowFirst: string): boolean {
  return candidateFirst === rowFirst || firstNameVariants(candidateFirst).includes(rowFirst);
}

// "Name, Jr." (comma before a trailing generational suffix — live on
// CAN-26-3: CandidateName "Jarrett Barton Maupin, Jr.") parses to NOTHING in
// the shared parser: the comma reads as a surname boundary whose remainder
// is empty once the normalizer strips the suffix, so no pair ever aligns.
// Dropping ONLY the comma keeps the suffix in the string, so the shared
// Jr/Sr generational veto still sees it ("Maupin, Sr." must keep vetoing a
// "Maupin Jr." candidate). Same local-rewrite pattern as the quoted
// call-name rewrite below.
function rewriteCommaSuffix(name: string): string {
  return name.replace(/,(?=\s*(?:JR|SR|II|III|IV|V)\.?\s*$)/i, "");
}

/**
 * Person-name comparison against a roster display name under the Phoenix
 * normalization, nickname expansion, middle-evidence, and generational-suffix
 * gates — token-based, never substring. Used for the registration
 * CandidateName here and reused by the Phase 3 outside-spending aggregator
 * for Schedule B(6) target names (which can be BLANK live — callers fail
 * closed on empty input before calling this).
 */
export function phoenixPersonNameMatchesCandidate(
  rowPersonName: string,
  candidateDisplayName: string,
): boolean {
  return personNamesMatchWithMiddleEvidence({
    candidateName: rewriteCommaSuffix(
      candidateDisplayName.replace(/"([^"]+)"/g, " ($1) "),
    ),
    rowNames: [rewriteCommaSuffix(rowPersonName)],
    normalizePersonName: normalizePhoenixPersonName,
    firstNamesEquivalent,
  });
}

// Committee-name evidence (district digits, MAYOR/COUNCIL words) — a
// confirming veto when present ("Matt For District 2" observed live), never
// a match requirement ("Ed Hermes for Phoenix" carries none).
function committeeNameConflictsWithCandidate(
  committeeName: string,
  candidate: PhoenixAppCandidate,
): boolean {
  const normalized = normalizePhoenixTextKey(committeeName);
  const districts = new Set<number>();
  for (const match of normalized.matchAll(/\bDISTRICT (?:NO )?(\d{1,2})\b/g)) {
    districts.add(Number(match[1]));
  }
  for (const match of normalized.matchAll(/\bD(\d{1,2})\b/g)) {
    districts.add(Number(match[1]));
  }
  const mentionsMayor = /\bMAYOR[A-Z]*\b/.test(normalized);
  const mentionsCouncil = /\bCOUNCIL[A-Z]*\b/.test(normalized);
  if (candidate.officeName === "Mayor") {
    return mentionsCouncil || districts.size > 0;
  }
  if (mentionsMayor) return true;
  return districts.size > 0 && !districts.has(candidate.districtNumber!);
}

// The portal-cycle gate: OfficeSoughtElectionCycle is the ELECTION YEAR
// sought ("2026"); it corroborates when that election's November date falls
// in the same Phoenix candidate cycle (Apr 1 odd year → Mar 31 + 2y) as the
// row's election date — so a 2027-03 runoff row still matches a "2026"
// registration, and a "2022" one never does. Missing/unparseable fails.
function officeSoughtCycleMatches(
  officeSoughtElectionCycle: string | null,
  electionDate: string,
): boolean {
  if (
    officeSoughtElectionCycle === null ||
    !/^\d{4}$/.test(officeSoughtElectionCycle)
  ) {
    return false;
  }
  return (
    phoenixCandidateCycleForDate(`${officeSoughtElectionCycle}-11-01`)
      .startYear === phoenixCandidateCycleForDate(electionDate).startYear
  );
}

/** Every reason a canonical registration cannot serve as this candidate's
 * controlled committee; empty = all gates pass. */
function committeeGateFailures(
  committee: PhoenixRegistrationRow,
  candidate: PhoenixAppCandidate,
): string[] {
  const reasons: string[] = [];
  if (committee.committeeType !== PHOENIX_CANDIDATE_COMMITTEE_TYPE) {
    reasons.push(
      `registration ${committee.copId} is "${committee.committeeType}", not a ${PHOENIX_CANDIDATE_COMMITTEE_TYPE}`,
    );
  }
  if (PHOENIX_TEST_COMMITTEE_PATTERN.test(committee.committeeName)) {
    reasons.push(`registration ${committee.copId} is a test committee`);
  }
  if (committee.terminated) {
    reasons.push(`registration ${committee.copId} is terminated`);
  }
  if (committee.candidateName === null) {
    reasons.push(`registration ${committee.copId} carries no CandidateName`);
  } else if (
    !phoenixPersonNameMatchesCandidate(
      committee.candidateName,
      candidate.displayName,
    )
  ) {
    reasons.push(
      `registration ${committee.copId} CandidateName "${committee.candidateName}" does not match "${candidate.displayName}"`,
    );
  }
  if (
    !officeSoughtCycleMatches(
      committee.officeSoughtElectionCycle,
      candidate.electionDate,
    )
  ) {
    reasons.push(
      `registration ${committee.copId} office-sought cycle "${committee.officeSoughtElectionCycle ?? "(none)"}" is not this election's portal cycle`,
    );
  }
  if (committeeNameConflictsWithCandidate(committee.committeeName, candidate)) {
    reasons.push(
      `registration ${committee.copId} name "${committee.committeeName}" carries contradictory district/office evidence`,
    );
  }
  return reasons;
}

// Cover "Office Sought" corroboration for the name tier. Council format is
// verified live ("Council Member District 4"); the Mayor pattern is the
// natural reading of the same field and stays fail-closed until a live
// mayoral cover pins it.
function coverCorroboratesCandidate(
  officeSought: string,
  candidate: PhoenixAppCandidate,
): boolean {
  const normalized = normalizePhoenixTextKey(officeSought);
  const districtMatch = /\bCOUNCIL MEMBER DISTRICT (\d{1,2})\b/.exec(normalized);
  if (candidate.officeName === "Mayor") {
    return districtMatch === null && /\bMAYOR\b/.test(normalized);
  }
  return (
    districtMatch !== null &&
    Number(districtMatch[1]) === candidate.districtNumber
  );
}

/**
 * Resolves each candidate against the canonical registration index: the
 * curated COP-id tier, then the fully gated name tier (which additionally
 * requires report-cover corroboration). Council candidates without a valid
 * district fail closed (district evidence would be unverifiable).
 */
export function resolvePhoenixCandidateCommittees(input: {
  candidates: readonly PhoenixAppCandidate[];
  /** Canonical registrations (one per COP ID), ALL committee types — the
   * resolver gates on type itself so a stored id pointing at a PAC fails
   * with a precise reason. */
  committees: readonly PhoenixRegistrationRow[];
  /** Parsed report-cover "Office Sought" values per COP ID (Phase 3 report
   * parser). Omitted/empty = the name tier fails closed, by design. */
  coverOfficeSoughtByCopId?: ReadonlyMap<string, readonly string[]>;
}): PhoenixCandidateCommitteeResolution[] {
  const covers = input.coverOfficeSoughtByCopId ?? new Map<string, readonly string[]>();
  const byCopId = new Map<string, PhoenixRegistrationRow>();
  for (const committee of input.committees) {
    byCopId.set(committee.copId, committee);
  }

  const resolutions: PhoenixCandidateCommitteeResolution[] = input.candidates.map(
    (candidate) => {
      if (
        candidate.officeName === "City Council Member" &&
        !isPhoenixCityCouncilDistrictNumber(candidate.districtNumber)
      ) {
        return {
          candidate,
          status: "unmatched",
          reason:
            "Council candidate has no valid district number; cannot verify district evidence",
        };
      }

      const copIds = [
        ...new Set(
          candidate.stateFilingIds
            .map((id) => id.trim().toUpperCase())
            .filter((id) => COP_ID_PATTERN.test(id)),
        ),
      ];

      if (copIds.length > 0) {
        // COP-id tier ONLY: the stored id is curated roster evidence, so a
        // contradiction is a data problem for manual review — never a signal
        // to fall through to name matching (module header).
        const problems: string[] = [];
        const passing: PhoenixRegistrationRow[] = [];
        for (const copId of copIds) {
          const committee = byCopId.get(copId);
          if (committee === undefined) {
            problems.push(
              `stored COP id ${copId} has no approved registration in the portal index`,
            );
            continue;
          }
          const reasons = committeeGateFailures(committee, candidate);
          if (reasons.length > 0) {
            problems.push(...reasons);
            continue;
          }
          passing.push(committee);
        }
        if (passing.length > 1) {
          return {
            candidate,
            status: "ambiguous",
            reason: `stored COP ids match ${passing.length} registrations: ${passing.map((committee) => committee.copId).join(", ")}`,
          };
        }
        if (passing.length === 1) {
          const committee = passing[0]!;
          return {
            candidate,
            status: "matched",
            copId: committee.copId,
            committeeName: committee.committeeName,
            portalCycleName: committee.electionCycle,
            matchedBy: "cop_id",
          };
        }
        return {
          candidate,
          status: "unmatched",
          reason: `stored COP id evidence is contradicted; review manually: ${problems.join("; ")}`,
        };
      }

      // Name tier: registrations that pass every gate with a matching
      // CandidateName. Gate failures (terminated, wrong cycle, PAC, name
      // evidence) are affirmative "not this contest" signals and skip
      // silently — the terminated prior-cycle committee of the SAME person
      // is the expected sibling here (Hermes CAN-23-7).
      const corroborated: PhoenixRegistrationRow[] = [];
      const blocked: { committee: PhoenixRegistrationRow; why: string }[] = [];
      for (const committee of input.committees) {
        if (
          committee.candidateName === null ||
          !phoenixPersonNameMatchesCandidate(
            committee.candidateName,
            candidate.displayName,
          )
        ) {
          continue;
        }
        if (committeeGateFailures(committee, candidate).length > 0) continue;
        const officeSoughtValues = covers.get(committee.copId) ?? [];
        if (officeSoughtValues.length === 0) {
          blocked.push({
            committee,
            why: `registration ${committee.copId} ("${committee.committeeName}") has no parsed report cover; name-only evidence fails closed`,
          });
          continue;
        }
        if (
          !officeSoughtValues.some((value) =>
            coverCorroboratesCandidate(value, candidate),
          )
        ) {
          blocked.push({
            committee,
            why: `registration ${committee.copId} report cover office "${officeSoughtValues.join("; ")}" does not corroborate this contest`,
          });
          continue;
        }
        corroborated.push(committee);
      }

      if (corroborated.length > 1) {
        return {
          candidate,
          status: "ambiguous",
          reason: `${corroborated.length} registrations name-match with corroborating covers: ${corroborated.map((committee) => committee.copId).join(", ")}`,
        };
      }
      // A name-matching registration that cannot corroborate makes the single
      // corroborated match uncertain too — never auto-pick over an unresolved
      // sibling.
      if (blocked.length > 0) {
        return {
          candidate,
          status: "unmatched",
          reason: `cannot auto-link: ${blocked.map((entry) => entry.why).join("; ")}`,
        };
      }
      if (corroborated.length === 1) {
        const committee = corroborated[0]!;
        return {
          candidate,
          status: "matched",
          copId: committee.copId,
          committeeName: committee.committeeName,
          portalCycleName: committee.electionCycle,
          matchedBy: "name",
        };
      }
      return {
        candidate,
        status: "unmatched",
        reason: "no candidate-committee registration matches this candidate",
      };
    },
  );

  // One committee resolving to two candidates would attach the same money to
  // both — fail all of its links closed instead of letting order decide.
  const candidatesByCopId = new Map<string, number>();
  for (const resolution of resolutions) {
    if (resolution.status === "matched") {
      candidatesByCopId.set(
        resolution.copId,
        (candidatesByCopId.get(resolution.copId) ?? 0) + 1,
      );
    }
  }
  return resolutions.map((resolution) => {
    if (
      resolution.status === "matched" &&
      (candidatesByCopId.get(resolution.copId) ?? 0) > 1
    ) {
      return {
        candidate: resolution.candidate,
        status: "ambiguous",
        reason: `committee ${resolution.copId} resolves to multiple roster candidates; link manually`,
      };
    }
    return resolution;
  });
}
