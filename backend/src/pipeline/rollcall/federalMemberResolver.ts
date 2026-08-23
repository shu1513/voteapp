import type { Pool } from "pg";

import type { Legislator, LegislatorIndex, LegislatorTermType } from "./congressLegislators.js";
import type { FederalMemberVote } from "./federalRollCallMembers.js";
import type { LegislativeVoteChamber } from "./legislativeVotes.js";

// Member → candidate resolution for the roll-call fan-out
// (docs/plans/roll-call-vote-import.md §2): bioguide / LIS id → the
// crosswalk person → their FEC ids → an exact match on candidates.fec_ids.
// Every other signal (name, state, district) only ever feeds the report;
// nothing attaches on a name. A match also requires that the person held a
// seat in that chamber and state on the vote date, so a stale or mistyped
// id cannot attach a vote to someone who was not there.

export type CandidateFecMatch = {
  candidateId: string;
  name: string;
  // On a Nov-2026-or-later office election (the importer's scope).
  inScope: boolean;
};

/** Upper-cased FEC id → every live candidate whose fec_ids carry it. */
export type CandidateFecIndex = ReadonlyMap<string, readonly CandidateFecMatch[]>;

export type FederalMemberResolutionOutcome =
  | "matched"
  // The id is not in the crosswalk at the pinned sha.
  | "unknown_member"
  // The person exists but no term of that chamber + state covers the vote
  // date.
  | "term_mismatch"
  | "no_fec_id"
  // No candidate carries any of the person's FEC ids.
  | "no_candidate"
  // Exactly one candidate, but not on a Nov-2026-or-later election.
  | "out_of_scope"
  // The person's FEC ids land on more than one candidate row.
  | "ambiguous";

export type FederalMemberResolution = {
  member: FederalMemberVote;
  outcome: FederalMemberResolutionOutcome;
  legislator: Pick<Legislator, "bioguide" | "name" | "fecIds"> | null;
  // The one candidate a `matched` row attaches to.
  candidate: CandidateFecMatch | null;
  // Every distinct candidate the FEC ids hit (explains out_of_scope and
  // ambiguous).
  candidates: CandidateFecMatch[];
  detail: string;
};

const TERM_TYPE_BY_CHAMBER: Record<LegislativeVoteChamber, LegislatorTermType> = { house: "rep", senate: "sen" };

function heldSeatOn(legislator: Legislator, chamber: LegislativeVoteChamber, state: string, voteDate: string): boolean {
  const type = TERM_TYPE_BY_CHAMBER[chamber];
  return legislator.terms.some(
    (term) => term.type === type && term.state === state && term.start <= voteDate && voteDate <= term.end
  );
}

export function resolveFederalMember(
  member: FederalMemberVote,
  voteDate: string,
  legislators: LegislatorIndex,
  candidatesByFec: CandidateFecIndex
): FederalMemberResolution {
  const legislator =
    member.chamber === "house" ? legislators.byBioguide.get(member.memberId) : legislators.byLis.get(member.memberId);
  const base = { member, legislator: null, candidate: null, candidates: [] as CandidateFecMatch[] };
  if (!legislator) {
    return { ...base, outcome: "unknown_member", detail: `${member.memberId} is not in congress-legislators` };
  }
  const summary = { bioguide: legislator.bioguide, name: legislator.name, fecIds: legislator.fecIds };
  const state = member.state.toUpperCase();
  if (!heldSeatOn(legislator, member.chamber, state, voteDate)) {
    return {
      ...base,
      legislator: summary,
      outcome: "term_mismatch",
      detail: `no ${TERM_TYPE_BY_CHAMBER[member.chamber]} term in ${state} covers ${voteDate}`,
    };
  }
  if (legislator.fecIds.length === 0) {
    return { ...base, legislator: summary, outcome: "no_fec_id", detail: "congress-legislators lists no FEC id" };
  }

  const candidates = new Map<string, CandidateFecMatch>();
  for (const fecId of legislator.fecIds) {
    for (const candidate of candidatesByFec.get(fecId) ?? []) {
      candidates.set(candidate.candidateId, candidate);
    }
  }
  const distinct = [...candidates.values()];
  if (distinct.length === 0) {
    return {
      ...base,
      legislator: summary,
      outcome: "no_candidate",
      detail: `no candidate carries ${legislator.fecIds.join(", ")}`,
    };
  }
  if (distinct.length > 1) {
    return {
      ...base,
      legislator: summary,
      candidates: distinct,
      outcome: "ambiguous",
      detail: `${legislator.fecIds.join(", ")} land on ${distinct.length} candidates`,
    };
  }
  const candidate = distinct[0]!;
  if (!candidate.inScope) {
    return {
      ...base,
      legislator: summary,
      candidates: distinct,
      outcome: "out_of_scope",
      detail: `${candidate.name} is not on a Nov-2026-or-later election`,
    };
  }
  return { ...base, legislator: summary, candidate, candidates: distinct, outcome: "matched", detail: candidate.name };
}

export function resolveFederalMembers(
  members: readonly FederalMemberVote[],
  voteDate: string,
  legislators: LegislatorIndex,
  candidatesByFec: CandidateFecIndex
): FederalMemberResolution[] {
  return members.map((member) => resolveFederalMember(member, voteDate, legislators, candidatesByFec));
}

type Queryable = Pick<Pool, "query">;

type CandidateFecRow = {
  candidate_id: string;
  name: string;
  fec_id: string;
  in_scope: boolean;
};

/**
 * Every FEC id on a live candidate, with whether that candidate is on an
 * office election dated `scopeFrom` or later (status not withdrawn/lost).
 * Loaded once per run: the whole table is a few hundred ids.
 */
export async function loadCandidateFecIndex(db: Queryable, scopeFrom: string): Promise<CandidateFecIndex> {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(scopeFrom)) {
    throw new Error(`scopeFrom must be an ISO date, got: ${scopeFrom}`);
  }
  const result = await db.query(
    `
      SELECT
        c.id AS candidate_id,
        coalesce(c.display_name, c.first_name || ' ' || c.last_name) AS name,
        upper(trim(fec.value)) AS fec_id,
        EXISTS (
          SELECT 1
          FROM candidate_elections AS ce
          JOIN elections AS e ON e.id = ce.election_id
          WHERE ce.candidate_id = c.id
            AND e.race_type = 'office'
            AND e.election_date >= $1::date
            AND ce.status NOT IN ('withdrawn', 'lost')
        ) AS in_scope
      FROM candidates AS c
      CROSS JOIN LATERAL jsonb_array_elements_text(c.fec_ids) AS fec(value)
      WHERE c.deleted_at IS NULL
        AND c.merged_into_candidate_id IS NULL
        AND jsonb_typeof(c.fec_ids) = 'array'
    `,
    [scopeFrom]
  );
  const index = new Map<string, CandidateFecMatch[]>();
  for (const raw of result.rows as CandidateFecRow[]) {
    const fecId = raw.fec_id.trim();
    if (fecId.length === 0) {
      continue;
    }
    const matches = index.get(fecId) ?? [];
    if (!matches.some((match) => match.candidateId === raw.candidate_id)) {
      matches.push({ candidateId: raw.candidate_id, name: raw.name, inScope: raw.in_scope });
    }
    index.set(fecId, matches);
  }
  return index;
}
