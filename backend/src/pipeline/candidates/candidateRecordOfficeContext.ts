import type { PoolClient } from "pg";
import type { ElectionContestFamily } from "../../types/election.js";
import { getPresidentialGeneralElectionDate } from "../presidential/presidentialCycles.js";

type Queryable = Pick<PoolClient, "query">;

export type CandidateRecordPresidentialRole = "president" | "vice_president";

export type CandidateElectionOfficeContext = {
  candidateId: string;
  candidateDisplayName: string;
  currentOffice: string | null;
  hasHeldPublicOffice: boolean | null;
  electionId: string;
  presidentialCycleId?: string | null;
  presidentialRole?: CandidateRecordPresidentialRole | null;
  districtName: string;
  districtType: string;
  state: string;
  electionDate: string;
  officialBallotTitle: string;
  electionStage: string | null;
  senateClass: string | null;
  termEndYear: string | null;
  officeId: string | null;
  discoveryContestFamily: ElectionContestFamily | null;
  electionSources: unknown;
};

type PresidentialCycleRecordContextRow = {
  candidateId: string;
  candidateDisplayName: string;
  currentOffice: string | null;
  hasHeldPublicOffice: boolean | null;
  presidentialCycleId: string;
  electionYear: number;
  stage: string;
  party: string | null;
  officeId: string;
  sources: unknown;
};

function presidentialOfficeNameForRole(role: CandidateRecordPresidentialRole): string {
  return role === "vice_president"
    ? "Vice President of the United States"
    : "President of the United States";
}

function presidentialOfficialBallotTitleForRole(
  role: CandidateRecordPresidentialRole,
  row: Pick<PresidentialCycleRecordContextRow, "electionYear" | "stage" | "party">
): string {
  if (row.stage === "general") {
    return `${presidentialOfficeNameForRole(role)}, ${row.electionYear} general election`;
  }

  const party = row.party?.trim();
  return party
    ? `${presidentialOfficeNameForRole(role)}, ${row.electionYear} ${party} primary`
    : `${presidentialOfficeNameForRole(role)}, ${row.electionYear} primary`;
}

function toPresidentialRecordContext(
  row: PresidentialCycleRecordContextRow,
  role: CandidateRecordPresidentialRole
): CandidateElectionOfficeContext {
  return {
    candidateId: row.candidateId,
    candidateDisplayName: row.candidateDisplayName,
    currentOffice: row.currentOffice,
    hasHeldPublicOffice: row.hasHeldPublicOffice,
    electionId: "",
    presidentialCycleId: row.presidentialCycleId,
    presidentialRole: role,
    districtName: "United States",
    districtType: "presidential",
    state: "US",
    electionDate: getPresidentialGeneralElectionDate(row.electionYear),
    officialBallotTitle: presidentialOfficialBallotTitleForRole(role, row),
    electionStage: row.stage,
    senateClass: null,
    termEndYear: null,
    officeId: row.officeId,
    discoveryContestFamily: null,
    electionSources: row.sources,
  };
}

// Accepts the candidate either as the direct candidate_elections link or as a
// joint-ticket running mate (candidate_elections.running_mate_candidate_id),
// mirroring the vice_president path in the presidential loader below. The
// returned candidate fields are always the requested candidate's own row.
export async function loadCandidateElectionOfficeContext(
  client: Queryable,
  candidateId: string,
  electionId: string
): Promise<CandidateElectionOfficeContext | null> {
  const result = await client.query<CandidateElectionOfficeContext>(
    `
      SELECT
        c.id AS "candidateId",
        COALESCE(NULLIF(trim(c.display_name), ''), trim(c.first_name || ' ' || c.last_name)) AS "candidateDisplayName",
        c.current_office AS "currentOffice",
        c.has_held_public_office AS "hasHeldPublicOffice",
        e.id AS "electionId",
        d.name AS "districtName",
        d.district_type AS "districtType",
        d.state AS "state",
        e.election_date::text AS "electionDate",
        e.official_ballot_title AS "officialBallotTitle",
        e.election_stage::text AS "electionStage",
        sm.senate_class AS "senateClass",
        sm.term_end_year AS "termEndYear",
        e.office_id::text AS "officeId",
        e.discovery_contest_family AS "discoveryContestFamily",
        e.sources AS "electionSources"
      FROM public.candidate_elections ce
      JOIN public.candidates c
        ON c.id = $1
      JOIN public.elections e
        ON e.id = ce.election_id
      JOIN public.districts d
        ON d.id = e.district_id
      LEFT JOIN public.election_senate_metadata sm
        ON sm.election_id = e.id
      WHERE (ce.candidate_id = c.id OR ce.running_mate_candidate_id = c.id)
        AND ce.election_id = $2
        AND c.deleted_at IS NULL
      LIMIT 1
    `,
    [candidateId, electionId]
  );

  return result.rows[0] ?? null;
}

export async function loadCandidatePresidentialCycleOfficeContext(
  client: Queryable,
  candidateId: string,
  presidentialCycleId: string,
  role: CandidateRecordPresidentialRole
): Promise<CandidateElectionOfficeContext | null> {
  const trimmedCandidateId = candidateId.trim();
  const trimmedCycleId = presidentialCycleId.trim();
  if (trimmedCandidateId.length === 0 || trimmedCycleId.length === 0) {
    return null;
  }

  const officeName = presidentialOfficeNameForRole(role);
  const result = await client.query<PresidentialCycleRecordContextRow>(
    `
      SELECT
        c.id AS "candidateId",
        COALESCE(NULLIF(trim(c.display_name), ''), trim(c.first_name || ' ' || c.last_name)) AS "candidateDisplayName",
        c.current_office AS "currentOffice",
        c.has_held_public_office AS "hasHeldPublicOffice",
        cycle.id AS "presidentialCycleId",
        cycle.election_year AS "electionYear",
        cycle.stage::text AS "stage",
        cycle.party AS "party",
        office.id::text AS "officeId",
        cycle.sources AS "sources"
      FROM public.candidates AS c
      JOIN public.presidential_cycle_candidates AS cycle_candidate
        ON cycle_candidate.cycle_id = $2
       AND (
         ($4 = 'president' AND cycle_candidate.candidate_id = c.id)
         OR
         ($4 = 'vice_president' AND cycle_candidate.running_mate_candidate_id = c.id)
       )
      JOIN public.presidential_cycles AS cycle
        ON cycle.id = cycle_candidate.cycle_id
      JOIN public.offices AS office
        ON office.scope = 'presidential'
       AND office.canonical_name = $3
      WHERE c.id = $1
        AND c.deleted_at IS NULL
      LIMIT 1
    `,
    [trimmedCandidateId, trimmedCycleId, officeName, role]
  );

  const row = result.rows[0];
  return row ? toPresidentialRecordContext(row, role) : null;
}
