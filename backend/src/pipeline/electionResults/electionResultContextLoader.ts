import type { Pool, PoolClient } from "pg";

import type { ElectionContestFamily, ElectionDistrictType, ElectionRaceType, ElectionStage } from "../../types/election.js";
import type { CandidateElectionStatus } from "../../types/electionResults.js";

type Queryable = Pick<Pool | PoolClient, "query">;

export type ElectionResultCandidateContext = {
  candidateElectionId: string;
  candidateId: string;
  displayName: string;
  party: string;
  isIncumbent: boolean;
  status: CandidateElectionStatus;
  fecIds: string[];
  stateFilingIds: string[];
};

export type ElectionResultBallotMeasureContext = {
  ballotMeasureId: string;
  officialBallotTitle: string;
  summary: string | null;
  whatYesMeans: string | null;
  whatNoMeans: string | null;
  result: "passed" | "failed" | null;
  sourceUrls: string[];
  officialMeasureUrl: string | null;
};

export type ElectionResultContext = {
  electionId: string;
  raceType: ElectionRaceType;
  officialBallotTitle: string;
  electionDate: string;
  electionStage: ElectionStage | null;
  isPartisan: boolean | null;
  discoveryContestFamily: ElectionContestFamily | null;
  sourceUrls: string[];
  district: {
    id: string;
    name: string;
    districtType: ElectionDistrictType;
    state: string;
  };
  candidates: ElectionResultCandidateContext[];
  ballotMeasure: ElectionResultBallotMeasureContext | null;
};

type ElectionRow = {
  election_id: string;
  district_id: string;
  district_name: string;
  district_type: ElectionDistrictType;
  state: string;
  race_type: ElectionRaceType;
  official_ballot_title: string;
  election_date: string;
  election_stage: ElectionStage | null;
  is_partisan: boolean | null;
  discovery_contest_family: ElectionContestFamily | null;
  sources: unknown;
};

type CandidateRow = {
  election_id: string;
  candidate_election_id: string;
  candidate_id: string;
  display_name: string;
  party: string;
  is_incumbent: boolean;
  status: CandidateElectionStatus;
  fec_ids: unknown;
  state_filing_ids: unknown;
};

type BallotMeasureRow = {
  election_id: string;
  ballot_measure_id: string;
  official_ballot_title: string;
  summary: string | null;
  what_yes_means: string | null;
  what_no_means: string | null;
  result: "passed" | "failed" | null;
  source_url: unknown;
  official_measure_url: string | null;
};

function parseStringArray(raw: unknown): string[] {
  if (!Array.isArray(raw)) {
    return [];
  }
  return [
    ...new Set(
      raw
        .filter((item): item is string => typeof item === "string")
        .map((item) => item.trim())
        .filter((item) => item.length > 0)
    ),
  ];
}

function normalizeInputIds(electionIds: readonly string[]): string[] {
  return [...new Set(electionIds.map((id) => id.trim()).filter((id) => id.length > 0))];
}

export function chunkElectionResultContexts(
  contexts: readonly ElectionResultContext[],
  chunkSize = 10
): ElectionResultContext[][] {
  if (!Number.isInteger(chunkSize) || chunkSize <= 0) {
    throw new Error(`chunkSize must be a positive integer: ${chunkSize}`);
  }
  const chunks: ElectionResultContext[][] = [];
  for (let i = 0; i < contexts.length; i += chunkSize) {
    chunks.push(contexts.slice(i, i + chunkSize));
  }
  return chunks;
}

export async function loadElectionResultContexts(
  db: Queryable,
  electionIds: readonly string[]
): Promise<ElectionResultContext[]> {
  const ids = normalizeInputIds(electionIds);
  if (ids.length === 0) {
    return [];
  }

  const electionResult = await db.query<ElectionRow>(
    `
      SELECT
        e.id AS election_id,
        d.id AS district_id,
        d.name AS district_name,
        d.district_type,
        d.state,
        e.race_type,
        e.official_ballot_title,
        e.election_date::text AS election_date,
        e.election_stage,
        e.is_partisan,
        e.discovery_contest_family,
        e.sources
      FROM public.elections AS e
      JOIN public.districts AS d
        ON d.id = e.district_id
      WHERE e.id = ANY($1::uuid[])
    `,
    [ids]
  );

  const candidateResult = await db.query<CandidateRow>(
    `
      SELECT
        ce.election_id,
        ce.id AS candidate_election_id,
        c.id AS candidate_id,
        COALESCE(NULLIF(trim(c.display_name), ''), trim(c.first_name || ' ' || c.last_name)) AS display_name,
        c.party,
        ce.is_incumbent,
        ce.status,
        c.fec_ids,
        c.state_filing_ids
      FROM public.candidate_elections AS ce
      JOIN public.candidates AS c
        ON c.id = ce.candidate_id
      WHERE ce.election_id = ANY($1::uuid[])
        AND c.deleted_at IS NULL
      ORDER BY ce.election_id, lower(display_name), ce.id
    `,
    [ids]
  );

  const ballotMeasureResult = await db.query<BallotMeasureRow>(
    `
      SELECT
        bm.election_id,
        bm.id AS ballot_measure_id,
        bm.official_ballot_title,
        bm.summary,
        bm.what_yes_means,
        bm.what_no_means,
        bm.result,
        bm.source_url,
        bm.official_measure_url
      FROM public.ballot_measures AS bm
      WHERE bm.election_id = ANY($1::uuid[])
    `,
    [ids]
  );

  const candidatesByElection = new Map<string, ElectionResultCandidateContext[]>();
  for (const row of candidateResult.rows) {
    const list = candidatesByElection.get(row.election_id) ?? [];
    list.push({
      candidateElectionId: row.candidate_election_id,
      candidateId: row.candidate_id,
      displayName: row.display_name,
      party: row.party,
      isIncumbent: row.is_incumbent,
      status: row.status,
      fecIds: parseStringArray(row.fec_ids),
      stateFilingIds: parseStringArray(row.state_filing_ids),
    });
    candidatesByElection.set(row.election_id, list);
  }

  const ballotMeasureByElection = new Map<string, ElectionResultBallotMeasureContext>();
  for (const row of ballotMeasureResult.rows) {
    ballotMeasureByElection.set(row.election_id, {
      ballotMeasureId: row.ballot_measure_id,
      officialBallotTitle: row.official_ballot_title,
      summary: row.summary,
      whatYesMeans: row.what_yes_means,
      whatNoMeans: row.what_no_means,
      result: row.result,
      sourceUrls: parseStringArray(row.source_url),
      officialMeasureUrl: row.official_measure_url,
    });
  }

  const electionById = new Map(electionResult.rows.map((row) => [row.election_id, row]));
  const contexts: ElectionResultContext[] = [];
  for (const id of ids) {
    const row = electionById.get(id);
    if (!row) {
      continue;
    }
    contexts.push({
      electionId: row.election_id,
      raceType: row.race_type,
      officialBallotTitle: row.official_ballot_title,
      electionDate: row.election_date,
      electionStage: row.election_stage,
      isPartisan: row.is_partisan,
      discoveryContestFamily: row.discovery_contest_family,
      sourceUrls: parseStringArray(row.sources),
      district: {
        id: row.district_id,
        name: row.district_name,
        districtType: row.district_type,
        state: row.state,
      },
      candidates: candidatesByElection.get(row.election_id) ?? [],
      ballotMeasure: ballotMeasureByElection.get(row.election_id) ?? null,
    });
  }

  return contexts;
}
