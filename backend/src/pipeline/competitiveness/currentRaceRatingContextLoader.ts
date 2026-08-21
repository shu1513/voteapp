import type { Pool, PoolClient } from "pg";

import type { ElectionDistrictType, ElectionStage } from "../../types/election.js";
import type { CandidateElectionStatus } from "../../types/electionResults.js";
import type { CompetitivenessLabel } from "./competitivenessLabels.js";
import {
  calculateWeightedHistoricalContestMargin,
  lookupHistoricalContestMarginRows,
} from "./historicalContestMarginLookup.js";

type Queryable = Pick<Pool | PoolClient, "query">;

export type CurrentRaceRatingCandidateContext = {
  displayName: string;
  party: string;
  isIncumbent: boolean;
  status: CandidateElectionStatus;
};

// The historic decisiveness input this rating would override — shown to the
// researcher for orientation only; it never feeds the derived label.
export type CurrentRaceRatingHistoricalContext = {
  competitivenessLabel: CompetitivenessLabel;
  marginPercent: number;
  electionYears: number[];
};

export type CurrentRaceRatingResearchContext = {
  electionId: string;
  // The payload contract's context shape ({ electionId, isDcDelegate }) is a
  // structural subset of this type, so these contexts feed it directly.
  isDcDelegate: boolean;
  officialBallotTitle: string;
  electionDate: string;
  electionStage: ElectionStage | null;
  isPartisan: boolean | null;
  officeCanonicalName: string | null;
  district: {
    name: string;
    districtType: ElectionDistrictType;
    state: string;
  };
  candidates: CurrentRaceRatingCandidateContext[];
  historical: CurrentRaceRatingHistoricalContext | null;
};

type ElectionRow = {
  election_id: string;
  district_name: string;
  district_type: ElectionDistrictType;
  state: string;
  geoid_compact: string;
  state_fips: string;
  official_ballot_title: string;
  election_date: string;
  election_stage: ElectionStage | null;
  is_partisan: boolean | null;
  office_canonical_name: string | null;
};

type CandidateRow = {
  election_id: string;
  display_name: string;
  party: string;
  is_incumbent: boolean;
  status: CandidateElectionStatus;
};

function electionYear(electionDate: string): number | null {
  const year = Number.parseInt(electionDate.slice(0, 4), 10);
  return Number.isInteger(year) ? year : null;
}

export function isDcDelegateDistrict(districtType: ElectionDistrictType, state: string): boolean {
  return districtType === "us_house" && state.trim().toUpperCase() === "DC";
}

export async function loadCurrentRaceRatingContexts(
  db: Queryable,
  electionIds: readonly string[]
): Promise<CurrentRaceRatingResearchContext[]> {
  // Lowercase like every other id entry point: Postgres accepts uppercase
  // UUID input (the ::uuid cast) but returns lowercase text, so an uppercase
  // id would silently drop its context at the electionById lookup.
  const ids = [
    ...new Set(electionIds.map((id) => id.trim().toLowerCase()).filter((id) => id.length > 0)),
  ];
  if (ids.length === 0) {
    return [];
  }

  const electionResult = await db.query<ElectionRow>(
    `
      SELECT
        e.id AS election_id,
        d.name AS district_name,
        d.district_type,
        d.state,
        d.geoid_compact,
        d.state_fips,
        e.official_ballot_title,
        e.election_date::text AS election_date,
        e.election_stage,
        e.is_partisan,
        office.canonical_name AS office_canonical_name
      FROM public.elections AS e
      JOIN public.districts AS d
        ON d.id = e.district_id
      LEFT JOIN public.offices AS office
        ON office.id = e.office_id
      WHERE e.id = ANY($1::uuid[])
        AND e.race_type = 'office'
    `,
    [ids]
  );

  const candidateResult = await db.query<CandidateRow>(
    `
      SELECT
        ce.election_id,
        COALESCE(NULLIF(trim(c.display_name), ''), trim(c.first_name || ' ' || c.last_name)) AS display_name,
        c.party,
        ce.is_incumbent,
        ce.status
      FROM public.candidate_elections AS ce
      JOIN public.candidates AS c
        ON c.id = ce.candidate_id
      WHERE ce.election_id = ANY($1::uuid[])
        AND c.deleted_at IS NULL
      -- The COALESCE is repeated because an ORDER BY expression resolves
      -- bare display_name to the input column c.display_name, so fallback
      -- names (blank display_name) would sort as empty strings.
      ORDER BY ce.election_id,
        lower(COALESCE(NULLIF(trim(c.display_name), ''), trim(c.first_name || ' ' || c.last_name))),
        ce.id
    `,
    [ids]
  );

  const candidatesByElection = new Map<string, CurrentRaceRatingCandidateContext[]>();
  for (const row of candidateResult.rows) {
    const list = candidatesByElection.get(row.election_id) ?? [];
    list.push({
      displayName: row.display_name,
      party: row.party,
      isIncumbent: row.is_incumbent,
      status: row.status,
    });
    candidatesByElection.set(row.election_id, list);
  }

  // Same key derivation as ballotLookup's historic-competitiveness loader, so
  // the label the researcher sees is the label the rating would replace.
  const historicalRowsByElection = await lookupHistoricalContestMarginRows(
    db,
    electionResult.rows.map((row) => ({
      lookupId: row.election_id,
      officeCanonicalName: row.office_canonical_name,
      districtType: row.district_type,
      geoidCompact: row.geoid_compact,
      stateFips: row.state_fips,
      currentElectionYear: electionYear(row.election_date),
      maxElectionYear: (() => {
        const year = electionYear(row.election_date);
        return year === null ? null : year - 1;
      })(),
    }))
  );

  const electionById = new Map(electionResult.rows.map((row) => [row.election_id, row]));
  const contexts: CurrentRaceRatingResearchContext[] = [];
  for (const id of ids) {
    const row = electionById.get(id);
    if (!row) {
      continue;
    }
    const weightedMargin = calculateWeightedHistoricalContestMargin(historicalRowsByElection.get(id) ?? []);
    contexts.push({
      electionId: row.election_id,
      isDcDelegate: isDcDelegateDistrict(row.district_type, row.state),
      officialBallotTitle: row.official_ballot_title,
      electionDate: row.election_date,
      electionStage: row.election_stage,
      isPartisan: row.is_partisan,
      officeCanonicalName: row.office_canonical_name,
      district: {
        name: row.district_name,
        districtType: row.district_type,
        state: row.state,
      },
      candidates: candidatesByElection.get(row.election_id) ?? [],
      historical: weightedMargin
        ? {
            competitivenessLabel: weightedMargin.competitiveness_label,
            marginPercent: weightedMargin.margin_percent,
            electionYears: weightedMargin.election_years,
          }
        : null,
    });
  }

  return contexts;
}
