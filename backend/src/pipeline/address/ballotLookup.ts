import type { Pool, PoolClient } from "pg";

import type {
  ElectionContestFamily,
  ElectionDistrictType,
  ElectionRaceType,
  ElectionStage,
  OfficeScope,
} from "../../types/election.js";
import type { CandidateElectionStatus, ElectionResultPassType } from "../../types/electionResults.js";
import {
  calculateWeightedHistoricalContestMargin,
  lookupHistoricalContestMarginRows,
  type HistoricalContestWeightedMarginLookupRecord,
} from "../competitiveness/historicalContestMarginLookup.js";
import type { HistoricalContestCompetitivenessLabel } from "../competitiveness/competitivenessLabels.js";
import { calculateVotePower, type VotePowerResult } from "./votePower.js";
import {
  GENERAL_RESEARCH_AREA_SLUG,
  INTEGRITY_AND_ETHICS_RESEARCH_AREA_SLUG,
} from "../candidates/candidateRecordResearchAreaPolicy.js";
import { loadKentuckyCandidateFinanceSummariesByCandidateElection } from "../kentuckyFinance/kentuckyBallotLookupFinanceLoader.js";
import { loadAlaskaCandidateFinanceSummariesByCandidateElection } from "../alaskaFinance/alaskaCandidateFinanceBallotLookup.js";
import { loadArizonaCandidateFinanceSummariesByCandidateElection } from "../arizonaFinance/arizonaFinanceBallotLookup.js";
import { loadFloridaCandidateFinanceSummariesByCandidateElection } from "../floridaFinance/floridaFinanceBallotSummary.js";
import { loadLouisianaCandidateFinanceSummariesByCandidateElection } from "../louisianaFinance/louisianaBallotLookupFinanceLoader.js";
import { loadPennsylvaniaCandidateFinanceSummariesByCandidateElection } from "../pennsylvaniaFinance/pennsylvaniaBallotLookupFinance.js";
import { loadUtahCandidateFinanceSummariesByCandidateElection } from "../utahFinance/utahBallotLookupFinanceLoader.js";
import { loadVermontCandidateFinanceSummariesByCandidateElection } from "../vermontFinance/vermontBallotLookupFinanceLoader.js";
import { loadFecCandidateFinanceSummariesByCandidateElection } from "../finance/fecBallotLookupFinanceLoader.js";
import { loadMinnesotaCandidateFinanceSummariesByCandidateElection } from "../minnesotaFinance/minnesotaBallotLookupFinanceLoader.js";
import { loadNewJerseyCandidateFinanceSummariesByCandidateElection } from "../newJerseyFinance/newJerseyBallotLookupFinanceLoader.js";
import { loadTennesseeCandidateFinanceSummariesByCandidateElection } from "../tennesseeFinance/tennesseeBallotLookupFinanceLoader.js";
import { loadIllinoisCandidateFinanceSummariesByCandidateElection } from "../illinoisFinance/illinoisBallotLookupFinanceLoader.js";
import { loadVirginiaCandidateFinanceSummariesByCandidateElection } from "../virginiaFinance/virginiaBallotLookupFinanceLoader.js";
import { loadMaineCandidateFinanceSummariesByCandidateElection } from "../maineFinance/maineBallotLookupFinanceLoader.js";
import { loadMarylandCandidateFinanceSummariesByCandidateElection } from "../marylandFinance/marylandBallotLookupFinanceLoader.js";
import { loadMichiganCandidateFinanceSummariesByCandidateElection } from "../michiganFinance/michiganBallotLookupFinanceLoader.js";
import { loadMassachusettsCandidateFinanceSummariesByCandidateElection } from "../massachusettsFinance/massachusettsBallotLookupFinanceLoader.js";
import { loadWisconsinCandidateFinanceSummariesByCandidateElection } from "../wisconsinFinance/wisconsinBallotLookupFinanceLoader.js";
import { loadDistrictOfColumbiaCandidateFinanceSummariesByCandidateElection } from "../districtOfColumbiaFinance/districtOfColumbiaBallotLookupFinanceLoader.js";
import { loadHawaiiCandidateFinanceSummariesByCandidateElection } from "../hawaiiFinance/hawaiiBallotLookupFinanceLoader.js";
import { loadOregonCandidateFinanceSummariesByCandidateElection } from "../oregonFinance/oregonBallotLookupFinanceLoader.js";
import { loadWashingtonCandidateFinanceSummariesByCandidateElection } from "../washingtonFinance/washingtonBallotLookupFinanceLoader.js";
import { loadTexasCandidateFinanceSummariesByCandidateElection } from "../texasFinance/texasBallotLookupFinanceLoader.js";
import { loadNewMexicoCandidateFinanceSummariesByCandidateElection } from "../newMexicoFinance/newMexicoBallotLookupFinanceLoader.js";
import { loadIndianaCandidateFinanceSummariesByCandidateElection } from "../indianaFinance/indianaBallotLookupFinanceLoader.js";
import { loadOklahomaCandidateFinanceSummariesByCandidateElection } from "../oklahomaFinance/oklahomaBallotLookupFinanceLoader.js";
import { loadNebraskaCandidateFinanceSummariesByCandidateElection } from "../nebraskaFinance/nebraskaBallotLookupFinanceLoader.js";
import { loadConnecticutCandidateFinanceSummariesByCandidateElection } from "../connecticutFinance/connecticutBallotLookupFinanceLoader.js";
import { loadColoradoCandidateFinanceSummariesByCandidateElection } from "../coloradoFinance/coloradoBallotLookupFinanceLoader.js";
import { loadCaliforniaCandidateFinanceSummariesByCandidateElection } from "../californiaFinance/californiaBallotLookupFinanceLoader.js";
import {
} from "../../config/featureFlags.js";

import {
  electionYear,
  candidateElectionKey,
  type BallotLookupFinanceSummary,
} from "./ballotLookupFinanceShared.js";

type Queryable = Pick<Pool | PoolClient, "query">;

export type BallotLookupDistrict = {
  id: string;
  district_type: ElectionDistrictType;
  geoid_compact: string;
  name: string;
  state: string;
  state_fips: string;
  representation_power_score: number | null;
  population: number | null;
};

export type BallotLookupResearchAreaTag = {
  research_area_id: string;
  slug: string;
  name: string;
  stance: "for" | "against" | null;
};

export type BallotLookupResearchAreaSummary = {
  id: string;
  slug: string;
  name: string;
  description: string | null;
};

export type BallotLookupOfficeSummary = {
  id: string;
  scope: OfficeScope;
  canonical_name: string;
  summary: string;
};

export type BallotLookupHistoricalCompetitivenessMethod = "latest" | "weighted_last_3";

export type BallotLookupHistoricalCompetitivenessContest = {
  source: string;
  source_url: string | null;
  election_year: number;
  winner_party: string | null;
  runner_up_party: string | null;
  margin_percent: number;
  competitiveness_label: HistoricalContestCompetitivenessLabel;
  stale_after_redistricting: boolean;
  weight?: number;
};

export type BallotLookupHistoricalCompetitiveness = {
  display_label: string;
  display_description: string;
  source: string;
  source_url: string | null;
  election_year: number;
  winner_party: string | null;
  runner_up_party: string | null;
  margin_percent: number;
  competitiveness_label: HistoricalContestCompetitivenessLabel;
  stale_after_redistricting: boolean;
  method?: BallotLookupHistoricalCompetitivenessMethod;
  weights?: number[];
  election_years?: number[];
  contests_used?: BallotLookupHistoricalCompetitivenessContest[];
};

export type BallotLookupCandidateRecord = {
  id: string;
  description: string;
  source_url: string;
  event_date: string;
  created_at: string;
  research_area_tags: BallotLookupResearchAreaTag[];
};

export type BallotLookupRunningMate = {
  candidate_id: string;
  display_name: string;
  party: string;
};

export type BallotLookupCandidate = {
  candidate_election_id: string;
  candidate_id: string;
  display_name: string;
  party: string;
  is_incumbent: boolean;
  status: CandidateElectionStatus;
  summary: string | null;
  current_office: string | null;
  state: string;
  fec_ids: string[];
  state_filing_ids: string[];
  running_mate?: BallotLookupRunningMate;
  records: BallotLookupCandidateRecord[];
  finance_summary: BallotLookupFinanceSummary | null;
};

export type BallotLookupElectionResultWinner = {
  candidate_election_id?: string;
  candidate_id?: string;
  candidate_name?: string;
  party?: string;
};

export type BallotLookupElectionResult = {
  id: string;
  pass_type: ElectionResultPassType;
  result_status: string;
  outcome: string;
  winners: BallotLookupElectionResultWinner[];
  match_status: string;
  source_url: string;
  source_type: string;
  retrieved_at: string;
};

export type BallotLookupBallotMeasureResult = {
  id: string;
  pass_type: ElectionResultPassType;
  result_status: string;
  outcome: string;
  source_url: string;
  source_type: string;
  retrieved_at: string;
};

export type BallotLookupBallotMeasure = {
  id: string;
  official_ballot_title: string;
  summary: string | null;
  what_yes_means: string;
  what_no_means: string;
  result: "passed" | "failed" | null;
  source_urls: string[];
  official_measure_url: string | null;
  research_area_tags: BallotLookupResearchAreaTag[];
  results: BallotLookupBallotMeasureResult[];
};

export type BallotLookupElection = {
  id: string;
  district_id: string;
  district: BallotLookupDistrict;
  race_type: ElectionRaceType;
  official_ballot_title: string;
  election_date: string;
  election_stage: ElectionStage | null;
  is_partisan: boolean | null;
  discovery_contest_family: ElectionContestFamily | null;
  sources: string[];
  candidates: BallotLookupCandidate[];
  ballot_measure: BallotLookupBallotMeasure | null;
  results: BallotLookupElectionResult[];
  historical_competitiveness: BallotLookupHistoricalCompetitiveness | null;
  vote_power: VotePowerResult;
};

type BallotLookupElectionBase = Omit<BallotLookupElection, "historical_competitiveness" | "vote_power">;

export type BallotLookupElectionSummary = {
  id: string;
  district_id: string;
  district: BallotLookupDistrict;
  race_type: ElectionRaceType;
  official_ballot_title: string;
  election_date: string;
  election_stage: ElectionStage | null;
  is_partisan: boolean | null;
  discovery_contest_family: ElectionContestFamily | null;
  sources: string[];
  candidate_count: number;
  ballot_measure_id: string | null;
  has_results: boolean;
  current_result_outcome: string | null;
  office: BallotLookupOfficeSummary | null;
  research_areas: BallotLookupResearchAreaSummary[];
  historical_competitiveness: BallotLookupHistoricalCompetitiveness | null;
  vote_power: VotePowerResult;
};

export type BallotSummaryResult = {
  district_ids: string[];
  districts: BallotLookupDistrict[];
  elections: BallotLookupElectionSummary[];
};

type DistrictRow = Omit<BallotLookupDistrict, "representation_power_score" | "population"> & {
  representation_power_score: string | number | null;
  population?: string | number | null;
};

type ElectionRow = {
  election_id: string;
  district_id: string;
  district_type: ElectionDistrictType;
  geoid_compact: string;
  district_name: string;
  state: string;
  state_fips: string;
  representation_power_score: string | number | null;
  population?: string | number | null;
  race_type: ElectionRaceType;
  official_ballot_title: string;
  election_date: string;
  election_stage: ElectionStage | null;
  is_partisan: boolean | null;
  discovery_contest_family: ElectionContestFamily | null;
  sources: unknown;
  office_id?: string | null;
  office_scope?: OfficeScope | null;
  office_canonical_name?: string | null;
};

type ElectionDetailRow = ElectionRow;

type ElectionSummaryRow = ElectionRow & {
  office_id: string | null;
  office_scope: OfficeScope | null;
  office_canonical_name: string | null;
  office_summary: string | null;
};

type CandidateCountRow = {
  election_id: string;
  candidate_count: number;
};

type BallotMeasureSummaryRow = {
  election_id: string;
  ballot_measure_id: string;
};

type OfficeResearchAreaSummaryRow = {
  office_id: string;
  research_area_id: string;
  slug: string;
  name: string;
  description: string | null;
};

type MeasureResearchAreaSummaryRow = {
  election_id: string;
  research_area_id: string;
  slug: string;
  name: string;
  description: string | null;
};

function mergeResearchAreaSummaries(
  officeRows: readonly OfficeResearchAreaSummaryRow[],
  measureRows: readonly MeasureResearchAreaSummaryRow[]
): BallotLookupResearchAreaSummary[] {
  const merged: BallotLookupResearchAreaSummary[] = [];
  const seen = new Set<string>();
  for (const row of [...officeRows, ...measureRows]) {
    if (seen.has(row.research_area_id)) {
      continue;
    }
    seen.add(row.research_area_id);
    merged.push({
      id: row.research_area_id,
      slug: row.slug,
      name: row.name,
      description: row.description,
    });
  }
  return merged;
}

type ElectionResultSummaryRow = {
  election_id: string;
  outcome: string;
};

type CandidateRow = {
  election_id: string;
  candidate_election_id: string;
  candidate_id: string;
  display_name: string;
  party: string;
  is_incumbent: boolean;
  status: CandidateElectionStatus;
  summary: string | null;
  current_office: string | null;
  state: string;
  fec_ids: unknown;
  state_filing_ids: unknown;
  running_mate_candidate_id: string | null;
  running_mate_display_name: string | null;
  running_mate_party: string | null;
};

type CandidateRecordRow = {
  candidate_id: string;
  candidate_record_id: string;
  description: string;
  source_url: string;
  event_date: string;
  created_at: string;
};

type CandidateRecordTagRow = {
  candidate_record_id: string;
  research_area_id: string;
  slug: string;
  name: string;
  stance: "for" | "against" | null;
};

// office_id is null for the universal areas (general, integrity_and_ethics),
// which are allowed for every office.
type OfficeAllowedResearchAreaRow = {
  office_id: string | null;
  research_area_id: string;
};

type BallotMeasureRow = {
  election_id: string;
  ballot_measure_id: string;
  official_ballot_title: string;
  summary: string | null;
  what_yes_means: string;
  what_no_means: string;
  result: "passed" | "failed" | null;
  source_url: unknown;
  official_measure_url: string | null;
};

type BallotMeasureTagRow = {
  ballot_measure_id: string;
  research_area_id: string;
  slug: string;
  name: string;
  stance: "for" | "against";
};

type ElectionResultRow = {
  election_id: string;
  id: string;
  pass_type: ElectionResultPassType;
  result_status: string;
  outcome: string;
  winners: unknown;
  match_status: string;
  source_url: string;
  source_type: string;
  retrieved_at: string;
};

type BallotMeasureResultRow = {
  ballot_measure_id: string;
  id: string;
  pass_type: ElectionResultPassType;
  result_status: string;
  outcome: string;
  source_url: string;
  source_type: string;
  retrieved_at: string;
};

function normalizeIds(ids: readonly string[]): string[] {
  return [...new Set(ids.map((id) => id.trim()).filter((id) => id.length > 0))];
}

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

function normalizeOptionalString(value: unknown): string | undefined {
  if (typeof value !== "string") {
    return undefined;
  }
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : undefined;
}

function parseWinners(raw: unknown): BallotLookupElectionResultWinner[] {
  if (!Array.isArray(raw)) {
    return [];
  }

  return raw.flatMap((item): BallotLookupElectionResultWinner[] => {
    if (typeof item !== "object" || item === null || Array.isArray(item)) {
      return [];
    }
    const row = item as Record<string, unknown>;
    const winner: BallotLookupElectionResultWinner = {};
    const candidateElectionId = normalizeOptionalString(row.candidate_election_id);
    const candidateId = normalizeOptionalString(row.candidate_id);
    const candidateName = normalizeOptionalString(row.candidate_name);
    const party = normalizeOptionalString(row.party);
    if (candidateElectionId) {
      winner.candidate_election_id = candidateElectionId;
    }
    if (candidateId) {
      winner.candidate_id = candidateId;
    }
    if (candidateName) {
      winner.candidate_name = candidateName;
    }
    if (party) {
      winner.party = party;
    }
    return Object.keys(winner).length > 0 ? [winner] : [];
  });
}

function groupBy<T, K extends string>(rows: readonly T[], getKey: (row: T) => K): Map<K, T[]> {
  const grouped = new Map<K, T[]>();
  for (const row of rows) {
    const key = getKey(row);
    const list = grouped.get(key) ?? [];
    list.push(row);
    grouped.set(key, list);
  }
  return grouped;
}

function parseRepresentationPowerScore(value: string | number | null | undefined): number | null {
  if (value === null || value === undefined) {
    return null;
  }
  const parsed = typeof value === "number" ? value : Number(value);
  if (!Number.isFinite(parsed)) {
    return null;
  }
  return Math.min(100, Math.max(0, parsed));
}

function toDistrict(row: DistrictRow | ElectionRow | ElectionSummaryRow): BallotLookupDistrict {
  const id = "district_id" in row ? row.district_id : row.id;
  const name = "district_name" in row ? row.district_name : row.name;
  return {
    id,
    district_type: row.district_type,
    geoid_compact: row.geoid_compact,
    name,
    state: row.state,
    state_fips: row.state_fips,
    representation_power_score: parseRepresentationPowerScore(row.representation_power_score),
    population: parseDistrictPopulation(row.population),
  };
}

function parseDistrictPopulation(value: string | number | null | undefined): number | null {
  if (value === null || value === undefined) {
    return null;
  }
  const parsed = typeof value === "number" ? value : Number.parseInt(value, 10);
  return Number.isFinite(parsed) ? parsed : null;
}

function mapResearchAreaTag(row: CandidateRecordTagRow | BallotMeasureTagRow): BallotLookupResearchAreaTag {
  return {
    research_area_id: row.research_area_id,
    slug: row.slug,
    name: row.name,
    stance: row.stance,
  };
}

function priorElectionYear(electionDate: string): number | null {
  const year = Number.parseInt(electionDate.slice(0, 4), 10);
  return Number.isInteger(year) ? year - 1 : null;
}

function toHistoricalCompetitiveness(
  row: HistoricalContestWeightedMarginLookupRecord | null | undefined
): BallotLookupHistoricalCompetitiveness | null {
  const latestContest = row?.contests_used[0];
  if (!row || !latestContest) {
    return null;
  }

  return {
    display_label: historicalCompetitivenessDisplayLabel(row.competitiveness_label),
    display_description: historicalCompetitivenessDisplayDescription(row),
    source: row.source,
    source_url: row.source_url,
    election_year: latestContest.election_year,
    winner_party: latestContest.winner_party,
    runner_up_party: latestContest.runner_up_party,
    margin_percent: row.margin_percent,
    competitiveness_label: row.competitiveness_label,
    stale_after_redistricting: row.stale_after_redistricting,
    method: row.method,
    weights: [...row.weights],
    election_years: row.election_years,
    contests_used: row.contests_used.map((contest) => ({
      source: contest.source,
      source_url: contest.source_url,
      election_year: contest.election_year,
      winner_party: contest.winner_party,
      runner_up_party: contest.runner_up_party,
      margin_percent: contest.margin_percent,
      competitiveness_label: contest.competitiveness_label,
      stale_after_redistricting: contest.stale_after_redistricting,
      weight: contest.weight,
    })),
  };
}

function historicalCompetitivenessDisplayLabel(label: HistoricalContestCompetitivenessLabel): string {
  switch (label) {
    case "toss_up":
      return "Historically a toss-up";
    case "very_competitive":
    case "competitive":
      return "Historically competitive";
    case "somewhat_competitive":
      return "Historically somewhat competitive";
    case "safe":
      return "Historically safe";
  }
}

function historicalOfficeDisplayName(officeType: HistoricalContestWeightedMarginLookupRecord["office_type"]): string {
  switch (officeType) {
    case "US_PRESIDENT":
      return "President";
    case "US_SENATE":
      return "U.S. Senate";
    case "US_HOUSE":
      return "U.S. House";
    case "GOVERNOR":
      return "Governor";
    case "LIEUTENANT_GOVERNOR":
      return "Lieutenant Governor";
    case "SECRETARY_OF_STATE":
      return "Secretary of State";
    case "ATTORNEY_GENERAL":
      return "Attorney General";
    case "STATE_TREASURER":
      return "State Treasurer";
    case "STATE_AUDITOR":
      return "State Auditor";
    case "COMPTROLLER":
      return "Comptroller";
    case "SUPERINTENDENT_OF_PUBLIC_INSTRUCTION":
      return "Superintendent of Public Instruction";
    case "COMMISSIONER_OF_AGRICULTURE":
      return "Commissioner of Agriculture";
    case "COMMISSIONER_OF_INSURANCE":
      return "Commissioner of Insurance";
    case "LABOR_COMMISSIONER":
      return "Labor Commissioner";
    case "LAND_COMMISSIONER":
      return "Land Commissioner";
    case "STATE_SENATE":
      return "State Senate";
    case "STATE_HOUSE":
      return "State House";
    case "COUNTY_SHERIFF":
      return "Sheriff";
    case "DISTRICT_ATTORNEY":
      return "District Attorney";
    case "COUNTY_CLERK":
      return "County Clerk";
    case "COUNTY_ASSESSOR":
      return "County Assessor";
    case "COUNTY_AUDITOR":
      return "County Auditor";
    case "COUNTY_TREASURER":
      return "County Treasurer";
    case "COUNTY_RECORDER":
      return "County Recorder";
    case "COUNTY_CORONER":
      return "County Coroner";
  }
}

function formatYearList(years: readonly number[]): string {
  if (years.length === 1) {
    return String(years[0]);
  }
  if (years.length === 2) {
    return `${years[0]} and ${years[1]}`;
  }
  return `${years.slice(0, -1).join(", ")}, and ${years[years.length - 1]}`;
}

function historicalCompetitivenessDisplayDescription(row: HistoricalContestWeightedMarginLookupRecord): string {
  const office = historicalOfficeDisplayName(row.office_type);
  if (row.election_years.length === 1) {
    return `Based on the ${row.election_years[0]} ${office} result.`;
  }
  return `Based on weighted margins from ${formatYearList(row.election_years)} ${office} results.`;
}

async function loadHistoricalCompetitivenessByElection(
  db: Queryable,
  electionRows: readonly (ElectionSummaryRow | ElectionDetailRow)[]
): Promise<Map<string, BallotLookupHistoricalCompetitiveness>> {
  const historicalRowsByElection = await lookupHistoricalContestMarginRows(
    db,
    electionRows.map((row) => ({
      lookupId: row.election_id,
      officeCanonicalName: row.office_canonical_name,
      districtType: row.district_type,
      geoidCompact: row.geoid_compact,
      stateFips: row.state_fips,
      currentElectionYear: electionYear(row.election_date),
      maxElectionYear: priorElectionYear(row.election_date),
    }))
  );

  return new Map(
    [...historicalRowsByElection].flatMap(([electionId, rows]) => {
      const weightedMargin = calculateWeightedHistoricalContestMargin(rows);
      const historicalCompetitiveness = toHistoricalCompetitiveness(weightedMargin);
      return historicalCompetitiveness ? [[electionId, historicalCompetitiveness] as const] : [];
    })
  );
}

// Every ballot-lookup finance source, in the order the old hand-written
// aggregator awaited them. Each loader gates itself on its own feature flag
// and returns an empty map when disabled, so the registry carries no
// enabled-checks. Order is inherited, not semantic: since the federal-office
// gate (#214), no two sources can produce a summary for the same
// candidate/election key. Adding a state = write the family-folder loader
// and add one entry here.
// Everything the finance sources read off a candidate row: the shared
// request builder uses candidate_id/election_id and the FEC loader reads
// fec_ids. Declaring the narrow shape here lets the profile finance lookup
// query only these columns while the full-row callers pass CandidateRow
// unchanged (structural subtyping).
type FinanceLookupCandidateRow = Pick<CandidateRow, "candidate_id" | "election_id" | "fec_ids">;

type StateFinanceLookupAdapter = {
  state: string;
  load: (
    db: Queryable,
    candidateRows: readonly FinanceLookupCandidateRow[],
    electionRows: readonly ElectionRow[]
  ) => Promise<Map<string, BallotLookupFinanceSummary>>;
};

const STATE_FINANCE_LOOKUP_ADAPTERS: readonly StateFinanceLookupAdapter[] = [
  { state: "WI", load: loadWisconsinCandidateFinanceSummariesByCandidateElection },
  { state: "MA", load: loadMassachusettsCandidateFinanceSummariesByCandidateElection },
  { state: "VT", load: loadVermontCandidateFinanceSummariesByCandidateElection },
  { state: "LA", load: loadLouisianaCandidateFinanceSummariesByCandidateElection },
  { state: "MD", load: loadMarylandCandidateFinanceSummariesByCandidateElection },
  { state: "ME", load: loadMaineCandidateFinanceSummariesByCandidateElection },
  { state: "AK", load: loadAlaskaCandidateFinanceSummariesByCandidateElection },
  { state: "MI", load: loadMichiganCandidateFinanceSummariesByCandidateElection },
  { state: "IL", load: loadIllinoisCandidateFinanceSummariesByCandidateElection },
  { state: "MN", load: loadMinnesotaCandidateFinanceSummariesByCandidateElection },
  { state: "OR", load: loadOregonCandidateFinanceSummariesByCandidateElection },
  {
    state: "PA",
    load: (db, candidateRows, electionRows) =>
      loadPennsylvaniaCandidateFinanceSummariesByCandidateElection({ db, candidateRows, electionRows }),
  },
  { state: "WA", load: loadWashingtonCandidateFinanceSummariesByCandidateElection },
  { state: "HI", load: loadHawaiiCandidateFinanceSummariesByCandidateElection },
  { state: "DC", load: loadDistrictOfColumbiaCandidateFinanceSummariesByCandidateElection },
  { state: "KY", load: loadKentuckyCandidateFinanceSummariesByCandidateElection },
  { state: "FL", load: loadFloridaCandidateFinanceSummariesByCandidateElection },
  { state: "VA", load: loadVirginiaCandidateFinanceSummariesByCandidateElection },
  { state: "TN", load: loadTennesseeCandidateFinanceSummariesByCandidateElection },
  { state: "TX", load: loadTexasCandidateFinanceSummariesByCandidateElection },
  { state: "AZ", load: loadArizonaCandidateFinanceSummariesByCandidateElection },
  {
    state: "UT",
    load: (db, candidateRows, electionRows) =>
      loadUtahCandidateFinanceSummariesByCandidateElection({ db, candidateRows, electionRows }),
  },
  { state: "IN", load: loadIndianaCandidateFinanceSummariesByCandidateElection },
  { state: "NE", load: loadNebraskaCandidateFinanceSummariesByCandidateElection },
  { state: "OK", load: loadOklahomaCandidateFinanceSummariesByCandidateElection },
  { state: "NJ", load: loadNewJerseyCandidateFinanceSummariesByCandidateElection },
  { state: "NM", load: loadNewMexicoCandidateFinanceSummariesByCandidateElection },
  { state: "CT", load: loadConnecticutCandidateFinanceSummariesByCandidateElection },
  { state: "CO", load: loadColoradoCandidateFinanceSummariesByCandidateElection },
  { state: "CA", load: loadCaliforniaCandidateFinanceSummariesByCandidateElection },
];

async function loadCandidateFinanceSummariesByCandidateElection(
  db: Queryable,
  candidateRows: readonly FinanceLookupCandidateRow[],
  electionRows: readonly ElectionRow[]
): Promise<Map<string, BallotLookupFinanceSummary>> {
  const merged = new Map<string, BallotLookupFinanceSummary>();
  // Sequential on purpose: parallelizing changes query interleaving for no
  // practical win (only one state is non-empty per election) and would break
  // the ordered query mocks across the test suite.
  for (const adapter of STATE_FINANCE_LOOKUP_ADAPTERS) {
    for (const [key, summary] of await adapter.load(db, candidateRows, electionRows)) {
      merged.set(key, summary);
    }
  }
  // Federal FEC summaries intentionally win when both sources exist for the
  // same candidate/election (unreachable since the federal-office gate, and
  // pinned by the no-leak regression tests).
  for (const [key, summary] of await loadFecCandidateFinanceSummariesByCandidateElection(db, candidateRows, electionRows)) {
    merged.set(key, summary);
  }
  return merged;
}

async function loadFullElectionDetails(
  db: Queryable,
  electionRows: readonly ElectionRow[]
): Promise<BallotLookupElectionBase[]> {
  const electionIds = electionRows.map((row) => row.election_id);
  if (electionIds.length === 0) {
    return [];
  }

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
          c.summary,
          c.current_office,
          c.state,
          c.fec_ids,
          c.state_filing_ids,
          rm.id AS running_mate_candidate_id,
          CASE
            WHEN rm.id IS NULL THEN NULL
            ELSE COALESCE(NULLIF(trim(rm.display_name), ''), trim(rm.first_name || ' ' || rm.last_name))
          END AS running_mate_display_name,
          rm.party AS running_mate_party
        FROM public.candidate_elections AS ce
        JOIN public.candidates AS c
          ON c.id = ce.candidate_id
        LEFT JOIN public.candidates AS rm
          ON rm.id = ce.running_mate_candidate_id
          AND rm.deleted_at IS NULL
        WHERE ce.election_id = ANY($1::uuid[])
          AND c.deleted_at IS NULL
        ORDER BY
          ce.election_id,
          lower(COALESCE(NULLIF(trim(c.display_name), ''), trim(c.first_name || ' ' || c.last_name))),
          ce.id
      `,
    [electionIds]
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
        ORDER BY bm.election_id, bm.id
      `,
    [electionIds]
  );
  const officeResult = await db.query<ElectionResultRow>(
    `
        SELECT
          er.election_id,
          er.id,
          er.pass_type,
          er.result_status,
          er.outcome,
          er.winners,
          er.match_status,
          er.source_url,
          er.source_type,
          er.retrieved_at::text AS retrieved_at
        FROM public.election_results AS er
        WHERE er.election_id = ANY($1::uuid[])
        ORDER BY er.election_id, er.pass_type, er.retrieved_at DESC
      `,
    [electionIds]
  );
  const ballotMeasureOutcomeResult = await db.query<BallotMeasureResultRow>(
    `
        SELECT
          bmr.ballot_measure_id,
          bmr.id,
          bmr.pass_type,
          bmr.result_status,
          bmr.outcome,
          bmr.source_url,
          bmr.source_type,
          bmr.retrieved_at::text AS retrieved_at
        FROM public.ballot_measure_results AS bmr
        JOIN public.ballot_measures AS bm
          ON bm.id = bmr.ballot_measure_id
        WHERE bm.election_id = ANY($1::uuid[])
        ORDER BY bmr.ballot_measure_id, bmr.pass_type, bmr.retrieved_at DESC
      `,
    [electionIds]
  );

  const candidateIds = [...new Set(candidateResult.rows.map((row) => row.candidate_id))];
  const ballotMeasureIds = ballotMeasureResult.rows.map((row) => row.ballot_measure_id);

  const candidateRecordResult =
    candidateIds.length === 0
      ? { rows: [] as CandidateRecordRow[] }
      : await db.query<CandidateRecordRow>(
          `
            SELECT
              cr.candidate_id,
              cr.id AS candidate_record_id,
              cr.description,
              cr.source_url,
              cr.event_date::text AS event_date,
              cr.created_at::text AS created_at
            FROM public.candidate_records AS cr
            WHERE cr.candidate_id = ANY($1::uuid[])
            ORDER BY cr.candidate_id, cr.event_date DESC, cr.created_at DESC, cr.id
          `,
          [candidateIds]
        );
  const candidateRecordTagResult =
    candidateIds.length === 0
      ? { rows: [] as CandidateRecordTagRow[] }
      : await db.query<CandidateRecordTagRow>(
          `
            SELECT
              cr.id AS candidate_record_id,
              ra.id AS research_area_id,
              ra.slug,
              ra.name,
              tag.stance
            FROM public.candidate_records AS cr
            JOIN public.candidate_record_area_tags AS tag
              ON tag.candidate_record_id = cr.id
            JOIN public.research_areas AS ra
              ON ra.id = tag.research_area_id
            WHERE cr.candidate_id = ANY($1::uuid[])
            ORDER BY cr.id, ra.slug
          `,
          [candidateIds]
        );
  const ballotMeasureTagResult =
    ballotMeasureIds.length === 0
      ? { rows: [] as BallotMeasureTagRow[] }
      : await db.query<BallotMeasureTagRow>(
          `
            SELECT
              tag.ballot_measure_id,
              ra.id AS research_area_id,
              ra.slug,
              ra.name,
              tag.stance
            FROM public.ballot_measure_research_area_tags AS tag
            JOIN public.research_areas AS ra
              ON ra.id = tag.research_area_id
            WHERE tag.ballot_measure_id = ANY($1::uuid[])
            ORDER BY tag.ballot_measure_id, ra.slug
          `,
          [ballotMeasureIds]
        );

  // Candidate records and their area tags are candidate-wide: a tag can point
  // at a research area that is not allowed for THIS election's office (e.g. a
  // legacy tag from before the office's allowed set was curated, or a tag that
  // only applies to an office the candidate ran for previously). Tags are
  // never deleted for that reason, so the election view must scope them here:
  // keep a tag only when its area is in the election office's allowed set
  // (office_research_areas) or is one of the universal areas. Elections with
  // no linked office have no allowed set and keep every tag.
  const electionOfficeIds = [
    ...new Set(
      electionRows
        .map((row) => row.office_id)
        .filter((officeId): officeId is string => typeof officeId === "string" && officeId.length > 0)
    ),
  ];
  const shouldScopeTagsToOffice = candidateRecordTagResult.rows.length > 0 && electionOfficeIds.length > 0;
  const officeAllowedAreaResult = !shouldScopeTagsToOffice
    ? { rows: [] as OfficeAllowedResearchAreaRow[] }
    : await db.query<OfficeAllowedResearchAreaRow>(
        `
          SELECT
            ora.office_id,
            ora.research_area_id
          FROM public.office_research_areas AS ora
          WHERE ora.office_id = ANY($1::uuid[])
          UNION ALL
          SELECT
            NULL::uuid AS office_id,
            ra.id AS research_area_id
          FROM public.research_areas AS ra
          WHERE ra.slug = ANY($2::text[])
        `,
        [electionOfficeIds, [GENERAL_RESEARCH_AREA_SLUG, INTEGRITY_AND_ETHICS_RESEARCH_AREA_SLUG]]
      );
  const universalAreaIds = new Set<string>();
  const allowedAreaIdsByOffice = new Map<string, Set<string>>();
  for (const row of officeAllowedAreaResult.rows) {
    if (row.office_id === null) {
      universalAreaIds.add(row.research_area_id);
      continue;
    }
    const set = allowedAreaIdsByOffice.get(row.office_id) ?? new Set<string>();
    set.add(row.research_area_id);
    allowedAreaIdsByOffice.set(row.office_id, set);
  }
  const scopeRecordTagsToOffice = (
    records: BallotLookupCandidateRecord[],
    officeId: string | null
  ): BallotLookupCandidateRecord[] => {
    // Gate on whether the allowed-areas query ran, not on its row count: a
    // query that runs and finds nothing means nothing is allowed for these
    // offices beyond what the rows say, so filtering must still apply
    // (matching loadAllowedResearchAreasForOfficeId, where an empty result
    // rejects every non-listed area) rather than silently passing tags through.
    if (officeId === null || !shouldScopeTagsToOffice) {
      return records;
    }
    const allowed = allowedAreaIdsByOffice.get(officeId);
    return records.map((record) => ({
      ...record,
      research_area_tags: record.research_area_tags.filter(
        (tag) => universalAreaIds.has(tag.research_area_id) || allowed?.has(tag.research_area_id) === true
      ),
    }));
  };

  const financeSummaryByCandidateElection = await loadCandidateFinanceSummariesByCandidateElection(
    db,
    candidateResult.rows,
    electionRows
  );
  const candidateRecordTagsByRecord = groupBy(candidateRecordTagResult.rows, (row) => row.candidate_record_id);
  const candidateRecordsByCandidate = new Map<string, BallotLookupCandidateRecord[]>();
  for (const row of candidateRecordResult.rows) {
    const list = candidateRecordsByCandidate.get(row.candidate_id) ?? [];
    list.push({
      id: row.candidate_record_id,
      description: row.description,
      source_url: row.source_url,
      event_date: row.event_date,
      created_at: row.created_at,
      research_area_tags: (candidateRecordTagsByRecord.get(row.candidate_record_id) ?? []).map(mapResearchAreaTag),
    });
    candidateRecordsByCandidate.set(row.candidate_id, list);
  }

  const officeIdByElection = new Map<string, string | null>(
    electionRows.map((row) => [row.election_id, row.office_id ?? null])
  );
  const candidatesByElection = new Map<string, BallotLookupCandidate[]>();
  for (const row of candidateResult.rows) {
    const list = candidatesByElection.get(row.election_id) ?? [];
    list.push({
      candidate_election_id: row.candidate_election_id,
      candidate_id: row.candidate_id,
      display_name: row.display_name,
      party: row.party,
      is_incumbent: row.is_incumbent,
      status: row.status,
      summary: row.summary,
      current_office: row.current_office,
      state: row.state,
      fec_ids: parseStringArray(row.fec_ids),
      state_filing_ids: parseStringArray(row.state_filing_ids),
      ...(row.running_mate_candidate_id && row.running_mate_display_name
        ? {
            running_mate: {
              candidate_id: row.running_mate_candidate_id,
              display_name: row.running_mate_display_name,
              party: row.running_mate_party ?? "",
            },
          }
        : {}),
      records: scopeRecordTagsToOffice(
        candidateRecordsByCandidate.get(row.candidate_id) ?? [],
        officeIdByElection.get(row.election_id) ?? null
      ),
      finance_summary:
        financeSummaryByCandidateElection.get(candidateElectionKey(row.candidate_id, row.election_id)) ?? null,
    });
    candidatesByElection.set(row.election_id, list);
  }

  const ballotMeasureTagsByMeasure = groupBy(ballotMeasureTagResult.rows, (row) => row.ballot_measure_id);
  const ballotMeasureResultsByMeasure = groupBy(ballotMeasureOutcomeResult.rows, (row) => row.ballot_measure_id);
  const ballotMeasureByElection = new Map<string, BallotLookupBallotMeasure>();
  for (const row of ballotMeasureResult.rows) {
    ballotMeasureByElection.set(row.election_id, {
      id: row.ballot_measure_id,
      official_ballot_title: row.official_ballot_title,
      summary: row.summary,
      what_yes_means: row.what_yes_means,
      what_no_means: row.what_no_means,
      result: row.result,
      source_urls: parseStringArray(row.source_url),
      official_measure_url: row.official_measure_url,
      research_area_tags: (ballotMeasureTagsByMeasure.get(row.ballot_measure_id) ?? []).map(mapResearchAreaTag),
      results: (ballotMeasureResultsByMeasure.get(row.ballot_measure_id) ?? []).map((result) => ({
        id: result.id,
        pass_type: result.pass_type,
        result_status: result.result_status,
        outcome: result.outcome,
        source_url: result.source_url,
        source_type: result.source_type,
        retrieved_at: result.retrieved_at,
      })),
    });
  }

  const officeResultsByElection = groupBy(officeResult.rows, (row) => row.election_id);
  return electionRows.map((row) => ({
    id: row.election_id,
    district_id: row.district_id,
    district: toDistrict(row),
    race_type: row.race_type,
    official_ballot_title: row.official_ballot_title,
    election_date: row.election_date,
    election_stage: row.election_stage,
    is_partisan: row.is_partisan,
    discovery_contest_family: row.discovery_contest_family,
    sources: parseStringArray(row.sources),
    candidates: candidatesByElection.get(row.election_id) ?? [],
    ballot_measure: ballotMeasureByElection.get(row.election_id) ?? null,
    results: (officeResultsByElection.get(row.election_id) ?? []).map((result) => ({
      id: result.id,
      pass_type: result.pass_type,
      result_status: result.result_status,
      outcome: result.outcome,
      winners: parseWinners(result.winners),
      match_status: result.match_status,
      source_url: result.source_url,
      source_type: result.source_type,
      retrieved_at: result.retrieved_at,
    })),
  }));
}

export async function lookupBallotSummariesByDistrictIds(
  db: Queryable,
  districtIds: readonly string[]
): Promise<BallotSummaryResult> {
  const ids = normalizeIds(districtIds);
  if (ids.length === 0) {
    return { district_ids: [], districts: [], elections: [] };
  }

  const districtResult = await db.query<DistrictRow>(
    `
      SELECT
        id,
        district_type,
        geoid_compact,
        name,
        state,
        state_fips,
        representation_power_score,
        population
      FROM public.districts
      WHERE id = ANY($1::uuid[])
      ORDER BY array_position($1::uuid[], id), district_type, name
    `,
    [ids]
  );

  const electionResult = await db.query<ElectionSummaryRow>(
    `
      SELECT
        e.id AS election_id,
        d.id AS district_id,
        d.district_type,
        d.geoid_compact,
        d.name AS district_name,
        d.state,
        d.state_fips,
        d.representation_power_score,
        d.population,
        e.race_type,
        e.official_ballot_title,
        e.election_date::text AS election_date,
        e.election_stage,
        e.is_partisan,
        e.discovery_contest_family,
        e.sources,
        office.id AS office_id,
        office.scope AS office_scope,
        office.canonical_name AS office_canonical_name,
        office.summary AS office_summary
      FROM public.elections AS e
      JOIN public.districts AS d
        ON d.id = e.district_id
      LEFT JOIN public.offices AS office
        ON office.id = e.office_id
      WHERE e.district_id = ANY($1::uuid[])
      ORDER BY e.election_date ASC, e.race_type ASC, e.official_ballot_title ASC, e.id ASC
    `,
    [ids]
  );

  const electionIds = electionResult.rows.map((row) => row.election_id);
  if (electionIds.length === 0) {
    return {
      district_ids: ids,
      districts: districtResult.rows.map(toDistrict),
      elections: [],
    };
  }

  const candidateCountResult = await db.query<CandidateCountRow>(
    `
      SELECT
        ce.election_id,
        COUNT(*)::int AS candidate_count
      FROM public.candidate_elections AS ce
      JOIN public.candidates AS c
        ON c.id = ce.candidate_id
      WHERE ce.election_id = ANY($1::uuid[])
        AND c.deleted_at IS NULL
      GROUP BY ce.election_id
    `,
    [electionIds]
  );

  const ballotMeasureResult = await db.query<BallotMeasureSummaryRow>(
    `
      SELECT
        bm.election_id,
        bm.id AS ballot_measure_id
      FROM public.ballot_measures AS bm
      WHERE bm.election_id = ANY($1::uuid[])
      ORDER BY bm.election_id, bm.id
    `,
    [electionIds]
  );

  const officeIds = [
    ...new Set(
      electionResult.rows
        .map((row) => row.office_id)
        .filter((officeId): officeId is string => typeof officeId === "string" && officeId.length > 0)
    ),
  ];
  const officeResearchAreaResult =
    officeIds.length === 0
      ? { rows: [] as OfficeResearchAreaSummaryRow[] }
      : await db.query<OfficeResearchAreaSummaryRow>(
          `
            SELECT
              link.office_id,
              area.id AS research_area_id,
              area.slug,
              area.name,
              area.description
            FROM public.office_research_areas AS link
            JOIN public.research_areas AS area
              ON area.id = link.research_area_id
            WHERE link.office_id = ANY($1::uuid[])
            ORDER BY link.office_id, area.slug
          `,
          [officeIds]
        );

  // Ballot-measure elections have no office, so without this their
  // research_areas would always be empty and area-based personalization
  // (the my_areas sort, saved-area highlighting) could never match them.
  // Skipped entirely when the ballot has no measures, mirroring the office
  // research-area guard above.
  const measureElectionIds = [...new Set(ballotMeasureResult.rows.map((row) => row.election_id))];
  const measureResearchAreaResult =
    measureElectionIds.length === 0
      ? { rows: [] as MeasureResearchAreaSummaryRow[] }
      : await db.query<MeasureResearchAreaSummaryRow>(
          `
            SELECT
              bm.election_id,
              area.id AS research_area_id,
              area.slug,
              area.name,
              area.description
            FROM public.ballot_measures AS bm
            JOIN public.ballot_measure_research_area_tags AS tag
              ON tag.ballot_measure_id = bm.id
            JOIN public.research_areas AS area
              ON area.id = tag.research_area_id
            WHERE bm.election_id = ANY($1::uuid[])
            ORDER BY bm.election_id, area.slug
          `,
          [measureElectionIds]
        );

  const resultSummaryResult = await db.query<ElectionResultSummaryRow>(
    `
      WITH all_results AS (
        SELECT
          er.election_id,
          er.outcome,
          er.pass_type,
          er.retrieved_at
        FROM public.election_results AS er
        WHERE er.election_id = ANY($1::uuid[])

        UNION ALL

        SELECT
          bm.election_id,
          bmr.outcome,
          bmr.pass_type,
          bmr.retrieved_at
        FROM public.ballot_measure_results AS bmr
        JOIN public.ballot_measures AS bm
          ON bm.id = bmr.ballot_measure_id
        WHERE bm.election_id = ANY($1::uuid[])
      ),
      ranked AS (
        SELECT
          election_id,
          outcome,
          row_number() OVER (
            PARTITION BY election_id
            ORDER BY
              CASE pass_type
                WHEN 'certified' THEN 1
                WHEN 'election_night' THEN 2
                ELSE 3
              END,
              retrieved_at DESC,
              outcome ASC
          ) AS rn
        FROM all_results
      )
      SELECT election_id, outcome
      FROM ranked
      WHERE rn = 1
    `,
    [electionIds]
  );

  const candidateCountsByElection = new Map(
    candidateCountResult.rows.map((row) => [row.election_id, row.candidate_count])
  );
  const ballotMeasureIdsByElection = new Map<string, string>();
  for (const row of ballotMeasureResult.rows) {
    if (!ballotMeasureIdsByElection.has(row.election_id)) {
      ballotMeasureIdsByElection.set(row.election_id, row.ballot_measure_id);
    }
  }
  const researchAreasByOffice = groupBy(officeResearchAreaResult.rows, (row) => row.office_id);
  const measureResearchAreasByElection = groupBy(measureResearchAreaResult.rows, (row) => row.election_id);
  const resultOutcomeByElection = new Map(resultSummaryResult.rows.map((row) => [row.election_id, row.outcome]));
  const historicalCompetitivenessByElection = await loadHistoricalCompetitivenessByElection(db, electionResult.rows);

  const elections: BallotLookupElectionSummary[] = electionResult.rows.map((row) => {
    const currentResultOutcome = resultOutcomeByElection.get(row.election_id) ?? null;
    // Office columns are nullable only because this is a LEFT JOIN; resolved office rows have non-empty fields.
    const office =
      row.office_id && row.office_scope && row.office_canonical_name && row.office_summary
        ? {
            id: row.office_id,
            scope: row.office_scope,
            canonical_name: row.office_canonical_name,
            summary: row.office_summary,
          }
        : null;

    const district = toDistrict(row);
    const candidateCount = candidateCountsByElection.get(row.election_id) ?? 0;
    const historicalCompetitiveness = historicalCompetitivenessByElection.get(row.election_id) ?? null;

    return {
      id: row.election_id,
      district_id: row.district_id,
      district,
      race_type: row.race_type,
      official_ballot_title: row.official_ballot_title,
      election_date: row.election_date,
      election_stage: row.election_stage,
      is_partisan: row.is_partisan,
      discovery_contest_family: row.discovery_contest_family,
      sources: parseStringArray(row.sources),
      candidate_count: candidateCount,
      ballot_measure_id: ballotMeasureIdsByElection.get(row.election_id) ?? null,
      has_results: currentResultOutcome !== null,
      current_result_outcome: currentResultOutcome,
      office,
      // Office links first, then ballot-measure tags not already present
      // (deduped by area id); both sources arrive slug-ordered, so the
      // combined list stays deterministic.
      research_areas: mergeResearchAreaSummaries(
        row.office_id ? (researchAreasByOffice.get(row.office_id) ?? []) : [],
        measureResearchAreasByElection.get(row.election_id) ?? []
      ),
      historical_competitiveness: historicalCompetitiveness,
      vote_power: calculateVotePower({
        raceType: row.race_type,
        candidateCount,
        representationPowerScore: district.representation_power_score,
        competitivenessLabel: historicalCompetitiveness?.competitiveness_label,
      }),
    };
  });

  return {
    district_ids: ids,
    districts: districtResult.rows.map(toDistrict),
    elections,
  };
}

// The single-election ElectionRow projection shared by the detail and
// finance lookups; both issue it as their leading query, so extracting it
// leaves every ordered query mock in the test suite untouched.
async function loadElectionRowById(db: Queryable, electionId: string): Promise<ElectionDetailRow[]> {
  const result = await db.query<ElectionDetailRow>(
    `
      SELECT
        e.id AS election_id,
        d.id AS district_id,
        d.district_type,
        d.geoid_compact,
        d.name AS district_name,
        d.state,
        d.state_fips,
        d.representation_power_score,
        d.population,
        e.race_type,
        e.official_ballot_title,
        e.election_date::text AS election_date,
        e.election_stage,
        e.is_partisan,
        e.discovery_contest_family,
        e.sources,
        office.id AS office_id,
        office.scope AS office_scope,
        office.canonical_name AS office_canonical_name
      FROM public.elections AS e
      JOIN public.districts AS d
        ON d.id = e.district_id
      LEFT JOIN public.offices AS office
        ON office.id = e.office_id
      WHERE e.id = $1::uuid
      LIMIT 1
    `,
    [electionId]
  );
  return result.rows;
}

export async function lookupElectionDetailById(db: Queryable, electionId: string): Promise<BallotLookupElection | null> {
  const trimmedElectionId = electionId.trim();
  if (trimmedElectionId.length === 0) {
    return null;
  }

  const electionRows = await loadElectionRowById(db, trimmedElectionId);

  const details = await loadFullElectionDetails(db, electionRows);
  const detail = details[0];
  if (!detail) {
    return null;
  }

  const historicalCompetitivenessByElection = await loadHistoricalCompetitivenessByElection(db, electionRows);
  const historicalCompetitiveness = historicalCompetitivenessByElection.get(detail.id) ?? null;
  return {
    ...detail,
    historical_competitiveness: historicalCompetitiveness,
    vote_power: calculateVotePower({
      raceType: detail.race_type,
      candidateCount: detail.candidates.length,
      representationPowerScore: detail.district.representation_power_score,
      competitivenessLabel: historicalCompetitiveness?.competitiveness_label,
    }),
  };
}

export type CandidateElectionFinanceResult = {
  finance_summary: BallotLookupFinanceSummary | null;
};

// Narrow read for the candidate-profile page: one candidate's finance
// summary in one election, without loading every opponent's records and
// tags the way lookupElectionDetailById must. null = the election does not
// exist or the candidate is not in it — including deleted or merged
// candidates, matching the profile reader's identity guards, so this
// sub-resource 404s whenever the profile itself does. An existing pairing
// with no finance coverage is { finance_summary: null }.
export async function lookupCandidateElectionFinanceSummaryById(
  db: Queryable,
  electionId: string,
  candidateId: string
): Promise<CandidateElectionFinanceResult | null> {
  const trimmedElectionId = electionId.trim();
  const trimmedCandidateId = candidateId.trim();
  if (trimmedElectionId.length === 0 || trimmedCandidateId.length === 0) {
    return null;
  }

  const electionRows = await loadElectionRowById(db, trimmedElectionId);
  if (electionRows.length === 0) {
    return null;
  }

  const candidateResult = await db.query<FinanceLookupCandidateRow>(
    `
      SELECT
        ce.election_id,
        c.id AS candidate_id,
        c.fec_ids
      FROM public.candidate_elections AS ce
      JOIN public.candidates AS c
        ON c.id = ce.candidate_id
      WHERE ce.election_id = $1::uuid
        AND ce.candidate_id = $2::uuid
        AND c.deleted_at IS NULL
        AND c.merged_into_candidate_id IS NULL
      LIMIT 1
    `,
    [trimmedElectionId, trimmedCandidateId]
  );
  const candidateRow = candidateResult.rows[0];
  if (!candidateRow) {
    return null;
  }

  const financeSummaryByCandidateElection = await loadCandidateFinanceSummariesByCandidateElection(
    db,
    candidateResult.rows,
    electionRows
  );
  // Key on the DB-canonical row ids, not the request strings: the loaders
  // key their maps on lowercase ids from the database, and isUuid accepts
  // uppercase hex that Postgres matches but a string-keyed Map does not.
  return {
    finance_summary:
      financeSummaryByCandidateElection.get(candidateElectionKey(candidateRow.candidate_id, candidateRow.election_id)) ??
      null,
  };
}
