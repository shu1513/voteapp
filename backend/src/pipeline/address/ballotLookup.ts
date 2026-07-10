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
  isAlaskaCampaignFinanceEnabled,
  isCandidateFinanceEnabled,
  isFloridaCampaignFinanceEnabled,
  isVermontCampaignFinanceEnabled,
  isLouisianaCampaignFinanceEnabled,
} from "../../config/featureFlags.js";

import {
  addFinanceBreakdown,
  buildOutsideIndustrySupportExplanation,
  electionYear,
  candidateElectionKey,
  firstNonEmptySourceUrl,
  mapFinanceBreakdown,
  parseFinanceAmount,
  parseFinanceCount,
  type BallotLookupFinanceBreakdown,
  type BallotLookupFinanceOutsideGroup,
  type BallotLookupFinanceOutsideIndustrySupportEvidence,
  type BallotLookupFinanceOutsideIndustrySupportSummary,
  type BallotLookupFinanceSummary,
} from "./ballotLookupFinanceShared.js";

// Re-exported so existing importers (the per-state ballot-lookup finance
// loaders) keep compiling; new code should import from
// ballotLookupFinanceShared.js directly.
export type {
  BallotLookupFinanceBackingSummary,
  BallotLookupFinanceBreakdown,
  BallotLookupFinanceOutsideGroup,
  BallotLookupFinanceOutsideIndustrySupportEvidence,
  BallotLookupFinanceOutsideIndustrySupportSummary,
  BallotLookupFinanceSupportingCommitteeIndustrySummary,
  BallotLookupFinanceSummary,
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

type CandidateFinanceSummaryRequest = {
  candidate_id: string;
  election_id: string;
  fec_candidate_id: string;
  election_year: number;
};
























type CandidateFinanceSummaryRow = {
  candidate_id: string;
  election_id: string;
  fec_candidate_id: string;
  election_year: number;
  total_receipts: string | number | null;
  total_disbursements: string | number | null;
  cash_on_hand: string | number | null;
  debts_owed: string | number | null;
  outside_support_total: string | number | null;
  outside_oppose_total: string | number | null;
  source_url: string | null;
  last_synced_at: string;
};

type CandidateFinanceDirectBreakdownRow = {
  candidate_id: string;
  election_id: string;
  category_type: "occupation" | "employer" | "industry";
  category_name: string;
  amount: string | number;
  contributor_count: number | null;
  source_url: string | null;
};

type CandidateFinanceOutsideGroupRow = {
  candidate_id: string;
  election_id: string;
  committee_id: string;
  committee_name: string;
  support_oppose: "support" | "oppose";
  amount: string | number;
  source_url: string | null;
};

type CandidateFinanceOutsideIndustryRow = {
  candidate_id: string;
  election_id: string;
  support_oppose: "support" | "oppose";
  category_name: string;
  amount: string | number;
  contributor_count: string | number | null;
  source_url: string | null;
};

type CandidateFinanceOutsideIndustryEvidenceRow = {
  candidate_id: string;
  election_id: string;
  industry_name: string;
  organization_name: string;
  organization_type: "employer" | "donor";
  amount: string | number;
  contributor_count: string | number | null;
  committee_id: string;
  committee_name: string;
  source_url: string | null;
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

function normalizeFecCandidateIdForFinance(value: string): string | null {
  const normalized = value.trim().toUpperCase();
  return /^[HPS][0-9A-Z]{8}$/.test(normalized) ? normalized : null;
}

const GENERIC_FEC_DATA_SOURCE_URL = "https://www.fec.gov/data/";
const GENERIC_FEC_OUTSIDE_SPENDING_SOURCE_URL = "https://www.fec.gov/data/independent-expenditures/";
const OPTIONAL_FLORIDA_BALLOT_SUMMARY_MODULE_PATH = "../floridaFinance/floridaFinanceBallotSummary.js";

type OptionalFloridaBallotSummaryModule = {
  loadFloridaCandidateFinanceSummariesByCandidateElection: (
    db: Queryable,
    candidateRows: readonly CandidateRow[],
    electionRows: readonly ElectionRow[]
  ) => Promise<Map<string, BallotLookupFinanceSummary>>;
};




// FEC ids are office-typed (S = Senate, H = House, P = President), stored
// additively on the candidate, and candidate_finance_summaries is keyed
// (fec_candidate_id, election_year) with no election_id — so an id must only
// be requested for an election of its own federal office, or a candidate's
// federal money would attach to an unrelated same-year race (and win the
// merge, since FEC merges last). Mirrors the office gates in
// candidateFinanceBatchSync. US House is identified structurally (us_house
// districts hold nothing else). US Senate shares statewide districts with
// governors, so it needs identity metadata — and the two signals are not
// equally trustworthy: office_canonical_name comes from the curated offices
// table via write-time office matching, while discovery_contest_family is a
// breadcrumb of which search found the election, stored with no
// consistency check against the office. So a resolved office is
// authoritative in both directions (a linked Governor blocks Senate finance
// even if the family wrongly says us_senate), and the family only decides
// when no office is linked. Senate elections with neither signal stay
// fail-closed — no finance beats wrong finance. P ids never match:
// presidential contests live in presidential_cycles, never in district
// elections.
function isFecRequestableElection(row: ElectionRow, fecCandidateId: string): boolean {
  if (row.race_type !== "office") {
    return false;
  }
  if (fecCandidateId.startsWith("H")) {
    return row.district_type === "us_house";
  }
  if (fecCandidateId.startsWith("S")) {
    if (row.district_type !== "statewide") {
      return false;
    }
    const canonicalOffice = row.office_canonical_name?.trim();
    if (canonicalOffice) {
      return canonicalOffice === "United States Senator";
    }
    return row.discovery_contest_family === "us_senate";
  }
  return false;
}


function buildFinanceSummaryRequests(
  candidateRows: readonly CandidateRow[],
  electionRows: readonly ElectionRow[]
): CandidateFinanceSummaryRequest[] {
  const electionById = new Map(electionRows.map((row) => [row.election_id, row]));
  const requests = new Map<string, CandidateFinanceSummaryRequest>();

  for (const row of candidateRows) {
    const election = electionById.get(row.election_id);
    if (!election) {
      continue;
    }
    const year = electionYear(election.election_date);
    if (year === null) {
      continue;
    }
    for (const rawFecId of parseStringArray(row.fec_ids)) {
      const fecCandidateId = normalizeFecCandidateIdForFinance(rawFecId);
      if (!fecCandidateId || !isFecRequestableElection(election, fecCandidateId)) {
        continue;
      }
      const key = `${row.candidate_id}\u0000${row.election_id}\u0000${fecCandidateId}\u0000${year}`;
      requests.set(key, {
        candidate_id: row.candidate_id,
        election_id: row.election_id,
        fec_candidate_id: fecCandidateId,
        election_year: year,
      });
    }
  }

  return [...requests.values()];
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

async function loadFecCandidateFinanceSummariesByCandidateElection(
  db: Queryable,
  candidateRows: readonly CandidateRow[],
  electionRows: readonly ElectionRow[]
): Promise<Map<string, BallotLookupFinanceSummary>> {
  if (!isCandidateFinanceEnabled()) {
    return new Map();
  }

  const requests = buildFinanceSummaryRequests(candidateRows, electionRows);
  if (requests.length === 0) {
    return new Map();
  }

  const summaryResult = await db.query<CandidateFinanceSummaryRow>(
    `
      WITH requested AS (
        SELECT
          candidate_id::uuid AS candidate_id,
          election_id::uuid AS election_id,
          fec_candidate_id,
          election_year
        FROM jsonb_to_recordset($1::jsonb) AS x(
          candidate_id text,
          election_id text,
          fec_candidate_id text,
          election_year int
        )
      )
      SELECT DISTINCT ON (requested.candidate_id, requested.election_id)
        requested.candidate_id::text AS candidate_id,
        requested.election_id::text AS election_id,
        summary.fec_candidate_id,
        summary.election_year,
        summary.total_receipts,
        summary.total_disbursements,
        summary.cash_on_hand,
        summary.debts_owed,
        summary.outside_support_total,
        summary.outside_oppose_total,
        summary.source_url,
        summary.last_synced_at::text AS last_synced_at
      FROM requested
      JOIN public.candidate_finance_summaries AS summary
        ON summary.fec_candidate_id = requested.fec_candidate_id
       AND summary.election_year = requested.election_year
      ORDER BY requested.candidate_id, requested.election_id, summary.last_synced_at DESC, summary.id
    `,
    [JSON.stringify(requests)]
  );

  if (summaryResult.rows.length === 0) {
    return new Map();
  }

  const selectedRequests = summaryResult.rows.map((row) => ({
    candidate_id: row.candidate_id,
    election_id: row.election_id,
    fec_candidate_id: row.fec_candidate_id,
    election_year: row.election_year,
  }));

  const directBreakdownResult = await db.query<CandidateFinanceDirectBreakdownRow>(
    `
      WITH selected AS (
        SELECT
          candidate_id::uuid AS candidate_id,
          election_id::uuid AS election_id,
          fec_candidate_id,
          election_year
        FROM jsonb_to_recordset($1::jsonb) AS x(
          candidate_id text,
          election_id text,
          fec_candidate_id text,
          election_year int
        )
      ),
      ranked AS (
        SELECT
          selected.candidate_id::text AS candidate_id,
          selected.election_id::text AS election_id,
          breakdown.category_type,
          breakdown.category_name,
          breakdown.amount,
          breakdown.contributor_count,
          breakdown.source_url,
          row_number() OVER (
            PARTITION BY selected.candidate_id, selected.election_id, breakdown.category_type
            ORDER BY breakdown.amount DESC, breakdown.category_name ASC
          ) AS rn
        FROM selected
        JOIN public.candidate_finance_direct_breakdowns AS breakdown
          ON breakdown.fec_candidate_id = selected.fec_candidate_id
         AND breakdown.election_year = selected.election_year
        WHERE breakdown.category_type IN ('occupation', 'employer', 'industry')
      )
      SELECT candidate_id, election_id, category_type, category_name, amount, contributor_count, source_url
      FROM ranked
      WHERE rn <= 5
      ORDER BY candidate_id, election_id, category_type, amount DESC, category_name ASC
    `,
    [JSON.stringify(selectedRequests)]
  );

  const outsideGroupResult = await db.query<CandidateFinanceOutsideGroupRow>(
    `
      WITH selected AS (
        SELECT
          candidate_id::uuid AS candidate_id,
          election_id::uuid AS election_id,
          fec_candidate_id,
          election_year
        FROM jsonb_to_recordset($1::jsonb) AS x(
          candidate_id text,
          election_id text,
          fec_candidate_id text,
          election_year int
        )
      ),
      ranked AS (
        SELECT
          selected.candidate_id::text AS candidate_id,
          selected.election_id::text AS election_id,
          outside_group.committee_id,
          outside_group.committee_name,
          outside_group.support_oppose,
          outside_group.amount,
          outside_group.source_url,
          row_number() OVER (
            PARTITION BY selected.candidate_id, selected.election_id, outside_group.support_oppose
            ORDER BY outside_group.amount DESC, outside_group.committee_name ASC
          ) AS rn
        FROM selected
        JOIN public.candidate_finance_outside_groups AS outside_group
          ON outside_group.fec_candidate_id = selected.fec_candidate_id
         AND outside_group.election_year = selected.election_year
      )
      SELECT candidate_id, election_id, committee_id, committee_name, support_oppose, amount, source_url
      FROM ranked
      WHERE rn <= 5
      ORDER BY candidate_id, election_id, support_oppose, amount DESC, committee_name ASC
    `,
    [JSON.stringify(selectedRequests)]
  );

  const outsideIndustryResult = await db.query<CandidateFinanceOutsideIndustryRow>(
    `
      WITH selected AS (
        SELECT
          candidate_id::uuid AS candidate_id,
          election_id::uuid AS election_id,
          fec_candidate_id,
          election_year
        FROM jsonb_to_recordset($1::jsonb) AS x(
          candidate_id text,
          election_id text,
          fec_candidate_id text,
          election_year int
        )
      ),
      grouped AS (
        SELECT
          selected.candidate_id::text AS candidate_id,
          selected.election_id::text AS election_id,
          breakdown.support_oppose,
          breakdown.category_name,
          sum(breakdown.amount) AS amount,
          CASE
            WHEN count(breakdown.contributor_count) = 0 THEN NULL
            ELSE sum(breakdown.contributor_count)
          END AS contributor_count,
          min(breakdown.source_url) FILTER (WHERE breakdown.source_url IS NOT NULL) AS source_url
        FROM selected
        JOIN public.candidate_finance_outside_group_breakdowns AS breakdown
          ON breakdown.fec_candidate_id = selected.fec_candidate_id
         AND breakdown.election_year = selected.election_year
        WHERE breakdown.category_type = 'industry'
        GROUP BY selected.candidate_id, selected.election_id, breakdown.support_oppose, breakdown.category_name
      ),
      ranked AS (
        SELECT
          *,
          row_number() OVER (
            PARTITION BY candidate_id, election_id, support_oppose
            ORDER BY amount DESC, category_name ASC
          ) AS rn
        FROM grouped
      )
      SELECT candidate_id, election_id, support_oppose, category_name, amount, contributor_count, source_url
      FROM ranked
      WHERE rn <= 5
      ORDER BY candidate_id, election_id, support_oppose, amount DESC, category_name ASC
    `,
    [JSON.stringify(selectedRequests)]
  );

  const outsideIndustryEvidenceResult = await db.query<CandidateFinanceOutsideIndustryEvidenceRow>(
    `
      WITH selected AS (
        SELECT
          candidate_id::uuid AS candidate_id,
          election_id::uuid AS election_id,
          fec_candidate_id,
          election_year
        FROM jsonb_to_recordset($1::jsonb) AS x(
          candidate_id text,
          election_id text,
          fec_candidate_id text,
          election_year int
        )
      ),
      top_industries AS (
        SELECT *
        FROM (
          SELECT
            selected.candidate_id::text AS candidate_id,
            selected.election_id::text AS election_id,
            industry.category_name AS industry_name,
            row_number() OVER (
              PARTITION BY selected.candidate_id, selected.election_id
              ORDER BY sum(industry.amount) DESC, industry.category_name ASC
            ) AS rn
          FROM selected
          JOIN public.candidate_finance_outside_group_breakdowns AS industry
            ON industry.fec_candidate_id = selected.fec_candidate_id
           AND industry.election_year = selected.election_year
          WHERE industry.support_oppose = 'support'
            AND industry.category_type = 'industry'
          GROUP BY selected.candidate_id, selected.election_id, industry.category_name
        ) ranked_industries
        WHERE rn <= 5
      ),
      evidence AS (
        SELECT
          selected.candidate_id::text AS candidate_id,
          selected.election_id::text AS election_id,
          top_industries.industry_name,
          breakdown.category_name AS organization_name,
          breakdown.category_type AS organization_type,
          breakdown.amount,
          breakdown.contributor_count,
          breakdown.committee_id,
          COALESCE(outside_group.committee_name, breakdown.committee_id) AS committee_name,
          COALESCE(breakdown.source_url, outside_group.source_url) AS source_url,
          row_number() OVER (
            PARTITION BY selected.candidate_id, selected.election_id, top_industries.industry_name
            ORDER BY breakdown.amount DESC, breakdown.category_name ASC, breakdown.committee_id ASC
          ) AS rn
        FROM selected
        JOIN top_industries
          ON top_industries.candidate_id = selected.candidate_id::text
         AND top_industries.election_id = selected.election_id::text
        JOIN public.candidate_finance_outside_group_breakdowns AS breakdown
          ON breakdown.fec_candidate_id = selected.fec_candidate_id
         AND breakdown.election_year = selected.election_year
         AND breakdown.support_oppose = 'support'
         AND breakdown.category_type IN ('employer', 'donor')
        CROSS JOIN LATERAL (
          SELECT
            btrim(
              regexp_replace(
                regexp_replace(
                  btrim(
                    regexp_replace(
                      regexp_replace(
                        regexp_replace(upper(replace(breakdown.category_name, '&', ' AND ')), '[^A-Z0-9]+', ' ', 'g'),
                        '\\m(INC|INCORPORATED|LLC|L L C|LP|L P|LLP|L L P|LTD|LIMITED|CO|COMPANY|CORP|CORPORATION|PLC)\\M',
                        ' ',
                        'g'
                      ),
                      '\\s+',
                      ' ',
                      'g'
                    )
                  ),
                  '\\s+',
                  ' ',
                  'g'
                ),
                '^\\s+|\\s+$',
                '',
                'g'
              )
            ) AS normalized_label
        ) AS normalized_breakdown
        JOIN public.finance_label_classifications AS classification
          ON classification.label_type = breakdown.category_type
         AND classification.normalized_label = normalized_breakdown.normalized_label
         AND classification.industry_slug = top_industries.industry_name
        LEFT JOIN public.candidate_finance_outside_groups AS outside_group
          ON outside_group.fec_candidate_id = breakdown.fec_candidate_id
         AND outside_group.election_year = breakdown.election_year
         AND outside_group.committee_id = breakdown.committee_id
         AND outside_group.support_oppose = breakdown.support_oppose
      )
      SELECT
        candidate_id,
        election_id,
        industry_name,
        organization_name,
        organization_type,
        amount,
        contributor_count,
        committee_id,
        committee_name,
        source_url
      FROM evidence
      WHERE rn <= 3
      ORDER BY candidate_id, election_id, industry_name, amount DESC, organization_name ASC
    `,
    [JSON.stringify(selectedRequests)]
  );

  const directOccupationsByCandidateElection = new Map<string, BallotLookupFinanceBreakdown[]>();
  const directEmployersByCandidateElection = new Map<string, BallotLookupFinanceBreakdown[]>();
  const directIndustriesByCandidateElection = new Map<string, BallotLookupFinanceBreakdown[]>();
  const summaryByCandidateElection = new Map(
    summaryResult.rows.map((row) => [candidateElectionKey(row.candidate_id, row.election_id), row])
  );
  for (const row of directBreakdownResult.rows) {
    const summary = summaryByCandidateElection.get(candidateElectionKey(row.candidate_id, row.election_id));
    const mapped = mapFinanceBreakdown(row, summary?.source_url ?? GENERIC_FEC_DATA_SOURCE_URL);
    if (row.category_type === "occupation") {
      addFinanceBreakdown(directOccupationsByCandidateElection, row.candidate_id, row.election_id, mapped);
    } else if (row.category_type === "employer") {
      addFinanceBreakdown(directEmployersByCandidateElection, row.candidate_id, row.election_id, mapped);
    } else {
      addFinanceBreakdown(directIndustriesByCandidateElection, row.candidate_id, row.election_id, mapped);
    }
  }

  const supportingGroupsByCandidateElection = new Map<string, BallotLookupFinanceOutsideGroup[]>();
  const opposingGroupsByCandidateElection = new Map<string, BallotLookupFinanceOutsideGroup[]>();
  for (const row of outsideGroupResult.rows) {
    const key = candidateElectionKey(row.candidate_id, row.election_id);
    const map = row.support_oppose === "support" ? supportingGroupsByCandidateElection : opposingGroupsByCandidateElection;
    const list = map.get(key) ?? [];
    list.push({
      committee_id: row.committee_id,
      committee_name: row.committee_name,
      support_oppose: row.support_oppose,
      amount: parseFinanceAmount(row.amount) ?? 0,
      source_url: firstNonEmptySourceUrl(row.source_url, GENERIC_FEC_OUTSIDE_SPENDING_SOURCE_URL),
    });
    map.set(key, list);
  }

  const supportingIndustriesByCandidateElection = new Map<string, BallotLookupFinanceBreakdown[]>();
  const opposingIndustriesByCandidateElection = new Map<string, BallotLookupFinanceBreakdown[]>();
  for (const row of outsideIndustryResult.rows) {
    const map =
      row.support_oppose === "support" ? supportingIndustriesByCandidateElection : opposingIndustriesByCandidateElection;
    addFinanceBreakdown(
      map,
      row.candidate_id,
      row.election_id,
      mapFinanceBreakdown(row, GENERIC_FEC_OUTSIDE_SPENDING_SOURCE_URL)
    );
  }

  const outsideIndustryEvidenceByCandidateElectionAndIndustry = new Map<
    string,
    BallotLookupFinanceOutsideIndustrySupportEvidence[]
  >();
  for (const row of outsideIndustryEvidenceResult.rows) {
    const key = `${candidateElectionKey(row.candidate_id, row.election_id)}\u0000${row.industry_name}`;
    const list = outsideIndustryEvidenceByCandidateElectionAndIndustry.get(key) ?? [];
    list.push({
      organization_name: row.organization_name,
      organization_type: row.organization_type,
      amount: parseFinanceAmount(row.amount) ?? 0,
      contributor_count: parseFinanceCount(row.contributor_count),
      committee_id: row.committee_id,
      committee_name: row.committee_name,
      source_url: firstNonEmptySourceUrl(row.source_url, GENERIC_FEC_OUTSIDE_SPENDING_SOURCE_URL),
    });
    outsideIndustryEvidenceByCandidateElectionAndIndustry.set(key, list);
  }

  return new Map(
    summaryResult.rows.map((row) => {
      const key = candidateElectionKey(row.candidate_id, row.election_id);
      const topDirectDonorOccupations = directOccupationsByCandidateElection.get(key) ?? [];
      const topOutsideSupportingIndustries = (supportingIndustriesByCandidateElection.get(key) ?? []).map(
        (industry): BallotLookupFinanceOutsideIndustrySupportSummary => {
          const evidenceKey = `${key}\u0000${industry.category_name}`;
          const supportingOrganizations = outsideIndustryEvidenceByCandidateElectionAndIndustry.get(evidenceKey) ?? [];
          return {
            ...industry,
            explanation: buildOutsideIndustrySupportExplanation(industry.category_name, supportingOrganizations),
            supporting_organizations: supportingOrganizations,
          };
        }
      );
      return [
        key,
        {
          source: "FEC",
          cycle: row.election_year,
          fec_candidate_id: row.fec_candidate_id,
          last_synced_at: row.last_synced_at,
          direct_campaign: {
            total_raised: parseFinanceAmount(row.total_receipts),
            total_spent: parseFinanceAmount(row.total_disbursements),
            cash_on_hand: parseFinanceAmount(row.cash_on_hand),
            debts_owed: parseFinanceAmount(row.debts_owed),
            top_occupations: topDirectDonorOccupations,
            top_employers: directEmployersByCandidateElection.get(key) ?? [],
            top_industries: directIndustriesByCandidateElection.get(key) ?? [],
          },
          outside_spending: {
            support_total: parseFinanceAmount(row.outside_support_total),
            oppose_total: parseFinanceAmount(row.outside_oppose_total),
            top_supporting_groups: supportingGroupsByCandidateElection.get(key) ?? [],
            top_opposing_groups: opposingGroupsByCandidateElection.get(key) ?? [],
            top_supporting_industries: supportingIndustriesByCandidateElection.get(key) ?? [],
            top_opposing_industries: opposingIndustriesByCandidateElection.get(key) ?? [],
          },
          backing_summary: {
            top_direct_donor_occupations: topDirectDonorOccupations,
            top_outside_supporting_industries: topOutsideSupportingIndustries,
          },
        } satisfies BallotLookupFinanceSummary,
      ];
    })
  );
}








async function loadUtahCandidateFinanceSummariesByCandidateElection(
  db: Queryable,
  candidateRows: readonly CandidateRow[],
  electionRows: readonly ElectionRow[]
): Promise<Map<string, BallotLookupFinanceSummary>> {
  if (!electionRows.some((row) => row.state.trim().toUpperCase() === "UT")) {
    return new Map();
  }

  const modulePath = "../utahFinance/utahBallotLookupFinanceLoader.js";
  try {
    const module = (await import(modulePath)) as {
      loadUtahCandidateFinanceSummariesByCandidateElection(input: {
        db: Queryable;
        candidateRows: readonly CandidateRow[];
        electionRows: readonly ElectionRow[];
      }): Promise<Map<string, BallotLookupFinanceSummary>>;
    };
    return await module.loadUtahCandidateFinanceSummariesByCandidateElection({ db, candidateRows, electionRows });
  } catch (error) {
    const message = error instanceof Error ? error.message : "";
    if (
      error &&
      typeof error === "object" &&
      "code" in error &&
      ((error as { code?: unknown }).code === "ERR_MODULE_NOT_FOUND" ||
        (error as { code?: unknown }).code === "MODULE_NOT_FOUND") &&
      message.includes("utahBallotLookupFinanceLoader")
    ) {
      return new Map();
    }
    throw error;
  }
}



type OptionalArizonaFinanceBallotLookupModule = {
  loadArizonaCandidateFinanceSummariesByCandidateElection: (
    db: Queryable,
    candidateRows: readonly CandidateRow[],
    electionRows: readonly ElectionRow[]
  ) => Promise<Map<string, BallotLookupFinanceSummary>>;
};

function readOptionalBooleanEnv(name: string, fallback: boolean): boolean {
  const raw = process.env[name];
  if (!raw || raw.trim().length === 0) {
    return fallback;
  }

  const normalized = raw.trim().toLowerCase();
  if (["1", "true", "yes", "y", "on"].includes(normalized)) {
    return true;
  }
  if (["0", "false", "no", "n", "off"].includes(normalized)) {
    return false;
  }

  throw new Error(`Invalid boolean env ${name}: ${raw}`);
}

async function loadArizonaCandidateFinanceSummariesByCandidateElection(
  db: Queryable,
  candidateRows: readonly CandidateRow[],
  electionRows: readonly ElectionRow[]
): Promise<Map<string, BallotLookupFinanceSummary>> {
  if (!readOptionalBooleanEnv("ARIZONA_CAMPAIGN_FINANCE_ENABLED", false)) {
    return new Map();
  }

  const modulePath = "../arizonaFinance/arizonaFinanceBallotLookup.js";
  const module = (await import(modulePath)) as OptionalArizonaFinanceBallotLookupModule;
  return module.loadArizonaCandidateFinanceSummariesByCandidateElection(db, candidateRows, electionRows);
}








type OptionalStateFinanceSummaryLoader = (input: {
  db: Queryable;
  candidateRows: readonly CandidateRow[];
  electionRows: readonly ElectionRow[];
}) => Promise<Map<string, BallotLookupFinanceSummary>>;

const OPTIONAL_PENNSYLVANIA_BALLOT_LOOKUP_FINANCE_MODULE =
  "../pennsylvaniaFinance/pennsylvaniaBallotLookupFinance.js";

function isMissingOptionalModule(error: unknown, moduleName: string): boolean {
  const value = error as { code?: unknown; message?: unknown };
  return value.code === "ERR_MODULE_NOT_FOUND" && typeof value.message === "string" && value.message.includes(moduleName);
}

async function loadOptionalPennsylvaniaCandidateFinanceSummariesByCandidateElection(
  db: Queryable,
  candidateRows: readonly CandidateRow[],
  electionRows: readonly ElectionRow[]
): Promise<Map<string, BallotLookupFinanceSummary>> {
  if (!electionRows.some((row) => row.state.trim().toUpperCase() === "PA")) {
    return new Map();
  }

  try {
    const modulePath = OPTIONAL_PENNSYLVANIA_BALLOT_LOOKUP_FINANCE_MODULE;
    const module = (await import(modulePath)) as {
      loadPennsylvaniaCandidateFinanceSummariesByCandidateElection?: OptionalStateFinanceSummaryLoader;
    };
    const loader = module.loadPennsylvaniaCandidateFinanceSummariesByCandidateElection;
    if (typeof loader !== "function") {
      throw new Error("Pennsylvania ballot finance module is missing its summary loader export");
    }
    return await loader({ db, candidateRows, electionRows });
  } catch (error) {
    if (isMissingOptionalModule(error, "pennsylvaniaBallotLookupFinance")) {
      return new Map();
    }
    throw error;
  }
}










const VERMONT_BALLOT_LOOKUP_FINANCE_LOADER_PATH = "../vermontFinance/vermontBallotLookupFinanceLoader.js";
const LOUISIANA_BALLOT_LOOKUP_FINANCE_LOADER_PATH = "../louisianaFinance/louisianaBallotLookupFinanceLoader.js";

type VermontBallotLookupFinanceLoaderModule = {
  loadVermontCandidateFinanceSummariesByCandidateElection: (
    db: Queryable,
    candidateRows: readonly CandidateRow[],
    electionRows: readonly ElectionRow[]
  ) => Promise<Map<string, BallotLookupFinanceSummary>>;
};

type LouisianaBallotLookupFinanceLoaderModule = {
  loadLouisianaCandidateFinanceSummariesByCandidateElection: (
    db: Queryable,
    candidateRows: readonly CandidateRow[],
    electionRows: readonly ElectionRow[]
  ) => Promise<Map<string, BallotLookupFinanceSummary>>;
};

type AlaskaCandidateFinanceBallotLookupModule = {
  loadAlaskaCandidateFinanceSummariesByCandidateElection: (
    db: Queryable,
    candidateRows: readonly CandidateRow[],
    electionRows: readonly ElectionRow[]
  ) => Promise<Map<string, BallotLookupFinanceSummary>>;
};

const ALASKA_CANDIDATE_FINANCE_BALLOT_LOOKUP_MODULE = "../alaskaFinance/alaskaCandidateFinanceBallotLookup.js";

function missingModuleSpecifier(error: Error): string | null {
  const match =
    error.message.match(/Cannot find module '([^']+)'/) ?? error.message.match(/Cannot find package '([^']+)'/);
  return match?.[1] ?? null;
}

function isMissingOptionalCampaignFinanceModule(error: unknown, modulePath: string): boolean {
  if (!(error instanceof Error)) {
    return false;
  }
  const code = "code" in error ? String(error.code) : "";
  if (code !== "ERR_MODULE_NOT_FOUND" && code !== "MODULE_NOT_FOUND") {
    return false;
  }
  const missingSpecifier = missingModuleSpecifier(error);
  if (!missingSpecifier) {
    return false;
  }
  const expectedFileName = modulePath.split("/").pop() ?? modulePath;
  return (
    missingSpecifier === modulePath ||
    missingSpecifier === expectedFileName ||
    missingSpecifier.endsWith(`/${modulePath}`) ||
    missingSpecifier.endsWith(`/${expectedFileName}`)
  );
}

async function loadVermontCandidateFinanceSummariesByCandidateElection(
  db: Queryable,
  candidateRows: readonly CandidateRow[],
  electionRows: readonly ElectionRow[]
): Promise<Map<string, BallotLookupFinanceSummary>> {
  if (!isVermontCampaignFinanceEnabled()) {
    return new Map();
  }

  try {
    const loader = (await import(VERMONT_BALLOT_LOOKUP_FINANCE_LOADER_PATH)) as VermontBallotLookupFinanceLoaderModule;
    return await loader.loadVermontCandidateFinanceSummariesByCandidateElection(db, candidateRows, electionRows);
  } catch (error) {
    if (isMissingOptionalCampaignFinanceModule(error, VERMONT_BALLOT_LOOKUP_FINANCE_LOADER_PATH)) {
      console.warn("Vermont campaign finance ballot lookup loader is unavailable; skipping Vermont finance summaries");
      return new Map();
    }
    throw error;
  }
}

async function loadLouisianaCandidateFinanceSummariesByCandidateElection(
  db: Queryable,
  candidateRows: readonly CandidateRow[],
  electionRows: readonly ElectionRow[]
): Promise<Map<string, BallotLookupFinanceSummary>> {
  if (!isLouisianaCampaignFinanceEnabled()) {
    return new Map();
  }

  try {
    const loader = (await import(LOUISIANA_BALLOT_LOOKUP_FINANCE_LOADER_PATH)) as LouisianaBallotLookupFinanceLoaderModule;
    return await loader.loadLouisianaCandidateFinanceSummariesByCandidateElection(db, candidateRows, electionRows);
  } catch (error) {
    if (isMissingOptionalCampaignFinanceModule(error, LOUISIANA_BALLOT_LOOKUP_FINANCE_LOADER_PATH)) {
      console.warn("Louisiana campaign finance ballot lookup loader is unavailable; skipping Louisiana finance summaries");
      return new Map();
    }
    throw error;
  }
}

async function loadOptionalAlaskaCandidateFinanceSummariesByCandidateElection(
  db: Queryable,
  candidateRows: readonly CandidateRow[],
  electionRows: readonly ElectionRow[]
): Promise<Map<string, BallotLookupFinanceSummary>> {
  if (!isAlaskaCampaignFinanceEnabled()) {
    return new Map();
  }

  try {
    const module = (await import(ALASKA_CANDIDATE_FINANCE_BALLOT_LOOKUP_MODULE)) as AlaskaCandidateFinanceBallotLookupModule;
    return module.loadAlaskaCandidateFinanceSummariesByCandidateElection(db, candidateRows, electionRows);
  } catch (error) {
    if (isMissingOptionalCampaignFinanceModule(error, ALASKA_CANDIDATE_FINANCE_BALLOT_LOOKUP_MODULE)) {
      console.warn("Alaska campaign finance adapter is unavailable; skipping Alaska finance summaries");
      return new Map();
    }
    throw error;
  }
}

function isMissingOptionalFloridaModuleError(error: unknown): boolean {
  const code = typeof error === "object" && error !== null && "code" in error ? String(error.code) : "";
  const message = error instanceof Error ? error.message : String(error);
  return (
    (code === "ERR_MODULE_NOT_FOUND" || code === "MODULE_NOT_FOUND") &&
    message.includes("floridaFinanceBallotSummary.js")
  );
}

async function loadOptionalFloridaCandidateFinanceSummariesByCandidateElection(
  db: Queryable,
  candidateRows: readonly CandidateRow[],
  electionRows: readonly ElectionRow[]
): Promise<Map<string, BallotLookupFinanceSummary>> {
  if (!isFloridaCampaignFinanceEnabled()) {
    return new Map();
  }

  try {
    const module = (await import(OPTIONAL_FLORIDA_BALLOT_SUMMARY_MODULE_PATH)) as OptionalFloridaBallotSummaryModule;
    return module.loadFloridaCandidateFinanceSummariesByCandidateElection(db, candidateRows, electionRows);
  } catch (error) {
    if (isMissingOptionalFloridaModuleError(error)) {
      console.warn("Florida campaign finance module is enabled but unavailable; skipping Florida finance summaries");
      return new Map();
    }
    throw error;
  }
}



async function loadCandidateFinanceSummariesByCandidateElection(
  db: Queryable,
  candidateRows: readonly CandidateRow[],
  electionRows: readonly ElectionRow[]
): Promise<Map<string, BallotLookupFinanceSummary>> {
  const wisconsinSummaries = await loadWisconsinCandidateFinanceSummariesByCandidateElection(db, candidateRows, electionRows);
  const massachusettsSummaries = await loadMassachusettsCandidateFinanceSummariesByCandidateElection(
    db,
    candidateRows,
    electionRows
  );
  const vermontSummaries = await loadVermontCandidateFinanceSummariesByCandidateElection(db, candidateRows, electionRows);
  const louisianaSummaries = await loadLouisianaCandidateFinanceSummariesByCandidateElection(
    db,
    candidateRows,
    electionRows
  );
  const marylandSummaries = await loadMarylandCandidateFinanceSummariesByCandidateElection(db, candidateRows, electionRows);
  const maineSummaries = await loadMaineCandidateFinanceSummariesByCandidateElection(db, candidateRows, electionRows);
  const alaskaSummaries = await loadOptionalAlaskaCandidateFinanceSummariesByCandidateElection(db, candidateRows, electionRows);
  const michiganSummaries = await loadMichiganCandidateFinanceSummariesByCandidateElection(db, candidateRows, electionRows);
  const illinoisSummaries = await loadIllinoisCandidateFinanceSummariesByCandidateElection(db, candidateRows, electionRows);
  const minnesotaSummaries = await loadMinnesotaCandidateFinanceSummariesByCandidateElection(
    db,
    candidateRows,
    electionRows
  );
  const oregonSummaries = await loadOregonCandidateFinanceSummariesByCandidateElection(db, candidateRows, electionRows);
  const pennsylvaniaSummaries = await loadOptionalPennsylvaniaCandidateFinanceSummariesByCandidateElection(
    db,
    candidateRows,
    electionRows
  );
  const washingtonSummaries = await loadWashingtonCandidateFinanceSummariesByCandidateElection(db, candidateRows, electionRows);
  const hawaiiSummaries = await loadHawaiiCandidateFinanceSummariesByCandidateElection(db, candidateRows, electionRows);
  const districtOfColumbiaSummaries = await loadDistrictOfColumbiaCandidateFinanceSummariesByCandidateElection(
    db,
    candidateRows,
    electionRows
  );
  const kentuckySummaries = await loadKentuckyCandidateFinanceSummariesByCandidateElection(db, candidateRows, electionRows);
  const floridaSummaries = await loadOptionalFloridaCandidateFinanceSummariesByCandidateElection(db, candidateRows, electionRows);
  const virginiaSummaries = await loadVirginiaCandidateFinanceSummariesByCandidateElection(db, candidateRows, electionRows);
  const tennesseeSummaries = await loadTennesseeCandidateFinanceSummariesByCandidateElection(db, candidateRows, electionRows);
  const texasSummaries = await loadTexasCandidateFinanceSummariesByCandidateElection(db, candidateRows, electionRows);
  const arizonaSummaries = await loadArizonaCandidateFinanceSummariesByCandidateElection(db, candidateRows, electionRows);
  const utahSummaries = await loadUtahCandidateFinanceSummariesByCandidateElection(db, candidateRows, electionRows);
  const indianaSummaries = await loadIndianaCandidateFinanceSummariesByCandidateElection(db, candidateRows, electionRows);
  const nebraskaSummaries = await loadNebraskaCandidateFinanceSummariesByCandidateElection(db, candidateRows, electionRows);
  const oklahomaSummaries = await loadOklahomaCandidateFinanceSummariesByCandidateElection(db, candidateRows, electionRows);
  const newJerseySummaries = await loadNewJerseyCandidateFinanceSummariesByCandidateElection(db, candidateRows, electionRows);
  const newMexicoSummaries = await loadNewMexicoCandidateFinanceSummariesByCandidateElection(db, candidateRows, electionRows);
  const connecticutSummaries = await loadConnecticutCandidateFinanceSummariesByCandidateElection(db, candidateRows, electionRows);
  const coloradoSummaries = await loadColoradoCandidateFinanceSummariesByCandidateElection(db, candidateRows, electionRows);
  const californiaSummaries = await loadCaliforniaCandidateFinanceSummariesByCandidateElection(db, candidateRows, electionRows);
  const fecSummaries = await loadFecCandidateFinanceSummariesByCandidateElection(db, candidateRows, electionRows);

  const merged = new Map(wisconsinSummaries);
  for (const [key, summary] of massachusettsSummaries) {
    merged.set(key, summary);
  }
  for (const [key, summary] of vermontSummaries) {
    merged.set(key, summary);
  }
  for (const [key, summary] of louisianaSummaries) {
    merged.set(key, summary);
  }
  for (const [key, summary] of marylandSummaries) {
    merged.set(key, summary);
  }
  for (const [key, summary] of maineSummaries) {
    merged.set(key, summary);
  }
  for (const [key, summary] of alaskaSummaries) {
    merged.set(key, summary);
  }
  for (const [key, summary] of michiganSummaries) {
    merged.set(key, summary);
  }
  for (const [key, summary] of illinoisSummaries) {
    merged.set(key, summary);
  }
  for (const [key, summary] of minnesotaSummaries) {
    merged.set(key, summary);
  }
  for (const [key, summary] of oregonSummaries) {
    merged.set(key, summary);
  }
  for (const [key, summary] of pennsylvaniaSummaries) {
    merged.set(key, summary);
  }
  for (const [key, summary] of washingtonSummaries) {
    merged.set(key, summary);
  }
  for (const [key, summary] of hawaiiSummaries) {
    merged.set(key, summary);
  }
  for (const [key, summary] of districtOfColumbiaSummaries) {
    merged.set(key, summary);
  }
  for (const [key, summary] of kentuckySummaries) {
    merged.set(key, summary);
  }
  for (const [key, summary] of floridaSummaries) {
    merged.set(key, summary);
  }
  for (const [key, summary] of virginiaSummaries) {
    merged.set(key, summary);
  }
  for (const [key, summary] of tennesseeSummaries) {
    merged.set(key, summary);
  }
  for (const [key, summary] of texasSummaries) {
    merged.set(key, summary);
  }
  for (const [key, summary] of arizonaSummaries) {
    merged.set(key, summary);
  }
  for (const [key, summary] of utahSummaries) {
    merged.set(key, summary);
  }
  for (const [key, summary] of indianaSummaries) {
    merged.set(key, summary);
  }
  for (const [key, summary] of nebraskaSummaries) {
    merged.set(key, summary);
  }
  for (const [key, summary] of oklahomaSummaries) {
    merged.set(key, summary);
  }
  for (const [key, summary] of newJerseySummaries) {
    merged.set(key, summary);
  }
  for (const [key, summary] of newMexicoSummaries) {
    merged.set(key, summary);
  }
  for (const [key, summary] of connecticutSummaries) {
    merged.set(key, summary);
  }
  for (const [key, summary] of coloradoSummaries) {
    merged.set(key, summary);
  }
  for (const [key, summary] of californiaSummaries) {
    merged.set(key, summary);
  }
  // Federal FEC summaries intentionally win when both sources exist for the same candidate/election.
  for (const [key, summary] of fecSummaries) {
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


export async function lookupElectionDetailById(db: Queryable, electionId: string): Promise<BallotLookupElection | null> {
  const trimmedElectionId = electionId.trim();
  if (trimmedElectionId.length === 0) {
    return null;
  }

  const electionResult = await db.query<ElectionDetailRow>(
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
    [trimmedElectionId]
  );

  const details = await loadFullElectionDetails(db, electionResult.rows);
  const detail = details[0];
  if (!detail) {
    return null;
  }

  const historicalCompetitivenessByElection = await loadHistoricalCompetitivenessByElection(db, electionResult.rows);
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
