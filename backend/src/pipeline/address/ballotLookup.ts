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
import { isVirginiaFinanceEligibleOffice } from "../virginiaFinance/virginiaFinanceEligibleOffices.js";
import { isWisconsinFinanceEligibleOffice } from "../wisconsinFinance/wisconsinFinanceEligibleOffices.js";
import { isMassachusettsFinanceEligibleOffice } from "../massachusettsFinance/massachusettsFinanceEligibleOffices.js";
import { isMichiganFinanceEligibleOffice } from "../michiganFinance/michiganFinanceEligibleOffices.js";
import {
  isCaliforniaCampaignFinanceEnabled,
  isCandidateFinanceEnabled,
  isColoradoCampaignFinanceEnabled,
  isConnecticutCampaignFinanceEnabled,
  isDistrictOfColumbiaCampaignFinanceEnabled,
  isNebraskaCampaignFinanceEnabled,
  isNewMexicoCampaignFinanceEnabled,
  isOklahomaCampaignFinanceEnabled,
  isTexasCampaignFinanceEnabled,
  isHawaiiCampaignFinanceEnabled,
  isVirginiaCampaignFinanceEnabled,
  isWashingtonCampaignFinanceEnabled,
  isWisconsinCampaignFinanceEnabled,
  isMassachusettsCampaignFinanceEnabled,
  isMarylandCampaignFinanceEnabled,
  isMichiganCampaignFinanceEnabled,
} from "../../config/featureFlags.js";

type Queryable = Pick<Pool | PoolClient, "query">;

export type BallotLookupDistrict = {
  id: string;
  district_type: ElectionDistrictType;
  geoid_compact: string;
  name: string;
  state: string;
  state_fips: string;
  representation_power_score: number | null;
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

export type BallotLookupFinanceBreakdown = {
  category_name: string;
  amount: number;
  contributor_count: number | null;
  source_url: string | null;
};

export type BallotLookupFinanceOutsideGroup = {
  committee_id: string;
  committee_name: string;
  support_oppose: "support" | "oppose";
  amount: number;
  source_url: string | null;
};

export type BallotLookupFinanceOutsideIndustrySupportEvidence = {
  organization_name: string;
  organization_type: "employer" | "donor";
  amount: number;
  contributor_count: number | null;
  committee_id: string;
  committee_name: string;
  source_url: string | null;
};

export type BallotLookupFinanceOutsideIndustrySupportSummary = BallotLookupFinanceBreakdown & {
  explanation: string;
  supporting_organizations: BallotLookupFinanceOutsideIndustrySupportEvidence[];
};

export type BallotLookupFinanceBackingSummary = {
  top_direct_donor_occupations: BallotLookupFinanceBreakdown[];
  top_outside_supporting_industries: BallotLookupFinanceOutsideIndustrySupportSummary[];
};

export type BallotLookupFinanceSummary = {
  source:
    | "FEC"
    | "CALIFORNIA_SOS"
    | "COLORADO_TRACER"
    | "CONNECTICUT_ECRIS"
    | "NEBRASKA_NADC"
    | "NEW_MEXICO_CFIS"
    | "OKLAHOMA_GUARDIAN"
    | "TEXAS_TEC"
    | "HAWAII_CSC"
    | "VIRGINIA_CFREPORTS"
    | "WASHINGTON_PDC"
    | "WISCONSIN_SUNSHINE"
    | "MASSACHUSETTS_OCPF"
    | "MARYLAND_CFS"
    | "MICHIGAN_MITN"
    | "DISTRICT_OF_COLUMBIA_OCF";
  cycle: number;
  fec_candidate_id: string | null;
  controlled_committee_id?: string | null;
  last_synced_at: string;
  direct_campaign: {
    total_raised: number | null;
    total_spent: number | null;
    cash_on_hand: number | null;
    debts_owed: number | null;
    top_occupations: BallotLookupFinanceBreakdown[];
    top_employers?: BallotLookupFinanceBreakdown[];
    top_industries: BallotLookupFinanceBreakdown[];
    contribution_size_buckets?: BallotLookupFinanceBreakdown[];
  };
  outside_spending: {
    support_total: number | null;
    oppose_total: number | null;
    top_supporting_groups: BallotLookupFinanceOutsideGroup[];
    top_opposing_groups: BallotLookupFinanceOutsideGroup[];
    top_supporting_industries: BallotLookupFinanceBreakdown[];
    top_opposing_industries: BallotLookupFinanceBreakdown[];
  };
  backing_summary: BallotLookupFinanceBackingSummary;
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

type DistrictRow = Omit<BallotLookupDistrict, "representation_power_score"> & {
  representation_power_score: string | number | null;
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
  race_type: ElectionRaceType;
  official_ballot_title: string;
  election_date: string;
  election_stage: ElectionStage | null;
  is_partisan: boolean | null;
  discovery_contest_family: ElectionContestFamily | null;
  sources: unknown;
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

type CaliforniaFinanceSummaryRequest = {
  candidate_id: string;
  election_id: string;
};

type ColoradoFinanceSummaryRequest = {
  candidate_id: string;
  election_id: string;
};

type ConnecticutFinanceSummaryRequest = {
  candidate_id: string;
  election_id: string;
};

type NebraskaFinanceSummaryRequest = {
  candidate_id: string;
  election_id: string;
};

type NewMexicoFinanceSummaryRequest = {
  candidate_id: string;
  election_id: string;
};

type OklahomaFinanceSummaryRequest = {
  candidate_id: string;
  election_id: string;
};

type TexasFinanceSummaryRequest = {
  candidate_id: string;
  election_id: string;
};

type WashingtonFinanceSummaryRequest = {
  candidate_id: string;
  election_id: string;
};

type WisconsinFinanceSummaryRequest = {
  candidate_id: string;
  election_id: string;
};

type HawaiiFinanceSummaryRequest = {
  candidate_id: string;
  election_id: string;
};

type DistrictOfColumbiaFinanceSummaryRequest = {
  candidate_id: string;
  election_id: string;
};

type VirginiaFinanceSummaryRequest = {
  candidate_id: string;
  election_id: string;
};

type MassachusettsFinanceSummaryRequest = {
  candidate_id: string;
  election_id: string;
};

type MarylandFinanceSummaryRequest = {
  candidate_id: string;
  election_id: string;
};

type MichiganFinanceSummaryRequest = {
  candidate_id: string;
  election_id: string;
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

type CaliforniaFinanceSummaryRow = {
  candidate_id: string;
  election_id: string;
  controlled_committee_id: string | null;
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

type CaliforniaFinanceDirectBreakdownRow = {
  candidate_id: string;
  election_id: string;
  category_type: "occupation" | "employer" | "industry";
  category_name: string;
  amount: string | number;
  contributor_count: string | number | null;
  source_url: string | null;
};

type ColoradoFinanceSummaryRow = {
  candidate_id: string;
  election_id: string;
  committee_id: string | null;
  election_year: number;
  total_receipts: string | number | null;
  source_url: string | null;
  last_synced_at: string;
};

type ColoradoFinanceDirectBreakdownRow = {
  candidate_id: string;
  election_id: string;
  category_type: "occupation" | "employer" | "industry";
  category_name: string;
  amount: string | number;
  contributor_count: string | number | null;
  source_url: string | null;
};

type ConnecticutFinanceSummaryRow = {
  candidate_id: string;
  election_id: string;
  committee_id: string | null;
  election_year: number;
  total_receipts: string | number | null;
  total_disbursements: string | number | null;
  source_url: string | null;
  last_synced_at: string;
};

type ConnecticutFinanceDirectBreakdownRow = {
  candidate_id: string;
  election_id: string;
  category_type: "occupation" | "employer" | "industry" | "contribution_size";
  category_name: string;
  amount: string | number;
  contributor_count: string | number | null;
  source_url: string | null;
};

type NebraskaFinanceSummaryRow = {
  candidate_id: string;
  election_id: string;
  committee_id: string | null;
  election_year: number;
  total_receipts: string | number | null;
  direct_contribution_total: string | number | null;
  source_url: string | null;
  last_synced_at: string;
};

type NebraskaFinanceDirectBreakdownRow = {
  candidate_id: string;
  election_id: string;
  category_type: "occupation" | "industry" | "contributor_source_type" | "contribution_size";
  category_name: string;
  amount: string | number;
  contributor_count: string | number | null;
  source_url: string | null;
};

type CaliforniaFinanceOutsideGroupRow = {
  candidate_id: string;
  election_id: string;
  committee_id: string;
  committee_name: string;
  support_oppose: "support" | "oppose";
  amount: string | number;
  source_url: string | null;
};

type CaliforniaFinanceOutsideIndustryRow = {
  candidate_id: string;
  election_id: string;
  support_oppose: "support" | "oppose";
  category_name: string;
  amount: string | number;
  contributor_count: string | number | null;
  source_url: string | null;
};

type NewMexicoFinanceSummaryRow = {
  candidate_id: string;
  election_id: string;
  committee_id: string | null;
  election_year: number;
  total_receipts: string | number | null;
  direct_contribution_total: string | number | null;
  total_disbursements: string | number | null;
  outside_support_total: string | number | null;
  outside_oppose_total: string | number | null;
  source_url: string | null;
  last_synced_at: string;
};

type NewMexicoFinanceDirectBreakdownRow = {
  candidate_id: string;
  election_id: string;
  category_type: "occupation" | "industry" | "contribution_size" | "contributor_source_type";
  category_name: string;
  amount: string | number;
  contributor_count: string | number | null;
  source_url: string | null;
};

type NewMexicoFinanceOutsideGroupRow = {
  candidate_id: string;
  election_id: string;
  committee_id: string;
  committee_name: string;
  support_oppose: "support" | "oppose";
  amount: string | number;
  source_url: string | null;
};

type NewMexicoFinanceOutsideIndustryRow = {
  candidate_id: string;
  election_id: string;
  support_oppose: "support" | "oppose";
  category_name: string;
  amount: string | number;
  contributor_count: string | number | null;
  source_url: string | null;
};

type OklahomaFinanceSummaryRow = {
  candidate_id: string;
  election_id: string;
  committee_id: string | null;
  election_year: number;
  total_receipts: string | number | null;
  direct_contribution_total: string | number | null;
  source_url: string | null;
  last_synced_at: string;
};

type OklahomaFinanceDirectBreakdownRow = {
  candidate_id: string;
  election_id: string;
  category_type: "occupation" | "contribution_size";
  category_name: string;
  amount: string | number;
  contributor_count: string | number | null;
  source_url: string | null;
};

type TexasFinanceSummaryRow = {
  candidate_id: string;
  election_id: string;
  committee_id: string | null;
  election_year: number;
  total_receipts: string | number | null;
  direct_contribution_total: string | number | null;
  total_disbursements: string | number | null;
  cash_on_hand: string | number | null;
  outside_support_total: string | number | null;
  outside_oppose_total: string | number | null;
  source_url: string | null;
  last_synced_at: string;
};

type TexasFinanceDirectBreakdownRow = {
  candidate_id: string;
  election_id: string;
  category_type: "occupation" | "contribution_size";
  category_name: string;
  amount: string | number;
  contributor_count: string | number | null;
  source_url: string | null;
};

type TexasFinanceOutsideGroupRow = {
  candidate_id: string;
  election_id: string;
  committee_id: string;
  committee_name: string;
  support_oppose: "support" | "oppose";
  amount: string | number;
  source_url: string | null;
};

type TexasFinanceOutsideIndustryRow = {
  candidate_id: string;
  election_id: string;
  support_oppose: "support" | "oppose";
  category_name: string;
  amount: string | number;
  contributor_count: string | number | null;
  source_url: string | null;
};

type TexasFinanceOutsideDonorEvidenceRow = {
  candidate_id: string;
  election_id: string;
  industry_name: string;
  committee_id: string;
  committee_name: string;
  support_oppose: "support" | "oppose";
  organization_name: string;
  amount: string | number;
  contributor_count: string | number | null;
  source_url: string | null;
};

type WashingtonFinanceSummaryRow = TexasFinanceSummaryRow;
type WashingtonFinanceDirectBreakdownRow = TexasFinanceDirectBreakdownRow;
type WashingtonFinanceOutsideGroupRow = TexasFinanceOutsideGroupRow;
type WashingtonFinanceOutsideIndustryRow = TexasFinanceOutsideIndustryRow;
type WashingtonFinanceOutsideDonorEvidenceRow = TexasFinanceOutsideDonorEvidenceRow;
type WisconsinFinanceSummaryRow = TexasFinanceSummaryRow;
type WisconsinFinanceDirectBreakdownRow = TexasFinanceDirectBreakdownRow;
type WisconsinFinanceOutsideGroupRow = TexasFinanceOutsideGroupRow;
type WisconsinFinanceOutsideIndustryRow = TexasFinanceOutsideIndustryRow;
type WisconsinFinanceOutsideDonorEvidenceRow = TexasFinanceOutsideDonorEvidenceRow;
type HawaiiFinanceSummaryRow = TexasFinanceSummaryRow;
type HawaiiFinanceDirectBreakdownRow = TexasFinanceDirectBreakdownRow;
type HawaiiFinanceOutsideGroupRow = TexasFinanceOutsideGroupRow;
type HawaiiFinanceOutsideIndustryRow = TexasFinanceOutsideIndustryRow;
type HawaiiFinanceOutsideDonorEvidenceRow = TexasFinanceOutsideDonorEvidenceRow;
type DistrictOfColumbiaFinanceSummaryRow = TexasFinanceSummaryRow;
type DistrictOfColumbiaFinanceDirectBreakdownRow = TexasFinanceDirectBreakdownRow;
type DistrictOfColumbiaFinanceOutsideGroupRow = TexasFinanceOutsideGroupRow;
type DistrictOfColumbiaFinanceOutsideIndustryRow = TexasFinanceOutsideIndustryRow;
type DistrictOfColumbiaFinanceOutsideDonorEvidenceRow = TexasFinanceOutsideDonorEvidenceRow;
type MassachusettsFinanceSummaryRow = TexasFinanceSummaryRow;
type MassachusettsFinanceDirectBreakdownRow = TexasFinanceDirectBreakdownRow;
type MassachusettsFinanceOutsideGroupRow = TexasFinanceOutsideGroupRow;
type MassachusettsFinanceOutsideIndustryRow = TexasFinanceOutsideIndustryRow;
type MassachusettsFinanceOutsideDonorEvidenceRow = TexasFinanceOutsideDonorEvidenceRow;
type MarylandFinanceSummaryRow = TexasFinanceSummaryRow;
type MarylandFinanceDirectBreakdownRow = TexasFinanceDirectBreakdownRow;
type MarylandFinanceOutsideGroupRow = TexasFinanceOutsideGroupRow;
type MarylandFinanceOutsideIndustryRow = TexasFinanceOutsideIndustryRow;
type MarylandFinanceOutsideDonorEvidenceRow = TexasFinanceOutsideDonorEvidenceRow;
type MichiganFinanceSummaryRow = TexasFinanceSummaryRow;
type MichiganFinanceDirectBreakdownRow = TexasFinanceDirectBreakdownRow;
type MichiganFinanceOutsideGroupRow = TexasFinanceOutsideGroupRow;
type MichiganFinanceOutsideIndustryRow = TexasFinanceOutsideIndustryRow;
type MichiganFinanceOutsideDonorEvidenceRow = TexasFinanceOutsideDonorEvidenceRow;

type VirginiaFinanceSummaryRow = {
  candidate_id: string;
  election_id: string;
  committee_id: string | null;
  election_year: number;
  total_receipts: string | number | null;
  direct_contribution_total: string | number | null;
  source_url: string | null;
  last_synced_at: string;
};

type VirginiaFinanceDirectBreakdownRow = {
  candidate_id: string;
  election_id: string;
  category_type: "occupation" | "contribution_size";
  category_name: string;
  amount: string | number;
  contributor_count: string | number | null;
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

function parseFinanceAmount(value: string | number | null | undefined): number | null {
  if (value === null || value === undefined) {
    return null;
  }
  const parsed = typeof value === "number" ? value : Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function parseFinanceCount(value: string | number | null | undefined): number | null {
  const parsed = parseFinanceAmount(value);
  return parsed === null ? null : Math.trunc(parsed);
}

function candidateElectionKey(candidateId: string, electionId: string): string {
  return `${candidateId}\u0000${electionId}`;
}

function normalizeFecCandidateIdForFinance(value: string): string | null {
  const normalized = value.trim().toUpperCase();
  return /^[HPS][0-9A-Z]{8}$/.test(normalized) ? normalized : null;
}

const GENERIC_FEC_DATA_SOURCE_URL = "https://www.fec.gov/data/";
const GENERIC_FEC_OUTSIDE_SPENDING_SOURCE_URL = "https://www.fec.gov/data/independent-expenditures/";
const GENERIC_CA_POWER_SEARCH_SOURCE_URL = "https://powersearch.sos.ca.gov/";
const GENERIC_CA_POWER_SEARCH_IE_SOURCE_URL = "https://powersearch.sos.ca.gov:3000/";
const GENERIC_COLORADO_TRACER_SOURCE_URL = "https://tracer.sos.colorado.gov/PublicSite/";
const GENERIC_CONNECTICUT_ECRIS_SOURCE_URL = "https://seec.ct.gov/portal/ecris/CurPreYears";
const GENERIC_NEBRASKA_NADC_SOURCE_URL = "https://nadc-e.nebraska.gov/PublicSite/";
const GENERIC_NEW_MEXICO_CFIS_SOURCE_URL = "https://www.cfis.state.nm.us/media/CFIS_Data_Download.aspx";
const GENERIC_OKLAHOMA_GUARDIAN_SOURCE_URL = "https://guardian.ok.gov/PublicSite/DataDownload.aspx";
const GENERIC_TEXAS_TEC_SOURCE_URL = "https://www.ethics.state.tx.us/search/cf/";
const GENERIC_HAWAII_CSC_SOURCE_URL = "https://hicscdata.hawaii.gov/";
const GENERIC_WASHINGTON_PDC_SOURCE_URL = "https://www.pdc.wa.gov/political-disclosure-reporting-data/browse-search-data";
const GENERIC_WISCONSIN_SUNSHINE_SOURCE_URL = "https://campaignfinance.wi.gov/";
const GENERIC_DISTRICT_OF_COLUMBIA_OCF_SOURCE_URL = "https://efiling.ocf.dc.gov/DataDownload";
const GENERIC_VIRGINIA_CFREPORTS_SOURCE_URL = "https://cfreports.elections.virginia.gov/";
const GENERIC_MASSACHUSETTS_OCPF_SOURCE_URL = "https://www.ocpf.us/";
const GENERIC_MARYLAND_CFS_SOURCE_URL = "https://campaignfinance.maryland.gov/public/cf/downloads";
const GENERIC_MICHIGAN_MITN_SOURCE_URL =
  "https://www.michigan.gov/sos/elections/disclosure/cfr/committee-search/intro/welcome-to-the-michigan-campaign-finance-searchable-database";
const MARYLAND_BALLOT_LOOKUP_FINANCE_ELIGIBLE_OFFICE_KEYS = new Set([
  "statewide::Governor",
  "statewide::Lieutenant Governor",
  "statewide::Attorney General",
  "statewide::Comptroller",
  "state_upper::State Senator",
  "state_lower::State Lower Chamber Legislator",
]);

function isMarylandFinanceEligibleOffice(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
}): boolean {
  const officeScope = input.officeScope?.trim();
  const officeCanonicalName = input.officeCanonicalName?.trim();
  if (!officeScope || !officeCanonicalName) {
    return false;
  }
  return MARYLAND_BALLOT_LOOKUP_FINANCE_ELIGIBLE_OFFICE_KEYS.has(`${officeScope}::${officeCanonicalName}`);
}

function firstNonEmptySourceUrl(...urls: Array<string | null | undefined>): string | null {
  for (const url of urls) {
    const trimmed = url?.trim();
    if (trimmed) {
      return trimmed;
    }
  }
  return null;
}

function mapFinanceBreakdown(
  row: {
    category_name: string;
    amount: string | number;
    contributor_count: string | number | null;
    source_url?: string | null;
  },
  fallbackSourceUrl: string | null = null
): BallotLookupFinanceBreakdown {
  return {
    category_name: row.category_name,
    amount: parseFinanceAmount(row.amount) ?? 0,
    contributor_count: parseFinanceCount(row.contributor_count),
    source_url: firstNonEmptySourceUrl(row.source_url, fallbackSourceUrl),
  };
}

function buildFinanceSummaryRequests(
  candidateRows: readonly CandidateRow[],
  electionRows: readonly ElectionRow[]
): CandidateFinanceSummaryRequest[] {
  const electionYearById = new Map(electionRows.map((row) => [row.election_id, electionYear(row.election_date)]));
  const requests = new Map<string, CandidateFinanceSummaryRequest>();

  for (const row of candidateRows) {
    const year = electionYearById.get(row.election_id) ?? null;
    if (year === null) {
      continue;
    }
    for (const rawFecId of parseStringArray(row.fec_ids)) {
      const fecCandidateId = normalizeFecCandidateIdForFinance(rawFecId);
      if (!fecCandidateId) {
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

function buildCaliforniaFinanceSummaryRequests(
  candidateRows: readonly CandidateRow[],
  electionRows: readonly ElectionRow[]
): CaliforniaFinanceSummaryRequest[] {
  const californiaElectionIds = new Set(
    electionRows
      .filter((row) => row.state.trim().toUpperCase() === "CA")
      .map((row) => row.election_id)
  );
  const requests = new Map<string, CaliforniaFinanceSummaryRequest>();

  for (const row of candidateRows) {
    if (!californiaElectionIds.has(row.election_id)) {
      continue;
    }
    const key = candidateElectionKey(row.candidate_id, row.election_id);
    requests.set(key, {
      candidate_id: row.candidate_id,
      election_id: row.election_id,
    });
  }

  return [...requests.values()];
}

function buildColoradoFinanceSummaryRequests(
  candidateRows: readonly CandidateRow[],
  electionRows: readonly ElectionRow[]
): ColoradoFinanceSummaryRequest[] {
  const electionIds = new Set(electionRows.filter((row) => row.state === "CO").map((row) => row.election_id));
  const requests = new Map<string, ColoradoFinanceSummaryRequest>();
  for (const row of candidateRows) {
    if (!electionIds.has(row.election_id)) {
      continue;
    }
    const key = candidateElectionKey(row.candidate_id, row.election_id);
    requests.set(key, {
      candidate_id: row.candidate_id,
      election_id: row.election_id,
    });
  }
  return [...requests.values()];
}

function buildConnecticutFinanceSummaryRequests(
  candidateRows: readonly CandidateRow[],
  electionRows: readonly ElectionRow[]
): ConnecticutFinanceSummaryRequest[] {
  const electionIds = new Set(electionRows.filter((row) => row.state === "CT").map((row) => row.election_id));
  const requests = new Map<string, ConnecticutFinanceSummaryRequest>();
  for (const row of candidateRows) {
    if (!electionIds.has(row.election_id)) {
      continue;
    }
    const key = candidateElectionKey(row.candidate_id, row.election_id);
    requests.set(key, {
      candidate_id: row.candidate_id,
      election_id: row.election_id,
    });
  }
  return [...requests.values()];
}

function buildNebraskaFinanceSummaryRequests(
  candidateRows: readonly CandidateRow[],
  electionRows: readonly ElectionRow[]
): NebraskaFinanceSummaryRequest[] {
  const electionIds = new Set(
    electionRows
      .filter((row) => row.state.trim().toUpperCase() === "NE")
      .map((row) => row.election_id)
  );
  const requests = new Map<string, NebraskaFinanceSummaryRequest>();
  for (const row of candidateRows) {
    if (!electionIds.has(row.election_id)) {
      continue;
    }
    const key = candidateElectionKey(row.candidate_id, row.election_id);
    requests.set(key, {
      candidate_id: row.candidate_id,
      election_id: row.election_id,
    });
  }
  return [...requests.values()];
}

function buildNewMexicoFinanceSummaryRequests(
  candidateRows: readonly CandidateRow[],
  electionRows: readonly ElectionRow[]
): NewMexicoFinanceSummaryRequest[] {
  const electionIds = new Set(
    electionRows
      .filter((row) => row.state.trim().toUpperCase() === "NM")
      .map((row) => row.election_id)
  );
  const requests = new Map<string, NewMexicoFinanceSummaryRequest>();
  for (const row of candidateRows) {
    if (!electionIds.has(row.election_id)) {
      continue;
    }
    const key = candidateElectionKey(row.candidate_id, row.election_id);
    requests.set(key, {
      candidate_id: row.candidate_id,
      election_id: row.election_id,
    });
  }
  return [...requests.values()];
}

function buildOklahomaFinanceSummaryRequests(
  candidateRows: readonly CandidateRow[],
  electionRows: readonly ElectionRow[]
): OklahomaFinanceSummaryRequest[] {
  const electionIds = new Set(
    electionRows
      .filter((row) => row.state.trim().toUpperCase() === "OK")
      .map((row) => row.election_id)
  );
  const requests = new Map<string, OklahomaFinanceSummaryRequest>();
  for (const row of candidateRows) {
    if (!electionIds.has(row.election_id)) {
      continue;
    }
    const key = candidateElectionKey(row.candidate_id, row.election_id);
    requests.set(key, {
      candidate_id: row.candidate_id,
      election_id: row.election_id,
    });
  }
  return [...requests.values()];
}

function buildTexasFinanceSummaryRequests(
  candidateRows: readonly CandidateRow[],
  electionRows: readonly ElectionRow[]
): TexasFinanceSummaryRequest[] {
  const electionIds = new Set(
    electionRows
      .filter((row) => row.state.trim().toUpperCase() === "TX")
      .map((row) => row.election_id)
  );
  const requests = new Map<string, TexasFinanceSummaryRequest>();
  for (const row of candidateRows) {
    if (!electionIds.has(row.election_id)) {
      continue;
    }
    const key = candidateElectionKey(row.candidate_id, row.election_id);
    requests.set(key, {
      candidate_id: row.candidate_id,
      election_id: row.election_id,
    });
  }
  return [...requests.values()];
}

function buildWashingtonFinanceSummaryRequests(
  candidateRows: readonly CandidateRow[],
  electionRows: readonly ElectionRow[]
): WashingtonFinanceSummaryRequest[] {
  const electionIds = new Set(
    electionRows
      .filter((row) => row.state.trim().toUpperCase() === "WA")
      .map((row) => row.election_id)
  );
  const requests = new Map<string, WashingtonFinanceSummaryRequest>();
  for (const row of candidateRows) {
    if (!electionIds.has(row.election_id)) {
      continue;
    }
    const key = candidateElectionKey(row.candidate_id, row.election_id);
    requests.set(key, {
      candidate_id: row.candidate_id,
      election_id: row.election_id,
    });
  }
  return [...requests.values()];
}

function buildWisconsinFinanceSummaryRequests(
  candidateRows: readonly CandidateRow[],
  electionRows: readonly ElectionRow[]
): WisconsinFinanceSummaryRequest[] {
  const electionIds = new Set(
    electionRows
      .filter(
        (row) =>
          row.state.trim().toUpperCase() === "WI" &&
          isWisconsinFinanceEligibleOffice({
            officeScope: row.office_scope ?? null,
            officeCanonicalName: row.office_canonical_name ?? null,
          })
      )
      .map((row) => row.election_id)
  );
  const requests = new Map<string, WisconsinFinanceSummaryRequest>();
  for (const row of candidateRows) {
    if (!electionIds.has(row.election_id)) {
      continue;
    }
    const key = candidateElectionKey(row.candidate_id, row.election_id);
    requests.set(key, {
      candidate_id: row.candidate_id,
      election_id: row.election_id,
    });
  }
  return [...requests.values()];
}

function buildHawaiiFinanceSummaryRequests(
  candidateRows: readonly CandidateRow[],
  electionRows: readonly ElectionRow[]
): HawaiiFinanceSummaryRequest[] {
  const electionIds = new Set(
    electionRows
      .filter((row) => row.state.trim().toUpperCase() === "HI")
      .map((row) => row.election_id)
  );
  const requests = new Map<string, HawaiiFinanceSummaryRequest>();
  for (const row of candidateRows) {
    if (!electionIds.has(row.election_id)) {
      continue;
    }
    const key = candidateElectionKey(row.candidate_id, row.election_id);
    requests.set(key, {
      candidate_id: row.candidate_id,
      election_id: row.election_id,
    });
  }
  return [...requests.values()];
}

function buildDistrictOfColumbiaFinanceSummaryRequests(
  candidateRows: readonly CandidateRow[],
  electionRows: readonly ElectionRow[]
): DistrictOfColumbiaFinanceSummaryRequest[] {
  const electionIds = new Set(
    electionRows
      .filter((row) => row.state.trim().toUpperCase() === "DC")
      .map((row) => row.election_id)
  );
  const requests = new Map<string, DistrictOfColumbiaFinanceSummaryRequest>();
  for (const row of candidateRows) {
    if (!electionIds.has(row.election_id)) {
      continue;
    }
    const key = candidateElectionKey(row.candidate_id, row.election_id);
    requests.set(key, {
      candidate_id: row.candidate_id,
      election_id: row.election_id,
    });
  }
  return [...requests.values()];
}

function buildMassachusettsFinanceSummaryRequests(
  candidateRows: readonly CandidateRow[],
  electionRows: readonly ElectionRow[]
): MassachusettsFinanceSummaryRequest[] {
  const electionIds = new Set(
    electionRows
      .filter(
        (row) =>
          row.state.trim().toUpperCase() === "MA" &&
          isMassachusettsFinanceEligibleOffice({
            officeScope: row.office_scope ?? null,
            officeCanonicalName: row.office_canonical_name ?? null,
          })
      )
      .map((row) => row.election_id)
  );
  const requests = new Map<string, MassachusettsFinanceSummaryRequest>();
  for (const row of candidateRows) {
    if (!electionIds.has(row.election_id)) {
      continue;
    }
    const key = candidateElectionKey(row.candidate_id, row.election_id);
    requests.set(key, {
      candidate_id: row.candidate_id,
      election_id: row.election_id,
    });
  }
  return [...requests.values()];
}

function buildMarylandFinanceSummaryRequests(
  candidateRows: readonly CandidateRow[],
  electionRows: readonly ElectionRow[]
): MarylandFinanceSummaryRequest[] {
  const electionIds = new Set(
    electionRows
      .filter(
        (row) =>
          row.state.trim().toUpperCase() === "MD" &&
          isMarylandFinanceEligibleOffice({
            officeScope: row.office_scope ?? null,
            officeCanonicalName: row.office_canonical_name ?? null,
          })
      )
      .map((row) => row.election_id)
  );
  const requests = new Map<string, MarylandFinanceSummaryRequest>();
  for (const row of candidateRows) {
    if (!electionIds.has(row.election_id)) {
      continue;
    }
    const key = candidateElectionKey(row.candidate_id, row.election_id);
    requests.set(key, {
      candidate_id: row.candidate_id,
      election_id: row.election_id,
    });
  }
  return [...requests.values()];
}

function buildMichiganFinanceSummaryRequests(
  candidateRows: readonly CandidateRow[],
  electionRows: readonly ElectionRow[]
): MichiganFinanceSummaryRequest[] {
  const electionIds = new Set(
    electionRows
      .filter(
        (row) =>
          row.state.trim().toUpperCase() === "MI" &&
          isMichiganFinanceEligibleOffice({
            officeScope: row.office_scope ?? null,
            officeCanonicalName: row.office_canonical_name ?? null,
          })
      )
      .map((row) => row.election_id)
  );
  const requests = new Map<string, MichiganFinanceSummaryRequest>();
  for (const row of candidateRows) {
    if (!electionIds.has(row.election_id)) {
      continue;
    }
    const key = candidateElectionKey(row.candidate_id, row.election_id);
    requests.set(key, {
      candidate_id: row.candidate_id,
      election_id: row.election_id,
    });
  }
  return [...requests.values()];
}

function buildVirginiaFinanceSummaryRequests(
  candidateRows: readonly CandidateRow[],
  electionRows: readonly ElectionRow[]
): VirginiaFinanceSummaryRequest[] {
  const electionIds = new Set(
    electionRows
      .filter(
        (row) =>
          row.state.trim().toUpperCase() === "VA" &&
          isVirginiaFinanceEligibleOffice({
            officeScope: row.office_scope ?? null,
            officeCanonicalName: row.office_canonical_name ?? null,
          })
      )
      .map((row) => row.election_id)
  );
  const requests = new Map<string, VirginiaFinanceSummaryRequest>();
  for (const row of candidateRows) {
    if (!electionIds.has(row.election_id)) {
      continue;
    }
    const key = candidateElectionKey(row.candidate_id, row.election_id);
    requests.set(key, {
      candidate_id: row.candidate_id,
      election_id: row.election_id,
    });
  }
  return [...requests.values()];
}

function addFinanceBreakdown(
  map: Map<string, BallotLookupFinanceBreakdown[]>,
  candidateId: string,
  electionId: string,
  row: BallotLookupFinanceBreakdown
): void {
  const key = candidateElectionKey(candidateId, electionId);
  const list = map.get(key) ?? [];
  list.push(row);
  map.set(key, list);
}

function formatShortList(values: readonly string[]): string {
  const unique = [...new Set(values.map((value) => value.trim()).filter((value) => value.length > 0))];
  if (unique.length === 0) {
    return "reported organizations";
  }
  if (unique.length === 1) {
    return unique[0]!;
  }
  if (unique.length === 2) {
    return `${unique[0]} and ${unique[1]}`;
  }
  return `${unique.slice(0, -1).join(", ")}, and ${unique[unique.length - 1]}`;
}

const FINANCE_INDUSTRY_DISPLAY_NAMES: Record<string, string> = {
  agriculture_and_food: "Agriculture and food",
  construction: "Construction",
  defense_aerospace: "Defense and aerospace",
  education: "Education",
  environmental_group: "Environmental groups",
  finance_investment: "Finance and investment",
  healthcare: "Healthcare",
  hospitality: "Hospitality",
  insurance: "Insurance",
  labor_unions: "Labor unions",
  lawyers_and_legal_services: "Lawyers and legal services",
  manufacturing: "Manufacturing",
  oil_gas_energy: "Oil, gas, and energy",
  pharmaceuticals: "Pharmaceuticals",
  real_estate: "Real estate",
  technology: "Technology",
  transportation: "Transportation",
  waste_management: "Waste management",
};

function financeIndustryDisplayName(industryName: string): string {
  const trimmed = industryName.trim();
  if (!trimmed) {
    return "This industry";
  }
  return (
    FINANCE_INDUSTRY_DISPLAY_NAMES[trimmed] ??
    trimmed
      .split("_")
      .filter((part) => part.length > 0)
      .map((part, index) => (index === 0 ? part.charAt(0).toUpperCase() + part.slice(1).toLowerCase() : part.toLowerCase()))
      .join(" ")
  );
}

function buildOutsideIndustrySupportExplanation(
  industryName: string,
  evidence: readonly BallotLookupFinanceOutsideIndustrySupportEvidence[]
): string {
  const displayName = financeIndustryDisplayName(industryName);
  if (evidence.length === 0) {
    return `The ${displayName} category is a top outside-spending support industry because organizations classified in this industry contributed to outside groups that reported independent spending supporting this candidate.`;
  }

  return `The ${displayName} category is a top outside-spending support industry because ${formatShortList(
    evidence.map((item) => item.organization_name)
  )} contributed to ${formatShortList(
    evidence.map((item) => item.committee_name)
  )}, which reported independent spending supporting this candidate.`;
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
  };
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

function electionYear(electionDate: string): number | null {
  const year = Number.parseInt(electionDate.slice(0, 4), 10);
  return Number.isInteger(year) ? year : null;
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

async function loadCaliforniaCandidateFinanceSummariesByCandidateElection(
  db: Queryable,
  candidateRows: readonly CandidateRow[],
  electionRows: readonly ElectionRow[]
): Promise<Map<string, BallotLookupFinanceSummary>> {
  if (!isCaliforniaCampaignFinanceEnabled()) {
    return new Map();
  }

  const requests = buildCaliforniaFinanceSummaryRequests(candidateRows, electionRows);
  if (requests.length === 0) {
    return new Map();
  }

  const summaryResult = await db.query<CaliforniaFinanceSummaryRow>(
    `
      WITH requested AS (
        SELECT
          candidate_id::uuid AS candidate_id,
          election_id::uuid AS election_id
        FROM jsonb_to_recordset($1::jsonb) AS x(
          candidate_id text,
          election_id text
        )
      )
      SELECT
        requested.candidate_id::text AS candidate_id,
        requested.election_id::text AS election_id,
        CASE
          WHEN count(DISTINCT link.controlled_committee_id) = 1 THEN min(link.controlled_committee_id)
          ELSE NULL
        END AS controlled_committee_id,
        max(summary.election_year) AS election_year,
        CASE WHEN count(summary.total_receipts) = 0 THEN NULL ELSE sum(summary.total_receipts) END AS total_receipts,
        CASE WHEN count(summary.total_disbursements) = 0 THEN NULL ELSE sum(summary.total_disbursements) END AS total_disbursements,
        CASE WHEN count(summary.cash_on_hand) = 0 THEN NULL ELSE sum(summary.cash_on_hand) END AS cash_on_hand,
        CASE WHEN count(summary.debts_owed) = 0 THEN NULL ELSE sum(summary.debts_owed) END AS debts_owed,
        max(summary.outside_support_total) AS outside_support_total,
        max(summary.outside_oppose_total) AS outside_oppose_total,
        min(summary.source_url) FILTER (WHERE summary.source_url IS NOT NULL) AS source_url,
        max(summary.last_synced_at)::text AS last_synced_at
      FROM requested
      JOIN public.ca_candidate_finance_links AS link
        ON link.candidate_id = requested.candidate_id
       AND link.election_id = requested.election_id
       AND link.link_status = 'active'
      JOIN public.ca_candidate_finance_summaries AS summary
        ON summary.link_id = link.id
       AND summary.election_year = link.election_year
      GROUP BY requested.candidate_id, requested.election_id
    `,
    [JSON.stringify(requests)]
  );

  if (summaryResult.rows.length === 0) {
    return new Map();
  }

  const selectedRequests = summaryResult.rows.map((row) => ({
    candidate_id: row.candidate_id,
    election_id: row.election_id,
  }));

  const directBreakdownResult = await db.query<CaliforniaFinanceDirectBreakdownRow>(
    `
      WITH selected AS (
        SELECT
          candidate_id::uuid AS candidate_id,
          election_id::uuid AS election_id
        FROM jsonb_to_recordset($1::jsonb) AS x(
          candidate_id text,
          election_id text
        )
      ),
      grouped AS (
        SELECT
          selected.candidate_id::text AS candidate_id,
          selected.election_id::text AS election_id,
          breakdown.category_type,
          breakdown.category_name,
          sum(breakdown.amount) AS amount,
          CASE
            WHEN count(breakdown.contributor_count) = 0 THEN NULL
            ELSE sum(breakdown.contributor_count)
          END AS contributor_count,
          min(breakdown.source_url) FILTER (WHERE breakdown.source_url IS NOT NULL) AS source_url
        FROM selected
        JOIN public.ca_candidate_finance_links AS link
          ON link.candidate_id = selected.candidate_id
         AND link.election_id = selected.election_id
         AND link.link_status = 'active'
        JOIN public.ca_candidate_finance_direct_breakdowns AS breakdown
          ON breakdown.link_id = link.id
         AND breakdown.election_year = link.election_year
        WHERE breakdown.category_type IN ('occupation', 'industry')
        GROUP BY selected.candidate_id, selected.election_id, breakdown.category_type, breakdown.category_name
      ),
      ranked AS (
        SELECT
          *,
          row_number() OVER (
            PARTITION BY candidate_id, election_id, category_type
            ORDER BY amount DESC, category_name ASC
          ) AS rn
        FROM grouped
      )
      SELECT candidate_id, election_id, category_type, category_name, amount, contributor_count, source_url
      FROM ranked
      WHERE rn <= 5
      ORDER BY candidate_id, election_id, category_type, amount DESC, category_name ASC
    `,
    [JSON.stringify(selectedRequests)]
  );

  const outsideGroupResult = await db.query<CaliforniaFinanceOutsideGroupRow>(
    `
      WITH selected AS (
        SELECT
          candidate_id::uuid AS candidate_id,
          election_id::uuid AS election_id
        FROM jsonb_to_recordset($1::jsonb) AS x(
          candidate_id text,
          election_id text
        )
      ),
      grouped AS (
        SELECT
          selected.candidate_id::text AS candidate_id,
          selected.election_id::text AS election_id,
          outside_group.committee_id,
          min(outside_group.committee_name) AS committee_name,
          outside_group.support_oppose,
          max(outside_group.amount) AS amount,
          min(outside_group.source_url) FILTER (WHERE outside_group.source_url IS NOT NULL) AS source_url
        FROM selected
        JOIN public.ca_candidate_finance_links AS link
          ON link.candidate_id = selected.candidate_id
         AND link.election_id = selected.election_id
         AND link.link_status = 'active'
        JOIN public.ca_candidate_finance_outside_groups AS outside_group
          ON outside_group.link_id = link.id
         AND outside_group.election_year = link.election_year
        GROUP BY selected.candidate_id, selected.election_id, outside_group.committee_id, outside_group.support_oppose
      ),
      ranked AS (
        SELECT
          *,
          row_number() OVER (
            PARTITION BY candidate_id, election_id, support_oppose
            ORDER BY amount DESC, committee_name ASC
          ) AS rn
        FROM grouped
      )
      SELECT candidate_id, election_id, committee_id, committee_name, support_oppose, amount, source_url
      FROM ranked
      WHERE rn <= 5
      ORDER BY candidate_id, election_id, support_oppose, amount DESC, committee_name ASC
    `,
    [JSON.stringify(selectedRequests)]
  );

  const outsideIndustryResult = await db.query<CaliforniaFinanceOutsideIndustryRow>(
    `
      WITH selected AS (
        SELECT
          candidate_id::uuid AS candidate_id,
          election_id::uuid AS election_id
        FROM jsonb_to_recordset($1::jsonb) AS x(
          candidate_id text,
          election_id text
        )
      ),
      per_group AS (
        SELECT
          selected.candidate_id::text AS candidate_id,
          selected.election_id::text AS election_id,
          breakdown.committee_id,
          breakdown.support_oppose,
          breakdown.category_name,
          max(breakdown.amount) AS amount,
          CASE
            WHEN count(breakdown.contributor_count) = 0 THEN NULL
            ELSE max(breakdown.contributor_count)
          END AS contributor_count,
          min(breakdown.source_url) FILTER (WHERE breakdown.source_url IS NOT NULL) AS source_url
        FROM selected
        JOIN public.ca_candidate_finance_links AS link
          ON link.candidate_id = selected.candidate_id
         AND link.election_id = selected.election_id
         AND link.link_status = 'active'
        JOIN public.ca_candidate_finance_outside_group_breakdowns AS breakdown
          ON breakdown.link_id = link.id
         AND breakdown.election_year = link.election_year
        WHERE breakdown.category_type = 'industry'
        GROUP BY
          selected.candidate_id,
          selected.election_id,
          breakdown.committee_id,
          breakdown.support_oppose,
          breakdown.category_name
      ),
      grouped AS (
        SELECT
          candidate_id,
          election_id,
          support_oppose,
          category_name,
          sum(amount) AS amount,
          CASE
            WHEN count(contributor_count) = 0 THEN NULL
            ELSE sum(contributor_count)
          END AS contributor_count,
          min(source_url) FILTER (WHERE source_url IS NOT NULL) AS source_url
        FROM per_group
        GROUP BY candidate_id, election_id, support_oppose, category_name
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

  const directOccupationsByCandidateElection = new Map<string, BallotLookupFinanceBreakdown[]>();
  const directIndustriesByCandidateElection = new Map<string, BallotLookupFinanceBreakdown[]>();
  const summaryByCandidateElection = new Map(
    summaryResult.rows.map((row) => [candidateElectionKey(row.candidate_id, row.election_id), row])
  );
  for (const row of directBreakdownResult.rows) {
    const summary = summaryByCandidateElection.get(candidateElectionKey(row.candidate_id, row.election_id));
    const mapped = mapFinanceBreakdown(row, summary?.source_url ?? GENERIC_CA_POWER_SEARCH_SOURCE_URL);
    if (row.category_type === "occupation") {
      addFinanceBreakdown(directOccupationsByCandidateElection, row.candidate_id, row.election_id, mapped);
    } else if (row.category_type === "industry") {
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
      source_url: firstNonEmptySourceUrl(row.source_url, GENERIC_CA_POWER_SEARCH_IE_SOURCE_URL),
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
      mapFinanceBreakdown(row, GENERIC_CA_POWER_SEARCH_IE_SOURCE_URL)
    );
  }

  return new Map(
    summaryResult.rows.map((row) => {
      const key = candidateElectionKey(row.candidate_id, row.election_id);
      const topDirectDonorOccupations = directOccupationsByCandidateElection.get(key) ?? [];
      const topOutsideSupportingIndustries = (supportingIndustriesByCandidateElection.get(key) ?? []).map(
        (industry): BallotLookupFinanceOutsideIndustrySupportSummary => ({
          ...industry,
          explanation: buildOutsideIndustrySupportExplanation(industry.category_name, []),
          supporting_organizations: [],
        })
      );
      return [
        key,
        {
          source: "CALIFORNIA_SOS",
          cycle: row.election_year,
          fec_candidate_id: null,
          controlled_committee_id: row.controlled_committee_id,
          last_synced_at: row.last_synced_at,
          direct_campaign: {
            total_raised: parseFinanceAmount(row.total_receipts),
            total_spent: parseFinanceAmount(row.total_disbursements),
            cash_on_hand: parseFinanceAmount(row.cash_on_hand),
            debts_owed: parseFinanceAmount(row.debts_owed),
            top_occupations: topDirectDonorOccupations,
            top_employers: [],
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

async function loadColoradoCandidateFinanceSummariesByCandidateElection(
  db: Queryable,
  candidateRows: readonly CandidateRow[],
  electionRows: readonly ElectionRow[]
): Promise<Map<string, BallotLookupFinanceSummary>> {
  if (!isColoradoCampaignFinanceEnabled()) {
    return new Map();
  }

  const requests = buildColoradoFinanceSummaryRequests(candidateRows, electionRows);
  if (requests.length === 0) {
    return new Map();
  }

  const summaryResult = await db.query<ColoradoFinanceSummaryRow>(
    `
      WITH requested AS (
        SELECT
          candidate_id::uuid AS candidate_id,
          election_id::uuid AS election_id
        FROM jsonb_to_recordset($1::jsonb) AS x(
          candidate_id text,
          election_id text
        )
      )
      SELECT
        requested.candidate_id::text AS candidate_id,
        requested.election_id::text AS election_id,
        CASE
          WHEN count(DISTINCT link.committee_id) = 1 THEN min(link.committee_id)
          ELSE NULL
        END AS committee_id,
        max(summary.election_year) AS election_year,
        CASE WHEN count(summary.total_receipts) = 0 THEN NULL ELSE sum(summary.total_receipts) END AS total_receipts,
        min(summary.source_url) FILTER (WHERE summary.source_url IS NOT NULL) AS source_url,
        max(summary.last_synced_at)::text AS last_synced_at
      FROM requested
      JOIN public.co_candidate_finance_links AS link
        ON link.candidate_id = requested.candidate_id
       AND link.election_id = requested.election_id
       AND link.link_status = 'active'
      JOIN public.co_candidate_finance_summaries AS summary
        ON summary.link_id = link.id
       AND summary.election_year = link.election_year
      GROUP BY requested.candidate_id, requested.election_id
    `,
    [JSON.stringify(requests)]
  );

  if (summaryResult.rows.length === 0) {
    return new Map();
  }

  const selectedRequests = summaryResult.rows.map((row) => ({
    candidate_id: row.candidate_id,
    election_id: row.election_id,
  }));

  const directBreakdownResult = await db.query<ColoradoFinanceDirectBreakdownRow>(
    `
      WITH selected AS (
        SELECT
          candidate_id::uuid AS candidate_id,
          election_id::uuid AS election_id
        FROM jsonb_to_recordset($1::jsonb) AS x(
          candidate_id text,
          election_id text
        )
      ),
      grouped AS (
        SELECT
          selected.candidate_id::text AS candidate_id,
          selected.election_id::text AS election_id,
          breakdown.category_type,
          breakdown.category_name,
          sum(breakdown.amount) AS amount,
          CASE
            WHEN count(breakdown.contributor_count) = 0 THEN NULL
            ELSE sum(breakdown.contributor_count)
          END AS contributor_count,
          min(breakdown.source_url) FILTER (WHERE breakdown.source_url IS NOT NULL) AS source_url
        FROM selected
        JOIN public.co_candidate_finance_links AS link
          ON link.candidate_id = selected.candidate_id
         AND link.election_id = selected.election_id
         AND link.link_status = 'active'
        JOIN public.co_candidate_finance_direct_breakdowns AS breakdown
          ON breakdown.link_id = link.id
         AND breakdown.election_year = link.election_year
        WHERE breakdown.category_type IN ('occupation', 'industry')
        GROUP BY selected.candidate_id, selected.election_id, breakdown.category_type, breakdown.category_name
      ),
      ranked AS (
        SELECT
          *,
          row_number() OVER (
            PARTITION BY candidate_id, election_id, category_type
            ORDER BY amount DESC, category_name ASC
          ) AS rn
        FROM grouped
      )
      SELECT candidate_id, election_id, category_type, category_name, amount, contributor_count, source_url
      FROM ranked
      WHERE rn <= 5
      ORDER BY candidate_id, election_id, category_type, amount DESC, category_name ASC
    `,
    [JSON.stringify(selectedRequests)]
  );

  const directOccupationsByCandidateElection = new Map<string, BallotLookupFinanceBreakdown[]>();
  const directIndustriesByCandidateElection = new Map<string, BallotLookupFinanceBreakdown[]>();
  const summaryByCandidateElection = new Map(
    summaryResult.rows.map((row) => [candidateElectionKey(row.candidate_id, row.election_id), row])
  );
  for (const row of directBreakdownResult.rows) {
    const summary = summaryByCandidateElection.get(candidateElectionKey(row.candidate_id, row.election_id));
    const mapped = mapFinanceBreakdown(row, summary?.source_url ?? GENERIC_COLORADO_TRACER_SOURCE_URL);
    if (row.category_type === "occupation") {
      addFinanceBreakdown(directOccupationsByCandidateElection, row.candidate_id, row.election_id, mapped);
    } else if (row.category_type === "industry") {
      addFinanceBreakdown(directIndustriesByCandidateElection, row.candidate_id, row.election_id, mapped);
    }
  }

  return new Map(
    summaryResult.rows.map((row) => {
      const key = candidateElectionKey(row.candidate_id, row.election_id);
      const topDirectDonorOccupations = directOccupationsByCandidateElection.get(key) ?? [];
      return [
        key,
        {
          source: "COLORADO_TRACER",
          cycle: row.election_year,
          fec_candidate_id: null,
          controlled_committee_id: row.committee_id,
          last_synced_at: row.last_synced_at,
          direct_campaign: {
            total_raised: parseFinanceAmount(row.total_receipts),
            total_spent: null,
            cash_on_hand: null,
            debts_owed: null,
            top_occupations: topDirectDonorOccupations,
            top_employers: [],
            top_industries: directIndustriesByCandidateElection.get(key) ?? [],
          },
          outside_spending: {
            support_total: null,
            oppose_total: null,
            top_supporting_groups: [],
            top_opposing_groups: [],
            top_supporting_industries: [],
            top_opposing_industries: [],
          },
          backing_summary: {
            top_direct_donor_occupations: topDirectDonorOccupations,
            top_outside_supporting_industries: [],
          },
        } satisfies BallotLookupFinanceSummary,
      ];
    })
  );
}

async function loadConnecticutCandidateFinanceSummariesByCandidateElection(
  db: Queryable,
  candidateRows: readonly CandidateRow[],
  electionRows: readonly ElectionRow[]
): Promise<Map<string, BallotLookupFinanceSummary>> {
  if (!isConnecticutCampaignFinanceEnabled()) {
    return new Map();
  }

  const requests = buildConnecticutFinanceSummaryRequests(candidateRows, electionRows);
  if (requests.length === 0) {
    return new Map();
  }

  const summaryResult = await db.query<ConnecticutFinanceSummaryRow>(
    `
      WITH requested AS (
        SELECT
          candidate_id::uuid AS candidate_id,
          election_id::uuid AS election_id
        FROM jsonb_to_recordset($1::jsonb) AS x(
          candidate_id text,
          election_id text
        )
      )
      SELECT
        requested.candidate_id::text AS candidate_id,
        requested.election_id::text AS election_id,
        CASE
          WHEN count(DISTINCT link.committee_id) = 1 THEN min(link.committee_id)
          ELSE NULL
        END AS committee_id,
        max(summary.election_year) AS election_year,
        CASE WHEN count(summary.total_receipts) = 0 THEN NULL ELSE sum(summary.total_receipts) END AS total_receipts,
        CASE WHEN count(summary.total_disbursements) = 0 THEN NULL ELSE sum(summary.total_disbursements) END AS total_disbursements,
        min(summary.source_url) FILTER (WHERE summary.source_url IS NOT NULL) AS source_url,
        max(summary.last_synced_at)::text AS last_synced_at
      FROM requested
      JOIN public.ct_candidate_finance_links AS link
        ON link.candidate_id = requested.candidate_id
       AND link.election_id = requested.election_id
       AND link.link_status = 'active'
      JOIN public.ct_candidate_finance_summaries AS summary
        ON summary.link_id = link.id
       AND summary.election_year = link.election_year
      GROUP BY requested.candidate_id, requested.election_id
    `,
    [JSON.stringify(requests)]
  );

  if (summaryResult.rows.length === 0) {
    return new Map();
  }

  const selectedRequests = summaryResult.rows.map((row) => ({
    candidate_id: row.candidate_id,
    election_id: row.election_id,
  }));

  const directBreakdownResult = await db.query<ConnecticutFinanceDirectBreakdownRow>(
    `
      WITH selected AS (
        SELECT
          candidate_id::uuid AS candidate_id,
          election_id::uuid AS election_id
        FROM jsonb_to_recordset($1::jsonb) AS x(
          candidate_id text,
          election_id text
        )
      ),
      grouped AS (
        SELECT
          selected.candidate_id::text AS candidate_id,
          selected.election_id::text AS election_id,
          breakdown.category_type,
          breakdown.category_name,
          sum(breakdown.amount) AS amount,
          CASE
            WHEN count(breakdown.contributor_count) = 0 THEN NULL
            ELSE sum(breakdown.contributor_count)
          END AS contributor_count,
          min(breakdown.source_url) FILTER (WHERE breakdown.source_url IS NOT NULL) AS source_url
        FROM selected
        JOIN public.ct_candidate_finance_links AS link
          ON link.candidate_id = selected.candidate_id
         AND link.election_id = selected.election_id
         AND link.link_status = 'active'
        JOIN public.ct_candidate_finance_direct_breakdowns AS breakdown
          ON breakdown.link_id = link.id
         AND breakdown.election_year = link.election_year
        WHERE breakdown.category_type IN ('occupation', 'industry')
        GROUP BY selected.candidate_id, selected.election_id, breakdown.category_type, breakdown.category_name
      ),
      ranked AS (
        SELECT
          *,
          row_number() OVER (
            PARTITION BY candidate_id, election_id, category_type
            ORDER BY amount DESC, category_name ASC
          ) AS rn
        FROM grouped
      )
      SELECT candidate_id, election_id, category_type, category_name, amount, contributor_count, source_url
      FROM ranked
      WHERE rn <= 5
      ORDER BY candidate_id, election_id, category_type, amount DESC, category_name ASC
    `,
    [JSON.stringify(selectedRequests)]
  );

  const directOccupationsByCandidateElection = new Map<string, BallotLookupFinanceBreakdown[]>();
  const directIndustriesByCandidateElection = new Map<string, BallotLookupFinanceBreakdown[]>();
  const summaryByCandidateElection = new Map(
    summaryResult.rows.map((row) => [candidateElectionKey(row.candidate_id, row.election_id), row])
  );
  for (const row of directBreakdownResult.rows) {
    const summary = summaryByCandidateElection.get(candidateElectionKey(row.candidate_id, row.election_id));
    const mapped = mapFinanceBreakdown(row, summary?.source_url ?? GENERIC_CONNECTICUT_ECRIS_SOURCE_URL);
    if (row.category_type === "occupation") {
      addFinanceBreakdown(directOccupationsByCandidateElection, row.candidate_id, row.election_id, mapped);
    } else if (row.category_type === "industry") {
      addFinanceBreakdown(directIndustriesByCandidateElection, row.candidate_id, row.election_id, mapped);
    }
  }

  return new Map(
    summaryResult.rows.map((row) => {
      const key = candidateElectionKey(row.candidate_id, row.election_id);
      const topDirectDonorOccupations = directOccupationsByCandidateElection.get(key) ?? [];
      return [
        key,
        {
          source: "CONNECTICUT_ECRIS",
          cycle: row.election_year,
          fec_candidate_id: null,
          controlled_committee_id: row.committee_id,
          last_synced_at: row.last_synced_at,
          direct_campaign: {
            total_raised: parseFinanceAmount(row.total_receipts),
            total_spent: parseFinanceAmount(row.total_disbursements),
            cash_on_hand: null,
            debts_owed: null,
            top_occupations: topDirectDonorOccupations,
            top_employers: [],
            top_industries: directIndustriesByCandidateElection.get(key) ?? [],
          },
          outside_spending: {
            support_total: null,
            oppose_total: null,
            top_supporting_groups: [],
            top_opposing_groups: [],
            top_supporting_industries: [],
            top_opposing_industries: [],
          },
          backing_summary: {
            top_direct_donor_occupations: topDirectDonorOccupations,
            top_outside_supporting_industries: [],
          },
        } satisfies BallotLookupFinanceSummary,
      ];
    })
  );
}

async function loadNebraskaCandidateFinanceSummariesByCandidateElection(
  db: Queryable,
  candidateRows: readonly CandidateRow[],
  electionRows: readonly ElectionRow[]
): Promise<Map<string, BallotLookupFinanceSummary>> {
  if (!isNebraskaCampaignFinanceEnabled()) {
    return new Map();
  }

  const requests = buildNebraskaFinanceSummaryRequests(candidateRows, electionRows);
  if (requests.length === 0) {
    return new Map();
  }

  const summaryResult = await db.query<NebraskaFinanceSummaryRow>(
    `
      WITH requested AS (
        SELECT
          candidate_id::uuid AS candidate_id,
          election_id::uuid AS election_id
        FROM jsonb_to_recordset($1::jsonb) AS x(
          candidate_id text,
          election_id text
        )
      )
      SELECT
        requested.candidate_id::text AS candidate_id,
        requested.election_id::text AS election_id,
        CASE
          WHEN count(DISTINCT link.committee_id) = 1 THEN min(link.committee_id)
          ELSE NULL
        END AS committee_id,
        max(summary.election_year) AS election_year,
        CASE WHEN count(summary.total_receipts) = 0 THEN NULL ELSE sum(summary.total_receipts) END AS total_receipts,
        CASE
          WHEN count(summary.direct_contribution_total) = 0 THEN NULL
          ELSE sum(summary.direct_contribution_total)
        END AS direct_contribution_total,
        min(summary.source_url) FILTER (WHERE summary.source_url IS NOT NULL) AS source_url,
        max(summary.last_synced_at)::text AS last_synced_at
      FROM requested
      JOIN public.ne_candidate_finance_links AS link
        ON link.candidate_id = requested.candidate_id
       AND link.election_id = requested.election_id
       AND link.link_status = 'active'
      JOIN public.ne_candidate_finance_summaries AS summary
        ON summary.link_id = link.id
       AND summary.election_year = link.election_year
      GROUP BY requested.candidate_id, requested.election_id
    `,
    [JSON.stringify(requests)]
  );

  if (summaryResult.rows.length === 0) {
    return new Map();
  }

  const selectedRequests = summaryResult.rows.map((row) => ({
    candidate_id: row.candidate_id,
    election_id: row.election_id,
  }));

  const directBreakdownResult = await db.query<NebraskaFinanceDirectBreakdownRow>(
    `
      WITH selected AS (
        SELECT
          candidate_id::uuid AS candidate_id,
          election_id::uuid AS election_id
        FROM jsonb_to_recordset($1::jsonb) AS x(
          candidate_id text,
          election_id text
        )
      ),
      grouped AS (
        SELECT
          selected.candidate_id::text AS candidate_id,
          selected.election_id::text AS election_id,
          breakdown.category_type,
          breakdown.category_name,
          sum(breakdown.amount) AS amount,
          CASE
            WHEN count(breakdown.contributor_count) = 0 THEN NULL
            ELSE sum(breakdown.contributor_count)
          END AS contributor_count,
          min(breakdown.source_url) FILTER (WHERE breakdown.source_url IS NOT NULL) AS source_url
        FROM selected
        JOIN public.ne_candidate_finance_links AS link
          ON link.candidate_id = selected.candidate_id
         AND link.election_id = selected.election_id
         AND link.link_status = 'active'
        JOIN public.ne_candidate_finance_direct_breakdowns AS breakdown
          ON breakdown.link_id = link.id
         AND breakdown.election_year = link.election_year
        WHERE breakdown.category_type IN ('occupation', 'industry')
        GROUP BY selected.candidate_id, selected.election_id, breakdown.category_type, breakdown.category_name
      ),
      ranked AS (
        SELECT
          *,
          row_number() OVER (
            PARTITION BY candidate_id, election_id, category_type
            ORDER BY amount DESC, category_name ASC
          ) AS rn
        FROM grouped
      )
      SELECT candidate_id, election_id, category_type, category_name, amount, contributor_count, source_url
      FROM ranked
      WHERE rn <= 5
      ORDER BY candidate_id, election_id, category_type, amount DESC, category_name ASC
    `,
    [JSON.stringify(selectedRequests)]
  );

  const directOccupationsByCandidateElection = new Map<string, BallotLookupFinanceBreakdown[]>();
  const directIndustriesByCandidateElection = new Map<string, BallotLookupFinanceBreakdown[]>();
  const summaryByCandidateElection = new Map(
    summaryResult.rows.map((row) => [candidateElectionKey(row.candidate_id, row.election_id), row])
  );
  for (const row of directBreakdownResult.rows) {
    const summary = summaryByCandidateElection.get(candidateElectionKey(row.candidate_id, row.election_id));
    const mapped = mapFinanceBreakdown(row, summary?.source_url ?? GENERIC_NEBRASKA_NADC_SOURCE_URL);
    if (row.category_type === "occupation") {
      addFinanceBreakdown(directOccupationsByCandidateElection, row.candidate_id, row.election_id, mapped);
    } else if (row.category_type === "industry") {
      addFinanceBreakdown(directIndustriesByCandidateElection, row.candidate_id, row.election_id, mapped);
    }
  }

  return new Map(
    summaryResult.rows.map((row) => {
      const key = candidateElectionKey(row.candidate_id, row.election_id);
      const topDirectDonorOccupations = directOccupationsByCandidateElection.get(key) ?? [];
      return [
        key,
        {
          source: "NEBRASKA_NADC",
          cycle: row.election_year,
          fec_candidate_id: null,
          controlled_committee_id: row.committee_id,
          last_synced_at: row.last_synced_at,
          direct_campaign: {
            total_raised: parseFinanceAmount(row.total_receipts),
            total_spent: null,
            cash_on_hand: null,
            debts_owed: null,
            top_occupations: topDirectDonorOccupations,
            top_employers: [],
            top_industries: directIndustriesByCandidateElection.get(key) ?? [],
          },
          outside_spending: {
            support_total: null,
            oppose_total: null,
            top_supporting_groups: [],
            top_opposing_groups: [],
            top_supporting_industries: [],
            top_opposing_industries: [],
          },
          backing_summary: {
            top_direct_donor_occupations: topDirectDonorOccupations,
            top_outside_supporting_industries: [],
          },
        } satisfies BallotLookupFinanceSummary,
      ];
    })
  );
}


async function loadOklahomaCandidateFinanceSummariesByCandidateElection(
  db: Queryable,
  candidateRows: readonly CandidateRow[],
  electionRows: readonly ElectionRow[]
): Promise<Map<string, BallotLookupFinanceSummary>> {
  if (!isOklahomaCampaignFinanceEnabled()) {
    return new Map();
  }

  const requests = buildOklahomaFinanceSummaryRequests(candidateRows, electionRows);
  if (requests.length === 0) {
    return new Map();
  }

  const summaryResult = await db.query<OklahomaFinanceSummaryRow>(
    `
      WITH requested AS (
        SELECT
          candidate_id::uuid AS candidate_id,
          election_id::uuid AS election_id
        FROM jsonb_to_recordset($1::jsonb) AS x(
          candidate_id text,
          election_id text
        )
      )
      SELECT
        requested.candidate_id::text AS candidate_id,
        requested.election_id::text AS election_id,
        CASE
          WHEN count(DISTINCT link.committee_id) = 1 THEN min(link.committee_id)
          ELSE NULL
        END AS committee_id,
        max(summary.election_year) AS election_year,
        CASE WHEN count(summary.total_receipts) = 0 THEN NULL ELSE sum(summary.total_receipts) END AS total_receipts,
        CASE
          WHEN count(summary.direct_contribution_total) = 0 THEN NULL
          ELSE sum(summary.direct_contribution_total)
        END AS direct_contribution_total,
        min(summary.source_url) FILTER (WHERE summary.source_url IS NOT NULL) AS source_url,
        max(summary.last_synced_at)::text AS last_synced_at
      FROM requested
      JOIN public.ok_candidate_finance_links AS link
        ON link.candidate_id = requested.candidate_id
       AND link.election_id = requested.election_id
       AND link.link_status = 'active'
      JOIN public.ok_candidate_finance_summaries AS summary
        ON summary.link_id = link.id
       AND summary.election_year = link.election_year
      GROUP BY requested.candidate_id, requested.election_id
    `,
    [JSON.stringify(requests)]
  );

  if (summaryResult.rows.length === 0) {
    return new Map();
  }

  const selectedRequests = summaryResult.rows.map((row) => ({
    candidate_id: row.candidate_id,
    election_id: row.election_id,
  }));

  const directBreakdownResult = await db.query<OklahomaFinanceDirectBreakdownRow>(
    `
      WITH selected AS (
        SELECT
          candidate_id::uuid AS candidate_id,
          election_id::uuid AS election_id
        FROM jsonb_to_recordset($1::jsonb) AS x(
          candidate_id text,
          election_id text
        )
      ),
      grouped AS (
        SELECT
          selected.candidate_id::text AS candidate_id,
          selected.election_id::text AS election_id,
          breakdown.category_type,
          breakdown.category_name,
          sum(breakdown.amount) AS amount,
          CASE
            WHEN count(breakdown.contributor_count) = 0 THEN NULL
            ELSE sum(breakdown.contributor_count)
          END AS contributor_count,
          min(breakdown.source_url) FILTER (WHERE breakdown.source_url IS NOT NULL) AS source_url
        FROM selected
        JOIN public.ok_candidate_finance_links AS link
          ON link.candidate_id = selected.candidate_id
         AND link.election_id = selected.election_id
         AND link.link_status = 'active'
        JOIN public.ok_candidate_finance_direct_breakdowns AS breakdown
          ON breakdown.link_id = link.id
         AND breakdown.election_year = link.election_year
        WHERE breakdown.category_type IN ('occupation', 'contribution_size')
        GROUP BY selected.candidate_id, selected.election_id, breakdown.category_type, breakdown.category_name
      ),
      ranked AS (
        SELECT
          *,
          row_number() OVER (
            PARTITION BY candidate_id, election_id, category_type
            ORDER BY amount DESC, category_name ASC
          ) AS rn
        FROM grouped
      )
      SELECT candidate_id, election_id, category_type, category_name, amount, contributor_count, source_url
      FROM ranked
      WHERE rn <= 5
      ORDER BY candidate_id, election_id, category_type, amount DESC, category_name ASC
    `,
    [JSON.stringify(selectedRequests)]
  );

  const directOccupationsByCandidateElection = new Map<string, BallotLookupFinanceBreakdown[]>();
  const contributionSizeBucketsByCandidateElection = new Map<string, BallotLookupFinanceBreakdown[]>();
  const summaryByCandidateElection = new Map(
    summaryResult.rows.map((row) => [candidateElectionKey(row.candidate_id, row.election_id), row])
  );
  for (const row of directBreakdownResult.rows) {
    const summary = summaryByCandidateElection.get(candidateElectionKey(row.candidate_id, row.election_id));
    const mapped = mapFinanceBreakdown(row, summary?.source_url ?? GENERIC_OKLAHOMA_GUARDIAN_SOURCE_URL);
    if (row.category_type === "occupation") {
      addFinanceBreakdown(directOccupationsByCandidateElection, row.candidate_id, row.election_id, mapped);
    } else if (row.category_type === "contribution_size") {
      addFinanceBreakdown(contributionSizeBucketsByCandidateElection, row.candidate_id, row.election_id, mapped);
    }
  }

  return new Map(
    summaryResult.rows.map((row) => {
      const key = candidateElectionKey(row.candidate_id, row.election_id);
      const topDirectDonorOccupations = directOccupationsByCandidateElection.get(key) ?? [];
      return [
        key,
        {
          source: "OKLAHOMA_GUARDIAN",
          cycle: row.election_year,
          fec_candidate_id: null,
          controlled_committee_id: row.committee_id,
          last_synced_at: row.last_synced_at,
          direct_campaign: {
            total_raised: parseFinanceAmount(row.total_receipts),
            total_spent: null,
            cash_on_hand: null,
            debts_owed: null,
            top_occupations: topDirectDonorOccupations,
            top_employers: [],
            top_industries: [],
            contribution_size_buckets: contributionSizeBucketsByCandidateElection.get(key) ?? [],
          },
          outside_spending: {
            support_total: null,
            oppose_total: null,
            top_supporting_groups: [],
            top_opposing_groups: [],
            top_supporting_industries: [],
            top_opposing_industries: [],
          },
          backing_summary: {
            top_direct_donor_occupations: topDirectDonorOccupations,
            top_outside_supporting_industries: [],
          },
        } satisfies BallotLookupFinanceSummary,
      ];
    })
  );
}

async function loadNewMexicoCandidateFinanceSummariesByCandidateElection(
  db: Queryable,
  candidateRows: readonly CandidateRow[],
  electionRows: readonly ElectionRow[]
): Promise<Map<string, BallotLookupFinanceSummary>> {
  if (!isNewMexicoCampaignFinanceEnabled()) {
    return new Map();
  }

  const requests = buildNewMexicoFinanceSummaryRequests(candidateRows, electionRows);
  if (requests.length === 0) {
    return new Map();
  }

  const summaryResult = await db.query<NewMexicoFinanceSummaryRow>(
    `
      WITH requested AS (
        SELECT
          candidate_id::uuid AS candidate_id,
          election_id::uuid AS election_id
        FROM jsonb_to_recordset($1::jsonb) AS x(
          candidate_id text,
          election_id text
        )
      )
      SELECT
        requested.candidate_id::text AS candidate_id,
        requested.election_id::text AS election_id,
        CASE
          WHEN count(DISTINCT link.committee_id) = 1 THEN min(link.committee_id)
          ELSE NULL
        END AS committee_id,
        max(summary.election_year) AS election_year,
        CASE WHEN count(summary.total_receipts) = 0 THEN NULL ELSE sum(summary.total_receipts) END AS total_receipts,
        CASE
          WHEN count(summary.direct_contribution_total) = 0 THEN NULL
          ELSE sum(summary.direct_contribution_total)
        END AS direct_contribution_total,
        CASE WHEN count(summary.total_disbursements) = 0 THEN NULL ELSE sum(summary.total_disbursements) END AS total_disbursements,
        max(summary.outside_support_total) AS outside_support_total,
        max(summary.outside_oppose_total) AS outside_oppose_total,
        min(summary.source_url) FILTER (WHERE summary.source_url IS NOT NULL) AS source_url,
        max(summary.last_synced_at)::text AS last_synced_at
      FROM requested
      JOIN public.nm_candidate_finance_links AS link
        ON link.candidate_id = requested.candidate_id
       AND link.election_id = requested.election_id
       AND link.link_status = 'active'
      JOIN public.nm_candidate_finance_summaries AS summary
        ON summary.link_id = link.id
       AND summary.election_year = link.election_year
      GROUP BY requested.candidate_id, requested.election_id
    `,
    [JSON.stringify(requests)]
  );

  if (summaryResult.rows.length === 0) {
    return new Map();
  }

  const selectedRequests = summaryResult.rows.map((row) => ({
    candidate_id: row.candidate_id,
    election_id: row.election_id,
  }));

  const directBreakdownResult = await db.query<NewMexicoFinanceDirectBreakdownRow>(
    `
      WITH selected AS (
        SELECT
          candidate_id::uuid AS candidate_id,
          election_id::uuid AS election_id
        FROM jsonb_to_recordset($1::jsonb) AS x(
          candidate_id text,
          election_id text
        )
      ),
      grouped AS (
        SELECT
          selected.candidate_id::text AS candidate_id,
          selected.election_id::text AS election_id,
          breakdown.category_type,
          breakdown.category_name,
          sum(breakdown.amount) AS amount,
          CASE
            WHEN count(breakdown.contributor_count) = 0 THEN NULL
            ELSE sum(breakdown.contributor_count)
          END AS contributor_count,
          min(breakdown.source_url) FILTER (WHERE breakdown.source_url IS NOT NULL) AS source_url
        FROM selected
        JOIN public.nm_candidate_finance_links AS link
          ON link.candidate_id = selected.candidate_id
         AND link.election_id = selected.election_id
         AND link.link_status = 'active'
        JOIN public.nm_candidate_finance_direct_breakdowns AS breakdown
          ON breakdown.link_id = link.id
         AND breakdown.election_year = link.election_year
        WHERE breakdown.category_type IN ('occupation', 'industry')
        GROUP BY selected.candidate_id, selected.election_id, breakdown.category_type, breakdown.category_name
      ),
      ranked AS (
        SELECT
          *,
          row_number() OVER (
            PARTITION BY candidate_id, election_id, category_type
            ORDER BY amount DESC, category_name ASC
          ) AS rn
        FROM grouped
      )
      SELECT candidate_id, election_id, category_type, category_name, amount, contributor_count, source_url
      FROM ranked
      WHERE rn <= 5
      ORDER BY candidate_id, election_id, category_type, amount DESC, category_name ASC
    `,
    [JSON.stringify(selectedRequests)]
  );

  const outsideGroupResult = await db.query<NewMexicoFinanceOutsideGroupRow>(
    `
      WITH selected AS (
        SELECT
          candidate_id::uuid AS candidate_id,
          election_id::uuid AS election_id
        FROM jsonb_to_recordset($1::jsonb) AS x(
          candidate_id text,
          election_id text
        )
      ),
      grouped AS (
        SELECT
          selected.candidate_id::text AS candidate_id,
          selected.election_id::text AS election_id,
          outside_group.committee_id,
          min(outside_group.committee_name) AS committee_name,
          outside_group.support_oppose,
          max(outside_group.amount) AS amount,
          min(outside_group.source_url) FILTER (WHERE outside_group.source_url IS NOT NULL) AS source_url
        FROM selected
        JOIN public.nm_candidate_finance_links AS link
          ON link.candidate_id = selected.candidate_id
         AND link.election_id = selected.election_id
         AND link.link_status = 'active'
        JOIN public.nm_candidate_finance_outside_groups AS outside_group
          ON outside_group.link_id = link.id
         AND outside_group.election_year = link.election_year
        GROUP BY selected.candidate_id, selected.election_id, outside_group.committee_id, outside_group.support_oppose
      ),
      ranked AS (
        SELECT
          *,
          row_number() OVER (
            PARTITION BY candidate_id, election_id, support_oppose
            ORDER BY amount DESC, committee_name ASC
          ) AS rn
        FROM grouped
      )
      SELECT candidate_id, election_id, committee_id, committee_name, support_oppose, amount, source_url
      FROM ranked
      WHERE rn <= 5
      ORDER BY candidate_id, election_id, support_oppose, amount DESC, committee_name ASC
    `,
    [JSON.stringify(selectedRequests)]
  );

  const outsideIndustryResult = await db.query<NewMexicoFinanceOutsideIndustryRow>(
    `
      WITH selected AS (
        SELECT
          candidate_id::uuid AS candidate_id,
          election_id::uuid AS election_id
        FROM jsonb_to_recordset($1::jsonb) AS x(
          candidate_id text,
          election_id text
        )
      ),
      per_group AS (
        SELECT
          selected.candidate_id::text AS candidate_id,
          selected.election_id::text AS election_id,
          breakdown.committee_id,
          breakdown.support_oppose,
          breakdown.category_name,
          max(breakdown.amount) AS amount,
          CASE
            WHEN count(breakdown.contributor_count) = 0 THEN NULL
            ELSE max(breakdown.contributor_count)
          END AS contributor_count,
          min(breakdown.source_url) FILTER (WHERE breakdown.source_url IS NOT NULL) AS source_url
        FROM selected
        JOIN public.nm_candidate_finance_links AS link
          ON link.candidate_id = selected.candidate_id
         AND link.election_id = selected.election_id
         AND link.link_status = 'active'
        JOIN public.nm_candidate_finance_outside_group_breakdowns AS breakdown
          ON breakdown.link_id = link.id
         AND breakdown.election_year = link.election_year
        WHERE breakdown.category_type = 'industry'
        GROUP BY
          selected.candidate_id,
          selected.election_id,
          breakdown.committee_id,
          breakdown.support_oppose,
          breakdown.category_name
      ),
      grouped AS (
        SELECT
          candidate_id,
          election_id,
          support_oppose,
          category_name,
          sum(amount) AS amount,
          CASE
            WHEN count(contributor_count) = 0 THEN NULL
            ELSE sum(contributor_count)
          END AS contributor_count,
          min(source_url) FILTER (WHERE source_url IS NOT NULL) AS source_url
        FROM per_group
        GROUP BY candidate_id, election_id, support_oppose, category_name
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

  const directOccupationsByCandidateElection = new Map<string, BallotLookupFinanceBreakdown[]>();
  const directIndustriesByCandidateElection = new Map<string, BallotLookupFinanceBreakdown[]>();
  const summaryByCandidateElection = new Map(
    summaryResult.rows.map((row) => [candidateElectionKey(row.candidate_id, row.election_id), row])
  );
  for (const row of directBreakdownResult.rows) {
    const summary = summaryByCandidateElection.get(candidateElectionKey(row.candidate_id, row.election_id));
    const mapped = mapFinanceBreakdown(row, summary?.source_url ?? GENERIC_NEW_MEXICO_CFIS_SOURCE_URL);
    if (row.category_type === "occupation") {
      addFinanceBreakdown(directOccupationsByCandidateElection, row.candidate_id, row.election_id, mapped);
    } else if (row.category_type === "industry") {
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
      source_url: firstNonEmptySourceUrl(row.source_url, GENERIC_NEW_MEXICO_CFIS_SOURCE_URL),
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
      mapFinanceBreakdown(row, GENERIC_NEW_MEXICO_CFIS_SOURCE_URL)
    );
  }

  return new Map(
    summaryResult.rows.map((row) => {
      const key = candidateElectionKey(row.candidate_id, row.election_id);
      const topDirectDonorOccupations = directOccupationsByCandidateElection.get(key) ?? [];
      const topOutsideSupportingIndustries = (supportingIndustriesByCandidateElection.get(key) ?? []).map(
        (industry): BallotLookupFinanceOutsideIndustrySupportSummary => ({
          ...industry,
          explanation: buildOutsideIndustrySupportExplanation(industry.category_name, []),
          supporting_organizations: [],
        })
      );
      return [
        key,
        {
          source: "NEW_MEXICO_CFIS",
          cycle: row.election_year,
          fec_candidate_id: null,
          controlled_committee_id: row.committee_id,
          last_synced_at: row.last_synced_at,
          direct_campaign: {
            total_raised: parseFinanceAmount(row.direct_contribution_total) ?? parseFinanceAmount(row.total_receipts),
            total_spent: parseFinanceAmount(row.total_disbursements),
            cash_on_hand: null,
            debts_owed: null,
            top_occupations: topDirectDonorOccupations,
            top_employers: [],
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

async function loadTexasCandidateFinanceSummariesByCandidateElection(
  db: Queryable,
  candidateRows: readonly CandidateRow[],
  electionRows: readonly ElectionRow[]
): Promise<Map<string, BallotLookupFinanceSummary>> {
  if (!isTexasCampaignFinanceEnabled()) {
    return new Map();
  }

  const requests = buildTexasFinanceSummaryRequests(candidateRows, electionRows);
  if (requests.length === 0) {
    return new Map();
  }

  const summaryResult = await db.query<TexasFinanceSummaryRow>(
    `
      WITH requested AS (
        SELECT
          candidate_id::uuid AS candidate_id,
          election_id::uuid AS election_id
        FROM jsonb_to_recordset($1::jsonb) AS x(
          candidate_id text,
          election_id text
        )
      )
      SELECT
        requested.candidate_id::text AS candidate_id,
        requested.election_id::text AS election_id,
        CASE
          WHEN count(DISTINCT link.committee_id) = 1 THEN min(link.committee_id)
          ELSE NULL
        END AS committee_id,
        max(summary.election_year) AS election_year,
        CASE WHEN count(summary.total_receipts) = 0 THEN NULL ELSE sum(summary.total_receipts) END AS total_receipts,
        CASE
          WHEN count(summary.direct_contribution_total) = 0 THEN NULL
          ELSE sum(summary.direct_contribution_total)
        END AS direct_contribution_total,
        CASE WHEN count(summary.total_disbursements) = 0 THEN NULL ELSE sum(summary.total_disbursements) END AS total_disbursements,
        CASE WHEN count(summary.cash_on_hand) = 0 THEN NULL ELSE sum(summary.cash_on_hand) END AS cash_on_hand,
        max(summary.outside_support_total) AS outside_support_total,
        max(summary.outside_oppose_total) AS outside_oppose_total,
        min(summary.source_url) FILTER (WHERE summary.source_url IS NOT NULL) AS source_url,
        max(summary.last_synced_at)::text AS last_synced_at
      FROM requested
      JOIN public.tx_candidate_finance_links AS link
        ON link.candidate_id = requested.candidate_id
       AND link.election_id = requested.election_id
       AND link.link_status = 'active'
      JOIN public.tx_candidate_finance_summaries AS summary
        ON summary.link_id = link.id
       AND summary.election_year = link.election_year
      GROUP BY requested.candidate_id, requested.election_id
    `,
    [JSON.stringify(requests)]
  );

  if (summaryResult.rows.length === 0) {
    return new Map();
  }

  const selectedRequests = summaryResult.rows.map((row) => ({
    candidate_id: row.candidate_id,
    election_id: row.election_id,
  }));

  const directBreakdownResult = await db.query<TexasFinanceDirectBreakdownRow>(
    `
      WITH selected AS (
        SELECT
          candidate_id::uuid AS candidate_id,
          election_id::uuid AS election_id
        FROM jsonb_to_recordset($1::jsonb) AS x(
          candidate_id text,
          election_id text
        )
      ),
      grouped AS (
        SELECT
          selected.candidate_id::text AS candidate_id,
          selected.election_id::text AS election_id,
          breakdown.category_type,
          breakdown.category_name,
          sum(breakdown.amount) AS amount,
          CASE
            WHEN count(breakdown.contributor_count) = 0 THEN NULL
            ELSE sum(breakdown.contributor_count)
          END AS contributor_count,
          min(breakdown.source_url) FILTER (WHERE breakdown.source_url IS NOT NULL) AS source_url
        FROM selected
        JOIN public.tx_candidate_finance_links AS link
          ON link.candidate_id = selected.candidate_id
         AND link.election_id = selected.election_id
         AND link.link_status = 'active'
        JOIN public.tx_candidate_finance_direct_breakdowns AS breakdown
          ON breakdown.link_id = link.id
         AND breakdown.election_year = link.election_year
        WHERE breakdown.category_type IN ('occupation', 'contribution_size')
        GROUP BY selected.candidate_id, selected.election_id, breakdown.category_type, breakdown.category_name
      ),
      ranked AS (
        SELECT
          *,
          row_number() OVER (
            PARTITION BY candidate_id, election_id, category_type
            ORDER BY amount DESC, category_name ASC
          ) AS rn
        FROM grouped
      )
      SELECT candidate_id, election_id, category_type, category_name, amount, contributor_count, source_url
      FROM ranked
      WHERE rn <= 5
      ORDER BY candidate_id, election_id, category_type, amount DESC, category_name ASC
    `,
    [JSON.stringify(selectedRequests)]
  );

  const outsideGroupResult = await db.query<TexasFinanceOutsideGroupRow>(
    `
      WITH selected AS (
        SELECT
          candidate_id::uuid AS candidate_id,
          election_id::uuid AS election_id
        FROM jsonb_to_recordset($1::jsonb) AS x(
          candidate_id text,
          election_id text
        )
      ),
      grouped AS (
        SELECT
          selected.candidate_id::text AS candidate_id,
          selected.election_id::text AS election_id,
          outside_group.committee_id,
          min(outside_group.committee_name) AS committee_name,
          outside_group.support_oppose,
          max(outside_group.amount) AS amount,
          min(outside_group.source_url) FILTER (WHERE outside_group.source_url IS NOT NULL) AS source_url
        FROM selected
        JOIN public.tx_candidate_finance_links AS link
          ON link.candidate_id = selected.candidate_id
         AND link.election_id = selected.election_id
         AND link.link_status = 'active'
        JOIN public.tx_candidate_finance_outside_groups AS outside_group
          ON outside_group.link_id = link.id
         AND outside_group.election_year = link.election_year
        GROUP BY selected.candidate_id, selected.election_id, outside_group.committee_id, outside_group.support_oppose
      ),
      ranked AS (
        SELECT
          *,
          row_number() OVER (
            PARTITION BY candidate_id, election_id, support_oppose
            ORDER BY amount DESC, committee_name ASC
          ) AS rn
        FROM grouped
      )
      SELECT candidate_id, election_id, committee_id, committee_name, support_oppose, amount, source_url
      FROM ranked
      WHERE rn <= 5
      ORDER BY candidate_id, election_id, support_oppose, amount DESC, committee_name ASC
    `,
    [JSON.stringify(selectedRequests)]
  );

  const outsideIndustryResult = await db.query<TexasFinanceOutsideIndustryRow>(
    `
      WITH selected AS (
        SELECT
          candidate_id::uuid AS candidate_id,
          election_id::uuid AS election_id
        FROM jsonb_to_recordset($1::jsonb) AS x(
          candidate_id text,
          election_id text
        )
      ),
      per_group AS (
        SELECT
          selected.candidate_id::text AS candidate_id,
          selected.election_id::text AS election_id,
          breakdown.committee_id,
          breakdown.support_oppose,
          breakdown.category_name,
          max(breakdown.amount) AS amount,
          CASE
            WHEN count(breakdown.contributor_count) = 0 THEN NULL
            ELSE max(breakdown.contributor_count)
          END AS contributor_count,
          min(breakdown.source_url) FILTER (WHERE breakdown.source_url IS NOT NULL) AS source_url
        FROM selected
        JOIN public.tx_candidate_finance_links AS link
          ON link.candidate_id = selected.candidate_id
         AND link.election_id = selected.election_id
         AND link.link_status = 'active'
        JOIN public.tx_candidate_finance_outside_group_breakdowns AS breakdown
          ON breakdown.link_id = link.id
         AND breakdown.election_year = link.election_year
        WHERE breakdown.category_type = 'industry'
        GROUP BY
          selected.candidate_id,
          selected.election_id,
          breakdown.committee_id,
          breakdown.support_oppose,
          breakdown.category_name
      ),
      grouped AS (
        SELECT
          candidate_id,
          election_id,
          support_oppose,
          category_name,
          sum(amount) AS amount,
          CASE
            WHEN count(contributor_count) = 0 THEN NULL
            ELSE sum(contributor_count)
          END AS contributor_count,
          min(source_url) FILTER (WHERE source_url IS NOT NULL) AS source_url
        FROM per_group
        GROUP BY candidate_id, election_id, support_oppose, category_name
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

  const outsideDonorEvidenceResult = await db.query<TexasFinanceOutsideDonorEvidenceRow>(
    `
      WITH selected AS (
        SELECT
          candidate_id::uuid AS candidate_id,
          election_id::uuid AS election_id
        FROM jsonb_to_recordset($1::jsonb) AS x(
          candidate_id text,
          election_id text
        )
      ),
      top_industries_per_group AS (
        SELECT
          selected.candidate_id::text AS candidate_id,
          selected.election_id::text AS election_id,
          industry.committee_id,
          industry.category_name AS industry_name,
          max(industry.amount) AS amount
        FROM selected
        JOIN public.tx_candidate_finance_links AS link
          ON link.candidate_id = selected.candidate_id
         AND link.election_id = selected.election_id
         AND link.link_status = 'active'
        JOIN public.tx_candidate_finance_outside_group_breakdowns AS industry
          ON industry.link_id = link.id
         AND industry.election_year = link.election_year
        WHERE industry.support_oppose = 'support'
          AND industry.category_type = 'industry'
        GROUP BY selected.candidate_id, selected.election_id, industry.committee_id, industry.category_name
      ),
      top_industries_grouped AS (
        SELECT
          candidate_id,
          election_id,
          industry_name,
          sum(amount) AS amount
        FROM top_industries_per_group
        GROUP BY candidate_id, election_id, industry_name
      ),
      top_industries AS (
        SELECT candidate_id, election_id, industry_name
        FROM (
          SELECT
            *,
            row_number() OVER (
              PARTITION BY candidate_id, election_id
              ORDER BY amount DESC, industry_name ASC
            ) AS rn
          FROM top_industries_grouped
        ) ranked_industries
        WHERE rn <= 5
      ),
      evidence AS (
        SELECT
          selected.candidate_id::text AS candidate_id,
          selected.election_id::text AS election_id,
          top_industries.industry_name,
          breakdown.committee_id,
          COALESCE(outside_group.committee_name, breakdown.committee_id) AS committee_name,
          breakdown.support_oppose,
          breakdown.category_name AS organization_name,
          breakdown.amount,
          breakdown.contributor_count,
          COALESCE(breakdown.source_url, outside_group.source_url) AS source_url,
          row_number() OVER (
            PARTITION BY selected.candidate_id, selected.election_id, top_industries.industry_name
            ORDER BY breakdown.amount DESC, breakdown.category_name ASC, breakdown.committee_id ASC
          ) AS rn
        FROM selected
        JOIN top_industries
          ON top_industries.candidate_id = selected.candidate_id::text
         AND top_industries.election_id = selected.election_id::text
        JOIN public.tx_candidate_finance_links AS link
          ON link.candidate_id = selected.candidate_id
         AND link.election_id = selected.election_id
         AND link.link_status = 'active'
        JOIN public.tx_candidate_finance_outside_group_breakdowns AS breakdown
          ON breakdown.link_id = link.id
         AND breakdown.election_year = link.election_year
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
          ON classification.label_type = 'donor'
         AND classification.normalized_label = normalized_breakdown.normalized_label
         AND classification.industry_slug = top_industries.industry_name
        LEFT JOIN public.tx_candidate_finance_outside_groups AS outside_group
          ON outside_group.link_id = breakdown.link_id
         AND outside_group.election_year = breakdown.election_year
         AND outside_group.committee_id = breakdown.committee_id
         AND outside_group.support_oppose = breakdown.support_oppose
        WHERE breakdown.category_type = 'donor'
          AND breakdown.support_oppose = 'support'
      )
      SELECT candidate_id, election_id, industry_name, committee_id, committee_name, support_oppose, organization_name, amount, contributor_count, source_url
      FROM evidence
      WHERE rn <= 3
      ORDER BY candidate_id, election_id, industry_name, amount DESC, organization_name ASC
    `,
    [JSON.stringify(selectedRequests)]
  );

  const directOccupationsByCandidateElection = new Map<string, BallotLookupFinanceBreakdown[]>();
  const contributionSizeBucketsByCandidateElection = new Map<string, BallotLookupFinanceBreakdown[]>();
  const summaryByCandidateElection = new Map(
    summaryResult.rows.map((row) => [candidateElectionKey(row.candidate_id, row.election_id), row])
  );
  for (const row of directBreakdownResult.rows) {
    const summary = summaryByCandidateElection.get(candidateElectionKey(row.candidate_id, row.election_id));
    const targetMap =
      row.category_type === "contribution_size"
        ? contributionSizeBucketsByCandidateElection
        : directOccupationsByCandidateElection;
    addFinanceBreakdown(
      targetMap,
      row.candidate_id,
      row.election_id,
      mapFinanceBreakdown(row, summary?.source_url ?? GENERIC_TEXAS_TEC_SOURCE_URL)
    );
  }

  const supportingGroupsByCandidateElection = new Map<string, BallotLookupFinanceOutsideGroup[]>();
  const opposingGroupsByCandidateElection = new Map<string, BallotLookupFinanceOutsideGroup[]>();
  const outsideGroupNameByCandidateElectionCommittee = new Map<string, string>();
  for (const row of outsideGroupResult.rows) {
    const key = candidateElectionKey(row.candidate_id, row.election_id);
    const map = row.support_oppose === "support" ? supportingGroupsByCandidateElection : opposingGroupsByCandidateElection;
    const list = map.get(key) ?? [];
    list.push({
      committee_id: row.committee_id,
      committee_name: row.committee_name,
      support_oppose: row.support_oppose,
      amount: parseFinanceAmount(row.amount) ?? 0,
      source_url: firstNonEmptySourceUrl(row.source_url, GENERIC_TEXAS_TEC_SOURCE_URL),
    });
    map.set(key, list);
    outsideGroupNameByCandidateElectionCommittee.set(`${key}\u0000${row.committee_id}\u0000${row.support_oppose}`, row.committee_name);
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
      mapFinanceBreakdown(row, GENERIC_TEXAS_TEC_SOURCE_URL)
    );
  }

  const outsideIndustryEvidenceByCandidateElectionAndIndustry = new Map<
    string,
    BallotLookupFinanceOutsideIndustrySupportEvidence[]
  >();
  for (const row of outsideDonorEvidenceResult.rows) {
    const candidateKey = candidateElectionKey(row.candidate_id, row.election_id);
    const evidenceKey = `${candidateKey}\u0000${row.industry_name}`;
    const list = outsideIndustryEvidenceByCandidateElectionAndIndustry.get(evidenceKey) ?? [];
    list.push({
      organization_name: row.organization_name,
      organization_type: "donor",
      amount: parseFinanceAmount(row.amount) ?? 0,
      contributor_count: parseFinanceCount(row.contributor_count),
      committee_id: row.committee_id,
      committee_name:
        outsideGroupNameByCandidateElectionCommittee.get(`${candidateKey}\u0000${row.committee_id}\u0000${row.support_oppose}`) ??
        row.committee_name,
      source_url: firstNonEmptySourceUrl(row.source_url, GENERIC_TEXAS_TEC_SOURCE_URL),
    });
    outsideIndustryEvidenceByCandidateElectionAndIndustry.set(
      evidenceKey,
      list.sort((left, right) => right.amount - left.amount || left.organization_name.localeCompare(right.organization_name)).slice(0, 5)
    );
  }

  return new Map(
    summaryResult.rows.map((row) => {
      const key = candidateElectionKey(row.candidate_id, row.election_id);
      const topDirectDonorOccupations = directOccupationsByCandidateElection.get(key) ?? [];
      const contributionSizeBuckets = contributionSizeBucketsByCandidateElection.get(key) ?? [];
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
          source: "TEXAS_TEC",
          cycle: row.election_year,
          fec_candidate_id: null,
          controlled_committee_id: row.committee_id,
          last_synced_at: row.last_synced_at,
          direct_campaign: {
            total_raised: parseFinanceAmount(row.direct_contribution_total) ?? parseFinanceAmount(row.total_receipts),
            total_spent: parseFinanceAmount(row.total_disbursements),
            cash_on_hand: parseFinanceAmount(row.cash_on_hand),
            debts_owed: null,
            top_occupations: topDirectDonorOccupations,
            top_employers: [],
            top_industries: [],
            contribution_size_buckets: contributionSizeBuckets,
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

async function loadWashingtonCandidateFinanceSummariesByCandidateElection(
  db: Queryable,
  candidateRows: readonly CandidateRow[],
  electionRows: readonly ElectionRow[]
): Promise<Map<string, BallotLookupFinanceSummary>> {
  if (!isWashingtonCampaignFinanceEnabled()) {
    return new Map();
  }

  const requests = buildWashingtonFinanceSummaryRequests(candidateRows, electionRows);
  if (requests.length === 0) {
    return new Map();
  }

  const summaryResult = await db.query<WashingtonFinanceSummaryRow>(
    `
      WITH requested AS (
        SELECT
          candidate_id::uuid AS candidate_id,
          election_id::uuid AS election_id
        FROM jsonb_to_recordset($1::jsonb) AS x(
          candidate_id text,
          election_id text
        )
      )
      SELECT
        requested.candidate_id::text AS candidate_id,
        requested.election_id::text AS election_id,
        CASE
          WHEN count(DISTINCT link.committee_id) = 1 THEN min(link.committee_id)
          ELSE NULL
        END AS committee_id,
        max(summary.election_year) AS election_year,
        CASE WHEN count(summary.total_receipts) = 0 THEN NULL ELSE sum(summary.total_receipts) END AS total_receipts,
        CASE
          WHEN count(summary.direct_contribution_total) = 0 THEN NULL
          ELSE sum(summary.direct_contribution_total)
        END AS direct_contribution_total,
        CASE WHEN count(summary.total_disbursements) = 0 THEN NULL ELSE sum(summary.total_disbursements) END AS total_disbursements,
        CASE WHEN count(summary.cash_on_hand) = 0 THEN NULL ELSE sum(summary.cash_on_hand) END AS cash_on_hand,
        max(summary.outside_support_total) AS outside_support_total,
        max(summary.outside_oppose_total) AS outside_oppose_total,
        min(summary.source_url) FILTER (WHERE summary.source_url IS NOT NULL) AS source_url,
        max(summary.last_synced_at)::text AS last_synced_at
      FROM requested
      JOIN public.wa_candidate_finance_links AS link
        ON link.candidate_id = requested.candidate_id
       AND link.election_id = requested.election_id
       AND link.link_status = 'active'
      JOIN public.wa_candidate_finance_summaries AS summary
        ON summary.link_id = link.id
       AND summary.election_year = link.election_year
      GROUP BY requested.candidate_id, requested.election_id
    `,
    [JSON.stringify(requests)]
  );

  if (summaryResult.rows.length === 0) {
    return new Map();
  }

  const selectedRequests = summaryResult.rows.map((row) => ({
    candidate_id: row.candidate_id,
    election_id: row.election_id,
  }));

  const directBreakdownResult = await db.query<WashingtonFinanceDirectBreakdownRow>(
    `
      WITH selected AS (
        SELECT
          candidate_id::uuid AS candidate_id,
          election_id::uuid AS election_id
        FROM jsonb_to_recordset($1::jsonb) AS x(
          candidate_id text,
          election_id text
        )
      ),
      grouped AS (
        SELECT
          selected.candidate_id::text AS candidate_id,
          selected.election_id::text AS election_id,
          breakdown.category_type,
          breakdown.category_name,
          sum(breakdown.amount) AS amount,
          CASE
            WHEN count(breakdown.contributor_count) = 0 THEN NULL
            ELSE sum(breakdown.contributor_count)
          END AS contributor_count,
          min(breakdown.source_url) FILTER (WHERE breakdown.source_url IS NOT NULL) AS source_url
        FROM selected
        JOIN public.wa_candidate_finance_links AS link
          ON link.candidate_id = selected.candidate_id
         AND link.election_id = selected.election_id
         AND link.link_status = 'active'
        JOIN public.wa_candidate_finance_direct_breakdowns AS breakdown
          ON breakdown.link_id = link.id
         AND breakdown.election_year = link.election_year
        WHERE breakdown.category_type IN ('occupation', 'contribution_size')
        GROUP BY selected.candidate_id, selected.election_id, breakdown.category_type, breakdown.category_name
      ),
      ranked AS (
        SELECT
          *,
          row_number() OVER (
            PARTITION BY candidate_id, election_id, category_type
            ORDER BY amount DESC, category_name ASC
          ) AS rn
        FROM grouped
      )
      SELECT candidate_id, election_id, category_type, category_name, amount, contributor_count, source_url
      FROM ranked
      WHERE rn <= 5
      ORDER BY candidate_id, election_id, category_type, amount DESC, category_name ASC
    `,
    [JSON.stringify(selectedRequests)]
  );

  const outsideGroupResult = await db.query<WashingtonFinanceOutsideGroupRow>(
    `
      WITH selected AS (
        SELECT
          candidate_id::uuid AS candidate_id,
          election_id::uuid AS election_id
        FROM jsonb_to_recordset($1::jsonb) AS x(
          candidate_id text,
          election_id text
        )
      ),
      grouped AS (
        SELECT
          selected.candidate_id::text AS candidate_id,
          selected.election_id::text AS election_id,
          outside_group.sponsor_id AS committee_id,
          min(outside_group.sponsor_name) AS committee_name,
          outside_group.support_oppose,
          max(outside_group.amount) AS amount,
          min(outside_group.source_url) FILTER (WHERE outside_group.source_url IS NOT NULL) AS source_url
        FROM selected
        JOIN public.wa_candidate_finance_links AS link
          ON link.candidate_id = selected.candidate_id
         AND link.election_id = selected.election_id
         AND link.link_status = 'active'
        JOIN public.wa_candidate_finance_outside_groups AS outside_group
          ON outside_group.link_id = link.id
         AND outside_group.election_year = link.election_year
        GROUP BY selected.candidate_id, selected.election_id, outside_group.sponsor_id, outside_group.support_oppose
      ),
      ranked AS (
        SELECT
          *,
          row_number() OVER (
            PARTITION BY candidate_id, election_id, support_oppose
            ORDER BY amount DESC, committee_name ASC
          ) AS rn
        FROM grouped
      )
      SELECT candidate_id, election_id, committee_id, committee_name, support_oppose, amount, source_url
      FROM ranked
      WHERE rn <= 5
      ORDER BY candidate_id, election_id, support_oppose, amount DESC, committee_name ASC
    `,
    [JSON.stringify(selectedRequests)]
  );

  const outsideIndustryResult = await db.query<WashingtonFinanceOutsideIndustryRow>(
    `
      WITH selected AS (
        SELECT
          candidate_id::uuid AS candidate_id,
          election_id::uuid AS election_id
        FROM jsonb_to_recordset($1::jsonb) AS x(
          candidate_id text,
          election_id text
        )
      ),
      per_group AS (
        SELECT
          selected.candidate_id::text AS candidate_id,
          selected.election_id::text AS election_id,
          breakdown.sponsor_id AS committee_id,
          breakdown.support_oppose,
          breakdown.category_name,
          max(breakdown.amount) AS amount,
          CASE
            WHEN count(breakdown.contributor_count) = 0 THEN NULL
            ELSE max(breakdown.contributor_count)
          END AS contributor_count,
          min(breakdown.source_url) FILTER (WHERE breakdown.source_url IS NOT NULL) AS source_url
        FROM selected
        JOIN public.wa_candidate_finance_links AS link
          ON link.candidate_id = selected.candidate_id
         AND link.election_id = selected.election_id
         AND link.link_status = 'active'
        JOIN public.wa_candidate_finance_outside_group_breakdowns AS breakdown
          ON breakdown.link_id = link.id
         AND breakdown.election_year = link.election_year
        WHERE breakdown.category_type = 'industry'
        GROUP BY
          selected.candidate_id,
          selected.election_id,
          breakdown.sponsor_id,
          breakdown.support_oppose,
          breakdown.category_name
      ),
      grouped AS (
        SELECT
          candidate_id,
          election_id,
          support_oppose,
          category_name,
          sum(amount) AS amount,
          CASE
            WHEN count(contributor_count) = 0 THEN NULL
            ELSE sum(contributor_count)
          END AS contributor_count,
          min(source_url) FILTER (WHERE source_url IS NOT NULL) AS source_url
        FROM per_group
        GROUP BY candidate_id, election_id, support_oppose, category_name
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

  const outsideDonorEvidenceResult = await db.query<WashingtonFinanceOutsideDonorEvidenceRow>(
    `
      WITH selected AS (
        SELECT
          candidate_id::uuid AS candidate_id,
          election_id::uuid AS election_id
        FROM jsonb_to_recordset($1::jsonb) AS x(
          candidate_id text,
          election_id text
        )
      ),
      top_industries_per_group AS (
        SELECT
          selected.candidate_id::text AS candidate_id,
          selected.election_id::text AS election_id,
          industry.sponsor_id,
          industry.category_name AS industry_name,
          max(industry.amount) AS amount
        FROM selected
        JOIN public.wa_candidate_finance_links AS link
          ON link.candidate_id = selected.candidate_id
         AND link.election_id = selected.election_id
         AND link.link_status = 'active'
        JOIN public.wa_candidate_finance_outside_group_breakdowns AS industry
          ON industry.link_id = link.id
         AND industry.election_year = link.election_year
        WHERE industry.support_oppose = 'support'
          AND industry.category_type = 'industry'
        GROUP BY selected.candidate_id, selected.election_id, industry.sponsor_id, industry.category_name
      ),
      top_industries_grouped AS (
        SELECT
          candidate_id,
          election_id,
          industry_name,
          sum(amount) AS amount
        FROM top_industries_per_group
        GROUP BY candidate_id, election_id, industry_name
      ),
      top_industries AS (
        SELECT candidate_id, election_id, industry_name
        FROM (
          SELECT
            *,
            row_number() OVER (
              PARTITION BY candidate_id, election_id
              ORDER BY amount DESC, industry_name ASC
            ) AS rn
          FROM top_industries_grouped
        ) ranked_industries
        WHERE rn <= 5
      ),
      evidence AS (
        SELECT
          selected.candidate_id::text AS candidate_id,
          selected.election_id::text AS election_id,
          top_industries.industry_name,
          breakdown.sponsor_id AS committee_id,
          COALESCE(outside_group.sponsor_name, breakdown.sponsor_id) AS committee_name,
          breakdown.support_oppose,
          breakdown.category_name AS organization_name,
          breakdown.amount,
          breakdown.contributor_count,
          COALESCE(breakdown.source_url, outside_group.source_url) AS source_url,
          row_number() OVER (
            PARTITION BY selected.candidate_id, selected.election_id, top_industries.industry_name
            ORDER BY breakdown.amount DESC, breakdown.category_name ASC, breakdown.sponsor_id ASC
          ) AS rn
        FROM selected
        JOIN top_industries
          ON top_industries.candidate_id = selected.candidate_id::text
         AND top_industries.election_id = selected.election_id::text
        JOIN public.wa_candidate_finance_links AS link
          ON link.candidate_id = selected.candidate_id
         AND link.election_id = selected.election_id
         AND link.link_status = 'active'
        JOIN public.wa_candidate_finance_outside_group_breakdowns AS breakdown
          ON breakdown.link_id = link.id
         AND breakdown.election_year = link.election_year
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
          ON classification.label_type = 'donor'
         AND classification.normalized_label = normalized_breakdown.normalized_label
         AND classification.industry_slug = top_industries.industry_name
        LEFT JOIN public.wa_candidate_finance_outside_groups AS outside_group
          ON outside_group.link_id = breakdown.link_id
         AND outside_group.election_year = breakdown.election_year
         AND outside_group.sponsor_id = breakdown.sponsor_id
         AND outside_group.support_oppose = breakdown.support_oppose
        WHERE breakdown.category_type = 'donor'
          AND breakdown.support_oppose = 'support'
      )
      SELECT candidate_id, election_id, industry_name, committee_id, committee_name, support_oppose, organization_name, amount, contributor_count, source_url
      FROM evidence
      WHERE rn <= 3
      ORDER BY candidate_id, election_id, industry_name, amount DESC, organization_name ASC
    `,
    [JSON.stringify(selectedRequests)]
  );

  const directOccupationsByCandidateElection = new Map<string, BallotLookupFinanceBreakdown[]>();
  const contributionSizeBucketsByCandidateElection = new Map<string, BallotLookupFinanceBreakdown[]>();
  const summaryByCandidateElection = new Map(
    summaryResult.rows.map((row) => [candidateElectionKey(row.candidate_id, row.election_id), row])
  );
  for (const row of directBreakdownResult.rows) {
    const summary = summaryByCandidateElection.get(candidateElectionKey(row.candidate_id, row.election_id));
    const targetMap =
      row.category_type === "contribution_size"
        ? contributionSizeBucketsByCandidateElection
        : directOccupationsByCandidateElection;
    addFinanceBreakdown(
      targetMap,
      row.candidate_id,
      row.election_id,
      mapFinanceBreakdown(row, summary?.source_url ?? GENERIC_WASHINGTON_PDC_SOURCE_URL)
    );
  }

  const supportingGroupsByCandidateElection = new Map<string, BallotLookupFinanceOutsideGroup[]>();
  const opposingGroupsByCandidateElection = new Map<string, BallotLookupFinanceOutsideGroup[]>();
  const outsideGroupNameByCandidateElectionCommittee = new Map<string, string>();
  for (const row of outsideGroupResult.rows) {
    const key = candidateElectionKey(row.candidate_id, row.election_id);
    const map = row.support_oppose === "support" ? supportingGroupsByCandidateElection : opposingGroupsByCandidateElection;
    const list = map.get(key) ?? [];
    list.push({
      committee_id: row.committee_id,
      committee_name: row.committee_name,
      support_oppose: row.support_oppose,
      amount: parseFinanceAmount(row.amount) ?? 0,
      source_url: firstNonEmptySourceUrl(row.source_url, GENERIC_WASHINGTON_PDC_SOURCE_URL),
    });
    map.set(key, list);
    outsideGroupNameByCandidateElectionCommittee.set(`${key}\u0000${row.committee_id}\u0000${row.support_oppose}`, row.committee_name);
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
      mapFinanceBreakdown(row, GENERIC_WASHINGTON_PDC_SOURCE_URL)
    );
  }

  const outsideIndustryEvidenceByCandidateElectionAndIndustry = new Map<
    string,
    BallotLookupFinanceOutsideIndustrySupportEvidence[]
  >();
  for (const row of outsideDonorEvidenceResult.rows) {
    const candidateKey = candidateElectionKey(row.candidate_id, row.election_id);
    const evidenceKey = `${candidateKey}\u0000${row.industry_name}`;
    const list = outsideIndustryEvidenceByCandidateElectionAndIndustry.get(evidenceKey) ?? [];
    list.push({
      organization_name: row.organization_name,
      organization_type: "donor",
      amount: parseFinanceAmount(row.amount) ?? 0,
      contributor_count: parseFinanceCount(row.contributor_count),
      committee_id: row.committee_id,
      committee_name:
        outsideGroupNameByCandidateElectionCommittee.get(`${candidateKey}\u0000${row.committee_id}\u0000${row.support_oppose}`) ??
        row.committee_name,
      source_url: firstNonEmptySourceUrl(row.source_url, GENERIC_WASHINGTON_PDC_SOURCE_URL),
    });
    outsideIndustryEvidenceByCandidateElectionAndIndustry.set(
      evidenceKey,
      list.sort((left, right) => right.amount - left.amount || left.organization_name.localeCompare(right.organization_name)).slice(0, 5)
    );
  }

  return new Map(
    summaryResult.rows.map((row) => {
      const key = candidateElectionKey(row.candidate_id, row.election_id);
      const topDirectDonorOccupations = directOccupationsByCandidateElection.get(key) ?? [];
      const contributionSizeBuckets = contributionSizeBucketsByCandidateElection.get(key) ?? [];
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
          source: "WASHINGTON_PDC",
          cycle: row.election_year,
          fec_candidate_id: null,
          controlled_committee_id: row.committee_id,
          last_synced_at: row.last_synced_at,
          direct_campaign: {
            total_raised: parseFinanceAmount(row.direct_contribution_total) ?? parseFinanceAmount(row.total_receipts),
            total_spent: parseFinanceAmount(row.total_disbursements),
            cash_on_hand: parseFinanceAmount(row.cash_on_hand),
            debts_owed: null,
            top_occupations: topDirectDonorOccupations,
            top_employers: [],
            top_industries: [],
            contribution_size_buckets: contributionSizeBuckets,
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


async function loadWisconsinCandidateFinanceSummariesByCandidateElection(
  db: Queryable,
  candidateRows: readonly CandidateRow[],
  electionRows: readonly ElectionRow[]
): Promise<Map<string, BallotLookupFinanceSummary>> {
  if (!isWisconsinCampaignFinanceEnabled()) {
    return new Map();
  }

  const requests = buildWisconsinFinanceSummaryRequests(candidateRows, electionRows);
  if (requests.length === 0) {
    return new Map();
  }

  const summaryResult = await db.query<WisconsinFinanceSummaryRow>(
    `
      WITH requested AS (
        SELECT
          candidate_id::uuid AS candidate_id,
          election_id::uuid AS election_id
        FROM jsonb_to_recordset($1::jsonb) AS x(
          candidate_id text,
          election_id text
        )
      )
      SELECT
        requested.candidate_id::text AS candidate_id,
        requested.election_id::text AS election_id,
        CASE
          WHEN count(DISTINCT link.committee_id) = 1 THEN min(link.committee_id)
          ELSE NULL
        END AS committee_id,
        max(summary.election_year) AS election_year,
        CASE WHEN count(summary.total_receipts) = 0 THEN NULL ELSE sum(summary.total_receipts) END AS total_receipts,
        CASE
          WHEN count(summary.direct_contribution_total) = 0 THEN NULL
          ELSE sum(summary.direct_contribution_total)
        END AS direct_contribution_total,
        CASE WHEN count(summary.total_disbursements) = 0 THEN NULL ELSE sum(summary.total_disbursements) END AS total_disbursements,
        CASE WHEN count(summary.cash_on_hand) = 0 THEN NULL ELSE sum(summary.cash_on_hand) END AS cash_on_hand,
        -- Outside totals are already candidate/election snapshot totals; max avoids double-counting multi-link joins.
        max(summary.outside_support_total) AS outside_support_total,
        max(summary.outside_oppose_total) AS outside_oppose_total,
        min(summary.source_url) FILTER (WHERE summary.source_url IS NOT NULL) AS source_url,
        max(summary.last_synced_at)::text AS last_synced_at
      FROM requested
      JOIN public.wi_candidate_finance_links AS link
        ON link.candidate_id = requested.candidate_id
       AND link.election_id = requested.election_id
       AND link.link_status = 'active'
      JOIN public.wi_candidate_finance_summaries AS summary
        ON summary.link_id = link.id
       AND summary.election_year = link.election_year
      GROUP BY requested.candidate_id, requested.election_id
    `,
    [JSON.stringify(requests)]
  );

  if (summaryResult.rows.length === 0) {
    return new Map();
  }

  const selectedRequests = summaryResult.rows.map((row) => ({
    candidate_id: row.candidate_id,
    election_id: row.election_id,
  }));

  const directBreakdownResult = await db.query<WisconsinFinanceDirectBreakdownRow>(
    `
      WITH selected AS (
        SELECT
          candidate_id::uuid AS candidate_id,
          election_id::uuid AS election_id
        FROM jsonb_to_recordset($1::jsonb) AS x(
          candidate_id text,
          election_id text
        )
      ),
      grouped AS (
        SELECT
          selected.candidate_id::text AS candidate_id,
          selected.election_id::text AS election_id,
          breakdown.category_type,
          breakdown.category_name,
          sum(breakdown.amount) AS amount,
          CASE
            WHEN count(breakdown.contributor_count) = 0 THEN NULL
            ELSE sum(breakdown.contributor_count)
          END AS contributor_count,
          min(breakdown.source_url) FILTER (WHERE breakdown.source_url IS NOT NULL) AS source_url
        FROM selected
        JOIN public.wi_candidate_finance_links AS link
          ON link.candidate_id = selected.candidate_id
         AND link.election_id = selected.election_id
         AND link.link_status = 'active'
        JOIN public.wi_candidate_finance_direct_breakdowns AS breakdown
          ON breakdown.link_id = link.id
         AND breakdown.election_year = link.election_year
        WHERE breakdown.category_type IN ('occupation', 'contribution_size')
        GROUP BY selected.candidate_id, selected.election_id, breakdown.category_type, breakdown.category_name
      ),
      ranked AS (
        SELECT
          *,
          row_number() OVER (
            PARTITION BY candidate_id, election_id, category_type
            ORDER BY amount DESC, category_name ASC
          ) AS rn
        FROM grouped
      )
      SELECT candidate_id, election_id, category_type, category_name, amount, contributor_count, source_url
      FROM ranked
      WHERE rn <= 5
      ORDER BY candidate_id, election_id, category_type, amount DESC, category_name ASC
    `,
    [JSON.stringify(selectedRequests)]
  );

  const outsideGroupResult = await db.query<WisconsinFinanceOutsideGroupRow>(
    `
      WITH selected AS (
        SELECT
          candidate_id::uuid AS candidate_id,
          election_id::uuid AS election_id
        FROM jsonb_to_recordset($1::jsonb) AS x(
          candidate_id text,
          election_id text
        )
      ),
      grouped AS (
        SELECT
          selected.candidate_id::text AS candidate_id,
          selected.election_id::text AS election_id,
          outside_group.sponsor_id AS committee_id,
          min(outside_group.sponsor_name) AS committee_name,
          outside_group.support_oppose,
          max(outside_group.amount) AS amount,
          min(outside_group.source_url) FILTER (WHERE outside_group.source_url IS NOT NULL) AS source_url
        FROM selected
        JOIN public.wi_candidate_finance_links AS link
          ON link.candidate_id = selected.candidate_id
         AND link.election_id = selected.election_id
         AND link.link_status = 'active'
        JOIN public.wi_candidate_finance_outside_groups AS outside_group
          ON outside_group.link_id = link.id
         AND outside_group.election_year = link.election_year
        GROUP BY selected.candidate_id, selected.election_id, outside_group.sponsor_id, outside_group.support_oppose
      ),
      ranked AS (
        SELECT
          *,
          row_number() OVER (
            PARTITION BY candidate_id, election_id, support_oppose
            ORDER BY amount DESC, committee_name ASC
          ) AS rn
        FROM grouped
      )
      SELECT candidate_id, election_id, committee_id, committee_name, support_oppose, amount, source_url
      FROM ranked
      WHERE rn <= 5
      ORDER BY candidate_id, election_id, support_oppose, amount DESC, committee_name ASC
    `,
    [JSON.stringify(selectedRequests)]
  );

  const outsideIndustryResult = await db.query<WisconsinFinanceOutsideIndustryRow>(
    `
      WITH selected AS (
        SELECT
          candidate_id::uuid AS candidate_id,
          election_id::uuid AS election_id
        FROM jsonb_to_recordset($1::jsonb) AS x(
          candidate_id text,
          election_id text
        )
      ),
      per_group AS (
        SELECT
          selected.candidate_id::text AS candidate_id,
          selected.election_id::text AS election_id,
          breakdown.sponsor_id AS committee_id,
          breakdown.support_oppose,
          breakdown.category_name,
          max(breakdown.amount) AS amount,
          CASE
            WHEN count(breakdown.contributor_count) = 0 THEN NULL
            ELSE max(breakdown.contributor_count)
          END AS contributor_count,
          min(breakdown.source_url) FILTER (WHERE breakdown.source_url IS NOT NULL) AS source_url
        FROM selected
        JOIN public.wi_candidate_finance_links AS link
          ON link.candidate_id = selected.candidate_id
         AND link.election_id = selected.election_id
         AND link.link_status = 'active'
        JOIN public.wi_candidate_finance_outside_group_breakdowns AS breakdown
          ON breakdown.link_id = link.id
         AND breakdown.election_year = link.election_year
        WHERE breakdown.category_type = 'industry'
        GROUP BY
          selected.candidate_id,
          selected.election_id,
          breakdown.sponsor_id,
          breakdown.support_oppose,
          breakdown.category_name
      ),
      grouped AS (
        SELECT
          candidate_id,
          election_id,
          support_oppose,
          category_name,
          sum(amount) AS amount,
          CASE
            WHEN count(contributor_count) = 0 THEN NULL
            ELSE sum(contributor_count)
          END AS contributor_count,
          min(source_url) FILTER (WHERE source_url IS NOT NULL) AS source_url
        FROM per_group
        GROUP BY candidate_id, election_id, support_oppose, category_name
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

  const outsideDonorEvidenceResult = await db.query<WisconsinFinanceOutsideDonorEvidenceRow>(
    `
      WITH selected AS (
        SELECT
          candidate_id::uuid AS candidate_id,
          election_id::uuid AS election_id
        FROM jsonb_to_recordset($1::jsonb) AS x(
          candidate_id text,
          election_id text
        )
      ),
      top_industries_per_group AS (
        SELECT
          selected.candidate_id::text AS candidate_id,
          selected.election_id::text AS election_id,
          industry.sponsor_id,
          industry.category_name AS industry_name,
          max(industry.amount) AS amount
        FROM selected
        JOIN public.wi_candidate_finance_links AS link
          ON link.candidate_id = selected.candidate_id
         AND link.election_id = selected.election_id
         AND link.link_status = 'active'
        JOIN public.wi_candidate_finance_outside_group_breakdowns AS industry
          ON industry.link_id = link.id
         AND industry.election_year = link.election_year
        WHERE industry.support_oppose = 'support'
          AND industry.category_type = 'industry'
        GROUP BY selected.candidate_id, selected.election_id, industry.sponsor_id, industry.category_name
      ),
      top_industries_grouped AS (
        SELECT
          candidate_id,
          election_id,
          industry_name,
          sum(amount) AS amount
        FROM top_industries_per_group
        GROUP BY candidate_id, election_id, industry_name
      ),
      top_industries AS (
        SELECT candidate_id, election_id, industry_name
        FROM (
          SELECT
            *,
            row_number() OVER (
              PARTITION BY candidate_id, election_id
              ORDER BY amount DESC, industry_name ASC
            ) AS rn
          FROM top_industries_grouped
        ) ranked_industries
        WHERE rn <= 5
      ),
      evidence AS (
        SELECT
          selected.candidate_id::text AS candidate_id,
          selected.election_id::text AS election_id,
          top_industries.industry_name,
          breakdown.sponsor_id AS committee_id,
          COALESCE(outside_group.sponsor_name, breakdown.sponsor_id) AS committee_name,
          breakdown.support_oppose,
          breakdown.category_name AS organization_name,
          breakdown.amount,
          breakdown.contributor_count,
          COALESCE(breakdown.source_url, outside_group.source_url) AS source_url,
          row_number() OVER (
            PARTITION BY selected.candidate_id, selected.election_id, top_industries.industry_name
            ORDER BY breakdown.amount DESC, breakdown.category_name ASC, breakdown.sponsor_id ASC
          ) AS rn
        FROM selected
        JOIN top_industries
          ON top_industries.candidate_id = selected.candidate_id::text
         AND top_industries.election_id = selected.election_id::text
        JOIN public.wi_candidate_finance_links AS link
          ON link.candidate_id = selected.candidate_id
         AND link.election_id = selected.election_id
         AND link.link_status = 'active'
        JOIN public.wi_candidate_finance_outside_group_breakdowns AS breakdown
          ON breakdown.link_id = link.id
         AND breakdown.election_year = link.election_year
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
          ON classification.label_type = 'donor'
         AND classification.normalized_label = normalized_breakdown.normalized_label
         AND classification.industry_slug = top_industries.industry_name
        LEFT JOIN public.wi_candidate_finance_outside_groups AS outside_group
          ON outside_group.link_id = breakdown.link_id
         AND outside_group.election_year = breakdown.election_year
         AND outside_group.sponsor_id = breakdown.sponsor_id
         AND outside_group.support_oppose = breakdown.support_oppose
        WHERE breakdown.category_type = 'donor'
          AND breakdown.support_oppose = 'support'
      )
      SELECT candidate_id, election_id, industry_name, committee_id, committee_name, support_oppose, organization_name, amount, contributor_count, source_url
      FROM evidence
      WHERE rn <= 3
      ORDER BY candidate_id, election_id, industry_name, amount DESC, organization_name ASC
    `,
    [JSON.stringify(selectedRequests)]
  );

  const directOccupationsByCandidateElection = new Map<string, BallotLookupFinanceBreakdown[]>();
  const contributionSizeBucketsByCandidateElection = new Map<string, BallotLookupFinanceBreakdown[]>();
  const summaryByCandidateElection = new Map(
    summaryResult.rows.map((row) => [candidateElectionKey(row.candidate_id, row.election_id), row])
  );
  for (const row of directBreakdownResult.rows) {
    const summary = summaryByCandidateElection.get(candidateElectionKey(row.candidate_id, row.election_id));
    const targetMap =
      row.category_type === "contribution_size"
        ? contributionSizeBucketsByCandidateElection
        : directOccupationsByCandidateElection;
    addFinanceBreakdown(
      targetMap,
      row.candidate_id,
      row.election_id,
      mapFinanceBreakdown(row, summary?.source_url ?? GENERIC_WISCONSIN_SUNSHINE_SOURCE_URL)
    );
  }

  const supportingGroupsByCandidateElection = new Map<string, BallotLookupFinanceOutsideGroup[]>();
  const opposingGroupsByCandidateElection = new Map<string, BallotLookupFinanceOutsideGroup[]>();
  const outsideGroupNameByCandidateElectionCommittee = new Map<string, string>();
  for (const row of outsideGroupResult.rows) {
    const key = candidateElectionKey(row.candidate_id, row.election_id);
    const map = row.support_oppose === "support" ? supportingGroupsByCandidateElection : opposingGroupsByCandidateElection;
    const list = map.get(key) ?? [];
    list.push({
      committee_id: row.committee_id,
      committee_name: row.committee_name,
      support_oppose: row.support_oppose,
      amount: parseFinanceAmount(row.amount) ?? 0,
      source_url: firstNonEmptySourceUrl(row.source_url, GENERIC_WISCONSIN_SUNSHINE_SOURCE_URL),
    });
    map.set(key, list);
    outsideGroupNameByCandidateElectionCommittee.set(`${key}\u0000${row.committee_id}\u0000${row.support_oppose}`, row.committee_name);
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
      mapFinanceBreakdown(row, GENERIC_WISCONSIN_SUNSHINE_SOURCE_URL)
    );
  }

  const outsideIndustryEvidenceByCandidateElectionAndIndustry = new Map<
    string,
    BallotLookupFinanceOutsideIndustrySupportEvidence[]
  >();
  for (const row of outsideDonorEvidenceResult.rows) {
    const candidateKey = candidateElectionKey(row.candidate_id, row.election_id);
    const evidenceKey = `${candidateKey}\u0000${row.industry_name}`;
    const list = outsideIndustryEvidenceByCandidateElectionAndIndustry.get(evidenceKey) ?? [];
    list.push({
      organization_name: row.organization_name,
      organization_type: "donor",
      amount: parseFinanceAmount(row.amount) ?? 0,
      contributor_count: parseFinanceCount(row.contributor_count),
      committee_id: row.committee_id,
      committee_name:
        outsideGroupNameByCandidateElectionCommittee.get(`${candidateKey}\u0000${row.committee_id}\u0000${row.support_oppose}`) ??
        row.committee_name,
      source_url: firstNonEmptySourceUrl(row.source_url, GENERIC_WISCONSIN_SUNSHINE_SOURCE_URL),
    });
    outsideIndustryEvidenceByCandidateElectionAndIndustry.set(
      evidenceKey,
      list.sort((left, right) => right.amount - left.amount || left.organization_name.localeCompare(right.organization_name)).slice(0, 5)
    );
  }

  return new Map(
    summaryResult.rows.map((row) => {
      const key = candidateElectionKey(row.candidate_id, row.election_id);
      const topDirectDonorOccupations = directOccupationsByCandidateElection.get(key) ?? [];
      const contributionSizeBuckets = contributionSizeBucketsByCandidateElection.get(key) ?? [];
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
          source: "WISCONSIN_SUNSHINE",
          cycle: row.election_year,
          fec_candidate_id: null,
          controlled_committee_id: row.committee_id,
          last_synced_at: row.last_synced_at,
          direct_campaign: {
            total_raised: parseFinanceAmount(row.direct_contribution_total) ?? parseFinanceAmount(row.total_receipts),
            total_spent: parseFinanceAmount(row.total_disbursements),
            cash_on_hand: parseFinanceAmount(row.cash_on_hand),
            debts_owed: null,
            top_occupations: topDirectDonorOccupations,
            top_employers: [],
            top_industries: [],
            contribution_size_buckets: contributionSizeBuckets,
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


async function loadMarylandCandidateFinanceSummariesByCandidateElection(
  db: Queryable,
  candidateRows: readonly CandidateRow[],
  electionRows: readonly ElectionRow[]
): Promise<Map<string, BallotLookupFinanceSummary>> {
  if (!isMarylandCampaignFinanceEnabled()) {
    return new Map();
  }

  const requests = buildMarylandFinanceSummaryRequests(candidateRows, electionRows);
  if (requests.length === 0) {
    return new Map();
  }

  const summaryResult = await db.query<MarylandFinanceSummaryRow>(
    `
      WITH requested AS (
        SELECT
          candidate_id::uuid AS candidate_id,
          election_id::uuid AS election_id
        FROM jsonb_to_recordset($1::jsonb) AS x(
          candidate_id text,
          election_id text
        )
      )
      SELECT
        requested.candidate_id::text AS candidate_id,
        requested.election_id::text AS election_id,
        CASE
          WHEN count(DISTINCT link.committee_id) = 1 THEN min(link.committee_id)
          ELSE NULL
        END AS committee_id,
        max(summary.election_year) AS election_year,
        CASE WHEN count(summary.total_receipts) = 0 THEN NULL ELSE sum(summary.total_receipts) END AS total_receipts,
        CASE
          WHEN count(summary.direct_contribution_total) = 0 THEN NULL
          ELSE sum(summary.direct_contribution_total)
        END AS direct_contribution_total,
        CASE WHEN count(summary.total_disbursements) = 0 THEN NULL ELSE sum(summary.total_disbursements) END AS total_disbursements,
        CASE WHEN count(summary.cash_on_hand) = 0 THEN NULL ELSE sum(summary.cash_on_hand) END AS cash_on_hand,
        max(summary.outside_support_total) AS outside_support_total,
        max(summary.outside_oppose_total) AS outside_oppose_total,
        min(summary.source_url) FILTER (WHERE summary.source_url IS NOT NULL) AS source_url,
        max(summary.last_synced_at)::text AS last_synced_at
      FROM requested
      JOIN public.md_candidate_finance_links AS link
        ON link.candidate_id = requested.candidate_id
       AND link.election_id = requested.election_id
       AND link.link_status = 'active'
      JOIN public.md_candidate_finance_summaries AS summary
        ON summary.link_id = link.id
       AND summary.election_year = link.election_year
      GROUP BY requested.candidate_id, requested.election_id
    `,
    [JSON.stringify(requests)]
  );

  if (summaryResult.rows.length === 0) {
    return new Map();
  }

  const selectedRequests = summaryResult.rows.map((row) => ({
    candidate_id: row.candidate_id,
    election_id: row.election_id,
  }));

  const directBreakdownResult = await db.query<MarylandFinanceDirectBreakdownRow>(
    `
      WITH selected AS (
        SELECT
          candidate_id::uuid AS candidate_id,
          election_id::uuid AS election_id
        FROM jsonb_to_recordset($1::jsonb) AS x(
          candidate_id text,
          election_id text
        )
      ),
      grouped AS (
        SELECT
          selected.candidate_id::text AS candidate_id,
          selected.election_id::text AS election_id,
          breakdown.category_type,
          breakdown.category_name,
          sum(breakdown.amount) AS amount,
          CASE
            WHEN count(breakdown.contributor_count) = 0 THEN NULL
            ELSE sum(breakdown.contributor_count)
          END AS contributor_count,
          min(breakdown.source_url) FILTER (WHERE breakdown.source_url IS NOT NULL) AS source_url
        FROM selected
        JOIN public.md_candidate_finance_links AS link
          ON link.candidate_id = selected.candidate_id
         AND link.election_id = selected.election_id
         AND link.link_status = 'active'
        JOIN public.md_candidate_finance_direct_breakdowns AS breakdown
          ON breakdown.link_id = link.id
         AND breakdown.election_year = link.election_year
        WHERE breakdown.category_type IN ('occupation', 'contribution_size')
        GROUP BY selected.candidate_id, selected.election_id, breakdown.category_type, breakdown.category_name
      ),
      ranked AS (
        SELECT
          *,
          row_number() OVER (
            PARTITION BY candidate_id, election_id, category_type
            ORDER BY amount DESC, category_name ASC
          ) AS rn
        FROM grouped
      )
      SELECT candidate_id, election_id, category_type, category_name, amount, contributor_count, source_url
      FROM ranked
      WHERE rn <= 5
      ORDER BY candidate_id, election_id, category_type, amount DESC, category_name ASC
    `,
    [JSON.stringify(selectedRequests)]
  );

  const outsideGroupResult = await db.query<MarylandFinanceOutsideGroupRow>(
    `
      WITH selected AS (
        SELECT
          candidate_id::uuid AS candidate_id,
          election_id::uuid AS election_id
        FROM jsonb_to_recordset($1::jsonb) AS x(
          candidate_id text,
          election_id text
        )
      ),
      grouped AS (
        SELECT
          selected.candidate_id::text AS candidate_id,
          selected.election_id::text AS election_id,
          outside_group.committee_id,
          min(outside_group.committee_name) AS committee_name,
          outside_group.support_oppose,
          max(outside_group.amount) AS amount,
          min(outside_group.source_url) FILTER (WHERE outside_group.source_url IS NOT NULL) AS source_url
        FROM selected
        JOIN public.md_candidate_finance_links AS link
          ON link.candidate_id = selected.candidate_id
         AND link.election_id = selected.election_id
         AND link.link_status = 'active'
        JOIN public.md_candidate_finance_outside_groups AS outside_group
          ON outside_group.link_id = link.id
         AND outside_group.election_year = link.election_year
        GROUP BY selected.candidate_id, selected.election_id, outside_group.committee_id, outside_group.support_oppose
      ),
      ranked AS (
        SELECT
          *,
          row_number() OVER (
            PARTITION BY candidate_id, election_id, support_oppose
            ORDER BY amount DESC, committee_name ASC
          ) AS rn
        FROM grouped
      )
      SELECT candidate_id, election_id, committee_id, committee_name, support_oppose, amount, source_url
      FROM ranked
      WHERE rn <= 5
      ORDER BY candidate_id, election_id, support_oppose, amount DESC, committee_name ASC
    `,
    [JSON.stringify(selectedRequests)]
  );

  const outsideIndustryResult = await db.query<MarylandFinanceOutsideIndustryRow>(
    `
      WITH selected AS (
        SELECT
          candidate_id::uuid AS candidate_id,
          election_id::uuid AS election_id
        FROM jsonb_to_recordset($1::jsonb) AS x(
          candidate_id text,
          election_id text
        )
      ),
      per_group AS (
        SELECT
          selected.candidate_id::text AS candidate_id,
          selected.election_id::text AS election_id,
          breakdown.committee_id,
          breakdown.support_oppose,
          breakdown.category_name,
          max(breakdown.amount) AS amount,
          CASE
            WHEN count(breakdown.contributor_count) = 0 THEN NULL
            ELSE max(breakdown.contributor_count)
          END AS contributor_count,
          min(breakdown.source_url) FILTER (WHERE breakdown.source_url IS NOT NULL) AS source_url
        FROM selected
        JOIN public.md_candidate_finance_links AS link
          ON link.candidate_id = selected.candidate_id
         AND link.election_id = selected.election_id
         AND link.link_status = 'active'
        JOIN public.md_candidate_finance_outside_group_breakdowns AS breakdown
          ON breakdown.link_id = link.id
         AND breakdown.election_year = link.election_year
        WHERE breakdown.category_type = 'industry'
        GROUP BY
          selected.candidate_id,
          selected.election_id,
          breakdown.committee_id,
          breakdown.support_oppose,
          breakdown.category_name
      ),
      grouped AS (
        SELECT
          candidate_id,
          election_id,
          support_oppose,
          category_name,
          sum(amount) AS amount,
          CASE
            WHEN count(contributor_count) = 0 THEN NULL
            ELSE sum(contributor_count)
          END AS contributor_count,
          min(source_url) FILTER (WHERE source_url IS NOT NULL) AS source_url
        FROM per_group
        GROUP BY candidate_id, election_id, support_oppose, category_name
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

  const outsideDonorEvidenceResult = await db.query<MarylandFinanceOutsideDonorEvidenceRow>(
    `
      WITH selected AS (
        SELECT
          candidate_id::uuid AS candidate_id,
          election_id::uuid AS election_id
        FROM jsonb_to_recordset($1::jsonb) AS x(
          candidate_id text,
          election_id text
        )
      ),
      top_industries_per_group AS (
        SELECT
          selected.candidate_id::text AS candidate_id,
          selected.election_id::text AS election_id,
          industry.committee_id,
          industry.category_name AS industry_name,
          max(industry.amount) AS amount
        FROM selected
        JOIN public.md_candidate_finance_links AS link
          ON link.candidate_id = selected.candidate_id
         AND link.election_id = selected.election_id
         AND link.link_status = 'active'
        JOIN public.md_candidate_finance_outside_group_breakdowns AS industry
          ON industry.link_id = link.id
         AND industry.election_year = link.election_year
        WHERE industry.support_oppose = 'support'
          AND industry.category_type = 'industry'
        GROUP BY selected.candidate_id, selected.election_id, industry.committee_id, industry.category_name
      ),
      top_industries_grouped AS (
        SELECT
          candidate_id,
          election_id,
          industry_name,
          sum(amount) AS amount
        FROM top_industries_per_group
        GROUP BY candidate_id, election_id, industry_name
      ),
      top_industries AS (
        SELECT candidate_id, election_id, industry_name
        FROM (
          SELECT
            *,
            row_number() OVER (
              PARTITION BY candidate_id, election_id
              ORDER BY amount DESC, industry_name ASC
            ) AS rn
          FROM top_industries_grouped
        ) ranked_industries
        WHERE rn <= 5
      ),
      evidence AS (
        SELECT
          selected.candidate_id::text AS candidate_id,
          selected.election_id::text AS election_id,
          top_industries.industry_name,
          breakdown.committee_id,
          COALESCE(outside_group.committee_name, breakdown.committee_id) AS committee_name,
          breakdown.support_oppose,
          breakdown.category_name AS organization_name,
          breakdown.amount,
          breakdown.contributor_count,
          COALESCE(breakdown.source_url, outside_group.source_url) AS source_url,
          row_number() OVER (
            PARTITION BY selected.candidate_id, selected.election_id, top_industries.industry_name
            ORDER BY breakdown.amount DESC, breakdown.category_name ASC, breakdown.committee_id ASC
          ) AS rn
        FROM selected
        JOIN top_industries
          ON top_industries.candidate_id = selected.candidate_id::text
         AND top_industries.election_id = selected.election_id::text
        JOIN public.md_candidate_finance_links AS link
          ON link.candidate_id = selected.candidate_id
         AND link.election_id = selected.election_id
         AND link.link_status = 'active'
        JOIN public.md_candidate_finance_outside_group_breakdowns AS breakdown
          ON breakdown.link_id = link.id
         AND breakdown.election_year = link.election_year
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
          ON classification.label_type = 'donor'
         AND classification.normalized_label = normalized_breakdown.normalized_label
         AND classification.industry_slug = top_industries.industry_name
        LEFT JOIN public.md_candidate_finance_outside_groups AS outside_group
          ON outside_group.link_id = breakdown.link_id
         AND outside_group.election_year = breakdown.election_year
         AND outside_group.committee_id = breakdown.committee_id
         AND outside_group.support_oppose = breakdown.support_oppose
        WHERE breakdown.category_type = 'donor'
          AND breakdown.support_oppose = 'support'
      )
      SELECT candidate_id, election_id, industry_name, committee_id, committee_name, support_oppose, organization_name, amount, contributor_count, source_url
      FROM evidence
      WHERE rn <= 3
      ORDER BY candidate_id, election_id, industry_name, amount DESC, organization_name ASC
    `,
    [JSON.stringify(selectedRequests)]
  );

  const directOccupationsByCandidateElection = new Map<string, BallotLookupFinanceBreakdown[]>();
  const contributionSizeBucketsByCandidateElection = new Map<string, BallotLookupFinanceBreakdown[]>();
  const summaryByCandidateElection = new Map(
    summaryResult.rows.map((row) => [candidateElectionKey(row.candidate_id, row.election_id), row])
  );
  for (const row of directBreakdownResult.rows) {
    const summary = summaryByCandidateElection.get(candidateElectionKey(row.candidate_id, row.election_id));
    const targetMap =
      row.category_type === "contribution_size"
        ? contributionSizeBucketsByCandidateElection
        : directOccupationsByCandidateElection;
    addFinanceBreakdown(
      targetMap,
      row.candidate_id,
      row.election_id,
      mapFinanceBreakdown(row, summary?.source_url ?? GENERIC_MARYLAND_CFS_SOURCE_URL)
    );
  }

  const supportingGroupsByCandidateElection = new Map<string, BallotLookupFinanceOutsideGroup[]>();
  const opposingGroupsByCandidateElection = new Map<string, BallotLookupFinanceOutsideGroup[]>();
  const outsideGroupNameByCandidateElectionCommittee = new Map<string, string>();
  for (const row of outsideGroupResult.rows) {
    const key = candidateElectionKey(row.candidate_id, row.election_id);
    const map = row.support_oppose === "support" ? supportingGroupsByCandidateElection : opposingGroupsByCandidateElection;
    const list = map.get(key) ?? [];
    list.push({
      committee_id: row.committee_id,
      committee_name: row.committee_name,
      support_oppose: row.support_oppose,
      amount: parseFinanceAmount(row.amount) ?? 0,
      source_url: firstNonEmptySourceUrl(row.source_url, GENERIC_MARYLAND_CFS_SOURCE_URL),
    });
    map.set(key, list);
    outsideGroupNameByCandidateElectionCommittee.set(`${key}\u0000${row.committee_id}\u0000${row.support_oppose}`, row.committee_name);
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
      mapFinanceBreakdown(row, GENERIC_MARYLAND_CFS_SOURCE_URL)
    );
  }

  const outsideIndustryEvidenceByCandidateElectionAndIndustry = new Map<
    string,
    BallotLookupFinanceOutsideIndustrySupportEvidence[]
  >();
  for (const row of outsideDonorEvidenceResult.rows) {
    const candidateKey = candidateElectionKey(row.candidate_id, row.election_id);
    const evidenceKey = `${candidateKey}\u0000${row.industry_name}`;
    const list = outsideIndustryEvidenceByCandidateElectionAndIndustry.get(evidenceKey) ?? [];
    list.push({
      organization_name: row.organization_name,
      organization_type: "donor",
      amount: parseFinanceAmount(row.amount) ?? 0,
      contributor_count: parseFinanceCount(row.contributor_count),
      committee_id: row.committee_id,
      committee_name:
        outsideGroupNameByCandidateElectionCommittee.get(`${candidateKey}\u0000${row.committee_id}\u0000${row.support_oppose}`) ??
        row.committee_name,
      source_url: firstNonEmptySourceUrl(row.source_url, GENERIC_MARYLAND_CFS_SOURCE_URL),
    });
    outsideIndustryEvidenceByCandidateElectionAndIndustry.set(
      evidenceKey,
      list.sort((left, right) => right.amount - left.amount || left.organization_name.localeCompare(right.organization_name)).slice(0, 5)
    );
  }

  return new Map(
    summaryResult.rows.map((row) => {
      const key = candidateElectionKey(row.candidate_id, row.election_id);
      const topDirectDonorOccupations = directOccupationsByCandidateElection.get(key) ?? [];
      const contributionSizeBuckets = contributionSizeBucketsByCandidateElection.get(key) ?? [];
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
          source: "MARYLAND_CFS",
          cycle: row.election_year,
          fec_candidate_id: null,
          controlled_committee_id: row.committee_id,
          last_synced_at: row.last_synced_at,
          direct_campaign: {
            total_raised: parseFinanceAmount(row.direct_contribution_total) ?? parseFinanceAmount(row.total_receipts),
            total_spent: parseFinanceAmount(row.total_disbursements),
            cash_on_hand: parseFinanceAmount(row.cash_on_hand),
            debts_owed: null,
            top_occupations: topDirectDonorOccupations,
            top_employers: [],
            top_industries: [],
            contribution_size_buckets: contributionSizeBuckets,
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

async function loadMichiganCandidateFinanceSummariesByCandidateElection(
  db: Queryable,
  candidateRows: readonly CandidateRow[],
  electionRows: readonly ElectionRow[]
): Promise<Map<string, BallotLookupFinanceSummary>> {
  if (!isMichiganCampaignFinanceEnabled()) {
    return new Map();
  }

  const requests = buildMichiganFinanceSummaryRequests(candidateRows, electionRows);
  if (requests.length === 0) {
    return new Map();
  }

  const summaryResult = await db.query<MichiganFinanceSummaryRow>(
    `
      WITH requested AS (
        SELECT
          candidate_id::uuid AS candidate_id,
          election_id::uuid AS election_id
        FROM jsonb_to_recordset($1::jsonb) AS x(
          candidate_id text,
          election_id text
        )
      )
      SELECT
        requested.candidate_id::text AS candidate_id,
        requested.election_id::text AS election_id,
        CASE
          WHEN count(DISTINCT link.committee_id) = 1 THEN min(link.committee_id)
          ELSE NULL
        END AS committee_id,
        max(summary.election_year) AS election_year,
        CASE WHEN count(summary.total_receipts) = 0 THEN NULL ELSE sum(summary.total_receipts) END AS total_receipts,
        CASE
          WHEN count(summary.direct_contribution_total) = 0 THEN NULL
          ELSE sum(summary.direct_contribution_total)
        END AS direct_contribution_total,
        CASE WHEN count(summary.total_disbursements) = 0 THEN NULL ELSE sum(summary.total_disbursements) END AS total_disbursements,
        CASE WHEN count(summary.cash_on_hand) = 0 THEN NULL ELSE sum(summary.cash_on_hand) END AS cash_on_hand,
        -- Outside totals are already candidate/election snapshot totals; max avoids double-counting multi-link joins.
        max(summary.outside_support_total) AS outside_support_total,
        max(summary.outside_oppose_total) AS outside_oppose_total,
        min(summary.source_url) FILTER (WHERE summary.source_url IS NOT NULL) AS source_url,
        max(summary.last_synced_at)::text AS last_synced_at
      FROM requested
      JOIN public.mi_candidate_finance_links AS link
        ON link.candidate_id = requested.candidate_id
       AND link.election_id = requested.election_id
       AND link.link_status = 'active'
      JOIN public.mi_candidate_finance_summaries AS summary
        ON summary.link_id = link.id
       AND summary.election_year = link.election_year
      GROUP BY requested.candidate_id, requested.election_id
    `,
    [JSON.stringify(requests)]
  );

  if (summaryResult.rows.length === 0) {
    return new Map();
  }

  const selectedRequests = summaryResult.rows.map((row) => ({
    candidate_id: row.candidate_id,
    election_id: row.election_id,
  }));

  const directBreakdownResult = await db.query<MichiganFinanceDirectBreakdownRow>(
    `
      WITH selected AS (
        SELECT
          candidate_id::uuid AS candidate_id,
          election_id::uuid AS election_id
        FROM jsonb_to_recordset($1::jsonb) AS x(
          candidate_id text,
          election_id text
        )
      ),
      grouped AS (
        SELECT
          selected.candidate_id::text AS candidate_id,
          selected.election_id::text AS election_id,
          breakdown.category_type,
          breakdown.category_name,
          sum(breakdown.amount) AS amount,
          CASE
            WHEN count(breakdown.contributor_count) = 0 THEN NULL
            ELSE sum(breakdown.contributor_count)
          END AS contributor_count,
          min(breakdown.source_url) FILTER (WHERE breakdown.source_url IS NOT NULL) AS source_url
        FROM selected
        JOIN public.mi_candidate_finance_links AS link
          ON link.candidate_id = selected.candidate_id
         AND link.election_id = selected.election_id
         AND link.link_status = 'active'
        JOIN public.mi_candidate_finance_direct_breakdowns AS breakdown
          ON breakdown.link_id = link.id
         AND breakdown.election_year = link.election_year
        WHERE breakdown.category_type IN ('occupation', 'contribution_size')
        GROUP BY selected.candidate_id, selected.election_id, breakdown.category_type, breakdown.category_name
      ),
      ranked AS (
        SELECT
          *,
          row_number() OVER (
            PARTITION BY candidate_id, election_id, category_type
            ORDER BY amount DESC, category_name ASC
          ) AS rn
        FROM grouped
      )
      SELECT candidate_id, election_id, category_type, category_name, amount, contributor_count, source_url
      FROM ranked
      WHERE rn <= 5
      ORDER BY candidate_id, election_id, category_type, amount DESC, category_name ASC
    `,
    [JSON.stringify(selectedRequests)]
  );

  const outsideGroupResult = await db.query<MichiganFinanceOutsideGroupRow>(
    `
      WITH selected AS (
        SELECT
          candidate_id::uuid AS candidate_id,
          election_id::uuid AS election_id
        FROM jsonb_to_recordset($1::jsonb) AS x(
          candidate_id text,
          election_id text
        )
      ),
      grouped AS (
        SELECT
          selected.candidate_id::text AS candidate_id,
          selected.election_id::text AS election_id,
          outside_group.committee_id AS committee_id,
          min(outside_group.committee_name) AS committee_name,
          outside_group.support_oppose,
          max(outside_group.amount) AS amount,
          min(outside_group.source_url) FILTER (WHERE outside_group.source_url IS NOT NULL) AS source_url
        FROM selected
        JOIN public.mi_candidate_finance_links AS link
          ON link.candidate_id = selected.candidate_id
         AND link.election_id = selected.election_id
         AND link.link_status = 'active'
        JOIN public.mi_candidate_finance_outside_groups AS outside_group
          ON outside_group.link_id = link.id
         AND outside_group.election_year = link.election_year
        GROUP BY selected.candidate_id, selected.election_id, outside_group.committee_id, outside_group.support_oppose
      ),
      ranked AS (
        SELECT
          *,
          row_number() OVER (
            PARTITION BY candidate_id, election_id, support_oppose
            ORDER BY amount DESC, committee_name ASC
          ) AS rn
        FROM grouped
      )
      SELECT candidate_id, election_id, committee_id, committee_name, support_oppose, amount, source_url
      FROM ranked
      WHERE rn <= 5
      ORDER BY candidate_id, election_id, support_oppose, amount DESC, committee_name ASC
    `,
    [JSON.stringify(selectedRequests)]
  );

  const outsideIndustryResult = await db.query<MichiganFinanceOutsideIndustryRow>(
    `
      WITH selected AS (
        SELECT
          candidate_id::uuid AS candidate_id,
          election_id::uuid AS election_id
        FROM jsonb_to_recordset($1::jsonb) AS x(
          candidate_id text,
          election_id text
        )
      ),
      per_group AS (
        SELECT
          selected.candidate_id::text AS candidate_id,
          selected.election_id::text AS election_id,
          breakdown.committee_id AS committee_id,
          breakdown.support_oppose,
          breakdown.category_name,
          max(breakdown.amount) AS amount,
          CASE
            WHEN count(breakdown.contributor_count) = 0 THEN NULL
            ELSE max(breakdown.contributor_count)
          END AS contributor_count,
          min(breakdown.source_url) FILTER (WHERE breakdown.source_url IS NOT NULL) AS source_url
        FROM selected
        JOIN public.mi_candidate_finance_links AS link
          ON link.candidate_id = selected.candidate_id
         AND link.election_id = selected.election_id
         AND link.link_status = 'active'
        JOIN public.mi_candidate_finance_outside_group_breakdowns AS breakdown
          ON breakdown.link_id = link.id
         AND breakdown.election_year = link.election_year
        WHERE breakdown.category_type = 'industry'
        GROUP BY
          selected.candidate_id,
          selected.election_id,
          breakdown.committee_id,
          breakdown.support_oppose,
          breakdown.category_name
      ),
      grouped AS (
        SELECT
          candidate_id,
          election_id,
          support_oppose,
          category_name,
          sum(amount) AS amount,
          CASE
            WHEN count(contributor_count) = 0 THEN NULL
            ELSE sum(contributor_count)
          END AS contributor_count,
          min(source_url) FILTER (WHERE source_url IS NOT NULL) AS source_url
        FROM per_group
        GROUP BY candidate_id, election_id, support_oppose, category_name
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

  const outsideDonorEvidenceResult = await db.query<MichiganFinanceOutsideDonorEvidenceRow>(
    `
      WITH selected AS (
        SELECT
          candidate_id::uuid AS candidate_id,
          election_id::uuid AS election_id
        FROM jsonb_to_recordset($1::jsonb) AS x(
          candidate_id text,
          election_id text
        )
      ),
      top_industries_per_group AS (
        SELECT
          selected.candidate_id::text AS candidate_id,
          selected.election_id::text AS election_id,
          industry.committee_id,
          industry.category_name AS industry_name,
          max(industry.amount) AS amount
        FROM selected
        JOIN public.mi_candidate_finance_links AS link
          ON link.candidate_id = selected.candidate_id
         AND link.election_id = selected.election_id
         AND link.link_status = 'active'
        JOIN public.mi_candidate_finance_outside_group_breakdowns AS industry
          ON industry.link_id = link.id
         AND industry.election_year = link.election_year
        WHERE industry.support_oppose = 'support'
          AND industry.category_type = 'industry'
        GROUP BY selected.candidate_id, selected.election_id, industry.committee_id, industry.category_name
      ),
      top_industries_grouped AS (
        SELECT
          candidate_id,
          election_id,
          industry_name,
          sum(amount) AS amount
        FROM top_industries_per_group
        GROUP BY candidate_id, election_id, industry_name
      ),
      top_industries AS (
        SELECT candidate_id, election_id, industry_name
        FROM (
          SELECT
            *,
            row_number() OVER (
              PARTITION BY candidate_id, election_id
              ORDER BY amount DESC, industry_name ASC
            ) AS rn
          FROM top_industries_grouped
        ) ranked_industries
        WHERE rn <= 5
      ),
      evidence AS (
        SELECT
          selected.candidate_id::text AS candidate_id,
          selected.election_id::text AS election_id,
          top_industries.industry_name,
          breakdown.committee_id AS committee_id,
          COALESCE(outside_group.committee_name, breakdown.committee_id) AS committee_name,
          breakdown.support_oppose,
          breakdown.category_name AS organization_name,
          breakdown.amount,
          breakdown.contributor_count,
          COALESCE(breakdown.source_url, outside_group.source_url) AS source_url,
          row_number() OVER (
            PARTITION BY selected.candidate_id, selected.election_id, top_industries.industry_name
            ORDER BY breakdown.amount DESC, breakdown.category_name ASC, breakdown.committee_id ASC
          ) AS rn
        FROM selected
        JOIN top_industries
          ON top_industries.candidate_id = selected.candidate_id::text
         AND top_industries.election_id = selected.election_id::text
        JOIN public.mi_candidate_finance_links AS link
          ON link.candidate_id = selected.candidate_id
         AND link.election_id = selected.election_id
         AND link.link_status = 'active'
        JOIN public.mi_candidate_finance_outside_group_breakdowns AS breakdown
          ON breakdown.link_id = link.id
         AND breakdown.election_year = link.election_year
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
          ON classification.label_type = 'donor'
         AND classification.normalized_label = normalized_breakdown.normalized_label
         AND classification.industry_slug = top_industries.industry_name
        LEFT JOIN public.mi_candidate_finance_outside_groups AS outside_group
          ON outside_group.link_id = breakdown.link_id
         AND outside_group.election_year = breakdown.election_year
         AND outside_group.committee_id = breakdown.committee_id
         AND outside_group.support_oppose = breakdown.support_oppose
        WHERE breakdown.category_type = 'donor'
          AND breakdown.support_oppose = 'support'
      )
      SELECT candidate_id, election_id, industry_name, committee_id, committee_name, support_oppose, organization_name, amount, contributor_count, source_url
      FROM evidence
      WHERE rn <= 3
      ORDER BY candidate_id, election_id, industry_name, amount DESC, organization_name ASC
    `,
    [JSON.stringify(selectedRequests)]
  );

  const directOccupationsByCandidateElection = new Map<string, BallotLookupFinanceBreakdown[]>();
  const contributionSizeBucketsByCandidateElection = new Map<string, BallotLookupFinanceBreakdown[]>();
  const summaryByCandidateElection = new Map(
    summaryResult.rows.map((row) => [candidateElectionKey(row.candidate_id, row.election_id), row])
  );
  for (const row of directBreakdownResult.rows) {
    const summary = summaryByCandidateElection.get(candidateElectionKey(row.candidate_id, row.election_id));
    const targetMap =
      row.category_type === "contribution_size"
        ? contributionSizeBucketsByCandidateElection
        : directOccupationsByCandidateElection;
    addFinanceBreakdown(
      targetMap,
      row.candidate_id,
      row.election_id,
      mapFinanceBreakdown(row, summary?.source_url ?? GENERIC_MICHIGAN_MITN_SOURCE_URL)
    );
  }

  const supportingGroupsByCandidateElection = new Map<string, BallotLookupFinanceOutsideGroup[]>();
  const opposingGroupsByCandidateElection = new Map<string, BallotLookupFinanceOutsideGroup[]>();
  const outsideGroupNameByCandidateElectionCommittee = new Map<string, string>();
  for (const row of outsideGroupResult.rows) {
    const key = candidateElectionKey(row.candidate_id, row.election_id);
    const map = row.support_oppose === "support" ? supportingGroupsByCandidateElection : opposingGroupsByCandidateElection;
    const list = map.get(key) ?? [];
    list.push({
      committee_id: row.committee_id,
      committee_name: row.committee_name,
      support_oppose: row.support_oppose,
      amount: parseFinanceAmount(row.amount) ?? 0,
      source_url: firstNonEmptySourceUrl(row.source_url, GENERIC_MICHIGAN_MITN_SOURCE_URL),
    });
    map.set(key, list);
    outsideGroupNameByCandidateElectionCommittee.set(`${key}\u0000${row.committee_id}\u0000${row.support_oppose}`, row.committee_name);
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
      mapFinanceBreakdown(row, GENERIC_MICHIGAN_MITN_SOURCE_URL)
    );
  }

  const outsideIndustryEvidenceByCandidateElectionAndIndustry = new Map<
    string,
    BallotLookupFinanceOutsideIndustrySupportEvidence[]
  >();
  for (const row of outsideDonorEvidenceResult.rows) {
    const candidateKey = candidateElectionKey(row.candidate_id, row.election_id);
    const evidenceKey = `${candidateKey}\u0000${row.industry_name}`;
    const list = outsideIndustryEvidenceByCandidateElectionAndIndustry.get(evidenceKey) ?? [];
    list.push({
      organization_name: row.organization_name,
      organization_type: "donor",
      amount: parseFinanceAmount(row.amount) ?? 0,
      contributor_count: parseFinanceCount(row.contributor_count),
      committee_id: row.committee_id,
      committee_name:
        outsideGroupNameByCandidateElectionCommittee.get(`${candidateKey}\u0000${row.committee_id}\u0000${row.support_oppose}`) ??
        row.committee_name,
      source_url: firstNonEmptySourceUrl(row.source_url, GENERIC_MICHIGAN_MITN_SOURCE_URL),
    });
    outsideIndustryEvidenceByCandidateElectionAndIndustry.set(
      evidenceKey,
      list.sort((left, right) => right.amount - left.amount || left.organization_name.localeCompare(right.organization_name)).slice(0, 5)
    );
  }

  return new Map(
    summaryResult.rows.map((row) => {
      const key = candidateElectionKey(row.candidate_id, row.election_id);
      const topDirectDonorOccupations = directOccupationsByCandidateElection.get(key) ?? [];
      const contributionSizeBuckets = contributionSizeBucketsByCandidateElection.get(key) ?? [];
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
          source: "MICHIGAN_MITN",
          cycle: row.election_year,
          fec_candidate_id: null,
          controlled_committee_id: row.committee_id,
          last_synced_at: row.last_synced_at,
          direct_campaign: {
            total_raised: parseFinanceAmount(row.direct_contribution_total) ?? parseFinanceAmount(row.total_receipts),
            total_spent: parseFinanceAmount(row.total_disbursements),
            cash_on_hand: parseFinanceAmount(row.cash_on_hand),
            debts_owed: null,
            top_occupations: topDirectDonorOccupations,
            top_employers: [],
            top_industries: [],
            contribution_size_buckets: contributionSizeBuckets,
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


async function loadHawaiiCandidateFinanceSummariesByCandidateElection(
  db: Queryable,
  candidateRows: readonly CandidateRow[],
  electionRows: readonly ElectionRow[]
): Promise<Map<string, BallotLookupFinanceSummary>> {
  if (!isHawaiiCampaignFinanceEnabled()) {
    return new Map();
  }

  const requests = buildHawaiiFinanceSummaryRequests(candidateRows, electionRows);
  if (requests.length === 0) {
    return new Map();
  }

  const summaryResult = await db.query<HawaiiFinanceSummaryRow>(
    `
      WITH requested AS (
        SELECT
          candidate_id::uuid AS candidate_id,
          election_id::uuid AS election_id
        FROM jsonb_to_recordset($1::jsonb) AS x(
          candidate_id text,
          election_id text
        )
      )
      SELECT
        requested.candidate_id::text AS candidate_id,
        requested.election_id::text AS election_id,
        CASE
          WHEN count(DISTINCT link.committee_id) = 1 THEN min(link.committee_id)
          ELSE NULL
        END AS committee_id,
        max(summary.election_year) AS election_year,
        CASE WHEN count(summary.total_receipts) = 0 THEN NULL ELSE sum(summary.total_receipts) END AS total_receipts,
        CASE
          WHEN count(summary.direct_contribution_total) = 0 THEN NULL
          ELSE sum(summary.direct_contribution_total)
        END AS direct_contribution_total,
        CASE WHEN count(summary.total_disbursements) = 0 THEN NULL ELSE sum(summary.total_disbursements) END AS total_disbursements,
        CASE WHEN count(summary.cash_on_hand) = 0 THEN NULL ELSE sum(summary.cash_on_hand) END AS cash_on_hand,
        max(summary.outside_support_total) AS outside_support_total,
        max(summary.outside_oppose_total) AS outside_oppose_total,
        min(summary.source_url) FILTER (WHERE summary.source_url IS NOT NULL) AS source_url,
        max(summary.last_synced_at)::text AS last_synced_at
      FROM requested
      JOIN public.hi_candidate_finance_links AS link
        ON link.candidate_id = requested.candidate_id
       AND link.election_id = requested.election_id
       AND link.link_status = 'active'
      JOIN public.hi_candidate_finance_summaries AS summary
        ON summary.link_id = link.id
       AND summary.election_year = link.election_year
      GROUP BY requested.candidate_id, requested.election_id
    `,
    [JSON.stringify(requests)]
  );

  if (summaryResult.rows.length === 0) {
    return new Map();
  }

  const selectedRequests = summaryResult.rows.map((row) => ({
    candidate_id: row.candidate_id,
    election_id: row.election_id,
  }));

  const directBreakdownResult = await db.query<HawaiiFinanceDirectBreakdownRow>(
    `
      WITH selected AS (
        SELECT
          candidate_id::uuid AS candidate_id,
          election_id::uuid AS election_id
        FROM jsonb_to_recordset($1::jsonb) AS x(
          candidate_id text,
          election_id text
        )
      ),
      grouped AS (
        SELECT
          selected.candidate_id::text AS candidate_id,
          selected.election_id::text AS election_id,
          breakdown.category_type,
          breakdown.category_name,
          sum(breakdown.amount) AS amount,
          CASE
            WHEN count(breakdown.contributor_count) = 0 THEN NULL
            ELSE sum(breakdown.contributor_count)
          END AS contributor_count,
          min(breakdown.source_url) FILTER (WHERE breakdown.source_url IS NOT NULL) AS source_url
        FROM selected
        JOIN public.hi_candidate_finance_links AS link
          ON link.candidate_id = selected.candidate_id
         AND link.election_id = selected.election_id
         AND link.link_status = 'active'
        JOIN public.hi_candidate_finance_direct_breakdowns AS breakdown
          ON breakdown.link_id = link.id
         AND breakdown.election_year = link.election_year
        WHERE breakdown.category_type IN ('occupation', 'contribution_size')
        GROUP BY selected.candidate_id, selected.election_id, breakdown.category_type, breakdown.category_name
      ),
      ranked AS (
        SELECT
          *,
          row_number() OVER (
            PARTITION BY candidate_id, election_id, category_type
            ORDER BY amount DESC, category_name ASC
          ) AS rn
        FROM grouped
      )
      SELECT candidate_id, election_id, category_type, category_name, amount, contributor_count, source_url
      FROM ranked
      WHERE rn <= 5
      ORDER BY candidate_id, election_id, category_type, amount DESC, category_name ASC
    `,
    [JSON.stringify(selectedRequests)]
  );

  const outsideGroupResult = await db.query<HawaiiFinanceOutsideGroupRow>(
    `
      WITH selected AS (
        SELECT
          candidate_id::uuid AS candidate_id,
          election_id::uuid AS election_id
        FROM jsonb_to_recordset($1::jsonb) AS x(
          candidate_id text,
          election_id text
        )
      ),
      grouped AS (
        SELECT
          selected.candidate_id::text AS candidate_id,
          selected.election_id::text AS election_id,
          outside_group.committee_id AS committee_id,
          min(outside_group.committee_name) AS committee_name,
          outside_group.support_oppose,
          max(outside_group.amount) AS amount,
          min(outside_group.source_url) FILTER (WHERE outside_group.source_url IS NOT NULL) AS source_url
        FROM selected
        JOIN public.hi_candidate_finance_links AS link
          ON link.candidate_id = selected.candidate_id
         AND link.election_id = selected.election_id
         AND link.link_status = 'active'
        JOIN public.hi_candidate_finance_outside_groups AS outside_group
          ON outside_group.link_id = link.id
         AND outside_group.election_year = link.election_year
        GROUP BY selected.candidate_id, selected.election_id, outside_group.committee_id, outside_group.support_oppose
      ),
      ranked AS (
        SELECT
          *,
          row_number() OVER (
            PARTITION BY candidate_id, election_id, support_oppose
            ORDER BY amount DESC, committee_name ASC
          ) AS rn
        FROM grouped
      )
      SELECT candidate_id, election_id, committee_id, committee_name, support_oppose, amount, source_url
      FROM ranked
      WHERE rn <= 5
      ORDER BY candidate_id, election_id, support_oppose, amount DESC, committee_name ASC
    `,
    [JSON.stringify(selectedRequests)]
  );

  const outsideIndustryResult = await db.query<HawaiiFinanceOutsideIndustryRow>(
    `
      WITH selected AS (
        SELECT
          candidate_id::uuid AS candidate_id,
          election_id::uuid AS election_id
        FROM jsonb_to_recordset($1::jsonb) AS x(
          candidate_id text,
          election_id text
        )
      ),
      per_group AS (
        SELECT
          selected.candidate_id::text AS candidate_id,
          selected.election_id::text AS election_id,
          breakdown.committee_id AS committee_id,
          breakdown.support_oppose,
          breakdown.category_name,
          max(breakdown.amount) AS amount,
          CASE
            WHEN count(breakdown.contributor_count) = 0 THEN NULL
            ELSE max(breakdown.contributor_count)
          END AS contributor_count,
          min(breakdown.source_url) FILTER (WHERE breakdown.source_url IS NOT NULL) AS source_url
        FROM selected
        JOIN public.hi_candidate_finance_links AS link
          ON link.candidate_id = selected.candidate_id
         AND link.election_id = selected.election_id
         AND link.link_status = 'active'
        JOIN public.hi_candidate_finance_outside_group_breakdowns AS breakdown
          ON breakdown.link_id = link.id
         AND breakdown.election_year = link.election_year
        WHERE breakdown.category_type = 'industry'
        GROUP BY
          selected.candidate_id,
          selected.election_id,
          breakdown.committee_id,
          breakdown.support_oppose,
          breakdown.category_name
      ),
      grouped AS (
        SELECT
          candidate_id,
          election_id,
          support_oppose,
          category_name,
          sum(amount) AS amount,
          CASE
            WHEN count(contributor_count) = 0 THEN NULL
            ELSE sum(contributor_count)
          END AS contributor_count,
          min(source_url) FILTER (WHERE source_url IS NOT NULL) AS source_url
        FROM per_group
        GROUP BY candidate_id, election_id, support_oppose, category_name
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

  const outsideDonorEvidenceResult = await db.query<HawaiiFinanceOutsideDonorEvidenceRow>(
    `
      WITH selected AS (
        SELECT
          candidate_id::uuid AS candidate_id,
          election_id::uuid AS election_id
        FROM jsonb_to_recordset($1::jsonb) AS x(
          candidate_id text,
          election_id text
        )
      ),
      top_industries_per_group AS (
        SELECT
          selected.candidate_id::text AS candidate_id,
          selected.election_id::text AS election_id,
          industry.committee_id,
          industry.category_name AS industry_name,
          max(industry.amount) AS amount
        FROM selected
        JOIN public.hi_candidate_finance_links AS link
          ON link.candidate_id = selected.candidate_id
         AND link.election_id = selected.election_id
         AND link.link_status = 'active'
        JOIN public.hi_candidate_finance_outside_group_breakdowns AS industry
          ON industry.link_id = link.id
         AND industry.election_year = link.election_year
        WHERE industry.support_oppose = 'support'
          AND industry.category_type = 'industry'
        GROUP BY selected.candidate_id, selected.election_id, industry.committee_id, industry.category_name
      ),
      top_industries_grouped AS (
        SELECT
          candidate_id,
          election_id,
          industry_name,
          sum(amount) AS amount
        FROM top_industries_per_group
        GROUP BY candidate_id, election_id, industry_name
      ),
      top_industries AS (
        SELECT candidate_id, election_id, industry_name
        FROM (
          SELECT
            *,
            row_number() OVER (
              PARTITION BY candidate_id, election_id
              ORDER BY amount DESC, industry_name ASC
            ) AS rn
          FROM top_industries_grouped
        ) ranked_industries
        WHERE rn <= 5
      ),
      evidence AS (
        SELECT
          selected.candidate_id::text AS candidate_id,
          selected.election_id::text AS election_id,
          top_industries.industry_name,
          breakdown.committee_id AS committee_id,
          COALESCE(outside_group.committee_name, breakdown.committee_id) AS committee_name,
          breakdown.support_oppose,
          breakdown.category_name AS organization_name,
          breakdown.amount,
          breakdown.contributor_count,
          COALESCE(breakdown.source_url, outside_group.source_url) AS source_url,
          row_number() OVER (
            PARTITION BY selected.candidate_id, selected.election_id, top_industries.industry_name
            ORDER BY breakdown.amount DESC, breakdown.category_name ASC, breakdown.committee_id ASC
          ) AS rn
        FROM selected
        JOIN top_industries
          ON top_industries.candidate_id = selected.candidate_id::text
         AND top_industries.election_id = selected.election_id::text
        JOIN public.hi_candidate_finance_links AS link
          ON link.candidate_id = selected.candidate_id
         AND link.election_id = selected.election_id
         AND link.link_status = 'active'
        JOIN public.hi_candidate_finance_outside_group_breakdowns AS breakdown
          ON breakdown.link_id = link.id
         AND breakdown.election_year = link.election_year
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
          ON classification.label_type = 'donor'
         AND classification.normalized_label = normalized_breakdown.normalized_label
         AND classification.industry_slug = top_industries.industry_name
        LEFT JOIN public.hi_candidate_finance_outside_groups AS outside_group
          ON outside_group.link_id = breakdown.link_id
         AND outside_group.election_year = breakdown.election_year
         AND outside_group.committee_id = breakdown.committee_id
         AND outside_group.support_oppose = breakdown.support_oppose
        WHERE breakdown.category_type = 'donor'
          AND breakdown.support_oppose = 'support'
      )
      SELECT candidate_id, election_id, industry_name, committee_id, committee_name, support_oppose, organization_name, amount, contributor_count, source_url
      FROM evidence
      WHERE rn <= 3
      ORDER BY candidate_id, election_id, industry_name, amount DESC, organization_name ASC
    `,
    [JSON.stringify(selectedRequests)]
  );

  const directOccupationsByCandidateElection = new Map<string, BallotLookupFinanceBreakdown[]>();
  const contributionSizeBucketsByCandidateElection = new Map<string, BallotLookupFinanceBreakdown[]>();
  const summaryByCandidateElection = new Map(
    summaryResult.rows.map((row) => [candidateElectionKey(row.candidate_id, row.election_id), row])
  );
  for (const row of directBreakdownResult.rows) {
    const summary = summaryByCandidateElection.get(candidateElectionKey(row.candidate_id, row.election_id));
    const targetMap =
      row.category_type === "contribution_size"
        ? contributionSizeBucketsByCandidateElection
        : directOccupationsByCandidateElection;
    addFinanceBreakdown(
      targetMap,
      row.candidate_id,
      row.election_id,
      mapFinanceBreakdown(row, summary?.source_url ?? GENERIC_HAWAII_CSC_SOURCE_URL)
    );
  }

  const supportingGroupsByCandidateElection = new Map<string, BallotLookupFinanceOutsideGroup[]>();
  const opposingGroupsByCandidateElection = new Map<string, BallotLookupFinanceOutsideGroup[]>();
  const outsideGroupNameByCandidateElectionCommittee = new Map<string, string>();
  for (const row of outsideGroupResult.rows) {
    const key = candidateElectionKey(row.candidate_id, row.election_id);
    const map = row.support_oppose === "support" ? supportingGroupsByCandidateElection : opposingGroupsByCandidateElection;
    const list = map.get(key) ?? [];
    list.push({
      committee_id: row.committee_id,
      committee_name: row.committee_name,
      support_oppose: row.support_oppose,
      amount: parseFinanceAmount(row.amount) ?? 0,
      source_url: firstNonEmptySourceUrl(row.source_url, GENERIC_HAWAII_CSC_SOURCE_URL),
    });
    map.set(key, list);
    outsideGroupNameByCandidateElectionCommittee.set(`${key}\u0000${row.committee_id}\u0000${row.support_oppose}`, row.committee_name);
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
      mapFinanceBreakdown(row, GENERIC_HAWAII_CSC_SOURCE_URL)
    );
  }

  const outsideIndustryEvidenceByCandidateElectionAndIndustry = new Map<
    string,
    BallotLookupFinanceOutsideIndustrySupportEvidence[]
  >();
  for (const row of outsideDonorEvidenceResult.rows) {
    const candidateKey = candidateElectionKey(row.candidate_id, row.election_id);
    const evidenceKey = `${candidateKey}\u0000${row.industry_name}`;
    const list = outsideIndustryEvidenceByCandidateElectionAndIndustry.get(evidenceKey) ?? [];
    list.push({
      organization_name: row.organization_name,
      organization_type: "donor",
      amount: parseFinanceAmount(row.amount) ?? 0,
      contributor_count: parseFinanceCount(row.contributor_count),
      committee_id: row.committee_id,
      committee_name:
        outsideGroupNameByCandidateElectionCommittee.get(`${candidateKey}\u0000${row.committee_id}\u0000${row.support_oppose}`) ??
        row.committee_name,
      source_url: firstNonEmptySourceUrl(row.source_url, GENERIC_HAWAII_CSC_SOURCE_URL),
    });
    outsideIndustryEvidenceByCandidateElectionAndIndustry.set(
      evidenceKey,
      list.sort((left, right) => right.amount - left.amount || left.organization_name.localeCompare(right.organization_name)).slice(0, 5)
    );
  }

  return new Map(
    summaryResult.rows.map((row) => {
      const key = candidateElectionKey(row.candidate_id, row.election_id);
      const topDirectDonorOccupations = directOccupationsByCandidateElection.get(key) ?? [];
      const contributionSizeBuckets = contributionSizeBucketsByCandidateElection.get(key) ?? [];
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
          source: "HAWAII_CSC",
          cycle: row.election_year,
          fec_candidate_id: null,
          controlled_committee_id: row.committee_id,
          last_synced_at: row.last_synced_at,
          direct_campaign: {
            total_raised: parseFinanceAmount(row.direct_contribution_total) ?? parseFinanceAmount(row.total_receipts),
            total_spent: parseFinanceAmount(row.total_disbursements),
            cash_on_hand: parseFinanceAmount(row.cash_on_hand),
            debts_owed: null,
            top_occupations: topDirectDonorOccupations,
            top_employers: [],
            top_industries: [],
            contribution_size_buckets: contributionSizeBuckets,
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

async function loadDistrictOfColumbiaCandidateFinanceSummariesByCandidateElection(
  db: Queryable,
  candidateRows: readonly CandidateRow[],
  electionRows: readonly ElectionRow[]
): Promise<Map<string, BallotLookupFinanceSummary>> {
  if (!isDistrictOfColumbiaCampaignFinanceEnabled()) {
    return new Map();
  }

  const requests = buildDistrictOfColumbiaFinanceSummaryRequests(candidateRows, electionRows);
  if (requests.length === 0) {
    return new Map();
  }

  const summaryResult = await db.query<DistrictOfColumbiaFinanceSummaryRow>(
    `
      WITH requested AS (
        SELECT
          candidate_id::uuid AS candidate_id,
          election_id::uuid AS election_id
        FROM jsonb_to_recordset($1::jsonb) AS x(
          candidate_id text,
          election_id text
        )
      )
      SELECT
        requested.candidate_id::text AS candidate_id,
        requested.election_id::text AS election_id,
        CASE
          WHEN count(DISTINCT link.committee_key) = 1 THEN min(link.committee_key)
          ELSE NULL
        END AS committee_id,
        max(summary.election_year) AS election_year,
        CASE WHEN count(summary.total_receipts) = 0 THEN NULL ELSE sum(summary.total_receipts) END AS total_receipts,
        CASE
          WHEN count(summary.direct_contribution_total) = 0 THEN NULL
          ELSE sum(summary.direct_contribution_total)
        END AS direct_contribution_total,
        CASE WHEN count(summary.total_disbursements) = 0 THEN NULL ELSE sum(summary.total_disbursements) END AS total_disbursements,
        CASE WHEN count(summary.cash_on_hand) = 0 THEN NULL ELSE sum(summary.cash_on_hand) END AS cash_on_hand,
        max(summary.outside_support_total) AS outside_support_total,
        max(summary.outside_oppose_total) AS outside_oppose_total,
        min(summary.source_url) FILTER (WHERE summary.source_url IS NOT NULL) AS source_url,
        max(summary.last_synced_at)::text AS last_synced_at
      FROM requested
      JOIN public.dc_candidate_finance_links AS link
        ON link.candidate_id = requested.candidate_id
       AND link.election_id = requested.election_id
       AND link.link_status = 'active'
      JOIN public.dc_candidate_finance_summaries AS summary
        ON summary.link_id = link.id
       AND summary.election_year = link.election_year
      GROUP BY requested.candidate_id, requested.election_id
    `,
    [JSON.stringify(requests)]
  );

  if (summaryResult.rows.length === 0) {
    return new Map();
  }

  const selectedRequests = summaryResult.rows.map((row) => ({
    candidate_id: row.candidate_id,
    election_id: row.election_id,
  }));

  const directBreakdownResult = await db.query<DistrictOfColumbiaFinanceDirectBreakdownRow>(
    `
      WITH selected AS (
        SELECT
          candidate_id::uuid AS candidate_id,
          election_id::uuid AS election_id
        FROM jsonb_to_recordset($1::jsonb) AS x(
          candidate_id text,
          election_id text
        )
      ),
      grouped AS (
        SELECT
          selected.candidate_id::text AS candidate_id,
          selected.election_id::text AS election_id,
          breakdown.category_type,
          breakdown.category_name,
          sum(breakdown.amount) AS amount,
          CASE
            WHEN count(breakdown.contributor_count) = 0 THEN NULL
            ELSE sum(breakdown.contributor_count)
          END AS contributor_count,
          min(breakdown.source_url) FILTER (WHERE breakdown.source_url IS NOT NULL) AS source_url
        FROM selected
        JOIN public.dc_candidate_finance_links AS link
          ON link.candidate_id = selected.candidate_id
         AND link.election_id = selected.election_id
         AND link.link_status = 'active'
        JOIN public.dc_candidate_finance_direct_breakdowns AS breakdown
          ON breakdown.link_id = link.id
         AND breakdown.election_year = link.election_year
        WHERE breakdown.category_type IN ('occupation', 'contribution_size')
        GROUP BY selected.candidate_id, selected.election_id, breakdown.category_type, breakdown.category_name
      ),
      ranked AS (
        SELECT
          *,
          row_number() OVER (
            PARTITION BY candidate_id, election_id, category_type
            ORDER BY amount DESC, category_name ASC
          ) AS rn
        FROM grouped
      )
      SELECT candidate_id, election_id, category_type, category_name, amount, contributor_count, source_url
      FROM ranked
      WHERE rn <= 5
      ORDER BY candidate_id, election_id, category_type, amount DESC, category_name ASC
    `,
    [JSON.stringify(selectedRequests)]
  );

  const outsideGroupResult = await db.query<DistrictOfColumbiaFinanceOutsideGroupRow>(
    `
      WITH selected AS (
        SELECT
          candidate_id::uuid AS candidate_id,
          election_id::uuid AS election_id
        FROM jsonb_to_recordset($1::jsonb) AS x(
          candidate_id text,
          election_id text
        )
      ),
      grouped AS (
        SELECT
          selected.candidate_id::text AS candidate_id,
          selected.election_id::text AS election_id,
          outside_group.committee_key AS committee_id,
          min(outside_group.committee_name) AS committee_name,
          outside_group.support_oppose,
          max(outside_group.amount) AS amount,
          min(outside_group.source_url) FILTER (WHERE outside_group.source_url IS NOT NULL) AS source_url
        FROM selected
        JOIN public.dc_candidate_finance_links AS link
          ON link.candidate_id = selected.candidate_id
         AND link.election_id = selected.election_id
         AND link.link_status = 'active'
        JOIN public.dc_candidate_finance_outside_groups AS outside_group
          ON outside_group.link_id = link.id
         AND outside_group.election_year = link.election_year
        GROUP BY selected.candidate_id, selected.election_id, outside_group.committee_key, outside_group.support_oppose
      ),
      ranked AS (
        SELECT
          *,
          row_number() OVER (
            PARTITION BY candidate_id, election_id, support_oppose
            ORDER BY amount DESC, committee_name ASC
          ) AS rn
        FROM grouped
      )
      SELECT candidate_id, election_id, committee_id, committee_name, support_oppose, amount, source_url
      FROM ranked
      WHERE rn <= 5
      ORDER BY candidate_id, election_id, support_oppose, amount DESC, committee_name ASC
    `,
    [JSON.stringify(selectedRequests)]
  );

  const outsideIndustryResult = await db.query<DistrictOfColumbiaFinanceOutsideIndustryRow>(
    `
      WITH selected AS (
        SELECT
          candidate_id::uuid AS candidate_id,
          election_id::uuid AS election_id
        FROM jsonb_to_recordset($1::jsonb) AS x(
          candidate_id text,
          election_id text
        )
      ),
      per_group AS (
        SELECT
          selected.candidate_id::text AS candidate_id,
          selected.election_id::text AS election_id,
          breakdown.committee_key AS committee_id,
          breakdown.support_oppose,
          breakdown.category_name,
          max(breakdown.amount) AS amount,
          CASE
            WHEN count(breakdown.contributor_count) = 0 THEN NULL
            ELSE max(breakdown.contributor_count)
          END AS contributor_count,
          min(breakdown.source_url) FILTER (WHERE breakdown.source_url IS NOT NULL) AS source_url
        FROM selected
        JOIN public.dc_candidate_finance_links AS link
          ON link.candidate_id = selected.candidate_id
         AND link.election_id = selected.election_id
         AND link.link_status = 'active'
        JOIN public.dc_candidate_finance_outside_group_breakdowns AS breakdown
          ON breakdown.link_id = link.id
         AND breakdown.election_year = link.election_year
        WHERE breakdown.category_type = 'industry'
        GROUP BY
          selected.candidate_id,
          selected.election_id,
          breakdown.committee_key,
          breakdown.support_oppose,
          breakdown.category_name
      ),
      grouped AS (
        SELECT
          candidate_id,
          election_id,
          support_oppose,
          category_name,
          sum(amount) AS amount,
          CASE
            WHEN count(contributor_count) = 0 THEN NULL
            ELSE sum(contributor_count)
          END AS contributor_count,
          min(source_url) FILTER (WHERE source_url IS NOT NULL) AS source_url
        FROM per_group
        GROUP BY candidate_id, election_id, support_oppose, category_name
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

  const outsideDonorEvidenceResult = await db.query<DistrictOfColumbiaFinanceOutsideDonorEvidenceRow>(
    `
      WITH selected AS (
        SELECT
          candidate_id::uuid AS candidate_id,
          election_id::uuid AS election_id
        FROM jsonb_to_recordset($1::jsonb) AS x(
          candidate_id text,
          election_id text
        )
      ),
      top_industries_per_group AS (
        SELECT
          selected.candidate_id::text AS candidate_id,
          selected.election_id::text AS election_id,
          industry.committee_key,
          industry.category_name AS industry_name,
          max(industry.amount) AS amount
        FROM selected
        JOIN public.dc_candidate_finance_links AS link
          ON link.candidate_id = selected.candidate_id
         AND link.election_id = selected.election_id
         AND link.link_status = 'active'
        JOIN public.dc_candidate_finance_outside_group_breakdowns AS industry
          ON industry.link_id = link.id
         AND industry.election_year = link.election_year
        WHERE industry.support_oppose = 'support'
          AND industry.category_type = 'industry'
        GROUP BY selected.candidate_id, selected.election_id, industry.committee_key, industry.category_name
      ),
      top_industries_grouped AS (
        SELECT
          candidate_id,
          election_id,
          industry_name,
          sum(amount) AS amount
        FROM top_industries_per_group
        GROUP BY candidate_id, election_id, industry_name
      ),
      top_industries AS (
        SELECT candidate_id, election_id, industry_name
        FROM (
          SELECT
            *,
            row_number() OVER (
              PARTITION BY candidate_id, election_id
              ORDER BY amount DESC, industry_name ASC
            ) AS rn
          FROM top_industries_grouped
        ) ranked_industries
        WHERE rn <= 5
      ),
      evidence AS (
        SELECT
          selected.candidate_id::text AS candidate_id,
          selected.election_id::text AS election_id,
          top_industries.industry_name,
          breakdown.committee_key AS committee_id,
          COALESCE(outside_group.committee_name, breakdown.committee_key) AS committee_name,
          breakdown.support_oppose,
          breakdown.category_name AS organization_name,
          breakdown.amount,
          breakdown.contributor_count,
          COALESCE(breakdown.source_url, outside_group.source_url) AS source_url,
          row_number() OVER (
            PARTITION BY selected.candidate_id, selected.election_id, top_industries.industry_name
            ORDER BY breakdown.amount DESC, breakdown.category_name ASC, breakdown.committee_key ASC
          ) AS rn
        FROM selected
        JOIN top_industries
          ON top_industries.candidate_id = selected.candidate_id::text
         AND top_industries.election_id = selected.election_id::text
        JOIN public.dc_candidate_finance_links AS link
          ON link.candidate_id = selected.candidate_id
         AND link.election_id = selected.election_id
         AND link.link_status = 'active'
        JOIN public.dc_candidate_finance_outside_group_breakdowns AS breakdown
          ON breakdown.link_id = link.id
         AND breakdown.election_year = link.election_year
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
          ON classification.label_type = 'donor'
         AND classification.normalized_label = normalized_breakdown.normalized_label
         AND classification.industry_slug = top_industries.industry_name
        LEFT JOIN public.dc_candidate_finance_outside_groups AS outside_group
          ON outside_group.link_id = breakdown.link_id
         AND outside_group.election_year = breakdown.election_year
         AND outside_group.committee_key = breakdown.committee_key
         AND outside_group.support_oppose = breakdown.support_oppose
        WHERE breakdown.category_type = 'donor'
          AND breakdown.support_oppose = 'support'
      )
      SELECT candidate_id, election_id, industry_name, committee_id, committee_name, support_oppose, organization_name, amount, contributor_count, source_url
      FROM evidence
      WHERE rn <= 3
      ORDER BY candidate_id, election_id, industry_name, amount DESC, organization_name ASC
    `,
    [JSON.stringify(selectedRequests)]
  );

  const directOccupationsByCandidateElection = new Map<string, BallotLookupFinanceBreakdown[]>();
  const contributionSizeBucketsByCandidateElection = new Map<string, BallotLookupFinanceBreakdown[]>();
  const summaryByCandidateElection = new Map(
    summaryResult.rows.map((row) => [candidateElectionKey(row.candidate_id, row.election_id), row])
  );
  for (const row of directBreakdownResult.rows) {
    const summary = summaryByCandidateElection.get(candidateElectionKey(row.candidate_id, row.election_id));
    const targetMap =
      row.category_type === "contribution_size"
        ? contributionSizeBucketsByCandidateElection
        : directOccupationsByCandidateElection;
    addFinanceBreakdown(
      targetMap,
      row.candidate_id,
      row.election_id,
      mapFinanceBreakdown(row, summary?.source_url ?? GENERIC_DISTRICT_OF_COLUMBIA_OCF_SOURCE_URL)
    );
  }

  const supportingGroupsByCandidateElection = new Map<string, BallotLookupFinanceOutsideGroup[]>();
  const opposingGroupsByCandidateElection = new Map<string, BallotLookupFinanceOutsideGroup[]>();
  const outsideGroupNameByCandidateElectionCommittee = new Map<string, string>();
  for (const row of outsideGroupResult.rows) {
    const key = candidateElectionKey(row.candidate_id, row.election_id);
    const map = row.support_oppose === "support" ? supportingGroupsByCandidateElection : opposingGroupsByCandidateElection;
    const list = map.get(key) ?? [];
    list.push({
      committee_id: row.committee_id,
      committee_name: row.committee_name,
      support_oppose: row.support_oppose,
      amount: parseFinanceAmount(row.amount) ?? 0,
      source_url: firstNonEmptySourceUrl(row.source_url, GENERIC_DISTRICT_OF_COLUMBIA_OCF_SOURCE_URL),
    });
    map.set(key, list);
    outsideGroupNameByCandidateElectionCommittee.set(`${key}\u0000${row.committee_id}\u0000${row.support_oppose}`, row.committee_name);
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
      mapFinanceBreakdown(row, GENERIC_DISTRICT_OF_COLUMBIA_OCF_SOURCE_URL)
    );
  }

  const outsideIndustryEvidenceByCandidateElectionAndIndustry = new Map<
    string,
    BallotLookupFinanceOutsideIndustrySupportEvidence[]
  >();
  for (const row of outsideDonorEvidenceResult.rows) {
    const candidateKey = candidateElectionKey(row.candidate_id, row.election_id);
    const evidenceKey = `${candidateKey}\u0000${row.industry_name}`;
    const list = outsideIndustryEvidenceByCandidateElectionAndIndustry.get(evidenceKey) ?? [];
    list.push({
      organization_name: row.organization_name,
      organization_type: "donor",
      amount: parseFinanceAmount(row.amount) ?? 0,
      contributor_count: parseFinanceCount(row.contributor_count),
      committee_id: row.committee_id,
      committee_name:
        outsideGroupNameByCandidateElectionCommittee.get(`${candidateKey}\u0000${row.committee_id}\u0000${row.support_oppose}`) ??
        row.committee_name,
      source_url: firstNonEmptySourceUrl(row.source_url, GENERIC_DISTRICT_OF_COLUMBIA_OCF_SOURCE_URL),
    });
    outsideIndustryEvidenceByCandidateElectionAndIndustry.set(
      evidenceKey,
      list.sort((left, right) => right.amount - left.amount || left.organization_name.localeCompare(right.organization_name)).slice(0, 5)
    );
  }

  return new Map(
    summaryResult.rows.map((row) => {
      const key = candidateElectionKey(row.candidate_id, row.election_id);
      const topDirectDonorOccupations = directOccupationsByCandidateElection.get(key) ?? [];
      const contributionSizeBuckets = contributionSizeBucketsByCandidateElection.get(key) ?? [];
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
          source: "DISTRICT_OF_COLUMBIA_OCF",
          cycle: row.election_year,
          fec_candidate_id: null,
          controlled_committee_id: row.committee_id,
          last_synced_at: row.last_synced_at,
          direct_campaign: {
            total_raised: parseFinanceAmount(row.direct_contribution_total) ?? parseFinanceAmount(row.total_receipts),
            total_spent: parseFinanceAmount(row.total_disbursements),
            cash_on_hand: parseFinanceAmount(row.cash_on_hand),
            debts_owed: null,
            top_occupations: topDirectDonorOccupations,
            top_employers: [],
            top_industries: [],
            contribution_size_buckets: contributionSizeBuckets,
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

async function loadMassachusettsCandidateFinanceSummariesByCandidateElection(
  db: Queryable,
  candidateRows: readonly CandidateRow[],
  electionRows: readonly ElectionRow[]
): Promise<Map<string, BallotLookupFinanceSummary>> {
  if (!isMassachusettsCampaignFinanceEnabled()) {
    return new Map();
  }

  const requests = buildMassachusettsFinanceSummaryRequests(candidateRows, electionRows);
  if (requests.length === 0) {
    return new Map();
  }

  const summaryResult = await db.query<MassachusettsFinanceSummaryRow>(
    `
      WITH requested AS (
        SELECT
          candidate_id::uuid AS candidate_id,
          election_id::uuid AS election_id
        FROM jsonb_to_recordset($1::jsonb) AS x(
          candidate_id text,
          election_id text
        )
      )
      SELECT
        requested.candidate_id::text AS candidate_id,
        requested.election_id::text AS election_id,
        CASE
          WHEN count(DISTINCT link.candidate_cpf_id) = 1 THEN min(link.candidate_cpf_id)
          ELSE NULL
        END AS committee_id,
        max(summary.election_year) AS election_year,
        CASE WHEN count(summary.total_receipts) = 0 THEN NULL ELSE sum(summary.total_receipts) END AS total_receipts,
        CASE
          WHEN count(summary.direct_contribution_total) = 0 THEN NULL
          ELSE sum(summary.direct_contribution_total)
        END AS direct_contribution_total,
        CASE WHEN count(summary.total_disbursements) = 0 THEN NULL ELSE sum(summary.total_disbursements) END AS total_disbursements,
        CASE WHEN count(summary.cash_on_hand) = 0 THEN NULL ELSE sum(summary.cash_on_hand) END AS cash_on_hand,
        max(summary.outside_support_total) AS outside_support_total,
        max(summary.outside_oppose_total) AS outside_oppose_total,
        min(summary.source_url) FILTER (WHERE summary.source_url IS NOT NULL) AS source_url,
        max(summary.last_synced_at)::text AS last_synced_at
      FROM requested
      JOIN public.ma_candidate_finance_links AS link
        ON link.candidate_id = requested.candidate_id
       AND link.election_id = requested.election_id
       AND link.link_status = 'active'
      JOIN public.ma_candidate_finance_summaries AS summary
        ON summary.link_id = link.id
       AND summary.election_year = link.election_year
      GROUP BY requested.candidate_id, requested.election_id
    `,
    [JSON.stringify(requests)]
  );

  if (summaryResult.rows.length === 0) {
    return new Map();
  }

  const selectedRequests = summaryResult.rows.map((row) => ({
    candidate_id: row.candidate_id,
    election_id: row.election_id,
  }));

  const directBreakdownResult = await db.query<MassachusettsFinanceDirectBreakdownRow>(
    `
      WITH selected AS (
        SELECT
          candidate_id::uuid AS candidate_id,
          election_id::uuid AS election_id
        FROM jsonb_to_recordset($1::jsonb) AS x(
          candidate_id text,
          election_id text
        )
      ),
      grouped AS (
        SELECT
          selected.candidate_id::text AS candidate_id,
          selected.election_id::text AS election_id,
          breakdown.category_type,
          breakdown.category_name,
          sum(breakdown.amount) AS amount,
          CASE
            WHEN count(breakdown.contributor_count) = 0 THEN NULL
            ELSE sum(breakdown.contributor_count)
          END AS contributor_count,
          min(breakdown.source_url) FILTER (WHERE breakdown.source_url IS NOT NULL) AS source_url
        FROM selected
        JOIN public.ma_candidate_finance_links AS link
          ON link.candidate_id = selected.candidate_id
         AND link.election_id = selected.election_id
         AND link.link_status = 'active'
        JOIN public.ma_candidate_finance_direct_breakdowns AS breakdown
          ON breakdown.link_id = link.id
         AND breakdown.election_year = link.election_year
        WHERE breakdown.category_type IN ('occupation', 'contribution_size')
        GROUP BY selected.candidate_id, selected.election_id, breakdown.category_type, breakdown.category_name
      ),
      ranked AS (
        SELECT
          *,
          row_number() OVER (
            PARTITION BY candidate_id, election_id, category_type
            ORDER BY amount DESC, category_name ASC
          ) AS rn
        FROM grouped
      )
      SELECT candidate_id, election_id, category_type, category_name, amount, contributor_count, source_url
      FROM ranked
      WHERE rn <= 5
      ORDER BY candidate_id, election_id, category_type, amount DESC, category_name ASC
    `,
    [JSON.stringify(selectedRequests)]
  );

  const outsideGroupResult = await db.query<MassachusettsFinanceOutsideGroupRow>(
    `
      WITH selected AS (
        SELECT
          candidate_id::uuid AS candidate_id,
          election_id::uuid AS election_id
        FROM jsonb_to_recordset($1::jsonb) AS x(
          candidate_id text,
          election_id text
        )
      ),
      grouped AS (
        SELECT
          selected.candidate_id::text AS candidate_id,
          selected.election_id::text AS election_id,
          outside_group.iepac_cpf_id AS committee_id,
          min(outside_group.iepac_name) AS committee_name,
          outside_group.support_oppose,
          max(outside_group.amount) AS amount,
          min(outside_group.source_url) FILTER (WHERE outside_group.source_url IS NOT NULL) AS source_url
        FROM selected
        JOIN public.ma_candidate_finance_links AS link
          ON link.candidate_id = selected.candidate_id
         AND link.election_id = selected.election_id
         AND link.link_status = 'active'
        JOIN public.ma_candidate_finance_outside_groups AS outside_group
          ON outside_group.link_id = link.id
         AND outside_group.election_year = link.election_year
        GROUP BY selected.candidate_id, selected.election_id, outside_group.iepac_cpf_id, outside_group.support_oppose
      ),
      ranked AS (
        SELECT
          *,
          row_number() OVER (
            PARTITION BY candidate_id, election_id, support_oppose
            ORDER BY amount DESC, committee_name ASC
          ) AS rn
        FROM grouped
      )
      SELECT candidate_id, election_id, committee_id, committee_name, support_oppose, amount, source_url
      FROM ranked
      WHERE rn <= 5
      ORDER BY candidate_id, election_id, support_oppose, amount DESC, committee_name ASC
    `,
    [JSON.stringify(selectedRequests)]
  );

  const outsideIndustryResult = await db.query<MassachusettsFinanceOutsideIndustryRow>(
    `
      WITH selected AS (
        SELECT
          candidate_id::uuid AS candidate_id,
          election_id::uuid AS election_id
        FROM jsonb_to_recordset($1::jsonb) AS x(
          candidate_id text,
          election_id text
        )
      ),
      per_group AS (
        SELECT
          selected.candidate_id::text AS candidate_id,
          selected.election_id::text AS election_id,
          breakdown.iepac_cpf_id,
          breakdown.support_oppose,
          breakdown.category_name,
          max(breakdown.amount) AS amount,
          CASE
            WHEN count(breakdown.contributor_count) = 0 THEN NULL
            ELSE max(breakdown.contributor_count)
          END AS contributor_count,
          min(breakdown.source_url) FILTER (WHERE breakdown.source_url IS NOT NULL) AS source_url
        FROM selected
        JOIN public.ma_candidate_finance_links AS link
          ON link.candidate_id = selected.candidate_id
         AND link.election_id = selected.election_id
         AND link.link_status = 'active'
        JOIN public.ma_candidate_finance_outside_group_breakdowns AS breakdown
          ON breakdown.link_id = link.id
         AND breakdown.election_year = link.election_year
        WHERE breakdown.category_type = 'industry'
        GROUP BY
          selected.candidate_id,
          selected.election_id,
          breakdown.iepac_cpf_id,
          breakdown.support_oppose,
          breakdown.category_name
      ),
      grouped AS (
        SELECT
          candidate_id,
          election_id,
          support_oppose,
          category_name,
          sum(amount) AS amount,
          CASE
            WHEN count(contributor_count) = 0 THEN NULL
            ELSE sum(contributor_count)
          END AS contributor_count,
          min(source_url) FILTER (WHERE source_url IS NOT NULL) AS source_url
        FROM per_group
        GROUP BY candidate_id, election_id, support_oppose, category_name
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

  const outsideDonorEvidenceResult = await db.query<MassachusettsFinanceOutsideDonorEvidenceRow>(
    `
      WITH selected AS (
        SELECT
          candidate_id::uuid AS candidate_id,
          election_id::uuid AS election_id
        FROM jsonb_to_recordset($1::jsonb) AS x(
          candidate_id text,
          election_id text
        )
      ),
      top_industries_per_group AS (
        SELECT
          selected.candidate_id::text AS candidate_id,
          selected.election_id::text AS election_id,
          industry.iepac_cpf_id AS committee_id,
          industry.category_name AS industry_name,
          max(industry.amount) AS amount
        FROM selected
        JOIN public.ma_candidate_finance_links AS link
          ON link.candidate_id = selected.candidate_id
         AND link.election_id = selected.election_id
         AND link.link_status = 'active'
        JOIN public.ma_candidate_finance_outside_group_breakdowns AS industry
          ON industry.link_id = link.id
         AND industry.election_year = link.election_year
        WHERE industry.support_oppose = 'support'
          AND industry.category_type = 'industry'
        GROUP BY selected.candidate_id, selected.election_id, industry.iepac_cpf_id, industry.category_name
      ),
      top_industries_grouped AS (
        SELECT
          candidate_id,
          election_id,
          industry_name,
          sum(amount) AS amount
        FROM top_industries_per_group
        GROUP BY candidate_id, election_id, industry_name
      ),
      top_industries AS (
        SELECT candidate_id, election_id, industry_name
        FROM (
          SELECT
            *,
            row_number() OVER (
              PARTITION BY candidate_id, election_id
              ORDER BY amount DESC, industry_name ASC
            ) AS rn
          FROM top_industries_grouped
        ) ranked_industries
        WHERE rn <= 5
      ),
      evidence AS (
        SELECT
          selected.candidate_id::text AS candidate_id,
          selected.election_id::text AS election_id,
          top_industries.industry_name,
          breakdown.iepac_cpf_id AS committee_id,
          COALESCE(outside_group.iepac_name, breakdown.iepac_cpf_id) AS committee_name,
          breakdown.support_oppose,
          breakdown.category_name AS organization_name,
          breakdown.amount,
          breakdown.contributor_count,
          COALESCE(breakdown.source_url, outside_group.source_url) AS source_url,
          row_number() OVER (
            PARTITION BY selected.candidate_id, selected.election_id, top_industries.industry_name
            ORDER BY breakdown.amount DESC, breakdown.category_name ASC, breakdown.iepac_cpf_id ASC
          ) AS rn
        FROM selected
        JOIN top_industries
          ON top_industries.candidate_id = selected.candidate_id::text
         AND top_industries.election_id = selected.election_id::text
        JOIN public.ma_candidate_finance_links AS link
          ON link.candidate_id = selected.candidate_id
         AND link.election_id = selected.election_id
         AND link.link_status = 'active'
        JOIN public.ma_candidate_finance_outside_group_breakdowns AS breakdown
          ON breakdown.link_id = link.id
         AND breakdown.election_year = link.election_year
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
          ON classification.label_type = 'donor'
         AND classification.normalized_label = normalized_breakdown.normalized_label
         AND classification.industry_slug = top_industries.industry_name
        LEFT JOIN public.ma_candidate_finance_outside_groups AS outside_group
          ON outside_group.link_id = breakdown.link_id
         AND outside_group.election_year = breakdown.election_year
         AND outside_group.iepac_cpf_id = breakdown.iepac_cpf_id
         AND outside_group.support_oppose = breakdown.support_oppose
        WHERE breakdown.category_type = 'donor'
          AND breakdown.support_oppose = 'support'
      )
      SELECT candidate_id, election_id, industry_name, committee_id, committee_name, support_oppose, organization_name, amount, contributor_count, source_url
      FROM evidence
      WHERE rn <= 3
      ORDER BY candidate_id, election_id, industry_name, amount DESC, organization_name ASC
    `,
    [JSON.stringify(selectedRequests)]
  );

  const directOccupationsByCandidateElection = new Map<string, BallotLookupFinanceBreakdown[]>();
  const contributionSizeBucketsByCandidateElection = new Map<string, BallotLookupFinanceBreakdown[]>();
  const summaryByCandidateElection = new Map(
    summaryResult.rows.map((row) => [candidateElectionKey(row.candidate_id, row.election_id), row])
  );
  for (const row of directBreakdownResult.rows) {
    const summary = summaryByCandidateElection.get(candidateElectionKey(row.candidate_id, row.election_id));
    const targetMap =
      row.category_type === "contribution_size"
        ? contributionSizeBucketsByCandidateElection
        : directOccupationsByCandidateElection;
    addFinanceBreakdown(
      targetMap,
      row.candidate_id,
      row.election_id,
      mapFinanceBreakdown(row, summary?.source_url ?? GENERIC_MASSACHUSETTS_OCPF_SOURCE_URL)
    );
  }

  const supportingGroupsByCandidateElection = new Map<string, BallotLookupFinanceOutsideGroup[]>();
  const opposingGroupsByCandidateElection = new Map<string, BallotLookupFinanceOutsideGroup[]>();
  const outsideGroupNameByCandidateElectionCommittee = new Map<string, string>();
  for (const row of outsideGroupResult.rows) {
    const key = candidateElectionKey(row.candidate_id, row.election_id);
    const map = row.support_oppose === "support" ? supportingGroupsByCandidateElection : opposingGroupsByCandidateElection;
    const list = map.get(key) ?? [];
    list.push({
      committee_id: row.committee_id,
      committee_name: row.committee_name,
      support_oppose: row.support_oppose,
      amount: parseFinanceAmount(row.amount) ?? 0,
      source_url: firstNonEmptySourceUrl(row.source_url, GENERIC_MASSACHUSETTS_OCPF_SOURCE_URL),
    });
    map.set(key, list);
    outsideGroupNameByCandidateElectionCommittee.set(`${key}\u0000${row.committee_id}\u0000${row.support_oppose}`, row.committee_name);
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
      mapFinanceBreakdown(row, GENERIC_MASSACHUSETTS_OCPF_SOURCE_URL)
    );
  }

  const outsideIndustryEvidenceByCandidateElectionAndIndustry = new Map<
    string,
    BallotLookupFinanceOutsideIndustrySupportEvidence[]
  >();
  for (const row of outsideDonorEvidenceResult.rows) {
    const candidateKey = candidateElectionKey(row.candidate_id, row.election_id);
    const evidenceKey = `${candidateKey}\u0000${row.industry_name}`;
    const list = outsideIndustryEvidenceByCandidateElectionAndIndustry.get(evidenceKey) ?? [];
    list.push({
      organization_name: row.organization_name,
      organization_type: "donor",
      amount: parseFinanceAmount(row.amount) ?? 0,
      contributor_count: parseFinanceCount(row.contributor_count),
      committee_id: row.committee_id,
      committee_name:
        outsideGroupNameByCandidateElectionCommittee.get(`${candidateKey}\u0000${row.committee_id}\u0000${row.support_oppose}`) ??
        row.committee_name,
      source_url: firstNonEmptySourceUrl(row.source_url, GENERIC_MASSACHUSETTS_OCPF_SOURCE_URL),
    });
    outsideIndustryEvidenceByCandidateElectionAndIndustry.set(
      evidenceKey,
      list.sort((left, right) => right.amount - left.amount || left.organization_name.localeCompare(right.organization_name)).slice(0, 5)
    );
  }

  return new Map(
    summaryResult.rows.map((row) => {
      const key = candidateElectionKey(row.candidate_id, row.election_id);
      const topDirectDonorOccupations = directOccupationsByCandidateElection.get(key) ?? [];
      const contributionSizeBuckets = contributionSizeBucketsByCandidateElection.get(key) ?? [];
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
          source: "MASSACHUSETTS_OCPF",
          cycle: row.election_year,
          fec_candidate_id: null,
          controlled_committee_id: row.committee_id,
          last_synced_at: row.last_synced_at,
          direct_campaign: {
            total_raised: parseFinanceAmount(row.direct_contribution_total) ?? parseFinanceAmount(row.total_receipts),
            total_spent: parseFinanceAmount(row.total_disbursements),
            cash_on_hand: parseFinanceAmount(row.cash_on_hand),
            debts_owed: null,
            top_occupations: topDirectDonorOccupations,
            top_employers: [],
            top_industries: [],
            contribution_size_buckets: contributionSizeBuckets,
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

async function loadVirginiaCandidateFinanceSummariesByCandidateElection(
  db: Queryable,
  candidateRows: readonly CandidateRow[],
  electionRows: readonly ElectionRow[]
): Promise<Map<string, BallotLookupFinanceSummary>> {
  if (!isVirginiaCampaignFinanceEnabled()) {
    return new Map();
  }

  const requests = buildVirginiaFinanceSummaryRequests(candidateRows, electionRows);
  if (requests.length === 0) {
    return new Map();
  }

  const summaryResult = await db.query<VirginiaFinanceSummaryRow>(
    `
      WITH requested AS (
        SELECT
          candidate_id::uuid AS candidate_id,
          election_id::uuid AS election_id
        FROM jsonb_to_recordset($1::jsonb) AS x(
          candidate_id text,
          election_id text
        )
      )
      SELECT
        requested.candidate_id::text AS candidate_id,
        requested.election_id::text AS election_id,
        CASE
          WHEN count(DISTINCT link.committee_id) = 1 THEN min(link.committee_id)
          ELSE NULL
        END AS committee_id,
        max(summary.election_year) AS election_year,
        CASE WHEN count(summary.total_receipts) = 0 THEN NULL ELSE sum(summary.total_receipts) END AS total_receipts,
        CASE
          WHEN count(summary.direct_contribution_total) = 0 THEN NULL
          ELSE sum(summary.direct_contribution_total)
        END AS direct_contribution_total,
        min(summary.source_url) FILTER (WHERE summary.source_url IS NOT NULL) AS source_url,
        max(summary.last_synced_at)::text AS last_synced_at
      FROM requested
      JOIN public.va_candidate_finance_links AS link
        ON link.candidate_id = requested.candidate_id
       AND link.election_id = requested.election_id
       AND link.link_status = 'active'
      JOIN public.va_candidate_finance_summaries AS summary
        ON summary.link_id = link.id
       AND summary.election_year = link.election_year
      GROUP BY requested.candidate_id, requested.election_id
    `,
    [JSON.stringify(requests)]
  );

  if (summaryResult.rows.length === 0) {
    return new Map();
  }

  const selectedRequests = summaryResult.rows.map((row) => ({
    candidate_id: row.candidate_id,
    election_id: row.election_id,
  }));

  const directBreakdownResult = await db.query<VirginiaFinanceDirectBreakdownRow>(
    `
      WITH selected AS (
        SELECT
          candidate_id::uuid AS candidate_id,
          election_id::uuid AS election_id
        FROM jsonb_to_recordset($1::jsonb) AS x(
          candidate_id text,
          election_id text
        )
      ),
      grouped AS (
        SELECT
          selected.candidate_id::text AS candidate_id,
          selected.election_id::text AS election_id,
          breakdown.category_type,
          breakdown.category_name,
          sum(breakdown.amount) AS amount,
          CASE
            WHEN count(breakdown.contributor_count) = 0 THEN NULL
            ELSE sum(breakdown.contributor_count)
          END AS contributor_count,
          min(breakdown.source_url) FILTER (WHERE breakdown.source_url IS NOT NULL) AS source_url
        FROM selected
        JOIN public.va_candidate_finance_links AS link
          ON link.candidate_id = selected.candidate_id
         AND link.election_id = selected.election_id
         AND link.link_status = 'active'
        JOIN public.va_candidate_finance_direct_breakdowns AS breakdown
          ON breakdown.link_id = link.id
         AND breakdown.election_year = link.election_year
        WHERE breakdown.category_type IN ('occupation', 'contribution_size')
        GROUP BY selected.candidate_id, selected.election_id, breakdown.category_type, breakdown.category_name
      ),
      ranked AS (
        SELECT
          *,
          row_number() OVER (
            PARTITION BY candidate_id, election_id, category_type
            ORDER BY amount DESC, category_name ASC
          ) AS rn
        FROM grouped
      )
      SELECT candidate_id, election_id, category_type, category_name, amount, contributor_count, source_url
      FROM ranked
      WHERE rn <= 5
      ORDER BY candidate_id, election_id, category_type, amount DESC, category_name ASC
    `,
    [JSON.stringify(selectedRequests)]
  );

  const directOccupationsByCandidateElection = new Map<string, BallotLookupFinanceBreakdown[]>();
  const contributionSizeBucketsByCandidateElection = new Map<string, BallotLookupFinanceBreakdown[]>();
  const summaryByCandidateElection = new Map(
    summaryResult.rows.map((row) => [candidateElectionKey(row.candidate_id, row.election_id), row])
  );
  for (const row of directBreakdownResult.rows) {
    const summary = summaryByCandidateElection.get(candidateElectionKey(row.candidate_id, row.election_id));
    const targetMap =
      row.category_type === "contribution_size"
        ? contributionSizeBucketsByCandidateElection
        : directOccupationsByCandidateElection;
    addFinanceBreakdown(
      targetMap,
      row.candidate_id,
      row.election_id,
      mapFinanceBreakdown(row, summary?.source_url ?? GENERIC_VIRGINIA_CFREPORTS_SOURCE_URL)
    );
  }

  return new Map(
    summaryResult.rows.map((row) => {
      const key = candidateElectionKey(row.candidate_id, row.election_id);
      const topDirectDonorOccupations = directOccupationsByCandidateElection.get(key) ?? [];
      return [
        key,
        {
          source: "VIRGINIA_CFREPORTS",
          cycle: row.election_year,
          fec_candidate_id: null,
          controlled_committee_id: row.committee_id,
          last_synced_at: row.last_synced_at,
          direct_campaign: {
            total_raised: parseFinanceAmount(row.direct_contribution_total) ?? parseFinanceAmount(row.total_receipts),
            total_spent: null,
            cash_on_hand: null,
            debts_owed: null,
            top_occupations: topDirectDonorOccupations,
            top_employers: [],
            top_industries: [],
            contribution_size_buckets: contributionSizeBucketsByCandidateElection.get(key) ?? [],
          },
          outside_spending: {
            support_total: null,
            oppose_total: null,
            top_supporting_groups: [],
            top_opposing_groups: [],
            top_supporting_industries: [],
            top_opposing_industries: [],
          },
          backing_summary: {
            top_direct_donor_occupations: topDirectDonorOccupations,
            top_outside_supporting_industries: [],
          },
        } satisfies BallotLookupFinanceSummary,
      ];
    })
  );
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
  const marylandSummaries = await loadMarylandCandidateFinanceSummariesByCandidateElection(db, candidateRows, electionRows);
  const michiganSummaries = await loadMichiganCandidateFinanceSummariesByCandidateElection(db, candidateRows, electionRows);
  const washingtonSummaries = await loadWashingtonCandidateFinanceSummariesByCandidateElection(db, candidateRows, electionRows);
  const hawaiiSummaries = await loadHawaiiCandidateFinanceSummariesByCandidateElection(db, candidateRows, electionRows);
  const districtOfColumbiaSummaries = await loadDistrictOfColumbiaCandidateFinanceSummariesByCandidateElection(
    db,
    candidateRows,
    electionRows
  );
  const virginiaSummaries = await loadVirginiaCandidateFinanceSummariesByCandidateElection(db, candidateRows, electionRows);
  const texasSummaries = await loadTexasCandidateFinanceSummariesByCandidateElection(db, candidateRows, electionRows);
  const nebraskaSummaries = await loadNebraskaCandidateFinanceSummariesByCandidateElection(db, candidateRows, electionRows);
  const oklahomaSummaries = await loadOklahomaCandidateFinanceSummariesByCandidateElection(db, candidateRows, electionRows);
  const newMexicoSummaries = await loadNewMexicoCandidateFinanceSummariesByCandidateElection(db, candidateRows, electionRows);
  const connecticutSummaries = await loadConnecticutCandidateFinanceSummariesByCandidateElection(db, candidateRows, electionRows);
  const coloradoSummaries = await loadColoradoCandidateFinanceSummariesByCandidateElection(db, candidateRows, electionRows);
  const californiaSummaries = await loadCaliforniaCandidateFinanceSummariesByCandidateElection(db, candidateRows, electionRows);
  const fecSummaries = await loadFecCandidateFinanceSummariesByCandidateElection(db, candidateRows, electionRows);

  const merged = new Map(wisconsinSummaries);
  for (const [key, summary] of massachusettsSummaries) {
    merged.set(key, summary);
  }
  for (const [key, summary] of marylandSummaries) {
    merged.set(key, summary);
  }
  for (const [key, summary] of michiganSummaries) {
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
  for (const [key, summary] of virginiaSummaries) {
    merged.set(key, summary);
  }
  for (const [key, summary] of texasSummaries) {
    merged.set(key, summary);
  }
  for (const [key, summary] of nebraskaSummaries) {
    merged.set(key, summary);
  }
  for (const [key, summary] of oklahomaSummaries) {
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
          c.state_filing_ids
        FROM public.candidate_elections AS ce
        JOIN public.candidates AS c
          ON c.id = ce.candidate_id
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
      records: candidateRecordsByCandidate.get(row.candidate_id) ?? [],
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
        representation_power_score
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
      research_areas: row.office_id
        ? (researchAreasByOffice.get(row.office_id) ?? []).map((area) => ({
            id: area.research_area_id,
            slug: area.slug,
            name: area.name,
            description: area.description,
          }))
        : [],
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
        e.race_type,
        e.official_ballot_title,
        e.election_date::text AS election_date,
        e.election_stage,
        e.is_partisan,
        e.discovery_contest_family,
        e.sources,
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
