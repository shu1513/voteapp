// Finance types and helpers shared by ballotLookup.ts and the per-state
// ballot-lookup finance loaders (plan-ballot-lookup.md Phase 1). Lives in its
// own module so state loaders can import these without importing — or being
// imported by — the ballot lookup itself; everything here moved verbatim
// from ballotLookup.ts.

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
  expenditure_count?: number | null;
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

export type BallotLookupFinanceSupportingCommitteeIndustrySummary = BallotLookupFinanceBreakdown & {
  supporting_committee_name: string;
};

export type BallotLookupFinanceBackingSummary = {
  top_direct_donor_occupations: BallotLookupFinanceBreakdown[];
  top_outside_supporting_industries: BallotLookupFinanceOutsideIndustrySupportSummary[];
  top_pac_backed_industries?: BallotLookupFinanceOutsideIndustrySupportSummary[];
  top_supporting_committee_industries?: BallotLookupFinanceSupportingCommitteeIndustrySummary[];
};

export type BallotLookupFinanceSummary = {
  source:
    | "FEC"
    | "ARIZONA_SOS"
    | "CALIFORNIA_SOS"
    | "LOS_ANGELES_CITY_ETHICS"
    | "COLORADO_TRACER"
    | "CONNECTICUT_ECRIS"
    | "INDIANA_CAMPAIGN_FINANCE"
    | "NEBRASKA_NADC"
    | "NEW_JERSEY_ELEC"
    | "NEW_MEXICO_CFIS"
    | "NEW_YORK_SODA"
    | "NEW_YORK_CITY_CFB"
    | "OKLAHOMA_GUARDIAN"
    | "TEXAS_TEC"
    | "HOUSTON_CAMPAIGN_FINANCE"
    | "FLORIDA_DOS"
    | "UTAH_DISCLOSURES"
    | "HAWAII_CSC"
    | "VIRGINIA_CFREPORTS"
    | "TENNESSEE_CAMP"
    | "WASHINGTON_PDC"
    | "WISCONSIN_SUNSHINE"
    | "MASSACHUSETTS_OCPF"
    | "VERMONT_CFD"
    | "LOUISIANA_ETHICS"
    | "KENTUCKY_KREF"
    | "MARYLAND_CFS"
    | "MAINE_CFIS"
    | "MICHIGAN_MITN"
    | "ILLINOIS_SBE"
    | "MINNESOTA_CFB"
    | "ALASKA_APOC"
    | "ORESTAR"
    | "PENNSYLVANIA_DOS"
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
    public_funds_received?: number | null;
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

export function parseFinanceAmount(value: string | number | null | undefined): number | null {
  if (value === null || value === undefined) {
    return null;
  }
  const parsed = typeof value === "number" ? value : Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

export function parseFinanceCount(value: string | number | null | undefined): number | null {
  const parsed = parseFinanceAmount(value);
  return parsed === null ? null : Math.trunc(parsed);
}

export function candidateElectionKey(candidateId: string, electionId: string): string {
  return `${candidateId}\u0000${electionId}`;
}

export function firstNonEmptySourceUrl(...urls: Array<string | null | undefined>): string | null {
  for (const url of urls) {
    const trimmed = url?.trim();
    if (trimmed) {
      return trimmed;
    }
  }
  return null;
}

export function mapFinanceBreakdown(
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

export function addFinanceBreakdown(
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

export function formatShortList(values: readonly string[]): string {
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

export const FINANCE_INDUSTRY_DISPLAY_NAMES: Record<string, string> = {
  agriculture_and_food: "Agriculture and food",
  business_associations: "Business associations",
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

export function financeIndustryDisplayName(industryName: string): string {
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

export function buildOutsideIndustrySupportExplanation(
  industryName: string,
  evidence: readonly BallotLookupFinanceOutsideIndustrySupportEvidence[],
  supportAction = "independent spending supporting this candidate"
): string {
  const displayName = financeIndustryDisplayName(industryName);
  if (evidence.length === 0) {
    return `The ${displayName} category is a top outside-spending support industry because organizations classified in this industry contributed to outside groups that reported ${supportAction}.`;
  }

  return `The ${displayName} category is a top outside-spending support industry because ${formatShortList(
    evidence.map((item) => item.organization_name)
  )} contributed to ${formatShortList(
    evidence.map((item) => item.committee_name)
  )}, which reported ${supportAction}.`;
}

export type StateFinanceSummaryRequest = {
  candidate_id: string;
  election_id: string;
};

export type StateFinanceRequestCandidateRow = {
  candidate_id: string;
  election_id: string;
};

export type StateFinanceRequestElectionRow = {
  election_id: string;
  state: string;
  district_type?: string | null;
  geoid_compact?: string | null;
  office_scope?: string | null;
  office_canonical_name?: string | null;
};

/**
 * The request builder every state's ballot-lookup finance loader shares:
 * take the elections in this state (optionally narrowed to offices the
 * state's finance program covers), and emit one deduped
 * candidate/election request per candidate in them. Replaces 22
 * per-state copies that differed only in the state code and, for some,
 * the eligibility predicate. Both sides of the state match are
 * normalized with trim().toUpperCase() — two of the old copies (CO, CT)
 * compared row.state raw, but districts.state is uniformly normalized
 * (verified: 0 of 51 states differ), so the normalized compare is
 * behavior-identical on real data; normalizing stateCode too keeps a
 * future lowercase caller from silently matching nothing.
 */
export function buildStateFinanceSummaryRequests<TElectionRow extends StateFinanceRequestElectionRow>(
  stateCode: string,
  candidateRows: readonly StateFinanceRequestCandidateRow[],
  electionRows: readonly TElectionRow[],
  isEligibleElection?: (row: TElectionRow) => boolean
): StateFinanceSummaryRequest[] {
  const normalizedStateCode = stateCode.trim().toUpperCase();
  const electionIds = new Set(
    electionRows
      .filter((row) => row.state.trim().toUpperCase() === normalizedStateCode && (isEligibleElection?.(row) ?? true))
      .map((row) => row.election_id)
  );
  const requests = new Map<string, StateFinanceSummaryRequest>();
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

// The SQL row shapes most state ballot-lookup finance loaders share: 15 of
// the 22 states (the Texas-derived family, plus Virginia's identical direct
// breakdown) read their summary/breakdown/outside-spending tables into these
// exact shapes. States whose disclosure system returns different columns
// (e.g. California, New Mexico, New Jersey) keep their own row types beside
// their loader.

export type StateFinanceSummaryRow = {
  candidate_id: string;
  election_id: string;
  committee_id: string | null;
  election_year: number;
  total_receipts: string | number | null;
  direct_contribution_total: string | number | null;
  total_disbursements: string | number | null;
  cash_on_hand: string | number | null;
  debts_owed?: string | number | null;
  outside_support_total: string | number | null;
  outside_oppose_total: string | number | null;
  source_url: string | null;
  last_synced_at: string;
};

export type StateFinanceDirectBreakdownRow = {
  candidate_id: string;
  election_id: string;
  category_type: "occupation" | "contribution_size" | "industry";
  category_name: string;
  amount: string | number;
  contributor_count: string | number | null;
  source_url: string | null;
};

export type StateFinanceOutsideGroupRow = {
  candidate_id: string;
  election_id: string;
  committee_id: string;
  committee_name: string;
  support_oppose: "support" | "oppose";
  amount: string | number;
  expenditure_count?: string | number | null;
  source_url: string | null;
};

export type StateFinanceOutsideIndustryRow = {
  candidate_id: string;
  election_id: string;
  support_oppose: "support" | "oppose";
  category_name: string;
  amount: string | number;
  contributor_count: string | number | null;
  source_url: string | null;
};

export type StateFinanceOutsideDonorEvidenceRow = {
  candidate_id: string;
  election_id: string;
  industry_name: string;
  committee_id: string;
  committee_name: string;
  support_oppose: "support" | "oppose";
  organization_name: string;
  organization_type?: "employer" | "donor";
  amount: string | number;
  contributor_count: string | number | null;
  source_url: string | null;
};

// Used by the finance request builders (cycle = election year) and the
// ballot summaries assembly.

export function electionYear(electionDate: string): number | null {
  const year = Number.parseInt(electionDate.slice(0, 4), 10);
  return Number.isInteger(year) ? year : null;
}

// Adapter between the election rows ballot lookup loads and the
// {officeScope, officeCanonicalName} input every state eligible-office
// predicate takes.
export function officeInputFromElectionRow(row: StateFinanceRequestElectionRow): {
  officeScope: string | null;
  officeCanonicalName: string | null;
} {
  return {
    officeScope: row.office_scope ?? null,
    officeCanonicalName: row.office_canonical_name ?? null,
  };
}
