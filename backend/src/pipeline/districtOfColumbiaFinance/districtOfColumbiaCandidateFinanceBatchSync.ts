import type { Pool, PoolClient } from "pg";

import type { FinanceIndustryClassifier } from "../finance/financeIndustryClassificationService.js";
import {
  autoLinkMissingDistrictOfColumbiaCandidateFinanceLinks,
  listDistrictOfColumbiaCandidateElectionsMissingFinanceLinks,
  type DistrictOfColumbiaCandidateCommitteeResolver,
  type DistrictOfColumbiaFinanceAutoLinkCandidateElection,
} from "./districtOfColumbiaCandidateFinanceAutoLink.js";
import {
  syncDistrictOfColumbiaCandidateFinance,
  type DistrictOfColumbiaCandidateFinanceSyncResult,
} from "./districtOfColumbiaCandidateFinanceSync.js";
import {
  resolveDistrictOfColumbiaCandidateCommittee,
  type DistrictOfColumbiaCandidateCommitteeResolution,
} from "./districtOfColumbiaCandidateCommitteeResolver.js";
import { DISTRICT_OF_COLUMBIA_FINANCE_ELIGIBLE_OFFICE_KEYS } from "./districtOfColumbiaFinanceEligibleOffices.js";
import {
  buildDistrictOfColumbiaOcfDataDownloadUrl,
  downloadIndependentExpenditureContributions,
  downloadIndependentExpenditureExpenditures,
  downloadPrincipalCampaignContributions,
  type DistrictOfColumbiaOcfClientOptions,
  type DistrictOfColumbiaOcfContributionRecord,
  type DistrictOfColumbiaOcfExpenditureRecord,
} from "./districtOfColumbiaOcfClient.js";

type Queryable = Pick<Pool | PoolClient, "query">;
type ConnectableQueryable = Queryable & Pick<Pool, "connect">;

export type DistrictOfColumbiaCandidateFinanceDueRow = {
  candidateId: string;
  electionId: string;
  candidateName: string;
  electionYear: number;
  officeScope: string;
  officeName: string;
  district: string | null;
  committeeKey: string;
  committeeName: string;
  sourceUrl: string | null;
  lastSyncedAt: string | null;
};

export type DistrictOfColumbiaOcfDataForYear = {
  year: number;
  sourceUrl: string;
  principalContributionRecords: readonly DistrictOfColumbiaOcfContributionRecord[];
  independentExpenditureRecords: readonly DistrictOfColumbiaOcfExpenditureRecord[];
  independentExpenditureContributionRecords: readonly DistrictOfColumbiaOcfContributionRecord[];
};

export type DistrictOfColumbiaCandidateFinanceBatchSyncInput = {
  db: ConnectableQueryable;
  now?: Date;
  dryRun?: boolean;
  maxCandidates?: number;
  staleAfterDays?: number;
  electionLookbackDays?: number;
  electionLookaheadDays?: number;
  ocfClientOptions?: DistrictOfColumbiaOcfClientOptions;
  ocfDataByYear?: ReadonlyMap<number, DistrictOfColumbiaOcfDataForYear>;
  autoLinkMissingLinks?: boolean;
  financeIndustryClassifier?: FinanceIndustryClassifier;
  aiClassificationMinAmount?: number;
  syncDistrictOfColumbiaCandidateFinanceFn?: typeof syncDistrictOfColumbiaCandidateFinance;
  resolveCandidateCommittee?: DistrictOfColumbiaCandidateCommitteeResolver;
};

export type DistrictOfColumbiaCandidateFinanceBatchSyncItemResult = {
  candidateId: string;
  electionId: string;
  electionYear: number;
  committeeKey: string;
  ok: boolean;
  result?: DistrictOfColumbiaCandidateFinanceSyncResult;
  error?: string;
};

export type DistrictOfColumbiaCandidateFinanceBatchSyncResult = {
  dryRun: boolean;
  now: string;
  staleAfterDays: number;
  maxCandidates: number;
  dueCandidateCount: number;
  selectedCandidateCount: number;
  syncedCandidateCount: number;
  failedCandidateCount: number;
  autoLinkAttemptedCount: number;
  autoLinkLinkedCount: number;
  results: DistrictOfColumbiaCandidateFinanceBatchSyncItemResult[];
};

type DistrictOfColumbiaCandidateFinanceDueQueryRow = {
  candidate_id: string;
  election_id: string;
  candidate_name: string;
  election_year: number;
  office_scope: string;
  office_name: string;
  district: string | null;
  committee_key: string;
  committee_name: string;
  source_url: string | null;
  last_synced_at: string | null;
  total_due_rows: string | number;
};

const DEFAULT_MAX_CANDIDATES = 25;
const DEFAULT_STALE_AFTER_DAYS = 7;
// Keep one extra calendar day so UTC scheduler timing cannot skip election-night finance syncs.
const DEFAULT_POST_ELECTION_FINANCE_SYNC_GRACE_DAYS = 1;
const DEFAULT_ELECTION_LOOKAHEAD_DAYS = 730;

function assertValidDate(date: Date, label: string): void {
  if (!(date instanceof Date) || Number.isNaN(date.getTime())) {
    throw new Error(`Invalid D.C. finance batch sync ${label}`);
  }
}

function normalizePositiveInteger(value: number | undefined, fallback: number, label: string): number {
  const normalized = value ?? fallback;
  if (!Number.isInteger(normalized) || normalized <= 0) {
    throw new Error(`Invalid D.C. finance batch sync ${label}: ${value}`);
  }
  return normalized;
}

function normalizeCommitteeKey(value: string): string {
  return value.trim().replace(/\s+/g, " ").toUpperCase();
}

function parseTotalDueRows(value: string | number | undefined): number {
  const parsed = typeof value === "number" ? value : Number(value);
  return Number.isSafeInteger(parsed) && parsed >= 0 ? parsed : 0;
}

function mapDueRow(row: DistrictOfColumbiaCandidateFinanceDueQueryRow): DistrictOfColumbiaCandidateFinanceDueRow {
  return {
    candidateId: row.candidate_id,
    electionId: row.election_id,
    candidateName: row.candidate_name,
    electionYear: row.election_year,
    officeScope: row.office_scope,
    officeName: row.office_name,
    district: row.district,
    committeeKey: row.committee_key,
    committeeName: row.committee_name,
    sourceUrl: row.source_url,
    lastSyncedAt: row.last_synced_at,
  };
}

function cycleStartDate(electionYear: number): string {
  return `01/01/${electionYear - 1}`;
}

function cycleEndDate(electionYear: number): string {
  return `12/31/${electionYear}`;
}

function groupDueRowsByYear(
  rows: readonly DistrictOfColumbiaCandidateFinanceDueRow[]
): Map<number, DistrictOfColumbiaCandidateFinanceDueRow[]> {
  const byYear = new Map<number, DistrictOfColumbiaCandidateFinanceDueRow[]>();
  for (const row of rows) {
    const yearRows = byYear.get(row.electionYear) ?? [];
    yearRows.push(row);
    byYear.set(row.electionYear, yearRows);
  }
  return byYear;
}

function groupAutoLinkCandidatesByYear(
  rows: readonly DistrictOfColumbiaFinanceAutoLinkCandidateElection[]
): Map<number, DistrictOfColumbiaFinanceAutoLinkCandidateElection[]> {
  const byYear = new Map<number, DistrictOfColumbiaFinanceAutoLinkCandidateElection[]>();
  for (const row of rows) {
    const yearRows = byYear.get(row.electionYear) ?? [];
    yearRows.push(row);
    byYear.set(row.electionYear, yearRows);
  }
  return byYear;
}

async function loadPrincipalContributionDataForYear(input: {
  year: number;
  ocfClientOptions?: DistrictOfColumbiaOcfClientOptions;
}): Promise<Pick<DistrictOfColumbiaOcfDataForYear, "year" | "sourceUrl" | "principalContributionRecords">> {
  return {
    year: input.year,
    sourceUrl: buildDistrictOfColumbiaOcfDataDownloadUrl(),
    principalContributionRecords: await downloadPrincipalCampaignContributions(
      { fromDate: cycleStartDate(input.year), toDate: cycleEndDate(input.year) },
      input.ocfClientOptions
    ),
  };
}

async function loadOcfDataForYear(input: {
  year: number;
  ocfClientOptions?: DistrictOfColumbiaOcfClientOptions;
}): Promise<DistrictOfColumbiaOcfDataForYear> {
  const [principalContributionData, independentExpenditureRecords, independentExpenditureContributionRecords] =
    await Promise.all([
      loadPrincipalContributionDataForYear(input),
      downloadIndependentExpenditureExpenditures(
        { fromDate: cycleStartDate(input.year), toDate: cycleEndDate(input.year) },
        input.ocfClientOptions
      ),
      downloadIndependentExpenditureContributions(
        { fromDate: cycleStartDate(input.year), toDate: cycleEndDate(input.year) },
        input.ocfClientOptions
      ),
    ]);

  return {
    year: input.year,
    sourceUrl: principalContributionData.sourceUrl,
    principalContributionRecords: principalContributionData.principalContributionRecords,
    independentExpenditureRecords,
    independentExpenditureContributionRecords,
  };
}

async function getOrLoadOcfDataForYear(input: {
  year: number;
  dataByYear: Map<number, DistrictOfColumbiaOcfDataForYear>;
  ocfClientOptions?: DistrictOfColumbiaOcfClientOptions;
}): Promise<DistrictOfColumbiaOcfDataForYear> {
  const existing = input.dataByYear.get(input.year);
  if (existing) {
    return existing;
  }
  const loaded = await loadOcfDataForYear({ year: input.year, ocfClientOptions: input.ocfClientOptions });
  input.dataByYear.set(input.year, loaded);
  return loaded;
}

async function getOrLoadPrincipalContributionDataForYear(input: {
  year: number;
  dataByYear: Map<number, DistrictOfColumbiaOcfDataForYear>;
  principalDataByYear: Map<number, Pick<DistrictOfColumbiaOcfDataForYear, "year" | "sourceUrl" | "principalContributionRecords">>;
  ocfClientOptions?: DistrictOfColumbiaOcfClientOptions;
}): Promise<Pick<DistrictOfColumbiaOcfDataForYear, "year" | "sourceUrl" | "principalContributionRecords">> {
  const fullData = input.dataByYear.get(input.year);
  if (fullData) {
    return fullData;
  }
  const principalData = input.principalDataByYear.get(input.year);
  if (principalData) {
    return principalData;
  }
  const loaded = await loadPrincipalContributionDataForYear({
    year: input.year,
    ocfClientOptions: input.ocfClientOptions,
  });
  input.principalDataByYear.set(input.year, loaded);
  return loaded;
}

export async function listDueDistrictOfColumbiaCandidateFinanceSyncRows(
  db: Queryable,
  input: {
    now: Date;
    staleAfterDays: number;
    maxCandidates: number;
    electionLookbackDays: number;
    electionLookaheadDays: number;
  }
): Promise<{ rows: DistrictOfColumbiaCandidateFinanceDueRow[]; totalDueRows: number }> {
  const result = await db.query<DistrictOfColumbiaCandidateFinanceDueQueryRow>(
    `
      WITH due AS (
        SELECT
          link.candidate_id::text AS candidate_id,
          link.election_id::text AS election_id,
          COALESCE(
            NULLIF(trim(candidate.display_name), ''),
            NULLIF(trim(candidate.first_name || ' ' || candidate.last_name), ''),
            link.candidate_name_normalized
          ) AS candidate_name,
          link.election_year,
          office.scope AS office_scope,
          link.office_name,
          link.district,
          link.committee_key,
          link.committee_name,
          link.source_url,
          summary.last_synced_at::text AS last_synced_at,
          COUNT(*) OVER () AS total_due_rows
        FROM public.dc_candidate_finance_links AS link
        JOIN public.candidates AS candidate
          ON candidate.id = link.candidate_id
        JOIN public.candidate_elections AS candidate_election
          ON candidate_election.candidate_id = link.candidate_id
         AND candidate_election.election_id = link.election_id
        JOIN public.elections AS election
          ON election.id = link.election_id
        JOIN public.districts AS district
          ON district.id = election.district_id
        LEFT JOIN public.offices AS office
          ON office.id = election.office_id
        LEFT JOIN public.dc_candidate_finance_summaries AS summary
          ON summary.link_id = link.id
         AND summary.election_year = link.election_year
        WHERE link.link_status = 'active'
          AND candidate.deleted_at IS NULL
          AND district.state = 'DC'
          AND election.race_type = 'office'
          AND election.election_date >= (($1::timestamptz)::date - make_interval(days => $4::int))
          AND election.election_date <= (($1::timestamptz)::date + make_interval(days => $5::int))
          AND candidate_election.status NOT IN ('withdrawn', 'lost')
          AND (office.scope || '::' || office.canonical_name) = ANY($6::text[])
          AND (
            summary.last_synced_at IS NULL
            OR summary.last_synced_at < ($1::timestamptz - make_interval(days => $2::int))
          )
        ORDER BY summary.last_synced_at ASC NULLS FIRST,
                 election.election_date ASC,
                 link.candidate_name_normalized ASC,
                 link.id ASC
        LIMIT $3::int
      )
      SELECT
        candidate_id,
        election_id,
        candidate_name,
        election_year,
        office_scope,
        office_name,
        district,
        committee_key,
        committee_name,
        source_url,
        last_synced_at,
        total_due_rows
      FROM due
    `,
    [
      input.now.toISOString(),
      input.staleAfterDays,
      input.maxCandidates,
      input.electionLookbackDays,
      input.electionLookaheadDays,
      [...DISTRICT_OF_COLUMBIA_FINANCE_ELIGIBLE_OFFICE_KEYS],
    ]
  );

  return {
    rows: result.rows.map(mapDueRow),
    totalDueRows: result.rows.length > 0 ? parseTotalDueRows(result.rows[0]?.total_due_rows) : 0,
  };
}

export async function syncDueDistrictOfColumbiaCandidateFinance(
  input: DistrictOfColumbiaCandidateFinanceBatchSyncInput
): Promise<DistrictOfColumbiaCandidateFinanceBatchSyncResult> {
  const now = input.now ?? new Date();
  assertValidDate(now, "now");

  const maxCandidates = normalizePositiveInteger(input.maxCandidates, DEFAULT_MAX_CANDIDATES, "maxCandidates");
  const staleAfterDays = normalizePositiveInteger(input.staleAfterDays, DEFAULT_STALE_AFTER_DAYS, "staleAfterDays");
  const electionLookbackDays = normalizePositiveInteger(
    input.electionLookbackDays,
    DEFAULT_POST_ELECTION_FINANCE_SYNC_GRACE_DAYS,
    "electionLookbackDays"
  );
  const electionLookaheadDays = normalizePositiveInteger(
    input.electionLookaheadDays,
    DEFAULT_ELECTION_LOOKAHEAD_DAYS,
    "electionLookaheadDays"
  );
  const dryRun = input.dryRun === true;
  const syncFn = input.syncDistrictOfColumbiaCandidateFinanceFn ?? syncDistrictOfColumbiaCandidateFinance;
  const ocfDataByYear = new Map<number, DistrictOfColumbiaOcfDataForYear>(
    input.ocfDataByYear ? [...input.ocfDataByYear.entries()] : []
  );
  const principalDataByYear = new Map<
    number,
    Pick<DistrictOfColumbiaOcfDataForYear, "year" | "sourceUrl" | "principalContributionRecords">
  >();
  let autoLinkAttemptedCount = 0;
  let autoLinkLinkedCount = 0;

  if (!dryRun && input.autoLinkMissingLinks !== false) {
    try {
      const missingLinkCandidates = await listDistrictOfColumbiaCandidateElectionsMissingFinanceLinks(input.db, {
        now,
        maxCandidates,
        electionLookbackDays,
        electionLookaheadDays,
      });
      autoLinkAttemptedCount = missingLinkCandidates.length;

      let resolveCandidateCommittee = input.resolveCandidateCommittee;
      if (!resolveCandidateCommittee) {
        const contributionRowsByYear = new Map<number, readonly DistrictOfColumbiaOcfContributionRecord[]>();
        const sourceUrlByYear = new Map<number, string>();
        for (const [year] of groupAutoLinkCandidatesByYear(missingLinkCandidates).entries()) {
          const data = await getOrLoadPrincipalContributionDataForYear({
            year,
            dataByYear: ocfDataByYear,
            principalDataByYear,
            ocfClientOptions: input.ocfClientOptions,
          });
          contributionRowsByYear.set(year, data.principalContributionRecords);
          sourceUrlByYear.set(year, data.sourceUrl);
        }
        resolveCandidateCommittee = async (candidateInput): Promise<DistrictOfColumbiaCandidateCommitteeResolution> =>
          resolveDistrictOfColumbiaCandidateCommittee({
            ...candidateInput,
            contributionRecords: contributionRowsByYear.get(candidateInput.electionYear) ?? [],
            sourceUrl: sourceUrlByYear.get(candidateInput.electionYear) ?? buildDistrictOfColumbiaOcfDataDownloadUrl(),
          });
      }

      const autoLinkResults = await autoLinkMissingDistrictOfColumbiaCandidateFinanceLinks({
        db: input.db,
        now,
        maxCandidates,
        electionLookbackDays,
        electionLookaheadDays,
        candidateElections: missingLinkCandidates,
        resolveCandidateCommittee,
        ocfClientOptions: input.ocfClientOptions,
      });
      autoLinkLinkedCount = autoLinkResults.filter((result) => result.status === "linked").length;
      for (const result of autoLinkResults) {
        if (result.status === "error") {
          console.warn("D.C. finance auto-link did not link candidate election:", result);
        }
      }
    } catch (error) {
      console.warn(
        "D.C. finance auto-link skipped; continuing with already-linked candidate sync:",
        error instanceof Error ? error.message : error
      );
    }
  }

  const due = await listDueDistrictOfColumbiaCandidateFinanceSyncRows(input.db, {
    now,
    staleAfterDays,
    maxCandidates,
    electionLookbackDays,
    electionLookaheadDays,
  });

  const dataLoadErrorsByYear = new Map<number, string>();
  for (const [year] of groupDueRowsByYear(due.rows).entries()) {
    if (!ocfDataByYear.has(year)) {
      try {
        await getOrLoadOcfDataForYear({
          year,
          dataByYear: ocfDataByYear,
          ocfClientOptions: input.ocfClientOptions,
        });
      } catch (error) {
        dataLoadErrorsByYear.set(year, error instanceof Error ? error.message : String(error));
      }
    }
  }

  const results: DistrictOfColumbiaCandidateFinanceBatchSyncItemResult[] = [];
  for (const row of due.rows) {
    const dataLoadError = dataLoadErrorsByYear.get(row.electionYear);
    if (dataLoadError) {
      results.push({
        candidateId: row.candidateId,
        electionId: row.electionId,
        electionYear: row.electionYear,
        committeeKey: row.committeeKey,
        ok: false,
        error: dataLoadError,
      });
      continue;
    }

    const ocfData = ocfDataByYear.get(row.electionYear);
    try {
      const result = await syncFn({
        db: input.db,
        candidateId: row.candidateId,
        electionId: row.electionId,
        candidateName: row.candidateName,
        electionYear: row.electionYear,
        officeScope: row.officeScope,
        officeName: row.officeName,
        district: row.district,
        sourceUrl: row.sourceUrl ?? ocfData?.sourceUrl,
        contributionRecords: ocfData?.principalContributionRecords ?? [],
        expenditureRecords: ocfData?.independentExpenditureRecords ?? [],
        outsideContributionRecords: ocfData?.independentExpenditureContributionRecords ?? [],
        trustedCommittee: {
          committeeKey: normalizeCommitteeKey(row.committeeKey),
          committeeName: row.committeeName,
          sourceUrl: row.sourceUrl ?? ocfData?.sourceUrl,
        },
        financeIndustryClassifier: input.financeIndustryClassifier,
        aiClassificationMinAmount: input.aiClassificationMinAmount,
        dryRun,
        now,
      });
      results.push({
        candidateId: row.candidateId,
        electionId: row.electionId,
        electionYear: row.electionYear,
        committeeKey: row.committeeKey,
        ok: true,
        result,
      });
    } catch (error) {
      results.push({
        candidateId: row.candidateId,
        electionId: row.electionId,
        electionYear: row.electionYear,
        committeeKey: row.committeeKey,
        ok: false,
        error: error instanceof Error ? error.message : String(error),
      });
    }
  }

  const syncedCandidateCount = results.filter((result) => result.ok).length;
  return {
    dryRun,
    now: now.toISOString(),
    staleAfterDays,
    maxCandidates,
    dueCandidateCount: due.totalDueRows,
    selectedCandidateCount: due.rows.length,
    syncedCandidateCount,
    failedCandidateCount: results.length - syncedCandidateCount,
    autoLinkAttemptedCount,
    autoLinkLinkedCount,
    results,
  };
}
