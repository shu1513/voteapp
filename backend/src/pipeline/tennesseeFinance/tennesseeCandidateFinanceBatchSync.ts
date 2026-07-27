import type { Pool, PoolClient } from "pg";

import type { FinanceIndustryClassifier } from "../finance/financeIndustryClassificationService.js";
import {
  buildTennesseeCampContributionSearchUrl,
  fetchTennesseeCampContributionRecords,
  fetchTennesseeCampExpenditureRecords,
  fetchTennesseeCampPacContributionRecords,
  type TennesseeCampClientOptions,
  type TennesseeCampContributionRecord,
  type TennesseeCampExpenditureRecord,
} from "./tennesseeCampClient.js";
import {
  autoLinkMissingTennesseeCandidateFinanceLinks,
  listTennesseeCandidateElectionsMissingFinanceLinks,
  type TennesseeCandidateCommitteeResolver,
  type TennesseeFinanceAutoLinkCandidateElection,
} from "./tennesseeCandidateFinanceAutoLink.js";
import {
  syncTennesseeCandidateFinance,
  type TennesseeCandidateFinanceSyncResult,
} from "./tennesseeCandidateFinanceSync.js";
import { aggregateTennesseeOutsideSpending } from "./tennesseeOutsideSpendingAggregator.js";
import { TENNESSEE_FINANCE_ELIGIBLE_OFFICE_KEYS } from "./tennesseeFinanceEligibleOffices.js";
import type { TennesseeFinanceLinkSource } from "./tennesseeFinanceWriter.js";

type Queryable = Pick<Pool | PoolClient, "query">;
type TennesseeCampContributionFetchResult = {
  sourceUrl: string | null;
  records: TennesseeCampContributionRecord[];
};
type TennesseeCampExpenditureFetchResult = {
  sourceUrl: string | null;
  records: TennesseeCampExpenditureRecord[];
};

type TennesseeCandidateFinanceExportCache = {
  independentExpendituresByReportYear: Map<string, Promise<TennesseeCampExpenditureFetchResult>>;
  pacContributionsByRecipientReportYear: Map<string, Promise<TennesseeCampContributionFetchResult>>;
};

export type TennesseeCandidateFinanceDueRow = {
  candidateId: string;
  electionId: string;
  candidateName: string;
  electionYear: number;
  officeScope: string;
  officeName: string;
  district: string | null;
  campCandidateId: string;
  ownerName: string;
  committeeName: string | null;
  linkSource: TennesseeFinanceLinkSource;
  sourceUrl: string | null;
  reportListUrl: string | null;
  lastSyncedAt: string | null;
};

export type TennesseeCandidateFinanceContributionData = {
  sourceUrl: string | null;
  contributions: TennesseeCampContributionRecord[];
  expenditureSourceUrl: string | null;
  expenditures: TennesseeCampExpenditureRecord[];
  outsideContributionSourceUrl: string | null;
  outsideGroupContributionRecords: TennesseeCampContributionRecord[];
};

export type TennesseeCandidateFinanceBatchSyncInput = {
  db: Queryable;
  now?: Date;
  dryRun?: boolean;
  maxCandidates?: number;
  staleAfterDays?: number;
  electionLookbackDays?: number;
  electionLookaheadDays?: number;
  autoLinkMissingLinks?: boolean;
  clientOptions?: TennesseeCampClientOptions;
  financeIndustryClassifier?: FinanceIndustryClassifier;
  aiClassificationMinAmount?: number;
  syncTennesseeCandidateFinanceFn?: typeof syncTennesseeCandidateFinance;
  loadContributionDataForCandidate?: typeof loadTennesseeContributionDataForCandidate;
  resolveCandidateCommittee?: TennesseeCandidateCommitteeResolver;
};

export type TennesseeCandidateFinanceBatchSyncItemResult = {
  candidateId: string;
  electionId: string;
  electionYear: number;
  campCandidateId: string;
  ok: boolean;
  result?: TennesseeCandidateFinanceSyncResult;
  error?: string;
};

export type TennesseeCandidateFinanceBatchSyncResult = {
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
  results: TennesseeCandidateFinanceBatchSyncItemResult[];
};

type TennesseeCandidateFinanceDueQueryRow = {
  candidate_id: string;
  election_id: string;
  candidate_name: string;
  election_year: number;
  office_scope: string;
  office_name: string;
  district: string | null;
  camp_candidate_id: string;
  owner_name: string;
  committee_name: string | null;
  link_source: TennesseeFinanceLinkSource;
  source_url: string | null;
  report_list_url: string | null;
  last_synced_at: string | null;
  total_due_rows: string | number;
};

const DEFAULT_MAX_CANDIDATES = 25;
const DEFAULT_STALE_AFTER_DAYS = 7;
const DEFAULT_POST_ELECTION_FINANCE_SYNC_GRACE_DAYS = 1;
const DEFAULT_ELECTION_LOOKAHEAD_DAYS = 730;

function assertValidDate(date: Date, label: string): void {
  if (!(date instanceof Date) || Number.isNaN(date.getTime())) {
    throw new Error(`Invalid Tennessee finance batch sync ${label}`);
  }
}

function normalizePositiveInteger(value: number | undefined, fallback: number, label: string): number {
  const normalized = value ?? fallback;
  if (!Number.isInteger(normalized) || normalized <= 0) {
    throw new Error(`Invalid Tennessee finance batch sync ${label}: ${value}`);
  }
  return normalized;
}

function parseTotalDueRows(value: string | number | undefined): number {
  const parsed = typeof value === "number" ? value : Number(value);
  return Number.isSafeInteger(parsed) && parsed >= 0 ? parsed : 0;
}

function normalizeDistrict(value: string | null): string | null {
  const trimmed = value?.trim();
  return trimmed ? trimmed : null;
}

function normalizeSearchToken(value: string): string {
  return value
    .replace(/\([^()]+\)/g, " ")
    .replace(/\b(JR|SR|II|III|IV|V)\.?\b/gi, " ")
    .replace(/[.,]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function lastNameSearchToken(candidateName: string): string {
  const trimmed = candidateName.replace(/\([^()]+\)/g, " ").trim();
  if (trimmed.includes(",")) {
    const commaFirst = normalizeSearchToken(trimmed.split(",", 1)[0] ?? "");
    if (commaFirst) {
      return commaFirst;
    }
  }
  const normalized = normalizeSearchToken(trimmed);
  return normalized.split(/\s+/).filter(Boolean).at(-1) ?? trimmed;
}

function contributionRecordKey(contribution: TennesseeCampContributionRecord): string {
  return [
    contribution.electionYear,
    contribution.reportName,
    contribution.type,
    contribution.adjustment,
    contribution.amount,
    contribution.date,
    contribution.recipientName,
    contribution.contributorName,
    contribution.contributorOccupation,
    contribution.contributorEmployer,
  ].join("\u0000");
}

function expenditureRecordKey(expenditure: TennesseeCampExpenditureRecord): string {
  return [
    expenditure.electionYear,
    expenditure.reportName,
    expenditure.type,
    expenditure.adjustment,
    expenditure.amount,
    expenditure.date,
    expenditure.candidatePacName,
    expenditure.vendorName,
    expenditure.purpose,
    expenditure.candidateFor,
    expenditure.supportOpposeCode,
  ].join("\u0000");
}

function createTennesseeCandidateFinanceExportCache(): TennesseeCandidateFinanceExportCache {
  return {
    independentExpendituresByReportYear: new Map(),
    pacContributionsByRecipientReportYear: new Map(),
  };
}

function exportCacheKey(...parts: readonly (string | number | null | undefined)[]): string {
  return parts.map((part) => String(part ?? "")).join("\u0000");
}

function mergeTennesseeCampSourceUrl(existing: string | null, next: string | null): string | null {
  if (!existing) {
    return next;
  }
  if (!next || existing === next) {
    return existing;
  }
  return buildTennesseeCampContributionSearchUrl();
}

function fetchCachedIndependentExpenditures(
  input: { electionYear: number; reportYear: number },
  clientOptions: TennesseeCampClientOptions | undefined,
  exportCache: TennesseeCandidateFinanceExportCache | undefined
): Promise<TennesseeCampExpenditureFetchResult> {
  if (!exportCache) {
    return fetchTennesseeCampExpenditureRecords(
      {
        electionYear: input.electionYear,
        reportYear: input.reportYear,
        expenditureType: "independent",
      },
      clientOptions
    );
  }

  const key = exportCacheKey(input.electionYear, input.reportYear, "independent");
  const cached =
    exportCache.independentExpendituresByReportYear.get(key) ??
    fetchTennesseeCampExpenditureRecords(
      {
        electionYear: input.electionYear,
        reportYear: input.reportYear,
        expenditureType: "independent",
      },
      clientOptions
    );
  exportCache.independentExpendituresByReportYear.set(key, cached);
  return cached;
}

function fetchCachedPacContributions(
  input: { recipientName: string; electionYear: number; reportYear: number },
  clientOptions: TennesseeCampClientOptions | undefined,
  exportCache: TennesseeCandidateFinanceExportCache | undefined
): Promise<TennesseeCampContributionFetchResult> {
  if (!exportCache) {
    return fetchTennesseeCampPacContributionRecords(input, clientOptions);
  }

  const key = exportCacheKey(input.recipientName.trim().toUpperCase(), input.electionYear, input.reportYear);
  const cached =
    exportCache.pacContributionsByRecipientReportYear.get(key) ??
    fetchTennesseeCampPacContributionRecords(input, clientOptions);
  exportCache.pacContributionsByRecipientReportYear.set(key, cached);
  return cached;
}

function mapDueRow(row: TennesseeCandidateFinanceDueQueryRow): TennesseeCandidateFinanceDueRow {
  return {
    candidateId: row.candidate_id,
    electionId: row.election_id,
    candidateName: row.candidate_name,
    electionYear: row.election_year,
    officeScope: row.office_scope,
    officeName: row.office_name,
    district: normalizeDistrict(row.district),
    campCandidateId: row.camp_candidate_id,
    ownerName: row.owner_name,
    committeeName: row.committee_name,
    linkSource: row.link_source,
    sourceUrl: row.source_url,
    reportListUrl: row.report_list_url,
    lastSyncedAt: row.last_synced_at,
  };
}

export async function loadTennesseeContributionDataForCandidate(input: {
  candidateName: string;
  ownerName: string;
  electionYear: number;
  clientOptions?: TennesseeCampClientOptions;
  exportCache?: TennesseeCandidateFinanceExportCache;
}): Promise<TennesseeCandidateFinanceContributionData> {
  const recordsByKey = new Map<string, TennesseeCampContributionRecord>();
  const expendituresByKey = new Map<string, TennesseeCampExpenditureRecord>();
  const outsideContributionRecordsByKey = new Map<string, TennesseeCampContributionRecord>();
  let sourceUrl: string | null = null;
  let expenditureSourceUrl: string | null = null;
  let outsideContributionSourceUrl: string | null = null;
  for (const reportYear of [input.electionYear - 1, input.electionYear].filter((year) => year >= 2000)) {
    const result = await fetchTennesseeCampContributionRecords(
      {
        recipientName: lastNameSearchToken(input.ownerName || input.candidateName),
        electionYear: input.electionYear,
        reportYear,
      },
      input.clientOptions
    );
    sourceUrl = mergeTennesseeCampSourceUrl(sourceUrl, result.sourceUrl);
    for (const contribution of result.records) {
      recordsByKey.set(contributionRecordKey(contribution), contribution);
    }
    const expenditureResult = await fetchCachedIndependentExpenditures(
      {
        electionYear: input.electionYear,
        reportYear,
      },
      input.clientOptions,
      input.exportCache
    );
    expenditureSourceUrl = mergeTennesseeCampSourceUrl(expenditureSourceUrl, expenditureResult.sourceUrl);
    for (const expenditure of expenditureResult.records) {
      expendituresByKey.set(expenditureRecordKey(expenditure), expenditure);
    }
  }

  const outsideFinance = aggregateTennesseeOutsideSpending({
    candidateName: input.candidateName,
    ownerName: input.ownerName,
    electionYear: input.electionYear,
    expenditureRecords: [...expendituresByKey.values()],
    sourceUrl: expenditureSourceUrl,
  });
  const outsideCommitteeNames = [
    ...new Set((outsideFinance.summary?.groups ?? []).map((group) => group.committeeName.trim()).filter(Boolean)),
  ];
  for (const committeeName of outsideCommitteeNames) {
    for (const reportYear of [input.electionYear - 1, input.electionYear].filter((year) => year >= 2000)) {
      const result = await fetchCachedPacContributions(
        {
          recipientName: committeeName,
          electionYear: input.electionYear,
          reportYear,
        },
        input.clientOptions,
        input.exportCache
      );
      outsideContributionSourceUrl = mergeTennesseeCampSourceUrl(outsideContributionSourceUrl, result.sourceUrl);
      for (const contribution of result.records) {
        outsideContributionRecordsByKey.set(contributionRecordKey(contribution), contribution);
      }
    }
  }

  return {
    sourceUrl,
    contributions: [...recordsByKey.values()],
    expenditureSourceUrl,
    expenditures: [...expendituresByKey.values()],
    outsideContributionSourceUrl,
    outsideGroupContributionRecords: [...outsideContributionRecordsByKey.values()],
  };
}

export async function listDueTennesseeCandidateFinanceSyncRows(
  db: Queryable,
  input: {
    now: Date;
    staleAfterDays: number;
    maxCandidates: number;
    electionLookbackDays: number;
    electionLookaheadDays: number;
  }
): Promise<{ rows: TennesseeCandidateFinanceDueRow[]; totalDueRows: number }> {
  const result = await db.query<TennesseeCandidateFinanceDueQueryRow>(
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
          link.camp_candidate_id,
          link.owner_name,
          link.committee_name,
          link.link_source,
          link.source_url,
          link.report_list_url,
          summary.last_synced_at::text AS last_synced_at,
          COUNT(*) OVER () AS total_due_rows
        FROM public.tn_candidate_finance_links AS link
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
        LEFT JOIN public.tn_candidate_finance_summaries AS summary
          ON summary.link_id = link.id
         AND summary.election_year = link.election_year
        WHERE link.link_status = 'active'
          AND candidate.deleted_at IS NULL
          AND district.state = 'TN'
          AND election.race_type = 'office'
          AND election.election_date >= (($1::timestamptz AT TIME ZONE 'UTC')::date - make_interval(days => $4::int))
          AND election.election_date <= (($1::timestamptz AT TIME ZONE 'UTC')::date + make_interval(days => $5::int))
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
        camp_candidate_id,
        owner_name,
        committee_name,
        link_source,
        source_url,
        report_list_url,
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
      [...TENNESSEE_FINANCE_ELIGIBLE_OFFICE_KEYS],
    ]
  );

  return {
    rows: result.rows.map(mapDueRow),
    totalDueRows: result.rows.length > 0 ? parseTotalDueRows(result.rows[0]?.total_due_rows) : 0,
  };
}

export async function syncDueTennesseeCandidateFinance(
  input: TennesseeCandidateFinanceBatchSyncInput
): Promise<TennesseeCandidateFinanceBatchSyncResult> {
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
  const shouldAutoLinkMissingLinks = !dryRun && input.autoLinkMissingLinks !== false;
  const syncFn = input.syncTennesseeCandidateFinanceFn ?? syncTennesseeCandidateFinance;
  const exportCache = input.loadContributionDataForCandidate ? undefined : createTennesseeCandidateFinanceExportCache();
  const loadContributionData =
    input.loadContributionDataForCandidate ??
    ((loaderInput: {
      candidateName: string;
      ownerName: string;
      electionYear: number;
      clientOptions?: TennesseeCampClientOptions;
    }) => loadTennesseeContributionDataForCandidate({ ...loaderInput, exportCache }));
  let autoLinkAttemptedCount = 0;
  let autoLinkLinkedCount = 0;

  if (shouldAutoLinkMissingLinks) {
    try {
      const missingLinkCandidates: TennesseeFinanceAutoLinkCandidateElection[] =
        await listTennesseeCandidateElectionsMissingFinanceLinks(input.db, {
          now,
          maxCandidates,
          electionLookbackDays,
          electionLookaheadDays,
        });
      autoLinkAttemptedCount = missingLinkCandidates.length;
      const autoLinkResults = await autoLinkMissingTennesseeCandidateFinanceLinks({
        db: input.db,
        now,
        maxCandidates,
        electionLookbackDays,
        electionLookaheadDays,
        candidateElections: missingLinkCandidates,
        resolveCandidateCommittee: input.resolveCandidateCommittee,
        clientOptions: input.clientOptions,
      });
      autoLinkLinkedCount = autoLinkResults.filter((result) => result.status === "linked").length;
      for (const result of autoLinkResults) {
        if (result.status !== "linked") {
          console.warn("Tennessee finance auto-link did not link candidate election:", result);
        }
      }
    } catch (error) {
      console.warn(
        "Tennessee finance auto-link skipped; continuing with already-linked candidate sync:",
        error instanceof Error ? error.message : error
      );
    }
  }

  const due = await listDueTennesseeCandidateFinanceSyncRows(input.db, {
    now,
    staleAfterDays,
    maxCandidates,
    electionLookbackDays,
    electionLookaheadDays,
  });

  const results: TennesseeCandidateFinanceBatchSyncItemResult[] = [];
  for (const row of due.rows) {
    try {
      const contributionData = await loadContributionData({
        candidateName: row.candidateName,
        ownerName: row.ownerName,
        electionYear: row.electionYear,
        clientOptions: input.clientOptions,
      });
      const result = await syncFn({
        db: input.db,
        candidateId: row.candidateId,
        electionId: row.electionId,
        candidateName: row.candidateName,
        electionYear: row.electionYear,
        officeName: row.officeName,
        district: row.district,
        campCandidateId: row.campCandidateId,
        ownerName: row.ownerName,
        committeeName: row.committeeName,
        linkSource: row.linkSource,
        sourceUrl: row.sourceUrl,
        reportListUrl: row.reportListUrl,
        contributions: contributionData.contributions,
        contributionSourceUrl: contributionData.sourceUrl,
        expenditures: contributionData.expenditures,
        expenditureSourceUrl: contributionData.expenditureSourceUrl,
        outsideGroupContributionRecords: contributionData.outsideGroupContributionRecords,
        outsideContributionSourceUrl: contributionData.outsideContributionSourceUrl,
        financeIndustryClassifier: input.financeIndustryClassifier,
        aiClassificationMinAmount: input.aiClassificationMinAmount,
        now,
        dryRun,
      });
      results.push({
        candidateId: row.candidateId,
        electionId: row.electionId,
        electionYear: row.electionYear,
        campCandidateId: row.campCandidateId,
        ok: true,
        result,
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      console.warn("Tennessee finance sync failed for candidate; continuing:", {
        candidateId: row.candidateId,
        electionId: row.electionId,
        campCandidateId: row.campCandidateId,
        error: message,
      });
      results.push({
        candidateId: row.candidateId,
        electionId: row.electionId,
        electionYear: row.electionYear,
        campCandidateId: row.campCandidateId,
        ok: false,
        error: message,
      });
    }
  }

  return {
    dryRun,
    now: now.toISOString(),
    staleAfterDays,
    maxCandidates,
    dueCandidateCount: due.totalDueRows,
    selectedCandidateCount: due.rows.length,
    syncedCandidateCount: results.filter((result) => result.ok).length,
    failedCandidateCount: results.filter((result) => !result.ok).length,
    autoLinkAttemptedCount,
    autoLinkLinkedCount,
    results,
  };
}
