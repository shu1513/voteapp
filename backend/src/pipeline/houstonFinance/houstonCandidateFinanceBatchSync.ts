import type { Pool } from "pg";
import type { FinanceIndustryClassifier } from "../finance/financeIndustryClassificationService.js";
import { loadHoustonCandidateFinanceReports } from "./houstonCampaignFinanceReportSource.js";
import { autoLinkHoustonCandidateFinance, listHoustonCandidateElectionsMissingFinanceLinks } from "./houstonCandidateFinanceAutoLink.js";
import { syncHoustonCandidateFinance, type HoustonCandidateFinanceSyncResult } from "./houstonCandidateFinanceSync.js";
import { loadHoustonTexasTecData, type HoustonTexasTecData } from "./houstonTexasTecDataSource.js";
import { parseStoredHoustonFinanceOfficeTarget } from "./houstonFinanceOfficeTargets.js";

export type HoustonCandidateFinanceDueRow = {
  candidateId: string; electionId: string; candidateName: string; firstName: string; lastName: string;
  electionYear: number; electionDate: string; officeName: string; district: string | null;
  committeeId: string; committeeName: string; sourceUrl: string | null;
};

export type HoustonCandidateFinanceBatchSyncResult = {
  dryRun: boolean; now: string; staleAfterDays: number; maxCandidates: number;
  dueCandidateCount: number; selectedCandidateCount: number; syncedCandidateCount: number; failedCandidateCount: number;
  outsideSourceAvailable: boolean;
  results: Array<{ candidateId: string; ok: boolean; result?: HoustonCandidateFinanceSyncResult; error?: string }>;
};

export async function listDueHoustonCandidateFinanceRows(input: {
  db: Pick<Pool, "query">; now: Date; maxCandidates: number; staleAfterDays: number; lookbackDays: number; lookaheadDays: number;
  force?: boolean;
}): Promise<HoustonCandidateFinanceDueRow[]> {
  const result = await input.db.query<{
    candidate_id: string; election_id: string; candidate_name: string; first_name: string; last_name: string;
    election_year: number; election_date: string; office_name: string; district: string | null;
    committee_id: string; committee_name: string; source_url: string | null;
  }>(`
    SELECT candidate.id::text candidate_id, election.id::text election_id,
      COALESCE(NULLIF(trim(candidate.display_name), ''), trim(candidate.first_name || ' ' || candidate.last_name)) candidate_name,
      candidate.first_name, candidate.last_name, link.election_year, election.election_date::text election_date,
      link.office_name, link.district, link.committee_id, link.committee_name, link.source_url
    FROM public.hou_candidate_finance_links link
    JOIN public.candidates candidate ON candidate.id = link.candidate_id
    JOIN public.elections election ON election.id = link.election_id
    LEFT JOIN public.hou_candidate_finance_summaries summary ON summary.link_id = link.id AND summary.election_year = link.election_year
    WHERE link.link_status = 'active' AND candidate.deleted_at IS NULL AND candidate.merged_into_candidate_id IS NULL
      AND election.election_date BETWEEN ($1::date - make_interval(days => $4)) AND ($1::date + make_interval(days => $5))
      AND ($6::boolean OR election.election_date >= $1::date - 1)
      AND ($6::boolean OR summary.last_synced_at IS NULL OR summary.last_synced_at <= $1::timestamptz - make_interval(days => $3))
    ORDER BY election.election_date, candidate_name LIMIT $2
  `, [input.now.toISOString(), input.maxCandidates, input.staleAfterDays, input.lookbackDays, input.lookaheadDays, input.force === true]);
  return result.rows.map((row) => ({ candidateId: row.candidate_id, electionId: row.election_id, candidateName: row.candidate_name,
    firstName: row.first_name, lastName: row.last_name, electionYear: row.election_year, electionDate: row.election_date,
    officeName: row.office_name, district: row.district, committeeId: row.committee_id,
    committeeName: row.committee_name, sourceUrl: row.source_url }));
}

export async function syncDueHoustonCandidateFinance(input: {
  db: Pool; now?: Date; dryRun?: boolean; force?: boolean; maxCandidates?: number; staleAfterDays?: number;
  lookbackDays?: number; lookaheadDays?: number; electionLookbackDays?: number; electionLookaheadDays?: number;
  cacheDir?: string; tecZipPath?: string; rawDataCacheDir?: string; rawDataZipPath?: string;
  autoLink?: boolean; tecData?: HoustonTexasTecData; financeIndustryClassifier?: FinanceIndustryClassifier;
  aiClassificationMinAmount?: number;
  loadReportsFn?: typeof loadHoustonCandidateFinanceReports;
  syncFn?: typeof syncHoustonCandidateFinance;
}): Promise<HoustonCandidateFinanceBatchSyncResult> {
  const now = input.now ?? new Date();
  const maxCandidates = input.maxCandidates ?? 10;
  const staleAfterDays = input.staleAfterDays ?? 1;
  const lookbackDays = input.lookbackDays ?? input.electionLookbackDays ?? 1;
  const lookaheadDays = input.lookaheadDays ?? input.electionLookaheadDays ?? 730;
  const cacheDir = input.cacheDir ?? input.rawDataCacheDir;
  if (input.autoLink !== false) {
    const missing = await listHoustonCandidateElectionsMissingFinanceLinks({ db: input.db, now, maxCandidates, lookbackDays, lookaheadDays });
    for (const candidate of missing) {
      try { await autoLinkHoustonCandidateFinance({ db: input.db, candidate, now, cacheDir, dryRun: input.dryRun }); }
      catch (error) { console.warn("Houston finance auto-link failed; continuing:", error); }
    }
  }
  const dueRows = await listDueHoustonCandidateFinanceRows({ db: input.db, now, maxCandidates, staleAfterDays, lookbackDays, lookaheadDays, force: input.force });
  let tecData = input.tecData;
  if (!tecData && dueRows.length > 0) {
    try { tecData = await loadHoustonTexasTecData({ candidates: dueRows, zipPath: input.tecZipPath ?? input.rawDataZipPath, cacheDir: input.rawDataCacheDir }); }
    catch (error) { console.warn(error instanceof Error ? error.message : String(error)); }
  }
  const results: Array<{ candidateId: string; ok: boolean; result?: HoustonCandidateFinanceSyncResult; error?: string }> = [];
  for (const row of dueRows) {
    try {
      let reports;
      try {
        const officeTarget = parseStoredHoustonFinanceOfficeTarget(row);
        if (!officeTarget) throw new Error(`Houston finance link has an unsupported office target: ${row.officeName} ${row.district ?? ""}`);
        reports = await (input.loadReportsFn ?? loadHoustonCandidateFinanceReports)({ candidateName: row.candidateName, firstName: row.firstName,
          lastName: row.lastName, electionYear: row.electionYear, officeTarget, cacheDir });
      } catch (error) {
        console.warn(`Houston direct finance unavailable for candidate=${row.candidateId}; preserving prior direct data:`, error);
      }
      if (!reports && !tecData) throw new Error("Houston direct and outside finance sources are both unavailable");
      const result = await (input.syncFn ?? syncHoustonCandidateFinance)({ db: input.db, ...row, reports,
        purposeRows: tecData?.purposeRows, candidateRows: tecData?.candidateRows, expenditureRows: tecData?.expenditureRows,
        outsideContributionRows: tecData?.contributionRows, tecSourceUrl: tecData?.sourceUrl,
        excludedIndustryOrganizationNames: tecData?.politicalCommitteeNames,
        financeIndustryClassifier: input.financeIndustryClassifier, aiClassificationMinAmount: input.aiClassificationMinAmount,
        dryRun: input.dryRun, now });
      results.push({ candidateId: row.candidateId, ok: true, result });
    } catch (error) { results.push({ candidateId: row.candidateId, ok: false, error: error instanceof Error ? error.message : String(error) }); }
  }
  return {
    dryRun: input.dryRun === true, now: now.toISOString(), staleAfterDays, maxCandidates,
    dueCandidateCount: dueRows.length, selectedCandidateCount: dueRows.length,
    syncedCandidateCount: results.filter((item) => item.ok).length,
    failedCandidateCount: results.filter((item) => !item.ok).length,
    outsideSourceAvailable: Boolean(tecData), results,
  };
}
