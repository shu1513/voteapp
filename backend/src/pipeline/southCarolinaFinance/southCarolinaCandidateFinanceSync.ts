// South Carolina per-candidate finance sync: fetch the linked filer's report
// index, scope runs to the linked race's statutory cycle dates, fetch each
// accepted run's final-report detail plus the itemized contribution rows the
// runs' filing periods span, aggregate, and write one snapshot.
//
// Presence semantics (plan Phase 4): the writer is only called with filed
// data or a filed-zero aggregation. No accepted runs -> "no_filed_reports"
// and NOTHING is written (absence before a deadline is not a zero); a fetch
// or aggregation failure throws and writes nothing, preserving the prior
// snapshot. A due link costs three or four small JSON calls — there is no
// bulk pull.

import type { Pool, PoolClient } from "pg";

import {
  getSouthCarolinaCandidateReports,
  getSouthCarolinaReportDetails,
  searchSouthCarolinaContributions,
  SOUTH_CAROLINA_ETHICS_PUBLIC_REPORTING_URL,
  type SouthCarolinaEthicsClientOptions,
  type SouthCarolinaReportDetails,
} from "./southCarolinaEthicsClient.js";
import {
  aggregateSouthCarolinaDirectFinance,
  selectSouthCarolinaAcceptedRuns,
  southCarolinaContributionYearsForRuns,
} from "./southCarolinaDirectContributionAggregator.js";
import { southCarolinaFilerSearchTerm } from "./southCarolinaCandidateFilerResolver.js";
import { normalizeSouthCarolinaCandidateNameForStorage } from "./southCarolinaCandidateFinanceAutoLink.js";
import { isSouthCarolinaFinanceEligibleOffice } from "./southCarolinaFinanceEligibleOffices.js";
import {
  replaceSouthCarolinaCandidateFinanceSnapshot,
  type SouthCarolinaFinanceLinkSource,
} from "./southCarolinaFinanceWriter.js";

type Queryable = Pick<Pool | PoolClient, "query">;
type ConnectableQueryable = Queryable & { connect: () => Promise<PoolClient> };

export class SouthCarolinaCandidateFinanceSyncError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "SouthCarolinaCandidateFinanceSyncError";
  }
}

function nthWeekdayOfMonthUtc(year: number, monthIndex: number, weekday: number, nth: number): number {
  const firstWeekday = new Date(Date.UTC(year, monthIndex, 1)).getUTCDay();
  return 1 + ((weekday - firstWeekday + 7) % 7) + (nth - 1) * 7;
}

// Both the unpadded form the API serves today ("6/9/2026") and the
// zero-padded form ("06/09/2026"). The aggregator matches accepted dates as
// strings, so covering both keeps a source format drift from silently
// dropping runs instead of failing visibly downstream.
function mdyVariants(year: number, month: number, day: number): string[] {
  const padded = `${String(month).padStart(2, "0")}/${String(day).padStart(2, "0")}/${year}`;
  return [...new Set([`${month}/${day}/${year}`, padded])];
}

// The statutory cycle dates for a South Carolina general-election race:
// primary (second Tuesday in June, S.C. Code § 7-13-40), runoff (two weeks
// after the primary), and the linked general itself. Passing the FULL trio
// matters — omitting the primary date would silently drop the primary run's
// money (the aggregator's contract). A linked election whose date is not the
// statutory general (a special election) gets its own date only: its June
// events do not exist, and any special-primary run is conservatively
// excluded rather than guessed at.
export function southCarolinaAcceptedElectionDates(electionYear: number, electionDateIso: string): string[] {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(electionDateIso)) {
    throw new SouthCarolinaCandidateFinanceSyncError(
      `Invalid South Carolina election date: ${electionDateIso}`
    );
  }
  if (Number.parseInt(electionDateIso.slice(0, 4), 10) !== electionYear) {
    throw new SouthCarolinaCandidateFinanceSyncError(
      `election date ${electionDateIso} does not match election year ${electionYear}`
    );
  }
  const linkedMonth = Number.parseInt(electionDateIso.slice(5, 7), 10);
  const linkedDay = Number.parseInt(electionDateIso.slice(8, 10), 10);

  // General: the Tuesday after the first Monday in November.
  const statutoryGeneralDay = nthWeekdayOfMonthUtc(electionYear, 10, 1, 1) + 1;
  const isStatutoryGeneral = linkedMonth === 11 && linkedDay === statutoryGeneralDay;
  if (!isStatutoryGeneral) {
    return mdyVariants(electionYear, linkedMonth, linkedDay);
  }

  const primaryDay = nthWeekdayOfMonthUtc(electionYear, 5, 2, 2);
  const primary = new Date(Date.UTC(electionYear, 5, primaryDay));
  const runoff = new Date(Date.UTC(electionYear, 5, primaryDay + 14));
  return [
    ...mdyVariants(primary.getUTCFullYear(), primary.getUTCMonth() + 1, primary.getUTCDate()),
    ...mdyVariants(runoff.getUTCFullYear(), runoff.getUTCMonth() + 1, runoff.getUTCDate()),
    ...mdyVariants(electionYear, 11, statutoryGeneralDay),
  ];
}

export type SouthCarolinaCandidateFinanceSyncResult = {
  dryRun: boolean;
  status: "synced" | "no_filed_reports";
  acceptedElectionDates: string[];
  runCount: number;
  contributionYears: number[];
  totalReceipts: number | null;
  directContributionTotal: number | null;
  totalDisbursements: number | null;
  cashOnHand: number | null;
  directCoverageNote: string | null;
  includedContributionRowCount: number;
  otherRunContributionRowCount: number;
  summaryWritten: boolean;
  directBreakdownsWritten: number;
};

export async function syncSouthCarolinaCandidateFinance(input: {
  db: ConnectableQueryable;
  candidateId: string;
  electionId: string;
  candidateName: string;
  electionYear: number;
  /** ISO date of the linked (general) election. */
  electionDate: string;
  officeScope: string;
  officeName: string;
  district?: string | null;
  filer: {
    candidateFilerId: number;
    filerName: string;
    linkSource: SouthCarolinaFinanceLinkSource;
    sourceUrl?: string | null;
  };
  now?: Date;
  dryRun?: boolean;
  maxOccupationBreakdowns?: number;
  clientOptions?: SouthCarolinaEthicsClientOptions;
  fetchCandidateReports?: typeof getSouthCarolinaCandidateReports;
  fetchReportDetails?: typeof getSouthCarolinaReportDetails;
  fetchContributions?: typeof searchSouthCarolinaContributions;
}): Promise<SouthCarolinaCandidateFinanceSyncResult> {
  const candidateName = input.candidateName.trim();
  const filerName = input.filer.filerName.trim();
  if (!candidateName || !filerName) {
    throw new SouthCarolinaCandidateFinanceSyncError("candidateName and filer.filerName are required");
  }
  if (!isSouthCarolinaFinanceEligibleOffice({ officeScope: input.officeScope, officeCanonicalName: input.officeName })) {
    throw new SouthCarolinaCandidateFinanceSyncError(
      `office ${input.officeScope}::${input.officeName} is not South Carolina-finance eligible`
    );
  }
  if (!Number.isSafeInteger(input.filer.candidateFilerId) || input.filer.candidateFilerId <= 0) {
    throw new SouthCarolinaCandidateFinanceSyncError(
      `invalid candidate filer id: ${input.filer.candidateFilerId}`
    );
  }
  const now = input.now ?? new Date();
  if (Number.isNaN(now.getTime())) {
    throw new SouthCarolinaCandidateFinanceSyncError("invalid now");
  }
  const dryRun = input.dryRun === true;
  const acceptedElectionDates = southCarolinaAcceptedElectionDates(input.electionYear, input.electionDate);

  const fetchReports = input.fetchCandidateReports ?? getSouthCarolinaCandidateReports;
  const fetchDetails = input.fetchReportDetails ?? getSouthCarolinaReportDetails;
  const fetchContributions = input.fetchContributions ?? searchSouthCarolinaContributions;

  const reports = await fetchReports(input.filer.candidateFilerId, input.clientOptions);
  const runs = selectSouthCarolinaAcceptedRuns(reports, input.electionYear, acceptedElectionDates);
  if (runs.length === 0) {
    // No submitted report for this race's cycle events yet. Never manufacture
    // a zero — leave any prior snapshot untouched.
    return {
      dryRun,
      status: "no_filed_reports",
      acceptedElectionDates,
      runCount: 0,
      contributionYears: [],
      totalReceipts: null,
      directContributionTotal: null,
      totalDisbursements: null,
      cashOnHand: null,
      directCoverageNote: null,
      includedContributionRowCount: 0,
      otherRunContributionRowCount: 0,
      summaryWritten: false,
      directBreakdownsWritten: 0,
    };
  }

  const detailsByReportId = new Map<number, SouthCarolinaReportDetails>();
  for (const run of runs) {
    detailsByReportId.set(
      run.finalReport.reportId,
      await fetchDetails(run.finalReport.reportId, input.clientOptions)
    );
  }

  // Itemized rows: the search endpoint filters by candidate TEXT + calendar
  // year; the filer's surname maximizes recall on the contains-match, and the
  // aggregator narrows strictly by candidateId + accepted officeRunIds.
  const searchTerm = southCarolinaFilerSearchTerm(filerName);
  if (searchTerm === null) {
    throw new SouthCarolinaCandidateFinanceSyncError(
      `cannot derive a contribution search term from filer name ${JSON.stringify(filerName)}`
    );
  }
  const contributionYears = southCarolinaContributionYearsForRuns(
    reports,
    input.electionYear,
    acceptedElectionDates
  );
  const contributionRows = [];
  for (const year of contributionYears) {
    contributionRows.push(
      ...(await fetchContributions({ candidate: searchTerm, contributionYear: year }, input.clientOptions))
    );
  }

  const sourceUrl = input.filer.sourceUrl ?? SOUTH_CAROLINA_ETHICS_PUBLIC_REPORTING_URL;
  const aggregation = aggregateSouthCarolinaDirectFinance({
    candidateFilerId: input.filer.candidateFilerId,
    electionYear: input.electionYear,
    reports,
    detailsByReportId,
    contributionRows,
    acceptedElectionDates,
    sourceUrl,
    maxOccupationBreakdowns: input.maxOccupationBreakdowns,
  });
  if (aggregation.status === "failed") {
    throw new SouthCarolinaCandidateFinanceSyncError(
      `aggregation failed for filer ${input.filer.candidateFilerId}: ${aggregation.diagnostics.join("; ")}`
    );
  }
  if (aggregation.status === "no_filed_reports") {
    // Defensive: unreachable while runs is nonempty, but never write on it.
    return {
      dryRun,
      status: "no_filed_reports",
      acceptedElectionDates,
      runCount: 0,
      contributionYears,
      totalReceipts: null,
      directContributionTotal: null,
      totalDisbursements: null,
      cashOnHand: null,
      directCoverageNote: null,
      includedContributionRowCount: 0,
      otherRunContributionRowCount: 0,
      summaryWritten: false,
      directBreakdownsWritten: 0,
    };
  }

  let summaryWritten = false;
  let directBreakdownsWritten = 0;
  if (!dryRun) {
    const writeResult = await replaceSouthCarolinaCandidateFinanceSnapshot({
      db: input.db,
      link: {
        candidateId: input.candidateId,
        electionId: input.electionId,
        electionYear: input.electionYear,
        candidateNameNormalized: normalizeSouthCarolinaCandidateNameForStorage(candidateName),
        officeName: input.officeName,
        district: input.district ?? null,
        candidateFilerId: input.filer.candidateFilerId,
        filerName,
        linkStatus: "active",
        linkSource: input.filer.linkSource,
        sourceUrl,
        lastVerifiedAt: now,
      },
      syncedAt: now,
      summary: {
        totalReceipts: aggregation.totalReceipts,
        directContributionTotal: aggregation.directContributionTotal,
        totalDisbursements: aggregation.totalDisbursements,
        cashOnHand: aggregation.cashOnHand,
        sourceUrl,
      },
      directBreakdowns: aggregation.directBreakdowns.map((breakdown) => ({
        categoryType: breakdown.categoryType,
        categoryName: breakdown.categoryName,
        amount: breakdown.amount,
        contributorCount: breakdown.contributorCount,
        sourceUrl: breakdown.sourceUrl,
      })),
    });
    summaryWritten = writeResult.summaryWritten;
    directBreakdownsWritten = writeResult.directBreakdownsWritten;
  }

  return {
    dryRun,
    status: "synced",
    acceptedElectionDates,
    runCount: aggregation.runCount,
    contributionYears,
    totalReceipts: aggregation.totalReceipts,
    directContributionTotal: aggregation.directContributionTotal,
    totalDisbursements: aggregation.totalDisbursements,
    cashOnHand: aggregation.cashOnHand,
    directCoverageNote: aggregation.directCoverageNote,
    includedContributionRowCount: aggregation.includedContributionRowCount,
    otherRunContributionRowCount: aggregation.otherRunContributionRowCount,
    summaryWritten,
    directBreakdownsWritten,
  };
}
