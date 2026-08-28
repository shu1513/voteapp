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
import { southCarolinaAcceptedElectionDates } from "./southCarolinaElectionCalendar.js";
import { southCarolinaConflictingOfficeLabels } from "./southCarolinaOfficeEvidence.js";
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

export { southCarolinaAcceptedElectionDates };

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

  // Office-evidence gate: contribution rows carry the office label of their
  // RUN, and legislative labels are real and district-scoped even though the
  // per-filer labels are broken. A label among the accepted runs' rows that
  // contradicts the linked race means the linked filer is a same-named
  // different person — fail closed before any money is written. Veto-only:
  // uninterpretable labels (the literal "4" on current statewide runs) are
  // no evidence and never block.
  const acceptedRunIds = new Set(runs.map((run) => run.campaignId));
  const conflictingLabels = southCarolinaConflictingOfficeLabels({
    officeScope: input.officeScope,
    district: input.district,
    rowOfficeLabels: contributionRows
      .filter((row) => row.candidateId === input.filer.candidateFilerId && acceptedRunIds.has(row.officeRunId))
      .map((row) => row.officeName),
  });
  if (conflictingLabels.length > 0) {
    throw new SouthCarolinaCandidateFinanceSyncError(
      `filer ${input.filer.candidateFilerId} run office evidence contradicts the linked race ` +
        `${input.officeScope}::${input.officeName} (district ${input.district ?? "none"}): ` +
        conflictingLabels.join(", ")
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
