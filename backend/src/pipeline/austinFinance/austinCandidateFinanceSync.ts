// Phase 3 per-candidate sync: pulls one linked filer's cycle money from the
// City of Austin Socrata datasets, proves the plan's composition rules
// against the live rows, and writes one all-or-nothing snapshot.
//
// Fail-closed contract (the SF/SJ/Denver pattern): every health-check
// failure THROWS before replaceAustinCandidateFinanceSnapshot is called, so
// the prior snapshot survives untouched. A filer whose reports affirmatively
// say "$0 raised, $0 spent" writes a zero snapshot: "the source says
// nothing" and "the source is broken" are different outcomes.
//
// Checks proven on every sync:
//   1. The filer still has an effective report for THIS election date and
//      office code (a filer whose rows moved to another seat, or a Socrata
//      outage returning nothing, keeps the prior snapshot).
//   2. Per cycle report, the itemized contribution rows never exceed the
//      cover `contrib_total`, and reported contributions come with itemized
//      rows (inside the direct aggregator).
//   3. The DCE and Committee Purpose datasets returned rows at all — an
//      empty dataset would silently zero every candidate's outside spending.
//   4. Previous-vs-new receipts drop bound (the SF constants).
// Cash on hand = `contrib_balance` of the latest cycle report.

import type { Pool, PoolClient } from "pg";

import { aggregateAustinDirectFinance } from "./austinDirectFinanceAggregator.js";
import {
  aggregateAustinOutsideSpending,
  type AustinReportFacts,
} from "./austinOutsideSpendingAggregator.js";
import { AUSTIN_FINANCE_LINK_SOURCE_URL } from "./austinCandidateFinanceAutoLink.js";
import {
  isAustinFinanceSupportedElectionDate,
  type AustinOfficeCode,
} from "./austinFinanceEligibleOffices.js";
import { normalizeAustinFinanceTextKey } from "./austinFinanceKeys.js";
import { replaceAustinCandidateFinanceSnapshot } from "./austinFinanceWriter.js";
import {
  defaultAustinSocrataClientOptions,
  getAustinCommitteePurposeRows,
  getAustinContributionRowsByRecipient,
  getAustinDirectCampaignExpenditureRows,
  getAustinReportDetailRowsByFiler,
  getAustinReportDetailRowsByReportIds,
  requireIsoDate,
  type AustinCommitteePurposeRow,
  type AustinDirectCampaignExpenditureRow,
  type AustinSocrataClientOptions,
} from "./austinSocrataClient.js";

type Queryable = Pick<Pool | PoolClient, "query">;
type PoolLike = Queryable & { connect: () => Promise<PoolClient> };

// Anomaly bound, the SF constants: an order-of-magnitude receipts drop on a
// re-sync aborts the write; floored at $1,000 stored so micro-campaigns
// cannot trip it on rounding noise.
const ANOMALY_MIN_STORED_CENTS = 100_000;
const ANOMALY_DROP_FACTOR = 10;

/** Exact "[-]dollars.cc" text (numeric(16,2)) into integer cents; null passes. */
function dollarsTextToCents(value: string | null): number | null {
  if (value === null) return null;
  const match = /^(-?)(\d+)(?:\.(\d{1,2}))?$/.exec(value.trim());
  if (!match) throw new Error(`Unparseable stored dollar amount: ${value}`);
  const sign = match[1] === "-" ? -1 : 1;
  return (
    sign * (Number(match[2]) * 100 + Number((match[3] ?? "0").padEnd(2, "0")))
  );
}

function usd(cents: number): string {
  const sign = cents < 0 ? "-" : "";
  const abs = Math.abs(cents);
  return `${sign}$${Math.trunc(abs / 100)}.${String(abs % 100).padStart(2, "0")}`;
}

/**
 * The two small city-wide datasets plus the Report Detail facts (form type,
 * period) of every report they reference, fetched once per batch run.
 */
export type AustinOutsideDatasets = {
  dceRows: AustinDirectCampaignExpenditureRow[];
  purposeRows: AustinCommitteePurposeRow[];
  reportsById: Map<string, AustinReportFacts>;
};

export async function loadAustinOutsideDatasets(
  options: AustinSocrataClientOptions = defaultAustinSocrataClientOptions(),
): Promise<AustinOutsideDatasets> {
  const dceRows = await getAustinDirectCampaignExpenditureRows(options);
  const purposeRows = await getAustinCommitteePurposeRows(options);
  // Check 3: both datasets hold hundreds of rows; an empty answer is a
  // broken source, not a city with no outside spending.
  if (dceRows.length === 0)
    throw new Error("Austin Direct Campaign Expenditures dataset returned no rows; refusing to zero outside spending");
  if (purposeRows.length === 0)
    throw new Error("Austin Committee Purpose dataset returned no rows; refusing to zero outside spending");
  const reportIds = [
    ...dceRows.map((row) => row.reportId),
    ...purposeRows.flatMap((row) => (row.reportId === null ? [] : [row.reportId])),
  ];
  const reports = await getAustinReportDetailRowsByReportIds(reportIds, options);
  // Report Detail covers filings from 2023 on; every live DCE/purpose report
  // resolves today (182/182 on 2026-08-19). Zero hits for hundreds of ids is
  // a broken join, not a gap.
  if (reports.length === 0)
    throw new Error(`Austin Report Detail returned no rows for ${new Set(reportIds).size} referenced PAC reports; refusing to aggregate outside spending`);
  const reportsById = new Map<string, AustinReportFacts>();
  for (const report of reports)
    reportsById.set(report.reportId, {
      formTypeCode: report.formTypeCode,
      periodFrom: report.periodFrom,
      periodTo: report.periodTo,
      dateFiled: report.dateFiled,
    });
  return { dceRows, purposeRows, reportsById };
}

export type AustinCandidateFinanceSyncResult = {
  written: boolean;
  totalRaisedCents: number;
  totalSpentCents: number;
  cashOnHandCents: number | null;
  outsideSupportCents: number;
  outsideOpposeCents: number;
  directBreakdownCount: number;
  outsideGroupCount: number;
  cycleReportCount: number;
  keptSpecialReportCount: number;
  itemizedRowCount: number;
  nonReceiptRowCount: number;
  /** Reported on covers but not itemized on the schedule (Σ over cycle reports). */
  unitemizedCents: number;
  /** ISO window the outside spending was scoped to. */
  outsideWindow: { from: string; to: string };
  /** Excluded outside dollars, by reason (see austinOutsideSpendingAggregator). */
  outsideMultiTargetCents: number;
  outsideUndirectedCents: number;
  outsideUndirectedSpenders: string[];
  outsideAmbiguousDirectionCents: number;
  outsideSelfCents: number;
};

export async function syncAustinCandidateFinance(input: {
  db: PoolLike;
  candidateId: string;
  electionId: string;
  electionYear: number;
  candidateDisplayName: string;
  /** Link facts (office/filer identity stay the link's, not re-derived). */
  officeName: string;
  district: string | null;
  filerName: string;
  /** Election facts from the elections row (batch sync resolves them). */
  electionDate: string;
  officeCode: AustinOfficeCode;
  /** City-wide DCE + purpose rows, prefetched once per batch run. */
  outsideDatasets?: AustinOutsideDatasets;
  /** Operator override for the previous-vs-new drop bound only. */
  bypassAnomalyCheck?: boolean;
  dryRun?: boolean;
  now?: Date;
  clientOptions?: AustinSocrataClientOptions;
}): Promise<AustinCandidateFinanceSyncResult> {
  const now = input.now ?? new Date();
  if (Number.isNaN(now.getTime()))
    throw new Error("Invalid Austin finance sync timestamp");
  const electionDate = requireIsoDate(input.electionDate, "election date");
  if (!isAustinFinanceSupportedElectionDate(electionDate))
    throw new Error(
      `Austin finance election date ${electionDate} is not in the v1 allowlist`,
    );
  const options = input.clientOptions ?? defaultAustinSocrataClientOptions();
  const filerName = input.filerName.trim();
  if (!filerName) throw new Error("Austin finance sync requires the link's filer name");

  // --- Direct money: every report and every itemized row under the exact
  // filer string, scoped to the cycle inside the aggregator (check 1 + 2).
  const reports = await getAustinReportDetailRowsByFiler(filerName, options);
  const contributions = await getAustinContributionRowsByRecipient(filerName, options);
  const direct = aggregateAustinDirectFinance({
    reports,
    contributions,
    filerName,
    electionDate,
    officeCode: input.officeCode,
  });

  // --- Outside spending, scoped to the span of the cycle's reports (the
  // Phase 0 window) extended through election day: DCE rows carry no
  // election tag, and the same candidate can run for the same seat in
  // consecutive cycles (Qadri D9 2022 and 2026), so the date window is what
  // separates cycles.
  const cycleReports = [...direct.cycleReports, ...direct.keptSpecialReports];
  const periodsFrom = cycleReports.map((row) => row.periodFrom!).sort();
  const periodsTo = cycleReports.map((row) => row.periodTo!).sort();
  const windowFrom = periodsFrom[0]!;
  const windowTo = [periodsTo[periodsTo.length - 1]!, electionDate].sort()[1]!;
  const datasets = input.outsideDatasets ?? (await loadAustinOutsideDatasets(options));
  const outside = aggregateAustinOutsideSpending({
    dceRows: datasets.dceRows,
    purposeRows: datasets.purposeRows,
    reportsById: datasets.reportsById,
    candidateDisplayName: input.candidateDisplayName,
    filerName,
    officeCode: input.officeCode,
    electionDate,
    windowFrom,
    windowTo,
  });

  // --- Previous-vs-new drop bound (baseline = this filer's active link).
  const filerKey = normalizeAustinFinanceTextKey(filerName);
  const stored = await input.db.query<{ total_receipts: string | null }>(
    `SELECT summary.total_receipts::text FROM public.atx_candidate_finance_summaries summary JOIN public.atx_candidate_finance_links link ON link.id=summary.link_id WHERE link.candidate_id=$1::uuid AND link.election_id=$2::uuid AND summary.election_year=$3 AND link.link_status='active' AND link.filer_key=$4`,
    [input.candidateId, input.electionId, input.electionYear, filerKey],
  );
  const storedReceiptsCents = dollarsTextToCents(
    stored.rows[0]?.total_receipts ?? null,
  );
  if (
    !input.bypassAnomalyCheck &&
    storedReceiptsCents !== null &&
    storedReceiptsCents >= ANOMALY_MIN_STORED_CENTS &&
    direct.totalRaisedCents < storedReceiptsCents / ANOMALY_DROP_FACTOR
  )
    throw new Error(
      `Austin total receipts collapsed for filer ${JSON.stringify(filerName)}: ${usd(storedReceiptsCents)} -> ${usd(direct.totalRaisedCents)} (pass bypassAnomalyCheck to override)`,
    );

  const directBreakdowns = direct.breakdowns.map((row) => ({
    ...row,
    sourceUrl: AUSTIN_FINANCE_LINK_SOURCE_URL,
  }));
  const outsideGroups = outside.groups.map((group) => ({
    ...group,
    sourceUrl: AUSTIN_FINANCE_LINK_SOURCE_URL,
  }));

  if (!input.dryRun)
    await replaceAustinCandidateFinanceSnapshot({
      db: input.db,
      link: {
        candidateId: input.candidateId,
        electionId: input.electionId,
        electionYear: input.electionYear,
        candidateNameNormalized: normalizeAustinFinanceTextKey(
          input.candidateDisplayName,
        ),
        officeName: input.officeName,
        district: input.district,
        filerName,
        linkStatus: "active",
        // Always the automatic source: on a protected manual link with this
        // filer the writer refreshes filer_name + last_verified_at only and
        // the snapshot attaches to the operator's row (Phase 1 contract).
        linkSource: "austin_clerk",
        sourceUrl: AUSTIN_FINANCE_LINK_SOURCE_URL,
        lastVerifiedAt: now,
      },
      summary: {
        totalReceiptsCents: direct.totalRaisedCents,
        directContributionTotalCents: direct.totalRaisedCents,
        totalDisbursementsCents: direct.totalSpentCents,
        cashOnHandCents: direct.cashOnHandCents,
        outsideSupportCents: outside.supportTotalCents,
        outsideOpposeCents: outside.opposeTotalCents,
        sourceUrl: AUSTIN_FINANCE_LINK_SOURCE_URL,
      },
      directBreakdowns,
      outsideGroups,
      // PAC funder breakdowns are a follow-up phase; the table stays empty.
      outsideGroupBreakdowns: [],
      syncedAt: now,
    });

  return {
    written: !input.dryRun,
    totalRaisedCents: direct.totalRaisedCents,
    totalSpentCents: direct.totalSpentCents,
    cashOnHandCents: direct.cashOnHandCents,
    outsideSupportCents: outside.supportTotalCents,
    outsideOpposeCents: outside.opposeTotalCents,
    directBreakdownCount: directBreakdowns.length,
    outsideGroupCount: outsideGroups.length,
    cycleReportCount: direct.cycleReports.length,
    keptSpecialReportCount: direct.keptSpecialReports.length,
    itemizedRowCount: direct.itemizedRowCount,
    nonReceiptRowCount: direct.nonReceiptRowCount,
    unitemizedCents: direct.unitemizedCents,
    outsideWindow: { from: windowFrom, to: windowTo },
    outsideMultiTargetCents: outside.multiTargetCents,
    outsideUndirectedCents: outside.undirectedCents,
    outsideUndirectedSpenders: outside.undirectedSpenders,
    outsideAmbiguousDirectionCents: outside.ambiguousDirectionCents,
    outsideSelfCents: outside.selfCents,
  };
}
