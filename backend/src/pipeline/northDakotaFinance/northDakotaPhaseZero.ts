// Phase 0A only: pure summarization/reconciliation helpers behind the probe
// script. No database, cache, scheduler, or published snapshot.

import { createHash } from "node:crypto";

import {
  NORTH_DAKOTA_CANDIDATE_COMMITTEE_ORG_TYPE,
  type NorthDakotaChartSeries,
  type NorthDakotaCommitteeRow,
  type NorthDakotaTransactionRow,
} from "./northDakotaCfrsClient.js";
import type {
  NorthDakotaContributionCsvRow,
  NorthDakotaExpenditureCsvRow,
  NorthDakotaFiledReportCsvRow,
  NorthDakotaReportingScheduleCsvRow,
} from "./northDakotaCfrsCsv.js";

export function centsToDollars(cents: number): string {
  return (cents / 100).toFixed(2);
}

// API amounts arrive as JSON numbers; observed values are cent-precise.
// Convert defensively and fail on sub-cent residue.
export function apiAmountToCents(amount: number): number {
  const cents = Math.round(amount * 100);
  if (!Number.isSafeInteger(cents) || Math.abs(cents - amount * 100) > 1e-6 * Math.max(1, Math.abs(cents))) {
    throw new Error(`North Dakota API amount is not cent-precise: ${amount}`);
  }
  return cents;
}

export type NorthDakotaBucket = { rowCount: number; totalCents: number; totalDollars: string };

function bucketRows<T>(
  rows: readonly T[],
  key: (row: T) => string,
  cents: (row: T) => number
): Record<string, NorthDakotaBucket> {
  const buckets: Record<string, { rowCount: number; totalCents: number }> = {};
  for (const row of rows) {
    const k = key(row);
    const bucket = (buckets[k] ??= { rowCount: 0, totalCents: 0 });
    bucket.rowCount += 1;
    bucket.totalCents += cents(row);
  }
  return Object.fromEntries(
    Object.entries(buckets)
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([k, bucket]) => [k, { ...bucket, totalDollars: centsToDollars(bucket.totalCents) }])
  );
}

// Lump-sum rows carry a blank counterparty type in the CSV; the portal charts
// label the same slice "Lumpsum".
export const LUMPSUM_COUNTERPARTY_LABEL = "Lumpsum";

function counterpartyLabel(type: string): string {
  return type === "" ? LUMPSUM_COUNTERPARTY_LABEL : type;
}

// --- registry lookups -------------------------------------------------------

export type NorthDakotaOfficeClass = "statewide" | "legislative" | "judicial" | "unknown";

// Probe-only classifier over the registry's office labels. Legislative and
// judicial are matched by label; every other named office is statewide (ND
// centralizes only statewide, legislative, judicial and district-party
// filers — local offices never appear in CFRS).
export function classifyNorthDakotaOffice(office: string | null): NorthDakotaOfficeClass {
  if (office === null) return "unknown";
  if (/^State (Representative|Senator)\b/i.test(office)) return "legislative";
  if (/\b(Judge|Justice|Judicial|Supreme Court|District Court)\b/i.test(office)) return "judicial";
  return "statewide";
}

export function indexNorthDakotaCommittees(
  committees: readonly NorthDakotaCommitteeRow[]
): Map<string, NorthDakotaCommitteeRow> {
  return new Map(committees.map((committee) => [committee.entityId, committee]));
}

// --- bulk CSV summaries -----------------------------------------------------

export type NorthDakotaContributionCsvSummary = {
  rowCount: number;
  recoveredRowCount: number;
  totalCents: number;
  totalDollars: string;
  registrantCount: number;
  byCategory: Record<string, NorthDakotaBucket>;
  byContributorType: Record<string, NorthDakotaBucket>;
  /** Registry orgType of the owning registrant; "<unregistered>" when the
   * RegistrantID is missing from the registry. */
  byCommitteeType: Record<string, NorthDakotaBucket>;
  duplicateRowCount: number;
  dateRange: { min: string; max: string } | null;
};

export function summarizeNorthDakotaContributionCsv(
  rows: readonly NorthDakotaContributionCsvRow[],
  committeesById: ReadonlyMap<string, NorthDakotaCommitteeRow>
): NorthDakotaContributionCsvSummary {
  const seen = new Set<string>();
  let duplicateRowCount = 0;
  let min: string | null = null;
  let max: string | null = null;
  for (const row of rows) {
    const key = [
      row.registrantId,
      row.transactionCategory,
      row.transactionDate,
      row.amountCents,
      row.contributorType,
      row.contributorName,
      row.employerName ?? "",
      row.filedDate,
    ].join("\u0000");
    if (seen.has(key)) duplicateRowCount += 1;
    else seen.add(key);
    if (min === null || row.transactionDate < min) min = row.transactionDate;
    if (max === null || row.transactionDate > max) max = row.transactionDate;
  }
  const totalCents = rows.reduce((sum, row) => sum + row.amountCents, 0);
  return {
    rowCount: rows.length,
    recoveredRowCount: rows.filter((row) => row.recovered).length,
    totalCents,
    totalDollars: centsToDollars(totalCents),
    registrantCount: new Set(rows.map((row) => row.registrantId)).size,
    byCategory: bucketRows(rows, (row) => row.transactionCategory, (row) => row.amountCents),
    byContributorType: bucketRows(rows, (row) => counterpartyLabel(row.contributorType), (row) => row.amountCents),
    byCommitteeType: bucketRows(
      rows,
      (row) => committeesById.get(row.registrantId)?.orgType ?? "<unregistered>",
      (row) => row.amountCents
    ),
    duplicateRowCount,
    dateRange: min && max ? { min, max } : null,
  };
}

// Year-end statements report expenditures "by expenditure category" (NDCC
// 16.1-08.1-02.3). In the bulk file those lumps carry ExpenditureType
// "Monetary", a December-31 transaction date, a purpose label and no
// recipient (verified 2026-09-01: 562 rows in the 2025 file, and the only
// shape candidate committees ever appear in). The portal's "By Purpose Type"
// chart series is exactly these rows.
export const NORTH_DAKOTA_YEAR_END_EXPENDITURE_TYPE = "Monetary";

export type NorthDakotaExpenditureCsvSummary = {
  rowCount: number;
  recoveredRowCount: number;
  totalCents: number;
  totalDollars: string;
  byExpenditureType: Record<string, NorthDakotaBucket>;
  byRecipientType: Record<string, NorthDakotaBucket>;
  byCommitteeType: Record<string, NorthDakotaBucket>;
  yearEndCategoryLumps: {
    rowCount: number;
    totalCents: number;
    totalDollars: string;
    byPurpose: Record<string, NorthDakotaBucket>;
    byCommitteeType: Record<string, NorthDakotaBucket>;
    transactionDates: string[];
  };
};

export function summarizeNorthDakotaExpenditureCsv(
  rows: readonly NorthDakotaExpenditureCsvRow[],
  committeesById: ReadonlyMap<string, NorthDakotaCommitteeRow>
): NorthDakotaExpenditureCsvSummary {
  const totalCents = rows.reduce((sum, row) => sum + row.amountCents, 0);
  const committeeType = (row: NorthDakotaExpenditureCsvRow) => committeesById.get(row.registrantId)?.orgType ?? "<unregistered>";
  const lumps = rows.filter((row) => row.expenditureType === NORTH_DAKOTA_YEAR_END_EXPENDITURE_TYPE);
  const lumpCents = lumps.reduce((sum, row) => sum + row.amountCents, 0);
  return {
    rowCount: rows.length,
    recoveredRowCount: rows.filter((row) => row.recovered).length,
    totalCents,
    totalDollars: centsToDollars(totalCents),
    byExpenditureType: bucketRows(rows, (row) => row.expenditureType, (row) => row.amountCents),
    byRecipientType: bucketRows(rows, (row) => counterpartyLabel(row.recipientType), (row) => row.amountCents),
    byCommitteeType: bucketRows(rows, committeeType, (row) => row.amountCents),
    yearEndCategoryLumps: {
      rowCount: lumps.length,
      totalCents: lumpCents,
      totalDollars: centsToDollars(lumpCents),
      byPurpose: bucketRows(lumps, (row) => row.expenditurePurpose, (row) => row.amountCents),
      byCommitteeType: bucketRows(lumps, committeeType, (row) => row.amountCents),
      transactionDates: [...new Set(lumps.map((row) => row.transactionDate))].sort(),
    },
  };
}

// --- portal chart reconciliation --------------------------------------------

// The chart endpoints return all-years totals split by counterparty type and
// by committee type. The CSV files (every year in the catalog) must sum to the
// same figures to the cent, per slice.
export type NorthDakotaChartReconciliation = {
  chartTotalCents: number;
  csvTotalCents: number;
  totalMatch: boolean;
  series: Array<{
    name: string;
    chartTotalCents: number;
    compared: boolean;
    mismatches: Array<{ description: string; chartCents: number | null; csvCents: number | null }>;
  }>;
};

export function reconcileNorthDakotaChart(input: {
  chart: readonly NorthDakotaChartSeries[];
  csvTotalCents: number;
  /** CSV slices keyed by the chart's series name, e.g. "By Contributor Type"
   * -> byContributorType buckets. Series without a CSV analog are reported
   * as uncompared. */
  csvSlices: Record<string, Record<string, NorthDakotaBucket>>;
}): NorthDakotaChartReconciliation {
  const chartTotalCents = input.chart.length > 0 ? apiAmountToCents(input.chart[0].totalAmount) : 0;
  return {
    chartTotalCents,
    csvTotalCents: input.csvTotalCents,
    totalMatch: input.chart.length > 0 && chartTotalCents === input.csvTotalCents,
    series: input.chart.map((series) => {
      const slice = input.csvSlices[series.name];
      if (!slice) {
        return { name: series.name, chartTotalCents: apiAmountToCents(series.totalAmount), compared: false, mismatches: [] };
      }
      const mismatches: NorthDakotaChartReconciliation["series"][number]["mismatches"] = [];
      const seen = new Set<string>();
      for (const point of series.data) {
        seen.add(point.description);
        const chartCents = apiAmountToCents(point.amount);
        const csvCents = slice[point.description]?.totalCents ?? null;
        if (csvCents !== chartCents) mismatches.push({ description: point.description, chartCents, csvCents });
      }
      for (const [description, bucket] of Object.entries(slice)) {
        if (!seen.has(description)) mismatches.push({ description, chartCents: null, csvCents: bucket.totalCents });
      }
      return { name: series.name, chartTotalCents: apiAmountToCents(series.totalAmount), compared: true, mismatches };
    }),
  };
}

// --- CSV vs API committee reconciliation ------------------------------------

// The bulk CSV has no report identity, so the comparison is committee-level:
// a multiset of (transactionDate, amountCents, category, counterparty type).
// Both surfaces use the same category and type vocabulary (verified
// 2026-09-02: 8,254 rows across 2025-2026 agree row for row; the CSV's blank
// type on lump rows is the API's null), so a classification change — an
// amendment that turns an "Individual" gift into "Candidate" money at the
// same date and amount — is a mismatch, not a pass. The aggregator keys its
// money model on exactly these two fields.
export type NorthDakotaCommitteeReconciliation = {
  entityId: string;
  csvRowCount: number;
  apiRowCount: number;
  csvTotalCents: number;
  apiTotalCents: number;
  totalsMatch: boolean;
  multisetMatch: boolean;
  onlyInCsv: number;
  onlyInApi: number;
  amendedApiRowCount: number;
  csvCategoryCounts: Record<string, number>;
  apiCategoryCounts: Record<string, number>;
  apiReports: Array<{ reportFileName: string; reportVersionID: string | null; rowCount: number; totalDollars: string }>;
};

function countBy<T>(rows: readonly T[], key: (row: T) => string): Record<string, number> {
  const counts: Record<string, number> = {};
  for (const row of rows) {
    const k = key(row);
    counts[k] = (counts[k] ?? 0) + 1;
  }
  return counts;
}

export function reconcileNorthDakotaCommittee(input: {
  entityId: string;
  csvRows: readonly NorthDakotaContributionCsvRow[];
  apiRows: readonly NorthDakotaTransactionRow[];
}): NorthDakotaCommitteeReconciliation {
  const csvRows = input.csvRows.filter((row) => row.registrantId === input.entityId);
  const apiRows = input.apiRows.filter((row) => row.entityID === input.entityId);
  const csvTotalCents = csvRows.reduce((sum, row) => sum + row.amountCents, 0);
  const apiTotalCents = apiRows.reduce((sum, row) => sum + apiAmountToCents(row.transactionAmount), 0);

  const key = (date: string, cents: number, category: string, counterpartyType: string) =>
    [date.slice(0, 10), cents, category, counterpartyType].join("\u0000");
  const remaining = new Map<string, number>();
  for (const row of csvRows) {
    const k = key(row.transactionDate, row.amountCents, row.transactionCategory, row.contributorType);
    remaining.set(k, (remaining.get(k) ?? 0) + 1);
  }
  let onlyInApi = 0;
  for (const row of apiRows) {
    const k = key(
      row.transactionDate,
      apiAmountToCents(row.transactionAmount),
      row.transactionCategoryDesc ?? "",
      row.entityTypeDesc ?? ""
    );
    const count = remaining.get(k) ?? 0;
    if (count > 0) remaining.set(k, count - 1);
    else onlyInApi += 1;
  }
  const onlyInCsv = [...remaining.values()].reduce((sum, count) => sum + count, 0);

  const reports = new Map<string, { reportFileName: string; reportVersionID: string | null; rowCount: number; totalCents: number }>();
  for (const row of apiRows) {
    const reportFileName = row.reportFileName ?? "<no report>";
    const groupKey = `${reportFileName}\u0000${row.reportVersionID ?? ""}`;
    const entry = reports.get(groupKey) ?? { reportFileName, reportVersionID: row.reportVersionID, rowCount: 0, totalCents: 0 };
    entry.rowCount += 1;
    entry.totalCents += apiAmountToCents(row.transactionAmount);
    reports.set(groupKey, entry);
  }

  return {
    entityId: input.entityId,
    csvRowCount: csvRows.length,
    apiRowCount: apiRows.length,
    csvTotalCents,
    apiTotalCents,
    totalsMatch: csvTotalCents === apiTotalCents,
    multisetMatch: onlyInCsv === 0 && onlyInApi === 0,
    onlyInCsv,
    onlyInApi,
    amendedApiRowCount: apiRows.filter((row) => row.amendedFlag).length,
    csvCategoryCounts: countBy(csvRows, (row) => row.transactionCategory),
    apiCategoryCounts: countBy(apiRows, (row) => row.transactionCategoryDesc ?? "<null>"),
    apiReports: [...reports.values()]
      .sort((a, b) => a.reportFileName.localeCompare(b.reportFileName))
      .map(({ totalCents, ...entry }) => ({ ...entry, totalDollars: centsToDollars(totalCents) })),
  };
}

// --- transactionTotalYTD semantics ------------------------------------------

// Verified 2026-09-01: transactionTotalYTD is the committee x counterparty
// year-to-date aggregate as of the row's report - a donor's running total on
// contribution rows, a payee's running total on IE rows. It is neither a
// report total nor a committee total (North Dakotans for Public Schools paid
// one vendor across three filings: 2,414.57 -> 4,332.23 -> 6,716.09; StrongND
// used a different vendor per filing, so each control equalled its own
// filing). The check: per calendar year of the transaction date (the
// aggregate resets each year), for every (committee, counterparty), the
// largest YTD value equals the sum of that pair's unique rows.
export type NorthDakotaYtdSemanticsCheck = {
  groupCount: number;
  matchingGroupCount: number;
  /** Groups with no parseable YTD on any row. */
  missingControlGroupCount: number;
  /** counterpartyRef is a numeric payee id or a short hash of the name key —
   * contributor names never reach diagnostics. */
  mismatches: Array<{ entityId: string; year: string; counterpartyRef: string; sumCents: number; maxYtdCents: number; rowCount: number }>;
};

function ytdToCents(value: string | null): number | null {
  if (value === null) return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? apiAmountToCents(parsed) : null;
}

function counterpartyKey(row: NorthDakotaTransactionRow): string {
  return row.contributorPayeeID !== null ? `id:${row.contributorPayeeID}` : `name:${row.contributorPayeeName ?? ""}`;
}

function counterpartyRef(key: string): string {
  return key.startsWith("id:") ? key : `name#${createHash("sha256").update(key).digest("hex").slice(0, 12)}`;
}

export function checkNorthDakotaYtdSemantics(rows: readonly NorthDakotaTransactionRow[]): NorthDakotaYtdSemanticsCheck {
  const groups = new Map<string, { entityId: string; year: string; counterparty: string; ids: Set<number>; sumCents: number; maxYtd: number | null }>();
  for (const row of rows) {
    const counterparty = counterpartyKey(row);
    const year = row.transactionDate.slice(0, 4);
    const key = `${row.entityID} ${year} ${counterparty}`;
    const group = groups.get(key) ?? { entityId: row.entityID, year, counterparty, ids: new Set<number>(), sumCents: 0, maxYtd: null };
    if (!group.ids.has(row.transactionID)) {
      group.ids.add(row.transactionID);
      group.sumCents += apiAmountToCents(row.transactionAmount);
    }
    const ytd = ytdToCents(row.transactionTotalYTD);
    if (ytd !== null && (group.maxYtd === null || ytd > group.maxYtd)) group.maxYtd = ytd;
    groups.set(key, group);
  }
  const mismatches: NorthDakotaYtdSemanticsCheck["mismatches"] = [];
  let matching = 0;
  let missing = 0;
  for (const group of groups.values()) {
    if (group.maxYtd === null) missing += 1;
    else if (group.maxYtd === group.sumCents) matching += 1;
    else if (mismatches.length < 25) {
      mismatches.push({
        entityId: group.entityId,
        year: group.year,
        counterpartyRef: counterpartyRef(group.counterparty),
        sumCents: group.sumCents,
        maxYtdCents: group.maxYtd,
        rowCount: group.ids.size,
      });
    }
  }
  return { groupCount: groups.size, matchingGroupCount: matching, missingControlGroupCount: missing, mismatches };
}

// --- independent expenditures -----------------------------------------------

// IE rows are per-candidate allocations of one filing: each row has its own
// transactionID. Committee totals = sum over unique transactionIDs; the
// per-payee YTD check above is the reconciliation control.
export type NorthDakotaIndependentExpenditureSummary = {
  rowCount: number;
  distinctTransactionIdCount: number;
  /** Rows whose transactionTypeDesc is not "Independent Expenditures" - the
   * selector is a query mode, so every row is self-classified. */
  offTypeRowCount: number;
  /** Rows missing a filed Support/Oppose stance, a candidate target or an
   * election year — the fields every support/oppose summary depends on. */
  missingStanceRowCount: number;
  missingCandidateRowCount: number;
  missingElectionYearRowCount: number;
  stanceCounts: Record<string, number>;
  candidateCount: number;
  committees: Array<{ entityId: string; committeeName: string | null; reportCount: number; rowCount: number; totalDollars: string }>;
  reports: Array<{ entityId: string; reportKey: string; filedDate: string | null; rowCount: number; sumDollars: string }>;
  payeeYtd: NorthDakotaYtdSemanticsCheck;
  totalCents: number;
  totalDollars: string;
  /** sha256 over the sorted distinct transactionIDs - compare across days to
   * test identifier stability (hard fact 4 / gate 5). */
  transactionIdDigest: string;
};

export function summarizeNorthDakotaIndependentExpenditures(
  rows: readonly NorthDakotaTransactionRow[]
): NorthDakotaIndependentExpenditureSummary {
  const seenIds = new Set<number>();
  let totalCents = 0;
  for (const row of rows) {
    if (seenIds.has(row.transactionID)) continue;
    seenIds.add(row.transactionID);
    totalCents += apiAmountToCents(row.transactionAmount);
  }

  const reportGroups = new Map<string, { entityId: string; rows: NorthDakotaTransactionRow[] }>();
  for (const row of rows) {
    const reportKey = row.s3ReportFilePath ?? `${row.reportFileName ?? "<no report>"} ${row.reportVersionID ?? ""}`;
    const groupKey = `${row.entityID} ${reportKey}`;
    const group = reportGroups.get(groupKey) ?? { entityId: row.entityID, rows: [] };
    group.rows.push(row);
    reportGroups.set(groupKey, group);
  }
  const reports = [...reportGroups.entries()]
    .map(([groupKey, group]) => ({
      entityId: group.entityId,
      reportKey: groupKey.slice(group.entityId.length + 1),
      filedDate: group.rows[0].filedDate,
      rowCount: group.rows.length,
      sumDollars: centsToDollars(group.rows.reduce((sum, row) => sum + apiAmountToCents(row.transactionAmount), 0)),
    }))
    .sort((a, b) => a.entityId.localeCompare(b.entityId) || (a.filedDate ?? "").localeCompare(b.filedDate ?? ""));

  const byCommittee = new Map<string, { committeeName: string | null; reportKeys: Set<string>; rowCount: number; totalCents: number; ids: Set<number> }>();
  for (const row of rows) {
    const entry = byCommittee.get(row.entityID) ?? {
      committeeName: row.committeeName,
      reportKeys: new Set<string>(),
      rowCount: 0,
      totalCents: 0,
      ids: new Set<number>(),
    };
    entry.reportKeys.add(row.s3ReportFilePath ?? `${row.reportFileName ?? ""}${row.reportVersionID ?? ""}`);
    entry.rowCount += 1;
    if (!entry.ids.has(row.transactionID)) {
      entry.ids.add(row.transactionID);
      entry.totalCents += apiAmountToCents(row.transactionAmount);
    }
    byCommittee.set(row.entityID, entry);
  }

  const sortedIds = [...seenIds].sort((a, b) => a - b);
  return {
    rowCount: rows.length,
    distinctTransactionIdCount: seenIds.size,
    offTypeRowCount: rows.filter((row) => row.transactionTypeDesc !== "Independent Expenditures").length,
    missingStanceRowCount: rows.filter((row) => row.stanceDescription !== "Support" && row.stanceDescription !== "Oppose").length,
    missingCandidateRowCount: rows.filter((row) => row.candidateNameAssocation === null).length,
    missingElectionYearRowCount: rows.filter((row) => row.electionYear === null).length,
    stanceCounts: countBy(rows, (row) => row.stanceDescription ?? "<null>"),
    candidateCount: new Set(rows.map((row) => row.candidateNameAssocation ?? "<null>")).size,
    committees: [...byCommittee.entries()]
      .map(([entityId, entry]) => ({
        entityId,
        committeeName: entry.committeeName,
        reportCount: entry.reportKeys.size,
        rowCount: entry.rowCount,
        totalDollars: centsToDollars(entry.totalCents),
      }))
      .sort((a, b) => a.entityId.localeCompare(b.entityId)),
    reports,
    payeeYtd: checkNorthDakotaYtdSemantics(rows),
    totalCents,
    totalDollars: centsToDollars(totalCents),
    transactionIdDigest: createHash("sha256").update(sortedIds.join(",")).digest("hex"),
  };
}

// --- occupation coverage ----------------------------------------------------

// NDCC 16.1-08.1-02.3: occupation/employer are required once an INDIVIDUAL's
// aggregate reaches $5,000 in a reporting period, except for judicial (and
// local) candidates. ND reporting periods are cumulative from January 1, so
// a donor's calendar-year aggregate per committee is exactly the year-end
// period aggregate — the statutory test can be applied per (committee,
// donor, year) without period tables.
export const NORTH_DAKOTA_OCCUPATION_THRESHOLD_CENTS = 500_000;

const DIRECT_DONATION_CATEGORIES = new Set(["Monetary", "In-Kind"]);

export type NorthDakotaOccupationClassSummary = {
  committeeCount: number;
  individualRowCount: number;
  individualCents: number;
  donorsAtThreshold: number;
  donorsAtThresholdWithOccupation: number;
  centsAtThreshold: number;
  centsAtThresholdWithOccupation: number;
  /** Committees whose occupation-bearing individual dollars are >=20% of
   * itemized individual dollars AND have >=3 occupation-bearing donors
   * (plan hard fact 3 display gate). */
  committeesPassingDisplayGate: number;
};

export type NorthDakotaOccupationSummary = {
  byOfficeClass: Record<NorthDakotaOfficeClass, NorthDakotaOccupationClassSummary>;
  distinctOccupations: Array<{ value: string; count: number }>;
};

function emptyOccupationClass(): NorthDakotaOccupationClassSummary {
  return {
    committeeCount: 0,
    individualRowCount: 0,
    individualCents: 0,
    donorsAtThreshold: 0,
    donorsAtThresholdWithOccupation: 0,
    centsAtThreshold: 0,
    centsAtThresholdWithOccupation: 0,
    committeesPassingDisplayGate: 0,
  };
}

export function summarizeNorthDakotaOccupations(
  apiRows: readonly NorthDakotaTransactionRow[],
  committeesById: ReadonlyMap<string, NorthDakotaCommitteeRow>
): NorthDakotaOccupationSummary {
  const donations = apiRows.filter(
    (row) =>
      row.orgType === NORTH_DAKOTA_CANDIDATE_COMMITTEE_ORG_TYPE &&
      row.entityTypeDesc === "Individual" &&
      row.transactionCategoryDesc !== null &&
      DIRECT_DONATION_CATEGORIES.has(row.transactionCategoryDesc)
  );

  type Donor = { cents: number; occupationCents: number; hasOccupation: boolean };
  const perCommittee = new Map<string, Map<string, Donor>>();
  for (const row of donations) {
    const donors = perCommittee.get(row.entityID) ?? new Map<string, Donor>();
    const donorKey = counterpartyKey(row);
    const donor = donors.get(donorKey) ?? { cents: 0, occupationCents: 0, hasOccupation: false };
    const cents = apiAmountToCents(row.transactionAmount);
    donor.cents += cents;
    if (row.employerOccupation !== null) {
      donor.hasOccupation = true;
      donor.occupationCents += cents;
    }
    donors.set(donorKey, donor);
    perCommittee.set(row.entityID, donors);
  }

  const byOfficeClass: Record<NorthDakotaOfficeClass, NorthDakotaOccupationClassSummary> = {
    statewide: emptyOccupationClass(),
    legislative: emptyOccupationClass(),
    judicial: emptyOccupationClass(),
    unknown: emptyOccupationClass(),
  };
  for (const [entityId, donors] of perCommittee) {
    const summary = byOfficeClass[classifyNorthDakotaOffice(committeesById.get(entityId)?.office ?? null)];
    summary.committeeCount += 1;
    let committeeCents = 0;
    let committeeOccupationCents = 0;
    let occupationDonors = 0;
    for (const donor of donors.values()) {
      committeeCents += donor.cents;
      committeeOccupationCents += donor.occupationCents;
      if (donor.hasOccupation) occupationDonors += 1;
      if (donor.cents >= NORTH_DAKOTA_OCCUPATION_THRESHOLD_CENTS) {
        summary.donorsAtThreshold += 1;
        summary.centsAtThreshold += donor.cents;
        if (donor.hasOccupation) {
          summary.donorsAtThresholdWithOccupation += 1;
          summary.centsAtThresholdWithOccupation += donor.occupationCents;
        }
      }
    }
    summary.individualCents += committeeCents;
    if (committeeCents > 0 && committeeOccupationCents * 5 >= committeeCents && occupationDonors >= 3) {
      summary.committeesPassingDisplayGate += 1;
    }
  }
  for (const row of donations) {
    byOfficeClass[classifyNorthDakotaOffice(committeesById.get(row.entityID)?.office ?? null)].individualRowCount += 1;
  }

  const occupationCounts = countBy(
    donations.filter((row) => row.employerOccupation !== null),
    (row) => row.employerOccupation as string
  );
  return {
    byOfficeClass,
    distinctOccupations: Object.entries(occupationCounts)
      .map(([value, count]) => ({ value, count }))
      .sort((a, b) => b.count - a.count || a.value.localeCompare(b.value)),
  };
}

// --- registry / resolver gold set -------------------------------------------

export type NorthDakotaRegistrySummary = {
  committeeCount: number;
  byOrgType: Record<string, number>;
  electionValues: Record<string, number>;
  /** Candidate committees registered for the current statewide election. */
  currentCycleCandidates: {
    election: string;
    count: number;
    active: number;
    byOfficeClass: Record<NorthDakotaOfficeClass, number>;
    /** office + election present, and a district for legislative seats. */
    completeIdentity: number;
    distinctOffices: string[];
  };
};

export function summarizeNorthDakotaRegistry(
  committees: readonly NorthDakotaCommitteeRow[],
  currentElection: string
): NorthDakotaRegistrySummary {
  const current = committees.filter(
    (committee) => committee.orgType === NORTH_DAKOTA_CANDIDATE_COMMITTEE_ORG_TYPE && committee.election === currentElection
  );
  const byOfficeClass: Record<NorthDakotaOfficeClass, number> = { statewide: 0, legislative: 0, judicial: 0, unknown: 0 };
  let completeIdentity = 0;
  for (const committee of current) {
    const officeClass = classifyNorthDakotaOffice(committee.office);
    byOfficeClass[officeClass] += 1;
    if (committee.office !== null && (officeClass !== "legislative" || committee.district !== null)) completeIdentity += 1;
  }
  return {
    committeeCount: committees.length,
    byOrgType: countBy(committees, (committee) => committee.orgType),
    electionValues: countBy(committees, (committee) => committee.election ?? "<null>"),
    currentCycleCandidates: {
      election: currentElection,
      count: current.length,
      active: current.filter((committee) => committee.orgStatus === "Active").length,
      byOfficeClass,
      completeIdentity,
      distinctOffices: [...new Set(current.map((committee) => committee.office ?? "<null>"))].sort(),
    },
  };
}

export type NorthDakotaRegistryJoinCheck = {
  csvRegistrantCount: number;
  matchedCount: number;
  unmatchedRegistrantIds: string[];
};

export function checkNorthDakotaRegistryJoin(input: {
  csvRegistrantIds: ReadonlySet<string>;
  committeesById: ReadonlyMap<string, NorthDakotaCommitteeRow>;
}): NorthDakotaRegistryJoinCheck {
  const unmatched = [...input.csvRegistrantIds].filter((id) => !input.committeesById.has(id)).sort();
  return {
    csvRegistrantCount: input.csvRegistrantIds.size,
    matchedCount: input.csvRegistrantIds.size - unmatched.length,
    unmatchedRegistrantIds: unmatched.slice(0, 25),
  };
}

// --- election-cycle window --------------------------------------------------

// Gate 4: which election do 2025 reporting rows belong to? Two independent
// pins: the Reporting Schedules file maps each reporting cycle to an election
// name, and the registry's election field on registrants with prior-year
// activity.
export type NorthDakotaReportingCycleSummary = Array<{
  electionName: string;
  reportingCycles: string[];
  periodCount: number;
  earliestBeginDate: string;
  latestEndDate: string;
}>;

export function summarizeNorthDakotaReportingCycles(
  rows: readonly NorthDakotaReportingScheduleCsvRow[]
): NorthDakotaReportingCycleSummary {
  const elections = new Map<string, { cycles: Set<string>; periods: number; minBegin: string; maxEnd: string }>();
  for (const row of rows) {
    const entry = elections.get(row.electionName);
    if (!entry) {
      elections.set(row.electionName, { cycles: new Set([row.reportingCycle]), periods: 1, minBegin: row.beginDate, maxEnd: row.endDate });
    } else {
      entry.cycles.add(row.reportingCycle);
      entry.periods += 1;
      if (row.beginDate < entry.minBegin) entry.minBegin = row.beginDate;
      if (row.endDate > entry.maxEnd) entry.maxEnd = row.endDate;
    }
  }
  return [...elections.entries()]
    .map(([electionName, entry]) => ({
      electionName,
      reportingCycles: [...entry.cycles].sort(),
      periodCount: entry.periods,
      earliestBeginDate: entry.minBegin,
      latestEndDate: entry.maxEnd,
    }))
    .sort((a, b) => a.electionName.localeCompare(b.electionName));
}

export type NorthDakotaCycleWindowCheck = {
  priorYear: number;
  candidateRegistrantsWithPriorYearActivity: number;
  byElection: Record<string, number>;
};

export function checkNorthDakotaCycleWindow(input: {
  priorYear: number;
  priorYearRows: readonly NorthDakotaContributionCsvRow[];
  committeesById: ReadonlyMap<string, NorthDakotaCommitteeRow>;
}): NorthDakotaCycleWindowCheck {
  const registrants = [...new Set(input.priorYearRows.map((row) => row.registrantId))]
    .map((id) => input.committeesById.get(id))
    .filter(
      (committee): committee is NorthDakotaCommitteeRow =>
        committee !== undefined && committee.orgType === NORTH_DAKOTA_CANDIDATE_COMMITTEE_ORG_TYPE
    );
  return {
    priorYear: input.priorYear,
    candidateRegistrantsWithPriorYearActivity: registrants.length,
    byElection: countBy(registrants, (committee) => committee.election ?? "<null>"),
  };
}

// --- filed reports (amendment lineage) -------------------------------------

export type NorthDakotaFiledReportSummary = {
  rowCount: number;
  byReportTypeAndVersion: Record<string, number>;
  /** RegistrantIDs with at least one Amended filing, candidate committees
   * first, for the amendment reconciliation fixture. */
  amendedRegistrantIds: string[];
  amendedFilingsByRegistrant: Record<string, number>;
};

export function summarizeNorthDakotaFiledReports(
  rows: readonly NorthDakotaFiledReportCsvRow[],
  committeesById: ReadonlyMap<string, NorthDakotaCommitteeRow>
): NorthDakotaFiledReportSummary {
  const amendedRows = rows.filter((row) => row.reportVersion === "Amended");
  const amended = [...new Set(amendedRows.map((row) => row.registrantId))];
  const isCandidate = (id: string) => committeesById.get(id)?.orgType === NORTH_DAKOTA_CANDIDATE_COMMITTEE_ORG_TYPE;
  return {
    rowCount: rows.length,
    byReportTypeAndVersion: countBy(rows, (row) => `${row.reportType} | ${row.reportVersion}`),
    amendedRegistrantIds: amended.sort((a, b) => Number(isCandidate(b)) - Number(isCandidate(a)) || a.localeCompare(b)),
    amendedFilingsByRegistrant: countBy(amendedRows, (row) => row.registrantId),
  };
}

// --- evidence gates ---------------------------------------------------------

// Transport and parse failures throw as they happen. The evidence gates run
// on the collected report instead, so a failing run still prints everything
// it gathered and then exits non-zero.
export function evaluateNorthDakotaPhaseZeroGates(input: {
  contributionChart: Pick<NorthDakotaChartReconciliation, "totalMatch" | "series">;
  expenditureChart: Pick<NorthDakotaChartReconciliation, "totalMatch" | "series">;
  unknownContributionCategories: readonly string[];
  unknownExpenditureTypes: readonly string[];
  registryJoin: NorthDakotaRegistryJoinCheck;
  reconciliations: ReadonlyArray<
    Pick<NorthDakotaCommitteeReconciliation, "entityId" | "csvRowCount" | "apiRowCount" | "totalsMatch" | "multisetMatch">
  >;
  independentExpenditures: Pick<
    NorthDakotaIndependentExpenditureSummary,
    | "rowCount"
    | "distinctTransactionIdCount"
    | "offTypeRowCount"
    | "missingStanceRowCount"
    | "missingCandidateRowCount"
    | "missingElectionYearRowCount"
    | "payeeYtd"
    | "totalCents"
  >;
  independentExpenditureChartTotalCents: number;
  /** The election every prior-year candidate registrant and the prior-year
   * reporting cycle must map to (plan gate 4). */
  currentElection: string;
  cycleWindow: NorthDakotaCycleWindowCheck;
  reportingCycles: NorthDakotaReportingCycleSummary;
  occupations: Pick<NorthDakotaOccupationSummary, "byOfficeClass">;
  registry: Pick<NorthDakotaRegistrySummary, "currentCycleCandidates">;
}): string[] {
  const failures: string[] = [];

  for (const [label, chart] of [
    ["contributions", input.contributionChart],
    ["expenditures", input.expenditureChart],
  ] as const) {
    if (!chart.totalMatch) failures.push(`${label} chart: CSV total differs from the portal chart total`);
    for (const series of chart.series) {
      if (series.compared && series.mismatches.length > 0) {
        failures.push(
          `${label} chart "${series.name}": ${series.mismatches.length} slice(s) differ (${series.mismatches
            .map((entry) => entry.description)
            .join(", ")})`
        );
      }
    }
  }
  if (input.unknownContributionCategories.length > 0) {
    failures.push(`contribution categories not in the pinned vocabulary: ${input.unknownContributionCategories.join(", ")}`);
  }
  if (input.unknownExpenditureTypes.length > 0) {
    failures.push(`expenditure types not in the pinned vocabulary: ${input.unknownExpenditureTypes.join(", ")}`);
  }

  const unmatched = input.registryJoin.csvRegistrantCount - input.registryJoin.matchedCount;
  if (unmatched > 0) failures.push(`registry join: ${unmatched} CSV registrants missing from the committee registry`);

  if (input.reconciliations.length === 0) failures.push("reconciliation: no committees reconciled");
  for (const entry of input.reconciliations) {
    if (entry.csvRowCount === 0 || entry.apiRowCount === 0) {
      failures.push(`reconciliation ${entry.entityId}: empty sample (csv ${entry.csvRowCount}, api ${entry.apiRowCount})`);
    } else if (!entry.totalsMatch || !entry.multisetMatch) {
      failures.push(`reconciliation ${entry.entityId}: CSV and API rows differ`);
    }
  }

  const ie = input.independentExpenditures;
  if (ie.rowCount === 0) failures.push("independent expenditures: no rows returned");
  if (ie.distinctTransactionIdCount !== ie.rowCount) failures.push("independent expenditures: repeated transactionIDs");
  if (ie.offTypeRowCount > 0) failures.push(`independent expenditures: ${ie.offTypeRowCount} rows are not typed Independent Expenditures`);
  if (ie.missingStanceRowCount > 0) failures.push(`independent expenditures: ${ie.missingStanceRowCount} rows without a Support/Oppose stance`);
  if (ie.missingCandidateRowCount > 0) failures.push(`independent expenditures: ${ie.missingCandidateRowCount} rows without a candidate target`);
  if (ie.missingElectionYearRowCount > 0) failures.push(`independent expenditures: ${ie.missingElectionYearRowCount} rows without an election year`);
  if (ie.payeeYtd.mismatches.length > 0 || ie.payeeYtd.missingControlGroupCount > 0) {
    failures.push(
      `independent expenditures: ${ie.payeeYtd.mismatches.length} payee group(s) do not sum to their YTD control (${ie.payeeYtd.missingControlGroupCount} without a control)`
    );
  }
  if (ie.totalCents !== input.independentExpenditureChartTotalCents) {
    failures.push("independent expenditures: unique-row total differs from the portal chart total");
  }

  if (input.cycleWindow.candidateRegistrantsWithPriorYearActivity < 10) {
    failures.push(
      `cycle window: only ${input.cycleWindow.candidateRegistrantsWithPriorYearActivity} candidate registrants with ${input.cycleWindow.priorYear} activity`
    );
  }
  const otherElections = Object.keys(input.cycleWindow.byElection).filter((election) => election !== input.currentElection);
  if (otherElections.length > 0) {
    failures.push(
      `cycle window: candidate registrants with ${input.cycleWindow.priorYear} activity map to other elections (${otherElections.join(", ")})`
    );
  }
  const priorCycle = `${input.cycleWindow.priorYear} REPORTING CYCLE`;
  const currentSchedule = input.reportingCycles.find((entry) => entry.electionName === input.currentElection);
  if (!currentSchedule?.reportingCycles.some((cycle) => cycle.toUpperCase() === priorCycle)) {
    failures.push(`cycle window: reporting schedules do not map "${priorCycle}" to "${input.currentElection}"`);
  }

  const measured = Object.values(input.occupations.byOfficeClass).reduce((sum, entry) => sum + entry.donorsAtThreshold, 0);
  if (measured === 0) failures.push("occupation: no individual donors at the $5,000 threshold were measured");

  const current = input.registry.currentCycleCandidates;
  if (current.count === 0) {
    failures.push("resolver gold set: no current-cycle candidate committees in the registry");
  } else if (current.completeIdentity !== current.count) {
    failures.push(
      `resolver gold set: ${current.count - current.completeIdentity} of ${current.count} current-cycle candidate committees lack a complete office identity`
    );
  }
  return failures;
}
