// Phase 0 only: pure summarization/reconciliation helpers behind the probe
// script. No database, cache, scheduler, or published snapshot.

import type {
  WestVirginiaCommitteeRow,
  WestVirginiaOrgDocumentRow,
  WestVirginiaTransactionRow,
} from "./westVirginiaCfrsClient.js";
import type {
  WestVirginiaContributionCsvRow,
  WestVirginiaReportingScheduleCsvRow,
} from "./westVirginiaCfrsCsv.js";

function centsToDollars(cents: number): string {
  return (cents / 100).toFixed(2);
}

function countBy<T>(rows: readonly T[], key: (row: T) => string): Record<string, number> {
  const counts: Record<string, number> = {};
  for (const row of rows) {
    const k = key(row);
    counts[k] = (counts[k] ?? 0) + 1;
  }
  return counts;
}

// --- contributions CSV summary ---------------------------------------------

export type WestVirginiaContributionCsvSummary = {
  rowCount: number;
  totalCents: number;
  totalDollars: string;
  registrantCount: number;
  byCategory: Record<string, number>;
  byContributorType: Record<string, number>;
  duplicateRowCount: number;
  dateRange: { min: string; max: string } | null;
};

export function summarizeWestVirginiaContributionCsv(
  rows: readonly WestVirginiaContributionCsvRow[]
): WestVirginiaContributionCsvSummary {
  const totalCents = rows.reduce((sum, row) => sum + row.amountCents, 0);
  const seen = new Set<string>();
  let duplicateRowCount = 0;
  for (const row of rows) {
    const key = [
      row.registrantId,
      row.transactionType,
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
  }
  let min: string | null = null;
  let max: string | null = null;
  for (const row of rows) {
    if (min === null || row.transactionDate < min) min = row.transactionDate;
    if (max === null || row.transactionDate > max) max = row.transactionDate;
  }
  return {
    rowCount: rows.length,
    totalCents,
    totalDollars: centsToDollars(totalCents),
    registrantCount: new Set(rows.map((row) => row.registrantId)).size,
    byCategory: countBy(rows, (row) => row.transactionCategory),
    byContributorType: countBy(rows, (row) => row.contributorType),
    duplicateRowCount,
    dateRange: min && max ? { min, max } : null,
  };
}

// --- CSV vs API committee reconciliation ------------------------------------

// The bulk CSV has no report identity, so the comparison is committee-level:
// a multiset of (transactionDate, amountCents, category). Category is the
// only other field both sides carry under one vocabulary; contributor names
// are not keyed because recovered CSV rows truncate them. Report grouping
// comes from the API side and is listed for later cover checks.
export type WestVirginiaCommitteeReconciliation = {
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
  apiReports: Array<{
    reportFileName: string;
    reportVersionID: string | null;
    rowCount: number;
    totalCents: number;
    totalDollars: string;
    s3ReportFilePath: string | null;
  }>;
};

// API amounts arrive as JSON numbers; observed values are cent-precise.
// Convert defensively and fail on sub-cent residue.
export function apiAmountToCents(amount: number): number {
  const cents = Math.round(amount * 100);
  if (!Number.isSafeInteger(cents) || Math.abs(cents - amount * 100) > 1e-6 * Math.max(1, Math.abs(cents))) {
    throw new Error(`West Virginia API amount is not cent-precise: ${amount}`);
  }
  return cents;
}

export function reconcileWestVirginiaCommittee(input: {
  entityId: string;
  csvRows: readonly WestVirginiaContributionCsvRow[];
  apiRows: readonly WestVirginiaTransactionRow[];
  /** API category descriptions counted as contribution-file material. */
  contributionCategories: ReadonlySet<string>;
}): WestVirginiaCommitteeReconciliation {
  const csvRows = input.csvRows.filter((row) => row.registrantId === input.entityId);
  const apiRows = input.apiRows.filter(
    (row) =>
      row.entityID === input.entityId &&
      row.transactionCategoryDesc !== null &&
      input.contributionCategories.has(row.transactionCategoryDesc)
  );

  const csvTotalCents = csvRows.reduce((sum, row) => sum + row.amountCents, 0);
  const apiTotalCents = apiRows.reduce((sum, row) => sum + apiAmountToCents(row.transactionAmount), 0);

  const key = (date: string, cents: number, category: string) =>
    `${date.slice(0, 10)}\u0000${cents}\u0000${category}`;
  const csvCounts = new Map<string, number>();
  for (const row of csvRows) {
    const k = key(row.transactionDate, row.amountCents, row.transactionCategory);
    csvCounts.set(k, (csvCounts.get(k) ?? 0) + 1);
  }
  let onlyInApi = 0;
  const remaining = new Map(csvCounts);
  for (const row of apiRows) {
    const k = key(row.transactionDate, apiAmountToCents(row.transactionAmount), row.transactionCategoryDesc ?? "");
    const count = remaining.get(k) ?? 0;
    if (count > 0) remaining.set(k, count - 1);
    else onlyInApi += 1;
  }
  const onlyInCsv = [...remaining.values()].reduce((sum, count) => sum + count, 0);

  const reportTotals = new Map<
    string,
    { reportFileName: string; reportVersionID: string | null; rowCount: number; totalCents: number; s3ReportFilePath: string | null }
  >();
  for (const row of apiRows) {
    const name = row.reportFileName ?? "<no report>";
    const groupKey = `${name}\u0000${row.reportVersionID ?? ""}`;
    const entry = reportTotals.get(groupKey) ?? {
      reportFileName: name,
      reportVersionID: row.reportVersionID,
      rowCount: 0,
      totalCents: 0,
      s3ReportFilePath: row.s3ReportFilePath,
    };
    entry.rowCount += 1;
    entry.totalCents += apiAmountToCents(row.transactionAmount);
    reportTotals.set(groupKey, entry);
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
    apiReports: [...reportTotals.values()]
      .sort((a, b) => a.reportFileName.localeCompare(b.reportFileName))
      .map((entry) => ({ ...entry, totalDollars: centsToDollars(entry.totalCents) })),
  };
}

// --- occupation sweep -------------------------------------------------------

export type WestVirginiaOccupationSummary = {
  apiRowCount: number;
  /** Individual rows in the donation categories; loans, returns and other
   * income from individuals are not donor rows and are excluded. */
  individualRowCount: number;
  over250SingleTransaction: { rowCount: number; occupationFilled: number; employerFilled: number };
  over250Ytd: { rowCount: number; occupationFilled: number };
  distinctOccupations: Array<{ value: string; count: number }>;
};

export function summarizeWestVirginiaOccupations(
  apiRows: readonly WestVirginiaTransactionRow[],
  /** API category descriptions that count as direct donations. */
  donationCategories: ReadonlySet<string>
): WestVirginiaOccupationSummary {
  const individuals = apiRows.filter(
    (row) =>
      row.entityTypeDesc === "Individual" &&
      row.transactionCategoryDesc !== null &&
      donationCategories.has(row.transactionCategoryDesc)
  );
  const over250 = individuals.filter((row) => apiAmountToCents(row.transactionAmount) > 25_000);
  const over250Ytd = individuals.filter((row) => {
    const ytd = row.transactionTotalYTD === null ? NaN : Number(row.transactionTotalYTD);
    return Number.isFinite(ytd) && ytd > 250;
  });
  const occupationCounts = countBy(
    individuals.filter((row) => row.employerOccupation !== null),
    (row) => row.employerOccupation as string
  );
  return {
    apiRowCount: apiRows.length,
    individualRowCount: individuals.length,
    over250SingleTransaction: {
      rowCount: over250.length,
      occupationFilled: over250.filter((row) => row.employerOccupation !== null).length,
      employerFilled: over250.filter((row) => row.employerName !== null).length,
    },
    over250Ytd: {
      rowCount: over250Ytd.length,
      occupationFilled: over250Ytd.filter((row) => row.employerOccupation !== null).length,
    },
    distinctOccupations: Object.entries(occupationCounts)
      .map(([value, count]) => ({ value, count }))
      .sort((a, b) => b.count - a.count || a.value.localeCompare(b.value)),
  };
}

// --- registry / CSV join ----------------------------------------------------

export type WestVirginiaRegistryJoinCheck = {
  csvRegistrantCount: number;
  matchedCount: number;
  unmatchedRegistrantIds: string[];
};

export function checkWestVirginiaRegistryJoin(input: {
  csvRegistrantIds: ReadonlySet<string>;
  committees: readonly WestVirginiaCommitteeRow[];
}): WestVirginiaRegistryJoinCheck {
  const registryIds = new Set(input.committees.map((committee) => committee.entityId));
  const unmatched = [...input.csvRegistrantIds].filter((id) => !registryIds.has(id)).sort();
  return {
    csvRegistrantCount: input.csvRegistrantIds.size,
    matchedCount: input.csvRegistrantIds.size - unmatched.length,
    unmatchedRegistrantIds: unmatched.slice(0, 25),
  };
}

// --- reporting cycles -------------------------------------------------------

export type WestVirginiaReportingCycleSummary = {
  reportingCycle: string;
  periodCount: number;
  earliestBeginDate: string;
  latestEndDate: string;
};

export function summarizeWestVirginiaReportingCycles(
  rows: readonly WestVirginiaReportingScheduleCsvRow[]
): WestVirginiaReportingCycleSummary[] {
  const cycles = new Map<string, { periods: number; minBegin: string; maxEnd: string }>();
  for (const row of rows) {
    const entry = cycles.get(row.reportingCycle);
    if (!entry) {
      cycles.set(row.reportingCycle, { periods: 1, minBegin: row.beginDate, maxEnd: row.endDate });
    } else {
      entry.periods += 1;
      if (row.beginDate < entry.minBegin) entry.minBegin = row.beginDate;
      if (row.endDate > entry.maxEnd) entry.maxEnd = row.endDate;
    }
  }
  return [...cycles.entries()]
    .map(([reportingCycle, entry]) => ({
      reportingCycle,
      periodCount: entry.periods,
      earliestBeginDate: entry.minBegin,
      latestEndDate: entry.maxEnd,
    }))
    .sort((a, b) => a.reportingCycle.localeCompare(b.reportingCycle));
}

// --- outside document inventory --------------------------------------------

export type WestVirginiaOutsideInventorySummary = {
  committeeCount: number;
  documentCount: number;
  independentExpenditureDocumentCount: number;
  byCommittee: Array<{
    entityId: string;
    orgID: number;
    orgName: string;
    registrationYear: string | null;
    documentCount: number;
    independentExpenditureDocumentCount: number;
  }>;
};

export function isWestVirginiaIndependentExpenditureDocument(document: WestVirginiaOrgDocumentRow): boolean {
  const haystack = `${document.documentName} ${document.documentType}`;
  return haystack.includes("Independent Expenditure") || haystack.includes("Electioneering");
}

// Document receivedDate arrives as "MM/DD/YYYY HH:MM:SS" (verified live
// 2026-09-01); ISO dates are accepted too. Unparseable dates return null so
// the caller can decide — the probe keeps those documents rather than hide them.
export function westVirginiaDocumentReceivedYear(document: Pick<WestVirginiaOrgDocumentRow, "receivedDate">): number | null {
  const us = /^(\d{2})\/(\d{2})\/(\d{4})/.exec(document.receivedDate);
  if (us) return Number(us[3]);
  const iso = /^(\d{4})-(\d{2})-(\d{2})/.exec(document.receivedDate);
  if (iso) return Number(iso[1]);
  return null;
}

export function summarizeWestVirginiaOutsideInventory(
  entries: ReadonlyArray<{ committee: WestVirginiaCommitteeRow; documents: readonly WestVirginiaOrgDocumentRow[] }>
): WestVirginiaOutsideInventorySummary {
  const byCommittee = entries.map(({ committee, documents }) => ({
    entityId: committee.entityId,
    orgID: committee.orgID,
    orgName: committee.orgName ?? "<unnamed>",
    registrationYear: committee.registrationYear,
    documentCount: documents.length,
    independentExpenditureDocumentCount: documents.filter(isWestVirginiaIndependentExpenditureDocument).length,
  }));
  return {
    committeeCount: entries.length,
    documentCount: byCommittee.reduce((sum, entry) => sum + entry.documentCount, 0),
    independentExpenditureDocumentCount: byCommittee.reduce(
      (sum, entry) => sum + entry.independentExpenditureDocumentCount,
      0
    ),
    byCommittee: byCommittee.sort((a, b) => b.independentExpenditureDocumentCount - a.independentExpenditureDocumentCount),
  };
}

// --- evidence gates ---------------------------------------------------------

// Transport and parse failures throw as they happen. The evidence gates run
// on the collected report instead, so a failing run still prints everything
// it gathered and then exits non-zero.
export function evaluateWestVirginiaPhaseZeroGates(input: {
  reconciliations: ReadonlyArray<
    Pick<WestVirginiaCommitteeReconciliation, "entityId" | "csvRowCount" | "apiRowCount" | "totalsMatch" | "multisetMatch">
  >;
  registryJoin: WestVirginiaRegistryJoinCheck;
  outsideInventory: Pick<WestVirginiaOutsideInventorySummary, "independentExpenditureDocumentCount">;
  /** null when no filed-report PDF was downloaded. */
  sampleFiledReportHasFontMarker: boolean | null;
}): string[] {
  const failures: string[] = [];
  if (input.reconciliations.length === 0) {
    failures.push("reconciliation: no committees reconciled");
  }
  for (const entry of input.reconciliations) {
    if (entry.csvRowCount === 0 || entry.apiRowCount === 0) {
      failures.push(
        `reconciliation ${entry.entityId}: empty sample (csv ${entry.csvRowCount}, api ${entry.apiRowCount})`
      );
    } else if (!entry.totalsMatch || !entry.multisetMatch) {
      failures.push(`reconciliation ${entry.entityId}: CSV and API rows differ`);
    }
  }
  const unmatched = input.registryJoin.csvRegistrantCount - input.registryJoin.matchedCount;
  if (unmatched > 0) {
    failures.push(`registry join: ${unmatched} CSV registrants missing from the committee registry`);
  }
  if (input.outsideInventory.independentExpenditureDocumentCount === 0) {
    failures.push("outside inventory: no independent-expenditure documents found");
  }
  if (input.sampleFiledReportHasFontMarker !== true) {
    failures.push("cover pdf: no filed-report PDF with a text layer");
  }
  return failures;
}

// --- PDF text-layer sniff ---------------------------------------------------

// Scanned F-7b filings carry no /Font objects; portal-generated report PDFs
// should. A crude marker check is all Phase 0 needs.
export function pdfHasFontMarker(bytes: Uint8Array): boolean {
  const marker = new TextEncoder().encode("/Font");
  outer: for (let i = 0; i <= bytes.length - marker.length; i += 1) {
    for (let j = 0; j < marker.length; j += 1) {
      if (bytes[i + j] !== marker[j]) continue outer;
    }
    return true;
  }
  return false;
}
