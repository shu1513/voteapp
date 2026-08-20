import type {
  NewHampshireIndependentExpenditureRow,
  NewHampshireReceiptRow,
} from "./newHampshireCfsClient.js";
import {
  parseNewHampshireCurrencyCents,
  type NewHampshireReceiptCsvRow,
} from "./newHampshireCfsCsv.js";

export type NewHampshireMoneySummary = {
  rowCount: number;
  amountCents: number;
};

export type NewHampshireAmendmentReconciliation = {
  bulk: NewHampshireMoneySummary;
  apiAllVersions: NewHampshireMoneySummary;
  apiCurrentVersions: NewHampshireMoneySummary;
  amendedReportCount: number;
  deltaCents: number;
  status: "match" | "mismatch";
  strategy: "bulk_fixture_matches_current_versions" | "search_api_current_report_versions_required";
};

function sumApiRows(rows: readonly NewHampshireReceiptRow[]): NewHampshireMoneySummary {
  return {
    rowCount: rows.length,
    amountCents: rows.reduce((sum, row) => sum + Math.round(row.transactionAmount * 100), 0),
  };
}

export function selectCurrentNewHampshireReceiptReportVersions(
  rows: readonly NewHampshireReceiptRow[]
): NewHampshireReceiptRow[] {
  const maxVersionByReport = new Map<number, number>();
  for (const row of rows) {
    const current = maxVersionByReport.get(row.filerReportId) ?? 0;
    maxVersionByReport.set(row.filerReportId, Math.max(current, row.filerReportVersionId));
  }
  return rows.filter((row) => row.filerReportVersionId === maxVersionByReport.get(row.filerReportId));
}

export function summarizeNewHampshireBulkReceipts(
  rows: readonly NewHampshireReceiptCsvRow[],
  filingEntityId: number
): NewHampshireMoneySummary {
  const id = String(filingEntityId);
  const matched = rows.filter((row) => row["Filing Entity ID"].trim() === id);
  return {
    rowCount: matched.length,
    amountCents: matched.reduce(
      (sum, row) => sum + parseNewHampshireCurrencyCents(row["Amount of receipt"]),
      0
    ),
  };
}

export function reconcileNewHampshireAmendmentFixture(input: {
  bulkRows: readonly NewHampshireReceiptCsvRow[];
  apiRows: readonly NewHampshireReceiptRow[];
  filingEntityId: number;
}): NewHampshireAmendmentReconciliation {
  const wrongFiler = input.apiRows.find((row) => row.filerEntityId !== input.filingEntityId);
  if (wrongFiler) {
    throw new Error(
      `New Hampshire receipt search was not exact: expected filer ${input.filingEntityId}, received ${wrongFiler.filerEntityId}`
    );
  }
  const currentRows = selectCurrentNewHampshireReceiptReportVersions(input.apiRows);
  const reportVersions = new Map<number, Set<number>>();
  for (const row of input.apiRows) {
    const versions = reportVersions.get(row.filerReportId) ?? new Set<number>();
    versions.add(row.filerReportVersionId);
    reportVersions.set(row.filerReportId, versions);
  }
  const amendedReportCount = [...reportVersions.values()].filter((versions) => Math.max(...versions) > 1).length;
  const bulk = summarizeNewHampshireBulkReceipts(input.bulkRows, input.filingEntityId);
  const apiAllVersions = sumApiRows(input.apiRows);
  const apiCurrentVersions = sumApiRows(currentRows);
  const deltaCents = bulk.amountCents - apiCurrentVersions.amountCents;
  const status = bulk.rowCount === apiCurrentVersions.rowCount && deltaCents === 0 ? "match" : "mismatch";
  return {
    bulk,
    apiAllVersions,
    apiCurrentVersions,
    amendedReportCount,
    deltaCents,
    status,
    strategy:
      status === "match"
        ? "bulk_fixture_matches_current_versions"
        : "search_api_current_report_versions_required",
  };
}

export type NewHampshireIndependentExpenditureProbeSummary = {
  sourceRowCount: number;
  rowCount: number;
  supersededRowCount: number;
  support: NewHampshireMoneySummary;
  oppose: NewHampshireMoneySummary;
  blankStance: NewHampshireMoneySummary;
  blankTargetCount: number;
};

export function selectCurrentNewHampshireIndependentExpenditureReportVersions(
  rows: readonly NewHampshireIndependentExpenditureRow[]
): NewHampshireIndependentExpenditureRow[] {
  const maxVersionByReport = new Map<number, number>();
  for (const row of rows) {
    if (row.filerReportId === null) continue;
    if (row.filerReportVersionId === null) {
      throw new Error(`New Hampshire IE report ${row.filerReportId} has no version`);
    }
    const current = maxVersionByReport.get(row.filerReportId) ?? 0;
    maxVersionByReport.set(row.filerReportId, Math.max(current, row.filerReportVersionId));
  }
  return rows.filter(
    (row) =>
      row.filerReportId === null ||
      row.filerReportVersionId === maxVersionByReport.get(row.filerReportId)
  );
}

export function summarizeNewHampshireIndependentExpenditures(
  rows: readonly NewHampshireIndependentExpenditureRow[],
  expectedCycleName: string
): NewHampshireIndependentExpenditureProbeSummary {
  const identities = new Set<string>();
  const support: NewHampshireIndependentExpenditureRow[] = [];
  const oppose: NewHampshireIndependentExpenditureRow[] = [];
  const blankStance: NewHampshireIndependentExpenditureRow[] = [];
  let blankTargetCount = 0;

  for (const row of rows) {
    if (row.transactionTypeCode !== "TIE" || row.transactionSubTypeCode !== "TIE") {
      throw new Error(`New Hampshire IE search returned non-IE transaction ${row.transactionId}`);
    }
    if (row.electionCycle !== expectedCycleName) {
      throw new Error(
        `New Hampshire IE search returned cycle ${JSON.stringify(row.electionCycle)}; expected ${expectedCycleName}`
      );
    }
    const identity = `${row.transactionId}:${row.transactionVersionId}:${row.guid}`;
    if (identities.has(identity)) {
      throw new Error(`New Hampshire IE search returned duplicate identity ${identity}`);
    }
    identities.add(identity);
  }

  const currentRows = selectCurrentNewHampshireIndependentExpenditureReportVersions(rows);
  for (const row of currentRows) {
    if (row.candidateMeasure === null) blankTargetCount += 1;
    if (row.stance === "Support") support.push(row);
    else if (row.stance === "Oppose") oppose.push(row);
    else if (row.stance === null) blankStance.push(row);
    else throw new Error(`New Hampshire IE search returned unknown stance ${JSON.stringify(row.stance)}`);
  }

  const sum = (selected: readonly NewHampshireIndependentExpenditureRow[]): NewHampshireMoneySummary => ({
    rowCount: selected.length,
    amountCents: selected.reduce((total, row) => total + Math.round(row.transactionAmount * 100), 0),
  });
  return {
    sourceRowCount: rows.length,
    rowCount: currentRows.length,
    supersededRowCount: rows.length - currentRows.length,
    support: sum(support),
    oppose: sum(oppose),
    blankStance: sum(blankStance),
    blankTargetCount,
  };
}
