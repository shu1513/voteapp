import {
  normalizeMissouriMecReportLineage,
  type MissouriMecReportInventoryRow,
} from "./missouriMecParsers.js";

export type MissouriReportSelectionDiagnostic = {
  lineageKey: string;
  reason: "missing_inventory" | "unexpected_amendment" | "ambiguous_amendment";
  excludedRowCount: number;
  excludedAmountCents: number;
};

export type MissouriCanonicalReportSelection<T> = {
  rows: T[];
  diagnostics: MissouriReportSelectionDiagnostic[];
};

function normalizeValue(value: string | null | undefined): string {
  return (value ?? "").trim().replace(/\s+/g, " ").toLocaleUpperCase("en-US");
}

/**
 * Selects transaction rows that can be attributed to a canonical MEC report
 * lineage without guessing. MEC exports have no transaction/version ids.
 * When both base and AMENDED labels appear, only an exact safe-field
 * multiset is provably equivalent; any correction/delta shape is quarantined
 * in full with excluded-dollar diagnostics.
 *
 * MEC sometimes emits only the base label after an amendment. A lone label
 * is accepted because the exact-committee export is then the portal's sole
 * current representation of that inventory lineage.
 */
export function selectMissouriCanonicalReportRows<T>(input: {
  inventory: readonly MissouriMecReportInventoryRow[];
  rows: readonly T[];
  reportName: (row: T) => string;
  amountCents: (row: T) => number;
  safeFingerprint: (row: T) => string;
}): MissouriCanonicalReportSelection<T> {
  const inventoryByLineage = new Map<string, MissouriMecReportInventoryRow[]>();
  for (const report of input.inventory) {
    const list = inventoryByLineage.get(report.lineageKey) ?? [];
    list.push(report);
    inventoryByLineage.set(report.lineageKey, list);
  }

  const rowsByLineage = new Map<string, T[]>();
  for (const row of input.rows) {
    const lineage = normalizeMissouriMecReportLineage(input.reportName(row));
    const list = rowsByLineage.get(lineage) ?? [];
    list.push(row);
    rowsByLineage.set(lineage, list);
  }

  const selected: T[] = [];
  const diagnostics: MissouriReportSelectionDiagnostic[] = [];
  for (const [lineageKey, rows] of rowsByLineage) {
    const inventory = inventoryByLineage.get(lineageKey);
    if (!inventory) {
      diagnostics.push({
        lineageKey,
        reason: "missing_inventory",
        excludedRowCount: rows.length,
        excludedAmountCents: rows.reduce((sum, row) => sum + input.amountCents(row), 0),
      });
      continue;
    }

    const baseRows = rows.filter((row) => !/^AMENDED\s+/i.test(input.reportName(row).trim()));
    const amendedRows = rows.filter((row) => /^AMENDED\s+/i.test(input.reportName(row).trim()));
    const hasInventoryAmendment = inventory.some((report) => report.isAmended);
    if (!hasInventoryAmendment && amendedRows.length > 0) {
      diagnostics.push({
        lineageKey,
        reason: "unexpected_amendment",
        excludedRowCount: rows.length,
        excludedAmountCents: rows.reduce((sum, row) => sum + input.amountCents(row), 0),
      });
      continue;
    }
    if (!hasInventoryAmendment || baseRows.length === 0 || amendedRows.length === 0) {
      selected.push(...rows);
      continue;
    }

    const baseFingerprints = baseRows.map(input.safeFingerprint).map(normalizeValue).sort();
    const amendedFingerprints = amendedRows.map(input.safeFingerprint).map(normalizeValue).sort();
    const exactRestatement =
      baseFingerprints.length === amendedFingerprints.length &&
      baseFingerprints.every((fingerprint, index) => fingerprint === amendedFingerprints[index]);
    if (exactRestatement) {
      selected.push(...amendedRows);
      continue;
    }

    diagnostics.push({
      lineageKey,
      reason: "ambiguous_amendment",
      excludedRowCount: rows.length,
      excludedAmountCents: rows.reduce((sum, row) => sum + input.amountCents(row), 0),
    });
  }
  return { rows: selected, diagnostics };
}
