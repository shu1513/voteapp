// Montana canonical report selection (docs/plans/montana-finance.md).
//
// CERS keeps exactly one row per (entity, form, period) with amendments
// replacing the original in place (Phase 0 Q2: current-only, no history
// endpoint). Selection is still defensive: if duplicates ever appear, the
// canonical row is picked by amendedDate, then receivedDate, then reportId
// — NEVER by a filed-date string (observed inconsistent).
//
// Only C5 (candidate) reports carry the cash-begin chain and period
// totals. C7/C7E rows with status Incorporated are the plan's hard
// exclusion (their contents re-appear inside the next C5); any other
// non-C5 or non-final row is excluded with a diagnostic.

import type { MontanaCersReportInventoryRow } from "./montanaCersParsers.js";

export const MONTANA_CHAIN_FORM_TYPE = "C5";
export const MONTANA_INCORPORATED_STATUS_CODE = "INCRP";

// PENDA ("Pending-Amended"): the filed report has an amendment in progress;
// the FILED version is still the operative public filing — its detail rows
// stay served and the CONTR/EXPEND exports include them. Excluding it drops
// whole periods from the chain and totals (live Phase 3: a filer with two
// PENDA periods lost $3,380 of contributions). Once the amendment lands the
// row becomes AMEND with the new data (Q2: current-only, in place).
const CANONICAL_STATUS_CODES = new Set(["FILED", "AMEND", "PENDA"]);

export type MontanaReportSelectionDiagnostic = {
  reportId: number;
  reason: "incorporated" | "non_chain_form" | "unexpected_status" | "superseded_duplicate";
};

export type MontanaCanonicalReportSelection = {
  /** Canonical C5 rows, sorted by period start ascending. */
  reports: MontanaCersReportInventoryRow[];
  diagnostics: MontanaReportSelectionDiagnostic[];
  /**
   * True when two canonical reports cover overlapping periods — a shape the
   * chain math cannot order. Callers fail closed on it.
   */
  hasOverlappingPeriods: boolean;
};

/** MM/DD/YYYY -> sortable YYYYMMDD integer. Format is parser-validated. */
function periodDateSortKey(value: string): number {
  const [month, day, year] = value.split("/");
  return Number(`${year}${month}${day}`);
}

function canonicalRank(row: MontanaCersReportInventoryRow): [number, number, number] {
  return [row.amendedDate ?? Number.NEGATIVE_INFINITY, row.receivedDate, row.reportId];
}

export function selectMontanaCanonicalReports(
  inventory: readonly MontanaCersReportInventoryRow[]
): MontanaCanonicalReportSelection {
  const diagnostics: MontanaReportSelectionDiagnostic[] = [];
  const byPeriod = new Map<string, MontanaCersReportInventoryRow[]>();

  for (const row of inventory) {
    if (row.statusCode === MONTANA_INCORPORATED_STATUS_CODE) {
      diagnostics.push({ reportId: row.reportId, reason: "incorporated" });
      continue;
    }
    if (row.formTypeCode !== MONTANA_CHAIN_FORM_TYPE) {
      diagnostics.push({ reportId: row.reportId, reason: "non_chain_form" });
      continue;
    }
    if (!CANONICAL_STATUS_CODES.has(row.statusCode)) {
      diagnostics.push({ reportId: row.reportId, reason: "unexpected_status" });
      continue;
    }
    const key = [row.entitySubId, row.fromDateStr, row.toDateStr].join("\u0000");
    const list = byPeriod.get(key) ?? [];
    list.push(row);
    byPeriod.set(key, list);
  }

  const reports: MontanaCersReportInventoryRow[] = [];
  for (const rows of byPeriod.values()) {
    const sorted = [...rows].sort((left, right) => {
      const a = canonicalRank(left);
      const b = canonicalRank(right);
      return a[0] - b[0] || a[1] - b[1] || a[2] - b[2];
    });
    const canonical = sorted.at(-1)!;
    for (const row of sorted.slice(0, -1)) {
      diagnostics.push({ reportId: row.reportId, reason: "superseded_duplicate" });
    }
    reports.push(canonical);
  }

  reports.sort(
    (left, right) =>
      periodDateSortKey(left.fromDateStr) - periodDateSortKey(right.fromDateStr) ||
      periodDateSortKey(left.toDateStr) - periodDateSortKey(right.toDateStr)
  );

  let hasOverlappingPeriods = false;
  for (let index = 1; index < reports.length; index += 1) {
    if (periodDateSortKey(reports[index]!.fromDateStr) <= periodDateSortKey(reports[index - 1]!.toDateStr)) {
      hasOverlappingPeriods = true;
      break;
    }
  }

  return { reports, diagnostics, hasOverlappingPeriods };
}
