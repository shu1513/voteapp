import { utils as xlsxUtils, write as writeXlsxWorkbook } from "xlsx";

import {
  EFILE_CAL_REQUIRED_COLUMNS_BY_SHEET,
  EFILE_CAL_REQUIRED_SHEETS,
} from "../../../src/pipeline/efileCalFinance/efileCalWorkbookParser.js";

type FixtureCell = string | number | boolean | null;
type FixtureRow = Record<string, FixtureCell | undefined>;

export type EfileCalWorkbookFixtureInput = {
  /** Data rows per sheet name; unspecified required sheets are emitted header-only. */
  rowsBySheet?: Partial<Record<string, readonly FixtureRow[]>>;
  /** Required sheets to leave out entirely (for missing-sheet tests). */
  omitSheets?: readonly string[];
  /** Header override per sheet (for missing-column tests). */
  headersBySheet?: Partial<Record<string, readonly string[]>>;
};

/**
 * Base filing-identity cells shared by every sheet. Mirrors the real export:
 * everything is a text cell (dates YYYYMMDD, amounts "123.00"), flags are
 * boolean cells.
 */
export const EFILE_CAL_FIXTURE_BASE: Readonly<Record<string, FixtureCell>> = {
  Filer_ID: "1480385",
  Filer_NamL: "Test Committee for City Council 2026",
  Report_Num: "000",
  e_filing_id: "24690",
  orig_e_filing_id: "24690",
  Cmtte_Type: "C",
  Rpt_Date: "20260516",
  From_Date: "20260101",
  Thru_Date: "20260418",
  Elect_Date: "20261103",
};

/** Builds a real XLSX workbook shaped like the efile.systems CAL bulk export. */
export function buildEfileCalExportWorkbook(input: EfileCalWorkbookFixtureInput = {}): Uint8Array {
  const omit = new Set(input.omitSheets ?? []);
  const workbook = xlsxUtils.book_new();
  for (const sheetName of EFILE_CAL_REQUIRED_SHEETS) {
    if (omit.has(sheetName)) continue;
    const headers = input.headersBySheet?.[sheetName] ?? EFILE_CAL_REQUIRED_COLUMNS_BY_SHEET[sheetName]!;
    const rows = input.rowsBySheet?.[sheetName] ?? [];
    const aoa: FixtureCell[][] = [
      [...headers],
      ...rows.map((row) => headers.map((header) => row[header] ?? null)),
    ];
    xlsxUtils.book_append_sheet(workbook, xlsxUtils.aoa_to_sheet(aoa), sheetName);
  }
  return new Uint8Array(writeXlsxWorkbook(workbook, { type: "buffer", bookType: "xlsx" }));
}
