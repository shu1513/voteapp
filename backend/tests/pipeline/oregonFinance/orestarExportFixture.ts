import { utils as xlsxUtils, write as writeXlsWorkbook } from "xlsx";

export const OREGON_EXPORT_FIXTURE_HEADERS = [
  "Tran Id",
  "Original Id",
  "Tran Date",
  "Tran Status",
  "Filer",
  "Contributor/Payee",
  "Sub Type",
  "Amount",
  "Aggregate Amount",
  "Filer Id",
  "Filed Date",
  "Book Type",
  "Occptn Txt",
  "Emp Name",
  "Purp Desc",
] as const;

/** Builds a real BIFF .xls workbook the way ORESTAR's XcelCNESearch serves one. */
export function buildOregonOrestarExportWorkbook(
  rows: readonly Partial<Record<(typeof OREGON_EXPORT_FIXTURE_HEADERS)[number], string | number>>[],
  headers: readonly string[] = OREGON_EXPORT_FIXTURE_HEADERS
): Uint8Array {
  const aoa: (string | number | null)[][] = [
    [...headers],
    ...rows.map((row) => headers.map((header) => (row as Record<string, string | number>)[header] ?? null)),
  ];
  const sheet = xlsxUtils.aoa_to_sheet(aoa);
  const workbook = xlsxUtils.book_new();
  xlsxUtils.book_append_sheet(workbook, sheet, "Sheet1");
  return new Uint8Array(writeXlsWorkbook(workbook, { type: "buffer", bookType: "biff8" }));
}
