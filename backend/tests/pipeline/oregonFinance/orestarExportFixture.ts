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

export const OREGON_COMMITTEE_EXPORT_FIXTURE_HEADERS = [
  "Committee Id",
  "Committee Name",
  "Committee Type",
  "Committee SubType",
  "Candidate Office",
  "Candidate Office Group",
  "Filing Date",
  "Organization Filing Date",
  "Treasurer First Name",
  "Treasurer Last Name",
  "Treasurer Mailing Address",
  "Treasurer Work Phone",
  "Treasurer Fax",
  "Candidate First Name",
  "Candidate Last Name",
  "Candidate Maling Address",
  "Candidate Work Phone",
  "Candidate Residence Phone",
  "Candidate Fax",
  "Candidate Email",
  "Active Election",
  "Measure",
] as const;

function buildWorkbook(
  rows: readonly Record<string, string | number | null | undefined>[],
  headers: readonly string[]
): Uint8Array {
  const aoa: (string | number | null)[][] = [
    [...headers],
    ...rows.map((row) => headers.map((header) => row[header] ?? null)),
  ];
  const sheet = xlsxUtils.aoa_to_sheet(aoa);
  const workbook = xlsxUtils.book_new();
  xlsxUtils.book_append_sheet(workbook, sheet, "Sheet1");
  return new Uint8Array(writeXlsWorkbook(workbook, { type: "buffer", bookType: "biff8" }));
}

/** Builds a real BIFF .xls workbook the way ORESTAR's XcelCNESearch serves one. */
export function buildOregonOrestarExportWorkbook(
  rows: readonly Partial<Record<(typeof OREGON_EXPORT_FIXTURE_HEADERS)[number], string | number>>[],
  headers: readonly string[] = OREGON_EXPORT_FIXTURE_HEADERS
): Uint8Array {
  return buildWorkbook(rows, headers);
}

export function buildOregonOrestarCommitteeExportWorkbook(
  rows: readonly Partial<Record<(typeof OREGON_COMMITTEE_EXPORT_FIXTURE_HEADERS)[number], string | number>>[],
  headers: readonly string[] = OREGON_COMMITTEE_EXPORT_FIXTURE_HEADERS
): Uint8Array {
  return buildWorkbook(rows, headers);
}
