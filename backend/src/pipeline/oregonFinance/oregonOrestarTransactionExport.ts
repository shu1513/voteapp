import { read as readXlsWorkbook, utils as xlsxUtils } from "xlsx";

import type { OregonOrestarTransactionDetail } from "./oregonOrestarParser.js";

/**
 * Parser for the ORESTAR transaction search's XcelCNESearch export — a binary
 * BIFF .xls workbook whose first sheet holds one header row plus one row per
 * transaction. The export returns the FULL result set of the search that
 * produced it (verified live: 116 rows for a search reporting 116 records),
 * which is what makes it the right money path for large committees: one
 * download instead of one detail-page fetch per transaction.
 */

// Compound File Binary magic (D0 CF 11 E0): every real .xls starts with it. A
// blocked or degraded portal answers with HTML instead, so this is the
// cheapest fail-closed check before handing bytes to the workbook reader.
const CFB_MAGIC = [0xd0, 0xcf, 0x11, 0xe0] as const;

export function isOregonOrestarExportWorkbook(data: Uint8Array): boolean {
  return CFB_MAGIC.every((byte, index) => data[index] === byte);
}

const REQUIRED_EXPORT_HEADERS = [
  "Tran Id",
  "Tran Date",
  "Filer",
  "Filer Id",
  "Contributor/Payee",
  "Sub Type",
  "Amount",
] as const;

function toText(value: unknown): string | null {
  if (typeof value === "string") {
    const trimmed = value.trim().replace(/\s+/g, " ");
    return trimmed.length > 0 ? trimmed : null;
  }
  if (typeof value === "number" && Number.isFinite(value)) {
    return String(value);
  }
  return null;
}

function toAmount(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === "string") {
    const parsed = Number(value.replace(/[$,]/g, "").trim());
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}

export function parseOregonOrestarTransactionExport(input: {
  data: Uint8Array;
  /** Stamped onto every row; the caller knows which cneSearchTranType it searched for. */
  transactionType?: string | null;
  sourceUrl?: string | null;
}): OregonOrestarTransactionDetail[] {
  if (!isOregonOrestarExportWorkbook(input.data)) {
    throw new Error("ORESTAR export response is not an .xls workbook; treating as blocked rather than parsing");
  }

  let rows: Record<string, unknown>[];
  try {
    const workbook = readXlsWorkbook(input.data, { type: "buffer" });
    const sheetName = workbook.SheetNames[0];
    const sheet = sheetName ? workbook.Sheets[sheetName] : undefined;
    if (!sheet) {
      throw new Error("workbook has no sheets");
    }
    rows = xlsxUtils.sheet_to_json<Record<string, unknown>>(sheet, { raw: true, defval: null });
  } catch (error) {
    throw new Error(
      `ORESTAR export workbook could not be read: ${error instanceof Error ? error.message : String(error)}`
    );
  }

  if (rows.length > 0) {
    const headers = new Set(Object.keys(rows[0] ?? {}));
    const missing = REQUIRED_EXPORT_HEADERS.filter((header) => !headers.has(header));
    if (missing.length > 0) {
      throw new Error(`ORESTAR export is missing required columns: ${missing.join(", ")}`);
    }
  }

  const transactionType = input.transactionType?.trim() || null;
  const sourceUrl = input.sourceUrl?.trim() || null;
  return rows.map((row) => ({
    transactionId: toText(row["Tran Id"]),
    transactionDate: toText(row["Tran Date"]),
    transactionType,
    transactionSubType: toText(row["Sub Type"]),
    filedDate: toText(row["Filed Date"]),
    amount: toAmount(row["Amount"]),
    aggregate: toAmount(row["Aggregate Amount"]),
    processStatus: toText(row["Tran Status"]),
    purpose: toText(row["Purp Desc"]),
    filerCommitteeName: toText(row["Filer"]),
    filerCommitteeId: toText(row["Filer Id"]),
    addressBookType: toText(row["Book Type"]),
    contributorPayeeName: toText(row["Contributor/Payee"]),
    address: null,
    occupation: toText(row["Occptn Txt"]),
    employerName: toText(row["Emp Name"]),
    // Independent-expenditure associations only exist on transaction detail
    // pages; the export has no equivalent column. This path has never carried
    // them (see the outside-spending limitation note in the batch sync).
    outsideAssociations: [],
    sourceUrl,
  }));
}
