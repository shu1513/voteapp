import { createReadStream } from "node:fs";

import type { IllinoisSbeContributionRecord } from "./illinoisSbeCsvReader.js";
import { IllinoisSbeTsvParser } from "./illinoisSbeTsvParser.js";

// Itemized receipts from the SBE bulk export (Receipts.txt on
// DownloadCDDataFiles.aspx). The file spans every committee since the 1990s
// and streams without a content length, so it is parsed incrementally and
// only rows for allow-listed committees within the year window are kept.

export const ILLINOIS_SBE_RECEIPTS_HEADER = [
  "ID",
  "CommitteeID",
  "FiledDocID",
  "ETransID",
  "LastOnlyName",
  "FirstName",
  "RcvDate",
  "Amount",
  "AggregateAmount",
  "LoanAmount",
  "Occupation",
  "Employer",
  "Address1",
  "Address2",
  "City",
  "State",
  "Zip",
  "D2Part",
  "Description",
  "VendorLastOnlyName",
  "VendorFirstName",
  "VendorAddress1",
  "VendorAddress2",
  "VendorCity",
  "VendorState",
  "VendorZip",
  "Archived",
  "Country",
  "RedactionRequested",
] as const;

export type IllinoisSbeReceiptRecord = {
  committeeId: string;
  contributorName: string | null;
  contributorAddress: string | null;
  occupation: string | null;
  employer: string | null;
  amount: number;
  receivedDate: string | null;
  d2Part: string | null;
  description: string | null;
};

export type IllinoisSbeReceiptsLoadResult = {
  receiptsByCommitteeId: Map<string, IllinoisSbeReceiptRecord[]>;
  visitedRowCount: number;
  keptRowCount: number;
  archivedRowCount: number;
};

// D-2 form parts per the SBE data dictionary; D2Part values carry a schedule
// suffix ("1A"), so only the leading digit is meaningful.
const D2_PART_CONTRIBUTION_TYPES: Readonly<Record<string, string>> = {
  "1": "Individual Contribution",
  "2": "Transfer In",
  "3": "Loan Received",
  "4": "Other Receipt",
  "5": "In-Kind Contribution",
};

function clean(value: string | undefined): string {
  return (value ?? "").replace(/\0/g, "").trim().replace(/\s+/g, " ");
}

function cleanOrNull(value: string | undefined): string | null {
  const cleaned = clean(value);
  return cleaned.length > 0 ? cleaned : null;
}

function parseReceiptYear(rawDate: string): number | null {
  const match = /^(\d{4})-\d{2}-\d{2}/.exec(rawDate);
  return match ? Number(match[1]) : null;
}

function normalizeMinReceiptYear(value: number | undefined): number | null {
  if (value === undefined) {
    return null;
  }
  if (!Number.isInteger(value) || value < 1900 || value > 2100) {
    throw new Error(`Invalid Illinois SBE receipts minReceiptYear: ${value}`);
  }
  return value;
}

export async function loadIllinoisSbeReceiptsByCommitteeId(input: {
  path: string;
  committeeIds: ReadonlySet<string>;
  minReceiptYear?: number;
}): Promise<IllinoisSbeReceiptsLoadResult> {
  const minReceiptYear = normalizeMinReceiptYear(input.minReceiptYear);
  const receiptsByCommitteeId = new Map<string, IllinoisSbeReceiptRecord[]>();
  let visitedRowCount = 0;
  let keptRowCount = 0;
  let archivedRowCount = 0;

  const parser = new IllinoisSbeTsvParser({
    label: "Receipts.txt",
    expectedHeader: ILLINOIS_SBE_RECEIPTS_HEADER,
    visit: (row) => {
      visitedRowCount += 1;
      const committeeId = clean(row[1]);
      if (!committeeId || !input.committeeIds.has(committeeId)) {
        return;
      }
      if (clean(row[26]).toLowerCase() === "true") {
        // Superseded by an amendment; counting it would double-report.
        archivedRowCount += 1;
        return;
      }
      const receivedDate = cleanOrNull(row[6]);
      if (minReceiptYear !== null && receivedDate !== null) {
        const year = parseReceiptYear(receivedDate);
        if (year !== null && year < minReceiptYear) {
          return;
        }
      }
      const amount = Number(clean(row[7]).replace(/[$,]/g, ""));
      if (!Number.isFinite(amount)) {
        return;
      }
      const contributorName = cleanOrNull([clean(row[5]), clean(row[4])].filter(Boolean).join(" "));
      const contributorAddress = cleanOrNull(
        [clean(row[12]), clean(row[13]), clean(row[14]), clean(row[15]), clean(row[16])].filter(Boolean).join(" ")
      );
      const record: IllinoisSbeReceiptRecord = {
        committeeId,
        contributorName,
        contributorAddress,
        occupation: cleanOrNull(row[10]),
        employer: cleanOrNull(row[11]),
        amount,
        receivedDate,
        d2Part: cleanOrNull(row[17]),
        description: cleanOrNull(row[18]),
      };
      const records = receiptsByCommitteeId.get(committeeId);
      if (records) {
        records.push(record);
      } else {
        receiptsByCommitteeId.set(committeeId, [record]);
      }
      keptRowCount += 1;
    },
  });

  const stream = createReadStream(input.path, { encoding: "utf8" });
  for await (const chunk of stream) {
    parser.push(chunk as string);
  }
  parser.end();

  return { receiptsByCommitteeId, visitedRowCount, keptRowCount, archivedRowCount };
}

export function contributionTypeFromIllinoisSbeD2Part(d2Part: string | null): string | null {
  const leadingDigit = d2Part?.trim().charAt(0) ?? "";
  return D2_PART_CONTRIBUTION_TYPES[leadingDigit] ?? null;
}

export function toIllinoisSbeContributionRecordFromReceipt(input: {
  receipt: IllinoisSbeReceiptRecord;
  recipientCommitteeName: string;
  sourceUrl: string;
}): IllinoisSbeContributionRecord {
  return {
    contributorName: input.receipt.contributorName,
    contributorAddress: input.receipt.contributorAddress,
    occupation: input.receipt.occupation,
    employer: input.receipt.employer,
    amount: input.receipt.amount,
    receivedDate: input.receipt.receivedDate,
    reportReceivedDate: null,
    contributionType: contributionTypeFromIllinoisSbeD2Part(input.receipt.d2Part),
    recipientCommitteeName: input.recipientCommitteeName,
    description: input.receipt.description,
    vendorName: null,
    vendorAddress: null,
    sourceUrl: input.sourceUrl,
  };
}
