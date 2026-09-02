import { describe, expect, it } from "vitest";

import {
  ARKANSAS_EXPENDITURE_CSV_COLUMNS,
  ARKANSAS_RECEIPT_CSV_COLUMNS,
  forEachArkansasReceiptCsvRow,
  mergeArkansasOccupation,
  parseArkansasCurrencyCents,
  parseArkansasExpenditureCsv,
  parseArkansasReceiptCsv,
  type ArkansasMalformedCsvRecord,
} from "../../../src/pipeline/arkansasFinance/arkansasCfisCsv.js";

const RECEIPT_HEADER = ARKANSAS_RECEIPT_CSV_COLUMNS.join(",");
const EXPENDITURE_HEADER = ARKANSAS_EXPENDITURE_CSV_COLUMNS.join(",");

function receiptLine(overrides: Partial<Record<(typeof ARKANSAS_RECEIPT_CSV_COLUMNS)[number], string>>): string {
  const defaults: Record<(typeof ARKANSAS_RECEIPT_CSV_COLUMNS)[number], string> = {
    "Filing Entity ID": "1004",
    "Entity Name": '"Sanders, Sarah  (Sarah for Governor )"',
    FilerType: "Candidate",
    "Transaction Type": "Contribution",
    "Transaction Sub Type": "Itemized Monetary",
    "Funding Source / Loan Source Type": "Individual",
    "Source Name": '"Walton, Thomas "',
    "Source Address": '"PO Box 1860, Bentonville, AR 72712"',
    "Employer Name": "Runway Group LLC",
    Occupation: "Financial / Investment",
    "Occupation Other": "",
    "Transaction Date": "07/31/2026",
    "Transaction Amount": '"$3,500.00"',
    "Transaction Description": "",
    "Transaction ID": "1758108",
    "Election Type": "General",
    "Election Year": "2026",
    "Guarantor Name": "",
    "Guarantor Address": "",
    "Report Filed Date": "08/20/2026",
    "Report Name": "2026 July Monthly Report",
    Amended: "N",
  };
  return ARKANSAS_RECEIPT_CSV_COLUMNS.map((column) => overrides[column] ?? defaults[column]).join(",");
}

describe("Arkansas CFIS CSV parsing", () => {
  it("parses a receipt row with quoted commas", () => {
    const rows = parseArkansasReceiptCsv(`${RECEIPT_HEADER}\n${receiptLine({})}\n`);
    expect(rows).toHaveLength(1);
    expect(rows[0]!["Entity Name"]).toBe("Sanders, Sarah  (Sarah for Governor )");
    expect(rows[0]!["Transaction Amount"]).toBe("$3,500.00");
    expect(rows[0]!.Amended).toBe("N");
  });

  it("keeps quoted multiline fields inside one record", () => {
    const multiline = receiptLine({ "Transaction Description": '"line one\n2, line two"' });
    const rows = parseArkansasReceiptCsv(`${RECEIPT_HEADER}\n${multiline}\n${receiptLine({ "Filing Entity ID": "7" })}\n`);
    expect(rows).toHaveLength(2);
    expect(rows[0]!["Transaction Description"]).toBe("line one\n2, line two");
    expect(rows[1]!["Filing Entity ID"]).toBe("7");
  });

  it("rejects a changed receipt header", () => {
    const badHeader = RECEIPT_HEADER.replace("Occupation Other", "Occupation2");
    expect(() => parseArkansasReceiptCsv(`${badHeader}\n${receiptLine({})}\n`)).toThrow(
      /receipt CSV header changed/
    );
  });

  it("strips a UTF-8 BOM before header validation", () => {
    const rows = parseArkansasReceiptCsv(`﻿${RECEIPT_HEADER}\n${receiptLine({})}\n`);
    expect(rows).toHaveLength(1);
  });

  it("keeps a raw quote inside a value and closes on the escaped-quote boundary (live Civix shape)", () => {
    // TEXP 2024/2026: Civix writes raw quotes inside values, so `"",` means
    // "literal quote, field ends" — not an RFC escaped quote continuing the field.
    const line = receiptLine({
      "Source Name": '"Lead, Encourage, Elect PAC "LEE PAC""',
    });
    const rows = parseArkansasReceiptCsv(`${RECEIPT_HEADER}\n${line}\n`);
    expect(rows).toHaveLength(1);
    expect(rows[0]!["Source Name"]).toBe('Lead, Encourage, Elect PAC "LEE PAC"');
    expect(rows[0]!["Source Address"]).toBe("PO Box 1860, Bentonville, AR 72712");
  });

  it("quarantines mis-quoted source records through onMalformed", () => {
    // Live defect (TCON 2023): a field VALUE containing a literal double-quote
    // breaks Civix's export quoting and splits into extra columns.
    const malformedLine = receiptLine({
      "Source Address": '""1904 Lee Creek Drive\t", Van Buren, AR 72956"',
    });
    const csv = `${RECEIPT_HEADER}\n${malformedLine}\n${receiptLine({ "Filing Entity ID": "7" })}\n`;
    expect(() => parseArkansasReceiptCsv(csv)).toThrow(/columns; expected 22/);

    const malformed: ArkansasMalformedCsvRecord[] = [];
    const rows: string[] = [];
    const recordCount = forEachArkansasReceiptCsvRow(
      csv,
      (row) => rows.push(row["Filing Entity ID"]),
      (record) => malformed.push(record)
    );
    expect(recordCount).toBe(2);
    expect(rows).toEqual(["7"]);
    expect(malformed).toHaveLength(1);
    expect(malformed[0]!.columnCount).toBe(24);
    expect(malformed[0]!.rowNumber).toBe(2);
  });

  it("parses the expenditure header shape", () => {
    const line = [
      "564",
      '"Allen, Fred "',
      "Candidate",
      "Expenditure",
      "Itemized Monetary",
      "Business/Organization/Unlisted PAC",
      "St. Mark Baptist Church",
      '"5722 W 12th St, Little Rock, AR 72204"',
      "03/01/2026",
      '"$1,000.00"',
      "",
      "1900000",
      "Entertainment",
      "",
      "General",
      "2026",
      "04/23/2026",
      "2026 Q1 Quarterly Report",
      "N",
    ].join(",");
    const rows = parseArkansasExpenditureCsv(`${EXPENDITURE_HEADER}\n${line}\n`);
    expect(rows).toHaveLength(1);
    expect(rows[0]!["Payee Name"]).toBe("St. Mark Baptist Church");
    expect(rows[0]!["Transaction Category"]).toBe("Entertainment");
  });
});

describe("parseArkansasCurrencyCents", () => {
  it("parses dollar-formatted strings", () => {
    expect(parseArkansasCurrencyCents("$5,000.00")).toBe(500_000);
    expect(parseArkansasCurrencyCents("$10.00")).toBe(1_000);
    expect(parseArkansasCurrencyCents("0.5")).toBe(50);
  });

  it("treats parenthesized amounts as negative", () => {
    expect(parseArkansasCurrencyCents("($164.99)")).toBe(-16_499);
  });

  it("accepts a leading minus sign", () => {
    expect(parseArkansasCurrencyCents("-$25.00")).toBe(-2_500);
  });

  it("rejects malformed amounts", () => {
    expect(() => parseArkansasCurrencyCents("")).toThrow(/currency amount/);
    expect(() => parseArkansasCurrencyCents("$1.234")).toThrow(/currency amount/);
    expect(() => parseArkansasCurrencyCents("abc")).toThrow(/currency amount/);
  });
});

describe("mergeArkansasOccupation", () => {
  it("prefers the dropdown column", () => {
    expect(mergeArkansasOccupation("Financial / Investment", "ignored")).toEqual({
      value: "Financial / Investment",
      source: "occupation",
    });
  });

  it("unwraps the Other(...) wrapper", () => {
    expect(mergeArkansasOccupation("Other(Chief Deputy Coroner)", "")).toEqual({
      value: "Chief Deputy Coroner",
      source: "occupation",
    });
  });

  it("falls back to the free-text column", () => {
    expect(mergeArkansasOccupation("", "Farmer")).toEqual({ value: "Farmer", source: "occupation_other" });
    expect(mergeArkansasOccupation("Other", "Farmer")).toEqual({ value: "Farmer", source: "occupation_other" });
  });

  it("returns none when both columns are empty", () => {
    expect(mergeArkansasOccupation("", " ")).toEqual({ value: null, source: "none" });
  });
});
