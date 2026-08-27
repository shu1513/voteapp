import { describe, expect, it } from "vitest";

import {
  decodeWestVirginiaCsvBytes,
  parseWestVirginiaAmountToCents,
  parseWestVirginiaContributionCsv,
  parseWestVirginiaCsvText,
  parseWestVirginiaExpenditureCsv,
} from "../../../src/pipeline/westVirginiaFinance/westVirginiaCfrsCsv.js";

const CONTRIBUTION_HEADER =
  "RegistrantID,CommitteeName,CandidateName,TransactionType,TransactionCategory,TransactionDate,TransactionAmount,ContributorPayeeType,ContributorPayeeName,ContributorAddress,EmployerName,FiledDate";

const EXPENDITURE_HEADER =
  "RegistrantID,CommitteeName,CandidateName,TransactionType,ExpenditureType,ExpenditurePurpose,TransactionDate,TransactionAmount,RecipientType,RecipientName,RecipientAddress,FiledDate";

describe("parseWestVirginiaAmountToCents", () => {
  it("parses four-decimal amounts exactly", () => {
    expect(parseWestVirginiaAmountToCents("25.4400")).toBe(2_540 + 4);
    expect(parseWestVirginiaAmountToCents("897.1000")).toBe(89_710);
    expect(parseWestVirginiaAmountToCents("500")).toBe(50_000);
  });

  it("parses sub-dollar amounts without a leading zero", () => {
    expect(parseWestVirginiaAmountToCents(".9300")).toBe(93);
    expect(parseWestVirginiaAmountToCents("-.5000")).toBe(-50);
  });

  it("fails closed on sub-cent precision and garbage", () => {
    expect(parseWestVirginiaAmountToCents("1.2345")).toBeNull();
    expect(parseWestVirginiaAmountToCents("$5.00")).toBeNull();
    expect(parseWestVirginiaAmountToCents("")).toBeNull();
  });
});

describe("parseWestVirginiaCsvText", () => {
  it("treats quotes inside unquoted fields as literal characters", () => {
    const rows = parseWestVirginiaCsvText('a,Warren "Dean" Jeffries,c');
    expect(rows).toEqual([["a", 'Warren "Dean" Jeffries', "c"]]);
  });

  it("still honors standard quoted fields with embedded commas", () => {
    const rows = parseWestVirginiaCsvText('a,"x, y",c');
    expect(rows).toEqual([["a", "x, y", "c"]]);
  });
});

describe("decodeWestVirginiaCsvBytes", () => {
  it("decodes cp1252 smart quotes that break UTF-8", () => {
    const bytes = Uint8Array.from([0x4f, 0x92, 0x73]); // O’s in cp1252
    expect(decodeWestVirginiaCsvBytes(bytes)).toBe("O’s");
  });
});

describe("parseWestVirginiaContributionCsv", () => {
  it("parses a well-formed row and drops the address column", () => {
    const text = `${CONTRIBUTION_HEADER}\n1010003778,Richie Robb for State Senate,Richie Robb,Contributions,In-Kind,2026-06-17,25.4400,Individual,Jacob Hively,  12 Main St  ,Acme,2026-07-07\n`;
    const result = parseWestVirginiaContributionCsv(text);
    expect(result.errors).toEqual([]);
    expect(result.rows).toHaveLength(1);
    const row = result.rows[0];
    expect(row.amountCents).toBe(2_544);
    expect(row.employerName).toBe("Acme");
    expect(row).not.toHaveProperty("contributorAddress");
  });

  it("fails the artifact on header drift", () => {
    expect(() => parseWestVirginiaContributionCsv("A,B,C\n1,2,3\n")).toThrow(/header drift/);
  });

  it("reports rows with wrong width as errors instead of guessing", () => {
    const text = `${CONTRIBUTION_HEADER}\n1,2,3\n`;
    const result = parseWestVirginiaContributionCsv(text);
    expect(result.rows).toEqual([]);
    expect(result.errors).toEqual([{ line: 2, reason: "expected 12 columns, got 3" }]);
  });

  it("recovers a bad-width row from an unescaped comma in the contributor name", () => {
    // Live fixture shape: "Alonzio Perry, II" splits the name column.
    const text = `${CONTRIBUTION_HEADER}\n1020001301,Berkeley County Republican Club,,Contributions,Monetary,2025-10-28,72.5200,Individual,Alonzio Perry, II,496 Hogan Drive   Martinsburg WV 25405  ,,2026-01-03\n`;
    const result = parseWestVirginiaContributionCsv(text);
    expect(result.errors).toEqual([]);
    expect(result.recoveredRowCount).toBe(1);
    const row = result.rows[0];
    expect(row.recovered).toBe(true);
    expect(row.amountCents).toBe(7_252);
    expect(row.contributorName).toBe("Alonzio Perry");
    expect(row.employerName).toBeNull();
    expect(row.filedDate).toBe("2026-01-03");
  });
});

describe("parseWestVirginiaExpenditureCsv", () => {
  const goodPrefix = "1010003903,Stiles for WV,Deborah K Stiles,Expenditures,Monetary,Advertising,2026-02-23,100.0000,Business or Organization";

  it("passes a well-formed 12-column row through unrecovered", () => {
    const text = `${EXPENDITURE_HEADER}\n${goodPrefix},Acme,1 Elm St,2026-04-06\n`;
    const result = parseWestVirginiaExpenditureCsv(text);
    expect(result.errors).toEqual([]);
    expect(result.recoveredRowCount).toBe(0);
    expect(result.rows[0].recipientName).toBe("Acme");
    expect(result.rows[0].recovered).toBe(false);
  });

  it("recovers 13-15 column rows via the prefix rule", () => {
    // goodPrefix is 9 columns; name+address+filed adds 3 more, so one to
    // three extra middle columns produce 13-15 column rows.
    const thirteen = `${goodPrefix},Secretary of State,1900 Kanawha Blvd., East Charleston WV,2026-04-06`;
    const fourteen = `${goodPrefix},A,B,C,2026-04-06`;
    const fifteen = `${goodPrefix},A,B,C,D,2026-04-06`;
    const text = `${EXPENDITURE_HEADER}\n${thirteen}\n${fourteen}\n${fifteen}\n`;
    const result = parseWestVirginiaExpenditureCsv(text);
    expect(result.errors).toEqual([]);
    expect(result.recoveredRowCount).toBe(3);
    // Only the first column of the damaged span is kept; the ambiguous tail
    // (which mixes in the address) is discarded.
    expect(result.rows[0].recipientName).toBe("Secretary of State");
    expect(result.rows.every((row) => row.filedDate === "2026-04-06")).toBe(true);
    expect(result.rows.every((row) => row.amountCents === 10_000)).toBe(true);
  });

  it("fails rows beyond three extra columns or with a broken prefix", () => {
    const sixteen = `${goodPrefix},A,B,C,D,E,F,2026-04-06`;
    const brokenPrefix = `1,2,3,4,5,6,not-a-date,100.0000,X,Y,Z,2026-04-06`;
    const text = `${EXPENDITURE_HEADER}\n${sixteen}\n${brokenPrefix}\n`;
    const result = parseWestVirginiaExpenditureCsv(text);
    expect(result.rows).toEqual([]);
    expect(result.errors).toEqual([
      { line: 2, reason: "expected 12 columns, got 16" },
      { line: 3, reason: "invalid date or amount" },
    ]);
  });
});
