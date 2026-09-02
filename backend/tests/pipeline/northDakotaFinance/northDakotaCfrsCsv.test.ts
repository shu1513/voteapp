import { describe, expect, it } from "vitest";

import {
  decodeNorthDakotaCsvBytes,
  parseNorthDakotaAmountToCents,
  parseNorthDakotaContributionCsv,
  parseNorthDakotaCsvText,
  parseNorthDakotaExpenditureCsv,
  parseNorthDakotaFiledReportCsv,
  parseNorthDakotaReportingScheduleCsv,
} from "../../../src/pipeline/northDakotaFinance/northDakotaCfrsCsv.js";

const CONTRIBUTION_HEADER =
  "RegistrantID,CommitteeName,CandidateName,TransactionType,TransactionCategory,TransactionDate,TransactionAmount,ContributorPayeeType,ContributorPayeeName,ContributorAddress,EmployerName,FiledDate";

const EXPENDITURE_HEADER =
  "RegistrantID,CommitteeName,CandidateName,TransactionType,ExpenditureType,ExpenditurePurpose,TransactionDate,TransactionAmount,RecipientType,RecipientName,RecipientAddress,FiledDate";

describe("parseNorthDakotaAmountToCents", () => {
  it("parses four-decimal amounts exactly", () => {
    expect(parseNorthDakotaAmountToCents("960.6000")).toBe(96_060);
    expect(parseNorthDakotaAmountToCents("500.0000")).toBe(50_000);
    expect(parseNorthDakotaAmountToCents("500")).toBe(50_000);
    expect(parseNorthDakotaAmountToCents(".9300")).toBe(93);
  });

  it("fails closed on sub-cent precision and garbage", () => {
    expect(parseNorthDakotaAmountToCents("1.2345")).toBeNull();
    expect(parseNorthDakotaAmountToCents("$5.00")).toBeNull();
    expect(parseNorthDakotaAmountToCents("")).toBeNull();
  });
});

describe("parseNorthDakotaCsvText", () => {
  it("splits CRLF rows and treats quotes inside unquoted fields as literal characters", () => {
    expect(parseNorthDakotaCsvText('a,Warren "Dean" Jeffries,c\r\nd,e,f\r\n')).toEqual([
      ["a", 'Warren "Dean" Jeffries', "c"],
      ["d", "e", "f"],
    ]);
  });

  it("still honors standard quoted fields with embedded commas", () => {
    expect(parseNorthDakotaCsvText('a,"x, y",c')).toEqual([["a", "x, y", "c"]]);
  });
});

describe("decodeNorthDakotaCsvBytes", () => {
  it("passes ASCII through and decodes cp1252 smart quotes", () => {
    expect(decodeNorthDakotaCsvBytes(new TextEncoder().encode("plain,ascii"))).toBe("plain,ascii");
    expect(decodeNorthDakotaCsvBytes(Uint8Array.from([0x4f, 0x92, 0x73]))).toBe("O’s");
  });
});

describe("parseNorthDakotaContributionCsv", () => {
  it("parses a live-shaped row and drops the address column", () => {
    const text = `${CONTRIBUTION_HEADER}\r\n1010001478,Blaine DesLauriers for ND,Blaine DesLauriers,Contributions,Monetary,2026-01-12,960.6000,Individual,Patrick Jones,PO BOX 179   Minot ND 58702  ,,2026-05-06\r\n`;
    const result = parseNorthDakotaContributionCsv(text);
    expect(result.errors).toEqual([]);
    expect(result.rows).toHaveLength(1);
    const row = result.rows[0];
    expect(row.amountCents).toBe(96_060);
    expect(row.contributorType).toBe("Individual");
    expect(row.employerName).toBeNull();
    expect(row.recovered).toBe(false);
    expect(row).not.toHaveProperty("contributorAddress");
  });

  it("keeps lump-sum rows with a blank contributor type", () => {
    const text = `${CONTRIBUTION_HEADER}\r\n1030001017,District 1 Republicans,,Contributions,Total - $200 or less,2025-12-31,1250.0000,,Total - $200 or less,,,2026-01-15\r\n`;
    const result = parseNorthDakotaContributionCsv(text);
    expect(result.errors).toEqual([]);
    expect(result.rows[0].transactionCategory).toBe("Total - $200 or less");
    expect(result.rows[0].contributorType).toBe("");
    expect(result.rows[0].amountCents).toBe(125_000);
  });

  it("fails the artifact on header drift", () => {
    expect(() => parseNorthDakotaContributionCsv("A,B,C\n1,2,3\n")).toThrow(/header drift/);
  });

  it("recovers a bad-width row from an unescaped comma in the contributor name", () => {
    const text = `${CONTRIBUTION_HEADER}\n1020001301,Some PAC,,Contributions,Monetary,2025-10-28,72.5200,Individual,Alonzio Perry, II,496 Hogan Drive   Martinsburg ND 58405  ,,2026-01-03\n`;
    const result = parseNorthDakotaContributionCsv(text);
    expect(result.errors).toEqual([]);
    expect(result.recoveredRowCount).toBe(1);
    expect(result.rows[0].recovered).toBe(true);
    expect(result.rows[0].contributorName).toBe("Alonzio Perry");
    expect(result.rows[0].filedDate).toBe("2026-01-03");
  });

  it("reports rows with wrong width as errors instead of guessing", () => {
    const result = parseNorthDakotaContributionCsv(`${CONTRIBUTION_HEADER}\n1,2,3\n`);
    expect(result.rows).toEqual([]);
    expect(result.errors).toEqual([{ line: 2, reason: "expected 12 columns, got 3" }]);
  });
});

describe("parseNorthDakotaExpenditureCsv", () => {
  const goodPrefix =
    "1020001030,Some PAC,,Expenditures,Itemized - greater than $200,,2026-03-19,10000.0000,Candidate";

  it("passes a well-formed 12-column row through unrecovered", () => {
    const result = parseNorthDakotaExpenditureCsv(`${EXPENDITURE_HEADER}\r\n${goodPrefix},Jonathan Sickler,152 Christian Dr  Grand Forks ND 58201  ,2026-05-01\r\n`);
    expect(result.errors).toEqual([]);
    expect(result.recoveredRowCount).toBe(0);
    expect(result.rows[0].recipientName).toBe("Jonathan Sickler");
    expect(result.rows[0].expenditureType).toBe("Itemized - greater than $200");
    expect(result.rows[0]).not.toHaveProperty("recipientAddress");
  });

  it("recovers the live 13-column shape (vendor address split by a comma)", () => {
    // Live fixture shape from the 2026 file: the address contains a comma, so
    // the row has 13 columns with the name still in column 10.
    const text = `${EXPENDITURE_HEADER}\r\n${goodPrefix},Kelly Armstrong,1515 Burnt Boat Drive Suite C, Box 112 Bismarck ND 58503  ,2026-05-04\r\n`;
    const result = parseNorthDakotaExpenditureCsv(text);
    expect(result.errors).toEqual([]);
    expect(result.recoveredRowCount).toBe(1);
    expect(result.rows[0].recipientName).toBe("Kelly Armstrong");
    expect(result.rows[0].amountCents).toBe(1_000_000);
    expect(result.rows[0].filedDate).toBe("2026-05-04");
    expect(result.rows[0].recovered).toBe(true);
  });

  it("fails rows beyond three extra columns or with a broken prefix", () => {
    const sixteen = `${goodPrefix},A,B,C,D,E,F,2026-04-06`;
    const brokenPrefix = `1,2,3,4,5,6,not-a-date,100.0000,X,Y,Z,2026-04-06`;
    const result = parseNorthDakotaExpenditureCsv(`${EXPENDITURE_HEADER}\n${sixteen}\n${brokenPrefix}\n`);
    expect(result.rows).toEqual([]);
    expect(result.errors).toEqual([
      { line: 2, reason: "expected 12 columns, got 16" },
      { line: 3, reason: "invalid date or amount" },
    ]);
  });
});

describe("parseNorthDakotaReportingScheduleCsv", () => {
  it("parses the live header (including the portal's 'Enddate' spelling)", () => {
    const text =
      "ElectionName,ReportingCycle,ReportingPeriodDescription,FormType,ReportType,BeginDate,Enddate,DueDate\r\n2026 Election - Statewide,2025 REPORTING CYCLE,2025 Year End Report,Campaign Financial Statement,Year End,2025-01-01,2025-12-31,2026-01-31\r\n";
    const result = parseNorthDakotaReportingScheduleCsv(text);
    expect(result.errors).toEqual([]);
    expect(result.rows[0]).toMatchObject({
      electionName: "2026 Election - Statewide",
      reportingCycle: "2025 REPORTING CYCLE",
      beginDate: "2025-01-01",
      endDate: "2025-12-31",
    });
  });
});

describe("parseNorthDakotaFiledReportCsv", () => {
  it("parses Original/Amended report rows", () => {
    const text =
      "RegistrantID,CommitteeName,CandidateName,ReportName,ReportType,StartDate,EndDate,DueDate,FiledDate,ReportVersion\r\n1020001001,Some PAC,,2026 Pre-Primary Report,Campaign Financial Statement,2026-01-01,2026-04-30,2026-05-08,2026-05-04,Original\r\n1020001001,Some PAC,,2026 Pre-Primary Report,Campaign Financial Statement,2026-01-01,2026-04-30,2026-05-08,2026-05-20,Amended\r\n";
    const result = parseNorthDakotaFiledReportCsv(text);
    expect(result.errors).toEqual([]);
    expect(result.rows.map((row) => row.reportVersion)).toEqual(["Original", "Amended"]);
    expect(result.rows[1].filedDate).toBe("2026-05-20");
  });

  it("reports width errors", () => {
    const text =
      "RegistrantID,CommitteeName,CandidateName,ReportName,ReportType,StartDate,EndDate,DueDate,FiledDate,ReportVersion\r\n1,2,3\r\n";
    expect(parseNorthDakotaFiledReportCsv(text).errors).toEqual([{ line: 2, reason: "expected 10 columns, got 3" }]);
  });
});
