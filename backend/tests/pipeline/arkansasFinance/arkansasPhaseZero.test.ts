import { describe, expect, it } from "vitest";

import type {
  ArkansasFiledReportRow,
  ArkansasFilerRegistrationRow,
  ArkansasTransactionRow,
} from "../../../src/pipeline/arkansasFinance/arkansasCfisClient.js";
import type { ArkansasReceiptCsvRow } from "../../../src/pipeline/arkansasFinance/arkansasCfisCsv.js";
import {
  createArkansasExpenditureCsvAccumulator,
  createArkansasReceiptCsvAccumulator,
  findArkansasMultiCycleCandidates,
  reconcileArkansasRegistrationTotals,
  summarizeArkansasFiledReports,
  summarizeArkansasOfficeVocabulary,
  summarizeArkansasTransactionRows,
} from "../../../src/pipeline/arkansasFinance/arkansasPhaseZero.js";

const GUID_A = "689c554c-5120-46a4-828e-6798f3298f22";
const GUID_B = "4b87047f-4fdb-467f-b69b-962eadd81745";

function receiptRow(overrides: Partial<ArkansasReceiptCsvRow>): ArkansasReceiptCsvRow {
  return {
    "Filing Entity ID": "1004",
    "Entity Name": "Sanders, Sarah",
    FilerType: "Candidate",
    "Transaction Type": "Contribution",
    "Transaction Sub Type": "Itemized Monetary",
    "Funding Source / Loan Source Type": "Individual",
    "Source Name": "Walton, Thomas",
    "Source Address": "Bentonville, AR",
    "Employer Name": "Runway Group LLC",
    Occupation: "Financial / Investment",
    "Occupation Other": "",
    "Transaction Date": "07/31/2026",
    "Transaction Amount": "$100.00",
    "Transaction Description": "",
    "Transaction ID": "1",
    "Election Type": "General",
    "Election Year": "2026",
    "Guarantor Name": "",
    "Guarantor Address": "",
    "Report Filed Date": "08/20/2026",
    "Report Name": "2026 July Monthly Report",
    Amended: "N",
    ...overrides,
  };
}

function registration(overrides: Partial<ArkansasFilerRegistrationRow>): ArkansasFilerRegistrationRow {
  return {
    registrationGuid: GUID_A,
    filerEntityId: 1004,
    filerEntityVersionId: 1,
    filerType: "Candidate",
    filerTypeCode: "CAN",
    filerStatus: "Active",
    firstName: "Sarah",
    lastName: "Sanders",
    suffix: null,
    committeeName: null,
    office: "Governor",
    officeDistrictName: null,
    jurisdictionName: "Arkansas",
    politicalParty: "Republican Party",
    electionYear: 2026,
    filingYear: 2026,
    isPaperFiler: false,
    totalRaised: 0,
    totalSpent: 0,
    balanceOfFunds: 0,
    ...overrides,
  };
}

describe("createArkansasReceiptCsvAccumulator", () => {
  it("aggregates gold-entity detail and occupation statistics", () => {
    const accumulator = createArkansasReceiptCsvAccumulator(new Set([1004]));
    accumulator.add(receiptRow({}));
    accumulator.add(
      receiptRow({ "Transaction Amount": "$50.00", Occupation: "", "Occupation Other": "Farmer" })
    );
    accumulator.add(
      receiptRow({
        "Filing Entity ID": "9999",
        "Transaction Amount": "$5.00",
        Occupation: "",
        "Occupation Other": "",
        "Employer Name": "",
      })
    );
    const summary = accumulator.result();
    expect(summary.rowCount).toBe(3);
    expect(summary.entities["1004"]!.total).toEqual({ rowCount: 2, amountCents: 15_000 });
    expect(summary.entities["9999"]).toBeUndefined();
    expect(summary.occupation.individualRowCount).toBe(3);
    expect(summary.occupation.occupationFilledCount).toBe(2);
    expect(summary.occupation.occupationFromOtherCount).toBe(1);
    expect(summary.occupation.itemizedSmallRowCount).toBe(3);
    expect(summary.occupation.itemizedSmallWithOccupationCount).toBe(2);
    expect(summary.entities["1004"]!.byElectionYear).toEqual({ "2026": { rowCount: 2, amountCents: 15_000 } });
  });

  it("tracks candidate-filer occupation coverage separately from PAC-dominated totals", () => {
    const accumulator = createArkansasReceiptCsvAccumulator(new Set());
    accumulator.add(receiptRow({}));
    accumulator.add(
      receiptRow({ "Filing Entity ID": "559", FilerType: "Political Action Committee", Occupation: "", "Occupation Other": "" })
    );
    accumulator.add(receiptRow({ "Filing Entity ID": "560", FilerType: "Political Action Committee" }));
    // Candidate self-loans are individual-source rows but not contributions.
    accumulator.add(receiptRow({ "Transaction Type": "Loan", "Transaction Sub Type": "" }));
    const summary = accumulator.result();
    expect(summary.occupation).toMatchObject({ individualRowCount: 3, occupationFilledCount: 2 });
    expect(summary.candidateOccupation).toMatchObject({ individualRowCount: 1, occupationFilledCount: 1 });
  });

  it("discovers candidate filers with amended rows", () => {
    const accumulator = createArkansasReceiptCsvAccumulator(new Set());
    accumulator.add(receiptRow({ "Filing Entity ID": "222", Amended: "Y" }));
    accumulator.add(receiptRow({ "Filing Entity ID": "333", FilerType: "Political Action Committee", Amended: "Y" }));
    const summary = accumulator.result();
    expect(summary.amendedRowCount).toBe(2);
    expect(summary.candidateEntitiesWithAmendedRows).toEqual([222]);
  });
});

describe("reconcileArkansasRegistrationTotals", () => {
  it("finds the exact raised and spent formulas", () => {
    const receipts = createArkansasReceiptCsvAccumulator(new Set([1004]));
    receipts.add(receiptRow({ "Transaction Amount": "$300.00" }));
    receipts.add(
      receiptRow({ "Transaction Sub Type": "Non-Itemized Monetary", "Transaction Amount": "$50.00" })
    );
    receipts.add(
      receiptRow({ "Transaction Sub Type": "Itemized Nonmoney", "Transaction Amount": "$25.00" })
    );
    receipts.add(receiptRow({ "Transaction Type": "Loan", "Transaction Sub Type": "", "Transaction Amount": "$1,000.00" }));
    const expenditures = createArkansasExpenditureCsvAccumulator(new Set([1004]));

    const result = reconcileArkansasRegistrationTotals({
      registration: registration({ totalRaised: 350, totalSpent: 0 }),
      receiptDetail: receipts.result().entities["1004"],
      expenditureDetail: expenditures.result().entities["1004"],
    });
    expect(result.raisedExactFormulas).toContain("monetary");
    expect(result.raisedExactFormulas).not.toContain("monetary_plus_loans");
    expect(result.components.loanCents).toBe(100_000);
    expect(result.spentExactFormulas).toContain("expenditure");
  });
});

describe("summarizeArkansasTransactionRows", () => {
  const row = (overrides: Partial<ArkansasTransactionRow>): ArkansasTransactionRow => ({
    guid: "2d22d67f-6a58-414a-8e7f-e9c2a1b6210b",
    filerName: "Sanders, Sarah",
    filerRegistrationGuid: GUID_A,
    transactionAmount: 35,
    transactionDate: "07/31/2026",
    sourceName: "Walton, Thomas",
    employerName: null,
    occupation: null,
    transactionSource: "Individual",
    reportName: "2026 July Monthly Report",
    transactionSubTypeDescription: "Itemized Monetary",
    transactionCategory: null,
    hasChild: false,
    ...overrides,
  });

  it("sums rows and counts duplicates and children", () => {
    const summary = summarizeArkansasTransactionRows(GUID_A, [
      row({}),
      row({ guid: "3d22d67f-6a58-414a-8e7f-e9c2a1b6210b", transactionAmount: 15, hasChild: true }),
      row({}),
    ]);
    expect(summary).toEqual({
      registrationGuid: GUID_A,
      rowCount: 3,
      amountCents: 8_500,
      hasChildRowCount: 1,
      duplicateGuidCount: 1,
    });
  });

  it("fails closed on rows from another registration", () => {
    expect(() => summarizeArkansasTransactionRows(GUID_A, [row({ filerRegistrationGuid: GUID_B })])).toThrow(
      /not exact/
    );
  });
});

describe("summarizeArkansasFiledReports", () => {
  it("scopes to the registration and counts version lineage", () => {
    const report = (overrides: Partial<ArkansasFiledReportRow>): ArkansasFiledReportRow => ({
      reportName: "2026 Q1 Quarterly Report",
      reportType: "Scheduled Financial Report",
      reportStatus: "Original",
      reportVersion: "Original",
      filerReportVersionId: 1,
      filerReportGuid: "e616bad7-97ee-4552-8f12-69914079be34",
      filerRegistrationGuid: GUID_A,
      filerEntityId: 1004,
      filerName: "Sanders, Sarah",
      filerType: "Candidate",
      officeName: "Governor",
      jurisdictionName: "Arkansas",
      startDate: "01/01/2026",
      endDate: "03/31/2026",
      dueDate: "04/15/2026",
      filedDate: "04/10/2026",
      isPaperFile: false,
      ...overrides,
    });
    const summary = summarizeArkansasFiledReports(GUID_A, [
      report({}),
      report({ reportName: "2026 Q2 Quarterly Report", reportVersion: "Amended", filerReportVersionId: 2 }),
      report({ filerRegistrationGuid: GUID_B }),
    ]);
    expect(summary.reportCount).toBe(2);
    expect(summary.versionCounts).toEqual({ Original: 1, Amended: 1 });
    expect(summary.reportsWithVersionAboveOne).toBe(1);
  });
});

describe("findArkansasMultiCycleCandidates", () => {
  it("returns candidate entities with more than one cycle", () => {
    const rows = [
      registration({}),
      registration({ registrationGuid: GUID_B, electionYear: 2024, filerEntityVersionId: 2 }),
      registration({ filerEntityId: 2, registrationGuid: "11111111-2222-4333-8444-555555555555" }),
      registration({ filerEntityId: 3, filerTypeCode: "SFIFILER", filerType: "SFI Filer" }),
    ];
    const result = findArkansasMultiCycleCandidates(rows);
    expect(result).toHaveLength(1);
    expect(result[0]!.filingEntityId).toBe(1004);
    expect(result[0]!.registrations.map((entry) => entry.electionYear)).toEqual([2024, 2026]);
  });
});

describe("summarizeArkansasOfficeVocabulary", () => {
  it("reports missing required offices", () => {
    const summary = summarizeArkansasOfficeVocabulary(
      [
        { value: "1", name: "Governor" },
        { value: "2", name: "State Senate" },
      ],
      ["Governor", "State Senate", "Supreme Court"]
    );
    expect(summary.officeCount).toBe(2);
    expect(summary.missingRequiredOffices).toEqual(["Supreme Court"]);
  });
});

describe("createArkansasExpenditureCsvAccumulator IE scan", () => {
  it("classifies IEF description stance patterns", () => {
    const accumulator = createArkansasExpenditureCsvAccumulator(new Set());
    const base = {
      "Filing Entity ID": "555",
      "Entity Name": "NRA Political Victory Fund - IE Committee",
      FilerType: "Independent Expenditure Filer",
      "Transaction Type": "Expenditure",
      "Transaction Sub Type": "Itemized Monetary",
      "Payee Type": "Business/Organization/Unlisted PAC",
      "Payee Name": "i360",
      "Payee Address": "",
      "Transaction Date": "03/01/2026",
      "Transaction Amount": "$134.85",
      "Transaction Description": "SMS MESSAGING IN SUPPORT OF BRANDON ACHOR, AR-SD-13",
      "Transaction ID": "1",
      "Transaction Category": "Other(list)",
      "Transaction Category Others": "",
      "Election Type": "Primary",
      "Election Year": "2026",
      "Report Filed Date": "05/14/2026",
      "Report Name": "2026 Q1 Quarterly Report",
      Amended: "N",
    };
    accumulator.add(base);
    accumulator.add({ ...base, "Transaction Description": "MAILER IN OPPOSITION TO JANE DOE, AR-HD-01" });
    accumulator.add({ ...base, "Transaction Description": "Bank Fee" });
    const summary = accumulator.result();
    expect(summary.independentExpenditureFilers).toEqual({
      rowCount: 3,
      amountCents: 3 * 13_485,
      distinctEntityCount: 1,
      supportPatternRowCount: 1,
      opposePatternRowCount: 1,
      noPatternRowCount: 1,
    });
  });
});
