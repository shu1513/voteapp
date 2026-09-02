import { describe, expect, it } from "vitest";

import type { NorthDakotaTransactionRow } from "../../../src/pipeline/northDakotaFinance/northDakotaCfrsClient.js";
import { parseNorthDakotaContributionCsv } from "../../../src/pipeline/northDakotaFinance/northDakotaCfrsCsv.js";
import {
  NORTH_DAKOTA_CONTRIBUTION_FILE_CATEGORIES,
  aggregateNorthDakotaDirectFinance,
  normalizeNorthDakotaOccupationLabel,
} from "../../../src/pipeline/northDakotaFinance/northDakotaDirectContributionAggregator.js";

const ENTITY = "1010001478";
const HEADER =
  "RegistrantID,CommitteeName,CandidateName,TransactionType,TransactionCategory,TransactionDate,TransactionAmount,ContributorPayeeType,ContributorPayeeName,ContributorAddress,EmployerName,FiledDate";
const WINDOW = { windowStart: "2025-01-01", windowEnd: "2026-12-31" };

function rows(lines: string[]) {
  const parsed = parseNorthDakotaContributionCsv(`${HEADER}\r\n${lines.join("\r\n")}\r\n`);
  expect(parsed.errors).toEqual([]);
  return parsed.rows;
}

function line(input: {
  entity?: string;
  category: string;
  date?: string;
  amount: string;
  type: string;
  name?: string;
}): string {
  return [
    input.entity ?? ENTITY,
    "Friends of Jane Doe",
    "Doe Jane",
    "Contributions",
    input.category,
    input.date ?? "2026-03-01",
    input.amount,
    input.type,
    input.name ?? "Roe Richard",
    "2 Oak St",
    "",
    "2026-05-01",
  ].join(",");
}

function api(overrides: Partial<NorthDakotaTransactionRow>): NorthDakotaTransactionRow {
  return {
    transactionID: 1,
    entityID: ENTITY,
    orgID: 1478,
    committeeName: "Friends of Jane Doe",
    candidateName: "Doe, Jane",
    transactionAmount: 500,
    transactionDate: "2026-03-01T00:00:00",
    filedDate: "2026-05-01T00:00:00",
    entityTypeDesc: "Individual",
    transactionCategoryDesc: "Monetary",
    transactionTypeDesc: "Contributions",
    transactionPurpose: null,
    contributorPayeeName: "Roe Richard",
    contributorPayeeID: 77,
    employerName: null,
    employerOccupation: null,
    transactionTotalYTD: null,
    amendedFlag: false,
    reportVersionID: "1",
    reportFileName: null,
    s3ReportFilePath: null,
    stanceDescription: null,
    candidateNameAssocation: null,
    electionYear: 2026,
    orgType: "Candidate/Candidate Committee",
    ...overrides,
  };
}

describe("aggregateNorthDakotaDirectFinance", () => {
  it("splits total receipts from donor money and buckets itemized individual gifts", () => {
    const result = aggregateNorthDakotaDirectFinance({
      entityId: ENTITY,
      window: WINDOW,
      apiRows: [],
      contributionRows: rows([
        line({ category: "Monetary", amount: "500.0000", type: "Individual", name: "Roe Richard" }),
        line({ category: "Monetary", amount: "250.0000", type: "Individual", name: "roe richard", date: "2025-06-01" }),
        line({ category: "Monetary", amount: "5000.0000", type: "Individual", name: "Poe Paula" }),
        line({ category: "In-Kind", amount: "120.5000", type: "Individual", name: "Poe Paula" }),
        line({ category: "Monetary", amount: "1000.0000", type: "Committee/PAC", name: "Prairie PAC" }),
        line({ category: "Monetary", amount: "300.0000", type: "Business or Organization", name: "Acme LLC" }),
        line({ category: "In-Kind", amount: "200.0000", type: "Party Committee", name: "District 1 GOP" }),
        line({ category: "Monetary", amount: "2000.0000", type: "Candidate", name: "Doe Jane" }),
        line({ category: "In-Kind", amount: "50.0000", type: "Self", name: "Doe Jane" }),
        line({ category: "Reimbursement of Expenditure", amount: "75.2500", type: "Individual", name: "Vendor Vic" }),
        line({ category: "Total - $200 or less", amount: "1234.5600", type: "", name: "" }),
        line({ category: "Total - $100 or less", amount: "10.0000", type: "", name: "" }),
        // Other committee and out-of-window rows never count.
        line({ entity: "1010009999", category: "Monetary", amount: "99999.0000", type: "Individual" }),
        line({ category: "Monetary", amount: "777.0000", type: "Individual", date: "2024-12-31" }),
        line({ category: "Monetary", amount: "888.0000", type: "Individual", date: "2027-01-01" }),
      ]),
    });
    expect(result).toMatchObject({
      // 500 + 250 + 5000 + 120.50 + 1000 + 300 + 200 + 2000 + 50 + 75.25 + 1234.56 + 10
      totalReceiptsCents: 1_074_031,
      // total minus Candidate/Self (2050) minus reimbursement (75.25)
      directContributionCents: 861_506,
      unitemizedCents: 124_456,
      selfFundingCents: 205_000,
      reimbursementCents: 7_525,
      contributionRowCount: 12,
      lumpRowCount: 2,
      unrecognizedContributionCategories: [],
      unrecognizedContributorTypes: [],
    });
    // Monetary individual rows only; the same donor under two spellings of
    // case counts once; In-Kind and non-individual rows stay out.
    expect(result.breakdowns).toEqual([
      { categoryType: "contribution_size", categoryName: "$5,000+", amount: 5000, contributorCount: 1 },
      { categoryType: "contribution_size", categoryName: "$500-$999", amount: 500, contributorCount: 1 },
      { categoryType: "contribution_size", categoryName: "$250-$499", amount: 250, contributorCount: 1 },
    ]);
  });

  it("surfaces vocabulary outside the pinned sets instead of guessing", () => {
    const result = aggregateNorthDakotaDirectFinance({
      entityId: ENTITY,
      window: WINDOW,
      apiRows: [],
      contributionRows: rows([
        line({ category: "Loan", amount: "500.0000", type: "Individual" }),
        line({ category: "Monetary", amount: "500.0000", type: "Anonymous" }),
        line({ category: "Monetary", amount: "100.0000", type: "Individual" }),
      ]),
    });
    expect(result.unrecognizedContributionCategories).toEqual(["Loan"]);
    expect(result.unrecognizedContributorTypes).toEqual(["Anonymous"]);
    // Unrecognized rows are excluded from every total until classified.
    expect(result.totalReceiptsCents).toBe(10_000);
    expect(result.directContributionCents).toBe(10_000);
    expect(result.contributionRowCount).toBe(3);
  });

  it("returns zeros and no breakdowns for a committee with no rows in the window", () => {
    const result = aggregateNorthDakotaDirectFinance({ entityId: ENTITY, window: WINDOW, contributionRows: [], apiRows: [] });
    expect(result).toEqual({
      totalReceiptsCents: 0,
      directContributionCents: 0,
      unitemizedCents: 0,
      selfFundingCents: 0,
      reimbursementCents: 0,
      contributionRowCount: 0,
      lumpRowCount: 0,
      unrecognizedContributionCategories: [],
      unrecognizedContributorTypes: [],
      occupation: { individualCents: 0, occupationCents: 0, donorCount: 0, occupationDonorCount: 0, displayGatePassed: false },
      breakdowns: [],
    });
  });

  it("publishes filed occupations from the API rows when the display gate passes", () => {
    const result = aggregateNorthDakotaDirectFinance({
      entityId: ENTITY,
      window: WINDOW,
      contributionRows: rows([line({ category: "Monetary", amount: "500.0000", type: "Individual" })]),
      apiRows: [
        api({ transactionID: 1, employerOccupation: "Healthcare/Medical" }),
        // Same donor again, label with stray whitespace: one donor, one label.
        api({ transactionID: 2, transactionAmount: 250, employerOccupation: " Healthcare/Medical " }),
        api({ transactionID: 3, contributorPayeeID: 78, contributorPayeeName: "Poe Paula", transactionAmount: 5000, employerOccupation: "Agriculture" }),
        api({ transactionID: 4, contributorPayeeID: 79, contributorPayeeName: "Quinn Quincy", transactionAmount: 250, transactionCategoryDesc: "In-Kind", employerOccupation: "Attorney/Legal" }),
        // Individuals without a usable label count toward the denominator only.
        api({ transactionID: 5, contributorPayeeID: 80, contributorPayeeName: "Nada Nan", transactionAmount: 1000 }),
        api({ transactionID: 6, contributorPayeeID: 81, contributorPayeeName: "Unk Uma", transactionAmount: 100, employerOccupation: "Unknown" }),
        // Non-individuals, lump rows, other committees and out-of-window rows never count, label or not.
        api({ transactionID: 7, entityTypeDesc: "Committee/PAC", contributorPayeeID: 90, contributorPayeeName: "Prairie PAC", transactionAmount: 1000, employerOccupation: "Business Owner" }),
        api({ transactionID: 8, entityTypeDesc: "Candidate", contributorPayeeID: 91, contributorPayeeName: "Doe Jane", transactionAmount: 3000, employerOccupation: "Attorney/Legal" }),
        api({ transactionID: 9, entityTypeDesc: null, transactionCategoryDesc: "Total - $200 or less", contributorPayeeID: null, contributorPayeeName: null, transactionAmount: 640, employerOccupation: "Retired" }),
        api({ transactionID: 10, entityID: "1010009999", contributorPayeeID: 92, transactionAmount: 99_999, employerOccupation: "Retired" }),
        api({ transactionID: 11, transactionDate: "2024-12-31T00:00:00", contributorPayeeID: 93, transactionAmount: 777, employerOccupation: "Retired" }),
      ],
    });
    expect(result.occupation).toEqual({
      // 500 + 250 + 5000 + 250 + 1000 + 100
      individualCents: 710_000,
      // minus the unlabeled 1000 and the "Unknown" 100
      occupationCents: 600_000,
      donorCount: 5,
      occupationDonorCount: 3,
      displayGatePassed: true,
    });
    expect(result.breakdowns).toEqual([
      { categoryType: "occupation", categoryName: "Agriculture", amount: 5000, contributorCount: 1 },
      { categoryType: "occupation", categoryName: "Healthcare/Medical", amount: 750, contributorCount: 1 },
      { categoryType: "occupation", categoryName: "Attorney/Legal", amount: 250, contributorCount: 1 },
      { categoryType: "contribution_size", categoryName: "$500-$999", amount: 500, contributorCount: 1 },
    ]);
  });

  it("keeps totals and size buckets but publishes no occupation rows below the display gate", () => {
    const csv = rows([line({ category: "Monetary", amount: "500.0000", type: "Individual" })]);
    // Two labeled donors: fewer than three.
    const fewDonors = aggregateNorthDakotaDirectFinance({
      entityId: ENTITY,
      window: WINDOW,
      contributionRows: csv,
      apiRows: [
        api({ transactionID: 1, employerOccupation: "Healthcare/Medical" }),
        api({ transactionID: 2, contributorPayeeID: 78, contributorPayeeName: "Poe Paula", transactionAmount: 5000, employerOccupation: "Agriculture" }),
      ],
    });
    expect(fewDonors.occupation).toEqual({ individualCents: 550_000, occupationCents: 550_000, donorCount: 2, occupationDonorCount: 2, displayGatePassed: false });
    expect(fewDonors.directContributionCents).toBe(50_000);
    expect(fewDonors.breakdowns).toEqual([{ categoryType: "contribution_size", categoryName: "$500-$999", amount: 500, contributorCount: 1 }]);
    // Three labeled donors, but their dollars are under a fifth of individual money.
    const thinShare = aggregateNorthDakotaDirectFinance({
      entityId: ENTITY,
      window: WINDOW,
      contributionRows: csv,
      apiRows: [
        api({ transactionID: 1, transactionAmount: 100, employerOccupation: "Retired" }),
        api({ transactionID: 2, contributorPayeeID: 78, contributorPayeeName: "Poe Paula", transactionAmount: 100, employerOccupation: "Retired" }),
        api({ transactionID: 3, contributorPayeeID: 79, contributorPayeeName: "Quinn Quincy", transactionAmount: 100, employerOccupation: "Retired" }),
        api({ transactionID: 4, contributorPayeeID: 80, contributorPayeeName: "Big Bill", transactionAmount: 5000 }),
      ],
    });
    expect(thinShare.occupation).toEqual({ individualCents: 530_000, occupationCents: 30_000, donorCount: 4, occupationDonorCount: 3, displayGatePassed: false });
    expect(thinShare.breakdowns.map((row) => row.categoryType)).toEqual(["contribution_size"]);
  });

  it("normalizes occupation labels: whitespace collapsed, blank and Unknown dropped, nothing inferred", () => {
    expect(normalizeNorthDakotaOccupationLabel("  Attorney/Legal  ")).toBe("Attorney/Legal");
    expect(normalizeNorthDakotaOccupationLabel("Business\t Owner")).toBe("Business Owner");
    expect(normalizeNorthDakotaOccupationLabel("Unknown")).toBeNull();
    expect(normalizeNorthDakotaOccupationLabel("UNKNOWN")).toBeNull();
    expect(normalizeNorthDakotaOccupationLabel("   ")).toBeNull();
    expect(normalizeNorthDakotaOccupationLabel(null)).toBeNull();
  });

  it("pins the live category vocabulary", () => {
    expect([...NORTH_DAKOTA_CONTRIBUTION_FILE_CATEGORIES].sort()).toEqual([
      "In-Kind",
      "Monetary",
      "Reimbursement of Expenditure",
      "Total - $100 or less",
      "Total - $200 or less",
    ]);
  });
});
