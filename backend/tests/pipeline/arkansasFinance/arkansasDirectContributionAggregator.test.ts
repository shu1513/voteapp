import { describe, expect, it } from "vitest";

import type {
  ArkansasFilerRegistrationRow,
  ArkansasTransactionRow,
} from "../../../src/pipeline/arkansasFinance/arkansasCfisClient.js";
import {
  aggregateArkansasDirectContributions,
  arkansasOccupationLabel,
} from "../../../src/pipeline/arkansasFinance/arkansasDirectContributionAggregator.js";

const REGISTRATION_GUID = "7c482853-2ec8-4435-94ea-ae709b14e7ed";
const SOURCE_URL = "https://ethics-disclosures.sos.arkansas.gov/";

function registration(overrides: Partial<ArkansasFilerRegistrationRow> = {}): ArkansasFilerRegistrationRow {
  return {
    registrationGuid: REGISTRATION_GUID,
    filerEntityId: 7968,
    filerEntityVersionId: 1,
    filerType: "Candidate",
    filerTypeCode: "CAN",
    filerStatus: "Active",
    filerName: "Doe, Jane A.",
    firstName: "Jane",
    lastName: "Doe",
    suffix: null,
    committeeName: null,
    office: "State Representative",
    officeDistrictName: "District 59",
    jurisdictionName: "Arkansas",
    politicalParty: "Republican",
    electionYear: 2026,
    filingYear: 2026,
    isPaperFiler: false,
    totalRaised: 1_560.5,
    totalSpent: 320.25,
    balanceOfFunds: -55.75,
    ...overrides,
  };
}

let nextGuid = 0;
function receipt(overrides: Partial<ArkansasTransactionRow> = {}): ArkansasTransactionRow {
  nextGuid += 1;
  return {
    guid: `00000000-0000-4000-8000-${String(nextGuid).padStart(12, "0")}`,
    filerName: "Doe, Jane A.",
    filerRegistrationGuid: REGISTRATION_GUID,
    transactionAmount: 100,
    transactionDate: "03/01/2026",
    sourceName: "Ann Early",
    employerName: "Acme Farms",
    occupation: "Agriculture",
    transactionSource: "Individual",
    reportName: "Q1 2026",
    transactionSubTypeDescription: "Itemized Monetary",
    transactionCategory: null,
    hasChild: false,
    ...overrides,
  };
}

// Individual 100 + 250 (same donor, Agriculture) + 700 (Other-wrapped) +
// 75 - 25 (blank occupation, one returned) + PAC 300 + Candidate 150 +
// non-itemized lump 10.50 + interest 0.001 (sub-cent noise, rounds to 0)
// = 1,560.50; the in-kind row, the lump, the interest, and the negative row
// sit outside the buckets.
function reconciledRows(): ArkansasTransactionRow[] {
  return [
    receipt({ transactionAmount: 100, sourceName: "Ann Early" }),
    receipt({ transactionAmount: 250, sourceName: "Ann  Early" }),
    receipt({ transactionAmount: 700, sourceName: "Bo Ridge", occupation: "Other(Ferry pilot)" }),
    receipt({ transactionAmount: 75, sourceName: "Cy Lake", occupation: "" }),
    receipt({ transactionAmount: -25, sourceName: "Cy Lake", occupation: "" }),
    receipt({
      transactionAmount: 300,
      sourceName: "Growers PAC",
      occupation: null,
      employerName: null,
      transactionSource: "Political Action Committee",
    }),
    receipt({ transactionAmount: 150, sourceName: "Jane Doe", occupation: "Candidate", transactionSource: "Candidate" }),
    receipt({
      transactionAmount: 10.5,
      sourceName: null,
      occupation: null,
      employerName: null,
      transactionSource: null,
      transactionSubTypeDescription: "Non-Itemized Monetary",
    }),
    receipt({ transactionAmount: 400, sourceName: "Di Hall", transactionSubTypeDescription: "Itemized Nonmoney" }),
    receipt({
      transactionAmount: 0.001,
      sourceName: null,
      occupation: null,
      employerName: null,
      transactionSource: null,
      transactionSubTypeDescription: "Interest",
    }),
  ];
}

describe("aggregateArkansasDirectContributions", () => {
  it("publishes registration totals and receipt-derived breakdowns when the monetary sum reconciles", () => {
    const result = aggregateArkansasDirectContributions({
      registration: registration(),
      receiptRows: reconciledRows(),
      sourceUrl: SOURCE_URL,
    });

    expect(result.summary).toEqual({
      totalReceipts: 1_560.5,
      directContributionTotal: 1_560.5,
      totalDisbursements: 320.25,
      cashOnHand: -55.75,
    });
    expect(result.reconciliation).toEqual({
      status: "reconciled",
      registrationRaisedCents: 156_050,
      receiptMonetaryCents: 156_050,
      deltaCents: 0,
    });
    expect(result.rowCounts).toEqual({
      total: 10,
      itemizedMonetary: 7,
      nonItemizedMonetary: 1,
      nonmoney: 1,
      interest: 1,
      nonPositive: 1,
      subCentAmount: 1,
      hasChild: 0,
    });
    expect(result.diagnostics).toEqual([]);
    expect(result.directBreakdowns).toEqual([
      { categoryType: "occupation", categoryName: "Ferry pilot", amount: 700, contributorCount: 1, sourceUrl: SOURCE_URL },
      { categoryType: "occupation", categoryName: "Agriculture", amount: 350, contributorCount: 1, sourceUrl: SOURCE_URL },
      { categoryType: "occupation", categoryName: "Unknown", amount: 75, contributorCount: 1, sourceUrl: SOURCE_URL },
      { categoryType: "contribution_size", categoryName: "$500-$999", amount: 700, contributorCount: 1, sourceUrl: SOURCE_URL },
      { categoryType: "contribution_size", categoryName: "$250-$499", amount: 550, contributorCount: 2, sourceUrl: SOURCE_URL },
      { categoryType: "contribution_size", categoryName: "$100-$249", amount: 250, contributorCount: 2, sourceUrl: SOURCE_URL },
      { categoryType: "contribution_size", categoryName: "$1-$99", amount: 75, contributorCount: 1, sourceUrl: SOURCE_URL },
    ]);
  });

  it("caps occupation rows but never size buckets", () => {
    const result = aggregateArkansasDirectContributions({
      registration: registration({ totalRaised: 5_100 }),
      receiptRows: [
        receipt({ transactionAmount: 5_000, sourceName: "A", occupation: "Attorney / Legal" }),
        receipt({ transactionAmount: 60, sourceName: "B", occupation: "Retired" }),
        receipt({ transactionAmount: 40, sourceName: "C", occupation: "Agriculture" }),
      ],
      sourceUrl: null,
      maxOccupationBreakdowns: 1,
    });
    expect(result.directBreakdowns.map((row) => `${row.categoryType}:${row.categoryName}`)).toEqual([
      "occupation:Attorney / Legal",
      "contribution_size:$5,000+",
      "contribution_size:$1-$99",
    ]);
  });

  it("counts interest toward the registration total and rounds sub-cent amounts", () => {
    const result = aggregateArkansasDirectContributions({
      registration: registration({ totalRaised: 1_505.33 }),
      receiptRows: [
        receipt({ transactionAmount: 1_500.001, sourceName: "A" }),
        receipt({ transactionAmount: 5.33, sourceName: null, transactionSource: null, occupation: null, transactionSubTypeDescription: "Interest" }),
      ],
      sourceUrl: null,
    });
    expect(result.reconciliation).toMatchObject({ status: "reconciled", receiptMonetaryCents: 150_533 });
    expect(result.rowCounts).toMatchObject({ interest: 1, subCentAmount: 1, itemizedMonetary: 1 });
    expect(result.directBreakdowns.map((row) => [row.categoryType, row.categoryName, row.amount])).toEqual([
      ["occupation", "Agriculture", 1_500],
      ["contribution_size", "$1,000-$4,999", 1_500],
    ]);
  });

  it("withholds breakdowns and reports the delta when receipts do not reconcile", () => {
    const rows = reconciledRows();
    rows.push(receipt({ transactionAmount: 118, sourceName: "Superseded Version" }));
    const result = aggregateArkansasDirectContributions({ registration: registration(), receiptRows: rows, sourceUrl: SOURCE_URL });

    expect(result.reconciliation).toEqual({
      status: "unreconciled",
      registrationRaisedCents: 156_050,
      receiptMonetaryCents: 167_850,
      deltaCents: 11_800,
    });
    expect(result.directBreakdowns).toEqual([]);
    expect(result.summary.totalReceipts).toBe(1_560.5);
    expect(result.diagnostics).toEqual([
      `Arkansas finance receipts for registration ${REGISTRATION_GUID} sum to 1678.50 against the registration total 1560.50 (delta 118.00); breakdowns withheld`,
    ]);
  });

  it("fails closed on foreign, duplicate, or unknown receipt rows", () => {
    const base = { registration: registration({ totalRaised: 100 }), sourceUrl: null };
    expect(() =>
      aggregateArkansasDirectContributions({
        ...base,
        receiptRows: [receipt({ filerRegistrationGuid: "01e37c68-aa22-4559-a14d-f23a617a415b" })],
      })
    ).toThrow(/belongs to registration/);
    const duplicate = receipt();
    expect(() =>
      aggregateArkansasDirectContributions({ ...base, receiptRows: [duplicate, { ...duplicate }] })
    ).toThrow(/appears twice/);
    expect(() =>
      aggregateArkansasDirectContributions({
        ...base,
        receiptRows: [receipt({ transactionSubTypeDescription: "Loan" })],
      })
    ).toThrow(/unknown sub type: Loan/);
    expect(() =>
      aggregateArkansasDirectContributions({
        ...base,
        receiptRows: [receipt({ transactionAmount: Number.NaN })],
      })
    ).toThrow(/not a money amount/);
    expect(() =>
      aggregateArkansasDirectContributions({ ...base, receiptRows: [], maxOccupationBreakdowns: 0 })
    ).toThrow(/maxOccupationBreakdowns/);
  });

  it("treats an empty pull against a zero registration as reconciled with no breakdowns", () => {
    const result = aggregateArkansasDirectContributions({
      registration: registration({ totalRaised: 0, totalSpent: 0, balanceOfFunds: 0 }),
      receiptRows: [],
      sourceUrl: null,
    });
    expect(result.reconciliation.status).toBe("reconciled");
    expect(result.directBreakdowns).toEqual([]);
    expect(result.summary).toEqual({ totalReceipts: 0, directContributionTotal: 0, totalDisbursements: 0, cashOnHand: 0 });
  });
});

describe("arkansasOccupationLabel", () => {
  it("unwraps the Other wrapper and collapses placeholders", () => {
    expect(arkansasOccupationLabel("Healthcare / Medical")).toBe("Healthcare / Medical");
    expect(arkansasOccupationLabel("Other( Ferry   pilot )")).toBe("Ferry pilot");
    expect(arkansasOccupationLabel("Other()")).toBe("Unknown");
    expect(arkansasOccupationLabel("Other")).toBe("Unknown");
    expect(arkansasOccupationLabel("N/A")).toBe("Unknown");
    expect(arkansasOccupationLabel(null)).toBe("Unknown");
  });
});
