import { describe, expect, it } from "vitest";

import type {
  MontanaCersDetailRow,
  MontanaCersExportRow,
  MontanaCersReportDetailArtifact,
  MontanaCersReportInventoryRow,
} from "../../../src/pipeline/montanaFinance/montanaCersParsers.js";
import { MONTANA_CERS_ALL_CANDIDATE_DETAIL_LISTS } from "../../../src/pipeline/montanaFinance/montanaCersParsers.js";
import {
  aggregateMontanaDirectFinance,
  MONTANA_UNKNOWN_OCCUPATION_LABEL,
} from "../../../src/pipeline/montanaFinance/montanaDirectFinanceAggregator.js";

function detailRow(overrides: Partial<MontanaCersDetailRow>): MontanaCersDetailRow {
  return {
    amountTypeDescr: "Primary",
    cashAmtCents: 0,
    inKindAmtCents: 0,
    totalAmtCents: 0,
    debtAmtCents: 0,
    entityName: "Doe, Jane",
    occupationDescr: null,
    employerDescr: null,
    datePaid: null,
    lineItemCompositeDescr: null,
    purposeDescr: null,
    electioneeringInd: "N",
    candidateContrInd: "N",
    ...overrides,
  };
}

function emptyArtifact(reportId: number): MontanaCersReportDetailArtifact {
  const lists = Object.fromEntries(
    MONTANA_CERS_ALL_CANDIDATE_DETAIL_LISTS.map((name) => [name, [] as MontanaCersDetailRow[]])
  ) as MontanaCersReportDetailArtifact["lists"];
  return { reportId, lists };
}

function inventoryRow(overrides: Partial<MontanaCersReportInventoryRow>): MontanaCersReportInventoryRow {
  return {
    reportId: 1,
    entitySubId: 21020,
    formTypeCode: "C5",
    formTypeDescr: null,
    fromDateStr: "01/01/2026",
    toDateStr: "03/15/2026",
    reportTypeDescr: "Periodic",
    statusCode: "FILED",
    statusDescr: "Filed",
    primCashBegCents: 0,
    genCashBegCents: 0,
    receivedDate: 1_000,
    amendedDate: null,
    ...overrides,
  };
}

function csvRow(overrides: Partial<MontanaCersExportRow>): MontanaCersExportRow {
  return {
    candidateId: 21020,
    candidateName: "Bedey, David F.",
    reportingDateRange: "01/01/2026 - 03/15/2026",
    entityName: "Doe, Jane",
    occupation: "Retired",
    employer: "Retired",
    datePaid: "01/01/2026",
    purpose: null,
    description: null,
    lineItem: "Individual Contributions",
    amountCents: 0,
    electionType: "Primary",
    amountSubtype: "Cash",
    officeTitle: "Senate District No. 43",
    ...overrides,
  };
}

// One canonical report: begin 0, individual cash 100_00 + in-kind 50_00,
// committee 25_00 cash, loan proceeds 200_00 cash, spending 80_00, debt
// repayment (payment list) 30_00; next-begin leaves a $2 unitemized lump.
function fixture() {
  const artifact = emptyArtifact(1);
  artifact.lists.individual = [
    detailRow({ cashAmtCents: 10_000, totalAmtCents: 10_000, entityName: "Doe, Jane" }),
    detailRow({ inKindAmtCents: 5_000, totalAmtCents: 5_000, entityName: "Roe, Rick" }),
  ];
  artifact.lists.committee = [detailRow({ cashAmtCents: 2_500, totalAmtCents: 2_500, entityName: "Good PAC" })];
  artifact.lists.loan = [detailRow({ cashAmtCents: 20_000, totalAmtCents: 20_000 })];
  artifact.lists.expendOther = [detailRow({ cashAmtCents: 8_000, totalAmtCents: 8_000, entityName: "Print Shop" })];
  artifact.lists.payment = [detailRow({ cashAmtCents: 3_000, totalAmtCents: 3_000 })];
  const first = inventoryRow({ reportId: 1 });
  // cash: 0 + (10_000 + 2_500 + 20_000) - (8_000 + 3_000) = 21_500; +200 lump.
  const second = inventoryRow({ reportId: 2, primCashBegCents: 21_700, receivedDate: 2_000 });
  const contributionRows = [
    csvRow({ amountCents: 10_000, entityName: "Doe, Jane", occupation: "Retired" }),
    csvRow({ amountCents: 5_000, entityName: "Roe, Rick", occupation: null, amountSubtype: "In-Kind" }),
    csvRow({ amountCents: 2_500, entityName: "Good PAC", lineItem: "Independent Committee Contributions" }),
    csvRow({ amountCents: 20_000, lineItem: "Loans" }),
  ];
  const expenditureRows = [
    csvRow({ amountCents: 8_000, entityName: "Print Shop", lineItem: "All Other Expenditures" }),
  ];
  return {
    canonicalReports: [
      { inventory: first, artifact },
      { inventory: second, artifact: emptyArtifact(2) },
    ],
    contributionRows,
    expenditureRows,
    sourceUrl: "https://cers-ext.mt.gov/CampaignTracker/dashboard",
  };
}

describe("aggregateMontanaDirectFinance", () => {
  it("aggregates chain-verified totals with the derived lump and both breakdowns", () => {
    const result = aggregateMontanaDirectFinance(fixture());
    expect(result.chain.ok).toBe(true);
    // 100 + 50 + 25 direct + $2 derived lump; loans and repayments excluded.
    expect(result.directContributionTotal).toBe(177);
    expect(result.totalDisbursements).toBe(80);
    // Last report: begin 217 + 0 - 0.
    expect(result.cashOnHand).toBe(217);
    expect(result.derivedUnitemizedTotal).toBe(2);
    expect(result.debtRepaymentTotal).toBe(30);
    expect(result.loanProceedsTotal).toBe(200);
    const occupations = result.directBreakdowns.filter((row) => row.categoryType === "occupation");
    expect(occupations).toEqual([
      expect.objectContaining({ categoryName: "Retired", amount: 100, contributorCount: 1 }),
      // In-kind counts as dollars; missing occupation files as Unknown.
      expect.objectContaining({ categoryName: MONTANA_UNKNOWN_OCCUPATION_LABEL, amount: 50 }),
    ]);
    const sizes = result.directBreakdowns.filter((row) => row.categoryType === "contribution_size");
    expect(sizes).toEqual([
      expect.objectContaining({ categoryName: "$100-$249", amount: 100 }),
      expect.objectContaining({ categoryName: "$1-$99", amount: 50 }),
    ]);
  });

  it("fails closed when the cash-begin chain does not reconcile", () => {
    const input = fixture();
    input.canonicalReports[1]!.inventory.primCashBegCents = 5_000; // negative residual
    expect(() => aggregateMontanaDirectFinance(input)).toThrow("cash-begin chain failed");
  });

  it("fails closed when the CSV and JSON surfaces disagree", () => {
    const individualDrift = fixture();
    individualDrift.contributionRows[0]!.amountCents = 9_999;
    // No sub-$50 JSON rows exist, so even a one-cent CSV shortfall is drift.
    expect(() => aggregateMontanaDirectFinance(individualDrift)).toThrow("beyond the itemization threshold");

    // The CSV must never exceed the JSON itemization.
    const csvExcess = fixture();
    csvExcess.contributionRows[0]!.amountCents = 10_001;
    expect(() => aggregateMontanaDirectFinance(csvExcess)).toThrow("beyond the itemization threshold");

    const committeeDrift = fixture();
    committeeDrift.contributionRows[2]!.amountCents = 2_400;
    expect(() => aggregateMontanaDirectFinance(committeeDrift)).toThrow("committee-contribution totals disagree");

    const expenditureDrift = fixture();
    expenditureDrift.expenditureRows.push(csvRow({ amountCents: 1, lineItem: "All Other Expenditures" }));
    expect(() => aggregateMontanaDirectFinance(expenditureDrift)).toThrow("expenditure totals disagree");
  });

  it("tolerates a CSV shortfall explained by sub-threshold rows (Eddy shape)", () => {
    const input = fixture();
    // The JSON itemizes a $20 entry the public CSV drops entirely.
    input.canonicalReports[0]!.artifact.lists.individual.push(
      detailRow({ cashAmtCents: 2_000, totalAmtCents: 2_000, entityName: "Small, Donor" })
    );
    // Keep the chain closing: next begin absorbs the extra $20.
    input.canonicalReports[1]!.inventory.primCashBegCents! += 2_000;
    const result = aggregateMontanaDirectFinance(input);
    // The JSON remains the total's source; the CSV shortfall is accepted.
    expect(result.directContributionTotal).toBe(197);

    // A "Less Than $35" roll-up row counts toward the CSV side.
    input.contributionRows.push(
      csvRow({ amountCents: 2_000, lineItem: "Contributions Less Than $35 Each", occupation: null })
    );
    expect(aggregateMontanaDirectFinance(input).directContributionTotal).toBe(197);
  });

  it("excludes inter-side transfers from spending on both surfaces (Eddy shape)", () => {
    const input = fixture();
    // Booked as an ordinary expenditure, identified only by its purpose.
    input.canonicalReports[0]!.artifact.lists.expendOther.push(
      detailRow({
        cashAmtCents: 241_307_00,
        totalAmtCents: 241_307_00,
        purposeDescr: "Transfer of primary funds to general, no primary",
        entityName: "Stockman Bank",
      })
    );
    // The chain still sees the cash leave: keep it closing by raising inflow.
    input.canonicalReports[0]!.artifact.lists.refunds.push(
      detailRow({ cashAmtCents: 241_307_00, totalAmtCents: 241_307_00 })
    );
    const result = aggregateMontanaDirectFinance(input);
    expect(result.totalDisbursements).toBe(80);
    expect(result.sideTransferTotal).toBe(241_307);
    // A CSV that carries the transfer row must match it exactly.
    input.expenditureRows.push(
      csvRow({ amountCents: 241_306_99, purpose: "Transfer of primary funds to general", lineItem: "All Other Expenditures" })
    );
    expect(() => aggregateMontanaDirectFinance(input)).toThrow("side-transfer totals disagree");
  });

  it("fails closed on electioneering rows in a candidate report", () => {
    const input = fixture();
    input.canonicalReports[0]!.artifact.lists.individual.push(
      detailRow({ cashAmtCents: 1, totalAmtCents: 1, electioneeringInd: "Y" })
    );
    expect(() => aggregateMontanaDirectFinance(input)).toThrow("electioneering");
  });

  it("rejects mismatched inventory/detail pairings and empty report sets", () => {
    const input = fixture();
    input.canonicalReports[0]!.artifact.reportId = 99;
    expect(() => aggregateMontanaDirectFinance(input)).toThrow("paired with inventory row");
    expect(() =>
      aggregateMontanaDirectFinance({ ...fixture(), canonicalReports: [] })
    ).toThrow("at least one canonical report");
  });
});
