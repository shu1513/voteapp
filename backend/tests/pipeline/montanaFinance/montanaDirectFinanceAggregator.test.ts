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
    expect(result.csvCrossCheckWarnings).toEqual([]);
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

  it("records contribution drift as warnings when real anchors verify the JSON side", () => {
    // Live Phase 3: the CONTR CSV drops rows and shows stale pre-amendment
    // amounts. With the chain closed over real anchors the JSON publishes
    // and the drift is surfaced, not quarantined.
    const individualDrift = fixture();
    individualDrift.contributionRows[0]!.amountCents = 4_000;
    const individualResult = aggregateMontanaDirectFinance(individualDrift);
    expect(individualResult.directContributionTotal).toBe(177);
    expect(individualResult.csvCrossCheckWarnings).toEqual([
      expect.stringContaining("beyond the itemization threshold"),
    ]);

    const committeeDrift = fixture();
    committeeDrift.contributionRows[2]!.amountCents = 2_400;
    expect(aggregateMontanaDirectFinance(committeeDrift).csvCrossCheckWarnings).toEqual([
      expect.stringContaining("committee-contribution totals disagree"),
    ]);

    // Expenditures stay strict — no live drift observed on that surface.
    const expenditureDrift = fixture();
    expenditureDrift.expenditureRows.push(csvRow({ amountCents: 1, lineItem: "All Other Expenditures" }));
    expect(() => aggregateMontanaDirectFinance(expenditureDrift)).toThrow("expenditure totals disagree");
  });

  it("fails closed on contribution drift when no link ever landed on a real anchor", () => {
    // An all-null chain closes tautologically: the CSV is the only
    // verification left, so drift beyond the tolerance still throws.
    const allNull = fixture();
    for (const report of allNull.canonicalReports) {
      report.inventory.primCashBegCents = null;
      report.inventory.genCashBegCents = null;
    }
    allNull.contributionRows[0]!.amountCents = 4_000;
    expect(() => aggregateMontanaDirectFinance(allNull)).toThrow("beyond the itemization threshold");

    // A real FIRST anchor with a carried second is existence, not
    // verification — the only link closes by construction, so demotion
    // must stay off and the same drift still throws.
    const carriedTail = fixture();
    carriedTail.canonicalReports[1]!.inventory.primCashBegCents = null;
    carriedTail.canonicalReports[1]!.inventory.genCashBegCents = null;
    carriedTail.contributionRows[0]!.amountCents = 4_000;
    expect(() => aggregateMontanaDirectFinance(carriedTail)).toThrow("beyond the itemization threshold");

    // A null FIRST anchor with a real second: the only link lands on a real
    // figure but its derivation is unrooted (assumed-$0 start), which
    // verifies nothing — drift still throws.
    const unrootedPrefix = fixture();
    unrootedPrefix.canonicalReports[0]!.inventory.primCashBegCents = null;
    unrootedPrefix.canonicalReports[0]!.inventory.genCashBegCents = null;
    unrootedPrefix.contributionRows[0]!.amountCents = 4_000;
    expect(() => aggregateMontanaDirectFinance(unrootedPrefix)).toThrow("beyond the itemization threshold");
  });

  it("rejects an empty CONTR export next to JSON contribution money — regardless of anchors", () => {
    // The broken-export shape a spurious no-fileName prepareDownloadFile
    // response would produce; never demotable to a warning.
    const input = fixture();
    input.contributionRows = [];
    expect(() => aggregateMontanaDirectFinance(input)).toThrow("broken export");
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

  it("matched small rows never widen the tolerance (large-row drift stays caught)", () => {
    // Both surfaces carry the same $40 small row; the CSV is missing the
    // $100 large donor. The matched small row explains nothing — under an
    // aggregate small-row budget this drift would slip through silently;
    // here it surfaces as a warning (the fixture chain has real anchors).
    const input = fixture();
    input.canonicalReports[0]!.artifact.lists.individual.push(
      detailRow({ cashAmtCents: 4_000, totalAmtCents: 4_000, entityName: "Both, Sides" })
    );
    input.contributionRows.push(csvRow({ amountCents: 4_000, entityName: "Both, Sides" }));
    input.canonicalReports[1]!.inventory.primCashBegCents! += 4_000;
    input.contributionRows[0]!.amountCents = 0; // the $100 Doe row vanishes from the CSV
    expect(aggregateMontanaDirectFinance(input).csvCrossCheckWarnings).toEqual([
      expect.stringContaining("beyond the itemization threshold"),
    ]);
  });

  it("publishes no cash balance when the derived ending is negative or unanchored", () => {
    // Negative derivation: the last period spends past the derived balance
    // (a real overdraft OR an unknowable-lump artifact — unpublishable).
    const negative = fixture();
    negative.canonicalReports[1]!.artifact.lists.expendOther.push(
      detailRow({ cashAmtCents: 25_000, totalAmtCents: 25_000, entityName: "Big Vendor" })
    );
    negative.expenditureRows.push(
      csvRow({ amountCents: 25_000, entityName: "Big Vendor", lineItem: "All Other Expenditures" })
    );
    const negativeResult = aggregateMontanaDirectFinance(negative);
    expect(negativeResult.cashOnHand).toBeNull();
    expect(negativeResult.directContributionTotal).toBe(177);

    // All-null anchors: totals publish on the cross-checks, balance stays null.
    const unanchored = fixture();
    for (const report of unanchored.canonicalReports) {
      report.inventory.primCashBegCents = null;
      report.inventory.genCashBegCents = null;
    }
    const unanchoredResult = aggregateMontanaDirectFinance(unanchored);
    expect(unanchoredResult.cashOnHand).toBeNull();
    // No anchors -> no derivable lump; raised is the itemized money only.
    expect(unanchoredResult.directContributionTotal).toBe(175);
  });

  it("sums the CSV's three committee line-item classes (live Phase 3 shape)", () => {
    // The CSV splits committee money by class; the JSON committee list
    // holds all of it. $10 + $10 + $5 must reconcile against the $25 JSON.
    const input = fixture();
    input.contributionRows[2]!.amountCents = 1_000;
    input.contributionRows.push(
      csvRow({ amountCents: 1_000, entityName: "Party Committee", lineItem: "Political Party Committee Contributions" }),
      csvRow({ amountCents: 500, entityName: "Incidental Inc", lineItem: "Incidental Committee Contributions" })
    );
    expect(aggregateMontanaDirectFinance(input).directContributionTotal).toBe(177);
  });

  it("tolerates threshold-row disagreement in both directions (Davis / Griffith shapes)", () => {
    // CSV itemizes a ≤$50 row the canonical JSON lumps — the CSV side is
    // LARGER, bounded by its own small rows (live: Ben Davis, +$90).
    const csvLarger = fixture();
    csvLarger.contributionRows.push(csvRow({ amountCents: 4_000, entityName: "Extra, Small" }));
    expect(aggregateMontanaDirectFinance(csvLarger).directContributionTotal).toBe(177);

    // A row of exactly $50.00 is itemizable on one surface and lumpable on
    // the other — MCA 13-37-229 requires itemization only "in excess of"
    // $50 (live: Griffith, nine $50.00 rows = a $450 CSV shortfall).
    const exactly50 = fixture();
    exactly50.canonicalReports[0]!.artifact.lists.individual.push(
      detailRow({ cashAmtCents: 5_000, totalAmtCents: 5_000, entityName: "Fifty, Frank" })
    );
    exactly50.canonicalReports[1]!.inventory.primCashBegCents! += 5_000;
    expect(aggregateMontanaDirectFinance(exactly50).directContributionTotal).toBe(227);
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

  it("never drops an ordinary expense whose purpose merely mentions a transfer", () => {
    const input = fixture();
    const purpose = "Wire transfer for general election mailers";
    input.canonicalReports[0]!.artifact.lists.expendOther.push(
      detailRow({ cashAmtCents: 5_000, totalAmtCents: 5_000, purposeDescr: purpose, entityName: "Print Shop" })
    );
    input.expenditureRows.push(csvRow({ amountCents: 5_000, purpose, lineItem: "All Other Expenditures" }));
    // Keep the chain closing over the extra outflow.
    input.canonicalReports[1]!.inventory.primCashBegCents! -= 5_000;
    const result = aggregateMontanaDirectFinance(input);
    expect(result.totalDisbursements).toBe(130);
    expect(result.sideTransferTotal).toBe(0);
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
