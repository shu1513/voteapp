import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

import { describe, expect, it } from "vitest";

import { aggregateNorthCarolinaDirectFinance } from "../../../src/pipeline/northCarolinaFinance/northCarolinaDirectContributionAggregator.js";
import {
  NCSBE_COVER_SECTIONS,
  parseNcsbeDate,
  parseNcsbeDocumentListPage,
  parseNcsbeReceiptsPage,
  parseNcsbeReportDetailPage,
  type NcsbeDocumentRow,
  type NcsbeExpenditureRow,
  type NcsbeReceiptRow,
  type NcsbeReportDetail,
} from "../../../src/pipeline/northCarolinaFinance/northCarolinaNcsbeParsers.js";

function fixture(name: string): string {
  return readFileSync(
    fileURLToPath(new URL(`../../fixtures/northCarolinaFinance/${name}`, import.meta.url)),
    "utf8"
  );
}

const GADSON_INVENTORY = parseNcsbeDocumentListPage(fixture("document-inventory-gadson.html"));
const GADSON_COVER = parseNcsbeReportDetailPage(fixture("report-cover-gadson-229931.html"));
const GADSON_RECEIPTS = parseNcsbeReceiptsPage(fixture("receipts-gadson-229931-p0.json")).rows;
const GADSON_Q1_ROW = GADSON_INVENTORY.find((row) => row.dataLink === "229931")!;

const SOURCE_URL = "https://cf.ncsbe.gov/CFOrgLkup/DocumentGeneralResult/?OGID=57190&SID=STA-JV516O-C-001";

// Builds a full 34-section synthetic cover; `values` maps a sequence to
// [periodDollars, cycleDollars].
function makeCover(input: {
  beginDate: string;
  endDate: string;
  filedDate?: string;
  reportId?: string;
  values: Record<number, [number, number]>;
}): NcsbeReportDetail {
  return {
    cover: {
      reportId: input.reportId ?? "229931",
      boeId: "STA-JV516O-C-001",
      orgName: "Gadson for North Carolina",
      entityTypeDesc: null,
      fullReportName: null,
      reportVersion: "2007",
      beginDate: parseNcsbeDate(input.beginDate),
      endDate: parseNcsbeDate(input.endDate),
      filedDate: parseNcsbeDate(input.filedDate ?? "01/01/2026"),
    },
    summarySections: [...NCSBE_COVER_SECTIONS.entries()].map(([sequence, section]) => ({
      sequence,
      section,
      periodCents: Math.round((input.values[sequence]?.[0] ?? 0) * 100),
      cycleCents: Math.round((input.values[sequence]?.[1] ?? 0) * 100),
    })),
  };
}

function makeInventoryRow(overrides: Partial<NcsbeDocumentRow> = {}): NcsbeDocumentRow {
  return {
    committeeName: "GADSON FOR NORTH CAROLINA",
    sboeId: "STA-JV516O-C-001",
    reportYear: 2025,
    documentType: "Disclosure Report",
    reportType: "Year End Semi-Annual",
    isAmendment: false,
    imageReceiptDate: parseNcsbeDate("01/18/2026"),
    dataImportDate: parseNcsbeDate("01/18/2026"),
    periodStartDate: parseNcsbeDate("07/01/2025"),
    periodEndDate: parseNcsbeDate("12/31/2025"),
    dataLink: "227042",
    imageLink: "image.pdf",
    ...overrides,
  };
}

function makeReceipt(overrides: Partial<NcsbeReceiptRow> = {}): NcsbeReceiptRow {
  return {
    groupId: 1,
    occurDate: parseNcsbeDate("02/01/2026"),
    orgName: "DOE, JANE",
    isOrg: false,
    amountCents: 100_00,
    sumToDateCents: 100_00,
    profession: "Attorney",
    employersName: "Firm LLP",
    isAggregated: false,
    receiptTypeDesc: "Individual Contribution",
    receiptTypeCode: "IND ",
    accountAbbr: null,
    formOfPaymentDesc: null,
    purpose: null,
    ...overrides,
  };
}

function makeExpenditure(overrides: Partial<NcsbeExpenditureRow> = {}): NcsbeExpenditureRow {
  return {
    occurDate: parseNcsbeDate("02/01/2026"),
    orgName: "VENDOR LLC",
    isOrg: true,
    amountCents: 500_00,
    ieAmountCents: null,
    isAggregated: false,
    expenditureTypeDesc: "Operating Expense",
    purposeTypeCode: null,
    purpose: null,
    accountAbbr: null,
    formOfPaymentDesc: null,
    candidate: null,
    officeSought: null,
    declaration: null,
    ...overrides,
  };
}

describe("aggregateNorthCarolinaDirectFinance", () => {
  it("reproduces the official Gadson Q1 numbers from real fixture bytes", () => {
    const result = aggregateNorthCarolinaDirectFinance({
      electionYear: 2026,
      inventoryRows: [GADSON_Q1_ROW],
      reports: [{ reportId: "229931", cover: GADSON_COVER, receiptRows: GADSON_RECEIPTS }],
      sourceUrl: SOURCE_URL,
    });

    expect(result.status).toBe("ok");
    // Spike-verified official values: cover is authoritative.
    expect(result.summary).toEqual({
      totalReceipts: 6073.24,
      directContributionTotal: 5573.24,
      totalDisbursements: 24743.78,
      cashOnHand: 13158.56,
      sourceUrl: SOURCE_URL,
    });
    // The itemized rows reconcile exactly on this report: 4 aggregated
    // individual rows sum to the cover's $110 and 14 itemized individual
    // rows to its $5,463.24; the PPTY row is known non-individual money.
    expect(result.itemizedReceiptsCents).toBe(607_324);
    expect(result.itemizedIndividualCents).toBe(557_324);
    expect(result.coverIndividualContributionCents).toBe(557_324);
    expect(result.aggregatedIndividualRowCount).toBe(4);
    expect(result.includedIndividualRowCount).toBe(14);
    expect(result.derivedBreakdownsQuarantined).toBe(false);
    expect(result.unknownReceiptTypeCodes).toEqual([]);

    // Occupation shipping (decision 7): "Not Employed" is a placeholder,
    // real professions aggregate with first-seen casing.
    expect(result.placeholderOccupationRowCount).toBe(5);
    expect(result.placeholderOccupationCents).toBe(135_000);
    expect(result.occupationAttributedCents).toBe(411_324);
    const occupations = result.directBreakdowns.filter(
      (breakdown) => breakdown.categoryType === "occupation"
    );
    expect(occupations[0]).toMatchObject({ categoryName: "Broker", amount: 1000 });
    expect(occupations.map((breakdown) => breakdown.categoryName)).toContain("psychiatrist");
    expect(occupations.map((breakdown) => breakdown.categoryName)).not.toContain("Not Employed");

    const buckets = new Map(
      result.directBreakdowns
        .filter((breakdown) => breakdown.categoryType === "contribution_size")
        .map((breakdown) => [breakdown.categoryName, breakdown])
    );
    expect(buckets.get("$1,000-$4,999")).toMatchObject({ amount: 3000, contributorCount: 3 });
    expect(buckets.get("$1-$99")).toMatchObject({ amount: 213.24, contributorCount: 4 });

    expect(result.cycleChainMismatches).toEqual([]);
    expect(result.coverPeriodMismatchReportIds).toEqual([]);
    expect(result.negativeCashOnHand).toBe(false);
    expect(result.fortyEightHourNoticeSumCents).toBe(0);
  });

  it("excludes a 48-Hour Notice without failing the candidate — its money rides the covering report", () => {
    // Live PR 9 finding (RID 230343): the 48-hour form has a 3-heading
    // all-zero cover and its receipt reappears on the regular report, so the
    // acquisition never fetches it. Without the pinned exclusion this row
    // would surface as a missing artifact and fail the whole candidate.
    const fortyEightHour = makeInventoryRow({
      reportType: "48-Hour Notice",
      periodStartDate: parseNcsbeDate("02/26/2026"),
      periodEndDate: parseNcsbeDate("03/03/2026"),
      dataLink: "230343",
    });
    const result = aggregateNorthCarolinaDirectFinance({
      electionYear: 2026,
      inventoryRows: [GADSON_Q1_ROW, fortyEightHour],
      reports: [{ reportId: "229931", cover: GADSON_COVER, receiptRows: GADSON_RECEIPTS }],
      sourceUrl: SOURCE_URL,
    });
    expect(result.status).toBe("ok");
    expect(result.excludedNoTotalReportRowCount).toBe(1);
    expect(result.selectedReportIds).toEqual(["229931"]);
    expect(result.missingReportIds).toEqual([]);
    expect(result.summary.totalReceipts).toBe(6073.24);
  });

  it("leaves an Independent Expenditure Report to the outside leg, image-only or not", () => {
    // Live PR 9 finding: an IE filing is DocumentType "Disclosure Report"
    // with this ReportType, so it reached the direct reader. Summing it would
    // double-count IE money against decision 3, and the image-only ones read
    // as superseded-unavailable — which is what killed the funder leg.
    const ieImageOnly = makeInventoryRow({
      reportType: "Independent Expenditure Report",
      periodStartDate: parseNcsbeDate("02/15/2026"),
      periodEndDate: parseNcsbeDate("06/30/2026"),
      dataLink: null,
      imageLink: "ie.pdf",
    });
    const result = aggregateNorthCarolinaDirectFinance({
      electionYear: 2026,
      inventoryRows: [GADSON_Q1_ROW, ieImageOnly],
      reports: [{ reportId: "229931", cover: GADSON_COVER, receiptRows: GADSON_RECEIPTS }],
      sourceUrl: SOURCE_URL,
    });
    expect(result.status).toBe("ok");
    expect(result.excludedOutsideLegReportRowCount).toBe(1);
    expect(result.supersededUnavailablePeriods).toEqual([]);
    expect(result.summary.totalReceipts).toBe(6073.24);
  });

  it("drops an undated 1990s filing instead of failing the candidate on a report nobody fetched", () => {
    // The acquisition never fetches these rows, so letting selection demand
    // their artifacts would mark every such committee incomplete forever.
    const legacy = makeInventoryRow({
      reportType: "Annual",
      reportYear: 1994,
      periodStartDate: parseNcsbeDate(""),
      periodEndDate: parseNcsbeDate(""),
      dataLink: "27160",
    });
    const result = aggregateNorthCarolinaDirectFinance({
      electionYear: 2026,
      inventoryRows: [GADSON_Q1_ROW, legacy],
      reports: [{ reportId: "229931", cover: GADSON_COVER, receiptRows: GADSON_RECEIPTS }],
      sourceUrl: SOURCE_URL,
    });
    expect(result.status).toBe("ok");
    expect(result.excludedUndatedOutOfCycleRowCount).toBe(1);
    expect(result.selectedReportIds).toEqual(["229931"]);
    expect(result.missingReportIds).toEqual([]);
  });

  it("sums covers across reports, checks the Cycle chain, and takes cash from the latest report", () => {
    const organizational = makeInventoryRow({
      reportType: "Organizational",
      periodStartDate: parseNcsbeDate("10/31/2025"),
      periodEndDate: parseNcsbeDate("11/10/2025"),
      dataLink: "226297",
    });
    const yearEnd = makeInventoryRow();
    const coverOrganizational = makeCover({
      reportId: "226297",
      beginDate: "10/31/2025",
      endDate: "11/10/2025",
      values: { 20: [1000, 1000], 60: [1000, 1000], 90: [100, 100], 95: [900, 900] },
    });
    const coverYearEnd = makeCover({
      reportId: "227042",
      beginDate: "07/01/2025",
      endDate: "12/31/2025",
      // Chain-exact: Cycle = 1000 + 5000 and 100 + 400.
      values: { 15: [200, 200], 20: [4800, 5800], 60: [5000, 6000], 90: [400, 500], 95: [-100, -100] },
    });

    const result = aggregateNorthCarolinaDirectFinance({
      electionYear: 2026,
      inventoryRows: [organizational, yearEnd],
      reports: [
        { reportId: "226297", cover: coverOrganizational, receiptRows: [] },
        { reportId: "227042", cover: coverYearEnd, receiptRows: [] },
      ],
    });

    expect(result.status).toBe("ok");
    expect(result.selectedReportIds).toEqual(["226297", "227042"]);
    expect(result.summary.totalReceipts).toBe(6000);
    expect(result.summary.directContributionTotal).toBe(6000);
    expect(result.summary.totalDisbursements).toBe(500);
    // Latest report's balance is negative: NULL + diagnostic, never a clamp.
    expect(result.summary.cashOnHand).toBeNull();
    expect(result.negativeCashOnHand).toBe(true);
    expect(result.cycleChainMismatches).toEqual([]);
  });

  it("reports a Cycle chain break — a missing filing between two selected reports", () => {
    const organizational = makeInventoryRow({
      reportType: "Organizational",
      periodStartDate: parseNcsbeDate("10/31/2025"),
      periodEndDate: parseNcsbeDate("11/10/2025"),
      dataLink: "226297",
    });
    const yearEnd = makeInventoryRow();
    const result = aggregateNorthCarolinaDirectFinance({
      electionYear: 2026,
      inventoryRows: [organizational, yearEnd],
      reports: [
        {
          reportId: "226297",
          cover: makeCover({
            reportId: "226297",
            beginDate: "10/31/2025",
            endDate: "11/10/2025",
            values: { 60: [1000, 1000], 90: [0, 0], 95: [1000, 1000] },
          }),
          receiptRows: [],
        },
        {
          reportId: "227042",
          cover: makeCover({
            reportId: "227042",
            beginDate: "07/01/2025",
            endDate: "12/31/2025",
            // Cycle claims 8000 but 1000 + 5000 = 6000: a filing is missing.
            values: { 60: [5000, 8000], 90: [0, 0], 95: [6000, 6000] },
          }),
          receiptRows: [],
        },
      ],
    });
    expect(result.cycleChainMismatches).toEqual([
      {
        previousReportId: "226297",
        reportId: "227042",
        section: "total_receipts",
        expectedCycleCents: 600_000,
        actualCycleCents: 800_000,
      },
    ]);
  });

  it("writes the honest snapshot when the current filing is image-only", () => {
    // An image-only amendment supersedes the structured Q1 (decision 8):
    // direct summary nulls + emptied breakdowns, never stale money.
    const imageOnlyAmendment: NcsbeDocumentRow = {
      ...GADSON_Q1_ROW,
      isAmendment: true,
      dataLink: null,
      imageReceiptDate: parseNcsbeDate("03/15/2026"),
      dataImportDate: parseNcsbeDate(""),
    };
    const result = aggregateNorthCarolinaDirectFinance({
      electionYear: 2026,
      inventoryRows: [GADSON_Q1_ROW, imageOnlyAmendment],
      reports: [{ reportId: "229931", cover: GADSON_COVER, receiptRows: GADSON_RECEIPTS }],
      sourceUrl: SOURCE_URL,
    });
    expect(result.status).toBe("honest_null");
    expect(result.summary).toEqual({
      totalReceipts: null,
      directContributionTotal: null,
      totalDisbursements: null,
      cashOnHand: null,
      sourceUrl: SOURCE_URL,
    });
    expect(result.directBreakdowns).toEqual([]);
    expect(result.supersededUnavailablePeriods).toEqual([
      { reportType: "First Quarter", periodStartRaw: "01/01/2026", periodEndRaw: "02/14/2026" },
    ]);
  });

  it("does not aggregate when a selected report's artifacts are missing", () => {
    const result = aggregateNorthCarolinaDirectFinance({
      electionYear: 2026,
      inventoryRows: GADSON_INVENTORY,
      reports: [{ reportId: "229931", cover: GADSON_COVER, receiptRows: GADSON_RECEIPTS }],
    });
    expect(result.status).toBe("incomplete_artifacts");
    // Period-END order (decision 11's cash rule keys on it): the
    // Organizational report (ends 11/10/2025) precedes Year End (12/31/2025).
    expect(result.selectedReportIds).toEqual(["226297", "227042", "229931"]);
    expect(result.missingReportIds).toEqual(["226297", "227042"]);
    expect(result.summary.totalReceipts).toBeNull();
    expect(result.directBreakdowns).toEqual([]);
  });

  it("quarantines derived breakdowns on an unknown receipt code but keeps cover totals", () => {
    const result = aggregateNorthCarolinaDirectFinance({
      electionYear: 2026,
      inventoryRows: [GADSON_Q1_ROW],
      reports: [
        {
          reportId: "229931",
          cover: GADSON_COVER,
          receiptRows: [...GADSON_RECEIPTS, makeReceipt({ receiptTypeCode: "LOAN", amountCents: 50_00 })],
        },
      ],
    });
    expect(result.status).toBe("ok");
    expect(result.summary.totalReceipts).toBe(6073.24);
    expect(result.derivedBreakdownsQuarantined).toBe(true);
    expect(result.directBreakdowns).toEqual([]);
    expect(result.unknownReceiptTypeCodes).toEqual([{ code: "LOAN", rowCount: 1, amountCents: 5_000 }]);
  });

  it("counts IE-typed regular-report rows as the single-source cross-check, ignoring junk IE columns", () => {
    const result = aggregateNorthCarolinaDirectFinance({
      electionYear: 2026,
      inventoryRows: [GADSON_Q1_ROW],
      reports: [
        {
          reportId: "229931",
          cover: GADSON_COVER,
          receiptRows: GADSON_RECEIPTS,
          expenditureRows: [
            makeExpenditure({
              expenditureTypeDesc: "Independent Expenditure",
              declaration: "Support",
              amountCents: 10_500_00,
            }),
            // Junk ShowIEColumns values on a plain operating row (spike
            // results item 9) are excluded by the type conjunction.
            makeExpenditure({ declaration: "Oppose", candidate: "SOME VENDOR" }),
          ],
        },
      ],
    });
    expect(result.ieTypedRegularReportRowCount).toBe(1);
    expect(result.ieTypedRegularReportCents).toBe(1_050_000);
  });

  it("refuses to aggregate a cover that declares a different report id", () => {
    // The cover names itself; bytes cached for another report are provably
    // mispaired and never writable money — do not write, reacquire.
    const wrongCover = makeCover({
      reportId: "999999",
      beginDate: "01/01/2026",
      endDate: "02/14/2026",
      values: { 60: [1, 1], 90: [0, 0], 95: [1, 1] },
    });
    const result = aggregateNorthCarolinaDirectFinance({
      electionYear: 2026,
      inventoryRows: [GADSON_Q1_ROW],
      reports: [{ reportId: "229931", cover: wrongCover, receiptRows: [] }],
    });
    expect(result.status).toBe("incomplete_artifacts");
    expect(result.coverPeriodMismatchReportIds).toEqual(["229931"]);
    expect(result.summary.totalReceipts).toBeNull();
    expect(result.directBreakdowns).toEqual([]);
  });

  it("still aggregates when only the cover's dates disagree, recording the disagreement", () => {
    // Live PR 9 evidence: the portal serves begin-after-end and off-by-days
    // covers for the right report (RID 233220: 07/01/2026 -> 06/30/2026).
    // Eight real candidates were withheld by treating that as mispairing.
    const sloppyDates = makeCover({
      beginDate: "12/31/2025",
      endDate: "03/31/2026",
      values: { 20: [500, 500], 60: [500, 500], 90: [100, 100], 95: [400, 400] },
    });
    const result = aggregateNorthCarolinaDirectFinance({
      electionYear: 2026,
      inventoryRows: [GADSON_Q1_ROW],
      reports: [{ reportId: "229931", cover: sloppyDates, receiptRows: [] }],
    });
    expect(result.status).toBe("ok");
    expect(result.coverPeriodMismatchReportIds).toEqual([]);
    expect(result.coverPeriodDisagreementReportIds).toEqual(["229931"]);
    expect(result.summary.totalReceipts).toBe(500);
  });
});
