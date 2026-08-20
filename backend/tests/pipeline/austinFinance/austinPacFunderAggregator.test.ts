import { describe, expect, it } from "vitest";
import {
  aggregateAustinPacFunders,
  isAustinIndustryEligibleOrganizationName,
} from "../../../src/pipeline/austinFinance/austinPacFunderAggregator.js";
import type { AustinReportFacts } from "../../../src/pipeline/austinFinance/austinOutsideSpendingAggregator.js";
import type { AustinContributionRow } from "../../../src/pipeline/austinFinance/austinSocrataClient.js";

const PAC = "Vibrant Austin PAC";
const WINDOW = { windowFrom: "2024-01-01", windowTo: "2024-12-31" };

let nextTransaction = 1;
function receipt(
  reportId: string,
  donor: string,
  amountCents: number,
  overrides: Partial<AustinContributionRow> = {},
): AustinContributionRow {
  const transactionId = `${reportId}-A${String(nextTransaction++).padStart(5, "0")}`;
  return {
    transactionId,
    reportId,
    recipient: PAC,
    donor,
    donorType: "ENTITY",
    contributionType: "Monetary Political Contribution",
    amountCents,
    contributionDate: "2024-10-01",
    occupation: null,
    employer: null,
    reportFiled: null,
    correction: false,
    reportUrl: null,
    ...overrides,
  };
}

// Vibrant-shaped 2024 reports: two regular GPACs (disjoint periods), an
// ATX.1 special, a PACATX.7 special, and a CORPAC whose period covers the
// second GPAC and both specials (the live 10-28..12-04 shape).
function reportsById(): Map<string, AustinReportFacts> {
  return new Map<string, AustinReportFacts>([
    ["R30", { formTypeCode: "GPAC", periodFrom: "2024-07-01", periodTo: "2024-09-26", dateFiled: "2024-10-07" }],
    ["R8", { formTypeCode: "GPAC", periodFrom: "2024-11-05", periodTo: "2024-12-04", dateFiled: "2024-12-06" }],
    ["RATX1", { formTypeCode: "ATX1", periodFrom: "2024-10-31", periodTo: "2024-11-04", dateFiled: "2024-11-04" }],
    ["RATX7", { formTypeCode: "PACATX7", periodFrom: "2024-10-31", periodTo: "2024-11-04", dateFiled: "2024-11-04" }],
    ["RCOR", { formTypeCode: "CORPAC", periodFrom: "2024-10-28", periodTo: "2024-12-04", dateFiled: "2024-12-06" }],
  ]);
}

describe("aggregateAustinPacFunders", () => {
  it("aggregates entity donors and nets refunds", () => {
    const result = aggregateAustinPacFunders({
      contributions: [
        receipt("R30", "Kilroy Realty, L.P.", 100_000_00),
        receipt("R30", "Kilroy Realty, L.P.", 25_000_00, { contributionDate: "2024-08-01" }),
        receipt("R30", "HPI Real Estate", 10_000_00),
        // Refund nets against the donor's total.
        receipt("R30", "HPI Real Estate", -2_000_00, { contributionDate: "2024-09-01" }),
        // Nets to zero -> dropped.
        receipt("R30", "Round Trip LLC", 5_000_00),
        receipt("R30", "Round Trip LLC", -5_000_00, { contributionDate: "2024-09-02" }),
      ],
      reportsById: reportsById(),
      ...WINDOW,
    });
    expect(result.donors).toEqual([
      { donorName: "Kilroy Realty, L.P.", donorKey: "KILROY REALTY L P", amountCents: 125_000_00, receiptCount: 2 },
      { donorName: "HPI Real Estate", donorKey: "HPI REAL ESTATE", amountCents: 8_000_00, receiptCount: 1 },
    ]);
    expect(result.entityDonorCents).toBe(133_000_00);
  });

  it("scopes to the window and sets non-receipt rows aside", () => {
    const result = aggregateAustinPacFunders({
      contributions: [
        receipt("R30", "In Window LLC", 1_000_00),
        receipt("R30", "Too Early LLC", 9_000_00, { contributionDate: "2023-12-31" }),
        receipt("R30", "Undated LLC", 9_000_00, { contributionDate: null }),
        receipt("R30", "Promised LLC", 9_000_00, { contributionType: "Pledged Contribution" }),
      ],
      reportsById: reportsById(),
      ...WINDOW,
    });
    expect(result.donors.map((donor) => donor.donorName)).toEqual(["In Window LLC"]);
    expect(result.windowRowCount).toBe(2);
    expect(result.nonReceiptRowCount).toBe(1);
  });

  it("lets a correction void same-period rows on non-correction reports (rule 0)", () => {
    // The live Vibrant shape: the CORPAC re-lists a GPAC row, an ATX.1 row
    // and a PACATX.7 row of its period. Only the CORPAC copies count.
    const result = aggregateAustinPacFunders({
      contributions: [
        receipt("R8", "Corrected Donor Inc", 5_000_00, { contributionDate: "2024-12-04" }),
        receipt("RCOR", "Corrected Donor Inc", 5_500_00, { contributionDate: "2024-12-04" }),
        receipt("RATX1", "Nosek Holdings LLC", 750_000_00, { contributionDate: "2024-11-01" }),
        receipt("RATX7", "Nosek Holdings LLC", 750_000_00, { contributionDate: "2024-11-01" }),
        receipt("RCOR", "Nosek Holdings LLC", 750_000_00, { contributionDate: "2024-11-01" }),
        // Outside every correction period: survives on its regular report.
        receipt("R30", "Early Donor Inc", 20_000_00, { contributionDate: "2024-08-15" }),
      ],
      reportsById: reportsById(),
      ...WINDOW,
    });
    expect(result.supersededRowCount).toBe(3);
    expect(result.donors).toEqual([
      { donorName: "Nosek Holdings LLC", donorKey: "NOSEK HOLDINGS LLC", amountCents: 750_000_00, receiptCount: 1 },
      { donorName: "Early Donor Inc", donorKey: "EARLY DONOR INC", amountCents: 20_000_00, receiptCount: 1 },
      { donorName: "Corrected Donor Inc", donorKey: "CORRECTED DONOR INC", amountCents: 5_500_00, receiptCount: 1 },
    ]);
  });

  it("keeps only the latest-filed correction of one period", () => {
    const reports = reportsById();
    reports.set("RCOR2", { formTypeCode: "CORPAC", periodFrom: "2024-10-28", periodTo: "2024-12-04", dateFiled: "2024-12-10" });
    const result = aggregateAustinPacFunders({
      contributions: [
        receipt("RCOR", "Stale Fix LLC", 1_000_00, { contributionDate: "2024-11-20" }),
        receipt("RCOR2", "Stale Fix LLC", 1_200_00, { contributionDate: "2024-11-20" }),
      ],
      reportsById: reports,
      ...WINDOW,
    });
    expect(result.supersededRowCount).toBe(1);
    expect(result.donors[0]).toMatchObject({ donorName: "Stale Fix LLC", amountCents: 1_200_00 });
  });

  it("folds special re-listings by (donor, date, amount) across reports", () => {
    // ANCHOR COULTER live shape: the same gift on the ATX.1 and the next
    // regular report, with no correction anywhere.
    const result = aggregateAustinPacFunders({
      contributions: [
        receipt("RATX1", "Anchor Coulter Holdings LLC", 100_000_00, { contributionDate: "2024-11-02" }),
        receipt("R8", "Anchor Coulter Holdings LLC", 100_000_00, { contributionDate: "2024-11-02" }),
      ],
      reportsById: new Map<string, AustinReportFacts>([
        ["RATX1", { formTypeCode: "ATX1", periodFrom: "2024-10-31", periodTo: "2024-11-04", dateFiled: "2024-11-04" }],
        ["R8", { formTypeCode: "GPAC", periodFrom: "2024-11-05", periodTo: "2024-12-04", dateFiled: "2024-12-06" }],
      ]),
      ...WINDOW,
    });
    expect(result.duplicateReceiptCount).toBe(1);
    expect(result.donors[0]).toMatchObject({ amountCents: 100_000_00, receiptCount: 1 });
  });

  it("reports individual and committee-named money instead of attributing it", () => {
    const result = aggregateAustinPacFunders({
      contributions: [
        receipt("R30", "Nosek, Nicole", 7_500_00, { donorType: "INDIVIDUAL" }),
        receipt("R30", "Untyped, Pat", 100_00, { donorType: null }),
        receipt("R30", "Way To Lead PAC", 5_000_00),
        receipt("R30", "Friends of Austin Parks", 2_000_00),
        receipt("R30", "Real Donor Inc", 1_000_00),
      ],
      reportsById: reportsById(),
      ...WINDOW,
    });
    expect(result.individualCents).toBe(7_600_00);
    expect(result.ineligibleOrgCents).toBe(7_000_00);
    expect(result.donors.map((donor) => donor.donorName)).toEqual(["Real Donor Inc"]);
  });

  it("throws on an inverted window", () => {
    expect(() =>
      aggregateAustinPacFunders({
        contributions: [],
        reportsById: new Map(),
        windowFrom: "2024-12-31",
        windowTo: "2024-01-01",
      }),
    ).toThrow(/inverted/);
  });
});

describe("isAustinIndustryEligibleOrganizationName", () => {
  it("accepts operating businesses and rejects committee-shaped names", () => {
    expect(isAustinIndustryEligibleOrganizationName("Kilroy Realty, L.P.")).toBe(true);
    expect(isAustinIndustryEligibleOrganizationName("Laborers Local 1095")).toBe(true);
    expect(isAustinIndustryEligibleOrganizationName("Way To Lead PAC")).toBe(false);
    expect(isAustinIndustryEligibleOrganizationName("Texas Political Action Committee")).toBe(false);
    expect(isAustinIndustryEligibleOrganizationName("Friends of Austin Parks")).toBe(false);
    expect(isAustinIndustryEligibleOrganizationName("Citizens for Growth")).toBe(false);
    expect(isAustinIndustryEligibleOrganizationName("Adler for Mayor")).toBe(false);
    expect(isAustinIndustryEligibleOrganizationName("  ")).toBe(false);
  });
});
