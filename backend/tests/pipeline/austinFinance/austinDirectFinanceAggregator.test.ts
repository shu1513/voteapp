import { describe, expect, it } from "vitest";
import {
  aggregateAustinDirectFinance,
  isAustinNonReceiptContributionType,
} from "../../../src/pipeline/austinFinance/austinDirectFinanceAggregator.js";
import type {
  AustinContributionRow,
  AustinReportDetailRow,
} from "../../../src/pipeline/austinFinance/austinSocrataClient.js";

const FILER = "Watson, Kirk P.";
const ELECTION = "2024-11-05";

function report(
  overrides: Partial<AustinReportDetailRow> & { reportId: string; periodFrom: string; periodTo: string },
): AustinReportDetailRow {
  return {
    filerName: FILER,
    formTypeCode: "COH",
    formType: "COH - Candidate /Officeholder Campaign Finance Report",
    reportType: "Semiannual",
    dateFiled: `${overrides.periodTo}`,
    electionDate: ELECTION,
    electionType: "General",
    officeSought: "MAYOR",
    officeHeld: "MAYOR",
    contribTotalCents: 0,
    expendTotalCents: 0,
    contribBalanceCents: 0,
    outstandingLoanCents: 0,
    reportUrl: null,
    ...overrides,
  };
}

let nextTransaction = 1;
function contribution(
  reportId: string,
  amountCents: number,
  overrides: Partial<AustinContributionRow> = {},
): AustinContributionRow {
  const transactionId = `${reportId}-A${String(nextTransaction++).padStart(5, "0")}`;
  return {
    transactionId,
    reportId,
    recipient: FILER,
    donor: "Doe, Jane",
    donorType: "INDIVIDUAL",
    contributionType: "Monetary Political Contribution",
    amountCents,
    contributionDate: "2024-03-01",
    occupation: "Attorney",
    employer: "Firm",
    reportFiled: null,
    correction: false,
    reportUrl: null,
    ...overrides,
  };
}

// Watson-shaped cycle: 2023-H2 (null cover, no rows), 2024-H1 original +
// correction, 30th-day original + correction, 8th-day, an ATX.7 inside the
// Jan-15 semiannual (dropped), the Jan-15 semiannual; plus a 2026 report for
// another seat and a duplicate row.
function watsonReports(): AustinReportDetailRow[] {
  const rows = [
    report({ reportId: "R2023H2", periodFrom: "2023-07-01", periodTo: "2023-12-31", dateFiled: "2024-01-16", contribTotalCents: null, expendTotalCents: 904_291, contribBalanceCents: 0 }),
    report({ reportId: "R2024H1", periodFrom: "2024-01-01", periodTo: "2024-06-30", dateFiled: "2024-07-15", contribTotalCents: 71_058_084, expendTotalCents: 16_638_852, contribBalanceCents: 51_268_666 }),
    report({ reportId: "R2024H1C", periodFrom: "2024-01-01", periodTo: "2024-06-30", dateFiled: "2024-12-02", formTypeCode: "CORCOH", contribTotalCents: 71_058_084, expendTotalCents: 16_683_852, contribBalanceCents: 51_268_666 }),
    report({ reportId: "R30", periodFrom: "2024-07-01", periodTo: "2024-09-26", dateFiled: "2024-10-07", contribTotalCents: 21_648_300, expendTotalCents: 48_865_764, contribBalanceCents: 26_689_161 }),
    report({ reportId: "R30C", periodFrom: "2024-07-01", periodTo: "2024-09-26", dateFiled: "2024-12-02", formTypeCode: "CORCOH", contribTotalCents: 21_767_294, expendTotalCents: 48_865_764, contribBalanceCents: 26_689_161 }),
    report({ reportId: "R8", periodFrom: "2024-09-27", periodTo: "2024-10-26", dateFiled: "2024-10-28", contribTotalCents: 10_241_456, expendTotalCents: 30_020_609, contribBalanceCents: 9_365_743 }),
    report({ reportId: "RATX7", periodFrom: "2024-10-27", periodTo: "2024-10-30", dateFiled: "2024-10-31", formTypeCode: "COHATX7", contribTotalCents: null, expendTotalCents: null, contribBalanceCents: null }),
    report({ reportId: "R2025H1", periodFrom: "2024-10-27", periodTo: "2024-12-31", dateFiled: "2025-01-15", contribTotalCents: 1_706_156, expendTotalCents: 11_123_569, contribBalanceCents: 650_619 }),
    // Another cycle / seat: must not count.
    report({ reportId: "R2026", periodFrom: "2026-01-01", periodTo: "2026-06-30", dateFiled: "2026-07-15", electionDate: "2026-11-03", officeSought: "COUNCIL_MBR_DISTRICT_09", contribTotalCents: 5_000_00, contribBalanceCents: 99_00 }),
  ];
  // Exact duplicate row of the 8th-day report (the dataset does this).
  return [...rows, { ...rows[5]! }];
}

function watsonContributions(): AustinContributionRow[] {
  return [
    // 2024-H1: three rows = 710,580.84 (the correction's cover; the original's rows share the id space, but only the correction id counts).
    contribution("R2024H1C", 45_000, { occupation: "Retired" }),
    contribution("R2024H1C", 45_000, { occupation: "retired " }),
    contribution("R2024H1C", 70_968_084, { occupation: "Attorney", donorType: "INDIVIDUAL" }),
    // Rows under the superseded original must be ignored entirely.
    contribution("R2024H1", 999_999),
    // 30th-day correction = 217,672.94: monetary + one pledge that must NOT count.
    contribution("R30C", 21_767_294, { occupation: null }),
    contribution("R30C", 50_000, { contributionType: "Pledged Contribution" }),
    // 8th day = 102,414.56 with an entity donor (in the total + size buckets, no occupation) and a self row (total only).
    contribution("R8", 2_000_00, { donorType: "ENTITY", contributionType: "Monetary Contribution From Corporation Or Labor Organization", donor: "Some LLC", occupation: null }),
    contribution("R8", 10_000_00, { donor: FILER, occupation: "Candidate" }),
    contribution("R8", 10_241_456 - 12_000_00, { occupation: "Consultant" }),
    // ATX.7 rows re-reported inside the Jan-15 semiannual — dropped with their report.
    contribution("RATX7", 45_000),
    // Jan-15 semiannual = 17,061.56.
    contribution("R2025H1", 1_706_156, { occupation: "Attorney" }),
    // Another cycle.
    contribution("R2026", 5_000_00),
  ];
}

describe("aggregateAustinDirectFinance", () => {
  it("sums corrected covers, takes cash from the latest cycle report, and buckets non-pledge rows", () => {
    const result = aggregateAustinDirectFinance({
      reports: watsonReports(),
      contributions: watsonContributions(),
      filerName: FILER,
      electionDate: ELECTION,
      officeCode: "MAYOR",
    });
    expect(result.cycleReports.map((row) => row.reportId)).toEqual([
      "R2023H2",
      "R2024H1C",
      "R30C",
      "R8",
      "R2025H1",
    ]);
    expect(result.keptSpecialReports).toEqual([]);
    expect(result.totalRaisedCents).toBe(104_772_990); // $1,047,729.90 (Phase 0 gate 2)
    expect(result.totalSpentCents).toBe(107_598_085); // $1,075,980.85
    expect(result.cashOnHandCents).toBe(650_619);
    expect(result.itemizedRowCount).toBe(8);
    expect(result.nonReceiptRowCount).toBe(1);
    expect(result.unitemizedCents).toBe(0);
    expect(result.selfRowCount).toBe(1);
    expect(result.breakdowns).toEqual([
      // Occupations: INDIVIDUAL rows only, keyed case/space-insensitively, first spelling kept.
      { categoryType: "occupation", categoryName: "Attorney", amountCents: 70_968_084 + 1_706_156, contributorCount: 2 },
      { categoryType: "occupation", categoryName: "Consultant", amountCents: 10_241_456 - 12_000_00, contributorCount: 1 },
      { categoryType: "occupation", categoryName: "Retired", amountCents: 90_000, contributorCount: 2 },
      // Sizes: every positive non-self row incl. the entity donor.
      { categoryType: "contribution_size", categoryName: "$5,000+", amountCents: 70_968_084 + 21_767_294 + 10_241_456 - 12_000_00 + 1_706_156, contributorCount: 4 },
      { categoryType: "contribution_size", categoryName: "$1,000-$4,999", amountCents: 2_000_00, contributorCount: 1 },
      { categoryType: "contribution_size", categoryName: "$250-$499", amountCents: 90_000, contributorCount: 2 },
    ]);
  });

  it("counts a kept ATX.7 report's itemized rows into raised (no cover to reconcile)", () => {
    // Drop the Jan-15 semiannual so the ATX.7 is not yet re-reported.
    const reports = watsonReports().filter((row) => row.reportId !== "R2025H1");
    const contributions = watsonContributions().filter((row) => row.reportId !== "R2025H1");
    const result = aggregateAustinDirectFinance({
      reports,
      contributions,
      filerName: FILER,
      electionDate: ELECTION,
      officeCode: "MAYOR",
    });
    expect(result.keptSpecialReports.map((row) => row.reportId)).toEqual(["RATX7"]);
    expect(result.totalRaisedCents).toBe(104_772_990 - 1_706_156 + 45_000);
    expect(result.totalSpentCents).toBe(107_598_085 - 11_123_569);
    // Cash comes from the latest REGULAR report, never the special.
    expect(result.cashOnHandCents).toBe(9_365_743);
  });

  it("scopes by office code as well as election date", () => {
    const result = aggregateAustinDirectFinance({
      reports: watsonReports(),
      contributions: watsonContributions(),
      filerName: FILER,
      electionDate: "2026-11-03",
      officeCode: "COUNCIL_MBR_DISTRICT_09",
    });
    expect(result.cycleReports.map((row) => row.reportId)).toEqual(["R2026"]);
    expect(result.totalRaisedCents).toBe(5_000_00);
    expect(result.cashOnHandCents).toBe(99_00);
    expect(() =>
      aggregateAustinDirectFinance({
        reports: watsonReports(),
        contributions: watsonContributions(),
        filerName: FILER,
        electionDate: "2026-11-03",
        officeCode: "MAYOR",
      }),
    ).toThrow(/has no effective report for MAYOR \/ 2026-11-03/);
  });

  it("tolerates itemized rows short of a cover (unitemized remainder) but never in excess", () => {
    // Drop $20 from the Consultant row: rows fall short of the R8 cover.
    const short = watsonContributions().map((row) =>
      row.occupation === "Consultant" ? { ...row, amountCents: row.amountCents - 2_000 } : row,
    );
    const result = aggregateAustinDirectFinance({
      reports: watsonReports(),
      contributions: short,
      filerName: FILER,
      electionDate: ELECTION,
      officeCode: "MAYOR",
    });
    expect(result.totalRaisedCents).toBe(104_772_990); // the cover is authoritative
    expect(result.unitemizedCents).toBe(2_000);
    // One cent over is a contamination signal.
    expect(() =>
      aggregateAustinDirectFinance({
        reports: watsonReports(),
        contributions: [...watsonContributions(), contribution("R8", 1)],
        filerName: FILER,
        electionDate: ELECTION,
        officeCode: "MAYOR",
      }),
    ).toThrow(/R8: itemized \$102414\.57 exceeds cover \$102414\.56/);
    // A null cover with itemized rows is an excess too (null means $0).
    expect(() =>
      aggregateAustinDirectFinance({
        reports: watsonReports(),
        contributions: [...watsonContributions(), contribution("R2023H2", 100)],
        filerName: FILER,
        electionDate: ELECTION,
        officeCode: "MAYOR",
      }),
    ).toThrow(/R2023H2: itemized \$1\.00 exceeds cover \$0\.00/);
  });

  it("fails closed when covers report contributions but no itemized rows came back", () => {
    expect(() =>
      aggregateAustinDirectFinance({
        reports: watsonReports(),
        contributions: [],
        filerName: FILER,
        electionDate: ELECTION,
        officeCode: "MAYOR",
      }),
    ).toThrow(/reports \$1047729\.90 in contributions but no itemized rows were returned/);
  });

  it("writes zeros for a filer whose only report is an empty cover", () => {
    const result = aggregateAustinDirectFinance({
      reports: [report({ reportId: "R1", periodFrom: "2026-01-01", periodTo: "2026-06-30", electionDate: "2026-11-03", officeSought: "COUNCIL_MBR_DISTRICT_01 District 1", contribTotalCents: null, expendTotalCents: null, contribBalanceCents: 0 })],
      contributions: [],
      filerName: "Rogers, Kyra",
      electionDate: "2026-11-03",
      officeCode: "COUNCIL_MBR_DISTRICT_01",
    });
    expect(result).toMatchObject({
      totalRaisedCents: 0,
      totalSpentCents: 0,
      cashOnHandCents: 0,
      breakdowns: [],
      itemizedRowCount: 0,
    });
  });

  it("recognizes non-receipt contribution types", () => {
    expect(isAustinNonReceiptContributionType("Pledged Contribution")).toBe(true);
    expect(isAustinNonReceiptContributionType("Pledged Contribution From Corporation Or Labor Organization")).toBe(true);
    expect(isAustinNonReceiptContributionType("Political Expenditures From Political Contribution")).toBe(true);
    expect(isAustinNonReceiptContributionType("Monetary Political Contribution")).toBe(false);
    expect(isAustinNonReceiptContributionType("Non-Monetary (In-Kind) Political Contribution")).toBe(false);
    expect(isAustinNonReceiptContributionType(null)).toBe(false);
  });
});
