import { describe, expect, it } from "vitest";

import type {
  NorthDakotaCommitteeRow,
  NorthDakotaTransactionRow,
} from "../../../src/pipeline/northDakotaFinance/northDakotaCfrsClient.js";
import type { NorthDakotaContributionCsvRow } from "../../../src/pipeline/northDakotaFinance/northDakotaCfrsCsv.js";
import type { NorthDakotaExpenditureCsvRow } from "../../../src/pipeline/northDakotaFinance/northDakotaCfrsCsv.js";
import {
  apiAmountToCents,
  checkNorthDakotaCycleWindow,
  checkNorthDakotaYtdSemantics,
  classifyNorthDakotaOffice,
  evaluateNorthDakotaPhaseZeroGates,
  indexNorthDakotaCommittees,
  reconcileNorthDakotaChart,
  reconcileNorthDakotaCommittee,
  summarizeNorthDakotaContributionCsv,
  summarizeNorthDakotaExpenditureCsv,
  summarizeNorthDakotaIndependentExpenditures,
  summarizeNorthDakotaOccupations,
  summarizeNorthDakotaRegistry,
  summarizeNorthDakotaReportingCycles,
} from "../../../src/pipeline/northDakotaFinance/northDakotaPhaseZero.js";

function committee(overrides: Partial<NorthDakotaCommitteeRow>): NorthDakotaCommitteeRow {
  return {
    orgID: 1,
    entityId: "1010000001",
    orgName: "Friends of Test",
    candidateName: "Test, Candidate",
    orgType: "Candidate/Candidate Committee",
    orgTypeCode: "101",
    orgSubType: "Candidate Committee",
    orgSubTypeCode: "CNCM",
    election: "2026 Election - Statewide",
    office: "State Representative",
    district: "District 11",
    party: "North Dakota Republican Party",
    orgStatus: "Active",
    registrationYear: "2026",
    ...overrides,
  };
}

function csvRow(overrides: Partial<NorthDakotaContributionCsvRow>): NorthDakotaContributionCsvRow {
  return {
    line: 2,
    registrantId: "1010000001",
    committeeName: "Friends of Test",
    candidateName: "Test Candidate",
    transactionType: "Contributions",
    transactionCategory: "Monetary",
    transactionDate: "2026-01-12",
    amountCents: 96_060,
    contributorType: "Individual",
    contributorName: "Patrick Jones",
    employerName: null,
    filedDate: "2026-05-06",
    recovered: false,
    ...overrides,
  };
}

function apiRow(overrides: Partial<NorthDakotaTransactionRow>): NorthDakotaTransactionRow {
  return {
    transactionID: 1,
    entityID: "1010000001",
    orgID: 1,
    committeeName: "Friends of Test",
    candidateName: "Test, Candidate",
    transactionAmount: 960.6,
    transactionDate: "2026-01-12T00:00:00",
    filedDate: "2026-05-06T00:00:00",
    entityTypeDesc: "Individual",
    transactionCategoryDesc: "Monetary",
    transactionTypeDesc: "Contributions",
    transactionPurpose: null,
    contributorPayeeName: "Patrick Jones",
    contributorPayeeID: 10,
    employerName: null,
    employerOccupation: null,
    transactionTotalYTD: "960.6000",
    amendedFlag: false,
    reportVersionID: "1",
    reportFileName: "2026 Pre-Primary Report",
    s3ReportFilePath: null,
    stanceDescription: null,
    candidateNameAssocation: null,
    electionYear: null,
    orgType: "Candidate/Candidate Committee",
    ...overrides,
  };
}

const committeesById = indexNorthDakotaCommittees([
  committee({}),
  committee({ entityId: "1010000002", orgID: 2, office: "Supreme Court Justice", district: null }),
  committee({ entityId: "1010000003", orgID: 3, office: "Governor", district: null }),
  committee({ entityId: "1030000004", orgID: 4, orgType: "Party Committee", orgTypeCode: "103", office: null, district: "District 1" }),
]);

describe("apiAmountToCents", () => {
  it("converts cent-precise amounts and rejects sub-cent residue", () => {
    expect(apiAmountToCents(16857.14)).toBe(1_685_714);
    expect(apiAmountToCents(960.6)).toBe(96_060);
    expect(() => apiAmountToCents(0.001)).toThrow(/not cent-precise/);
  });
});

describe("classifyNorthDakotaOffice", () => {
  it("splits legislative, judicial and statewide labels", () => {
    expect(classifyNorthDakotaOffice("State Representative")).toBe("legislative");
    expect(classifyNorthDakotaOffice("State Senator")).toBe("legislative");
    expect(classifyNorthDakotaOffice("Supreme Court Justice")).toBe("judicial");
    expect(classifyNorthDakotaOffice("District Court Judge")).toBe("judicial");
    expect(classifyNorthDakotaOffice("Governor")).toBe("statewide");
    expect(classifyNorthDakotaOffice(null)).toBe("unknown");
  });
});

describe("summarizeNorthDakotaContributionCsv", () => {
  it("buckets by category, contributor type (blank -> Lumpsum) and registry committee type", () => {
    const summary = summarizeNorthDakotaContributionCsv(
      [
        csvRow({}),
        csvRow({}),
        csvRow({ registrantId: "1030000004", transactionCategory: "Total - $200 or less", contributorType: "", amountCents: 100 }),
        csvRow({ registrantId: "1099999999", amountCents: 5 }),
      ],
      committeesById
    );
    expect(summary.rowCount).toBe(4);
    expect(summary.duplicateRowCount).toBe(1);
    expect(summary.totalCents).toBe(96_060 * 2 + 105);
    expect(summary.byContributorType.Lumpsum).toEqual({ rowCount: 1, totalCents: 100, totalDollars: "1.00" });
    expect(summary.byCommitteeType["Candidate/Candidate Committee"].totalCents).toBe(96_060 * 2);
    expect(summary.byCommitteeType["Party Committee"].totalCents).toBe(100);
    expect(summary.byCommitteeType["<unregistered>"].totalCents).toBe(5);
    expect(summary.dateRange).toEqual({ min: "2026-01-12", max: "2026-01-12" });
  });
});

describe("summarizeNorthDakotaExpenditureCsv", () => {
  const expRow = (overrides: Partial<NorthDakotaExpenditureCsvRow>): NorthDakotaExpenditureCsvRow => ({
    line: 2,
    registrantId: "1010000001",
    committeeName: "Friends of Test",
    candidateName: "Test Candidate",
    transactionType: "Expenditures",
    expenditureType: "Itemized - greater than $200",
    expenditurePurpose: "",
    transactionDate: "2026-03-19",
    amountCents: 1_000_000,
    recipientType: "Candidate",
    recipientName: "Kelly Armstrong",
    filedDate: "2026-05-04",
    recovered: false,
    ...overrides,
  });

  it("separates year-end category lumps (type Monetary, purpose, no recipient) from itemized rows", () => {
    const summary = summarizeNorthDakotaExpenditureCsv(
      [
        expRow({}),
        expRow({ expenditureType: "Monetary", expenditurePurpose: "Operations", recipientType: "", recipientName: "", transactionDate: "2025-12-31", amountCents: 40_000 }),
        expRow({ expenditureType: "Monetary", expenditurePurpose: "Travel", recipientType: "", recipientName: "", transactionDate: "2025-12-31", amountCents: 2_500, registrantId: "1030000004" }),
      ],
      committeesById
    );
    expect(summary.totalCents).toBe(1_042_500);
    expect(summary.byRecipientType.Lumpsum.rowCount).toBe(2);
    expect(summary.yearEndCategoryLumps).toEqual({
      rowCount: 2,
      totalCents: 42_500,
      totalDollars: "425.00",
      byPurpose: {
        Operations: { rowCount: 1, totalCents: 40_000, totalDollars: "400.00" },
        Travel: { rowCount: 1, totalCents: 2_500, totalDollars: "25.00" },
      },
      byCommitteeType: {
        "Candidate/Candidate Committee": { rowCount: 1, totalCents: 40_000, totalDollars: "400.00" },
        "Party Committee": { rowCount: 1, totalCents: 2_500, totalDollars: "25.00" },
      },
      transactionDates: ["2025-12-31"],
    });
  });
});

describe("reconcileNorthDakotaChart", () => {
  const chart = [
    { name: "By Contributor Type", totalAmount: 11.0, data: [{ description: "Individual", amount: 10.0 }, { description: "Lumpsum", amount: 1.0 }] },
    { name: "By Purpose Type", totalAmount: 11.0, data: [] },
  ];
  const bucket = (totalCents: number) => ({ rowCount: 1, totalCents, totalDollars: (totalCents / 100).toFixed(2) });

  it("matches cent-exact totals and slices, leaving unmapped series uncompared", () => {
    const result = reconcileNorthDakotaChart({
      chart,
      csvTotalCents: 1_100,
      csvSlices: { "By Contributor Type": { Individual: bucket(1_000), Lumpsum: bucket(100) } },
    });
    expect(result.totalMatch).toBe(true);
    expect(result.series[0]).toMatchObject({ compared: true, mismatches: [] });
    expect(result.series[1]).toMatchObject({ compared: false, mismatches: [] });
  });

  it("reports slices that differ on either side", () => {
    const result = reconcileNorthDakotaChart({
      chart,
      csvTotalCents: 1_099,
      csvSlices: { "By Contributor Type": { Individual: bucket(999), Candidate: bucket(100) } },
    });
    expect(result.totalMatch).toBe(false);
    expect(result.series[0].mismatches).toEqual([
      { description: "Individual", chartCents: 1_000, csvCents: 999 },
      { description: "Lumpsum", chartCents: 100, csvCents: null },
      { description: "Candidate", chartCents: null, csvCents: 100 },
    ]);
  });
});

describe("reconcileNorthDakotaCommittee", () => {
  it("matches identical (date, amount) multisets regardless of category labels", () => {
    const result = reconcileNorthDakotaCommittee({
      entityId: "1010000001",
      csvRows: [csvRow({}), csvRow({ amountCents: 100, transactionDate: "2026-01-02", transactionCategory: "Total - $200 or less" })],
      apiRows: [apiRow({}), apiRow({ transactionAmount: 1, transactionDate: "2026-01-02T00:00:00", transactionCategoryDesc: "Lumpsum" })],
    });
    expect(result.totalsMatch).toBe(true);
    expect(result.multisetMatch).toBe(true);
    expect(result.csvCategoryCounts).toEqual({ Monetary: 1, "Total - $200 or less": 1 });
    expect(result.apiCategoryCounts).toEqual({ Monetary: 1, Lumpsum: 1 });
    expect(result.apiReports).toEqual([{ reportFileName: "2026 Pre-Primary Report", reportVersionID: "1", rowCount: 2, totalDollars: "961.60" }]);
  });

  it("surfaces asymmetric rows and ignores other committees", () => {
    const result = reconcileNorthDakotaCommittee({
      entityId: "1010000001",
      csvRows: [csvRow({}), csvRow({ registrantId: "1010000002" })],
      apiRows: [apiRow({ transactionAmount: 99 }), apiRow({ entityID: "1010000002" })],
    });
    expect(result.csvRowCount).toBe(1);
    expect(result.apiRowCount).toBe(1);
    expect(result.totalsMatch).toBe(false);
    expect(result.onlyInCsv).toBe(1);
    expect(result.onlyInApi).toBe(1);
  });
});

describe("summarizeNorthDakotaIndependentExpenditures", () => {
  const ieRow = (overrides: Partial<NorthDakotaTransactionRow>) =>
    apiRow({
      entityID: "1040001626",
      committeeName: "StrongND Fund",
      entityTypeDesc: "Business or Organization",
      transactionTypeDesc: "Independent Expenditures",
      orgType: "Independent Expenditure Committee",
      stanceDescription: "Support",
      s3ReportFilePath: "nd-cfs/Reports/1626/june.pdf",
      filedDate: "2026-06-08T00:00:00",
      transactionTotalYTD: "153999.9800",
      ...overrides,
    });

  it("sums unique transactionIDs and reconciles each payee's YTD control (live StrongND shape)", () => {
    // June filing: 25 allocations to one vendor; May filing: one row to a
    // different vendor. Each vendor's YTD equals its own rows.
    const rows = [
      ...Array.from({ length: 7 }, (_, i) => ieRow({ transactionID: 100 + i, transactionAmount: 16857.14, contributorPayeeID: 2, candidateNameAssocation: `Cand ${i}` })),
      ...Array.from({ length: 18 }, (_, i) => ieRow({ transactionID: 200 + i, transactionAmount: 2000, contributorPayeeID: 2, candidateNameAssocation: `Cand ${10 + i}` })),
      ieRow({ transactionID: 300, transactionAmount: 44281.36, contributorPayeeID: 1, s3ReportFilePath: "nd-cfs/Reports/1626/may.pdf", transactionTotalYTD: "44281.3600", filedDate: "2026-06-01T00:00:00" }),
    ];
    const summary = summarizeNorthDakotaIndependentExpenditures(rows);
    expect(summary.rowCount).toBe(26);
    expect(summary.distinctTransactionIdCount).toBe(26);
    expect(summary.offTypeRowCount).toBe(0);
    expect(summary.totalDollars).toBe("198281.34");
    expect(summary.reports.map((report) => [report.rowCount, report.sumDollars])).toEqual([[1, "44281.36"], [25, "153999.98"]]);
    expect(summary.payeeYtd).toEqual({ groupCount: 2, matchingGroupCount: 2, missingControlGroupCount: 0, mismatches: [] });
    expect(summary.committees).toEqual([
      { entityId: "1040001626", committeeName: "StrongND Fund", reportCount: 2, rowCount: 26, totalDollars: "198281.34" },
    ]);
    expect(summary.stanceCounts).toEqual({ Support: 26 });
  });

  it("flags repeated ids, off-type rows and YTD mismatches", () => {
    const summary = summarizeNorthDakotaIndependentExpenditures([
      ieRow({ transactionID: 1, transactionAmount: 100, transactionTotalYTD: "300.0000" }),
      ieRow({ transactionID: 1, transactionAmount: 100, transactionTotalYTD: "300.0000" }),
      ieRow({ transactionID: 2, transactionAmount: 50, transactionTotalYTD: "300.0000", transactionTypeDesc: "Contributions" }),
    ]);
    expect(summary.distinctTransactionIdCount).toBe(2);
    expect(summary.offTypeRowCount).toBe(1);
    expect(summary.totalCents).toBe(15_000);
    expect(summary.payeeYtd.mismatches).toEqual([
      { entityId: "1040001626", counterparty: "id:10", sumCents: 15_000, maxYtdCents: 30_000, rowCount: 2 },
    ]);
  });
});

describe("checkNorthDakotaYtdSemantics", () => {
  it("treats YTD as the running committee x counterparty aggregate (live NDPS shape)", () => {
    const rows = [
      apiRow({ transactionID: 1, contributorPayeeID: 5, transactionAmount: 2414.57, transactionTotalYTD: "2414.5700" }),
      apiRow({ transactionID: 2, contributorPayeeID: 5, transactionAmount: 1917.66, transactionTotalYTD: "4332.2300" }),
      apiRow({ transactionID: 3, contributorPayeeID: 5, transactionAmount: 2383.86, transactionTotalYTD: "6716.0900" }),
      apiRow({ transactionID: 4, contributorPayeeID: null, contributorPayeeName: "Total - $200 or less", transactionAmount: 100, transactionTotalYTD: null }),
    ];
    expect(checkNorthDakotaYtdSemantics(rows)).toEqual({ groupCount: 2, matchingGroupCount: 1, missingControlGroupCount: 1, mismatches: [] });
  });
});

describe("summarizeNorthDakotaOccupations", () => {
  it("aggregates individual donations per committee+donor and measures the $5,000 threshold by office class", () => {
    const rows = [
      // Legislative committee: one donor reaches $5,000 across two gifts, with occupation on one.
      apiRow({ transactionID: 1, transactionAmount: 3000, contributorPayeeID: 7, employerOccupation: "Healthcare/Medical" }),
      apiRow({ transactionID: 2, transactionAmount: 2000, contributorPayeeID: 7, employerOccupation: null }),
      apiRow({ transactionID: 3, transactionAmount: 4999, contributorPayeeID: 8, employerOccupation: null }),
      // Judicial committee: a $15,000 donor with no occupation (statutorily exempt).
      apiRow({ transactionID: 4, entityID: "1010000002", transactionAmount: 15000, contributorPayeeID: 9 }),
      // Not a donation row / not an individual / not a candidate committee.
      apiRow({ transactionID: 5, transactionAmount: 9000, transactionCategoryDesc: "Reimbursement of Expenditure" }),
      apiRow({ transactionID: 6, transactionAmount: 9000, entityTypeDesc: "Committee/PAC" }),
      apiRow({ transactionID: 7, entityID: "1030000004", orgType: "Party Committee", transactionAmount: 9000 }),
    ];
    const summary = summarizeNorthDakotaOccupations(rows, committeesById);
    expect(summary.byOfficeClass.legislative).toMatchObject({
      committeeCount: 1,
      individualRowCount: 3,
      individualCents: 999_900,
      donorsAtThreshold: 1,
      donorsAtThresholdWithOccupation: 1,
      centsAtThreshold: 500_000,
      centsAtThresholdWithOccupation: 300_000,
      committeesPassingDisplayGate: 0,
    });
    expect(summary.byOfficeClass.judicial).toMatchObject({ donorsAtThreshold: 1, donorsAtThresholdWithOccupation: 0 });
    expect(summary.byOfficeClass.statewide.committeeCount).toBe(0);
    expect(summary.distinctOccupations).toEqual([{ value: "Healthcare/Medical", count: 1 }]);
  });
});

describe("summarizeNorthDakotaRegistry / cycle window / reporting cycles", () => {
  it("counts current-cycle candidate committees with a complete identity", () => {
    const registry = summarizeNorthDakotaRegistry(
      [
        committee({}),
        committee({ entityId: "1010000002", office: "Supreme Court Justice", district: null }),
        committee({ entityId: "1010000005", office: "State Senator", district: null }),
        committee({ entityId: "1010000006", election: "2024 Election - Statewide" }),
        committee({ entityId: "1030000004", orgType: "Party Committee", office: null }),
      ],
      "2026 Election - Statewide"
    );
    expect(registry.currentCycleCandidates).toMatchObject({
      count: 3,
      active: 3,
      byOfficeClass: { statewide: 0, legislative: 2, judicial: 1, unknown: 0 },
      completeIdentity: 2,
      distinctOffices: ["State Representative", "State Senator", "Supreme Court Justice"],
    });
    expect(registry.byOrgType).toEqual({ "Candidate/Candidate Committee": 4, "Party Committee": 1 });
  });

  it("maps prior-year candidate activity to registry elections", () => {
    const check = checkNorthDakotaCycleWindow({
      priorYear: 2025,
      priorYearRows: [csvRow({}), csvRow({ registrantId: "1010000003" }), csvRow({ registrantId: "1030000004" }), csvRow({ registrantId: "1099999999" })],
      committeesById,
    });
    expect(check).toEqual({
      priorYear: 2025,
      candidateRegistrantsWithPriorYearActivity: 2,
      byElection: { "2026 Election - Statewide": 2 },
    });
  });

  it("groups reporting schedule rows by election", () => {
    const base = { line: 2, reportingPeriodDescription: "x", formType: "Campaign Financial Statement", reportType: "Year End", dueDate: "2026-01-31" };
    const cycles = summarizeNorthDakotaReportingCycles([
      { ...base, electionName: "2026 Election - Statewide", reportingCycle: "2025 REPORTING CYCLE", beginDate: "2025-01-01", endDate: "2025-12-31" },
      { ...base, electionName: "2026 Election - Statewide", reportingCycle: "2026 REPORTING CYCLE", beginDate: "2026-01-01", endDate: "2026-04-30" },
    ]);
    expect(cycles).toEqual([
      {
        electionName: "2026 Election - Statewide",
        reportingCycles: ["2025 REPORTING CYCLE", "2026 REPORTING CYCLE"],
        periodCount: 2,
        earliestBeginDate: "2025-01-01",
        latestEndDate: "2026-04-30",
      },
    ]);
  });
});

describe("evaluateNorthDakotaPhaseZeroGates", () => {
  const greenChart = { totalMatch: true, series: [{ name: "By Contributor Type", chartTotalCents: 1, compared: true, mismatches: [] }] };
  const green = {
    contributionChart: greenChart,
    expenditureChart: greenChart,
    unknownContributionCategories: [],
    unknownExpenditureTypes: [],
    registryJoin: { csvRegistrantCount: 5, matchedCount: 5, unmatchedRegistrantIds: [] },
    reconciliations: [{ entityId: "1010000001", csvRowCount: 2, apiRowCount: 2, totalsMatch: true, multisetMatch: true }],
    independentExpenditures: {
      rowCount: 52,
      distinctTransactionIdCount: 52,
      offTypeRowCount: 0,
      payeeYtd: { groupCount: 4, matchingGroupCount: 4, missingControlGroupCount: 0, mismatches: [] },
      totalCents: 100,
    },
    independentExpenditureChartTotalCents: 100,
    cycleWindow: { priorYear: 2025, candidateRegistrantsWithPriorYearActivity: 40, byElection: { "2026 Election - Statewide": 40 } },
    reportingCycles: [
      { electionName: "2026 Election - Statewide", reportingCycles: ["2025 REPORTING CYCLE"], periodCount: 1, earliestBeginDate: "2025-01-01", latestEndDate: "2025-12-31" },
    ],
    occupations: {
      byOfficeClass: {
        statewide: { committeeCount: 0, individualRowCount: 0, individualCents: 0, donorsAtThreshold: 0, donorsAtThresholdWithOccupation: 0, centsAtThreshold: 0, centsAtThresholdWithOccupation: 0, committeesPassingDisplayGate: 0 },
        legislative: { committeeCount: 1, individualRowCount: 3, individualCents: 1, donorsAtThreshold: 2, donorsAtThresholdWithOccupation: 2, centsAtThreshold: 1, centsAtThresholdWithOccupation: 1, committeesPassingDisplayGate: 1 },
        judicial: { committeeCount: 0, individualRowCount: 0, individualCents: 0, donorsAtThreshold: 0, donorsAtThresholdWithOccupation: 0, centsAtThreshold: 0, centsAtThresholdWithOccupation: 0, committeesPassingDisplayGate: 0 },
        unknown: { committeeCount: 0, individualRowCount: 0, individualCents: 0, donorsAtThreshold: 0, donorsAtThresholdWithOccupation: 0, centsAtThreshold: 0, centsAtThresholdWithOccupation: 0, committeesPassingDisplayGate: 0 },
      },
    },
    registry: {
      currentCycleCandidates: {
        election: "2026 Election - Statewide",
        count: 200,
        active: 190,
        byOfficeClass: { statewide: 10, legislative: 180, judicial: 5, unknown: 5 },
        completeIdentity: 195,
        distinctOffices: [],
      },
    },
  };

  it("passes when every gate holds", () => {
    expect(evaluateNorthDakotaPhaseZeroGates(green)).toEqual([]);
  });

  it("names every failing gate", () => {
    const failures = evaluateNorthDakotaPhaseZeroGates({
      ...green,
      contributionChart: { totalMatch: false, series: [{ name: "By Contributor Type", chartTotalCents: 1, compared: true, mismatches: [{ description: "Lumpsum", chartCents: 1, csvCents: 2 }] }] },
      unknownContributionCategories: ["Loan"],
      registryJoin: { csvRegistrantCount: 5, matchedCount: 4, unmatchedRegistrantIds: ["1019999999"] },
      reconciliations: [
        { entityId: "1010000001", csvRowCount: 0, apiRowCount: 0, totalsMatch: true, multisetMatch: true },
        { entityId: "1010000002", csvRowCount: 3, apiRowCount: 3, totalsMatch: true, multisetMatch: false },
      ],
      independentExpenditures: {
        ...green.independentExpenditures,
        distinctTransactionIdCount: 51,
        totalCents: 99,
        payeeYtd: { groupCount: 4, matchingGroupCount: 3, missingControlGroupCount: 0, mismatches: [{ entityId: "1040001621", counterparty: "id:1", sumCents: 1, maxYtdCents: 2, rowCount: 1 }] },
      },
      cycleWindow: { priorYear: 2025, candidateRegistrantsWithPriorYearActivity: 3, byElection: {} },
      reportingCycles: [],
      registry: { currentCycleCandidates: { ...green.registry.currentCycleCandidates, completeIdentity: 4 } },
    });
    expect(failures).toEqual([
      "contributions chart: CSV total differs from the portal chart total",
      'contributions chart "By Contributor Type": 1 slice(s) differ (Lumpsum)',
      "contribution categories not in the pinned vocabulary: Loan",
      "registry join: 1 CSV registrants missing from the committee registry",
      "reconciliation 1010000001: empty sample (csv 0, api 0)",
      "reconciliation 1010000002: CSV and API rows differ",
      "independent expenditures: repeated transactionIDs",
      "independent expenditures: 1 payee group(s) do not sum to their YTD control (0 without a control)",
      "independent expenditures: unique-row total differs from the portal chart total",
      "cycle window: only 3 candidate registrants with 2025 activity",
      'cycle window: reporting schedules do not map "2025 REPORTING CYCLE" to an election',
      "resolver gold set: only 4 current-cycle candidate committees carry a complete office identity",
    ]);
  });
});
