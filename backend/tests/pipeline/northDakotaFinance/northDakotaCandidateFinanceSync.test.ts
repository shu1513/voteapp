import { describe, expect, it, vi } from "vitest";

import type { NorthDakotaTransactionRow } from "../../../src/pipeline/northDakotaFinance/northDakotaCfrsClient.js";
import {
  parseNorthDakotaContributionCsv,
  parseNorthDakotaReportingScheduleCsv,
  type NorthDakotaContributionCsvRow,
} from "../../../src/pipeline/northDakotaFinance/northDakotaCfrsCsv.js";
import {
  syncNorthDakotaCandidateFinance,
  type NorthDakotaFinanceArtifactLoader,
} from "../../../src/pipeline/northDakotaFinance/northDakotaCandidateFinanceSync.js";

const LINK_ID = "33333333-3333-4333-8333-333333333333";
const ENTITY = "1010001478";

const CON_HEADER =
  "RegistrantID,CommitteeName,CandidateName,TransactionType,TransactionCategory,TransactionDate,TransactionAmount,ContributorPayeeType,ContributorPayeeName,ContributorAddress,EmployerName,FiledDate";
const REPS_HEADER = "ElectionName,ReportingCycle,ReportingPeriodDescription,FormType,ReportType,BeginDate,Enddate,DueDate";

const SCHEDULES: Record<number, string[]> = {
  2026: [
    "2026 Election - Statewide,2025 REPORTING CYCLE,2025 Year End Report,Campaign Financial Statement,Year End,2025-01-01,2025-12-31,2026-01-31",
    "2026 Election - Statewide,2026 Reporting Cycle,2026 Pre-General Report,Campaign Financial Statement,Pre-General,2026-01-01,2026-09-24,2026-10-02",
    "2026 Election - Statewide,2026 Reporting Cycle,2026 Pre-Primary Report,Campaign Financial Statement,Pre-Primary,2026-01-01,2026-04-30,2026-05-08",
  ],
  2027: [
    "2026 Election - Statewide,2026 Reporting Cycle,2026 Year End Report,Campaign Financial Statement,Year End,2026-01-01,2026-12-31,2027-01-31",
  ],
};

const CONTRIBUTIONS: Record<number, string[]> = {
  2025: [`${ENTITY},Friends of Jane Doe,Doe Jane,Contributions,Monetary,2025-08-15,250.0000,Individual,Early Ann,1 Elm St,Acme Coal,2026-01-15`],
  2026: [
    `${ENTITY},Friends of Jane Doe,Doe Jane,Contributions,Monetary,2026-03-01,500.0000,Individual,Roe Richard,2 Oak St,Sanford Health,2026-05-01`,
    `${ENTITY},Friends of Jane Doe,Doe Jane,Contributions,Monetary,2026-03-02,1000.0000,Committee/PAC,Prairie PAC,3 Pine St,,2026-05-01`,
    `${ENTITY},Friends of Jane Doe,Doe Jane,Contributions,Monetary,2026-03-03,3000.0000,Candidate,Doe Jane,4 Ash St,,2026-05-01`,
    `${ENTITY},Friends of Jane Doe,Doe Jane,Contributions,Total - $200 or less,2026-04-30,640.0000,,,,,2026-05-01`,
    `1010009999,Other Committee,Other Person,Contributions,Monetary,2026-03-03,99.0000,Individual,Someone Else,5 Fir St,,2026-05-01`,
  ],
};

function apiRow(overrides: Partial<NorthDakotaTransactionRow>): NorthDakotaTransactionRow {
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
    employerName: "Sanford Health",
    employerOccupation: "Healthcare/Medical",
    transactionTotalYTD: "500",
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

const API_ROWS: Record<number, NorthDakotaTransactionRow[]> = {
  2025: [apiRow({ transactionID: 10, transactionAmount: 250, transactionDate: "2025-08-15T00:00:00", contributorPayeeName: "Early Ann", contributorPayeeID: 76 })],
  2026: [
    apiRow({ transactionID: 11 }),
    apiRow({ transactionID: 12, transactionAmount: 1000, transactionDate: "2026-03-02T00:00:00", entityTypeDesc: "Committee/PAC", employerOccupation: null }),
    apiRow({ transactionID: 13, transactionAmount: 3000, transactionDate: "2026-03-03T00:00:00", entityTypeDesc: "Candidate", employerOccupation: null }),
    apiRow({ transactionID: 14, transactionAmount: 640, transactionDate: "2026-04-30T00:00:00", entityTypeDesc: null, transactionCategoryDesc: "Total - $200 or less", contributorPayeeName: null, contributorPayeeID: null, employerOccupation: null }),
    // Another committee's row never disturbs this committee's reconciliation.
    apiRow({ transactionID: 15, entityID: "1010009999", transactionAmount: 99, transactionDate: "2026-03-03T00:00:00" }),
  ],
};

function loader(overrides: Partial<NorthDakotaFinanceArtifactLoader> = {}): NorthDakotaFinanceArtifactLoader {
  const parseCon = (lines: string[]): NorthDakotaContributionCsvRow[] =>
    parseNorthDakotaContributionCsv(`${CON_HEADER}\r\n${lines.join("\r\n")}\r\n`).rows;
  const missing = (label: string, year: number) =>
    Promise.reject(new Error(`North Dakota CFRS artifact ${label} ${year} has no cached metadata`));
  return {
    scheduleRows: vi.fn(async (year: number) =>
      SCHEDULES[year] ? parseNorthDakotaReportingScheduleCsv(`${REPS_HEADER}\r\n${SCHEDULES[year]!.join("\r\n")}\r\n`).rows : missing("reporting_schedules", year)
    ),
    contributionRows: vi.fn(async (year: number) => (CONTRIBUTIONS[year] ? parseCon(CONTRIBUTIONS[year]!) : missing("contributions", year))),
    apiContributionRows: vi.fn(async (year: number) => API_ROWS[year] ?? missing("api_contributions", year)),
    ...overrides,
  };
}

function writingDb() {
  const client = {
    query: vi.fn((sql: unknown) => {
      if (String(sql).includes("INSERT INTO public.nd_candidate_finance_links")) {
        return Promise.resolve({ rows: [{ id: LINK_ID }], rowCount: 1 });
      }
      return Promise.resolve({ rows: [], rowCount: 0 });
    }),
    release: vi.fn(),
  };
  const db = { query: vi.fn(), connect: vi.fn().mockResolvedValue(client) };
  return { db, client };
}

function baseInput(db: { query: unknown; connect: unknown }) {
  return {
    db: db as never,
    candidateId: "candidate-1",
    electionId: "election-1",
    candidateName: "Jane Doe",
    electionYear: 2026,
    officeScope: "state_upper",
    officeName: "State Senator",
    district: "State Senate District 11 (2024); North Dakota",
    link: {
      entityId: ENTITY,
      committeeName: "Friends of Jane Doe",
      linkSource: "cfrs_registry" as const,
      sourceUrl: null,
    },
    now: new Date("2026-09-02T00:00:00Z"),
  };
}

describe("syncNorthDakotaCandidateFinance", () => {
  it("resolves the window, reconciles every year, aggregates and writes one snapshot", async () => {
    const { db, client } = writingDb();
    const artifacts = loader();
    const result = await syncNorthDakotaCandidateFinance({ ...baseInput(db), loadArtifacts: artifacts });
    expect(result).toMatchObject({
      status: "synced",
      entityId: ENTITY,
      window: { election: "2026 Election - Statewide", windowStart: "2025-01-01", windowEnd: "2026-12-31" },
      windowYears: [2025, 2026],
      // 250 + 500 + 1000 + 3000 + 640
      totalReceipts: 5390,
      // minus the candidate's own 3000
      directContributionTotal: 2390,
      aggregation: {
        unitemizedCents: 64_000,
        selfFundingCents: 300_000,
        lumpRowCount: 1,
        // Two labeled individuals (250 + 500): under the three-donor gate, so no occupation rows.
        occupation: { individualCents: 75_000, occupationCents: 75_000, donorCount: 2, occupationDonorCount: 2, displayGatePassed: false },
      },
      breakdownCounts: { occupation: 0, contribution_size: 2 },
      summaryWritten: true,
      directBreakdownsWritten: 2,
    });
    expect(result.reconciliation).toEqual([
      { year: 2025, csvRowCount: 1, apiRowCount: 1, csvTotalCents: 25_000, apiTotalCents: 25_000, amendedApiRowCount: 0 },
      { year: 2026, csvRowCount: 4, apiRowCount: 4, csvTotalCents: 514_000, apiTotalCents: 514_000, amendedApiRowCount: 0 },
    ]);
    expect(artifacts.scheduleRows).toHaveBeenCalledTimes(2);
    const summaryInsert = client.query.mock.calls.find((call) =>
      String(call[0]).includes("INSERT INTO public.nd_candidate_finance_summaries")
    );
    expect(summaryInsert).toBeDefined();
    // Spending, cash and both outside totals are passed as NULL so the
    // writer's preserve-when-NULL policy leaves other phases' columns alone.
    expect(summaryInsert?.[1]).toEqual([LINK_ID, 2026, 5390, 2390, null, null, null, null, "https://cfrs.sos.nd.gov/", "2026-09-02T00:00:00.000Z"]);
    const breakdownInserts = client.query.mock.calls.filter((call) =>
      String(call[0]).includes("INSERT INTO public.nd_candidate_finance_direct_breakdowns")
    );
    expect(breakdownInserts.map((call) => [call[1]![2], call[1]![3], call[1]![4]])).toEqual([
      ["contribution_size", "$500-$999", 500],
      ["contribution_size", "$250-$499", 250],
    ]);
  });

  it("writes filed occupations from the reconciled API rows when the display gate passes", async () => {
    const { db, client } = writingDb();
    const extraCsv = [
      `${ENTITY},Friends of Jane Doe,Doe Jane,Contributions,Monetary,2026-03-05,5000.0000,Individual,Poe Paula,6 Elm St,Poe Farms,2026-05-01`,
      `${ENTITY},Friends of Jane Doe,Doe Jane,Contributions,In-Kind,2026-03-06,250.0000,Individual,Quinn Quincy,7 Elm St,,2026-05-01`,
    ];
    const extraApi = [
      apiRow({ transactionID: 16, transactionAmount: 5000, transactionDate: "2026-03-05T00:00:00", contributorPayeeName: "Poe Paula", contributorPayeeID: 78, employerName: "Poe Farms", employerOccupation: "Agriculture" }),
      apiRow({ transactionID: 17, transactionAmount: 250, transactionDate: "2026-03-06T00:00:00", transactionCategoryDesc: "In-Kind", contributorPayeeName: "Quinn Quincy", contributorPayeeID: 79, employerName: null, employerOccupation: " Attorney/Legal " }),
    ];
    const result = await syncNorthDakotaCandidateFinance({
      ...baseInput(db),
      loadArtifacts: loader({
        contributionRows: vi.fn(async (year: number) =>
          parseNorthDakotaContributionCsv(`${CON_HEADER}\r\n${[...CONTRIBUTIONS[year]!, ...(year === 2026 ? extraCsv : [])].join("\r\n")}\r\n`).rows
        ),
        apiContributionRows: vi.fn(async (year: number) => (year === 2026 ? [...API_ROWS[2026]!, ...extraApi] : API_ROWS[year]!)),
      }),
    });
    expect(result).toMatchObject({
      status: "synced",
      totalReceipts: 10_640,
      directContributionTotal: 7640,
      // Four labeled individuals: 250 + 500 + 5000 + 250, all with an occupation.
      aggregation: { occupation: { individualCents: 600_000, occupationCents: 600_000, donorCount: 4, occupationDonorCount: 4, displayGatePassed: true } },
      breakdownCounts: { occupation: 3, contribution_size: 3 },
      directBreakdownsWritten: 6,
    });
    const breakdownInserts = client.query.mock.calls.filter((call) =>
      String(call[0]).includes("INSERT INTO public.nd_candidate_finance_direct_breakdowns")
    );
    expect(breakdownInserts.map((call) => [call[1]![2], call[1]![3], call[1]![4], call[1]![5]])).toEqual([
      ["occupation", "Agriculture", 5000, 1],
      ["occupation", "Healthcare/Medical", 750, 2],
      ["occupation", "Attorney/Legal", 250, 1],
      ["contribution_size", "$5,000+", 5000, 1],
      ["contribution_size", "$500-$999", 500, 1],
      ["contribution_size", "$250-$499", 250, 1],
    ]);
  });

  it("writes NULL money, not $0, for a committee with no filed rows in the window", async () => {
    const { db, client } = writingDb();
    const base = baseInput(db);
    // A registered committee whose first cumulative report is not yet due:
    // neither file carries a row for it.
    const result = await syncNorthDakotaCandidateFinance({
      ...base,
      link: { ...base.link, entityId: "1010001624", committeeName: "Nelson for Tax Commissioner" },
      loadArtifacts: loader(),
    });
    expect(result).toMatchObject({
      status: "no_filed_rows",
      totalReceipts: null,
      directContributionTotal: null,
      aggregation: { contributionRowCount: 0, occupation: { donorCount: 0, displayGatePassed: false } },
      breakdownCounts: { occupation: 0, contribution_size: 0 },
      summaryWritten: true,
      directBreakdownsWritten: 0,
    });
    expect(result.reconciliation.map((year) => year.csvRowCount)).toEqual([0, 0]);
    const summaryInsert = client.query.mock.calls.find((call) =>
      String(call[0]).includes("INSERT INTO public.nd_candidate_finance_summaries")
    );
    expect(summaryInsert?.[1]).toEqual([LINK_ID, 2026, null, null, null, null, null, null, "https://cfrs.sos.nd.gov/", "2026-09-02T00:00:00.000Z"]);
  });

  it("dry run computes without touching the database", async () => {
    const { db, client } = writingDb();
    const result = await syncNorthDakotaCandidateFinance({ ...baseInput(db), dryRun: true, loadArtifacts: loader() });
    expect(result).toMatchObject({ dryRun: true, summaryWritten: false, directBreakdownsWritten: 0, totalReceipts: 5390 });
    expect(db.connect).not.toHaveBeenCalled();
    expect(client.query).not.toHaveBeenCalled();
  });

  it("fails closed when the CSV and API rows for a year disagree", async () => {
    const { db, client } = writingDb();
    await expect(
      syncNorthDakotaCandidateFinance({
        ...baseInput(db),
        loadArtifacts: loader({
          apiContributionRows: vi.fn(async (year: number) =>
            year === 2026 ? API_ROWS[2026]!.filter((row) => row.transactionID !== 12) : API_ROWS[year]!
          ),
        }),
      })
    ).rejects.toThrow(/2026 contributions do not reconcile for 1010001478: CSV 4 rows \/ 514000c vs API 3 rows \/ 414000c/);
    expect(client.query).not.toHaveBeenCalled();
  });

  it("fails closed when the API reclassifies a same-date, same-amount row (donor money vs the candidate's own)", async () => {
    const { db, client } = writingDb();
    await expect(
      syncNorthDakotaCandidateFinance({
        ...baseInput(db),
        loadArtifacts: loader({
          apiContributionRows: vi.fn(async (year: number) =>
            year === 2026
              ? API_ROWS[2026]!.map((row) => (row.transactionID === 11 ? { ...row, entityTypeDesc: "Candidate" } : row))
              : API_ROWS[year]!
          ),
        }),
      })
    ).rejects.toThrow(/2026 contributions do not reconcile for 1010001478: .*\(only-in-CSV 1, only-in-API 1\)/);
    expect(client.query).not.toHaveBeenCalled();
  });

  it("fails closed when a window year's artifact or a schedule file is not cached", async () => {
    const { db, client } = writingDb();
    await expect(
      syncNorthDakotaCandidateFinance({
        ...baseInput(db),
        loadArtifacts: loader({ contributionRows: vi.fn(async () => Promise.reject(new Error("no cached metadata"))) }),
      })
    ).rejects.toThrow(/no cached metadata/);
    await expect(
      syncNorthDakotaCandidateFinance({
        ...baseInput(db),
        loadArtifacts: loader({
          scheduleRows: vi.fn(async (year: number) => (year === 2027 ? Promise.reject(new Error("no cached metadata")) : [])),
        }),
      })
    ).rejects.toThrow(/no cached metadata/);
    expect(client.query).not.toHaveBeenCalled();
  });

  it("fails closed on vocabulary outside the pinned sets", async () => {
    const { db, client } = writingDb();
    const extra = `${ENTITY},Friends of Jane Doe,Doe Jane,Contributions,Loan,2026-03-04,120.0000,Individual,Lender Lou,1 Elm,,2026-05-01`;
    await expect(
      syncNorthDakotaCandidateFinance({
        ...baseInput(db),
        loadArtifacts: loader({
          contributionRows: vi.fn(async (year: number) =>
            parseNorthDakotaContributionCsv(`${CON_HEADER}\r\n${[...CONTRIBUTIONS[year]!, ...(year === 2026 ? [extra] : [])].join("\r\n")}\r\n`).rows
          ),
          apiContributionRows: vi.fn(async (year: number) =>
            year === 2026
              ? [...API_ROWS[2026]!, apiRow({ transactionID: 16, transactionAmount: 120, transactionDate: "2026-03-04T00:00:00", transactionCategoryDesc: "Loan" })]
              : API_ROWS[year]!
          ),
        }),
      })
    ).rejects.toThrow(/unrecognized values in window for 1010001478: contribution category "Loan"/);
    expect(client.query).not.toHaveBeenCalled();
  });

  it("rejects ineligible offices, bad ids and pre-2026 years before reading anything", async () => {
    const { db } = writingDb();
    const artifacts = loader();
    await expect(
      syncNorthDakotaCandidateFinance({ ...baseInput(db), officeScope: "county", officeName: "Sheriff", loadArtifacts: artifacts })
    ).rejects.toThrow(/not North Dakota-finance eligible/);
    const base = baseInput(db);
    await expect(
      syncNorthDakotaCandidateFinance({ ...base, link: { ...base.link, entityId: "123" }, loadArtifacts: artifacts })
    ).rejects.toThrow(/Invalid North Dakota CFRS entityId/);
    await expect(syncNorthDakotaCandidateFinance({ ...base, electionYear: 2024, loadArtifacts: artifacts })).rejects.toThrow(
      /invalid election year/
    );
    expect(artifacts.scheduleRows).not.toHaveBeenCalled();
  });
});
