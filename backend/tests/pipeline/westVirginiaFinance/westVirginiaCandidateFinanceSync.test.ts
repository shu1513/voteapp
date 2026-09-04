import { describe, expect, it, vi } from "vitest";

import type { WestVirginiaTransactionRow } from "../../../src/pipeline/westVirginiaFinance/westVirginiaCfrsClient.js";
import {
  parseWestVirginiaContributionCsv,
  parseWestVirginiaExpenditureCsv,
  parseWestVirginiaReportingScheduleCsv,
  type WestVirginiaContributionCsvRow,
  type WestVirginiaExpenditureCsvRow,
} from "../../../src/pipeline/westVirginiaFinance/westVirginiaCfrsCsv.js";
import {
  syncWestVirginiaCandidateFinance,
  type WestVirginiaFinanceArtifactLoader,
} from "../../../src/pipeline/westVirginiaFinance/westVirginiaCandidateFinanceSync.js";

const LINK_ID = "33333333-3333-4333-8333-333333333333";
const ENTITY = "1010003610";

const CON_HEADER =
  "RegistrantID,CommitteeName,CandidateName,TransactionType,TransactionCategory,TransactionDate,TransactionAmount,ContributorPayeeType,ContributorPayeeName,ContributorAddress,EmployerName,FiledDate";
const EXP_HEADER =
  "RegistrantID,CommitteeName,CandidateName,TransactionType,ExpenditureType,ExpenditurePurpose,TransactionDate,TransactionAmount,RecipientType,RecipientName,RecipientAddress,FiledDate";
const REPS_HEADER = "ElectionName,ReportingCycle,ReportingPeriodDescription,FormType,ReportType,BeginDate,Enddate,DueDate";

const SCHEDULES: Record<number, string[]> = {
  2025: ["2026 Election,2026 Candidate Election Cycle,2025 3rd Quarter Report,Campaign Financial Statement,Quarterly,2025-07-01,2025-09-30,2025-10-07"],
  2026: [
    "2026 Election,2026 Candidate Election Cycle,2025 4th Quarter Report,Campaign Financial Statement,Quarterly,2025-10-01,2025-12-31,2026-01-07",
    "2026 Election,2026 Candidate Election Cycle,2026 1st Quarter Report,Campaign Financial Statement,Quarterly,2026-01-01,2026-03-31,2026-04-07",
    "2026 Election,2026 Candidate Election Cycle,2026 General Report,Campaign Financial Statement,General,2026-10-01,2026-10-18,2026-10-23",
  ],
  2027: ["2026 Election,2026 Candidate Election Cycle,2026 4th Quarter Report,Campaign Financial Statement,Quarterly,2026-10-19,2026-12-31,2027-01-07"],
};

const CONTRIBUTIONS: Record<number, string[]> = {
  2025: [`${ENTITY},Committee to Elect Dean Jeffries,Warren Dean Jeffries,Contributions,Monetary,2025-08-15,250.0000,Individual,Ann Early,1 Elm St,Acme Coal,2025-10-07`],
  2026: [
    `${ENTITY},Committee to Elect Dean Jeffries,Warren Dean Jeffries,Contributions,Monetary,2026-03-01,500.0000,Individual,Jane Doe,2 Oak St,Charleston General Hospital,2026-04-07`,
    `${ENTITY},Committee to Elect Dean Jeffries,Warren Dean Jeffries,Contributions,Monetary,2026-03-02,1000.0000,Political Action Committee,WV Realtors PAC,3 Pine St,,2026-04-07`,
    `${ENTITY},Committee to Elect Dean Jeffries,Warren Dean Jeffries,Contributions,Monetary,2026-03-03,3000.0000,Self,Warren Dean Jeffries,4 Ash St,,2026-04-07`,
    `1010009999,Other Committee,Other Person,Contributions,Monetary,2026-03-03,99.0000,Individual,Someone Else,5 Fir St,,2026-04-07`,
  ],
};

const EXPENDITURES: Record<number, string[]> = {
  2025: [],
  2026: [`${ENTITY},Committee to Elect Dean Jeffries,Warren Dean Jeffries,Expenditures,Monetary,Advertising,2026-03-05,120.0000,Business or Organization,Acme Print,6 Elm St,2026-04-07`],
};

function apiRow(overrides: Partial<WestVirginiaTransactionRow>): WestVirginiaTransactionRow {
  return {
    transactionID: 1,
    entityID: ENTITY,
    orgID: 3610,
    committeeName: "Committee to Elect Dean Jeffries",
    candidateName: "Jeffries, Warren Dean",
    transactionAmount: 500,
    transactionDate: "2026-03-01T00:00:00",
    filedDate: "2026-04-07T00:00:00",
    entityTypeDesc: "Individual",
    transactionCategoryDesc: "Monetary",
    transactionTypeDesc: "Contributions",
    transactionPurpose: null,
    contributorPayeeName: "Doe Jane",
    employerName: "Charleston General Hospital",
    employerOccupation: "Healthcare/Medical",
    transactionTotalYTD: null,
    amendedFlag: false,
    reportVersionID: "1",
    reportFileName: null,
    s3ReportFilePath: null,
    stanceDescription: null,
    candidateNameAssocation: null,
    ballotMeasureDescription: null,
    orgType: "State Candidate",
    ...overrides,
  };
}

const API_ROWS: Record<number, WestVirginiaTransactionRow[]> = {
  2025: [apiRow({ transactionID: 10, transactionAmount: 250, transactionDate: "2025-08-15T00:00:00", contributorPayeeName: "Early Ann", employerOccupation: "Retired" })],
  2026: [
    apiRow({ transactionID: 11 }),
    apiRow({ transactionID: 12, transactionAmount: 1000, transactionDate: "2026-03-02T00:00:00", entityTypeDesc: "Political Action Committee", employerOccupation: null }),
    apiRow({ transactionID: 13, transactionAmount: 3000, transactionDate: "2026-03-03T00:00:00", entityTypeDesc: "Self", employerOccupation: null }),
    // A loan row rides along on the CON selector and must not disturb the reconciliation.
    apiRow({ transactionID: 14, transactionAmount: 7000, transactionCategoryDesc: "Loans", transactionTypeDesc: "Loans", entityTypeDesc: "Self" }),
  ],
};

function loader(overrides: Partial<WestVirginiaFinanceArtifactLoader> = {}): WestVirginiaFinanceArtifactLoader {
  const parseCon = (lines: string[]): WestVirginiaContributionCsvRow[] =>
    parseWestVirginiaContributionCsv(`${CON_HEADER}\n${lines.join("\n")}\n`).rows;
  const parseExp = (lines: string[]): WestVirginiaExpenditureCsvRow[] =>
    parseWestVirginiaExpenditureCsv(`${EXP_HEADER}\n${lines.join("\n")}\n`).rows;
  const missing = (label: string, year: number) => Promise.reject(new Error(`West Virginia CFRS artifact ${label} ${year} has no cached metadata`));
  return {
    scheduleRows: vi.fn(async (year: number) =>
      SCHEDULES[year] ? parseWestVirginiaReportingScheduleCsv(`${REPS_HEADER}\n${SCHEDULES[year]!.join("\n")}\n`).rows : missing("reporting_schedules", year)
    ),
    contributionRows: vi.fn(async (year: number) => (CONTRIBUTIONS[year] ? parseCon(CONTRIBUTIONS[year]!) : missing("contributions", year))),
    expenditureRows: vi.fn(async (year: number) => (EXPENDITURES[year] ? parseExp(EXPENDITURES[year]!) : missing("expenditures", year))),
    apiContributionRows: vi.fn(async (year: number) => API_ROWS[year] ?? missing("api_contributions", year)),
    ...overrides,
  };
}

function writingDb() {
  const client = {
    query: vi.fn((sql: unknown) => {
      if (String(sql).includes("INSERT INTO public.wv_candidate_finance_links")) {
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
    candidateName: "Dean Jeffries",
    electionYear: 2026,
    officeScope: "state_lower",
    officeName: "State Lower Chamber Legislator",
    district: "Delegate District 12 (2024); West Virginia",
    link: {
      entityId: ENTITY,
      committeeName: "Committee to Elect Dean Jeffries",
      linkSource: "cfrs_registry" as const,
      sourceUrl: null,
    },
    now: new Date("2026-09-01T00:00:00Z"),
  };
}

describe("syncWestVirginiaCandidateFinance", () => {
  it("resolves the window, reconciles every year, aggregates and writes one snapshot", async () => {
    const { db, client } = writingDb();
    const artifacts = loader();
    const result = await syncWestVirginiaCandidateFinance({ ...baseInput(db), loadArtifacts: artifacts });
    expect(result).toMatchObject({
      status: "synced",
      entityId: ENTITY,
      window: { reportingCycle: "2026 Candidate Election Cycle", windowStart: "2025-07-01", windowEnd: "2026-12-31" },
      windowYears: [2025, 2026],
      totalReceipts: 4750,
      directContributionTotal: 1750,
      totalDisbursements: 120,
      breakdownCounts: { occupation: 2, industry: 1, contribution_size: 2 },
      summaryWritten: true,
      directBreakdownsWritten: 5,
    });
    expect(result.reconciliation).toEqual([
      { year: 2025, csvRowCount: 1, apiRowCount: 1, csvTotalCents: 25_000, apiTotalCents: 25_000, amendedApiRowCount: 0 },
      { year: 2026, csvRowCount: 3, apiRowCount: 3, csvTotalCents: 450_000, apiTotalCents: 450_000, amendedApiRowCount: 0 },
    ]);
    expect(artifacts.scheduleRows).toHaveBeenCalledTimes(3);
    const summaryInsert = client.query.mock.calls.find((call) =>
      String(call[0]).includes("INSERT INTO public.wv_candidate_finance_summaries")
    );
    expect(summaryInsert).toBeDefined();
    // cash_on_hand and both outside totals are forced to NULL.
    expect(summaryInsert?.[1]).toEqual([LINK_ID, 2026, 4750, 1750, 120, null, null, null, "https://cfrs.wvsos.gov/", "2026-09-01T00:00:00.000Z"]);
    const breakdownInserts = client.query.mock.calls.filter((call) =>
      String(call[0]).includes("INSERT INTO public.wv_candidate_finance_direct_breakdowns")
    );
    expect(breakdownInserts.map((call) => [call[1]![2], call[1]![3], call[1]![4]])).toEqual([
      ["occupation", "Healthcare/Medical", 500],
      ["occupation", "Retired", 250],
      ["industry", "healthcare", 500],
      ["contribution_size", "$500-$999", 500],
      ["contribution_size", "$250-$499", 250],
    ]);
  });

  it("dry run computes without touching the database", async () => {
    const { db, client } = writingDb();
    const result = await syncWestVirginiaCandidateFinance({ ...baseInput(db), dryRun: true, loadArtifacts: loader() });
    expect(result).toMatchObject({ dryRun: true, summaryWritten: false, directBreakdownsWritten: 0, totalReceipts: 4750 });
    expect(db.connect).not.toHaveBeenCalled();
    expect(client.query).not.toHaveBeenCalled();
  });

  it("fails closed when the CSV and API rows for a year disagree", async () => {
    const { db, client } = writingDb();
    await expect(
      syncWestVirginiaCandidateFinance({
        ...baseInput(db),
        loadArtifacts: loader({
          apiContributionRows: vi.fn(async (year: number) =>
            year === 2026 ? API_ROWS[2026]!.filter((row) => row.transactionID !== 12) : API_ROWS[year]!
          ),
        }),
      })
    ).rejects.toThrow(/2026 contributions do not reconcile for 1010003610: CSV 3 rows \/ 450000c vs API 2 rows \/ 350000c/);
    expect(client.query).not.toHaveBeenCalled();
  });

  it("fails closed when a window year's artifact is not cached", async () => {
    const { db, client } = writingDb();
    await expect(
      syncWestVirginiaCandidateFinance({
        ...baseInput(db),
        loadArtifacts: loader({ expenditureRows: vi.fn(async () => Promise.reject(new Error("no cached metadata"))) }),
      })
    ).rejects.toThrow(/no cached metadata/);
    expect(client.query).not.toHaveBeenCalled();
  });

  it("fails closed when a schedule file is missing (the window could be short)", async () => {
    const { db } = writingDb();
    await expect(
      syncWestVirginiaCandidateFinance({
        ...baseInput(db),
        loadArtifacts: loader({
          scheduleRows: vi.fn(async (year: number) => (year === 2027 ? Promise.reject(new Error("no cached metadata")) : [])),
        }),
      })
    ).rejects.toThrow(/no cached metadata/);
  });

  it("fails closed on vocabulary outside the pinned sets", async () => {
    const { db, client } = writingDb();
    await expect(
      syncWestVirginiaCandidateFinance({
        ...baseInput(db),
        loadArtifacts: loader({
          expenditureRows: vi.fn(async (year: number) =>
            year === 2026
              ? parseWestVirginiaExpenditureCsv(
                  `${EXP_HEADER}\n${ENTITY},C,W,Expenditures,Refund,Advertising,2026-03-05,120.0000,Business or Organization,Acme,1 Elm,2026-04-07\n`
                ).rows
              : []
          ),
        }),
      })
    ).rejects.toThrow(/unrecognized values in window for 1010003610: expenditure type "Refund"/);
    expect(client.query).not.toHaveBeenCalled();
  });

  it("publishes NULL totals, not $0, for a committee with no rows in the window", async () => {
    const { db, client } = writingDb();
    const base = baseInput(db);
    const result = await syncWestVirginiaCandidateFinance({
      ...base,
      link: { ...base.link, entityId: "1010009998", committeeName: "Late Registrant for House" },
      loadArtifacts: loader(),
    });
    expect(result).toMatchObject({
      status: "synced",
      reportedActivity: false,
      totalReceipts: null,
      directContributionTotal: null,
      totalDisbursements: null,
      breakdownCounts: { occupation: 0, industry: 0, contribution_size: 0 },
      summaryWritten: true,
      directBreakdownsWritten: 0,
    });
    const summaryInsert = client.query.mock.calls.find((call) =>
      String(call[0]).includes("INSERT INTO public.wv_candidate_finance_summaries")
    );
    expect(summaryInsert?.[1]).toEqual([LINK_ID, 2026, null, null, null, null, null, null, "https://cfrs.wvsos.gov/", "2026-09-01T00:00:00.000Z"]);
    expect(
      client.query.mock.calls.some((call) => String(call[0]).includes("INSERT INTO public.wv_candidate_finance_direct_breakdowns"))
    ).toBe(false);
  });

  it("rejects ineligible offices, bad ids and pre-2026 years before reading anything", async () => {
    const { db } = writingDb();
    const artifacts = loader();
    await expect(
      syncWestVirginiaCandidateFinance({ ...baseInput(db), officeScope: "county", officeName: "Sheriff", loadArtifacts: artifacts })
    ).rejects.toThrow(/not West Virginia-finance eligible/);
    const base = baseInput(db);
    await expect(
      syncWestVirginiaCandidateFinance({ ...base, link: { ...base.link, entityId: "123" }, loadArtifacts: artifacts })
    ).rejects.toThrow(/Invalid West Virginia CFRS entityId/);
    await expect(
      syncWestVirginiaCandidateFinance({ ...base, electionYear: 2024, loadArtifacts: artifacts })
    ).rejects.toThrow(/invalid election year/);
    expect(artifacts.scheduleRows).not.toHaveBeenCalled();
  });
});
