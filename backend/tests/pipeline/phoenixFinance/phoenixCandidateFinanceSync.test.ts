import { describe, expect, it, vi } from "vitest";
import {
  syncPhoenixCandidateFinance,
  type PhoenixFinanceRunContext,
} from "../../../src/pipeline/phoenixFinance/phoenixCandidateFinanceSync.js";
import type { PhoenixCanonicalReport } from "../../../src/pipeline/phoenixFinance/phoenixDirectFinanceAggregator.js";
import type { PhoenixRegistrationRow } from "../../../src/pipeline/phoenixFinance/phoenixEfilingClient.js";
import type { PhoenixOutsidePoolEntry } from "../../../src/pipeline/phoenixFinance/phoenixOutsideSpendingAggregator.js";

const registration: PhoenixRegistrationRow = {
  copId: "CAN-25-4",
  committeeName: "Ed Hermes for Phoenix",
  committeeType: "Candidate Committee",
  candidateName: "Ed Hermes",
  electionCycle: "2025 Election Cycle",
  officeSoughtElectionCycle: "2026",
  terminated: false,
  approved: true,
  approvedTimestamp: 1,
  isStandingCommittee: false,
};

function context(over: Partial<PhoenixFinanceRunContext> = {}): PhoenixFinanceRunContext {
  return {
    registrations: [registration],
    registrationsByCopId: new Map([[registration.copId, registration]]),
    outsidePool: [],
    diagnostics: { ieRegistrations: 0, cityFilingIePacs: 0, standingIePacs: 0, b6Packages: 0 },
    ...over,
  };
}

/** One consistent in-cycle report: raised $500.00 (net of $10 refunds),
 * loans $25.00, spent $200.00, close $325.00. */
function healthyReports(): PhoenixCanonicalReport[] {
  const a = 310_00;
  const b = 200_00;
  const refunds = 10_00;
  const loans = 25_00;
  const spent = 200_00;
  const k = a + b;
  const m = k - refunds;
  return [
    {
      reportPackageId: "00000000-0000-0000-0000-000000000001",
      reportName: "Annual Report - 2026",
      submittedDateMs: 1,
      parsed: {
        cover: {
          reportName: "Annual Report - 2026",
          periodFrom: "2025-04-01",
          periodTo: "2026-03-31",
          officeSought: "Council Member District 4",
          beginCents: 0,
          receiptsPeriodCents: m + loans,
          receiptsCycleCents: null,
          disbursementsPeriodCents: spent,
          disbursementsCycleCents: null,
          closeCents: m + loans - spent,
        },
        receipts: {
          line1: { a, b, c: 0, d: 0, e: 0, f: 0, g: 0, h: 0, i: 0, j: 0, k, l: refunds, m },
          line2eCents: loans,
          otherCashCents: 0,
          line13CashCents: m + loans,
        },
        line16CashCents: spent,
        line6CashCents: null,
        a1aEntries: [
          {
            amountCents: a,
            date: "01/15/2026",
            name: "Pat Donor",
            occupation: "Attorney",
            employer: "Desert Law LLP",
          },
        ],
        a1cEntries: [],
        b6Entries: [],
      },
    },
  ];
}

function poolEntry(): PhoenixOutsidePoolEntry {
  return {
    spenderCopId: "PAC-22-14",
    spenderName: "Some IE PAC",
    reportPackageId: "d7118529-0000-0000-0000-000000000000",
    entry: {
      amountCents: 650_000,
      supportedNames: ["Ed", "Hermes"],
      supportedPercents: [100],
      opposedNames: [],
      opposedPercents: [],
      electionText: "2026",
      officeText: "City Council",
    },
  };
}

// db.query answers the anomaly SELECT (override) and the classification
// reads/writes with empty rows; the transaction client answers the link
// INSERT with an id.
function makeDb(storedSummary?: { total_raised: string | null; reported_through: string | null }) {
  const clientQuery = vi.fn().mockImplementation((sql: unknown) => {
    const s = String(sql);
    if (s.startsWith("INSERT INTO public.phx_candidate_finance_links"))
      return Promise.resolve({ rows: [{ id: "link-1" }] });
    return Promise.resolve({ rows: [] });
  });
  const release = vi.fn();
  const query = vi.fn().mockImplementation((sql: unknown) => {
    const s = String(sql);
    if (s.startsWith("SELECT summary.total_raised"))
      return Promise.resolve({ rows: storedSummary ? [storedSummary] : [] });
    return Promise.resolve({ rows: [] });
  });
  const connect = vi.fn().mockResolvedValue({ query: clientQuery, release });
  return { db: { query, connect }, query, clientQuery, connect };
}

const syncInput = {
  candidateId: "c",
  electionId: "e",
  electionYear: 2026,
  candidateDisplayName: "Ed Hermes",
  officeName: "City Council Member" as const,
  districtNumber: 4,
  electionDate: "2026-11-03",
  copId: "CAN-25-4",
  portalCycleStart: "2025-04-01",
  portalCycleEnd: "2027-03-31",
  now: new Date("2026-08-12T00:00:00Z"),
};

describe("Phoenix candidate finance sync", () => {
  it("writes one full snapshot with exact dollar totals, notes, and outside groups", async () => {
    const { db, clientQuery, connect } = makeDb();
    const result = await syncPhoenixCandidateFinance({
      db: db as never,
      ...syncInput,
      context: context({ outsidePool: [poolEntry()] }),
      loadCommitteeReports: async () => healthyReports(),
      supplements: [],
    });
    expect(connect).toHaveBeenCalledTimes(1);
    const sql = clientQuery.mock.calls.map((call) => String(call[0]));
    expect(sql[0]).toBe("BEGIN");
    expect(sql.at(-1)).toBe("COMMIT");
    const summaryCall = clientQuery.mock.calls.find((call) =>
      String(call[0]).includes("phx_candidate_finance_summaries"),
    );
    // raised 500.00 (net of refunds), spent 200.00, cash 325.00, loans
    // 25.00, outside support 6500.00 — exact strings; BOTH always-on notes.
    expect(summaryCall?.[1]).toEqual(
      expect.arrayContaining(["500.00", "200.00", "325.00", "25.00", "6500.00"]),
    );
    expect(summaryCall?.[1]).toEqual(
      expect.arrayContaining([
        expect.stringContaining("in-state donors giving more than $100"),
        expect.stringContaining("not automatically included"),
      ]),
    );
    const linkCall = clientQuery.mock.calls.find((call) =>
      String(call[0]).startsWith("INSERT INTO public.phx_candidate_finance_links"),
    );
    expect(linkCall?.[1]).toEqual(
      expect.arrayContaining([
        "efiling_portal",
        "Ed Hermes for Phoenix",
        "ED HERMES",
        "2025 Election Cycle",
        "2025-04-01",
        "2027-03-31",
      ]),
    );
    const outsideCall = clientQuery.mock.calls.find((call) =>
      String(call[0]).includes("phx_candidate_finance_outside_groups (link_id"),
    );
    expect(outsideCall?.[1]).toEqual(
      expect.arrayContaining(["PAC-22-14", "Some IE PAC", "support", "6500.00", 1]),
    );
    expect(result).toMatchObject({
      linkWritten: true,
      totalRaisedCents: 500_00,
      totalSpentCents: 200_00,
      loansReceivedCents: 25_00,
      cashOnHandCents: 325_00,
      outsideSupportCents: 650_000,
      outsideOpposeCents: 0,
      reportedThrough: "2026-03-31",
      canonicalReportCount: 1,
    });
    expect(result.directCoverageNote).not.toContain("has not filed");
    expect(result.outsideCoverageNote).not.toContain("excluded");
  });

  it("refuses to sync a committee missing from the registration index", async () => {
    const { db, connect } = makeDb();
    await expect(
      syncPhoenixCandidateFinance({
        db: db as never,
        ...syncInput,
        copId: "CAN-99-9",
        context: context(),
        loadCommitteeReports: async () => [],
      }),
    ).rejects.toThrow(/no approved registration in the portal index/);
    expect(connect).not.toHaveBeenCalled();
  });

  it("quarantines blocking violations before any write", async () => {
    const { db, connect } = makeDb();
    const reports = healthyReports();
    reports[0]!.parsed.cover.closeCents += 27_00; // (a)+(b)-(c)=(d) breaks
    await expect(
      syncPhoenixCandidateFinance({
        db: db as never,
        ...syncInput,
        context: context(),
        loadCommitteeReports: async () => reports,
      }),
    ).rejects.toThrow(/quarantined.*cover_arithmetic/);
    expect(connect).not.toHaveBeenCalled();
  });

  it("writes an affirmative zero snapshot when nothing is filed this cycle", async () => {
    const { db, clientQuery } = makeDb();
    const result = await syncPhoenixCandidateFinance({
      db: db as never,
      ...syncInput,
      context: context(),
      loadCommitteeReports: async () => [],
      supplements: [],
    });
    expect(result.totalRaisedCents).toBe(0);
    expect(result.cashOnHandCents).toBeNull();
    expect(result.directCoverageNote).toContain(
      "has not filed a campaign finance report for this election cycle yet",
    );
    const summaryCall = clientQuery.mock.calls.find((call) =>
      String(call[0]).includes("phx_candidate_finance_summaries"),
    );
    expect(summaryCall?.[1]).toEqual(expect.arrayContaining(["0.00"]));
  });

  it("discloses unattributable IE entries in the outside note", async () => {
    const { db } = makeDb();
    const blankNames = poolEntry();
    blankNames.entry = {
      ...blankNames.entry,
      supportedNames: [],
      supportedPercents: [],
      opposedPercents: [100],
    };
    const result = await syncPhoenixCandidateFinance({
      db: db as never,
      ...syncInput,
      context: context({ outsidePool: [blankNames] }),
      loadCommitteeReports: async () => healthyReports(),
      supplements: [],
      dryRun: true,
    });
    expect(result.outsideSupportCents).toBe(0);
    expect(result.outsideCoverageNote).toContain(
      "1 disclosed independent expenditure was not attributable to a single candidate",
    );
  });

  it("aborts when filing history goes backwards (never bypassable)", async () => {
    const { db } = makeDb({ total_raised: "500.00", reported_through: "2026-06-30" });
    await expect(
      syncPhoenixCandidateFinance({
        db: db as never,
        ...syncInput,
        context: context(),
        loadCommitteeReports: async () => healthyReports(), // through 2026-03-31
        bypassAnomalyCheck: true,
      }),
    ).rejects.toThrow(/filing history went backwards/);
  });

  it("aborts on an order-of-magnitude raise collapse unless bypassed", async () => {
    const stored = { total_raised: "50000.00", reported_through: "2026-03-31" };
    const { db } = makeDb(stored);
    await expect(
      syncPhoenixCandidateFinance({
        db: db as never,
        ...syncInput,
        context: context(),
        loadCommitteeReports: async () => healthyReports(), // raised 500.00
      }),
    ).rejects.toThrow(/collapsed on an unchanged report set/);
    const { db: db2 } = makeDb(stored);
    const result = await syncPhoenixCandidateFinance({
      db: db2 as never,
      ...syncInput,
      context: context(),
      loadCommitteeReports: async () => healthyReports(),
      bypassAnomalyCheck: true,
      dryRun: true,
    });
    expect(result.totalRaisedCents).toBe(500_00);
  });

  it("dry run aggregates without connecting a transaction", async () => {
    const { db, connect } = makeDb();
    const result = await syncPhoenixCandidateFinance({
      db: db as never,
      ...syncInput,
      context: context(),
      loadCommitteeReports: async () => healthyReports(),
      dryRun: true,
    });
    expect(result.linkWritten).toBe(false);
    expect(connect).not.toHaveBeenCalled();
  });
});
