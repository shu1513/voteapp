import { describe, expect, it, vi } from "vitest";

import { syncArizonaCandidateFinance } from "../../../src/pipeline/arizonaFinance/arizonaCandidateFinanceSync.js";

const CANDIDATE_ID = "11111111-1111-4111-8111-111111111111";
const ELECTION_ID = "22222222-2222-4222-8222-222222222222";

function poolWithClientQuery(query = vi.fn()) {
  const release = vi.fn();
  return {
    query,
    release,
    pool: {
      connect: vi.fn(async () => ({ query, release })),
      query: vi.fn(),
    },
  };
}

describe("arizonaCandidateFinanceSync", () => {
  it("dry-runs a trusted committee snapshot without database writes", async () => {
    const db = { query: vi.fn() };
    const spotlightClient = {
      searchIncomeTransactions: vi.fn(async (input: { filerId?: string | null }) =>
        input.filerId === "AZ100"
          ? [
              {
                transactionDate: "2026-01-15",
                committeeId: "AZ100",
                committeeName: "Jane Arizonan for Governor",
                amount: 1000,
                transactionName: "Taylor Example",
                transactionType: "Individual Contribution",
                occupation: "Attorney",
                sourceUrl: "https://seethemoney.az.gov/Reporting/Explore",
              },
            ]
          : []
      ),
      searchIndependentExpenditures: vi.fn(async () => []),
    };

    const result = await syncArizonaCandidateFinance({
      db,
      candidateId: CANDIDATE_ID,
      electionId: ELECTION_ID,
      candidateName: "Jane Arizonan",
      electionYear: 2026,
      officeScope: "statewide",
      officeName: "Governor",
      dryRun: true,
      trustedCommittee: {
        committeeId: "AZ100",
        committeeName: "Jane Arizonan for Governor",
      },
      spotlightClient,
    });

    expect(result).toMatchObject({
      dryRun: true,
      linkWritten: false,
      totalReceipts: 1000,
      directContributionTotal: 1000,
      includedIncomeTransactionCount: 1,
    });
    expect(db.query).not.toHaveBeenCalled();
  });

  it("writes a resolved Arizona finance snapshot", async () => {
    const query = vi
      .fn()
      .mockResolvedValueOnce({ rows: [] })
      .mockResolvedValueOnce({ rows: [{ id: "link-1" }] })
      .mockResolvedValue({ rows: [] });
    const { pool } = poolWithClientQuery(query);
    const spotlightClient = {
      searchIncomeTransactions: vi.fn(async (input: { filerName?: string | null; filerId?: string | null }) => {
        if (input.filerName) {
          return [
            {
              transactionDate: "2026-01-01",
              committeeId: "AZ100",
              committeeName: "Jane Arizonan for Governor",
              amount: 25,
              sourceUrl: "https://seethemoney.az.gov/Reporting/Explore",
            },
          ];
        }
        if (input.filerId === "AZ100") {
          return [
            {
              transactionDate: "2026-01-15",
              committeeId: "AZ100",
              committeeName: "Jane Arizonan for Governor",
              amount: 1000,
              transactionName: "Taylor Example",
              transactionType: "Individual Contribution",
              occupation: "Attorney",
              sourceUrl: "https://seethemoney.az.gov/Reporting/Explore",
            },
          ];
        }
        return [];
      }),
      searchIndependentExpenditures: vi.fn(async () => []),
    };

    const result = await syncArizonaCandidateFinance({
      db: pool,
      candidateId: CANDIDATE_ID,
      electionId: ELECTION_ID,
      candidateName: "Jane Arizonan",
      electionYear: 2026,
      officeScope: "statewide",
      officeName: "Governor",
      now: new Date("2026-06-25T12:00:00.000Z"),
      spotlightClient,
    });

    expect(result).toMatchObject({
      dryRun: false,
      resolution: {
        status: "matched",
        committeeId: "AZ100",
      },
      linkWritten: true,
      summaryWritten: true,
      directBreakdownsWritten: 2,
      totalReceipts: 1000,
    });
    expect(query.mock.calls.map((call) => String(call[0])).join("\n")).toContain(
      "INSERT INTO public.az_candidate_finance_links"
    );
  });

  it("preserves manual link source for trusted committees", async () => {
    const query = vi
      .fn()
      .mockResolvedValueOnce({ rows: [] })
      .mockResolvedValueOnce({ rows: [{ id: "link-1" }] })
      .mockResolvedValue({ rows: [] });
    const { pool } = poolWithClientQuery(query);

    await syncArizonaCandidateFinance({
      db: pool,
      candidateId: CANDIDATE_ID,
      electionId: ELECTION_ID,
      candidateName: "Jane Arizonan",
      electionYear: 2026,
      officeScope: "statewide",
      officeName: "Governor",
      now: new Date("2026-06-25T12:00:00.000Z"),
      trustedCommittee: {
        committeeId: "AZ100",
        committeeName: "Jane Arizonan for Governor",
        linkSource: "manual",
      },
      spotlightClient: {
        searchIncomeTransactions: vi.fn(async () => []),
        searchIndependentExpenditures: vi.fn(async () => []),
      },
    });

    const linkInsertCall = query.mock.calls.find((call) =>
      String(call[0]).includes("INSERT INTO public.az_candidate_finance_links")
    );
    expect(linkInsertCall?.[1]).toEqual(expect.arrayContaining(["manual"]));
  });
});
