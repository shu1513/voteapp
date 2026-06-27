import { afterEach, describe, expect, it, vi } from "vitest";

import {
  ArizonaSpotlightClientError,
  arizonaSpotlightCycleForElectionYear,
  buildArizonaSpotlightAdvancedSearchUrl,
  buildArizonaSpotlightDataTablesBody,
  searchArizonaSpotlightCandidateCommittees,
  searchArizonaSpotlightIncomeTransactions,
  searchArizonaSpotlightIndependentExpenditures,
} from "../../../src/pipeline/arizonaFinance/arizonaSpotlightClient.js";

function jsonResponse(payload: unknown, init: ResponseInit = {}): Response {
  return new Response(JSON.stringify(payload), {
    status: 200,
    statusText: "OK",
    ...init,
  });
}

describe("arizonaSpotlightClient", () => {
  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  it("maps supported election years to Spotlight cycle identifiers", () => {
    expect(arizonaSpotlightCycleForElectionYear(2024)).toEqual({
      electionYear: 2024,
      cycleId: "43~1/1/2023 12:00:00 AM~12/31/2024 11:59:59 PM",
      startDate: "2023-01-01",
      endDate: "2024-12-31",
    });
    expect(arizonaSpotlightCycleForElectionYear(2026).cycleId).toBe(
      "44~1/1/2025 12:00:00 AM~12/31/2026 11:59:59 PM"
    );
    expect(() => arizonaSpotlightCycleForElectionYear(2025)).toThrow(AZ_EXPECTED_INVALID_YEAR_ERROR);
  });

  it("builds advanced search URLs with the observed Spotlight query shape", () => {
    const url = new URL(
      buildArizonaSpotlightAdvancedSearchUrl({
        categoryType: "IndependentExpenditures",
        electionYear: 2024,
        candidateName: " Katie   Hobbs ",
        candidateFilerId: "201800057",
        position: "Support",
        officeTypeId: 1,
        officeId: 2000,
      })
    );

    expect(url.origin + url.pathname).toBe("https://seethemoney.az.gov/Reporting/AdvancedSearch/");
    expect(url.searchParams.get("JurisdictionId")).toBe("0");
    expect(url.searchParams.get("CategoryType")).toBe("IndependentExpenditures");
    expect(url.searchParams.get("CycleId")).toBe("43~1/1/2023 12:00:00 AM~12/31/2024 11:59:59 PM");
    expect(url.searchParams.get("StartDate")).toBe("2023-01-01");
    expect(url.searchParams.get("EndDate")).toBe("2024-12-31");
    expect(url.searchParams.get("CandidateName")).toBe("Katie Hobbs");
    expect(url.searchParams.get("CandidateFilerId")).toBe("201800057");
    expect(url.searchParams.get("Position")).toBe("Support");
    expect(url.searchParams.get("OfficeTypeId")).toBe("1");
    expect(url.searchParams.get("OfficeId")).toBe("2000");
    expect(url.searchParams.get("FilerId")).toBe("");
  });

  it("builds DataTables POST bodies for income and independent expenditure grids", () => {
    const incomeBody = buildArizonaSpotlightDataTablesBody({
      categoryType: "Income",
      start: 100,
      length: 25,
      draw: 2,
    });
    expect(incomeBody.get("draw")).toBe("2");
    expect(incomeBody.get("start")).toBe("100");
    expect(incomeBody.get("length")).toBe("25");
    expect(incomeBody.get("columns[0][data]")).toBe("TransactionDate");
    expect(incomeBody.get("columns[5][data]")).toBe("Occupation");
    expect(incomeBody.get("order[0][dir]")).toBe("asc");

    const independentExpenditureBody = buildArizonaSpotlightDataTablesBody({
      categoryType: "IndependentExpenditures",
    });
    expect(independentExpenditureBody.get("columns[5][data]")).toBe("Memo");
  });

  it("fetches and parses income transactions conservatively", async () => {
    const fetchImpl = vi.fn().mockResolvedValue(
      jsonResponse({
        draw: 1,
        recordsTotal: 3,
        recordsFiltered: 3,
        data: [
          {
            TransactionDate: "/Date(1672556400000)/",
            CommitteeID: 201800057,
            CommitteeName: "Katie Hobbs for Governor",
            Amount: "$250.50",
            TransactionName: "Jane Doe",
            TransactionType: "Individual Contribution",
            Occupation: "Teacher",
            Employer: "Phoenix Union High School District",
            City: "Phoenix",
            State: "AZ",
            ZipCode: "85001",
            FilerName: "Katie Hobbs",
          },
          {
            CommitteeID: 201800057,
            CommitteeName: "Katie Hobbs for Governor",
            Amount: 0,
          },
          {
            CommitteeID: "",
            CommitteeName: "Bad Row",
            Amount: 100,
          },
        ],
      })
    ) as unknown as typeof fetch;

    await expect(
      searchArizonaSpotlightIncomeTransactions(
        {
          electionYear: 2024,
          filerId: "201800057",
          limit: 10,
        },
        { fetchImpl, timeoutMs: 1000 }
      )
    ).resolves.toEqual([
      {
        transactionDate: "2023-01-01",
        committeeId: "201800057",
        committeeName: "Katie Hobbs for Governor",
        amount: 250.5,
        transactionName: "Jane Doe",
        transactionType: "Individual Contribution",
        occupation: "Teacher",
        employer: "Phoenix Union High School District",
        city: "Phoenix",
        state: "AZ",
        zipCode: "85001",
        filerName: "Katie Hobbs",
        sourceUrl: expect.stringContaining("CategoryType=Income"),
      },
    ]);

    expect(fetchImpl).toHaveBeenCalledTimes(1);
    const [url, init] = vi.mocked(fetchImpl).mock.calls[0] ?? [];
    expect(String(url)).toContain("CategoryType=Income");
    expect(init?.method).toBe("POST");
    expect(String(init?.body)).toContain("columns%5B5%5D%5Bdata%5D=Occupation");
    expect((init?.headers as Record<string, string>)["x-requested-with"]).toBe("XMLHttpRequest");
  });

  it("looks up candidate committees from income rows by candidate name", async () => {
    const fetchImpl = vi.fn().mockResolvedValue(
      jsonResponse({
        draw: 1,
        recordsTotal: 3,
        recordsFiltered: 3,
        data: [
          {
            CommitteeID: "AZ100",
            CommitteeName: "Katie Hobbs for Governor",
            Amount: "$250.00",
          },
          {
            CommitteeID: "AZ100",
            CommitteeName: "Katie Hobbs for Governor",
            Amount: "$750.00",
          },
          {
            CommitteeID: "AZ200",
            CommitteeName: "Katie Hobbs Exploratory",
            Amount: "$25.00",
          },
        ],
      })
    ) as unknown as typeof fetch;

    await expect(
      searchArizonaSpotlightCandidateCommittees(
        {
          candidateName: " Katie   Hobbs ",
          officeName: "Governor",
          electionYear: 2024,
          limit: 10,
        },
        { fetchImpl, timeoutMs: 1000 }
      )
    ).resolves.toEqual([
      {
        committeeId: "AZ100",
        committeeName: "Katie Hobbs for Governor",
        amount: 1000,
        rowCount: 2,
        sourceUrl: expect.stringContaining("FilerName=Katie+Hobbs"),
      },
      {
        committeeId: "AZ200",
        committeeName: "Katie Hobbs Exploratory",
        amount: 25,
        rowCount: 1,
        sourceUrl: expect.stringContaining("FilerName=Katie+Hobbs"),
      },
    ]);
  });

  it("fetches support independent expenditures with candidate filters", async () => {
    const fetchImpl = vi.fn().mockResolvedValue(
      jsonResponse({
        draw: 1,
        recordsTotal: 1,
        recordsFiltered: 1,
        data: [
          {
            TransactionDate: "10/20/2024",
            CommitteeID: "201000285",
            CommitteeName: "Toa Pac",
            Amount: 5400,
            TransactionName: "Elect Katie Hobbs",
            TransactionType: "Ind. Expend. (Non-Recall) - cash",
            Memo: "Supports candidate",
          },
        ],
      })
    ) as unknown as typeof fetch;

    await expect(
      searchArizonaSpotlightIndependentExpenditures(
        {
          electionYear: 2024,
          candidateName: "Katie Hobbs",
          candidateFilerId: "201800057",
          position: "Support",
          limit: 5,
        },
        { fetchImpl, timeoutMs: 1000 }
      )
    ).resolves.toEqual([
      {
        transactionDate: "2024-10-20",
        committeeId: "201000285",
        committeeName: "Toa Pac",
        amount: 5400,
        transactionName: "Elect Katie Hobbs",
        transactionType: "Ind. Expend. (Non-Recall) - cash",
        memo: "Supports candidate",
        supportOppose: "Support",
        sourceUrl: expect.stringContaining("CategoryType=IndependentExpenditures"),
      },
    ]);

    const [url, init] = vi.mocked(fetchImpl).mock.calls[0] ?? [];
    expect(String(url)).toContain("CategoryType=IndependentExpenditures");
    expect(String(url)).toContain("CandidateName=Katie+Hobbs");
    expect(String(init?.body)).toContain("columns%5B5%5D%5Bdata%5D=Memo");
  });

  it("paginates until the requested limit is reached", async () => {
    const fetchImpl = vi
      .fn()
      .mockResolvedValueOnce(
        jsonResponse({
          draw: 1,
          recordsTotal: 3,
          recordsFiltered: 3,
          data: [
            {
              CommitteeID: "1",
              CommitteeName: "Committee",
              Amount: 10,
            },
          ],
        })
      )
      .mockResolvedValueOnce(
        jsonResponse({
          draw: 2,
          recordsTotal: 3,
          recordsFiltered: 3,
          data: [
            {
              CommitteeID: "1",
              CommitteeName: "Committee",
              Amount: 20,
            },
          ],
        })
      ) as unknown as typeof fetch;

    await expect(
      searchArizonaSpotlightIncomeTransactions(
        {
          electionYear: 2024,
          filerId: "1",
          limit: 2,
        },
        {
          fetchImpl,
          pageLength: 1,
          timeoutMs: 1000,
        }
      )
    ).resolves.toHaveLength(2);

    expect(fetchImpl).toHaveBeenCalledTimes(2);
    expect(String(vi.mocked(fetchImpl).mock.calls[0]?.[1]?.body)).toContain("start=0");
    expect(String(vi.mocked(fetchImpl).mock.calls[1]?.[1]?.body)).toContain("start=1");
  });

  it("rejects malformed responses and HTTP failures with typed errors", async () => {
    const malformedFetch = vi.fn().mockResolvedValue(jsonResponse({ data: null })) as unknown as typeof fetch;

    await expect(
      searchArizonaSpotlightIncomeTransactions({ electionYear: 2024 }, { fetchImpl: malformedFetch, timeoutMs: 1000 })
    ).rejects.toMatchObject({
      name: "ArizonaSpotlightClientError",
      code: "bad_response",
    });

    const failingFetch = vi.fn().mockResolvedValue(jsonResponse({}, { status: 503, statusText: "Unavailable" })) as
      unknown as typeof fetch;

    await expect(
      searchArizonaSpotlightIndependentExpenditures(
        { electionYear: 2024, candidateName: "Katie Hobbs", position: "Support" },
        { fetchImpl: failingFetch, timeoutMs: 1000 }
      )
    ).rejects.toBeInstanceOf(ArizonaSpotlightClientError);
  });
});

const AZ_EXPECTED_INVALID_YEAR_ERROR = "cycle is not configured";
