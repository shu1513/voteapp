import { afterEach, describe, expect, it, vi } from "vitest";

import {
  WISCONSIN_SUNSHINE_TRANSACTIONS_URL,
  WisconsinSunshineClientError,
  buildWisconsinSunshineCommitteeSearchInput,
  buildWisconsinSunshineContributionTransactionInput,
  buildWisconsinSunshineIndependentExpenditureTransactionInput,
  buildWisconsinSunshineTrpcUrl,
  getWisconsinSunshineContributionSizeAggregates,
  getWisconsinSunshineDirectOccupationAggregates,
  getWisconsinSunshineIndependentExpenditureGroups,
  getWisconsinSunshineOffices,
  getWisconsinSunshineOutsideSpenderOrganizationFunders,
  getWisconsinSunshineTransactionCategories,
  searchWisconsinSunshineCommittees,
} from "../../../src/pipeline/wisconsinFinance/wisconsinSunshineClient.js";

function jsonResponse(payload: unknown, init: ResponseInit = {}): Response {
  return new Response(JSON.stringify(payload), {
    status: 200,
    statusText: "OK",
    ...init,
  });
}

function trpcResponse(payload: unknown, init: ResponseInit = {}): Response {
  return jsonResponse([{ result: { data: { json: payload } } }], init);
}

describe("wisconsinSunshineClient", () => {
  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  it("builds tRPC batch URLs", () => {
    const url = new URL(buildWisconsinSunshineTrpcUrl("publicFrontendApi.getTransactions", { take: 5, skip: 0 }));

    expect(url.origin + url.pathname).toBe("https://campaignfinance.wi.gov/api/trpc/publicFrontendApi.getTransactions");
    expect(url.searchParams.get("batch")).toBe("1");
    expect(JSON.parse(url.searchParams.get("input") ?? "{}")).toEqual({ "0": { json: { take: 5, skip: 0 } } });
    expect(() => buildWisconsinSunshineTrpcUrl("../bad", {})).toThrow(WisconsinSunshineClientError);
  });

  it("builds conservative committee search inputs", () => {
    expect(buildWisconsinSunshineCommitteeSearchInput({ searchTerm: " Tiffany   for Wisconsin ", limit: 7 })).toEqual({
      searchTerm: "Tiffany for Wisconsin",
      take: 7,
      skip: 0,
      sortBy: "createdAt",
      sortDirection: "desc",
    });
  });

  it("builds contribution transaction inputs with cycle window", () => {
    expect(buildWisconsinSunshineContributionTransactionInput({ entityId: "16621", electionYear: 2026 })).toEqual({
      createdByEntityId: [16621],
      transactionType: [1],
      dateFrom: "2025-01-01",
      dateTo: "2026-12-31",
      take: 100,
      skip: 0,
      sortBy: "date",
      sortDirection: "desc",
    });
  });

  it("builds independent expenditure transaction inputs with strict category defaults", () => {
    expect(
      buildWisconsinSunshineIndependentExpenditureTransactionInput({
        candidateCommitteeName: "Tiffany for Wisconsin",
        electionYear: 2026,
      })
    ).toEqual({
      transactionCategory: [33, 35],
      dateFrom: "2025-01-01",
      dateTo: "2026-12-31",
      take: 100,
      skip: 0,
      sortBy: "date",
      sortDirection: "desc",
    });
  });

  it("parses committee search results and skips malformed rows", async () => {
    const fetchImpl = vi.fn().mockResolvedValue(
      trpcResponse({
        results: [
          {
            id: 407,
            assignedCommitteeId: "0104212",
            entity: { id: 16621, name: "Tiffany for Wisconsin" },
            committeeType: { name: "State Candidate" },
            committeeStatus: { name: "Active", statusSlug: "ACTIVE" },
            entityConnections: [{ entity: { name: "Tom Tiffany" } }],
          },
          { id: 408, entity: { name: "missing entity id" } },
        ],
      })
    ) as unknown as typeof fetch;

    await expect(
      searchWisconsinSunshineCommittees({ searchTerm: "Tiffany for Wisconsin" }, { fetchImpl, timeoutMs: 1000 })
    ).resolves.toEqual([
      {
        entityId: "16621",
        committeeId: "407",
        assignedCommitteeId: "0104212",
        committeeName: "Tiffany for Wisconsin",
        committeeType: "State Candidate",
        committeeStatus: "Active",
        committeeStatusSlug: "ACTIVE",
        candidateNames: ["Tom Tiffany"],
        sourceUrl: "https://campaignfinance.wi.gov/browse-data/registrants/16621",
      },
    ]);

    expect(String(vi.mocked(fetchImpl).mock.calls[0]?.[0])).toContain("/api/trpc/publicFrontendApi.getCommittees");
  });

  it("aggregates direct donor occupations from individual contribution rows only", async () => {
    const fetchImpl = vi.fn().mockResolvedValueOnce(
      trpcResponse({
        results: [
          {
            amount: "100.50",
            fromOccupationTitle: "Attorney",
            from_entity: { entityType: { name: "Individual" } },
          },
          {
            amount: 25,
            fromOccupationTitle: " attorney ",
            from_entity: { entityType: { name: "Individual" } },
          },
          {
            amount: 999,
            fromOccupationTitle: "CEO",
            from_entity: { entityType: { name: "Business" } },
          },
          {
            amount: 50,
            fromOccupationTitle: "",
            from_entity: { entityType: { name: "Individual" } },
          },
        ],
      })
    ) as unknown as typeof fetch;

    await expect(
      getWisconsinSunshineDirectOccupationAggregates(
        { entityId: 16621, electionYear: 2026, limit: 5 },
        { fetchImpl, timeoutMs: 1000, pageLimit: 10 }
      )
    ).resolves.toEqual([
      {
        categoryName: "ATTORNEY",
        amount: 125.5,
        count: 2,
        sourceUrl: "https://campaignfinance.wi.gov/browse-data/transactions",
      },
    ]);
  });

  it("aggregates contribution size buckets", async () => {
    const fetchImpl = vi.fn().mockResolvedValueOnce(
      trpcResponse({
        results: [
          { amount: "25", from_entity: { entityType: { name: "Individual" } } },
          { amount: "75", from_entity: { entityType: { name: "Individual" } } },
          { amount: "1000", from_entity: { entityType: { name: "Individual" } } },
          { amount: "5000", from_entity: { entityType: { name: "Individual" } } },
          { amount: "-1", from_entity: { entityType: { name: "Individual" } } },
        ],
      })
    ) as unknown as typeof fetch;

    await expect(
      getWisconsinSunshineContributionSizeAggregates(
        { entityId: "16621", electionYear: 2026, limit: 5 },
        { fetchImpl, timeoutMs: 1000, pageLimit: 10 }
      )
    ).resolves.toEqual([
      {
        categoryName: "5000_plus",
        amount: 5000,
        count: 1,
        sourceUrl: "https://campaignfinance.wi.gov/browse-data/transactions",
      },
      {
        categoryName: "1000_4999",
        amount: 1000,
        count: 1,
        sourceUrl: "https://campaignfinance.wi.gov/browse-data/transactions",
      },
      {
        categoryName: "under_100",
        amount: 100,
        count: 2,
        sourceUrl: "https://campaignfinance.wi.gov/browse-data/transactions",
      },
    ]);
  });

  it("aggregates independent expenditure groups with exact candidate, office, district, and stance checks", async () => {
    const fetchImpl = vi.fn().mockResolvedValueOnce(
      trpcResponse({
        results: [
          {
            amount: 150000,
            supportStance: "FOR",
            relatedEntity: { name: "Tiffany for Wisconsin" },
            relatedOffice: { name: "Governor" },
            relatedDistrict: { name: "District 1" },
            createdByEntity: { id: 12231502, name: "AMERICANS FOR PROSPERITY" },
          },
          {
            amount: "25000",
            supportStance: "For",
            relatedEntity: { name: "Tiffany for Wisconsin" },
            relatedOffice: { name: "Governor" },
            relatedDistrict: { name: "District 1" },
            createdByEntity: { id: 12231502, name: "AMERICANS FOR PROSPERITY" },
          },
          {
            amount: 10000,
            supportStance: "AGAINST",
            relatedEntity: { name: "Tiffany for Wisconsin" },
            relatedOffice: { name: "Governor" },
            relatedDistrict: { name: "District 1" },
            createdByEntity: { id: 777, name: "OPPOSE PAC" },
          },
          {
            amount: 999999,
            supportStance: "ASSIST",
            relatedEntity: { name: "Tiffany for Wisconsin" },
            relatedOffice: { name: "Governor" },
            relatedDistrict: { name: "District 1" },
            createdByEntity: { id: 1, name: "BAD STANCE PAC" },
          },
          {
            amount: 888888,
            supportStance: "FOR",
            relatedEntity: { name: "Different Candidate" },
            relatedOffice: { name: "Governor" },
            relatedDistrict: { name: "District 1" },
            createdByEntity: { id: 2, name: "WRONG CANDIDATE PAC" },
          },
          {
            amount: 777777,
            supportStance: "FOR",
            relatedEntity: { name: "Tiffany for Wisconsin" },
            relatedOffice: { name: "State Senate" },
            relatedDistrict: { name: "District 1" },
            createdByEntity: { id: 3, name: "WRONG OFFICE PAC" },
          },
        ],
      })
    ) as unknown as typeof fetch;

    await expect(
      getWisconsinSunshineIndependentExpenditureGroups(
        {
          candidateCommitteeName: "Tiffany for Wisconsin",
          electionYear: 2026,
          office: "Governor",
          district: "District 1",
          limit: 5,
        },
        { fetchImpl, timeoutMs: 1000, pageLimit: 10 }
      )
    ).resolves.toEqual([
      {
        sponsorId: "12231502",
        sponsorName: "AMERICANS FOR PROSPERITY",
        supportOppose: "support",
        amount: 175000,
        expenditureCount: 2,
        sourceUrl: "https://campaignfinance.wi.gov/browse-data/transactions",
      },
      {
        sponsorId: "777",
        sponsorName: "OPPOSE PAC",
        supportOppose: "oppose",
        amount: 10000,
        expenditureCount: 1,
        sourceUrl: "https://campaignfinance.wi.gov/browse-data/transactions",
      },
    ]);
  });

  it("aggregates outside spender organization funders and skips individuals", async () => {
    const fetchImpl = vi.fn().mockResolvedValueOnce(
      trpcResponse({
        results: [
          {
            amount: 1_000_000,
            from_entity: { name: "Strategic Victory Fund", entityType: { name: "Business" } },
          },
          {
            amount: "250000",
            from_entity: { name: "Strategic Victory Fund", entityType: { name: "Business" } },
          },
          {
            amount: 500_000,
            from_entity: { name: "John Smith", entityType: { name: "Individual" } },
          },
          {
            amount: 125_000,
            from_entity: { name: "Planned Parenthood Advocates", entityType: { name: "Registrant" } },
          },
        ],
      })
    ) as unknown as typeof fetch;

    await expect(
      getWisconsinSunshineOutsideSpenderOrganizationFunders(
        { entityId: 12231502, electionYear: 2026, limit: 5 },
        { fetchImpl, timeoutMs: 1000, pageLimit: 10 }
      )
    ).resolves.toEqual([
      {
        categoryName: "STRATEGIC VICTORY FUND",
        amount: 1_250_000,
        count: 2,
        sourceUrl: "https://campaignfinance.wi.gov/browse-data/transactions",
      },
      {
        categoryName: "PLANNED PARENTHOOD ADVOCATES",
        amount: 125_000,
        count: 1,
        sourceUrl: "https://campaignfinance.wi.gov/browse-data/transactions",
      },
    ]);
  });

  it("parses offices and transaction categories metadata", async () => {
    const fetchImpl = vi
      .fn()
      .mockResolvedValueOnce(trpcResponse([{ id: 5, name: "Governor", isActive: true }, { name: "bad" }]))
      .mockResolvedValueOnce(
        trpcResponse([
          {
            id: 33,
            code: "IE",
            label: "Independent Expenditure",
            requiresRelatedEntity: true,
            requiresSupportStance: true,
            requiresOffice: true,
          },
        ])
      ) as unknown as typeof fetch;

    await expect(getWisconsinSunshineOffices({ fetchImpl, timeoutMs: 1000 })).resolves.toEqual([
      { id: "5", name: "Governor", isActive: true },
    ]);
    await expect(getWisconsinSunshineTransactionCategories({ fetchImpl, timeoutMs: 1000 })).resolves.toEqual([
      {
        id: 33,
        code: "IE",
        label: "Independent Expenditure",
        requiresRelatedEntity: true,
        requiresSupportStance: true,
        requiresOffice: true,
      },
    ]);
  });

  it("caps paged reads defensively when date splitting is disabled", async () => {
    const fetchImpl = vi.fn().mockImplementation(async () =>
      trpcResponse({
        results: [{ amount: 25, from_entity: { entityType: { name: "Individual" } } }],
      })
    ) as unknown as typeof fetch;

    await expect(
      getWisconsinSunshineContributionSizeAggregates(
        { entityId: 16621, electionYear: 2026, limit: 5 },
        { fetchImpl, timeoutMs: 1000, pageLimit: 1, maxPages: 2, maxDateSplitDepth: 0 }
      )
    ).rejects.toMatchObject({ code: "page_overflow" });

    expect(fetchImpl).toHaveBeenCalledTimes(2);
  });

  it("splits an over-full date window and merges the halves", async () => {
    const windowOf = (url: string): { dateFrom: string; dateTo: string; skip: number } => {
      const input = JSON.parse(new URL(url).searchParams.get("input") ?? "{}") as {
        "0": { json: { dateFrom: string; dateTo: string; skip: number } };
      };
      const { dateFrom, dateTo, skip } = input["0"].json;
      return { dateFrom, dateTo, skip };
    };
    const row = (id: string, amount: number) => ({
      id,
      amount,
      from_entity: { entityType: { name: "Individual" } },
    });
    const fetchImpl = vi.fn(async (url: string) => {
      const { dateFrom, dateTo } = windowOf(url);
      // The full cycle window [2025-01-01, 2026-12-31] keeps returning full
      // pages (overflow); each single-year half fits on one partial page.
      if (dateFrom === "2025-01-01" && dateTo === "2026-12-31") {
        return trpcResponse({ results: [row("overflow-a", 1), row("overflow-b", 1)] });
      }
      if (dateFrom === "2025-01-01") {
        return trpcResponse({ results: [row("a", 150)] });
      }
      return trpcResponse({ results: [row("b", 2_000)] });
    }) as unknown as typeof fetch;

    await expect(
      getWisconsinSunshineContributionSizeAggregates(
        { entityId: 16621, electionYear: 2026, limit: 5 },
        { fetchImpl, timeoutMs: 1000, pageLimit: 2, maxPages: 1 }
      )
    ).resolves.toEqual([
      { categoryName: "1000_4999", amount: 2_000, count: 1, sourceUrl: WISCONSIN_SUNSHINE_TRANSACTIONS_URL },
      { categoryName: "100_249", amount: 150, count: 1, sourceUrl: WISCONSIN_SUNSHINE_TRANSACTIONS_URL },
    ]);

    const windows = fetchImpl.mock.calls.map((call) => windowOf(String(call[0])));
    expect(windows).toEqual([
      { dateFrom: "2025-01-01", dateTo: "2026-12-31", skip: 0 },
      { dateFrom: "2025-01-01", dateTo: "2025-12-31", skip: 0 },
      { dateFrom: "2026-01-01", dateTo: "2026-12-31", skip: 0 },
    ]);
  });

  it("dedupes rows that appear in both halves of a split window", async () => {
    const windowOf = (url: string): { dateFrom: string; dateTo: string } =>
      (JSON.parse(new URL(url).searchParams.get("input") ?? "{}") as {
        "0": { json: { dateFrom: string; dateTo: string } };
      })["0"].json;
    const row = (id: string) => ({
      id,
      amount: 100,
      fromOccupationTitle: "Teacher",
      from_entity: { entityType: { name: "Individual" } },
    });
    const fetchImpl = vi.fn(async (url: string) => {
      const { dateFrom, dateTo } = windowOf(url);
      if (dateFrom === "2025-01-01" && dateTo === "2026-12-31") {
        return trpcResponse({ results: [row("full-1"), row("full-2"), row("full-3")] });
      }
      if (dateFrom === "2025-01-01") {
        return trpcResponse({ results: [row("a"), row("b")] });
      }
      // Boundary row "b" shows up in both halves; it must only count once.
      return trpcResponse({ results: [row("b"), row("c")] });
    }) as unknown as typeof fetch;

    await expect(
      getWisconsinSunshineDirectOccupationAggregates(
        { entityId: 16621, electionYear: 2026, limit: 5 },
        { fetchImpl, timeoutMs: 1000, pageLimit: 3, maxPages: 1 }
      )
    ).resolves.toEqual([
      { categoryName: "TEACHER", amount: 300, count: 3, sourceUrl: WISCONSIN_SUNSHINE_TRANSACTIONS_URL },
    ]);
  });

  it("rethrows page overflow once a window can no longer be split", async () => {
    const fetchImpl = vi.fn().mockResolvedValue(
      trpcResponse({
        results: [{ id: "x", amount: 25, from_entity: { entityType: { name: "Individual" } } }],
      })
    ) as unknown as typeof fetch;

    await expect(
      getWisconsinSunshineContributionSizeAggregates(
        { entityId: 16621, electionYear: 2026, dateFrom: "2026-01-01", dateTo: "2026-01-01", limit: 5 },
        { fetchImpl, timeoutMs: 1000, pageLimit: 1, maxPages: 1 }
      )
    ).rejects.toMatchObject({ code: "page_overflow" });

    expect(fetchImpl).toHaveBeenCalledTimes(1);
  });

  it("throws structured errors for HTTP and malformed tRPC responses", async () => {
    const httpFetch = vi
      .fn()
      .mockResolvedValue(jsonResponse({ ok: false }, { status: 500, statusText: "Server Error" })) as unknown as typeof fetch;
    await expect(
      searchWisconsinSunshineCommittees({ searchTerm: "Tiffany" }, { fetchImpl: httpFetch })
    ).rejects.toMatchObject({
      code: "http_error",
      status: 500,
    });

    const malformedFetch = vi.fn().mockResolvedValue(jsonResponse({ not: "batch" })) as unknown as typeof fetch;
    await expect(
      searchWisconsinSunshineCommittees({ searchTerm: "Tiffany" }, { fetchImpl: malformedFetch })
    ).rejects.toMatchObject({ code: "bad_response" });

    const trpcErrorFetch = vi
      .fn()
      .mockResolvedValue(jsonResponse([{ error: { message: "bad input" } }])) as unknown as typeof fetch;
    await expect(
      searchWisconsinSunshineCommittees({ searchTerm: "Tiffany" }, { fetchImpl: trpcErrorFetch })
    ).rejects.toMatchObject({ code: "bad_response", message: "bad input" });
  });
});
