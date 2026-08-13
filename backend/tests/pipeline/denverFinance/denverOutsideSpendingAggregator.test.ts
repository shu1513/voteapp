import { describe, expect, it, vi } from "vitest";

import {
  aggregateDenverOutsideSpending,
  resolveDenverOutsideSpenderId,
} from "../../../src/pipeline/denverFinance/denverOutsideSpendingAggregator.js";
import type { DenverFinancialOverview } from "../../../src/pipeline/denverFinance/denverSearchlightClient.js";

function overview(
  over: Partial<DenverFinancialOverview> = {},
): DenverFinancialOverview {
  return {
    fairElectionsFundToCandidateCents: 0,
    campaignContributionsToCandidateCents: 0,
    independentExpendituresSupportingCandidateCents: 0,
    independentExpendituresOpposingCandidateCents: 0,
    fairElectionsFundToOthersCents: 0,
    campaignContributionsToOthersCents: 0,
    independentExpendituresSupportingOthersCents: 0,
    independentExpendituresOpposingOthersCents: 0,
    ...over,
  };
}

type Routes = {
  support?: Array<{ name: string; total: number }>;
  oppose?: Array<{ name: string; total: number }>;
  search?: Record<string, Array<{ uniqueId: string; id: number; name: string; type: number }>>;
};

function makeFetch(routes: Routes) {
  return vi.fn(async (input: RequestInfo | URL): Promise<Response> => {
    const url = String(input);
    if (url.includes("GetSupportingorOpposingIndependentSpendersByCommittee")) {
      const list = url.includes("positionType=1")
        ? routes.support ?? []
        : routes.oppose ?? [];
      return new Response(JSON.stringify(list), { status: 200 });
    }
    if (url.includes("getAllCommitteesAndCandidate")) {
      const term = decodeURIComponent(url.split("search=")[1] ?? "");
      return new Response(JSON.stringify(routes.search?.[term] ?? []), {
        status: 200,
      });
    }
    throw new Error(`Unexpected URL in test fetch: ${url}`);
  });
}

describe("resolveDenverOutsideSpenderId", () => {
  it("prefers the exact raw-name tier when punctuation collides", async () => {
    const fetchImpl = makeFetch({
      search: {
        "A Better Denver": [
          { uniqueId: "Ind808", id: 808, name: "A Better Denver", type: 3 },
          { uniqueId: "Ind678", id: 678, name: "A Better Denver!", type: 3 },
        ],
      },
    });
    await expect(
      resolveDenverOutsideSpenderId("A Better Denver", { fetchImpl }),
    ).resolves.toBe("Ind808");
  });

  it("falls back to normalized matching only when the exact tier is empty", async () => {
    const fetchImpl = makeFetch({
      search: {
        "CWA-COPE PCC": [
          { uniqueId: "Ind555", id: 555, name: "CWA COPE PCC", type: 3 },
          // Non-IE entries never count, even with a matching name.
          { uniqueId: "com555", id: 555, name: "CWA-COPE PCC", type: 1 },
        ],
      },
    });
    await expect(
      resolveDenverOutsideSpenderId("CWA-COPE PCC", { fetchImpl }),
    ).resolves.toBe("Ind555");
  });

  it("fails closed on zero and on multiple normalized matches", async () => {
    const none = makeFetch({ search: { Ghost: [] } });
    await expect(
      resolveDenverOutsideSpenderId("Ghost", { fetchImpl: none }),
    ).rejects.toThrow(/resolves to 0 IE entities/);
    const many = makeFetch({
      search: {
        "Twin Name": [
          { uniqueId: "Ind1", id: 1, name: "Twin  Name", type: 3 },
          { uniqueId: "Ind2", id: 2, name: "TWIN NAME", type: 3 },
        ],
      },
    });
    await expect(
      resolveDenverOutsideSpenderId("Twin Name", { fetchImpl: many }),
    ).rejects.toThrow(/resolves to 2 IE entities/);
  });
});

describe("aggregateDenverOutsideSpending", () => {
  it("resolves both directions and cross-checks the overview sums", async () => {
    const fetchImpl = makeFetch({
      support: [
        { name: "Advancing Denver", total: 4962415.47 },
        { name: "CWA-COPE PCC", total: 13633.33 },
      ],
      oppose: [{ name: "CWA-COPE PCC", total: 356.77 }],
      search: {
        "Advancing Denver": [
          { uniqueId: "Ind787", id: 787, name: "Advancing Denver", type: 3 },
        ],
        "CWA-COPE PCC": [
          { uniqueId: "Ind555", id: 555, name: "CWA-COPE PCC", type: 3 },
        ],
      },
    });
    const result = await aggregateDenverOutsideSpending({
      filerId: 658,
      electionCycleId: 26,
      overview: overview({
        independentExpendituresSupportingCandidateCents: 496_241_547 + 1_363_333,
        independentExpendituresOpposingCandidateCents: 35_677,
      }),
      options: { fetchImpl },
    });
    expect(result.supportTotalCents).toBe(497_604_880);
    expect(result.opposeTotalCents).toBe(35_677);
    expect(result.groups).toEqual([
      {
        spenderId: "Ind787",
        spenderName: "Advancing Denver",
        supportOppose: "support",
        amountCents: 496_241_547,
      },
      {
        spenderId: "Ind555",
        spenderName: "CWA-COPE PCC",
        supportOppose: "support",
        amountCents: 1_363_333,
      },
      {
        spenderId: "Ind555",
        spenderName: "CWA-COPE PCC",
        supportOppose: "oppose",
        amountCents: 35_677,
      },
    ]);
    // The both-directions spender resolves once (memoized by name).
    const searchCalls = fetchImpl.mock.calls.filter(([url]) =>
      String(url).includes("getAllCommitteesAndCandidate"),
    );
    expect(searchCalls).toHaveLength(2);
  });

  it("fails closed when a direction's list disagrees with the overview", async () => {
    const fetchImpl = makeFetch({
      support: [{ name: "Advancing Denver", total: 100 }],
      oppose: [],
      search: {
        "Advancing Denver": [
          { uniqueId: "Ind787", id: 787, name: "Advancing Denver", type: 3 },
        ],
      },
    });
    await expect(
      aggregateDenverOutsideSpending({
        filerId: 658,
        electionCycleId: 26,
        overview: overview({
          independentExpendituresSupportingCandidateCents: 9_999,
        }),
        options: { fetchImpl },
      }),
    ).rejects.toThrow(/support spender list sums to \$100\.00 but the overview reports \$99\.99/);
  });

  it("refuses to merge two list rows that resolve to one entity", async () => {
    const fetchImpl = makeFetch({
      support: [
        { name: "Advancing Denver", total: 100 },
        { name: "ADVANCING DENVER", total: 50 },
      ],
      oppose: [],
      search: {
        "Advancing Denver": [
          { uniqueId: "Ind787", id: 787, name: "Advancing Denver", type: 3 },
        ],
        "ADVANCING DENVER": [
          { uniqueId: "Ind787", id: 787, name: "Advancing Denver", type: 3 },
        ],
      },
    });
    await expect(
      aggregateDenverOutsideSpending({
        filerId: 658,
        electionCycleId: 26,
        overview: overview({
          independentExpendituresSupportingCandidateCents: 150,
        }),
        options: { fetchImpl },
      }),
    ).rejects.toThrow(/resolves two rows to Ind787/);
  });
});
