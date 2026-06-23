import { afterEach, describe, expect, it, vi } from "vitest";

import {
  HawaiiCscClientError,
  buildHawaiiCscCandidateCommitteeSearchUrl,
  buildHawaiiCscContributionSizeAggregatesUrl,
  buildHawaiiCscDatasetUrl,
  buildHawaiiCscDirectOccupationAggregatesUrl,
  buildHawaiiCscIndependentExpenditureGroupsUrl,
  buildHawaiiCscNoncandidateCommitteeFundersUrl,
  getHawaiiCscContributionSizeAggregates,
  getHawaiiCscDirectOccupationAggregates,
  getHawaiiCscIndependentExpenditureGroups,
  getHawaiiCscNoncandidateCommitteeFunders,
  normalizeHawaiiCscPersonNameKeys,
  searchHawaiiCscCandidateCommittees,
} from "../../../src/pipeline/hawaiiFinance/hawaiiCscClient.js";

function jsonResponse(payload: unknown, init: ResponseInit = {}): Response {
  return new Response(JSON.stringify(payload), {
    status: 200,
    statusText: "OK",
    ...init,
  });
}

describe("hawaiiCscClient", () => {
  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  it("builds Socrata dataset URLs", () => {
    const url = new URL(
      buildHawaiiCscDatasetUrl("jexd-xbcg", {
        $select: "candidate_name,amount",
        $where: "amount > 0",
      })
    );

    expect(url.origin + url.pathname).toBe("https://hicscdata.hawaii.gov/resource/jexd-xbcg.json");
    expect(url.searchParams.get("$select")).toBe("candidate_name,amount");
    expect(url.searchParams.get("$where")).toBe("amount > 0");
    expect(() => buildHawaiiCscDatasetUrl("bad", {})).toThrow(HawaiiCscClientError);
  });

  it("normalizes Hawaii candidate names in both display and last-first order", () => {
    expect([...normalizeHawaiiCscPersonNameKeys("Green, Josh*")]).toEqual(["GREEN JOSH", "JOSH GREEN"]);
    expect(normalizeHawaiiCscPersonNameKeys("Josh Green").has("JOSH GREEN")).toBe(true);
  });

  it("builds candidate committee search URLs with year, token, office, and district filters", () => {
    const url = new URL(
      buildHawaiiCscCandidateCommitteeSearchUrl({
        candidateName: "Josh Green",
        electionYear: 2022,
        office: "Governor",
        district: "1",
        limit: 10,
      })
    );

    expect(url.origin + url.pathname).toBe("https://hicscdata.hawaii.gov/resource/jexd-xbcg.json");
    expect(url.searchParams.get("$where")).toContain("election_period like '%2022%'");
    expect(url.searchParams.get("$where")).toContain("lower(candidate_name) like '%green%'");
    expect(url.searchParams.get("$where")).toContain("upper(office) = upper('Governor')");
    expect(url.searchParams.get("$where")).toContain("upper(district) = upper('1')");
    expect(url.searchParams.get("$group")).toBe("candidate_name,office,district,county,party,reg_no,election_period");
    expect(url.searchParams.get("$limit")).toBe("10");
  });

  it("parses candidate committee summaries and skips malformed rows", async () => {
    const fetchImpl = vi.fn().mockResolvedValue(
      jsonResponse([
        {
          candidate_name: "Green, Josh",
          office: "Governor",
          district: "",
          county: "Statewide",
          party: "Democrat",
          reg_no: "CC10174",
          election_period: "2018-2022",
          total_amount: "4070153.38",
          total_count: "1432",
        },
        { candidate_name: "missing committee", total_amount: "10" },
      ])
    ) as unknown as typeof fetch;

    await expect(
      searchHawaiiCscCandidateCommittees(
        { candidateName: "Josh Green", electionYear: 2022, office: "Governor" },
        { fetchImpl, timeoutMs: 1000, appToken: "token" }
      )
    ).resolves.toEqual([
      {
        candidateName: "Green, Josh",
        committeeId: "CC10174",
        electionPeriod: "2018-2022",
        office: "Governor",
        county: "Statewide",
        party: "Democrat",
        totalAmount: 4070153.38,
        contributionCount: 1432,
      },
    ]);

    const request = vi.mocked(fetchImpl).mock.calls[0];
    expect(String(request?.[0])).toContain("/jexd-xbcg.json");
    expect((request?.[1]?.headers as Headers).get("X-App-Token")).toBe("token");
  });

  it("builds and parses direct occupation aggregates", async () => {
    const url = new URL(
      buildHawaiiCscDirectOccupationAggregatesUrl({ committeeId: "CC10174", electionPeriod: "2018-2022", limit: 5 })
    );
    expect(url.origin + url.pathname).toBe("https://hicscdata.hawaii.gov/resource/jexd-xbcg.json");
    expect(url.searchParams.get("$where")).toContain("reg_no = 'CC10174'");
    expect(url.searchParams.get("$where")).toContain("election_period = '2018-2022'");
    expect(url.searchParams.get("$where")).toContain("occupation IS NOT NULL");
    expect(url.searchParams.get("$group")).toBe("occupation");

    const fetchImpl = vi.fn().mockResolvedValue(
      jsonResponse([
        { category_name: "Attorney", total_amount: "332495", total_count: "276" },
        { category_name: "", total_amount: "100" },
      ])
    ) as unknown as typeof fetch;

    await expect(
      getHawaiiCscDirectOccupationAggregates(
        { committeeId: "CC10174", electionPeriod: "2018-2022", limit: 5 },
        { fetchImpl, timeoutMs: 1000 }
      )
    ).resolves.toEqual([{ categoryName: "Attorney", amount: 332495, count: 276 }]);
  });

  it("builds and parses contribution size aggregates", async () => {
    const url = new URL(
      buildHawaiiCscContributionSizeAggregatesUrl({ committeeId: "CC10174", electionPeriod: "2018-2022", limit: 5 })
    );
    expect(url.searchParams.get("$select")).toBe("amount");
    expect(url.searchParams.get("$where")).toContain("reg_no = 'CC10174'");
    expect(url.searchParams.get("$order")).toBe("amount DESC, :id ASC");

    const fetchImpl = vi
      .fn()
      .mockResolvedValueOnce(
        jsonResponse([{ amount: "25" }, { amount: "75" }, { amount: "1000" }, { amount: "5000" }, { amount: "-1" }])
      )
      .mockResolvedValueOnce(jsonResponse([])) as unknown as typeof fetch;

    await expect(
      getHawaiiCscContributionSizeAggregates(
        { committeeId: "CC10174", electionPeriod: "2018-2022", limit: 5 },
        { fetchImpl, timeoutMs: 1000, pageLimit: 5 }
      )
    ).resolves.toEqual([
      { categoryName: "5000_plus", amount: 5000, count: 1 },
      { categoryName: "1000_4999", amount: 1000, count: 1 },
      { categoryName: "under_100", amount: 100, count: 2 },
    ]);
  });

  it("aggregates explicit independent expenditure groups and skips ambiguous candidate rows", async () => {
    const url = new URL(buildHawaiiCscIndependentExpenditureGroupsUrl({ candidateName: "Josh Green", electionYear: 2022 }));
    expect(url.origin + url.pathname).toBe("https://hicscdata.hawaii.gov/resource/riiu-7d4b.json");
    expect(url.searchParams.get("$where")).toContain("independent_expenditure");
    expect(url.searchParams.get("$where")).toContain("support_oppose in('Support','Oppose')");
    expect(url.searchParams.get("$where")).toContain("lower(candidate_name_s) like '%green%'");
    expect(url.searchParams.get("$order")).toBe("amount DESC, noncandidate_committee_name ASC, :id ASC");

    const fetchImpl = vi
      .fn()
      .mockResolvedValueOnce(
        jsonResponse([
          {
            noncandidate_committee_name: "Be Change Now",
            reg_no: "NC20760",
            election_period: "2020-2022 (KP2)",
            candidate_name_s: "Green, Josh",
            support_oppose: "Support",
            independent_expenditure: "Y",
            amount: "500000",
          },
          {
            noncandidate_committee_name: "Be Change Now",
            reg_no: "NC20760",
            election_period: "2020-2022 (KP2)",
            candidate_name_s: "Green, Josh",
            support_oppose: "Support",
            independent_expenditure: "Y",
            amount: "557",
          },
          {
            noncandidate_committee_name: "Victory Calls 2022",
            reg_no: "NC20991",
            election_period: "2020-2022 (KP2)",
            candidate_name_s: "Green, Josh",
            support_oppose: "Oppose",
            independent_expenditure: "Y",
            amount: "234000",
          },
          {
            noncandidate_committee_name: "Ambiguous PAC",
            reg_no: "NC00000",
            election_period: "2020-2022 (KP2)",
            candidate_name_s: "Green, Josh and Luke, Sylvia",
            support_oppose: "Support",
            independent_expenditure: "Y",
            amount: "999999",
          },
          {
            noncandidate_committee_name: "Wrong Candidate PAC",
            reg_no: "NC00001",
            election_period: "2020-2022 (KP2)",
            candidate_name_s: "Greene, Madeline",
            support_oppose: "Support",
            independent_expenditure: "Y",
            amount: "999999",
          },
        ])
      )
      .mockResolvedValueOnce(jsonResponse([])) as unknown as typeof fetch;

    await expect(
      getHawaiiCscIndependentExpenditureGroups(
        { candidateName: "Josh Green", electionYear: 2022, limit: 10 },
        { fetchImpl, timeoutMs: 1000, pageLimit: 5 }
      )
    ).resolves.toEqual([
      {
        committeeId: "NC20760",
        committeeName: "Be Change Now",
        supportOppose: "support",
        amount: 500557,
        expenditureCount: 2,
        electionPeriod: "2020-2022 (KP2)",
      },
      {
        committeeId: "NC20991",
        committeeName: "Victory Calls 2022",
        supportOppose: "oppose",
        amount: 234000,
        expenditureCount: 1,
        electionPeriod: "2020-2022 (KP2)",
      },
    ]);
  });

  it("aggregates organization funders for noncandidate committees", async () => {
    const url = new URL(
      buildHawaiiCscNoncandidateCommitteeFundersUrl({ committeeId: "NC20760", electionPeriod: "2020-2022 (KP2)", limit: 5 })
    );
    expect(url.origin + url.pathname).toBe("https://hicscdata.hawaii.gov/resource/rajm-32md.json");
    expect(url.searchParams.get("$where")).toContain("reg_no = 'NC20760'");
    expect(url.searchParams.get("$where")).toContain("election_period = '2020-2022 (KP2)'");
    expect(url.searchParams.get("$order")).toBe("amount DESC, contributor_name ASC, :id ASC");

    const fetchImpl = vi
      .fn()
      .mockResolvedValueOnce(
        jsonResponse([
          {
            contributor_type: "Vendor / Business",
            contributor_name: "Hawaii Carpenters Market Recovery Program Fund",
            amount: "2086436.92",
          },
          {
            contributor_type: "Vendor / Business",
            contributor_name: "HAWAII CARPENTERS MARKET RECOVERY PROGRAM FUND",
            amount: "1000",
          },
          {
            contributor_type: "Individual",
            contributor_name: "Person, Example",
            amount: "999999",
          },
          {
            contributor_type: "Vendor / Business",
            contributor_name: "Bad Negative",
            amount: "-1",
          },
        ])
      )
      .mockResolvedValueOnce(jsonResponse([])) as unknown as typeof fetch;

    await expect(
      getHawaiiCscNoncandidateCommitteeFunders(
        { committeeId: "NC20760", electionPeriod: "2020-2022 (KP2)", limit: 5 },
        { fetchImpl, timeoutMs: 1000, pageLimit: 4 }
      )
    ).resolves.toEqual([
      {
        categoryName: "Hawaii Carpenters Market Recovery Program Fund",
        amount: 2087436.92,
        count: 2,
      },
    ]);
  });

  it("caps paged reads defensively", async () => {
    const fetchImpl = vi.fn().mockResolvedValue(jsonResponse([{ amount: "25" }])) as unknown as typeof fetch;

    await expect(
      getHawaiiCscContributionSizeAggregates(
        { committeeId: "CC10174", electionPeriod: "2018-2022", limit: 5 },
        { fetchImpl, timeoutMs: 1000, pageLimit: 1, maxPages: 1 }
      )
    ).rejects.toMatchObject({
      code: "bad_response",
      message: expect.stringContaining("exceeded 1 pages"),
    });
  });

  it("throws structured errors for HTTP and malformed responses", async () => {
    const httpFetch = vi.fn().mockResolvedValue(new Response("nope", { status: 500, statusText: "Server Error" })) as unknown as typeof fetch;
    await expect(
      searchHawaiiCscCandidateCommittees({ candidateName: "Josh Green", electionYear: 2022 }, { fetchImpl: httpFetch })
    ).rejects.toMatchObject({ code: "http_error", status: 500 });

    const badPayloadFetch = vi.fn().mockResolvedValue(jsonResponse({ results: [] })) as unknown as typeof fetch;
    await expect(
      searchHawaiiCscCandidateCommittees({ candidateName: "Josh Green", electionYear: 2022 }, { fetchImpl: badPayloadFetch })
    ).rejects.toMatchObject({ code: "bad_response" });
  });
});
