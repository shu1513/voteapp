import { afterEach, describe, expect, it, vi } from "vitest";

import {
  CaliforniaPowerSearchClientError,
  buildCaliforniaCandidateContributionSearchRequest,
  buildCaliforniaIndependentExpenditureSearchUrl,
  searchCaliforniaIndependentExpenditures,
  summarizeCaliforniaIndependentSpendingByCandidate,
  toCaliforniaElectionCycle,
} from "../../../src/pipeline/californiaFinance/californiaPowerSearchClient.js";

function jsonResponse(payload: unknown, init: ResponseInit = {}): Response {
  return new Response(JSON.stringify(payload), {
    status: 200,
    statusText: "OK",
    ...init,
  });
}

describe("californiaPowerSearchClient", () => {
  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  it("maps election years to California two-year cycles", () => {
    expect(toCaliforniaElectionCycle(2022)).toBe(2021);
    expect(toCaliforniaElectionCycle(2021)).toBe(2021);
    expect(toCaliforniaElectionCycle(2024)).toBe(2023);
  });

  it("builds independent expenditure search URLs", () => {
    const url = new URL(
      buildCaliforniaIndependentExpenditureSearchUrl({
        candidateName: " Newsom,   Gavin ",
        electionYear: 2022,
      })
    );

    expect(url.origin + url.pathname).toBe("https://powersearch.sos.ca.gov:3000/ie/search");
    expect(url.searchParams.get("candidatename")).toBe("Newsom, Gavin");
    expect(url.searchParams.get("electioncycle")).toBe("2021");
  });

  it("builds conservative contribution-search form requests", () => {
    const request = buildCaliforniaCandidateContributionSearchRequest({
      candidateName: "Newsom, Gavin",
      electionYear: 2022,
      officeName: "Governor",
      contributorState: "ca",
    });

    expect(request.url).toBe("https://powersearch.sos.ca.gov/advanced.php");
    expect(request.method).toBe("POST");
    expect(request.headers["content-type"]).toBe("application/x-www-form-urlencoded");
    expect(request.body.get("contrib_select")).toBe("all");
    expect(request.body.get("state_list")).toBe("CA");
    expect(request.body.get("contrib_types")).toBe("search_candidates");
    expect(request.body.get("search_candidates")).toBe("Newsom, Gavin");
    expect(request.body.get("office_list")).toBe("Governor");
    expect(request.body.get("date_select")).toBe("cycle");
    expect(request.body.getAll("cycles[]")).toEqual(["2021"]);
  });

  it("parses independent expenditure rows and skips malformed rows", async () => {
    const fetchImpl = vi.fn().mockResolvedValue(
      jsonResponse({
        amount: 3,
        payload: [
          {
            Amount: 1273.2,
            DateRange: "2022-10-28",
            ExpenderID: "1267335",
            ExpenderName: "Democratic Club of Ventura",
            TargetCandidateName: "Newsom, Gavin",
            TargetCandidateOffice: "Governor",
            ExpenderPosition: "S",
            ExpenditureDscr: "Mailer",
            PayeeName: "Example Vendor",
          },
          {
            Amount: "90112.00",
            DateStart: "2021-09-01T00:00:00Z",
            ExpenderID: "1442978",
            ExpenderName: "SAFE CA INC",
            TargetCandidateName: "Newsom, Gavin",
            TargetCandidateOffice: "Governor",
            ExpenderPosition: "O",
          },
          {
            Amount: -5,
            ExpenderID: "bad",
            ExpenderName: "Bad Row",
            TargetCandidateName: "Newsom, Gavin",
            ExpenderPosition: "S",
          },
        ],
      })
    ) as unknown as typeof fetch;

    await expect(
      searchCaliforniaIndependentExpenditures(
        { candidateName: "Newsom, Gavin", electionYear: 2022 },
        { fetchImpl, timeoutMs: 1000 }
      )
    ).resolves.toEqual({
      candidateName: "Newsom, Gavin",
      electionCycle: 2021,
      reportedRowCount: 3,
      sourceUrl: "https://powersearch.sos.ca.gov:3000/ie/search?candidatename=Newsom%2C+Gavin&electioncycle=2021",
      expenditures: [
        {
          candidateName: "Newsom, Gavin",
          candidateOffice: "Governor",
          expenderId: "1267335",
          expenderName: "Democratic Club of Ventura",
          supportOppose: "support",
          amount: 1273.2,
          expenditureDate: "2022-10-28",
          description: "Mailer",
          payeeName: "Example Vendor",
          sourceUrl: "https://powersearch.sos.ca.gov:3000/ie/search?candidatename=Newsom%2C+Gavin&electioncycle=2021",
        },
        {
          candidateName: "Newsom, Gavin",
          candidateOffice: "Governor",
          expenderId: "1442978",
          expenderName: "SAFE CA INC",
          supportOppose: "oppose",
          amount: 90112,
          expenditureDate: "2021-09-01",
          sourceUrl: "https://powersearch.sos.ca.gov:3000/ie/search?candidatename=Newsom%2C+Gavin&electioncycle=2021",
        },
      ],
    });
  });

  it("filters independent expenditure rows to the requested candidate name", async () => {
    const fetchImpl = vi.fn().mockResolvedValue(
      jsonResponse({
        amount: 3,
        payload: [
          {
            Amount: 100,
            ExpenderID: "1",
            ExpenderName: "Support Group",
            TargetCandidateName: "Newsom, Gavin",
            ExpenderPosition: "S",
          },
          {
            Amount: 200,
            ExpenderID: "2",
            ExpenderName: "Also Support",
            TargetCandidateName: "Gavin Newsom",
            ExpenderPosition: "S",
          },
          {
            Amount: 300,
            ExpenderID: "3",
            ExpenderName: "Wrong Candidate Group",
            TargetCandidateName: "Newsom, Jennifer",
            ExpenderPosition: "S",
          },
        ],
      })
    ) as unknown as typeof fetch;

    await expect(
      summarizeCaliforniaIndependentSpendingByCandidate(
        { candidateName: "Gavin Newsom", electionYear: 2022 },
        { fetchImpl, timeoutMs: 1000 }
      )
    ).resolves.toMatchObject({
      supportTotal: 300,
      opposeTotal: 0,
      groups: [
        {
          expenderId: "2",
          expenderName: "Also Support",
          amount: 200,
        },
        {
          expenderId: "1",
          expenderName: "Support Group",
          amount: 100,
        },
      ],
    });
  });

  it("retries independent expenditure searches with California last-first names", async () => {
    const fetchImpl = vi
      .fn()
      .mockResolvedValueOnce(
        jsonResponse({
          amount: 0,
          payload: [],
        })
      )
      .mockResolvedValueOnce(
        jsonResponse({
          amount: 1,
          payload: [
            {
              Amount: 500,
              ExpenderID: "1455754",
              ExpenderName: "Support Committee",
              TargetCandidateName: "Wahab, Aisha",
              ExpenderPosition: "S",
            },
          ],
        })
      ) as unknown as typeof fetch;

    await expect(
      summarizeCaliforniaIndependentSpendingByCandidate(
        { candidateName: "Aisha Wahab", electionYear: 2022 },
        { fetchImpl, timeoutMs: 1000 }
      )
    ).resolves.toMatchObject({
      candidateName: "Aisha Wahab",
      supportTotal: 500,
      opposeTotal: 0,
      groups: [
        {
          expenderId: "1455754",
          expenderName: "Support Committee",
          amount: 500,
        },
      ],
    });

    expect(fetchImpl).toHaveBeenCalledTimes(2);
    expect(String(fetchImpl.mock.calls[0]?.[0])).toContain("candidatename=Aisha+Wahab");
    expect(String(fetchImpl.mock.calls[1]?.[0])).toContain("candidatename=Wahab%2C+Aisha");
  });

  it("rejects California election years before Power Search coverage", () => {
    expect(() => toCaliforniaElectionCycle(2000)).toThrow("Invalid California election year");
  });

  it("summarizes independent spending totals and groups", async () => {
    const fetchImpl = vi.fn().mockResolvedValue(
      jsonResponse({
        amount: 3,
        payload: [
          {
            Amount: 100,
            ExpenderID: "1",
            ExpenderName: "Support Group",
            TargetCandidateName: "Newsom, Gavin",
            ExpenderPosition: "S",
          },
          {
            Amount: 200,
            ExpenderID: "1",
            ExpenderName: "Support Group",
            TargetCandidateName: "Newsom, Gavin",
            ExpenderPosition: "S",
          },
          {
            Amount: 50,
            ExpenderID: "2",
            ExpenderName: "Oppose Group",
            TargetCandidateName: "Newsom, Gavin",
            ExpenderPosition: "O",
          },
        ],
      })
    ) as unknown as typeof fetch;

    await expect(
      summarizeCaliforniaIndependentSpendingByCandidate(
        { candidateName: "Newsom, Gavin", electionYear: 2022 },
        { fetchImpl, timeoutMs: 1000 }
      )
    ).resolves.toMatchObject({
      candidateName: "Newsom, Gavin",
      electionCycle: 2021,
      supportTotal: 300,
      opposeTotal: 50,
      groups: [
        {
          expenderId: "1",
          expenderName: "Support Group",
          supportOppose: "support",
          amount: 300,
          count: 2,
        },
        {
          expenderId: "2",
          expenderName: "Oppose Group",
          supportOppose: "oppose",
          amount: 50,
          count: 1,
        },
      ],
    });
  });

  it("rejects malformed Power Search responses", async () => {
    const fetchImpl = vi.fn().mockResolvedValue(jsonResponse({ payload: null })) as unknown as typeof fetch;

    await expect(
      searchCaliforniaIndependentExpenditures(
        { candidateName: "Newsom, Gavin", electionYear: 2022 },
        { fetchImpl, timeoutMs: 1000 }
      )
    ).rejects.toMatchObject({
      name: "CaliforniaPowerSearchClientError",
      code: "bad_response",
    });
  });

  it("wraps HTTP failures with a typed error", async () => {
    const fetchImpl = vi.fn().mockResolvedValue(jsonResponse({}, { status: 503, statusText: "Service Unavailable" })) as
      unknown as typeof fetch;

    await expect(
      searchCaliforniaIndependentExpenditures(
        { candidateName: "Newsom, Gavin", electionYear: 2022 },
        { fetchImpl, timeoutMs: 1000 }
      )
    ).rejects.toBeInstanceOf(CaliforniaPowerSearchClientError);
  });
});
