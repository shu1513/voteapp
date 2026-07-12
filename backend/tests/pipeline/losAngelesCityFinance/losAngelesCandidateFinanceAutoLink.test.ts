import { beforeEach, describe, expect, it, vi } from "vitest";
import {
  autoLinkMissingLosAngelesCandidateFinanceLinks,
  listLosAngelesCandidateElectionsMissingFinanceLinks,
  type LosAngelesFinanceAutoLinkCandidate,
} from "../../../src/pipeline/losAngelesCityFinance/losAngelesCandidateFinanceAutoLink.js";
import {
  getLosAngelesEthicsCandidateTotals,
  getLosAngelesEthicsElections,
  type LosAngelesEthicsCandidateTotal,
} from "../../../src/pipeline/losAngelesCityFinance/losAngelesCityEthicsClient.js";
import { upsertLosAngelesFinanceLink } from "../../../src/pipeline/losAngelesCityFinance/losAngelesFinanceWriter.js";

vi.mock(
  "../../../src/pipeline/losAngelesCityFinance/losAngelesCityEthicsClient.js",
  async (importOriginal) => ({
    ...(await importOriginal<object>()),
    getLosAngelesEthicsCandidateTotals: vi.fn(),
    getLosAngelesEthicsElections: vi.fn(),
  }),
);
vi.mock(
  "../../../src/pipeline/losAngelesCityFinance/losAngelesFinanceWriter.js",
  async (importOriginal) => ({
    ...(await importOriginal<object>()),
    upsertLosAngelesFinanceLink: vi.fn(),
  }),
);

const NOW = new Date("2026-07-12T00:00:00.000Z");

function candidateTotal(
  candidateName: string,
  officeName: string,
  candidatePersonId: string,
): LosAngelesEthicsCandidateTotal {
  return {
    electionId: "76",
    electionSeatId: "276",
    electionSeatCandidateId: `seat-${candidatePersonId}`,
    candidatePersonId,
    candidateName,
    officeName,
    reportedThrough: "2026-07-01",
    fppcCommitteeId: `committee-${candidatePersonId}`,
    committeeName: `${candidateName} Committee`,
    internalCommitteePersonId: null,
    totalContributions: 100,
    totalExpenditures: 50,
    cashOnHand: 50,
    matchingFunds: 0,
    outsideSupportTotal: 0,
    outsideOpposeTotal: 0,
    membershipSupportTotal: 0,
    membershipOpposeTotal: 0,
    sourceUrl: "https://ethics.lacity.gov/election/76",
  };
}

describe("Los Angeles candidate finance auto-link", () => {
  beforeEach(() => {
    vi.mocked(getLosAngelesEthicsElections).mockResolvedValue([
      {
        electionId: "76",
        description: "2026 City and LAUSD Elections",
        electionYear: 2026,
      },
    ]);
    vi.mocked(getLosAngelesEthicsCandidateTotals).mockImplementation(
      async ({ officeName }) => {
        if (officeName === "Mayor")
          return [candidateTotal("Alex Mayor", officeName, "101")];
        if (officeName === "City Attorney")
          return [candidateTotal("Bailey Attorney", officeName, "202")];
        return [candidateTotal("Casey Controller", officeName, "303")];
      },
    );
    vi.mocked(upsertLosAngelesFinanceLink).mockResolvedValue("link-id");
  });

  it("lists all Phase 3 canonical offices from the shared allowlist", async () => {
    const query = vi.fn().mockResolvedValue({ rows: [], rowCount: 0 });
    await listLosAngelesCandidateElectionsMissingFinanceLinks(
      { query },
      {
        now: NOW,
        maxCandidates: 25,
        electionLookbackDays: 45,
        electionLookaheadDays: 730,
      },
    );
    expect(String(query.mock.calls[0]?.[0])).toContain(
      "office.canonical_name=ANY($5::text[])",
    );
    expect(query.mock.calls[0]?.[1]?.[4]).toEqual([
      "Mayor",
      "Municipal Attorney",
      "Municipal Controller",
      "City Council Member",
      "School Board Member",
    ]);
  });

  it("extracts the council seat from the election title", async () => {
    const query = vi.fn().mockResolvedValue({
      rows: [
        {
          candidate_id: "candidate",
          election_id: "election",
          candidate_name: "Jordan Lee",
          election_year: 2026,
          election_date: "2026-11-03",
          office_name: "City Council Member",
          official_ballot_title: "Member of the City Council, District No. 3",
        },
      ],
      rowCount: 1,
    });
    await expect(
      listLosAngelesCandidateElectionsMissingFinanceLinks(
        { query },
        {
          now: NOW,
          maxCandidates: 25,
          electionLookbackDays: 45,
          electionLookaheadDays: 730,
        },
      ),
    ).resolves.toEqual([
      {
        candidateId: "candidate",
        electionId: "election",
        candidateName: "Jordan Lee",
        electionYear: 2026,
        electionDate: "2026-11-03",
        officeName: "City Council Member",
        seatNumber: 3,
      },
    ]);
  });

  it("extracts the LAUSD board seat from the election title", async () => {
    const query = vi.fn().mockResolvedValue({
      rows: [
        {
          candidate_id: "candidate",
          election_id: "election",
          candidate_name: "Kelly Gonez",
          election_year: 2026,
          election_date: "2026-06-02",
          office_name: "School Board Member",
          official_ballot_title:
            "Member of the Board of Education, District 6",
        },
      ],
      rowCount: 1,
    });
    await expect(
      listLosAngelesCandidateElectionsMissingFinanceLinks(
        { query },
        {
          now: NOW,
          maxCandidates: 25,
          electionLookbackDays: 45,
          electionLookaheadDays: 730,
        },
      ),
    ).resolves.toEqual([
      {
        candidateId: "candidate",
        electionId: "election",
        candidateName: "Kelly Gonez",
        electionYear: 2026,
        electionDate: "2026-06-02",
        officeName: "School Board Member",
        seatNumber: 6,
      },
    ]);
    expect(String(query.mock.calls[0]?.[0])).toContain(
      "district.geoid_compact='0622710'",
    );
  });

  it("caches candidate totals by election and office", async () => {
    const candidates: LosAngelesFinanceAutoLinkCandidate[] = [
      {
        candidateId: "candidate-1",
        electionId: "election-1",
        candidateName: "Alex Mayor",
        electionYear: 2026,
        electionDate: "2026-06-02",
        officeName: "Mayor",
        seatNumber: null,
      },
      {
        candidateId: "candidate-2",
        electionId: "election-2",
        candidateName: "Bailey Attorney",
        electionYear: 2026,
        electionDate: "2026-06-02",
        officeName: "Municipal Attorney",
        seatNumber: null,
      },
      {
        candidateId: "candidate-3",
        electionId: "election-3",
        candidateName: "Casey Controller",
        electionYear: 2026,
        electionDate: "2026-06-02",
        officeName: "Municipal Controller",
        seatNumber: null,
      },
    ];

    await expect(
      autoLinkMissingLosAngelesCandidateFinanceLinks({
        db: { query: vi.fn() },
        now: NOW,
        candidates,
      }),
    ).resolves.toEqual([
      {
        candidateId: "candidate-1",
        electionId: "election-1",
        status: "linked",
      },
      {
        candidateId: "candidate-2",
        electionId: "election-2",
        status: "linked",
      },
      {
        candidateId: "candidate-3",
        electionId: "election-3",
        status: "linked",
      },
    ]);
    expect(getLosAngelesEthicsCandidateTotals).toHaveBeenCalledTimes(3);
    expect(getLosAngelesEthicsCandidateTotals).toHaveBeenNthCalledWith(
      2,
      { electionId: "76", officeName: "City Attorney" },
      undefined,
    );
    expect(getLosAngelesEthicsCandidateTotals).toHaveBeenNthCalledWith(
      3,
      { electionId: "76", officeName: "City Controller" },
      undefined,
    );
    expect(upsertLosAngelesFinanceLink).toHaveBeenNthCalledWith(
      2,
      expect.objectContaining({
        link: expect.objectContaining({ officeName: "Municipal Attorney" }),
      }),
    );
  });

  it("maps an exact LAUSD board seat and candidate", async () => {
    vi.mocked(getLosAngelesEthicsCandidateTotals).mockResolvedValue([
      candidateTotal("Kelly Gonez", "LAUSD District 6", "13549"),
    ]);
    await expect(
      autoLinkMissingLosAngelesCandidateFinanceLinks({
        db: { query: vi.fn() },
        now: NOW,
        candidates: [
          {
            candidateId: "candidate",
            electionId: "election",
            candidateName: "Kelly Gonez",
            electionYear: 2026,
            electionDate: "2026-06-02",
            officeName: "School Board Member",
            seatNumber: 6,
          },
        ],
      }),
    ).resolves.toEqual([
      {
        candidateId: "candidate",
        electionId: "election",
        status: "linked",
      },
    ]);
    expect(getLosAngelesEthicsCandidateTotals).toHaveBeenCalledWith(
      { electionId: "76", officeName: "LAUSD District 6" },
      undefined,
    );
    expect(upsertLosAngelesFinanceLink).toHaveBeenCalledWith(
      expect.objectContaining({
        link: expect.objectContaining({
          officeName: "School Board Member",
          seatNumber: 6,
        }),
      }),
    );
  });

  it("maps council titles to exact Ethics seats and stores the seat number", async () => {
    vi.mocked(getLosAngelesEthicsCandidateTotals).mockImplementation(
      async ({ officeName }) => [
        candidateTotal(
          "Jordan Lee",
          officeName,
          officeName.endsWith("3") ? "303" : "311",
        ),
      ],
    );
    const candidates: LosAngelesFinanceAutoLinkCandidate[] = [
      {
        candidateId: "candidate-3",
        electionId: "election-3",
        candidateName: "Jordan Lee",
        electionYear: 2026,
        electionDate: "2026-11-03",
        officeName: "City Council Member",
        seatNumber: 3,
      },
      {
        candidateId: "candidate-11",
        electionId: "election-11",
        candidateName: "Jordan Lee",
        electionYear: 2026,
        electionDate: "2026-11-03",
        officeName: "City Council Member",
        seatNumber: 11,
      },
    ];

    await expect(
      autoLinkMissingLosAngelesCandidateFinanceLinks({
        db: { query: vi.fn() },
        now: NOW,
        candidates,
      }),
    ).resolves.toEqual([
      {
        candidateId: "candidate-3",
        electionId: "election-3",
        status: "linked",
      },
      {
        candidateId: "candidate-11",
        electionId: "election-11",
        status: "linked",
      },
    ]);
    expect(getLosAngelesEthicsCandidateTotals).toHaveBeenNthCalledWith(
      1,
      { electionId: "76", officeName: "Council District 3" },
      undefined,
    );
    expect(getLosAngelesEthicsCandidateTotals).toHaveBeenNthCalledWith(
      2,
      { electionId: "76", officeName: "Council District 11" },
      undefined,
    );
    expect(upsertLosAngelesFinanceLink).toHaveBeenNthCalledWith(
      1,
      expect.objectContaining({
        link: expect.objectContaining({
          officeName: "City Council Member",
          seatNumber: 3,
        }),
      }),
    );
  });

  it("skips an unrecognized council title instead of making a citywide match", async () => {
    await expect(
      autoLinkMissingLosAngelesCandidateFinanceLinks({
        db: { query: vi.fn() },
        now: NOW,
        candidates: [
          {
            candidateId: "candidate",
            electionId: "election",
            candidateName: "Jordan Lee",
            electionYear: 2026,
            electionDate: "2026-11-03",
            officeName: "City Council Member",
            seatNumber: null,
          },
        ],
      }),
    ).resolves.toEqual([
      expect.objectContaining({
        candidateId: "candidate",
        electionId: "election",
        status: "not_found",
        reason:
          "Council seat number could not be parsed from the official ballot title",
      }),
    ]);
    expect(getLosAngelesEthicsCandidateTotals).not.toHaveBeenCalled();
    expect(upsertLosAngelesFinanceLink).not.toHaveBeenCalled();
  });
});
