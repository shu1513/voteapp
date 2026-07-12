import { beforeEach, describe, expect, it, vi } from "vitest";
import {
  autoLinkMissingLosAngelesCandidateFinanceLinks,
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
      async ({ officeName }) => [
        officeName === "Mayor"
          ? candidateTotal("Alex Mayor", officeName, "101")
          : candidateTotal("Bailey Attorney", officeName, "202"),
      ],
    );
    vi.mocked(upsertLosAngelesFinanceLink).mockResolvedValue("link-id");
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
      },
      {
        candidateId: "candidate-2",
        electionId: "election-2",
        candidateName: "Bailey Attorney",
        electionYear: 2026,
        electionDate: "2026-06-02",
        officeName: "City Attorney",
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
    ]);
    expect(getLosAngelesEthicsCandidateTotals).toHaveBeenCalledTimes(2);
    expect(getLosAngelesEthicsCandidateTotals).toHaveBeenNthCalledWith(
      2,
      { electionId: "76", officeName: "City Attorney" },
      undefined,
    );
  });
});
