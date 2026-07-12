import { describe, expect, it } from "vitest";
import {
  normalizeLosAngelesCandidateName,
  resolveLosAngelesCandidateCommittee,
  resolveLosAngelesEthicsElection,
} from "../../../src/pipeline/losAngelesCityFinance/losAngelesCandidateCommitteeResolver.js";
const candidate = {
  electionId: "76",
  electionSeatId: "276",
  electionSeatCandidateId: "1509",
  candidatePersonId: "172",
  candidateName: "Karen Bass",
  officeName: "Mayor",
  reportedThrough: "2026-05-27",
  fppcCommitteeId: "1471359",
  committeeName: "Bass",
  internalCommitteePersonId: "24713",
  totalContributions: 1,
  totalExpenditures: 1,
  cashOnHand: 1,
  matchingFunds: 1,
  outsideSupportTotal: 1,
  outsideOpposeTotal: 1,
  membershipSupportTotal: 1,
  membershipOpposeTotal: 1,
  sourceUrl: "https://example.test",
};
describe("Los Angeles candidate resolver", () => {
  it("normalizes punctuation and suffixes", () =>
    expect(normalizeLosAngelesCandidateName("José A. Doe, Jr.")).toBe(
      "JOSE A DOE",
    ));
  it("requires one exact name and office", () => {
    expect(
      resolveLosAngelesCandidateCommittee({
        candidateName: "Karen Bass",
        officeName: "Mayor",
        candidates: [candidate],
      }).status,
    ).toBe("matched");
    expect(
      resolveLosAngelesCandidateCommittee({
        candidateName: "Karen Bass",
        officeName: "Mayor",
        candidates: [candidate, candidate],
      }).status,
    ).toBe("ambiguous");
  });
  it("does not collide same-name candidates across council seats", () => {
    expect(
      resolveLosAngelesCandidateCommittee({
        candidateName: "Jordan Lee",
        officeName: "Council District 1",
        candidates: [
          {
            ...candidate,
            candidateName: "Jordan Lee",
            officeName: "Council District 1",
          },
          {
            ...candidate,
            candidateName: "Jordan Lee",
            officeName: "Council District 11",
          },
        ],
      }),
    ).toMatchObject({
      status: "matched",
      candidate: { officeName: "Council District 1" },
    });
  });
  it("requires one City and LAUSD election for year", () => {
    expect(
      resolveLosAngelesEthicsElection({
        elections: [
          {
            electionId: "76",
            description: "2026 City and LAUSD Elections",
            electionYear: 2026,
          },
        ],
        electionYear: 2026,
      })?.electionId,
    ).toBe("76");
  });
});
