import { describe, expect, it } from "vitest";

import {
  matchCandidateElectionResultWinner,
  matchCandidateElectionResultWinners,
} from "../../src/pipeline/electionResults/candidateElectionResultMatcher.js";
import type { ElectionResultCandidateContext } from "../../src/pipeline/electionResults/electionResultContextLoader.js";

const JANE: ElectionResultCandidateContext = {
  candidateElectionId: "10000000-0000-0000-0000-000000000001",
  candidateId: "20000000-0000-0000-0000-000000000001",
  displayName: "Jane Q. Candidate",
  party: "Democratic",
  isIncumbent: false,
  status: "declared",
  fecIds: [],
  stateFilingIds: [],
};

const JOHN: ElectionResultCandidateContext = {
  candidateElectionId: "10000000-0000-0000-0000-000000000002",
  candidateId: "20000000-0000-0000-0000-000000000002",
  displayName: "John Candidate",
  party: "Republican",
  isIncumbent: false,
  status: "declared",
  fecIds: [],
  stateFilingIds: [],
};

describe("matchCandidateElectionResultWinner", () => {
  it("matches exact candidate_election_id first", () => {
    const match = matchCandidateElectionResultWinner(
      {
        candidate_election_id: JANE.candidateElectionId,
      },
      [JANE, JOHN]
    );

    expect(match.method).toBe("exact_candidate_election_id");
    expect(match.confidence).toBe(1);
    expect(match.winner.candidate_id).toBe(JANE.candidateId);
    expect(match.winner.candidate_name).toBe(JANE.displayName);
  });

  it("matches exact normalized name plus normalized party", () => {
    const match = matchCandidateElectionResultWinner(
      {
        candidate_name: "Jane Q Candidate",
        party: "Democrat",
      },
      [JANE, JOHN]
    );

    expect(match.method).toBe("exact_name_party");
    expect(match.winner.candidate_election_id).toBe(JANE.candidateElectionId);
  });

  it("matches exact normalized name when party is absent", () => {
    const match = matchCandidateElectionResultWinner(
      {
        candidate_name: "John Candidate",
      },
      [JANE, JOHN]
    );

    expect(match.method).toBe("exact_name");
    expect(match.winner.candidate_election_id).toBe(JOHN.candidateElectionId);
  });

  it("matches high-confidence fuzzy names with middle initials omitted", () => {
    const match = matchCandidateElectionResultWinner(
      {
        candidate_name: "Jane Candidate",
      },
      [JANE, JOHN]
    );

    expect(match.method).toBe("fuzzy_name");
    expect(match.confidence).toBeGreaterThanOrEqual(0.88);
    expect(match.winner.candidate_election_id).toBe(JANE.candidateElectionId);
  });

  it("leaves ambiguous or low-confidence names unmatched", () => {
    const match = matchCandidateElectionResultWinner(
      {
        candidate_name: "Candidate",
      },
      [JANE, JOHN]
    );

    expect(match.method).toBe("unmatched");
    expect(match.winner.candidate_election_id).toBeUndefined();
    expect(match.winner.candidate_id).toBeUndefined();
  });

  it("matches each winner independently", () => {
    const matches = matchCandidateElectionResultWinners(
      [
        { candidate_name: "Jane Candidate" },
        { candidate_name: "John Candidate" },
      ],
      [JANE, JOHN]
    );

    expect(matches.map((match) => match.winner.candidate_election_id)).toEqual([
      JANE.candidateElectionId,
      JOHN.candidateElectionId,
    ]);
  });
});
