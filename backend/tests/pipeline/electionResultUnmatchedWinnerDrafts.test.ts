import { describe, expect, it } from "vitest";

import type { ElectionResultPayload } from "../../src/contracts/electionResultPayloadContract.js";
import { buildCandidateProfileDraftsForUnmatchedElectionResultWinners } from "../../src/pipeline/electionResults/electionResultUnmatchedWinnerDrafts.js";
import type { ElectionResultContext } from "../../src/pipeline/electionResults/electionResultContextLoader.js";

const ELECTION_ID = "00000000-0000-0000-0000-000000000001";

function makeContext(overrides: Partial<ElectionResultContext> = {}): ElectionResultContext {
  return {
    electionId: ELECTION_ID,
    raceType: "office",
    officialBallotTitle: "Judge of the Superior Court, Office No. 116",
    electionDate: "2026-06-02",
    electionStage: "primary",
    isPartisan: false,
    discoveryContestFamily: "judicial_office",
    sourceUrls: [],
    district: {
      id: "district-1",
      name: "Los Angeles County",
      districtType: "county",
      state: "CA",
    },
    candidates: [],
    ballotMeasure: null,
    ...overrides,
  };
}

function makePayload(overrides: Partial<ElectionResultPayload["results"][number]> = {}): ElectionResultPayload {
  return {
    results: [
      {
        election_id: ELECTION_ID,
        result_status: "unofficial",
        outcome: "won",
        winners: [{ candidate_name: "Pat Connolly", party: "Nonpartisan" }],
        match_status: "unmatched",
        source_url: "https://results.example.gov/race-116",
        source_type: "official",
        notes: "",
        ...overrides,
      },
    ],
  };
}

describe("buildCandidateProfileDraftsForUnmatchedElectionResultWinners", () => {
  it("creates conservative profile drafts for unmatched office winners", () => {
    const drafts = buildCandidateProfileDraftsForUnmatchedElectionResultWinners({
      contexts: [makeContext()],
      payload: makePayload(),
      runId: "run-1",
    });

    expect(drafts).toEqual([
      {
        electionId: ELECTION_ID,
        runId: "run-1",
        displayName: "Pat Connolly",
        rosterIndex: 100_000,
        rosterParty: "Nonpartisan",
        disambiguationHint:
          "Winner/advancer reported by election result source for Judge of the Superior Court, Office No. 116.",
        skipPerElectionNameDedupe: false,
        seedUrls: ["https://results.example.gov/race-116"],
        dedupeKey: `election_result_winner:${ELECTION_ID}:pat connolly`,
      },
    ]);
  });

  it("does not create drafts for matched winners or ballot measures", () => {
    expect(
      buildCandidateProfileDraftsForUnmatchedElectionResultWinners({
        contexts: [makeContext()],
        payload: makePayload({
          winners: [{ candidate_election_id: "ce-1", candidate_name: "Pat Connolly" }],
          match_status: "matched",
        }),
        runId: "run-1",
      })
    ).toEqual([]);

    expect(
      buildCandidateProfileDraftsForUnmatchedElectionResultWinners({
        contexts: [makeContext({ raceType: "ballot_measure", candidates: [], ballotMeasure: null })],
        payload: makePayload({ winners: [], match_status: "not_applicable", outcome: "passed" }),
        runId: "run-1",
      })
    ).toEqual([]);
  });
});
