import { describe, expect, it } from "vitest";

import { buildElectionResultPrompt } from "../../src/ai/providers/electionResultPrompt.js";
import type { ElectionResultContext } from "../../src/pipeline/electionResults/electionResultContextLoader.js";

function makeContext(overrides: Partial<ElectionResultContext> = {}): ElectionResultContext {
  return {
    electionId: "00000000-0000-0000-0000-000000000001",
    raceType: "office",
    officialBallotTitle: "United States Representative, District 31",
    electionDate: "2026-06-02",
    electionStage: "primary",
    isPartisan: true,
    discoveryContestFamily: "us_senate",
    sourceUrls: ["https://elections.example.gov/races/31"],
    district: {
      id: "district-1",
      name: "California Congressional District 31",
      districtType: "us_house",
      state: "CA",
    },
    candidates: [
      {
        candidateElectionId: "10000000-0000-0000-0000-000000000001",
        candidateId: "20000000-0000-0000-0000-000000000001",
        displayName: "Gil Cisneros",
        party: "Democratic",
        isIncumbent: true,
        status: "declared",
        fecIds: ["H8CA39174"],
        stateFilingIds: [],
      },
    ],
    ballotMeasure: null,
    ...overrides,
  };
}

describe("buildElectionResultPrompt", () => {
  it("includes exact output shape, roster ids, and authority rules", () => {
    const prompt = buildElectionResultPrompt({
      passType: "election_night",
      scheduledFor: "2026-06-03T03:10:00.000Z",
      contexts: [makeContext()],
    });

    expect(prompt).toContain("Return strict JSON only.");
    expect(prompt).toContain('"election_id": "provided election_id"');
    expect(prompt).toContain('candidate_election_id: "10000000-0000-0000-0000-000000000001"');
    expect(prompt).toContain('"candidate_name": "required only when no candidate_election_id is available"');
    expect(prompt).not.toContain('"candidate_id": "provided candidate_id when matched"');
    expect(prompt).toContain("Candidate winners must use the provided candidate_election_id when the winner appears in the provided roster.");
    expect(prompt).toContain("Return exactly one result row for each provided election_id");
    expect(prompt).toContain("AP/news sources are allowed only when result_status=\"projected\"");
    expect(prompt).not.toContain("Do not include vote counts");
  });

  it("includes ballot measure context", () => {
    const prompt = buildElectionResultPrompt({
      passType: "certified",
      scheduledFor: "2026-07-10T15:00:00.000Z",
      contexts: [
        makeContext({
          raceType: "ballot_measure",
          candidates: [],
          ballotMeasure: {
            ballotMeasureId: "30000000-0000-0000-0000-000000000001",
            officialBallotTitle: "Proposition 4",
            summary: null,
            whatYesMeans: null,
            whatNoMeans: null,
            result: null,
            sourceUrls: [],
            officialMeasureUrl: "https://elections.example.gov/prop-4",
          },
        }),
      ],
    });

    expect(prompt).toContain('ballot_measure_id: "30000000-0000-0000-0000-000000000001"');
    expect(prompt).toContain("For ballot measures, winners must be []");
  });

  it("escapes quoted and multiline context values", () => {
    const prompt = buildElectionResultPrompt({
      passType: "election_night",
      scheduledFor: "2026-06-03T03:10:00.000Z",
      contexts: [
        makeContext({
          officialBallotTitle: 'Measure "A"\nSchool Bond',
          district: {
            ...makeContext().district,
            name: 'Los Angeles "Unified"\nSchool District',
          },
          candidates: [
            {
              ...makeContext().candidates[0]!,
              displayName: 'Jane "JJ"\nCandidate',
              party: 'Independent "No Party"',
              fecIds: ['H8"CA"\n39174'],
            },
          ],
        }),
      ],
    });

    expect(prompt).toContain('official_ballot_title: "Measure \\"A\\"\\nSchool Bond"');
    expect(prompt).toContain('district_name: "Los Angeles \\"Unified\\"\\nSchool District"');
    expect(prompt).toContain('name: "Jane \\"JJ\\"\\nCandidate"');
    expect(prompt).toContain('party: "Independent \\"No Party\\""');
    expect(prompt).toContain('fec_ids: ["H8\\"CA\\"\\n39174"]');
  });

  it("rejects oversized chunks", () => {
    expect(() =>
      buildElectionResultPrompt({
        passType: "election_night",
        scheduledFor: "2026-06-03T03:10:00.000Z",
        contexts: Array.from({ length: 11 }, (_, index) =>
          makeContext({ electionId: `00000000-0000-0000-0000-${String(index + 1).padStart(12, "0")}` })
        ),
      })
    ).toThrow("at most 10");
  });
});
