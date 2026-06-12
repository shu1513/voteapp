import { describe, expect, it, vi } from "vitest";

import { resolveCandidateResearchMode } from "../../../src/ai/candidateResearchMode.js";
import { buildCandidateProfilePrompt } from "../../../src/ai/providers/candidateProfilePrompt.js";
import { loadPresidentialCycleProfileContext } from "../../../src/pipeline/presidential/presidentialProfileContext.js";

describe("resolveCandidateResearchMode presidential mode", () => {
  it("treats presidential contexts as federal president mode", () => {
    expect(
      resolveCandidateResearchMode({
        districtType: "presidential",
        officialBallotTitle: "President of the United States, 2028 Democratic primary",
      })
    ).toBe("federal_president");

    expect(
      resolveCandidateResearchMode({
        districtType: "statewide",
        officialBallotTitle: "President and Vice President",
      })
    ).toBe("federal_president");
  });
});

describe("loadPresidentialCycleProfileContext", () => {
  it("loads general presidential cycle profile context", async () => {
    const query = vi.fn().mockResolvedValue({
      rows: [
        {
          id: "cycle-general",
          election_year: 2028,
          stage: "general",
          party: null,
          election_date: "2028-11-07",
          sources: [" https://example.gov/general ", "https://example.gov/general"],
        },
      ],
    });

    await expect(loadPresidentialCycleProfileContext({ query }, " cycle-general ")).resolves.toEqual({
      cycleId: "cycle-general",
      electionYear: 2028,
      stage: "general",
      party: null,
      districtName: "United States",
      districtType: "presidential",
      state: "US",
      electionDate: "2028-11-07",
      officialBallotTitle: "President of the United States, 2028 general election",
      electionStage: "general",
      electionIsPartisan: true,
      seedUrls: ["https://example.gov/general"],
    });
    expect(query.mock.calls[0]?.[1]).toEqual(["cycle-general"]);
  });

  it("loads primary presidential cycle profile context without inventing an election date", async () => {
    const query = vi.fn().mockResolvedValue({
      rows: [
        {
          id: "cycle-dem-primary",
          election_year: 2028,
          stage: "primary",
          party: "Democratic",
          election_date: null,
          sources: "[\"https://example.gov/primary\"]",
        },
      ],
    });

    const context = await loadPresidentialCycleProfileContext({ query }, "cycle-dem-primary");

    expect(context).toMatchObject({
      cycleId: "cycle-dem-primary",
      electionYear: 2028,
      stage: "primary",
      party: "Democratic",
      districtName: "United States",
      districtType: "presidential",
      state: "US",
      electionDate: null,
      officialBallotTitle: "President of the United States, 2028 Democratic primary",
      electionStage: "primary",
      electionIsPartisan: true,
      seedUrls: ["https://example.gov/primary"],
    });
  });

  it("returns null without querying for blank cycle IDs", async () => {
    const query = vi.fn();

    await expect(loadPresidentialCycleProfileContext({ query }, "   ")).resolves.toBeNull();
    expect(query).not.toHaveBeenCalled();
  });

  it("omits election_date from presidential primary profile prompts", async () => {
    const prompt = buildCandidateProfilePrompt({
      candidateDisplayName: "Jane President",
      districtName: "United States",
      districtType: "presidential",
      state: "US",
      electionDate: null,
      officialBallotTitle: "President of the United States, 2028 Democratic primary",
      electionStage: "primary",
      researchMode: "federal_president",
      rosterParty: "Democratic",
      rosterFecIds: ["P80000001"],
      seedUrls: [],
    });

    expect(prompt).toContain('- district_type: "presidential"');
    expect(prompt).toContain('- research_mode: "federal_president"');
    expect(prompt).toContain('- candidate_fec_ids: ["P80000001"]');
    expect(prompt).not.toContain("- election_date:");
    expect(prompt).not.toContain('"date_of_birth"');
  });
});
