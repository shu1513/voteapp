import { describe, expect, it } from "vitest";

import { buildPresidentialNomineePrompt } from "../../src/ai/providers/presidentialNomineePrompt.js";

describe("buildPresidentialNomineePrompt", () => {
  it("builds a party-primary nominee research prompt with known candidates", () => {
    const prompt = buildPresidentialNomineePrompt({
      cycleId: "11111111-1111-4111-8111-111111111111",
      electionYear: 2028,
      party: "Democratic",
      candidates: [
        {
          candidateId: "candidate-1",
          displayName: "Jane President",
          party: "Democratic",
          fecIds: [" p80000001 "],
          sources: ["https://example.org/jane"],
        },
      ],
    });

    expect(prompt).toContain("Return strict JSON only.");
    expect(prompt).toContain('presidential_cycle_id: "11111111-1111-4111-8111-111111111111"');
    expect(prompt).toContain('election_name: "2028 Democratic presidential primary"');
    expect(prompt).toContain('- stage: "primary"');
    expect(prompt).toContain('- party: "Democratic"');
    expect(prompt).toContain("Known active primary candidates:");
    expect(prompt).toContain('- candidate_id: "candidate-1"');
    expect(prompt).toContain('display_name: "Jane President"');
    expect(prompt).toContain('fec_ids: ["P80000001"]');
    expect(prompt).toContain('"nominee_found": true');
    expect(prompt).toContain('"nominee_found": false');
    expect(prompt).toContain("nominee_found=true only when one of these is clearly true:");
    expect(prompt).toContain("the party has officially nominated the candidate");
    expect(prompt).toContain("the candidate has clinched a majority of delegates needed for nomination");
    expect(prompt).toContain("all meaningful remaining competitors have suspended/withdrawn");
    expect(prompt).toContain("Do not set nominee_found=true based only on polling");
    expect(prompt).toContain("being the frontrunner");
    expect(prompt).toContain("Use nominee_found=false when the candidate is only leading");
    expect(prompt).toContain("Use nominee_found=false when the primary is still unresolved or evidence is unclear.");
    expect(prompt).not.toContain("nominee_status");
  });

  it("trims party and includes retry feedback", () => {
    const prompt = buildPresidentialNomineePrompt({
      cycleId: "cycle-1",
      electionYear: 2028,
      party: " Republican ",
      candidates: [
        {
          candidateId: "candidate-1",
          displayName: "Jane GOP",
          party: "Republican",
          fecIds: [],
        },
      ],
      reviewFeedbackLines: ["The prior response named someone who is not in this primary."],
    });

    expect(prompt).toContain('election_name: "2028 Republican presidential primary"');
    expect(prompt).toContain('- party: "Republican"');
    expect(prompt).toContain("Previous feedback to fix:");
    expect(prompt).toContain("1. The prior response named someone who is not in this primary.");
  });

  it("rejects invalid context", () => {
    expect(() =>
      buildPresidentialNomineePrompt({
        cycleId: "cycle-1",
        electionYear: 2026,
        party: "Democratic",
        candidates: [{ candidateId: "candidate-1", displayName: "Jane", party: "Democratic", fecIds: [] }],
      })
    ).toThrow("Invalid presidential nominee prompt election year: 2026");

    expect(() =>
      buildPresidentialNomineePrompt({
        cycleId: " ",
        electionYear: 2028,
        party: "Democratic",
        candidates: [{ candidateId: "candidate-1", displayName: "Jane", party: "Democratic", fecIds: [] }],
      })
    ).toThrow("presidential_cycle_id is required");

    expect(() =>
      buildPresidentialNomineePrompt({
        cycleId: "cycle-1",
        electionYear: 2028,
        party: " ",
        candidates: [{ candidateId: "candidate-1", displayName: "Jane", party: "Democratic", fecIds: [] }],
      })
    ).toThrow("party is required");

    expect(() =>
      buildPresidentialNomineePrompt({
        cycleId: "cycle-1",
        electionYear: 2028,
        party: "Democratic",
        candidates: [],
      })
    ).toThrow("At least one presidential primary candidate is required");
  });
});
