import { describe, expect, it } from "vitest";

import { buildPresidentialRosterStatusPrompt } from "../../src/ai/providers/presidentialRosterStatusPrompt.js";

describe("buildPresidentialRosterStatusPrompt", () => {
  it("builds a party-primary omitted-candidate status verification prompt", () => {
    const prompt = buildPresidentialRosterStatusPrompt({
      cycleId: "cycle-1",
      electionYear: 2028,
      stage: "primary",
      party: "Democratic",
      candidates: [
        {
          candidateId: "candidate-1",
          displayName: "Jane President",
          party: "Democratic",
          fecIds: ["P80000001"],
          sources: ["https://example.org/jane"],
        },
      ],
    });

    expect(prompt).toContain("verifying the current status of presidential candidates omitted from the latest roster search");
    expect(prompt).toContain('presidential_cycle_id: "cycle-1"');
    expect(prompt).toContain('election_name: "2028 Democratic presidential primary"');
    expect(prompt).toContain('- stage: "primary"');
    expect(prompt).toContain('- party: "Democratic"');
    expect(prompt).toContain('candidate_id: "candidate-1"');
    expect(prompt).toContain('display_name: "Jane President"');
    expect(prompt).toContain('fec_ids: ["P80000001"]');
    expect(prompt).toContain('"status": "active|withdrawn"');
    expect(prompt).not.toContain('"notes"');
    expect(prompt).toContain("Return exactly one result row for each provided candidate_id.");
    expect(prompt).toContain("Do not infer withdrawal just because the candidate was missing from the latest roster list.");
    expect(prompt).not.toContain("Use status=unknown");
    expect(prompt).not.toContain("notes must briefly explain");
  });

  it("supports general-cycle prompt wording and retry feedback", () => {
    const prompt = buildPresidentialRosterStatusPrompt({
      cycleId: "cycle-general",
      electionYear: 2028,
      stage: "general",
      party: null,
      candidates: [
        {
          candidateId: "candidate-1",
          displayName: "Jane President",
          party: "Independent",
          fecIds: [" p80000001 ", "P80000001"],
        },
      ],
      reviewFeedbackLines: ["The prior response omitted candidate-1."],
    });

    expect(prompt).toContain('election_name: "2028 presidential general election"');
    expect(prompt).not.toContain("- party:");
    expect(prompt).toContain('fec_ids: ["P80000001"]');
    expect(prompt).toContain("Previous feedback to fix:");
    expect(prompt).toContain("1. The prior response omitted candidate-1.");
  });

  it("rejects invalid prompt inputs", () => {
    expect(() =>
      buildPresidentialRosterStatusPrompt({
        cycleId: "cycle-1",
        electionYear: 2026,
        stage: "primary",
        party: "Democratic",
        candidates: [
          {
            candidateId: "candidate-1",
            displayName: "Jane President",
            party: "Democratic",
            fecIds: ["P80000001"],
          },
        ],
      })
    ).toThrow("Invalid presidential roster status prompt election year: 2026");

    expect(() =>
      buildPresidentialRosterStatusPrompt({
        cycleId: " ",
        electionYear: 2028,
        stage: "primary",
        party: "Democratic",
        candidates: [
          {
            candidateId: "candidate-1",
            displayName: "Jane President",
            party: "Democratic",
            fecIds: ["P80000001"],
          },
        ],
      })
    ).toThrow("presidential_cycle_id is required");

    expect(() =>
      buildPresidentialRosterStatusPrompt({
        cycleId: "cycle-1",
        electionYear: 2028,
        stage: "primary",
        party: " ",
        candidates: [
          {
            candidateId: "candidate-1",
            displayName: "Jane President",
            party: "Democratic",
            fecIds: ["P80000001"],
          },
        ],
      })
    ).toThrow("primary party is required");

    expect(() =>
      buildPresidentialRosterStatusPrompt({
        cycleId: "cycle-1",
        electionYear: 2028,
        stage: "primary",
        party: "Democratic",
        candidates: [],
      })
    ).toThrow("At least one omitted presidential candidate is required");

    expect(() =>
      buildPresidentialRosterStatusPrompt({
        cycleId: "cycle-1",
        electionYear: 2028,
        stage: "primary",
        party: "Democratic",
        candidates: [
          {
            candidateId: "candidate-1",
            displayName: "Jane President",
            party: "Democratic",
            fecIds: [],
          },
        ],
      })
    ).toThrow("must include at least one FEC ID");
  });
});
