import { describe, expect, it } from "vitest";

import { buildPresidentialRosterPrompt } from "../../src/ai/providers/presidentialRosterPrompt.js";

describe("buildPresidentialRosterPrompt", () => {
  it("builds a party-specific primary roster prompt", () => {
    const prompt = buildPresidentialRosterPrompt({
      cycleId: "11111111-1111-4111-8111-111111111111",
      electionYear: 2028,
      stage: "primary",
      party: "Democratic",
    });

    expect(prompt).toContain("Return strict JSON only.");
    expect(prompt).toContain('presidential_cycle_id: "11111111-1111-4111-8111-111111111111"');
    expect(prompt).toContain('election_name: "2028 Democratic presidential primary"');
    expect(prompt).toContain("- election_year: 2028");
    expect(prompt).toContain('- stage: "primary"');
    expect(prompt).toContain('- party: "Democratic"');
    expect(prompt).toContain('"display_name": "candidate name; ballot-listed name if available"');
    expect(prompt).toContain('"fec_candidate_id": "FEC presidential candidate ID if known, otherwise omit"');
    expect(prompt).toContain('"status": "active|withdrawn"');
    expect(prompt).toContain('"running_mate": {');
    expect(prompt).toContain('"display_name": "officially announced running mate name"');
    expect(prompt).toContain("Include running_mate only if the candidate has officially announced a running mate");
    expect(prompt).toContain("omit running_mate if none is officially announced");
    expect(prompt).toContain("Do not include speculative, rumored, shortlist, possible, or expected running mates.");
    expect(prompt).not.toContain("campaign_website_url");
    expect(prompt).toContain("Return only candidates formally running for the 2028 Democratic presidential nomination.");
    expect(prompt).not.toContain(
      "Do not return independent candidates, third-party candidates, or general-election-only candidates."
    );
    expect(prompt).not.toContain("Every candidate.party must be");
    expect(prompt).not.toContain("possible-contender names");
    expect(prompt).toContain(
      "Only return people who have formally declared, filed with the FEC, or launched an official campaign. If no candidates meet this standard, return candidates: []."
    );
  });

  it("trims party and includes retry feedback", () => {
    const prompt = buildPresidentialRosterPrompt({
      cycleId: "cycle-1",
      electionYear: 2028,
      stage: "primary",
      party: " Republican ",
      reviewFeedbackLines: ["The prior response included an independent candidate."],
    });

    expect(prompt).toContain('election_name: "2028 Republican presidential primary"');
    expect(prompt).toContain('- party: "Republican"');
    expect(prompt).toContain("Previous feedback to fix:");
    expect(prompt).toContain("1. The prior response included an independent candidate.");
  });

  it("supports general-cycle prompt wording without a party", () => {
    const prompt = buildPresidentialRosterPrompt({
      cycleId: "cycle-general",
      electionYear: 2028,
      stage: "general",
      party: null,
    });

    expect(prompt).toContain('election_name: "2028 presidential general election"');
    expect(prompt).not.toContain("- party:");
    expect(prompt).toContain("Return only candidates formally running in the presidential general election.");
    expect(prompt).toContain("Include major-party nominees and ballot-qualified third-party or independent candidates");
  });

  it("rejects invalid cycle context", () => {
    expect(() =>
      buildPresidentialRosterPrompt({
        cycleId: "cycle-1",
        electionYear: 2026,
        stage: "primary",
        party: "Democratic",
      })
    ).toThrow("Invalid presidential roster prompt election year: 2026");

    expect(() =>
      buildPresidentialRosterPrompt({
        cycleId: " ",
        electionYear: 2028,
        stage: "primary",
        party: "Democratic",
      })
    ).toThrow("cycle_id is required");

    expect(() =>
      buildPresidentialRosterPrompt({
        cycleId: "cycle-1",
        electionYear: 2028,
        stage: "primary",
        party: " ",
      })
    ).toThrow("primary party is required");

    expect(() =>
      buildPresidentialRosterPrompt({
        cycleId: "cycle-1",
        electionYear: 2028,
        stage: "general",
        party: "Democratic",
      })
    ).toThrow("general party must be null");
  });
});
