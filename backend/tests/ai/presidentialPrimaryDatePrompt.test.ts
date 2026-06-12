import { describe, expect, it } from "vitest";

import { buildPresidentialPrimaryDatePrompt } from "../../src/ai/providers/presidentialPrimaryDatePrompt.js";

describe("buildPresidentialPrimaryDatePrompt", () => {
  it("includes cycle context, deterministic state names, output shape, and official-source rules", () => {
    const prompt = buildPresidentialPrimaryDatePrompt({
      cycleId: "11111111-1111-4111-8111-111111111111",
      electionName: "2028 Democratic presidential primary",
      electionYear: 2028,
      party: "Democratic",
      stateFipsList: ["06", "11"],
      scheduledFor: "2027-03-07T00:00:00.000Z",
    });

    expect(prompt).toContain("Return strict JSON only.");
    expect(prompt).toContain('presidential_cycle_id: "11111111-1111-4111-8111-111111111111"');
    expect(prompt).toContain('election_name: "2028 Democratic presidential primary"');
    expect(prompt).toContain("- election_year: 2028");
    expect(prompt).toContain('- party: "Democratic"');
    expect(prompt).toContain('state_fips: "06"; state_name: "California"');
    expect(prompt).toContain('state_fips: "11"; state_name: "District of Columbia"');
    expect(prompt).toContain('"status": "official_found|not_official_yet"');
    expect(prompt).toContain('"state_fips": "copy exactly from the provided state_fips"');
    expect(prompt).toContain('"primary_date": "YYYY-MM-DD when officially set, otherwise null"');
    expect(prompt).toContain("Use official sources for official_found: state election office");
    expect(prompt).toContain("News articles, blogs, Wikipedia, and unofficial calendars are not sufficient");
    expect(prompt).toContain("Do not infer a date from prior cycles");
    expect(prompt).toContain("Return exactly one result row for each provided state_fips");
  });

  it("dedupes state fips and trims party values", () => {
    const prompt = buildPresidentialPrimaryDatePrompt({
      cycleId: "11111111-1111-4111-8111-111111111111",
      electionName: "2028 Republican presidential primary",
      electionYear: 2028,
      party: " Republican ",
      stateFipsList: ["06", "06"],
      scheduledFor: "2027-03-07T00:00:00.000Z",
    });

    expect(prompt).toContain('- party: "Republican"');
    expect(prompt.match(/state_fips: "06"/g)).toHaveLength(1);
  });

  it("includes retry feedback when provided", () => {
    const prompt = buildPresidentialPrimaryDatePrompt({
      cycleId: "11111111-1111-4111-8111-111111111111",
      electionName: "2028 Democratic presidential primary",
      electionYear: 2028,
      party: "Democratic",
      stateFipsList: ["06"],
      scheduledFor: "2027-03-07T00:00:00.000Z",
      reviewFeedbackLines: ["The prior source URL was unreachable."],
    });

    expect(prompt).toContain("Previous feedback to fix:");
    expect(prompt).toContain("1. The prior source URL was unreachable.");
  });

  it("rejects invalid years, states, party, and oversized batches", () => {
    expect(() =>
      buildPresidentialPrimaryDatePrompt({
        cycleId: "11111111-1111-4111-8111-111111111111",
        electionName: "2028 Democratic presidential primary",
        electionYear: 2026,
        party: "Democratic",
        stateFipsList: ["06"],
        scheduledFor: "2027-03-07T00:00:00.000Z",
      })
    ).toThrow("Invalid presidential primary date prompt election year: 2026");

    expect(() =>
      buildPresidentialPrimaryDatePrompt({
        cycleId: "11111111-1111-4111-8111-111111111111",
        electionName: "2028 Democratic presidential primary",
        electionYear: 2028,
        party: " ",
        stateFipsList: ["06"],
        scheduledFor: "2027-03-07T00:00:00.000Z",
      })
    ).toThrow("party is required");

    expect(() =>
      buildPresidentialPrimaryDatePrompt({
        cycleId: "11111111-1111-4111-8111-111111111111",
        electionName: "2028 Democratic presidential primary",
        electionYear: 2028,
        party: "Democratic",
        stateFipsList: ["99"],
        scheduledFor: "2027-03-07T00:00:00.000Z",
      })
    ).toThrow("Unknown state FIPS code: 99");

    expect(() =>
      buildPresidentialPrimaryDatePrompt({
        cycleId: "11111111-1111-4111-8111-111111111111",
        electionName: "2028 Democratic presidential primary",
        electionYear: 2028,
        party: "Democratic",
        stateFipsList: [
          "01",
          "02",
          "04",
          "05",
          "06",
          "08",
          "09",
          "10",
          "11",
          "12",
          "13",
        ],
        scheduledFor: "2027-03-07T00:00:00.000Z",
      })
    ).toThrow("at most 10 states");
  });
});
