import { beforeEach, describe, expect, it, vi } from "vitest";

const { callResearchProviderMock, verifyHttpUrlReachabilityMock } = vi.hoisted(() => ({
  callResearchProviderMock: vi.fn(),
  verifyHttpUrlReachabilityMock: vi.fn(),
}));

vi.mock("../../src/ai/researchProviderClient.js", () => ({
  callResearchProvider: callResearchProviderMock,
  trimDebugText: (text: string) => text,
}));

vi.mock("../../src/ai/urlReachability.js", () => ({
  verifyHttpUrlReachability: verifyHttpUrlReachabilityMock,
}));

import { disambiguateCandidateDuplicateGroup } from "../../src/ai/enrichCandidateRoster.js";

const baseInput = {
  districtName: "Los Angeles County, California",
  districtType: "county",
  state: "CA",
  electionDate: "2026-06-02",
  officialBallotTitle: "Assessor",
  electionIsPartisan: true,
  duplicateDisplayName: "John Smith",
  options: [
    {
      roster_index: 0,
      display_name: "John Smith",
      party: "Democrat",
      sources: ["https://example.org/a"],
    },
    {
      roster_index: 1,
      display_name: "John Smith",
      party: "Democrat",
      sources: ["https://example.org/b"],
    },
  ],
  seedUrls: [] as const,
};

const baseConfig = {
  timeoutMs: 90_000,
};

describe("disambiguateCandidateDuplicateGroup validation", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    verifyHttpUrlReachabilityMock.mockResolvedValue({ ok: true });
  });

  it("rejects same_as_other without same_as_roster_index", async () => {
    callResearchProviderMock.mockResolvedValue({
      ok: true,
      parsed: {
        people: [
          { roster_index: 0, status: "clear", disambiguation_hint: "incumbent", sources: ["https://example.org/a"] },
          { roster_index: 1, status: "same_as_other", sources: ["https://example.org/b"] },
        ],
      },
      rawText: "missing-target",
    });

    const result = await disambiguateCandidateDuplicateGroup(baseInput, baseConfig, [
      { provider: "openai", model: "gpt-test" },
    ]);

    expect(result.ok).toBe(false);
    if (result.ok) return;
    expect(result.reason).toContain("same_as_roster_index");
  });

  it("rejects same_as_other pointing to ambiguous row", async () => {
    callResearchProviderMock.mockResolvedValue({
      ok: true,
      parsed: {
        people: [
          { roster_index: 0, status: "ambiguous", sources: ["https://example.org/a"] },
          { roster_index: 1, status: "same_as_other", same_as_roster_index: 0, sources: ["https://example.org/b"] },
        ],
      },
      rawText: "bad-target",
    });

    const result = await disambiguateCandidateDuplicateGroup(baseInput, baseConfig, [
      { provider: "openai", model: "gpt-test" },
    ]);

    expect(result.ok).toBe(false);
    if (result.ok) return;
    expect(result.reason).toContain("must point to row with status=clear");
  });

  it("accepts one clear and one same_as_other merge", async () => {
    callResearchProviderMock.mockResolvedValueOnce({
      ok: true,
      parsed: {
        people: [
          { roster_index: 0, status: "clear", disambiguation_hint: "incumbent", sources: ["https://example.org/a"] },
          { roster_index: 1, status: "same_as_other", same_as_roster_index: 0, sources: ["https://example.org/b"] },
        ],
      },
      rawText: "good-merge",
    });

    const result = await disambiguateCandidateDuplicateGroup(baseInput, baseConfig, [
      { provider: "openai", model: "gpt-test" },
    ]);

    expect(result.ok).toBe(true);
    if (!result.ok) return;
    expect(result.people).toEqual([
      {
        roster_index: 0,
        status: "clear",
        disambiguation_hint: "incumbent",
        sources: ["https://example.org/a"],
      },
      {
        roster_index: 1,
        status: "same_as_other",
        same_as_roster_index: 0,
        sources: ["https://example.org/b"],
      },
    ]);
  });

  it("requires fec_ids for federal duplicate disambiguation mode", async () => {
    callResearchProviderMock.mockResolvedValue({
      ok: true,
      parsed: {
        people: [
          {
            roster_index: 0,
            status: "clear",
            disambiguation_hint: "listed on FEC filing",
            sources: ["https://example.org/a"],
          },
          {
            roster_index: 1,
            status: "same_as_other",
            same_as_roster_index: 0,
            sources: ["https://example.org/b"],
          },
        ],
      },
      rawText: "missing-fec",
    });

    const result = await disambiguateCandidateDuplicateGroup(
      {
        ...baseInput,
        districtType: "us_house",
        officialBallotTitle: "United States Representative, District 12",
      },
      baseConfig,
      [{ provider: "openai", model: "gpt-test" }]
    );

    expect(result.ok).toBe(false);
    if (result.ok) return;
    expect(result.reason).toContain("fec_ids");
  });

  it("accepts federal duplicate disambiguation response when fec_ids are present", async () => {
    callResearchProviderMock.mockResolvedValue({
      ok: true,
      parsed: {
        people: [
          {
            roster_index: 0,
            status: "clear",
            disambiguation_hint: "listed on FEC filing",
            fec_ids: ["H0CA12000"],
            sources: ["https://example.org/a"],
          },
          {
            roster_index: 1,
            status: "same_as_other",
            same_as_roster_index: 0,
            fec_ids: ["H0CA12000"],
            sources: ["https://example.org/b"],
          },
        ],
      },
      rawText: "with-fec",
    });

    const result = await disambiguateCandidateDuplicateGroup(
      {
        ...baseInput,
        districtType: "us_house",
        officialBallotTitle: "United States Representative, District 12",
      },
      baseConfig,
      [{ provider: "openai", model: "gpt-test" }]
    );

    expect(result.ok).toBe(true);
    if (!result.ok) return;
    expect(result.people[0]?.fec_ids).toEqual(["H0CA12000"]);
  });
});
