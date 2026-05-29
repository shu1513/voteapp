import { afterEach, describe, expect, it, vi } from "vitest";

const callResearchProviderMock = vi.fn();

vi.mock("../../src/ai/researchProviderClient.ts", () => ({
  callResearchProvider: callResearchProviderMock,
  trimDebugText: (input: string) => input,
}));

vi.mock("../../src/ai/urlReachability.ts", () => ({
  verifyHttpUrlReachability: vi.fn(async (url: string) => ({
    ok: true,
    finalUrl: url,
  })),
}));

function makeOfficeEntry(input: {
  title: string;
  date: string;
  senate_class?: "class_i" | "class_ii" | "class_iii";
  term_end_year?: string;
  election_stage?: "primary" | "general" | "runoff" | "special";
}) {
  return {
    official_ballot_title: input.title,
    election_date: input.date,
    race_type: "office" as const,
    ...(input.senate_class ? { senate_class: input.senate_class } : {}),
    ...(input.term_end_year ? { term_end_year: input.term_end_year } : {}),
    ...(input.election_stage ? { election_stage: input.election_stage } : {}),
    sources: ["https://example.org/senate"],
  };
}

function buildGenericProviderResult() {
  return {
    ok: true as const,
    parsed: { entries: [] },
    rawText: "{\"entries\":[]}",
    debugMeta: {},
  };
}

afterEach(() => {
  callResearchProviderMock.mockReset();
});

describe("enrichElections U.S. Senate pair validation", () => {
  it("keeps two entries when senate_class differs", async () => {
    callResearchProviderMock.mockImplementation(async (_candidate, prompt: string) => {
      if (prompt.includes("Contest family for this call: us_senate")) {
        return {
          ok: true,
          parsed: {
            entries: [
              makeOfficeEntry({
                title: "United States Senator",
                date: "2026-11-03",
                senate_class: "class_i",
              }),
              makeOfficeEntry({
                title: "United States Senator (Unexpired Term)",
                date: "2026-11-03",
                senate_class: "class_ii",
                election_stage: "special",
              }),
            ],
          },
          rawText: "{}",
          debugMeta: {},
        };
      }
      return buildGenericProviderResult();
    });

    const { enrichElections } = await import("../../src/ai/enrichElections.ts");
    const result = await enrichElections(
      {
        ingestKey: "test:senate:class",
        draft: {
          district_id: "district-1",
          district_name: "California",
          district_type: "statewide",
          state: "CA",
        },
        promptVersion: "elections_v2",
        softRetryCount: 0,
        reviewFeedback: [],
      },
      { timeoutMs: 1000, openAiApiKey: "test-key" },
      [{ provider: "openai", model: "gpt-5.4-mini" }]
    );

    expect(result.ok).toBe(true);
    if (result.ok) {
      const senateEntries = result.payload.entries.filter((entry) =>
        entry.official_ballot_title.toLowerCase().includes("senator")
      );
      expect(senateEntries).toHaveLength(2);
    }
  });

  it("keeps two entries when term_end_year differs", async () => {
    callResearchProviderMock.mockImplementation(async (_candidate, prompt: string) => {
      if (prompt.includes("Contest family for this call: us_senate")) {
        return {
          ok: true,
          parsed: {
            entries: [
              makeOfficeEntry({
                title: "United States Senator",
                date: "2026-11-03",
                term_end_year: "2031",
              }),
              makeOfficeEntry({
                title: "United States Senator (Unexpired Term)",
                date: "2026-11-03",
                term_end_year: "2029",
                election_stage: "special",
              }),
            ],
          },
          rawText: "{}",
          debugMeta: {},
        };
      }
      return buildGenericProviderResult();
    });

    const { enrichElections } = await import("../../src/ai/enrichElections.ts");
    const result = await enrichElections(
      {
        ingestKey: "test:senate:term",
        draft: {
          district_id: "district-1",
          district_name: "California",
          district_type: "statewide",
          state: "CA",
        },
        promptVersion: "elections_v2",
        softRetryCount: 0,
        reviewFeedback: [],
      },
      { timeoutMs: 1000, openAiApiKey: "test-key" },
      [{ provider: "openai", model: "gpt-5.4-mini" }]
    );

    expect(result.ok).toBe(true);
    if (result.ok) {
      const senateEntries = result.payload.entries.filter((entry) =>
        entry.official_ballot_title.toLowerCase().includes("senator")
      );
      expect(senateEntries).toHaveLength(2);
    }
  });

  it("retries when two seats return identical senate_class", async () => {
    let senateAttempt = 0;
    callResearchProviderMock.mockImplementation(async (_candidate, prompt: string) => {
      if (prompt.includes("Contest family for this call: us_senate")) {
        senateAttempt += 1;
        if (senateAttempt === 1) {
          return {
            ok: true,
            parsed: {
              entries: [
                makeOfficeEntry({
                  title: "United States Senator",
                  date: "2026-11-03",
                  senate_class: "class_ii",
                }),
                makeOfficeEntry({
                  title: "United States Senator (Unexpired Term)",
                  date: "2026-11-03",
                  senate_class: "class_ii",
                  election_stage: "special",
                }),
              ],
            },
            rawText: "{}",
            debugMeta: {},
          };
        }
        return {
          ok: true,
          parsed: {
            entries: [
              makeOfficeEntry({
                title: "United States Senator",
                date: "2026-11-03",
                senate_class: "class_ii",
              }),
              makeOfficeEntry({
                title: "United States Senator (Unexpired Term)",
                date: "2026-11-03",
                senate_class: "class_iii",
                election_stage: "special",
              }),
            ],
          },
          rawText: "{}",
          debugMeta: {},
        };
      }
      return buildGenericProviderResult();
    });

    const { enrichElections } = await import("../../src/ai/enrichElections.ts");
    const result = await enrichElections(
      {
        ingestKey: "test:senate:retry-class",
        draft: {
          district_id: "district-1",
          district_name: "California",
          district_type: "statewide",
          state: "CA",
        },
        promptVersion: "elections_v2",
        softRetryCount: 0,
        reviewFeedback: [],
      },
      { timeoutMs: 1000, openAiApiKey: "test-key" },
      [{ provider: "openai", model: "gpt-5.4-mini" }]
    );

    expect(result.ok).toBe(true);
    expect(senateAttempt).toBe(2);
    const senatePrompts = callResearchProviderMock.mock.calls
      .map((call) => call[1] as string)
      .filter((prompt) => prompt.includes("Contest family for this call: us_senate"));
    expect(senatePrompts.length).toBe(2);
    expect(senatePrompts[1]).toContain("identical senate_class");
  });

  it("retries when two seats normalize to the same title key even if class differs", async () => {
    let senateAttempt = 0;
    callResearchProviderMock.mockImplementation(async (_candidate, prompt: string) => {
      if (prompt.includes("Contest family for this call: us_senate")) {
        senateAttempt += 1;
        if (senateAttempt === 1) {
          return {
            ok: true,
            parsed: {
              entries: [
                makeOfficeEntry({
                  title: "United States Senator",
                  date: "2026-11-03",
                  senate_class: "class_i",
                }),
                makeOfficeEntry({
                  title: "United States Senator",
                  date: "2026-11-03",
                  senate_class: "class_iii",
                }),
              ],
            },
            rawText: "{}",
            debugMeta: {},
          };
        }
        return {
          ok: true,
          parsed: {
            entries: [
              makeOfficeEntry({
                title: "United States Senator",
                date: "2026-11-03",
                senate_class: "class_i",
              }),
              makeOfficeEntry({
                title: "United States Senator (Unexpired Term)",
                date: "2026-11-03",
                senate_class: "class_iii",
                election_stage: "special",
              }),
            ],
          },
          rawText: "{}",
          debugMeta: {},
        };
      }
      return buildGenericProviderResult();
    });

    const { enrichElections } = await import("../../src/ai/enrichElections.ts");
    const result = await enrichElections(
      {
        ingestKey: "test:senate:retry-title-key",
        draft: {
          district_id: "district-1",
          district_name: "California",
          district_type: "statewide",
          state: "CA",
        },
        promptVersion: "elections_v2",
        softRetryCount: 0,
        reviewFeedback: [],
      },
      { timeoutMs: 1000, openAiApiKey: "test-key" },
      [{ provider: "openai", model: "gpt-5.4-mini" }]
    );

    expect(result.ok).toBe(true);
    expect(senateAttempt).toBe(2);
    const senatePrompts = callResearchProviderMock.mock.calls
      .map((call) => call[1] as string)
      .filter((prompt) => prompt.includes("Contest family for this call: us_senate"));
    expect(senatePrompts.length).toBe(2);
    expect(senatePrompts[1]).toContain("normalize to the same official_ballot_title key");
  });

  it("collapses to one when unresolved after retry", async () => {
    callResearchProviderMock.mockImplementation(async (_candidate, prompt: string) => {
      if (prompt.includes("Contest family for this call: us_senate")) {
        return {
          ok: true,
          parsed: {
            entries: [
              makeOfficeEntry({
                title: "United States Senator",
                date: "2026-11-03",
              }),
              makeOfficeEntry({
                title: "United States Senator",
                date: "2026-11-03",
              }),
            ],
          },
          rawText: "{}",
          debugMeta: {},
        };
      }
      return buildGenericProviderResult();
    });

    const { enrichElections } = await import("../../src/ai/enrichElections.ts");
    const result = await enrichElections(
      {
        ingestKey: "test:senate:collapse",
        draft: {
          district_id: "district-1",
          district_name: "California",
          district_type: "statewide",
          state: "CA",
        },
        promptVersion: "elections_v2",
        softRetryCount: 0,
        reviewFeedback: [],
      },
      { timeoutMs: 1000, openAiApiKey: "test-key" },
      [{ provider: "openai", model: "gpt-5.4-mini" }]
    );

    expect(result.ok).toBe(true);
    if (result.ok) {
      const senateEntries = result.payload.entries.filter((entry) =>
        entry.official_ballot_title.toLowerCase().includes("senator")
      );
      expect(senateEntries).toHaveLength(1);
      const familyDebug = ((result.aiRawDebug ?? {}) as { family_debug?: Record<string, unknown> }).family_debug;
      const usSenateDebug = familyDebug?.us_senate as Record<string, unknown> | undefined;
      expect(usSenateDebug?.us_senate_resolution_notes).toBeTruthy();
    }
  });
});
