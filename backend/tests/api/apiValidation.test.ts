import { describe, expect, it } from "vitest";

import {
  MAX_USER_RESEARCH_AREA_PREFERENCES,
  ME_RESEARCH_AREA_PREFERENCES_PATH,
  parseResearchAreaPreferencesBodyValue,
  RESEARCH_AREAS_PATH,
} from "../../src/api/apiValidation.js";

describe("research area API contract constants", () => {
  it("defines the public research-area catalog and authenticated preference paths", () => {
    expect(RESEARCH_AREAS_PATH).toBe("/api/research-areas");
    expect(ME_RESEARCH_AREA_PREFERENCES_PATH).toBe("/api/me/research-area-preferences");
  });

  it("exposes the shared maximum user preference count", () => {
    expect(MAX_USER_RESEARCH_AREA_PREFERENCES).toBe(7);
  });

  it("parses research area preference payloads into pipeline inputs", () => {
    expect(
      parseResearchAreaPreferencesBodyValue({
        preferences: [
          { research_area_id: "22222222-2222-4222-8222-222222222222", rank: 1 },
          { research_area_id: "33333333-3333-4333-8333-333333333333", rank: null },
        ],
      })
    ).toEqual({
      preferences: [
        { researchAreaId: "22222222-2222-4222-8222-222222222222", rank: 1 },
        { researchAreaId: "33333333-3333-4333-8333-333333333333", rank: null },
      ],
    });
  });

  it("allows clearing all research area preferences", () => {
    expect(parseResearchAreaPreferencesBodyValue({ preferences: [] })).toEqual({ preferences: [] });
  });

  it.each([
    [{}, "Request body must include array field: preferences"],
    [{ preferences: ["bad"] }, "preferences must contain only JSON objects"],
    [
      { preferences: [{ research_area_id: "not-a-uuid" }] },
      "preferences contains invalid research_area_id: not-a-uuid",
    ],
    [
      {
        preferences: [
          { research_area_id: "22222222-2222-4222-8222-222222222222", rank: 1 },
          { research_area_id: "22222222-2222-4222-8222-222222222222", rank: 2 },
        ],
      },
      "preferences contains duplicate research_area_id: 22222222-2222-4222-8222-222222222222",
    ],
    [
      { preferences: [{ research_area_id: "22222222-2222-4222-8222-222222222222", rank: 8 }] },
      "preferences[].rank must be an integer from 1 to 7",
    ],
    [
      {
        preferences: Array.from({ length: MAX_USER_RESEARCH_AREA_PREFERENCES + 1 }, (_value, index) => ({
          research_area_id: `22222222-2222-4222-8222-${(index + 1).toString().padStart(12, "0")}`,
        })),
      },
      "preferences supports at most 7 research areas",
    ],
    [
      {
        preferences: [
          { research_area_id: "22222222-2222-4222-8222-222222222222", rank: 1 },
          { research_area_id: "33333333-3333-4333-8333-333333333333", rank: 1 },
        ],
      },
      "preferences contains duplicate rank: 1",
    ],
  ])("rejects invalid research area preference payload %#", (payload, message) => {
    expect(() => parseResearchAreaPreferencesBodyValue(payload)).toThrow(message);
  });
});
