import { describe, expect, it } from "vitest";

import {
  ME_CANDIDATE_FOLLOWS_PATH,
  MAX_USER_RESEARCH_AREA_PREFERENCES,
  ME_ADDRESS_PATH,
  ME_RESEARCH_AREA_PREFERENCES_PATH,
  parseAuthenticatedAddressBodyValue,
  parseCandidateFollowBodyValue,
  parseResearchAreaPreferencesBodyValue,
  RESEARCH_AREAS_PATH,
} from "../../src/api/apiValidation.js";

describe("authenticated address API contract constants", () => {
  it("defines the authenticated address replacement path", () => {
    expect(ME_ADDRESS_PATH).toBe("/api/me/address");
  });

  it("parses authenticated address payloads like normal address lookups", () => {
    expect(parseAuthenticatedAddressBodyValue({ address: "  123 Main St Denver CO 80203  " })).toEqual({
      address: "123 Main St Denver CO 80203",
    });
  });

  it.each([
    [null, "Request body must be a JSON object"],
    [{}, "Request body must include non-empty string field: address"],
    [{ address: "   " }, "Request body must include non-empty string field: address"],
    [{ address: 123 }, "Request body must include non-empty string field: address"],
  ])("rejects invalid authenticated address payload %#", (payload, message) => {
    expect(() => parseAuthenticatedAddressBodyValue(payload)).toThrow(message);
  });
});

describe("candidate follow API contract constants", () => {
  it("defines the authenticated candidate-follow path", () => {
    expect(ME_CANDIDATE_FOLLOWS_PATH).toBe("/api/me/candidate-follows");
  });

  it("parses candidate follow payloads into pipeline inputs", () => {
    expect(
      parseCandidateFollowBodyValue({
        candidate_id: "  22222222-2222-4222-8222-222222222222  ",
        following: true,
        notify_elections: false,
        notify_updates: true,
      })
    ).toEqual({
      candidateId: "22222222-2222-4222-8222-222222222222",
      following: true,
      notifyElections: false,
      notifyUpdates: true,
    });
  });

  it("allows minimal unfollow payloads", () => {
    expect(
      parseCandidateFollowBodyValue({
        candidate_id: "22222222-2222-4222-8222-222222222222",
        following: false,
      })
    ).toEqual({
      candidateId: "22222222-2222-4222-8222-222222222222",
      following: false,
    });
  });

  it.each([
    [null, "Request body must be a JSON object"],
    [{}, "Request body must include UUID string field: candidate_id"],
    [{ candidate_id: 123, following: true }, "Request body must include UUID string field: candidate_id"],
    [{ candidate_id: "not-a-uuid", following: true }, "candidate_id must be a valid UUID: not-a-uuid"],
    [
      { candidate_id: "22222222-2222-4222-8222-222222222222" },
      "Request body must include boolean field: following",
    ],
    [
      { candidate_id: "22222222-2222-4222-8222-222222222222", following: "yes" },
      "Request body must include boolean field: following",
    ],
    [
      {
        candidate_id: "22222222-2222-4222-8222-222222222222",
        following: true,
        notify_elections: "yes",
      },
      "notify_elections must be a boolean",
    ],
    [
      {
        candidate_id: "22222222-2222-4222-8222-222222222222",
        following: true,
        notify_updates: "yes",
      },
      "notify_updates must be a boolean",
    ],
  ])("rejects invalid candidate follow payload %#", (payload, message) => {
    expect(() => parseCandidateFollowBodyValue(payload)).toThrow(message);
  });
});

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
