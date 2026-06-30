import { describe, expect, it } from "vitest";

import type { CandidateProfilePayload } from "../../src/contracts/candidateProfilePayloadContract.js";
import {
  applyConfirmedGaps,
  buildCandidateProfileQualityGaps,
} from "../../src/scripts/writeManualCandidateProfile.js";

function profile(overrides: Partial<CandidateProfilePayload> = {}): CandidateProfilePayload {
  return {
    display_name: "Jane Candidate",
    first_name: "Jane",
    last_name: "Candidate",
    official_website_url: "https://jane.example",
    summary: "A source-backed profile summary.",
    sources: ["https://jane.example/about"],
    ...overrides,
  };
}

describe("writeManualCandidateProfile quality gaps", () => {
  it("does not report a current-office gap when current_office is present", () => {
    const gaps = buildCandidateProfileQualityGaps({
      profile: profile({ current_office: "Governor" }),
      includeParty: false,
    });

    expect(gaps.some((gap) => gap.id === "candidate_profile.current_office")).toBe(false);
  });

  it("reports missing current_office as a focused repair gap", () => {
    const gaps = buildCandidateProfileQualityGaps({
      profile: profile(),
      includeParty: false,
    });

    expect(gaps).toContainEqual(
      expect.objectContaining({
        id: "candidate_profile.current_office",
        outcome: "needs_repair",
        field: "current_office",
        reason: "Candidate current office is missing.",
      })
    );
  });

  it("lets confirmed-gap mark missing current_office as confirmed_null", () => {
    const gaps = buildCandidateProfileQualityGaps({
      profile: profile(),
      includeParty: false,
    });

    const confirmed = applyConfirmedGaps(gaps, new Set(["candidate_profile.current_office"]));

    expect(confirmed).toContainEqual(
      expect.objectContaining({
        id: "candidate_profile.current_office",
        outcome: "confirmed_null",
      })
    );
  });
});
