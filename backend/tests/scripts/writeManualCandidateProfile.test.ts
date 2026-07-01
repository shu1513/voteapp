import { describe, expect, it } from "vitest";

import type { CandidateProfilePayload } from "../../src/contracts/candidateProfilePayloadContract.js";
import {
  applyRegularElectionProfileContext,
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

describe("applyRegularElectionProfileContext", () => {
  it("injects roster FEC IDs and strips fields the regular federal profile path strips", () => {
    const result = applyRegularElectionProfileContext({
      profile: profile({
        party: "Republican",
        date_of_birth: "1970-01-01",
        state_filing_ids: ["AK-state-id"],
      }),
      researchMode: "federal_us_senate",
      rosterHints: {
        rosterIndex: 0,
        displayName: "Jane Candidate",
        fecIds: ["S6AK00001"],
        stateFilingIds: ["AK-state-id"],
      },
    });

    expect(result.fec_ids).toEqual(["S6AK00001"]);
    expect(result.party).toBeUndefined();
    expect(result.date_of_birth).toBeUndefined();
    expect(result.state_filing_ids).toBeUndefined();
  });

  it("requires roster FEC IDs for federal profiles", () => {
    expect(() =>
      applyRegularElectionProfileContext({
        profile: profile(),
        researchMode: "federal_us_senate",
        rosterHints: null,
      })
    ).toThrow("candidate_fec_ids is required in roster context for federal profile import");
  });

  it("injects roster state filing IDs for state-level profiles", () => {
    const result = applyRegularElectionProfileContext({
      profile: profile({ party: "Independent" }),
      researchMode: "state_level",
      rosterHints: {
        rosterIndex: 0,
        displayName: "Jane Candidate",
        fecIds: [],
        stateFilingIds: ["AK-2026-1"],
      },
    });

    expect(result.state_filing_ids).toEqual(["AK-2026-1"]);
    expect(result.party).toBeUndefined();
  });
});
