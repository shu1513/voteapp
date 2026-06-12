import { describe, expect, it, vi } from "vitest";

import {
  findOrCreateCandidateFromProfile,
  mergeIdentifierLists,
} from "../../src/pipeline/candidates/candidateProfileIdentity.js";
import type { CandidateProfilePayload } from "../../src/contracts/candidateProfilePayloadContract.js";

function profile(overrides: Partial<CandidateProfilePayload> = {}): CandidateProfilePayload {
  return {
    display_name: "Jane Candidate",
    first_name: "Jane",
    last_name: "Candidate",
    summary: "Candidate summary",
    sources: ["https://example.com/profile"],
    ...overrides,
  };
}

describe("mergeIdentifierLists", () => {
  it("preserves existing ids and appends new distinct incoming ids", () => {
    const merged = mergeIdentifierLists(
      ["S123", "S234"],
      ["S234", "S345"]
    );

    expect(merged).toEqual(["S123", "S234", "S345"]);
  });

  it("dedupes case-insensitively and ignores blanks", () => {
    const merged = mergeIdentifierLists(
      ["  abc-1  "],
      ["ABC-1", "   ", "abc-2"]
    );

    expect(merged).toEqual(["abc-1", "abc-2"]);
  });

  it("returns undefined when both inputs are empty/missing", () => {
    expect(mergeIdentifierLists(undefined, undefined)).toBeUndefined();
    expect(mergeIdentifierLists([], ["   "])).toBeUndefined();
  });
});

describe("findOrCreateCandidateFromProfile", () => {
  it("inserts a new candidate when no same-name hard-identifier match exists", async () => {
    const query = vi
      .fn()
      .mockResolvedValueOnce({ rows: [] })
      .mockResolvedValueOnce({ rows: [{ id: "candidate-new" }], rowCount: 1 });

    const result = await findOrCreateCandidateFromProfile({
      client: { query } as never,
      profile: profile({ fec_ids: ["P80000001"] }),
      state: "US",
      rosterParty: "Democratic",
      includeParty: true,
    });

    expect(result).toEqual({ candidateId: "candidate-new", matchedExisting: false });
    expect(query).toHaveBeenCalledTimes(2);
    expect(query.mock.calls[1]?.[1]).toContain("Jane Candidate");
    expect(query.mock.calls[1]?.[1]).toContain("Democratic");
    expect(query.mock.calls[1]?.[1]).toContain("US");
  });

  it("reuses an existing same-name candidate when hard identifiers match", async () => {
    const query = vi
      .fn()
      .mockResolvedValueOnce({
        rows: [
          {
            id: "candidate-existing",
            first_name: "Jane",
            last_name: "Candidate",
            date_of_birth: null,
            twitter_handle: null,
            linkedin_url: null,
            official_website_url: null,
            fec_ids: ["P80000001"],
            state_filing_ids: null,
            state: "US",
          },
        ],
      })
      .mockResolvedValueOnce({ rows: [{ fec_ids: ["P80000001"], state_filing_ids: null }] });

    const result = await findOrCreateCandidateFromProfile({
      client: { query } as never,
      profile: profile({ fec_ids: ["P80000001"] }),
      state: "US",
      rosterParty: "Democratic",
      includeParty: true,
    });

    expect(result).toEqual({ candidateId: "candidate-existing", matchedExisting: true });
    expect(query).toHaveBeenCalledTimes(2);
    expect(query.mock.calls.some((call) => String(call[0]).includes("INSERT INTO public.candidates"))).toBe(false);
  });

  it("can reuse a same-name candidate from another state when explicitly allowed and hard identifiers match", async () => {
    const query = vi
      .fn()
      .mockResolvedValueOnce({
        rows: [
          {
            id: "candidate-home-state",
            first_name: "Jane",
            last_name: "Candidate",
            date_of_birth: null,
            twitter_handle: null,
            linkedin_url: null,
            official_website_url: null,
            fec_ids: ["P80000001"],
            state_filing_ids: null,
            state: "CA",
          },
        ],
      })
      .mockResolvedValueOnce({ rows: [{ fec_ids: ["P80000001"], state_filing_ids: null }] });

    const result = await findOrCreateCandidateFromProfile({
      client: { query } as never,
      profile: profile({ fec_ids: ["P80000001"] }),
      state: "US",
      rosterParty: "Democratic",
      includeParty: true,
      allowCrossStateHardIdentifierMatch: true,
    });

    expect(result).toEqual({ candidateId: "candidate-home-state", matchedExisting: true });
    expect(String(query.mock.calls[0]?.[0])).not.toContain("AND state = $3");
    expect(query.mock.calls[0]?.[1]).toEqual(["Jane", "Candidate"]);
    expect(query.mock.calls.some((call) => String(call[0]).includes("INSERT INTO public.candidates"))).toBe(false);
  });

  it("keeps default candidate matching scoped to the requested state", async () => {
    const query = vi
      .fn()
      .mockResolvedValueOnce({ rows: [] })
      .mockResolvedValueOnce({ rows: [{ id: "candidate-new-us" }], rowCount: 1 });

    const result = await findOrCreateCandidateFromProfile({
      client: { query } as never,
      profile: profile({ fec_ids: ["P80000001"] }),
      state: "US",
      rosterParty: "Democratic",
      includeParty: true,
    });

    expect(result).toEqual({ candidateId: "candidate-new-us", matchedExisting: false });
    expect(String(query.mock.calls[0]?.[0])).toContain("AND state = $3");
    expect(query.mock.calls[0]?.[1]).toEqual(["Jane", "Candidate", "US"]);
    expect(query.mock.calls.some((call) => String(call[0]).includes("INSERT INTO public.candidates"))).toBe(true);
  });
});
