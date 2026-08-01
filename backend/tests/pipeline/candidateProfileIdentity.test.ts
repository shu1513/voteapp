import { describe, expect, it, vi } from "vitest";

import {
  AmbiguousCandidateIdentityError,
  assertMergedOfficeRoutingConsistent,
  findOrCreateCandidateFromProfile,
  mergeIdentifierLists,
  mergeProfileSourceLists,
  resolveStoredCandidateParty,
} from "../../src/pipeline/candidates/candidateProfileIdentity.js";
import type { CandidateProfilePayload } from "../../src/contracts/candidateProfilePayloadContract.js";

function profile(overrides: Partial<CandidateProfilePayload> = {}): CandidateProfilePayload {
  return {
    display_name: "Jane Candidate",
    first_name: "Jane",
    last_name: "Candidate",
    summary: "Candidate summary",
    has_held_public_office: false,
    sources: ["https://example.com/profile"],
    ...overrides,
  };
}

// findOrCreateCandidateFromProfile's first query is always the per-person
// advisory lock; pre-load its response so tests only chain the identity reads.
function identityQueryMock() {
  return vi.fn().mockResolvedValueOnce({ rows: [] });
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

describe("mergeProfileSourceLists", () => {
  it("keeps stored sources first and appends new distinct incoming sources", () => {
    expect(
      mergeProfileSourceLists(
        ["https://stored.example/a", "https://both.example/page"],
        ["https://both.example/page", "https://incoming.example/b"]
      )
    ).toEqual(["https://stored.example/a", "https://both.example/page", "https://incoming.example/b"]);
  });

  it("dedupes on the normalized URL, not the raw string", () => {
    // Stored legacy rows predate contract-side normalizeHttpUrl: a trailing
    // slash or hash variant is the same source, and the wave-18 duplicate
    // incident came exactly from re-deriving instead of normalizing.
    expect(
      mergeProfileSourceLists(
        ["https://a.example/page/"],
        ["https://a.example/page", "https://a.example/page#bio"]
      )
    ).toEqual(["https://a.example/page/"]);
  });

  it("keeps URLs that differ only in path or query case — they may be distinct documents", () => {
    // Paths and queries are case-sensitive per RFC 3986; collapsing them
    // would silently discard a source. Scheme and hostname ARE
    // case-insensitive, and URL parsing inside normalizeHttpUrl already
    // lowercases both, so host-case variants still dedupe.
    expect(
      mergeProfileSourceLists(
        ["https://a.example/Bio", "https://a.example/results?Name=Alice"],
        ["https://a.example/bio", "https://a.example/results?name=alice"]
      )
    ).toEqual([
      "https://a.example/Bio",
      "https://a.example/results?Name=Alice",
      "https://a.example/bio",
      "https://a.example/results?name=alice",
    ]);

    expect(
      mergeProfileSourceLists(["HTTPS://EXAMPLE.com/bio"], ["https://example.com/bio"])
    ).toEqual(["HTTPS://EXAMPLE.com/bio"]);
  });

  it("ignores blanks and keeps a non-URL string on its raw form", () => {
    expect(mergeProfileSourceLists(["  ", "not a url"], ["NOT A URL"])).toEqual(["not a url"]);
  });
});

describe("resolveStoredCandidateParty", () => {
  it("uses the roster party for a partisan contest and Nonpartisan otherwise", () => {
    expect(
      resolveStoredCandidateParty({
        includeParty: true,
        rosterParty: "Republican",
        profileParty: "Democratic",
      })
    ).toBe("Republican");
    expect(
      resolveStoredCandidateParty({
        includeParty: false,
        rosterParty: undefined,
        profileParty: undefined,
      })
    ).toBe("Nonpartisan");
  });
});

describe("findOrCreateCandidateFromProfile", () => {
  it("refuses to discard a supplied party in a nonpartisan contest", async () => {
    const query = identityQueryMock();

    await expect(
      findOrCreateCandidateFromProfile({
        client: { query } as never,
        profile: profile({ party: "Republican" }),
        state: "WA",
        rosterParty: undefined,
        includeParty: false,
      })
    ).rejects.toThrow(/would discard candidate party/i);
    expect(query).not.toHaveBeenCalled();
  });

  it("inserts a new candidate when no same-name hard-identifier match exists", async () => {
    const query = identityQueryMock()
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
    expect(query).toHaveBeenCalledTimes(3);
    expect(query.mock.calls[2]?.[1]).toContain("Jane Candidate");
    expect(query.mock.calls[2]?.[1]).toContain("Democratic");
    expect(query.mock.calls[2]?.[1]).toContain("US");
  });

  it("writes current office when inserting a new candidate", async () => {
    const query = identityQueryMock()
      .mockResolvedValueOnce({ rows: [] })
      .mockResolvedValueOnce({ rows: [{ id: "candidate-new" }], rowCount: 1 });

    await findOrCreateCandidateFromProfile({
      client: { query } as never,
      profile: profile({ fec_ids: ["P80000001"], current_office: "Governor" }),
      state: "US",
      rosterParty: "Democratic",
      includeParty: true,
    });

    const insertSql = String(query.mock.calls[2]?.[0]);
    expect(insertSql).toContain("current_office");
    expect(insertSql).toContain("profile_sources");
    expect(query.mock.calls[2]?.[1]).toContain(JSON.stringify(["https://example.com/profile"]));
    expect(query.mock.calls[2]?.[1]).toContain("Governor");
  });

  it("reuses an existing same-name candidate when hard identifiers match", async () => {
    const query = identityQueryMock()
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
            current_office: null,
            state: "US",
          },
        ],
      })
      .mockResolvedValueOnce({ rows: [{ fec_ids: ["P80000001"], state_filing_ids: null, current_office: null }] })
      .mockResolvedValueOnce({ rowCount: 1 });

    const result = await findOrCreateCandidateFromProfile({
      client: { query } as never,
      profile: profile({ fec_ids: ["P80000001"] }),
      state: "US",
      rosterParty: "Democratic",
      includeParty: true,
    });

    expect(result).toEqual({ candidateId: "candidate-existing", matchedExisting: true });
    expect(query).toHaveBeenCalledTimes(4);
    expect(query.mock.calls.some((call) => String(call[0]).includes("INSERT INTO public.candidates"))).toBe(false);
    expect(String(query.mock.calls[3]?.[0])).toContain("last_researched = now()");
    expect(String(query.mock.calls[3]?.[0])).toContain("profile_sources = $4::jsonb");
  });

  it("fills a blank current office for an existing hard-identifier match", async () => {
    const query = identityQueryMock()
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
            current_office: null,
            state: "US",
          },
        ],
      })
      .mockResolvedValueOnce({ rows: [{ fec_ids: ["P80000001"], state_filing_ids: null, current_office: "  " }] })
      .mockResolvedValueOnce({ rowCount: 1 });

    const result = await findOrCreateCandidateFromProfile({
      client: { query } as never,
      // has_held_public_office must accompany a current office (contract
      // invariant; the merge guard enforces it too).
      profile: profile({ fec_ids: ["P80000001"], current_office: "Governor", has_held_public_office: true }),
      state: "US",
      rosterParty: "Democratic",
      includeParty: true,
    });

    expect(result).toEqual({ candidateId: "candidate-existing", matchedExisting: true });
    expect(query).toHaveBeenCalledTimes(4);
    expect(String(query.mock.calls[3]?.[0])).toContain("current_office = CASE");
    expect(query.mock.calls[3]?.[1]).toEqual([
      "candidate-existing",
      JSON.stringify(["P80000001"]),
      null,
      JSON.stringify(["https://example.com/profile"]),
      null,
      false,
      null,
      false,
      null,
      false,
      null,
      false,
      "Candidate summary",
      false,
      "Governor",
      false,
      // clear flags: no field listed for clearing
      false,
      false,
      false,
      false,
      false,
      false,
      // has_held_public_office value + overwrite flag
      true,
      false,
      // effective party + overwrite flag
      "Democratic",
      false,
    ]);
  });

  it("does not overwrite a non-blank current office for an existing hard-identifier match", async () => {
    const query = identityQueryMock()
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
            current_office: "Mayor",
            state: "US",
          },
        ],
      })
      .mockResolvedValueOnce({ rows: [{ fec_ids: ["P80000001"], state_filing_ids: null, current_office: "Mayor" }] })
      .mockResolvedValueOnce({ rowCount: 1 });

    const result = await findOrCreateCandidateFromProfile({
      client: { query } as never,
      profile: profile({ fec_ids: ["P80000001"], current_office: "Governor", has_held_public_office: true }),
      state: "US",
      rosterParty: "Democratic",
      includeParty: true,
    });

    expect(result).toEqual({ candidateId: "candidate-existing", matchedExisting: true });
    expect(query).toHaveBeenCalledTimes(4);
    expect(String(query.mock.calls[3]?.[0])).toContain("current_office = CASE");
    expect(String(query.mock.calls[3]?.[0])).toContain("last_researched = now()");
    expect(String(query.mock.calls[3]?.[0])).toContain("profile_sources = $4::jsonb");
    expect(query.mock.calls[3]?.[1]).toEqual([
      "candidate-existing",
      JSON.stringify(["P80000001"]),
      null,
      JSON.stringify(["https://example.com/profile"]),
      null,
      false,
      null,
      false,
      null,
      false,
      null,
      false,
      "Candidate summary",
      false,
      "Governor",
      false,
      // clear flags: no field listed for clearing
      false,
      false,
      false,
      false,
      false,
      false,
      // has_held_public_office value + overwrite flag
      true,
      false,
      // effective party + overwrite flag
      "Democratic",
      false,
    ]);
  });

  it("can reuse a same-name candidate from another state when explicitly allowed and hard identifiers match", async () => {
    const query = identityQueryMock()
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
            current_office: null,
            state: "CA",
          },
        ],
      })
      .mockResolvedValueOnce({ rows: [{ fec_ids: ["P80000001"], state_filing_ids: null, current_office: null }] })
      .mockResolvedValueOnce({ rowCount: 1 });

    const result = await findOrCreateCandidateFromProfile({
      client: { query } as never,
      profile: profile({ fec_ids: ["P80000001"] }),
      state: "US",
      rosterParty: "Democratic",
      includeParty: true,
      allowCrossStateHardIdentifierMatch: true,
    });

    expect(result).toEqual({ candidateId: "candidate-home-state", matchedExisting: true });
    expect(String(query.mock.calls[1]?.[0])).not.toContain("AND state = $3");
    expect(query.mock.calls[1]?.[1]).toEqual(["Jane", "Candidate"]);
    expect(query.mock.calls.some((call) => String(call[0]).includes("INSERT INTO public.candidates"))).toBe(false);
  });

  it("keeps default candidate matching scoped to the requested state", async () => {
    const query = identityQueryMock()
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
    expect(String(query.mock.calls[1]?.[0])).toContain("AND state = $3");
    expect(query.mock.calls[1]?.[1]).toEqual(["Jane", "Candidate", "US"]);
    expect(query.mock.calls.some((call) => String(call[0]).includes("INSERT INTO public.candidates"))).toBe(true);
  });
});


describe("findOrCreateCandidateFromProfile field persistence and election-scoped matching", () => {
  const existingRow = {
    id: "candidate-existing",
    first_name: "Jane",
    last_name: "Candidate",
    date_of_birth: null,
    twitter_handle: null,
    linkedin_url: null,
    official_website_url: "https://old-site.example",
    fec_ids: null,
    state_filing_ids: null,
    current_office: null,
    state: "OH",
  };

  it("fills empty scalar columns on a matched re-write (fill-if-empty, never clobber)", async () => {
    const query = identityQueryMock()
      // loadSameNameCandidates
      .mockResolvedValueOnce({ rows: [{ ...existingRow, linkedin_url: null }] })
      // merge lock SELECT
      .mockResolvedValueOnce({ rows: [{ fec_ids: null, state_filing_ids: null }] })
      // merge UPDATE
      .mockResolvedValueOnce({ rowCount: 1 });

    const result = await findOrCreateCandidateFromProfile({
      client: { query } as never,
      profile: profile({
        official_website_url: "https://old-site.example",
        linkedin_url: "https://www.linkedin.com/in/jane",
      }),
      state: "OH",
      rosterParty: "Democratic",
      includeParty: true,
    });

    expect(result.matchedExisting).toBe(true);
    const updateSql = String(query.mock.calls[3]?.[0]);
    expect(updateSql).toContain("linkedin_url = CASE");
    expect(updateSql).toContain("official_website_url = CASE");
    expect(updateSql).toContain("summary = CASE");
    const params = query.mock.calls[3]?.[1] as unknown[];
    // linkedin value present, overwrite false: fills only because column is empty
    expect(params).toContain("https://www.linkedin.com/in/jane");
  });

  it("matches a candidate already linked to the election when the sole hard identifier changed", async () => {
    const query = identityQueryMock()
      // loadSameNameCandidates: same-name row exists but hard identifiers no longer match
      .mockResolvedValueOnce({ rows: [existingRow] })
      // election-scoped display_name lookup
      .mockResolvedValueOnce({ rows: [{ id: "candidate-existing" }] })
      // merge lock SELECT
      .mockResolvedValueOnce({ rows: [{ fec_ids: null, state_filing_ids: null }] })
      // merge UPDATE
      .mockResolvedValueOnce({ rowCount: 1 });

    const result = await findOrCreateCandidateFromProfile({
      client: { query } as never,
      profile: profile({ official_website_url: "https://brand-new-site.example" }),
      state: "OH",
      rosterParty: "Democratic",
      includeParty: true,
      matchByLinkedElectionId: "election-1",
    });

    expect(result).toEqual({ candidateId: "candidate-existing", matchedExisting: true });
    const electionLookupSql = String(query.mock.calls[2]?.[0]);
    expect(electionLookupSql).toContain("candidate_elections");
    expect(electionLookupSql).toContain("running_mate_candidate_id");
  });

  it("replaces a non-empty stored value only for fields listed in overwriteProfileFields", async () => {
    const query = identityQueryMock()
      .mockResolvedValueOnce({ rows: [{ ...existingRow, official_website_url: "https://old-site.example" }] })
      .mockResolvedValueOnce({ rows: [{ fec_ids: null, state_filing_ids: null }] })
      .mockResolvedValueOnce({ rowCount: 1 });

    await findOrCreateCandidateFromProfile({
      client: { query } as never,
      profile: profile({ official_website_url: "https://old-site.example", summary: "Corrected summary" }),
      state: "OH",
      rosterParty: "Democratic",
      includeParty: true,
      overwriteProfileFields: new Set(["summary"]),
    });

    const params = query.mock.calls[3]?.[1] as unknown[];
    // summary value + its overwrite flag true; website overwrite flag stays false
    const summaryIndex = params.indexOf("Corrected summary");
    expect(summaryIndex).toBeGreaterThan(-1);
    expect(params[summaryIndex + 1]).toBe(true);
    const websiteIndex = params.indexOf("https://old-site.example");
    expect(params[websiteIndex + 1]).toBe(false);
  });

  it("unions stored profile_sources with the payload's on a matched re-write", async () => {
    // The merge UPDATE keeps stored facts (fill-if-empty scalars, additive
    // id lists), so a narrow correction payload must not wipe the sources
    // supporting them — six wave-19/20 corrections had to hand-build
    // supersets because the old write replaced the list unconditionally.
    const query = identityQueryMock()
      .mockResolvedValueOnce({ rows: [existingRow] })
      .mockResolvedValueOnce({
        rows: [{
          fec_ids: null,
          state_filing_ids: null,
          profile_sources: ["https://stored.example/bio", "https://example.com/profile/"],
        }],
      })
      .mockResolvedValueOnce({ rowCount: 1 });

    await findOrCreateCandidateFromProfile({
      client: { query } as never,
      profile: profile({ official_website_url: "https://old-site.example" }),
      state: "OH",
      rosterParty: "Democratic",
      includeParty: true,
    });

    const params = query.mock.calls[3]?.[1] as unknown[];
    // $4 = profile_sources; the payload's "https://example.com/profile" is
    // the stored trailing-slash variant, deduped on the normalized URL.
    expect(params[3]).toBe(
      JSON.stringify(["https://stored.example/bio", "https://example.com/profile/"])
    );
  });

  it("replaces profile_sources with exactly the payload's list when explicitly listed", async () => {
    const query = identityQueryMock()
      .mockResolvedValueOnce({ rows: [existingRow] })
      .mockResolvedValueOnce({
        rows: [{
          fec_ids: null,
          state_filing_ids: null,
          profile_sources: ["https://dead-host.example/bio"],
        }],
      })
      .mockResolvedValueOnce({ rowCount: 1 });

    await findOrCreateCandidateFromProfile({
      client: { query } as never,
      profile: profile({ official_website_url: "https://old-site.example" }),
      state: "OH",
      rosterParty: "Democratic",
      includeParty: true,
      overwriteProfileFields: new Set(["profile_sources"]),
    });

    const params = query.mock.calls[3]?.[1] as unknown[];
    expect(params[3]).toBe(JSON.stringify(["https://example.com/profile"]));
  });

  it("replaces a stored party only when party is explicitly listed", async () => {
    const query = identityQueryMock()
      .mockResolvedValueOnce({ rows: [{ ...existingRow, party: "Nonpartisan" }] })
      .mockResolvedValueOnce({
        rows: [{
          fec_ids: null,
          state_filing_ids: null,
          current_office: null,
          has_held_public_office: false,
          party: "Nonpartisan",
        }],
      })
      .mockResolvedValueOnce({ rowCount: 1 });

    await findOrCreateCandidateFromProfile({
      client: { query } as never,
      profile: profile({ official_website_url: "https://old-site.example" }),
      state: "OH",
      rosterParty: "Republican",
      includeParty: true,
      overwriteProfileFields: new Set(["party"]),
    });

    const updateSql = String(query.mock.calls[3]?.[0]);
    const params = query.mock.calls[3]?.[1] as unknown[];
    expect(updateSql).toContain("party = CASE");
    expect(updateSql).toContain("WHEN party IS NOT NULL AND length(trim(party)) = 0 THEN $25::text");
    expect(updateSql).not.toContain("WHEN party IS NULL OR");
    expect(params.slice(-2)).toEqual(["Republican", true]);
  });

  it("never overwrites a stored value with a blank string, even for overwrite-listed fields", async () => {
    const query = identityQueryMock()
      .mockResolvedValueOnce({ rows: [{ ...existingRow, official_website_url: "https://old-site.example" }] })
      .mockResolvedValueOnce({ rows: [{ fec_ids: null, state_filing_ids: null }] })
      .mockResolvedValueOnce({ rowCount: 1 });

    await findOrCreateCandidateFromProfile({
      client: { query } as never,
      // Contract-parsed profiles cannot carry blank strings, but the merge is
      // exported and must stay safe for non-contract callers.
      profile: { ...profile({ official_website_url: "https://old-site.example" }), summary: "  " },
      state: "OH",
      rosterParty: "Democratic",
      includeParty: true,
      overwriteProfileFields: new Set(["summary"]),
    });

    const updateSql = String(query.mock.calls[3]?.[0]);
    // The overwrite branch requires a non-blank incoming value.
    expect(updateSql).toContain("length(trim($13::text)) > 0");
  });

  it("clears listed fields to NULL on a matched re-write, winning over fill-if-empty", async () => {
    const query = identityQueryMock()
      .mockResolvedValueOnce({ rows: [{ ...existingRow, current_office: "Attorney, Noble Law" }] })
      .mockResolvedValueOnce({ rows: [{ fec_ids: null, state_filing_ids: null }] })
      .mockResolvedValueOnce({ rowCount: 1 });

    const result = await findOrCreateCandidateFromProfile({
      client: { query } as never,
      // Payload deliberately carries no current_office: the wrapper refuses
      // clear+supply for the same field, so the cleared field arrives absent.
      profile: profile({ official_website_url: "https://old-site.example" }),
      state: "OH",
      rosterParty: "Democratic",
      includeParty: true,
      clearProfileFields: new Set(["current_office"]),
    });

    expect(result.matchedExisting).toBe(true);
    const updateSql = String(query.mock.calls[3]?.[0]);
    // The clear branch leads every scalar CASE so it wins over overwrite and
    // fill-if-empty alike.
    expect(updateSql).toContain("current_office = CASE\n            WHEN $22::boolean THEN NULL");
    const params = query.mock.calls[3]?.[1] as unknown[];
    // The six per-field clear flags (declaration order, only current_office
    // set) are followed by the has_held_public_office value + overwrite flag.
    expect(params.slice(16, 24)).toEqual([false, false, false, false, false, true, false, false]);
  });

  it("passes clear flags as false for every field when clearing is not requested", async () => {
    const query = identityQueryMock()
      .mockResolvedValueOnce({ rows: [existingRow] })
      .mockResolvedValueOnce({ rows: [{ fec_ids: null, state_filing_ids: null }] })
      .mockResolvedValueOnce({ rowCount: 1 });

    await findOrCreateCandidateFromProfile({
      client: { query } as never,
      profile: profile({ official_website_url: "https://old-site.example" }),
      state: "OH",
      rosterParty: "Democratic",
      includeParty: true,
    });

    const params = query.mock.calls[3]?.[1] as unknown[];
    expect(params.slice(16, 24)).toEqual([false, false, false, false, false, false, false, false]);
  });

  it("throws when two same-name candidates are linked to the election", async () => {
    const query = identityQueryMock()
      .mockResolvedValueOnce({ rows: [existingRow] })
      .mockResolvedValueOnce({ rows: [{ id: "candidate-a" }, { id: "candidate-b" }] });

    await expect(
      findOrCreateCandidateFromProfile({
        client: { query } as never,
        profile: profile({ official_website_url: "https://brand-new-site.example" }),
        state: "OH",
        rosterParty: "Democratic",
        includeParty: true,
        matchByLinkedElectionId: "election-1",
      })
    ).rejects.toThrow(/Multiple candidates named/);
  });
});

describe("assertMergedOfficeRoutingConsistent", () => {
  it("refuses filling has_held_public_office=false against a stored current office", () => {
    expect(() =>
      assertMergedOfficeRoutingConsistent({
        profile: profile(),
        storedCurrentOffice: "Mayor",
        storedHasHeldPublicOffice: null,
      })
    ).toThrow(/contradictory candidate row/);
  });

  it("refuses filling a blank office while a stored false routing answer survives", () => {
    expect(() =>
      assertMergedOfficeRoutingConsistent({
        profile: profile({ current_office: "Mayor", has_held_public_office: true }),
        storedCurrentOffice: null,
        storedHasHeldPublicOffice: false,
      })
    ).toThrow(/--replace-profile-fields has_held_public_office/);
  });

  it("accepts the merge when the office is being cleared or replaced away", () => {
    expect(() =>
      assertMergedOfficeRoutingConsistent({
        profile: profile(),
        storedCurrentOffice: "Attorney, Noble Law",
        storedHasHeldPublicOffice: null,
        clearFields: new Set(["current_office"]),
      })
    ).not.toThrow();
  });

  it("accepts an overwrite that corrects the stale false answer", () => {
    expect(() =>
      assertMergedOfficeRoutingConsistent({
        profile: profile({ current_office: "Mayor", has_held_public_office: true }),
        storedCurrentOffice: null,
        storedHasHeldPublicOffice: false,
        overwriteFields: new Set(["has_held_public_office"]),
      })
    ).not.toThrow();
  });

  it("accepts consistent states: officeholder with office, never-held without one", () => {
    expect(() =>
      assertMergedOfficeRoutingConsistent({
        profile: profile({ current_office: "Governor", has_held_public_office: true }),
        storedCurrentOffice: "Governor",
        storedHasHeldPublicOffice: true,
      })
    ).not.toThrow();
    expect(() =>
      assertMergedOfficeRoutingConsistent({
        profile: profile(),
        storedCurrentOffice: null,
        storedHasHeldPublicOffice: null,
      })
    ).not.toThrow();
  });
});

describe("findOrCreateCandidateFromProfile identity hardening", () => {
  const duplicateRows = ["candidate-a", "candidate-b"].map((id) => ({
    id,
    first_name: "Jane",
    last_name: "Candidate",
    date_of_birth: null,
    twitter_handle: null,
    linkedin_url: null,
    official_website_url: null,
    fec_ids: ["P80000001"],
    state_filing_ids: null,
    current_office: null,
    state: "US",
  }));

  it("throws AmbiguousCandidateIdentityError instead of inserting when several rows match hard identifiers", async () => {
    const query = identityQueryMock().mockResolvedValueOnce({ rows: duplicateRows });

    await expect(
      findOrCreateCandidateFromProfile({
        client: { query } as never,
        profile: profile({ fec_ids: ["P80000001"] }),
        state: "US",
        rosterParty: "Democratic",
        includeParty: true,
      })
    ).rejects.toBeInstanceOf(AmbiguousCandidateIdentityError);

    expect(query.mock.calls.some((call) => String(call[0]).includes("INSERT INTO public.candidates"))).toBe(false);
  });

  it("takes the per-person advisory lock before reading candidate identity", async () => {
    const query = identityQueryMock()
      .mockResolvedValueOnce({ rows: [] })
      .mockResolvedValueOnce({ rows: [{ id: "candidate-new" }], rowCount: 1 });

    await findOrCreateCandidateFromProfile({
      client: { query } as never,
      profile: profile({ fec_ids: ["P80000001"] }),
      state: "US",
      rosterParty: "Democratic",
      includeParty: true,
    });

    const lockSql = String(query.mock.calls[0]?.[0]);
    expect(lockSql).toContain("pg_advisory_xact_lock");
    expect(query.mock.calls[0]?.[1]).toEqual(["jane candidate"]);
    expect(String(query.mock.calls[1]?.[0])).toContain("FROM public.candidates");
  });
});
