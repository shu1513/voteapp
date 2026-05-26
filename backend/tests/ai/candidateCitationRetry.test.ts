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

import { enrichCandidateRoster } from "../../src/ai/enrichCandidateRoster.js";
import { enrichCandidateProfile } from "../../src/ai/enrichCandidateProfile.js";

describe("candidate citation verification retry behavior", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("retries roster once on same model with blocked URL feedback", async () => {
    callResearchProviderMock
      .mockResolvedValueOnce({
        ok: true,
        parsed: {
          candidates: [
            {
              display_name: "Jane Doe",
              sources: ["https://bad.example/404"],
            },
          ],
        },
        rawText: "first",
      })
      .mockResolvedValueOnce({
        ok: true,
        parsed: {
          candidates: [
            {
              display_name: "Jane Doe",
              sources: ["https://good.example/profile"],
            },
          ],
        },
        rawText: "second",
      });

    verifyHttpUrlReachabilityMock.mockImplementation(async (url: string) => {
      if (url.includes("bad.example")) {
        return { ok: false, reason: "citation fetch returned status 404" };
      }
      return { ok: true };
    });

    const result = await enrichCandidateRoster(
      {
        districtName: "Los Angeles County, California",
        districtType: "county",
        state: "CA",
        electionDate: "2026-06-02",
        officialBallotTitle: "Assessor",
        seedUrls: [],
      },
      {
        timeoutMs: 90000,
      },
      [{ provider: "openai", model: "gpt-test" }]
    );

    expect(result.ok).toBe(true);
    expect(callResearchProviderMock).toHaveBeenCalledTimes(2);

    const secondPrompt = callResearchProviderMock.mock.calls[1]?.[1];
    expect(secondPrompt).toContain(
      'Do not use or cite this URL for "Jane Doe": https://bad.example/404 (citation fetch returned status 404)'
    );
  });

  it("carries blocked URL feedback from one profile model to the next", async () => {
    callResearchProviderMock
      .mockResolvedValueOnce({
        ok: true,
        parsed: {
          display_name: "John Smith",
          first_name: "John",
          last_name: "Smith",
          sources: ["https://bad.example/404"],
        },
        rawText: "first-model-attempt-1",
      })
      .mockResolvedValueOnce({
        ok: true,
        parsed: {
          display_name: "John Smith",
          first_name: "John",
          last_name: "Smith",
          sources: ["https://bad.example/404"],
        },
        rawText: "first-model-attempt-2",
      })
      .mockResolvedValueOnce({
        ok: true,
        parsed: {
          display_name: "John Smith",
          first_name: "John",
          last_name: "Smith",
          sources: ["https://good.example/profile"],
        },
        rawText: "second-model-attempt-1",
      });

    verifyHttpUrlReachabilityMock.mockImplementation(async (url: string) => {
      if (url.includes("bad.example")) {
        return { ok: false, reason: "citation fetch returned status 404" };
      }
      return { ok: true };
    });

    const result = await enrichCandidateProfile(
      {
        candidateDisplayName: "John Smith",
        districtName: "Los Angeles County, California",
        districtType: "county",
        state: "CA",
        electionDate: "2026-06-02",
        officialBallotTitle: "Assessor",
        seedUrls: [],
      },
      {
        timeoutMs: 90000,
      },
      [
        { provider: "openai", model: "gpt-model-a" },
        { provider: "openai", model: "gpt-model-b" },
      ]
    );

    expect(result.ok).toBe(true);
    expect(callResearchProviderMock).toHaveBeenCalledTimes(3);

    const thirdPrompt = callResearchProviderMock.mock.calls[2]?.[1];
    expect(thirdPrompt).toContain(
      'Do not use or cite this URL for "John Smith": https://bad.example/404 (citation fetch returned status 404)'
    );
  });

  it("omits party in nonpartisan school roster prompts and strips returned party", async () => {
    callResearchProviderMock.mockResolvedValueOnce({
      ok: true,
      parsed: {
        candidates: [
          {
            display_name: "Jane Doe",
            party: "Independent",
            is_incumbent: false,
            sources: ["https://good.example/profile"],
          },
        ],
      },
      rawText: "school-roster",
    });
    verifyHttpUrlReachabilityMock.mockResolvedValue({ ok: true });

    const result = await enrichCandidateRoster(
      {
        districtName: "Baldwin Park Unified School District, California",
        districtType: "school_unified",
        state: "CA",
        electionDate: "2026-11-03",
        officialBallotTitle: "Governing Board Member",
        seedUrls: [],
      },
      {
        timeoutMs: 90000,
      },
      [{ provider: "openai", model: "gpt-test" }]
    );

    expect(result.ok).toBe(true);
    if (!result.ok) {
      return;
    }
    expect(result.candidates).toEqual([
      {
        display_name: "Jane Doe",
        is_incumbent: false,
        sources: ["https://good.example/profile"],
      },
    ]);

    const prompt = callResearchProviderMock.mock.calls[0]?.[1];
    expect(prompt).not.toContain('"party": "party label when clearly known (optional)"');
  });

  it("omits party in judicial roster prompts", async () => {
    callResearchProviderMock.mockResolvedValueOnce({
      ok: true,
      parsed: {
        candidates: [
          {
            display_name: "Alex Kim",
            party: "Nonpartisan",
            sources: ["https://good.example/judge"],
          },
        ],
      },
      rawText: "judicial-roster",
    });
    verifyHttpUrlReachabilityMock.mockResolvedValue({ ok: true });

    const result = await enrichCandidateRoster(
      {
        districtName: "Los Angeles County, California",
        districtType: "county",
        state: "CA",
        electionDate: "2026-06-02",
        officialBallotTitle: "Judge of the Superior Court, Office No. 2",
        seedUrls: [],
      },
      {
        timeoutMs: 90000,
      },
      [{ provider: "openai", model: "gpt-test" }]
    );

    expect(result.ok).toBe(true);
    if (!result.ok) {
      return;
    }
    expect(result.candidates[0]).toEqual({
      display_name: "Alex Kim",
      sources: ["https://good.example/judge"],
    });

    const prompt = callResearchProviderMock.mock.calls[0]?.[1];
    expect(prompt).not.toContain('"party": "party label when clearly known (optional)"');
  });

  it("omits party in nonpartisan profile prompts and strips returned party", async () => {
    callResearchProviderMock.mockResolvedValueOnce({
      ok: true,
      parsed: {
        display_name: "Pat Lee",
        first_name: "Pat",
        last_name: "Lee",
        party: "Independent",
        sources: ["https://good.example/profile"],
      },
      rawText: "profile-nonpartisan",
    });
    verifyHttpUrlReachabilityMock.mockResolvedValue({ ok: true });

    const result = await enrichCandidateProfile(
      {
        candidateDisplayName: "Pat Lee",
        districtName: "Baldwin Park Unified School District, California",
        districtType: "school_unified",
        state: "CA",
        electionDate: "2026-11-03",
        officialBallotTitle: "Governing Board Member",
        rosterParty: "Independent",
        seedUrls: [],
      },
      {
        timeoutMs: 90000,
      },
      [{ provider: "openai", model: "gpt-test" }]
    );

    expect(result.ok).toBe(true);
    if (!result.ok) {
      return;
    }
    expect(result.profile).toEqual({
      display_name: "Pat Lee",
      first_name: "Pat",
      last_name: "Lee",
      sources: ["https://good.example/profile"],
    });

    const prompt = callResearchProviderMock.mock.calls[0]?.[1];
    expect(prompt).not.toContain('"party": "party label (optional)"');
    expect(prompt).not.toContain("- party:");
  });

  it("treats 'Court of Appeal' (singular) contests as nonpartisan", async () => {
    callResearchProviderMock.mockResolvedValueOnce({
      ok: true,
      parsed: {
        candidates: [
          {
            display_name: "Taylor Kim",
            party: "Democrat",
            sources: ["https://good.example/court-of-appeal"],
          },
        ],
      },
      rawText: "court-of-appeal-roster",
    });
    verifyHttpUrlReachabilityMock.mockResolvedValue({ ok: true });

    const result = await enrichCandidateRoster(
      {
        districtName: "California",
        districtType: "statewide",
        state: "CA",
        electionDate: "2026-11-03",
        officialBallotTitle: "Court of Appeal, Second Appellate District",
        seedUrls: [],
      },
      {
        timeoutMs: 90000,
      },
      [{ provider: "openai", model: "gpt-test" }]
    );

    expect(result.ok).toBe(true);
    if (!result.ok) {
      return;
    }
    expect(result.candidates[0]).toEqual({
      display_name: "Taylor Kim",
      sources: ["https://good.example/court-of-appeal"],
    });

    const prompt = callResearchProviderMock.mock.calls[0]?.[1];
    expect(prompt).not.toContain('"party": "party label when clearly known (optional)"');
  });

  it("keeps party for judicial contests in partisan-judicial states", async () => {
    callResearchProviderMock.mockResolvedValueOnce({
      ok: true,
      parsed: {
        candidates: [
          {
            display_name: "Jordan Smith",
            party: "Democrat",
            sources: ["https://good.example/tx-judge"],
          },
        ],
      },
      rawText: "tx-judicial-roster",
    });
    verifyHttpUrlReachabilityMock.mockResolvedValue({ ok: true });

    const result = await enrichCandidateRoster(
      {
        districtName: "Texas",
        districtType: "statewide",
        state: "TX",
        electionDate: "2026-11-03",
        officialBallotTitle: "Justice, Supreme Court, Place 3",
        seedUrls: [],
      },
      { timeoutMs: 90000 },
      [{ provider: "openai", model: "gpt-test" }]
    );

    expect(result.ok).toBe(true);
    if (!result.ok) {
      return;
    }
    expect(result.candidates[0]).toEqual({
      display_name: "Jordan Smith",
      party: "Democrat",
      sources: ["https://good.example/tx-judge"],
    });
    const prompt = callResearchProviderMock.mock.calls[0]?.[1];
    expect(prompt).toContain('"party": "party label when clearly known (optional)"');
  });

  it("keeps party for profile enrichment in partisan-judicial states", async () => {
    callResearchProviderMock.mockResolvedValueOnce({
      ok: true,
      parsed: {
        display_name: "Jordan Smith",
        first_name: "Jordan",
        last_name: "Smith",
        party: "Democrat",
        sources: ["https://good.example/tx-profile"],
      },
      rawText: "tx-judicial-profile",
    });
    verifyHttpUrlReachabilityMock.mockResolvedValue({ ok: true });

    const result = await enrichCandidateProfile(
      {
        candidateDisplayName: "Jordan Smith",
        districtName: "Texas",
        districtType: "statewide",
        state: "TX",
        electionDate: "2026-11-03",
        officialBallotTitle: "Justice, Supreme Court, Place 3",
        rosterParty: "Democrat",
        seedUrls: [],
      },
      { timeoutMs: 90000 },
      [{ provider: "openai", model: "gpt-test" }]
    );

    expect(result.ok).toBe(true);
    if (!result.ok) {
      return;
    }
    expect(result.profile.party).toBeUndefined();
    const prompt = callResearchProviderMock.mock.calls[0]?.[1];
    expect(prompt).not.toContain('"party": "party label (optional)"');
    expect(prompt).not.toContain('- party: "Democrat"');
  });

  it("keeps party for school contests in states with partisan or mixed school ballots", async () => {
    callResearchProviderMock.mockResolvedValueOnce({
      ok: true,
      parsed: {
        candidates: [
          {
            display_name: "Jordan Smith",
            party: "Independent",
            sources: ["https://good.example/pa-school"],
          },
        ],
      },
      rawText: "pa-school-roster",
    });
    verifyHttpUrlReachabilityMock.mockResolvedValue({ ok: true });

    const result = await enrichCandidateRoster(
      {
        districtName: "Pennsylvania",
        districtType: "school_unified",
        state: "PA",
        electionDate: "2026-11-03",
        officialBallotTitle: "School Board Director",
        seedUrls: [],
      },
      { timeoutMs: 90000 },
      [{ provider: "openai", model: "gpt-test" }]
    );

    expect(result.ok).toBe(true);
    if (!result.ok) {
      return;
    }
    expect(result.candidates[0]).toEqual({
      display_name: "Jordan Smith",
      party: "Independent",
      sources: ["https://good.example/pa-school"],
    });

    const prompt = callResearchProviderMock.mock.calls[0]?.[1];
    expect(prompt).toContain('"party": "party label when clearly known (optional)"');
  });

  it("keeps party for school profile enrichment in states with partisan or mixed school ballots", async () => {
    callResearchProviderMock.mockResolvedValueOnce({
      ok: true,
      parsed: {
        display_name: "Jordan Smith",
        first_name: "Jordan",
        last_name: "Smith",
        party: "Independent",
        sources: ["https://good.example/pa-school-profile"],
      },
      rawText: "pa-school-profile",
    });
    verifyHttpUrlReachabilityMock.mockResolvedValue({ ok: true });

    const result = await enrichCandidateProfile(
      {
        candidateDisplayName: "Jordan Smith",
        districtName: "Pennsylvania",
        districtType: "school_unified",
        state: "PA",
        electionDate: "2026-11-03",
        officialBallotTitle: "School Board Director",
        rosterParty: "Independent",
        seedUrls: [],
      },
      { timeoutMs: 90000 },
      [{ provider: "openai", model: "gpt-test" }]
    );

    expect(result.ok).toBe(true);
    if (!result.ok) {
      return;
    }
    expect(result.profile.party).toBeUndefined();
    const prompt = callResearchProviderMock.mock.calls[0]?.[1];
    expect(prompt).not.toContain('"party": "party label (optional)"');
    expect(prompt).not.toContain('- party: "Independent"');
  });

  it("uses electionIsPartisan=false to omit party in roster prompts", async () => {
    callResearchProviderMock.mockResolvedValueOnce({
      ok: true,
      parsed: {
        candidates: [
          {
            display_name: "Morgan Price",
            party: "Democrat",
            sources: ["https://good.example/morgan"],
          },
        ],
      },
      rawText: "roster-election-is-partisan-false",
    });
    verifyHttpUrlReachabilityMock.mockResolvedValue({ ok: true });

    const result = await enrichCandidateRoster(
      {
        districtName: "Los Angeles County, California",
        districtType: "county",
        state: "CA",
        electionDate: "2026-11-03",
        officialBallotTitle: "Assessor",
        electionIsPartisan: false,
        seedUrls: [],
      },
      { timeoutMs: 90000 },
      [{ provider: "openai", model: "gpt-test" }]
    );

    expect(result.ok).toBe(true);
    if (!result.ok) {
      return;
    }
    expect(result.candidates[0]).toEqual({
      display_name: "Morgan Price",
      sources: ["https://good.example/morgan"],
    });
    const prompt = callResearchProviderMock.mock.calls[0]?.[1];
    expect(prompt).not.toContain('"party": "party label when clearly known (optional)"');
  });

  it("uses electionIsPartisan=false to omit party in profile prompts", async () => {
    callResearchProviderMock.mockResolvedValueOnce({
      ok: true,
      parsed: {
        display_name: "Morgan Price",
        first_name: "Morgan",
        last_name: "Price",
        party: "Democrat",
        sources: ["https://good.example/morgan-profile"],
      },
      rawText: "profile-election-is-partisan-false",
    });
    verifyHttpUrlReachabilityMock.mockResolvedValue({ ok: true });

    const result = await enrichCandidateProfile(
      {
        candidateDisplayName: "Morgan Price",
        districtName: "Los Angeles County, California",
        districtType: "county",
        state: "CA",
        electionDate: "2026-11-03",
        officialBallotTitle: "Assessor",
        electionIsPartisan: false,
        rosterParty: "Democrat",
        seedUrls: [],
      },
      { timeoutMs: 90000 },
      [{ provider: "openai", model: "gpt-test" }]
    );

    expect(result.ok).toBe(true);
    if (!result.ok) {
      return;
    }
    expect(result.profile).toEqual({
      display_name: "Morgan Price",
      first_name: "Morgan",
      last_name: "Price",
      sources: ["https://good.example/morgan-profile"],
    });
    const prompt = callResearchProviderMock.mock.calls[0]?.[1];
    expect(prompt).not.toContain('"party": "party label (optional)"');
    expect(prompt).not.toContain("- party:");
  });

  it("uses backend candidate_fec_ids and stores null date_of_birth for federal profiles", async () => {
    callResearchProviderMock.mockResolvedValueOnce({
      ok: true,
      parsed: {
        display_name: "Casey Rivera",
        first_name: "Casey",
        last_name: "Rivera",
        date_of_birth: "1980-01-01",
        sources: ["https://good.example/casey-profile"],
      },
      rawText: "federal-profile-no-fec-in-output",
    });
    verifyHttpUrlReachabilityMock.mockResolvedValue({ ok: true });

    const result = await enrichCandidateProfile(
      {
        candidateDisplayName: "Casey Rivera",
        districtName: "California Congressional District 12",
        districtType: "us_house",
        state: "CA",
        electionDate: "2026-11-03",
        officialBallotTitle: "United States Representative, District 12",
        rosterFecIds: ["H0CA12000"],
        seedUrls: [],
      },
      { timeoutMs: 90000 },
      [{ provider: "openai", model: "gpt-test" }]
    );

    expect(result.ok).toBe(true);
    if (!result.ok) {
      return;
    }
    expect(result.profile.fec_ids).toEqual(["H0CA12000"]);
    expect(result.profile.date_of_birth).toBeUndefined();
    const prompt = callResearchProviderMock.mock.calls[0]?.[1];
    expect(prompt).toContain('- candidate_fec_ids: ["H0CA12000"]');
    expect(prompt).toContain("do not include date_of_birth; backend stores it as null.");
    expect(prompt).not.toContain('"date_of_birth": "YYYY-MM-DD (optional)"');
    expect(prompt).not.toContain('"fec_ids":');
  });

  it("uses backend candidate_state_filing_ids when provided for state-level profiles", async () => {
    callResearchProviderMock.mockResolvedValueOnce({
      ok: true,
      parsed: {
        display_name: "Jordan Lee",
        first_name: "Jordan",
        last_name: "Lee",
        state_filing_ids: ["SHOULD_NOT_WIN"],
        sources: ["https://good.example/jordan-profile"],
      },
      rawText: "state-profile-backend-state-filing-ids",
    });
    verifyHttpUrlReachabilityMock.mockResolvedValue({ ok: true });

    const result = await enrichCandidateProfile(
      {
        candidateDisplayName: "Jordan Lee",
        districtName: "Los Angeles County, California",
        districtType: "county",
        state: "CA",
        electionDate: "2026-11-03",
        officialBallotTitle: "Assessor",
        rosterStateFilingIds: ["ca-1234", "CA-5678"],
        seedUrls: [],
      },
      { timeoutMs: 90000 },
      [{ provider: "openai", model: "gpt-test" }]
    );

    expect(result.ok).toBe(true);
    if (!result.ok) {
      return;
    }
    expect(result.profile.state_filing_ids).toEqual(["CA-1234", "CA-5678"]);
    const prompt = callResearchProviderMock.mock.calls[0]?.[1];
    expect(prompt).toContain('- candidate_state_filing_ids: ["ca-1234","CA-5678"]');
  });
});
