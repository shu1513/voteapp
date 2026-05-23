import { describe, expect, it, vi } from "vitest";

import {
  resolveCandidateRosterForProfileDrafts,
  type CandidateRosterResolvedEntry,
} from "../../src/pipeline/enrichers/candidateRosterEnricher.js";
import type { CandidateRosterEntry } from "../../src/contracts/candidateRosterPayloadContract.js";

const baseInput = {
  districtName: "Los Angeles County, California",
  districtType: "county",
  state: "CA",
  electionDate: "2026-06-02",
  officialBallotTitle: "Assessor",
  seedUrls: [] as const,
};

const aiConfig = {
  timeoutMs: 90_000,
  anthropicWebSearchMaxUses: 3,
  openAiApiKey: "test",
  anthropicApiKey: "test",
  geminiApiKey: "test",
};

function withRosterIndexes(
  candidates: CandidateRosterEntry[]
): Array<CandidateRosterEntry & { roster_index: number }> {
  return candidates.map((candidate, rosterIndex) => ({ ...candidate, roster_index: rosterIndex }));
}

describe("resolveCandidateRosterForProfileDrafts", () => {
  it("keeps same-name rows when party differs (no AI disambiguation call)", async () => {
    const disambiguateMock = vi.fn();

    const result = await resolveCandidateRosterForProfileDrafts(
      {
        ...baseInput,
        candidates: withRosterIndexes([
          {
            display_name: "John Smith",
            party: "Democrat",
            sources: ["https://example.org/a"],
          },
          {
            display_name: "John Smith",
            party: "Republican",
            sources: ["https://example.org/b"],
          },
        ]),
      },
      aiConfig,
      disambiguateMock
    );

    expect(disambiguateMock).not.toHaveBeenCalled();
    expect(result.resolvedCandidates).toHaveLength(2);
    for (const row of result.resolvedCandidates) {
      expect(row.skip_per_election_name_dedupe).toBe(true);
      expect(row.disambiguation_hint).toBeUndefined();
    }
  });

  it("keeps one row when all duplicate rows are ambiguous", async () => {
    const disambiguateMock = vi.fn().mockResolvedValue({
      ok: true,
      provider: "openai",
      model: "gpt-test",
      people: [
        {
          roster_index: 0,
          status: "ambiguous",
          sources: ["https://example.org/a-amb"],
        },
        {
          roster_index: 1,
          status: "ambiguous",
          sources: ["https://example.org/b-amb"],
        },
      ],
      aiRawDebug: null,
    });

    const result = await resolveCandidateRosterForProfileDrafts(
      {
        ...baseInput,
        candidates: withRosterIndexes([
          {
            display_name: "Jane Doe",
            sources: ["https://example.org/a"],
          },
          {
            display_name: "Jane Doe",
            sources: ["https://example.org/b"],
          },
        ]),
      },
      aiConfig,
      disambiguateMock
    );

    expect(disambiguateMock).toHaveBeenCalledTimes(1);
    expect(result.resolvedCandidates).toHaveLength(1);
    expect(result.resolvedCandidates[0]?.display_name).toBe("Jane Doe");
    expect(result.resolvedCandidates[0]?.skip_per_election_name_dedupe).toBeUndefined();
  });

  it("keeps clear rows, drops ambiguous/same_as_other rows, and records merge breadcrumbs", async () => {
    const disambiguateMock = vi
      .fn()
      .mockResolvedValueOnce({
        ok: true,
        provider: "openai",
        model: "gpt-test",
        people: [
          {
            roster_index: 0,
            status: "clear",
            disambiguation_hint: "incumbent listed on official roster",
            sources: ["https://example.org/same"],
          },
          {
            roster_index: 1,
            status: "ambiguous",
            sources: ["https://example.org/same-ambiguous"],
          },
        ],
        aiRawDebug: null,
      })
      .mockResolvedValueOnce({
        ok: true,
        provider: "openai",
        model: "gpt-test",
        people: [
          {
            roster_index: 2,
            status: "clear",
            disambiguation_hint: "candidate tied to district 1 filing",
            sources: ["https://example.org/diff-a"],
          },
          {
            roster_index: 3,
            status: "same_as_other",
            same_as_roster_index: 2,
            sources: ["https://example.org/diff-b"],
          },
        ],
        aiRawDebug: null,
      });

    const result = await resolveCandidateRosterForProfileDrafts(
      {
        ...baseInput,
        candidates: withRosterIndexes([
          {
            display_name: "Alex Kim",
            sources: ["https://example.org/1"],
          },
          {
            display_name: "Alex Kim",
            sources: ["https://example.org/2"],
          },
          {
            display_name: "Sam Lee",
            sources: ["https://example.org/3"],
          },
          {
            display_name: "Sam Lee",
            sources: ["https://example.org/4"],
          },
          {
            display_name: "Unique Name",
            sources: ["https://example.org/u"],
          },
        ]),
      },
      aiConfig,
      disambiguateMock
    );

    const names = result.resolvedCandidates.map((row: CandidateRosterResolvedEntry) => row.display_name);
    expect(names).toEqual(["Alex Kim", "Sam Lee", "Unique Name"]);

    const samRows = result.resolvedCandidates.filter((row) => row.display_name === "Sam Lee");
    expect(samRows).toHaveLength(1);
    for (const row of samRows) {
      expect(row.skip_per_election_name_dedupe).toBe(true);
      expect(row.disambiguation_hint).toBeTruthy();
    }

    const uniqueRow = result.resolvedCandidates.find((row) => row.display_name === "Unique Name");
    expect(uniqueRow?.skip_per_election_name_dedupe).toBeUndefined();

    const duplicateGroups = (result.debug.duplicate_groups as Array<Record<string, unknown>>) ?? [];
    const samLeeDebug = duplicateGroups.find((item) => item.duplicate_display_name === "Sam Lee");
    expect(samLeeDebug).toBeTruthy();
    expect(samLeeDebug?.strategy).toBe("ai_person_level_partial_keep");
    expect(samLeeDebug?.merged_roster_indexes).toEqual({ merged_into_2: [3] });
  });

  it("falls back to keep-one when disambiguation AI call fails", async () => {
    const disambiguateMock = vi.fn().mockResolvedValue({
      ok: false,
      retryable: true,
      errorCode: "TEMP_PROVIDER_ERROR",
      reason: "provider timeout",
      failureDebug: { detail: "timeout" },
    });

    const result = await resolveCandidateRosterForProfileDrafts(
      {
        ...baseInput,
        candidates: withRosterIndexes([
          {
            display_name: "Jordan Blake",
            sources: ["https://example.org/a"],
          },
          {
            display_name: "Jordan Blake",
            sources: ["https://example.org/b"],
          },
        ]),
      },
      aiConfig,
      disambiguateMock
    );

    expect(disambiguateMock).toHaveBeenCalledTimes(1);
    expect(result.resolvedCandidates).toHaveLength(1);
    expect(result.resolvedCandidates[0]?.display_name).toBe("Jordan Blake");
    expect(result.resolvedCandidates[0]?.skip_per_election_name_dedupe).toBeUndefined();
  });

  it("preserves original roster_index after upstream filtering", async () => {
    const disambiguateMock = vi.fn().mockResolvedValue({
      ok: true,
      provider: "openai",
      model: "gpt-test",
      people: [
        {
          roster_index: 1,
          status: "clear",
          disambiguation_hint: "candidate with district filing page",
          sources: ["https://example.org/d1"],
        },
        {
          roster_index: 2,
          status: "clear",
          disambiguation_hint: "candidate with campaign site",
          sources: ["https://example.org/d2"],
        },
      ],
      aiRawDebug: null,
    });

    const result = await resolveCandidateRosterForProfileDrafts(
      {
        ...baseInput,
        candidates: [
          {
            display_name: "Chris Park",
            roster_index: 1,
            sources: ["https://example.org/c1"],
          },
          {
            display_name: "Chris Park",
            roster_index: 2,
            sources: ["https://example.org/c2"],
          },
        ],
      },
      aiConfig,
      disambiguateMock
    );

    expect(result.resolvedCandidates).toHaveLength(2);
    expect(result.resolvedCandidates.map((row) => row.roster_index)).toEqual([1, 2]);
  });
});
