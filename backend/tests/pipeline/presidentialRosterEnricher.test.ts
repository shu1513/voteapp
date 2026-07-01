import { describe, expect, it, vi } from "vitest";

import { enrichPresidentialRosterCycle } from "../../src/pipeline/enrichers/presidentialRosterEnricher.js";
import type { PresidentialRosterAiResult } from "../../src/ai/enrichPresidentialRoster.js";
import type { PresidentialRosterStatusAiResult } from "../../src/ai/enrichPresidentialRosterStatus.js";
import type { PresidentialCandidateFecMatch } from "../../src/pipeline/presidential/presidentialCandidateFecMatcher.js";
import type { ActivePresidentialCycleCandidateForReconciliation } from "../../src/pipeline/presidential/presidentialRosterReconciliation.js";

const CYCLE_ID = "00000000-0000-4000-8000-000000000001";

function makeDb(row: Record<string, unknown> | null = {
  id: CYCLE_ID,
  election_year: 2028,
  stage: "primary",
  party: "Democratic",
}) {
  return {
    query: vi.fn(async (sql: string) => {
      const text = String(sql);
      if (text.includes("FROM public.presidential_cycles")) {
        return { rows: row ? [row] : [], rowCount: row ? 1 : 0 };
      }
      return { rows: [], rowCount: 0 };
    }),
  };
}

function successfulAiResult(): PresidentialRosterAiResult {
  return {
    ok: true,
    provider: "claude",
    model: "claude-sonnet-4-6",
    aiRawDebug: { debug: "ok" },
    candidates: [
      {
        display_name: "Jane President",
        party: "Democratic",
        fec_candidate_id: "P80000001",
        sources: ["https://example.org/jane"],
        qualification_evidence: [
          {
            kind: "official_campaign_website",
            source_url: "https://jane.example.org",
          },
        ],
        status: "active",
      },
      {
        display_name: "Pat Maybe",
        party: "Democratic",
        fec_candidate_id: "P80000002",
        sources: ["https://example.org/pat"],
        qualification_evidence: [
          {
            kind: "public_campaign_launch",
            source_url: "https://example.org/pat-launch",
          },
        ],
        status: "active",
      },
      {
        display_name: "Chris Suspended",
        party: "Democratic",
        fec_candidate_id: "P80000003",
        sources: ["https://example.org/chris"],
        qualification_evidence: [
          {
            kind: "public_campaign_launch",
            source_url: "https://example.org/chris-suspends",
          },
        ],
        status: "withdrawn",
      },
    ],
  };
}

function matched(fecCandidateId: string): PresidentialCandidateFecMatch {
  return {
    matchStatus: "matched",
    method: "exact_fec_id",
    confidence: 1,
    matchedFecId: fecCandidateId,
    matchedCandidate: {
      fecCandidateId,
      name: "Jane President",
      party: "DEM",
      partyFull: "Democratic Party",
      office: "P",
      officeFull: "President",
      electionYears: [2028],
      principalCommittees: [],
      fecCandidateUrl: `https://www.fec.gov/data/candidate/${fecCandidateId}`,
    },
    fecSourceUrls: [`https://www.fec.gov/data/candidate/${fecCandidateId}`],
  };
}

function fuzzyMatched(fecCandidateId: string): PresidentialCandidateFecMatch {
  return {
    ...matched(fecCandidateId),
    method: "fuzzy_name_party",
    confidence: 0.95,
  };
}

function emptyReconciliationLoader() {
  return vi.fn().mockResolvedValue([]);
}

function activeCycleCandidate(input: {
  candidateId: string;
  displayName: string;
  fecIds: string[];
  party?: string;
}): ActivePresidentialCycleCandidateForReconciliation {
  return {
    candidateId: input.candidateId,
    displayName: input.displayName,
    party: input.party ?? "Democratic",
    fecIds: input.fecIds,
    sources: [`https://example.org/${input.candidateId}`],
  };
}

function existingCycleCandidateRow(overrides: Record<string, unknown> = {}) {
  return {
    candidate_id: "candidate-jane",
    presidential_profile_researched: false,
    running_mate_candidate_id: null,
    running_mate_profile_researched: false,
    running_mate_display_name: null,
    running_mate_first_name: null,
    running_mate_last_name: null,
    running_mate_fec_ids: [],
    ...overrides,
  };
}

describe("enrichPresidentialRosterCycle", () => {
  it("loads a primary cycle, matches FEC candidates, and emits presidential profile drafts", async () => {
    const db = makeDb();
    db.query = vi
      .fn()
      .mockResolvedValueOnce({
        rows: [
          {
            id: CYCLE_ID,
            election_year: 2028,
            stage: "primary",
            party: "Democratic",
          },
        ],
      })
      .mockResolvedValueOnce({ rows: [], rowCount: 0 })
      .mockResolvedValueOnce({ rows: [], rowCount: 1 });
    const redis = { sendCommand: vi.fn().mockResolvedValue(1) };
    const enrichRoster = vi.fn().mockResolvedValue(successfulAiResult());
    const matchCandidate = vi
      .fn()
      .mockResolvedValueOnce(matched("P80000001"))
      .mockResolvedValueOnce({
        matchStatus: "unmatched",
        method: "unmatched",
        confidence: 0,
        fecSourceUrls: [],
        reason: "no OpenFEC match",
      } satisfies PresidentialCandidateFecMatch)
      .mockResolvedValueOnce(matched("P80000003"));

    const result = await enrichPresidentialRosterCycle({
      db,
      redis,
      electionYear: 2028,
      stage: "primary",
      party: "Democratic",
      runId: "run-1",
      aiConfig: { timeoutMs: 1000 },
      fecOptions: { apiKeys: ["fec-key"], timeoutMs: 1000 },
      enrichRoster,
      matchCandidate,
      loadActiveCandidatesForReconciliation: emptyReconciliationLoader(),
    });

    expect(result).toMatchObject({
      ok: true,
      cycleId: CYCLE_ID,
      electionYear: 2028,
      stage: "primary",
      party: "Democratic",
      aiCandidateCount: 3,
      matchedCount: 1,
      ambiguousCount: 0,
      unmatchedCount: 1,
      withdrawnSkippedCount: 1,
      withdrawnDemotedCount: 1,
      emittedCount: 1,
      skippedCount: 0,
      dryRun: false,
      admissionPolicy: "fec_confirmed_only",
      statusVerification: expect.objectContaining({
        checkedCount: 0,
        skippedCount: 0,
        demotedCount: 0,
      }),
    });
    expect(db.query).toHaveBeenNthCalledWith(1, expect.stringContaining("FROM public.presidential_cycles"), [
      2028,
      "primary",
      "Democratic",
    ]);
    expect(enrichRoster).toHaveBeenCalledWith(
      {
        cycleId: CYCLE_ID,
        electionYear: 2028,
        stage: "primary",
        party: "Democratic",
      },
      { timeoutMs: 1000 },
      undefined
    );
    expect(matchCandidate).toHaveBeenCalledTimes(3);
    expect(matchCandidate).toHaveBeenCalledWith(
      expect.objectContaining({
        electionYear: 2028,
        expectedParty: "Democratic",
        candidate: expect.objectContaining({ display_name: "Jane President" }),
        options: expect.objectContaining({ apiKeys: ["fec-key"] }),
      })
    );

    expect(redis.sendCommand).toHaveBeenCalledTimes(1);
    const args = redis.sendCommand.mock.calls[0]?.[0] as string[];
    expect(args[3]).toBe("staging:candidates:profile:draft");
    expect(args[4]).toContain(
      `staging:candidate_profile_draft_emitted:presidential_cycle:${CYCLE_ID}:fec:P80000001`
    );
    expect(args[5]).toBe("");
    expect(args[8]).toBe("Jane President");
    expect(args[9]).toBe("Democratic");
    expect(args[13]).toContain("matched to OpenFEC candidate P80000001");
    expect(args[15]).toBe(JSON.stringify(["P80000001"]));
    expect(args[18]).toBe("presidential_cycle");
    expect(args[19]).toBe(CYCLE_ID);
    expect(args[20]).toBe("president");
    expect(args[21]).toBe("");
    expect(result.ok ? result.matches : []).toEqual([
      expect.objectContaining({
        displayName: "Jane President",
        matchStatus: "matched",
        admissionStatus: "admitted",
        admissionReason: "OpenFEC candidate match confirmed",
        matchedFecId: "P80000001",
      }),
      expect.objectContaining({
        displayName: "Pat Maybe",
        matchStatus: "unmatched",
        admissionStatus: "not_admitted",
        admissionReason: "No OpenFEC match; no profile draft emitted",
        reason: "no OpenFEC match",
      }),
      expect.objectContaining({
        displayName: "Chris Suspended",
        matchStatus: "matched",
        admissionStatus: "not_admitted",
        admissionReason: "OpenFEC-confirmed withdrawn candidate demoted existing presidential roster link",
        matchedFecId: "P80000003",
      }),
    ]);
    expect(db.query).toHaveBeenNthCalledWith(
      3,
      expect.stringContaining("UPDATE public.presidential_cycle_candidates"),
      [CYCLE_ID, "P80000003"]
    );
  });

  it("skips presidential profile drafts when the existing cycle candidate was already researched", async () => {
    const db = makeDb();
    db.query = vi
      .fn()
      .mockResolvedValueOnce({
        rows: [
          {
            id: CYCLE_ID,
            election_year: 2028,
            stage: "primary",
            party: "Democratic",
          },
        ],
      })
      .mockResolvedValueOnce({
        rows: [
          existingCycleCandidateRow({
            presidential_profile_researched: true,
          }),
        ],
        rowCount: 1,
      });
    const redis = { sendCommand: vi.fn().mockResolvedValue(1) };

    const result = await enrichPresidentialRosterCycle({
      db,
      redis,
      electionYear: 2028,
      stage: "primary",
      party: "Democratic",
      runId: "run-1",
      aiConfig: { timeoutMs: 1000 },
      fecOptions: { apiKeys: ["fec-key"], timeoutMs: 1000 },
      enrichRoster: vi.fn().mockResolvedValue({
        ok: true,
        provider: "claude",
        model: "claude-sonnet-4-6",
        aiRawDebug: null,
        candidates: [
          {
            display_name: "Jane President",
            party: "Democratic",
            fec_candidate_id: "P80000001",
            sources: ["https://example.org/jane"],
            status: "active",
          },
        ],
      } satisfies PresidentialRosterAiResult),
      matchCandidate: vi.fn().mockResolvedValue(matched("P80000001")),
      loadActiveCandidatesForReconciliation: emptyReconciliationLoader(),
    });

    expect(result).toMatchObject({
      ok: true,
      matchedCount: 1,
      emittedCount: 0,
      skippedCount: 1,
    });
    expect(redis.sendCommand).not.toHaveBeenCalled();
  });

  it("emits a vice-president profile draft when a running mate is officially announced", async () => {
    const db = makeDb();
    db.query = vi
      .fn()
      .mockResolvedValueOnce({
        rows: [
          {
            id: CYCLE_ID,
            election_year: 2028,
            stage: "primary",
            party: "Democratic",
          },
        ],
      })
      .mockResolvedValueOnce({ rows: [], rowCount: 0 });
    const redis = { sendCommand: vi.fn().mockResolvedValue(1) };

    const result = await enrichPresidentialRosterCycle({
      db,
      redis,
      electionYear: 2028,
      stage: "primary",
      party: "Democratic",
      runId: "run-1",
      aiConfig: { timeoutMs: 1000 },
      fecOptions: { apiKeys: ["fec-key"], timeoutMs: 1000 },
      enrichRoster: vi.fn().mockResolvedValue({
        ok: true,
        provider: "claude",
        model: "claude-sonnet-4-6",
        aiRawDebug: null,
        candidates: [
          {
            display_name: "Jane President",
            party: "Democratic",
            fec_candidate_id: "P80000001",
            sources: ["https://example.org/jane"],
            status: "active",
            running_mate: {
              display_name: "Pat Running Mate",
              fec_candidate_id: "P80000002",
              sources: ["https://example.org/pat"],
            },
          },
        ],
      } satisfies PresidentialRosterAiResult),
      matchCandidate: vi.fn().mockResolvedValue(matched("P80000001")),
      loadActiveCandidatesForReconciliation: emptyReconciliationLoader(),
    });

    expect(result).toMatchObject({
      ok: true,
      matchedCount: 1,
      emittedCount: 2,
      skippedCount: 0,
    });
    expect(redis.sendCommand).toHaveBeenCalledTimes(2);
    const presidentArgs = redis.sendCommand.mock.calls[0]?.[0] as string[];
    const runningMateArgs = redis.sendCommand.mock.calls[1]?.[0] as string[];
    expect(presidentArgs[8]).toBe("Jane President");
    expect(presidentArgs[20]).toBe("president");
    expect(runningMateArgs[8]).toBe("Pat Running Mate");
    expect(runningMateArgs[15]).toBe(JSON.stringify(["P80000002"]));
    expect(runningMateArgs[18]).toBe("presidential_cycle");
    expect(runningMateArgs[19]).toBe(CYCLE_ID);
    expect(runningMateArgs[20]).toBe("vice_president");
    expect(runningMateArgs[21]).toBe("P80000001");
    expect(runningMateArgs[13]).toContain("Officially announced running mate for Jane President");
  });

  it("skips a running mate draft when the same linked running mate was already researched", async () => {
    const db = makeDb();
    db.query = vi
      .fn()
      .mockResolvedValueOnce({
        rows: [
          {
            id: CYCLE_ID,
            election_year: 2028,
            stage: "primary",
            party: "Democratic",
          },
        ],
      })
      .mockResolvedValueOnce({
        rows: [
          existingCycleCandidateRow({
            running_mate_candidate_id: "candidate-running-mate",
            running_mate_profile_researched: true,
            running_mate_display_name: "Pat Running Mate",
            running_mate_fec_ids: ["P80000002"],
          }),
        ],
        rowCount: 1,
      });
    const redis = { sendCommand: vi.fn().mockResolvedValue(1) };

    const result = await enrichPresidentialRosterCycle({
      db,
      redis,
      electionYear: 2028,
      stage: "primary",
      party: "Democratic",
      runId: "run-1",
      aiConfig: { timeoutMs: 1000 },
      fecOptions: { apiKeys: ["fec-key"], timeoutMs: 1000 },
      enrichRoster: vi.fn().mockResolvedValue({
        ok: true,
        provider: "claude",
        model: "claude-sonnet-4-6",
        aiRawDebug: null,
        candidates: [
          {
            display_name: "Jane President",
            party: "Democratic",
            fec_candidate_id: "P80000001",
            sources: ["https://example.org/jane"],
            status: "active",
            running_mate: {
              display_name: "Pat Running Mate",
              fec_candidate_id: "P80000002",
              sources: ["https://example.org/pat"],
            },
          },
        ],
      } satisfies PresidentialRosterAiResult),
      matchCandidate: vi.fn().mockResolvedValue(matched("P80000001")),
      loadActiveCandidatesForReconciliation: emptyReconciliationLoader(),
    });

    expect(result).toMatchObject({
      ok: true,
      matchedCount: 1,
      emittedCount: 1,
      skippedCount: 1,
    });
    expect(redis.sendCommand).toHaveBeenCalledTimes(1);
    const args = redis.sendCommand.mock.calls[0]?.[0] as string[];
    expect(args[8]).toBe("Jane President");
    expect(args[20]).toBe("president");
  });

  it("does not emit drafts in dry-run mode", async () => {
    const db = makeDb();
    const redis = { sendCommand: vi.fn().mockResolvedValue(1) };

    const result = await enrichPresidentialRosterCycle({
      db,
      redis,
      electionYear: 2028,
      party: "Democratic",
      dryRun: true,
      aiConfig: { timeoutMs: 1000 },
      fecOptions: { apiKeys: ["fec-key"], timeoutMs: 1000 },
      enrichRoster: vi.fn().mockResolvedValue({
        ok: true,
        provider: "claude",
        model: "claude-sonnet-4-6",
        aiRawDebug: null,
        candidates: [
          {
            display_name: "Jane President",
            party: "Democratic",
            sources: ["https://example.org/jane"],
            status: "active",
          },
        ],
      } satisfies PresidentialRosterAiResult),
      matchCandidate: vi.fn().mockResolvedValue(matched("P80000001")),
      loadActiveCandidatesForReconciliation: emptyReconciliationLoader(),
    });

    expect(result).toMatchObject({
      ok: true,
      matchedCount: 1,
      emittedCount: 0,
      skippedCount: 0,
      dryRun: true,
      statusVerification: expect.objectContaining({
        checkedCount: 0,
        demotedCount: 0,
        dryRun: true,
      }),
    });
    expect(redis.sendCommand).not.toHaveBeenCalled();
  });

  it("does not demote withdrawn candidates matched only fuzzily", async () => {
    const db = makeDb();
    const redis = { sendCommand: vi.fn().mockResolvedValue(1) };

    const result = await enrichPresidentialRosterCycle({
      db,
      redis,
      electionYear: 2028,
      party: "Democratic",
      aiConfig: { timeoutMs: 1000 },
      fecOptions: { apiKeys: ["fec-key"], timeoutMs: 1000 },
      enrichRoster: vi.fn().mockResolvedValue({
        ok: true,
        provider: "claude",
        model: "claude-sonnet-4-6",
        aiRawDebug: null,
        candidates: [
          {
            display_name: "Jane Suspended",
            party: "Democratic",
            sources: ["https://example.org/jane"],
            status: "withdrawn",
          },
        ],
      } satisfies PresidentialRosterAiResult),
      matchCandidate: vi.fn().mockResolvedValue(fuzzyMatched("P80000001")),
      loadActiveCandidatesForReconciliation: emptyReconciliationLoader(),
    });

    expect(result).toMatchObject({
      ok: true,
      withdrawnSkippedCount: 1,
      withdrawnDemotedCount: 0,
    });
    expect(result.ok ? result.matches : []).toEqual([
      expect.objectContaining({
        displayName: "Jane Suspended",
        matchStatus: "matched",
        method: "fuzzy_name_party",
        admissionStatus: "not_admitted",
        admissionReason: "withdrawn candidate matched only fuzzily; existing links are not automatically demoted",
        reason: "automatic withdrawal requires exact_fec_id or exact_name_party match",
      }),
    ]);
    expect(
      db.query.mock.calls.some((call) => String(call[0]).includes("UPDATE public.presidential_cycle_candidates"))
    ).toBe(false);
  });

  it("tracks ambiguous FEC matches without emitting profile drafts", async () => {
    const db = makeDb();
    const redis = { sendCommand: vi.fn().mockResolvedValue(1) };
    const matchCandidate = vi.fn().mockResolvedValue({
      matchStatus: "ambiguous",
      method: "ambiguous",
      confidence: 0.99,
      fecSourceUrls: ["https://www.fec.gov/data/candidate/P80000001"],
      reason: "multiple OpenFEC candidates matched",
    } satisfies PresidentialCandidateFecMatch);

    const result = await enrichPresidentialRosterCycle({
      db,
      redis,
      electionYear: 2028,
      party: "Democratic",
      aiConfig: { timeoutMs: 1000 },
      fecOptions: { apiKeys: ["fec-key"], timeoutMs: 1000 },
      enrichRoster: vi.fn().mockResolvedValue({
        ok: true,
        provider: "claude",
        model: "claude-sonnet-4-6",
        aiRawDebug: null,
        candidates: [
          {
            display_name: "Jane President",
            party: "Democratic",
            sources: ["https://example.org/jane"],
            status: "active",
          },
        ],
      } satisfies PresidentialRosterAiResult),
      matchCandidate,
      loadActiveCandidatesForReconciliation: emptyReconciliationLoader(),
    });

    expect(result).toMatchObject({
      ok: true,
      matchedCount: 0,
      ambiguousCount: 1,
      unmatchedCount: 0,
      emittedCount: 0,
      skippedCount: 0,
    });
    expect(result.ok ? result.matches : []).toEqual([
      expect.objectContaining({
        displayName: "Jane President",
        matchStatus: "ambiguous",
        method: "ambiguous",
        admissionStatus: "not_admitted",
        admissionReason: "OpenFEC match was ambiguous; no profile draft emitted",
        reason: "multiple OpenFEC candidates matched",
      }),
    ]);
    expect(redis.sendCommand).not.toHaveBeenCalled();
  });

  it("returns a non-retryable failure when the cycle does not exist", async () => {
    const result = await enrichPresidentialRosterCycle({
      db: makeDb(null),
      redis: { sendCommand: vi.fn() },
      electionYear: 2028,
      party: "Democratic",
      aiConfig: { timeoutMs: 1000 },
      fecOptions: { apiKeys: ["fec-key"], timeoutMs: 1000 },
      enrichRoster: vi.fn(),
      loadActiveCandidatesForReconciliation: emptyReconciliationLoader(),
    });

    expect(result).toMatchObject({
      ok: false,
      electionYear: 2028,
      stage: "primary",
      party: "Democratic",
      retryable: false,
      errorCode: "CYCLE_NOT_FOUND",
    });
  });

  it("returns AI failures without attempting FEC matching", async () => {
    const matchCandidate = vi.fn();
    const result = await enrichPresidentialRosterCycle({
      db: makeDb(),
      redis: { sendCommand: vi.fn() },
      electionYear: 2028,
      party: "Democratic",
      aiConfig: { timeoutMs: 1000 },
      fecOptions: { apiKeys: ["fec-key"], timeoutMs: 1000 },
      enrichRoster: vi.fn().mockResolvedValue({
        ok: false,
        retryable: false,
        errorCode: "SCHEMA_MISMATCH",
        reason: "bad payload",
      } satisfies PresidentialRosterAiResult),
      matchCandidate,
      loadActiveCandidatesForReconciliation: emptyReconciliationLoader(),
    });

    expect(result).toMatchObject({
      ok: false,
      cycleId: CYCLE_ID,
      error: "bad payload",
      errorCode: "SCHEMA_MISMATCH",
    });
    expect(matchCandidate).not.toHaveBeenCalled();
  });

  it("verifies omitted active candidates and demotes confirmed withdrawals", async () => {
    const db = makeDb();
    db.query = vi
      .fn()
      .mockResolvedValueOnce({
        rows: [
          {
            id: CYCLE_ID,
            election_year: 2028,
            stage: "primary",
            party: "Democratic",
          },
        ],
      })
      .mockResolvedValueOnce({ rows: [], rowCount: 0 })
      .mockResolvedValueOnce({ rows: [], rowCount: 1 });
    const redis = { sendCommand: vi.fn().mockResolvedValue(1) };
    const loadActiveCandidatesForReconciliation = vi.fn().mockResolvedValue([
      activeCycleCandidate({
        candidateId: "candidate-jane",
        displayName: "Jane President",
        fecIds: ["P80000001"],
      }),
      activeCycleCandidate({
        candidateId: "candidate-old",
        displayName: "Old Candidate",
        fecIds: ["P80000002"],
      }),
    ]);
    const enrichRosterStatus = vi.fn().mockResolvedValue({
      ok: true,
      provider: "claude",
      model: "claude-sonnet-4-6",
      aiRawDebug: null,
      candidates: [
        {
          candidate_id: "candidate-old",
          status: "withdrawn",
          sources: ["https://example.org/old-withdrawn"],
        },
      ],
    } satisfies PresidentialRosterStatusAiResult);

    const result = await enrichPresidentialRosterCycle({
      db,
      redis,
      electionYear: 2028,
      stage: "primary",
      party: "Democratic",
      aiConfig: { timeoutMs: 1000 },
      fecOptions: { apiKeys: ["fec-key"], timeoutMs: 1000 },
      enrichRoster: vi.fn().mockResolvedValue({
        ok: true,
        provider: "claude",
        model: "claude-sonnet-4-6",
        aiRawDebug: null,
        candidates: [
          {
            display_name: "Jane President",
            party: "Democratic",
            fec_candidate_id: "P80000001",
            sources: ["https://example.org/jane"],
            status: "active",
          },
        ],
      } satisfies PresidentialRosterAiResult),
      matchCandidate: vi.fn().mockResolvedValue(matched("P80000001")),
      loadActiveCandidatesForReconciliation,
      enrichRosterStatus,
    });

    expect(result).toMatchObject({
      ok: true,
      statusVerification: {
        checkedCount: 1,
        withdrawnCount: 1,
        activeCount: 0,
        skippedCount: 1,
        demotedCount: 1,
        dryRun: false,
        provider: "claude",
        model: "claude-sonnet-4-6",
      },
    });
    expect(enrichRosterStatus).toHaveBeenCalledWith(
      expect.objectContaining({
        cycleId: CYCLE_ID,
        candidates: [
          expect.objectContaining({
            candidateId: "candidate-old",
            displayName: "Old Candidate",
            fecIds: ["P80000002"],
          }),
        ],
      }),
      { timeoutMs: 1000 },
      undefined
    );
    expect(db.query).toHaveBeenNthCalledWith(
      3,
      expect.stringContaining("UPDATE public.presidential_cycle_candidates"),
      [CYCLE_ID, "candidate-old"]
    );
  });

  it("skips status verification AI when active DB candidates are present in the fresh roster", async () => {
    const db = makeDb();
    const redis = { sendCommand: vi.fn().mockResolvedValue(1) };
    const enrichRosterStatus = vi.fn();

    const result = await enrichPresidentialRosterCycle({
      db,
      redis,
      electionYear: 2028,
      party: "Democratic",
      aiConfig: { timeoutMs: 1000 },
      fecOptions: { apiKeys: ["fec-key"], timeoutMs: 1000 },
      enrichRoster: vi.fn().mockResolvedValue({
        ok: true,
        provider: "claude",
        model: "claude-sonnet-4-6",
        aiRawDebug: null,
        candidates: [
          {
            display_name: "Jane President",
            party: "Democratic",
            fec_candidate_id: "P80000001",
            sources: ["https://example.org/jane"],
            status: "active",
          },
        ],
      } satisfies PresidentialRosterAiResult),
      matchCandidate: vi.fn().mockResolvedValue(matched("P80000001")),
      loadActiveCandidatesForReconciliation: vi.fn().mockResolvedValue([
        activeCycleCandidate({
          candidateId: "candidate-jane",
          displayName: "Jane President",
          fecIds: ["P80000099", "P80000001"],
        }),
      ]),
      enrichRosterStatus,
    });

    expect(result).toMatchObject({
      ok: true,
      statusVerification: {
        checkedCount: 0,
        withdrawnCount: 0,
        activeCount: 0,
        skippedCount: 1,
        demotedCount: 0,
        dryRun: false,
      },
    });
    expect(enrichRosterStatus).not.toHaveBeenCalled();
  });

  it("keeps omitted candidates when status verification returns active", async () => {
    const db = makeDb();
    const redis = { sendCommand: vi.fn().mockResolvedValue(1) };
    const enrichRosterStatus = vi.fn().mockResolvedValue({
      ok: true,
      provider: "claude",
      model: "claude-sonnet-4-6",
      aiRawDebug: null,
      candidates: [
        {
          candidate_id: "candidate-active",
          status: "active",
          sources: ["https://example.org/active"],
        },
      ],
    } satisfies PresidentialRosterStatusAiResult);

    const result = await enrichPresidentialRosterCycle({
      db,
      redis,
      electionYear: 2028,
      party: "Democratic",
      aiConfig: { timeoutMs: 1000 },
      fecOptions: { apiKeys: ["fec-key"], timeoutMs: 1000 },
      enrichRoster: vi.fn().mockResolvedValue({
        ok: true,
        provider: "claude",
        model: "claude-sonnet-4-6",
        aiRawDebug: null,
        candidates: [],
      } satisfies PresidentialRosterAiResult),
      matchCandidate: vi.fn(),
      loadActiveCandidatesForReconciliation: vi.fn().mockResolvedValue([
        activeCycleCandidate({
          candidateId: "candidate-active",
          displayName: "Active Candidate",
          fecIds: ["P80000004"],
        }),
      ]),
      enrichRosterStatus,
    });

    expect(result).toMatchObject({
      ok: true,
      statusVerification: expect.objectContaining({
        checkedCount: 1,
        withdrawnCount: 0,
        activeCount: 1,
        demotedCount: 0,
      }),
    });
    expect(db.query).toHaveBeenCalledTimes(1);
  });

  it("records reconciliation loader errors without failing the roster run", async () => {
    const db = makeDb();
    const redis = { sendCommand: vi.fn().mockResolvedValue(1) };
    const enrichRosterStatus = vi.fn();

    const result = await enrichPresidentialRosterCycle({
      db,
      redis,
      electionYear: 2028,
      party: "Democratic",
      aiConfig: { timeoutMs: 1000 },
      fecOptions: { apiKeys: ["fec-key"], timeoutMs: 1000 },
      enrichRoster: vi.fn().mockResolvedValue({
        ok: true,
        provider: "claude",
        model: "claude-sonnet-4-6",
        aiRawDebug: null,
        candidates: [
          {
            display_name: "Jane President",
            party: "Democratic",
            fec_candidate_id: "P80000001",
            sources: ["https://example.org/jane"],
            status: "active",
          },
        ],
      } satisfies PresidentialRosterAiResult),
      matchCandidate: vi.fn().mockResolvedValue(matched("P80000001")),
      loadActiveCandidatesForReconciliation: vi.fn().mockRejectedValue(new Error("database temporarily unavailable")),
      enrichRosterStatus,
    });

    expect(result).toMatchObject({
      ok: true,
      statusVerification: expect.objectContaining({
        checkedCount: 0,
        demotedCount: 0,
        error: "database temporarily unavailable",
        errorCode: "STATUS_VERIFICATION_ERROR",
      }),
    });
    expect(enrichRosterStatus).not.toHaveBeenCalled();
  });

  it("records status verification failures without failing the roster run", async () => {
    const db = makeDb();
    const redis = { sendCommand: vi.fn().mockResolvedValue(1) };

    const result = await enrichPresidentialRosterCycle({
      db,
      redis,
      electionYear: 2028,
      party: "Democratic",
      aiConfig: { timeoutMs: 1000 },
      fecOptions: { apiKeys: ["fec-key"], timeoutMs: 1000 },
      enrichRoster: vi.fn().mockResolvedValue({
        ok: true,
        provider: "claude",
        model: "claude-sonnet-4-6",
        aiRawDebug: null,
        candidates: [],
      } satisfies PresidentialRosterAiResult),
      matchCandidate: vi.fn(),
      loadActiveCandidatesForReconciliation: vi.fn().mockResolvedValue([
        activeCycleCandidate({
          candidateId: "candidate-old",
          displayName: "Old Candidate",
          fecIds: ["P80000002"],
        }),
      ]),
      enrichRosterStatus: vi.fn().mockResolvedValue({
        ok: false,
        retryable: true,
        errorCode: "TEMP_PROVIDER_ERROR",
        reason: "temporary provider failure",
      } satisfies PresidentialRosterStatusAiResult),
    });

    expect(result).toMatchObject({
      ok: true,
      statusVerification: expect.objectContaining({
        checkedCount: 1,
        demotedCount: 0,
        error: "temporary provider failure",
        errorCode: "TEMP_PROVIDER_ERROR",
      }),
    });
    expect(db.query).toHaveBeenCalledTimes(1);
  });

  it("verifies omitted candidates in dry-run mode without demoting", async () => {
    const db = makeDb();
    const redis = { sendCommand: vi.fn().mockResolvedValue(1) };

    const result = await enrichPresidentialRosterCycle({
      db,
      redis,
      electionYear: 2028,
      party: "Democratic",
      dryRun: true,
      aiConfig: { timeoutMs: 1000 },
      fecOptions: { apiKeys: ["fec-key"], timeoutMs: 1000 },
      enrichRoster: vi.fn().mockResolvedValue({
        ok: true,
        provider: "claude",
        model: "claude-sonnet-4-6",
        aiRawDebug: null,
        candidates: [],
      } satisfies PresidentialRosterAiResult),
      matchCandidate: vi.fn(),
      loadActiveCandidatesForReconciliation: vi.fn().mockResolvedValue([
        activeCycleCandidate({
          candidateId: "candidate-old",
          displayName: "Old Candidate",
          fecIds: ["P80000002"],
        }),
      ]),
      enrichRosterStatus: vi.fn().mockResolvedValue({
        ok: true,
        provider: "claude",
        model: "claude-sonnet-4-6",
        aiRawDebug: null,
        candidates: [
          {
            candidate_id: "candidate-old",
            status: "withdrawn",
            sources: ["https://example.org/old-withdrawn"],
          },
        ],
      } satisfies PresidentialRosterStatusAiResult),
    });

    expect(result).toMatchObject({
      ok: true,
      statusVerification: expect.objectContaining({
        checkedCount: 1,
        withdrawnCount: 1,
        demotedCount: 0,
        dryRun: true,
      }),
    });
    expect(db.query).toHaveBeenCalledTimes(1);
  });
});
