import type { Pool } from "pg";
import { describe, expect, it, vi } from "vitest";

import {
  parseManualPresidentialProfileScriptArgs,
  runManualPresidentialProfileWrite,
  type ManualPresidentialProfileScriptOptions,
} from "../../src/scripts/writeManualPresidentialProfile.js";
import type { CandidateProfilePayload } from "../../src/contracts/candidateProfilePayloadContract.js";

const CYCLE_ID = "00000000-0000-4000-8000-000000000001";
const NOW = new Date("2026-06-30T12:00:00.000Z");

function profile(overrides: Partial<CandidateProfilePayload> = {}): CandidateProfilePayload {
  return {
    display_name: "Jane President",
    first_name: "Jane",
    last_name: "President",
    party: "Democratic",
    official_website_url: "https://example.org/jane",
    fec_ids: ["P80000001"],
    current_office: "Governor",
    summary: "Jane President is a public official.",
    sources: ["https://example.org/jane"],
    ...overrides,
  };
}

function options(
  overrides: Partial<ManualPresidentialProfileScriptOptions> = {}
): ManualPresidentialProfileScriptOptions {
  return {
    presidentialCycleId: CYCLE_ID,
    presidentialRole: "president",
    parentPresidentialCandidateFecId: null,
    file: "profile.json",
    dryRun: false,
    runId: "manual-run",
    emitRecordDraft: false,
    strictQualityGate: false,
    allowNoHardIdentifier: false,
    confirmedGapIds: new Set(),
    repairReportFile: null,
    ...overrides,
  };
}

function poolWithCycle() {
  const client = {
    query: vi.fn().mockResolvedValue({ rows: [], rowCount: 0 }),
    release: vi.fn(),
  };
  const pool = {
    query: vi.fn().mockResolvedValue({
      rows: [
        {
          id: CYCLE_ID,
          election_year: 2028,
          stage: "primary",
          party: "Democratic",
          election_date: "2028-02-01",
          sources: ["https://example.org/cycle"],
        },
      ],
      rowCount: 1,
    }),
    connect: vi.fn().mockResolvedValue(client),
  };
  return { pool: pool as unknown as Pool, client };
}

describe("parseManualPresidentialProfileScriptArgs", () => {
  it("parses president profile flags", () => {
    expect(
      parseManualPresidentialProfileScriptArgs(
        [
          "--presidential-cycle-id",
          CYCLE_ID,
          "--presidential-role",
          "president",
          "--file",
          "profile.json",
          "--dry-run",
          "--run-id",
          "manual-run",
        ],
        NOW
      )
    ).toEqual(options({ dryRun: true }));
  });

  it("requires a parent FEC ID for vice president profiles", () => {
    expect(() =>
      parseManualPresidentialProfileScriptArgs(
        [
          "--presidential-cycle-id",
          CYCLE_ID,
          "--presidential-role",
          "vice_president",
          "--file",
          "profile.json",
        ],
        NOW
      )
    ).toThrow("--parent-presidential-candidate-fec-id is required for vice_president profiles");
  });

  it("rejects boolean flags with explicit values", () => {
    expect(() =>
      parseManualPresidentialProfileScriptArgs(
        [
          `--presidential-cycle-id=${CYCLE_ID}`,
          "--presidential-role=president",
          "--file=profile.json",
          "--dry-run=true",
        ],
        NOW
      )
    ).toThrow("Boolean flag must not include a value: --dry-run");
    expect(() =>
      parseManualPresidentialProfileScriptArgs(
        [
          `--presidential-cycle-id=${CYCLE_ID}`,
          "--presidential-role=president",
          "--file=profile.json",
          "--strict-quality-gate",
          "true",
        ],
        NOW
      )
    ).toThrow("Boolean flag must not include a value: --strict-quality-gate");
  });
});

describe("runManualPresidentialProfileWrite", () => {
  it("finds or creates a president profile and links it to the presidential cycle", async () => {
    const { pool, client } = poolWithCycle();
    const incomingProfile = profile({ party: "Independent" });
    const findOrCreateCandidate = vi.fn().mockResolvedValue({
      candidateId: "candidate-president",
      matchedExisting: false,
    });
    const upsertCycleCandidate = vi.fn().mockResolvedValue(undefined);
    const markCycleCandidateProfileResearched = vi.fn().mockResolvedValue({ updatedCount: 1 });

    const result = await runManualPresidentialProfileWrite({
      options: options(),
      rawPayload: incomingProfile,
      pool,
      deps: {
        validateProfile: vi.fn().mockResolvedValue({
          ok: true,
          profile: incomingProfile,
          sourceCount: 1,
        }),
        findOrCreateCandidate,
        upsertCycleCandidate,
        markCycleCandidateProfileResearched,
      },
    });

    expect(result).toMatchObject({
      ok: true,
      dryRun: false,
      candidateId: "candidate-president",
      matchedExisting: false,
      presidentialCycleCandidateLinked: true,
      runningMateLinked: false,
    });
    expect(findOrCreateCandidate).toHaveBeenCalledWith(
      expect.objectContaining({
        client,
        profile: incomingProfile,
        state: "US",
        rosterParty: "Democratic",
        includeParty: true,
        allowCrossStateHardIdentifierMatch: true,
      })
    );
    expect(upsertCycleCandidate).toHaveBeenCalledWith({
      client,
      cycleId: CYCLE_ID,
      candidateId: "candidate-president",
      party: "Democratic",
      sources: ["https://example.org/jane"],
    });
    expect(markCycleCandidateProfileResearched).toHaveBeenCalledWith({
      db: client,
      cycleId: CYCLE_ID,
      candidateId: "candidate-president",
    });
    expect(client.query.mock.calls.map((call) => call[0])).toEqual(["BEGIN", "COMMIT"]);
  });

  it("links a vice president profile to the parent presidential candidate", async () => {
    const { pool, client } = poolWithCycle();
    const findOrCreateCandidate = vi.fn().mockResolvedValue({
      candidateId: "candidate-vp",
      matchedExisting: true,
    });
    const findParentCandidateByFecId = vi.fn().mockResolvedValue("candidate-president");
    const setRunningMate = vi.fn().mockResolvedValue({ updatedCount: 1 });
    const markRunningMateProfileResearched = vi.fn().mockResolvedValue({ updatedCount: 1 });
    const enqueueRecordDrafts = vi.fn().mockResolvedValue({ emittedCount: 1, skippedCount: 0 });
    const redis = { sendCommand: vi.fn() };

    const result = await runManualPresidentialProfileWrite({
      options: options({
        presidentialRole: "vice_president",
        parentPresidentialCandidateFecId: "P80000001",
        emitRecordDraft: true,
      }),
      rawPayload: profile({
        display_name: "Vice Candidate",
        first_name: "Vice",
        last_name: "Candidate",
        fec_ids: undefined,
      }),
      pool,
      redis,
      deps: {
        validateProfile: vi.fn().mockResolvedValue({
          ok: true,
          profile: profile({
            display_name: "Vice Candidate",
            first_name: "Vice",
            last_name: "Candidate",
            fec_ids: undefined,
          }),
          sourceCount: 1,
        }),
        findOrCreateCandidate,
        findParentCandidateByFecId,
        setRunningMate,
        markRunningMateProfileResearched,
        enqueueRecordDrafts,
      },
    });

    expect(result).toMatchObject({
      ok: true,
      dryRun: false,
      candidateId: "candidate-vp",
      parentCandidateId: "candidate-president",
      presidentialCycleCandidateLinked: false,
      runningMateLinked: true,
      recordDraft: { emittedCount: 1, skippedCount: 0 },
    });
    expect(findParentCandidateByFecId).toHaveBeenCalledWith({
      db: client,
      cycleId: CYCLE_ID,
      fecCandidateId: "P80000001",
    });
    expect(setRunningMate).toHaveBeenCalledWith({
      db: client,
      cycleId: CYCLE_ID,
      candidateId: "candidate-president",
      runningMateCandidateId: "candidate-vp",
    });
    expect(markRunningMateProfileResearched).toHaveBeenCalledWith({
      db: client,
      cycleId: CYCLE_ID,
      candidateId: "candidate-president",
      runningMateCandidateId: "candidate-vp",
    });
    expect(enqueueRecordDrafts).toHaveBeenCalledWith(redis, [
      {
        contextType: "presidential_cycle",
        candidateId: "candidate-vp",
        presidentialCycleId: CYCLE_ID,
        presidentialRole: "vice_president",
        runId: "manual-run",
      },
    ]);
  });

  it("blocks strict quality-gate imports until focused gaps are repaired or confirmed", async () => {
    const { pool } = poolWithCycle();
    const result = await runManualPresidentialProfileWrite({
      options: options({ strictQualityGate: true }),
      rawPayload: profile({ summary: undefined }),
      pool,
      deps: {
        validateProfile: vi.fn().mockResolvedValue({
          ok: true,
          profile: profile({ summary: undefined }),
          sourceCount: 1,
        }),
      },
    });

    expect(result).toMatchObject({
      ok: false,
      errorCode: "QUALITY_GATE",
    });
    expect(result.ok ? [] : result.gaps).toEqual([
      expect.objectContaining({
        id: "candidate_profile.summary",
        outcome: "needs_repair",
      }),
    ]);
  });

  it("requires a presidential FEC ID for president profiles", async () => {
    const { pool } = poolWithCycle();
    const result = await runManualPresidentialProfileWrite({
      options: options(),
      rawPayload: profile({ fec_ids: undefined }),
      pool,
      deps: {
        validateProfile: vi.fn().mockResolvedValue({
          ok: true,
          profile: profile({ fec_ids: undefined }),
          sourceCount: 1,
        }),
      },
    });

    expect(result).toMatchObject({
      ok: false,
      errorCode: "MISSING_HARD_IDENTIFIER",
      error: "President profile must include at least one presidential FEC ID.",
    });
  });
});
