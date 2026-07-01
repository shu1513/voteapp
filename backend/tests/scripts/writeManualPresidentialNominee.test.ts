import type { Pool } from "pg";
import { describe, expect, it, vi } from "vitest";

import {
  parseManualPresidentialNomineeScriptArgs,
  runManualPresidentialNomineeWrite,
  type ManualPresidentialNomineeScriptOptions,
} from "../../src/scripts/writeManualPresidentialNominee.js";
import type { PresidentialNomineeCandidateForResolution } from "../../src/pipeline/presidential/presidentialNomineeResolver.js";

const CYCLE_ID = "11111111-1111-4111-8111-111111111111";
const CANDIDATE_ID = "22222222-2222-4222-8222-222222222222";
const GENERAL_CYCLE_ID = "33333333-3333-4333-8333-333333333333";
const NOW = new Date("2026-06-30T12:00:00.000Z");

function options(overrides: Partial<ManualPresidentialNomineeScriptOptions> = {}): ManualPresidentialNomineeScriptOptions {
  return {
    cycleId: CYCLE_ID,
    electionYear: 2028,
    party: "Democratic",
    file: "nominee.json",
    dryRun: false,
    confirmedAt: NOW,
    ...overrides,
  };
}

function candidates(overrides: Partial<PresidentialNomineeCandidateForResolution> = {}): PresidentialNomineeCandidateForResolution[] {
  return [
    {
      candidateId: CANDIDATE_ID,
      displayName: "Jane President",
      party: "Democratic",
      fecIds: ["P80000001"],
      ...overrides,
    },
  ];
}

function loadCycle(overrides: Record<string, unknown> = {}) {
  return vi.fn().mockResolvedValue({
    id: CYCLE_ID,
    election_year: 2028,
    stage: "primary",
    party: "Democratic",
    ...overrides,
  });
}

function payload() {
  return {
    nominee_found: true,
    candidate_name: "Jane President",
    fec_candidate_id: "P80000001",
    sources: ["https://example.org/nominee"],
  };
}

describe("parseManualPresidentialNomineeScriptArgs", () => {
  it("parses required nominee wrapper flags", () => {
    expect(
      parseManualPresidentialNomineeScriptArgs(
        [
          "--cycle-id",
          CYCLE_ID,
          "--election-year",
          "2028",
          "--party",
          "Democratic",
          "--file",
          "nominee.json",
          "--confirmed-at",
          "2026-06-30T12:00:00.000Z",
          "--dry-run",
        ],
        NOW
      )
    ).toEqual(options({ dryRun: true }));
  });

  it("supports aliases and rejects boolean values", () => {
    expect(
      parseManualPresidentialNomineeScriptArgs(
        [
          `--presidential-cycle-id=${CYCLE_ID}`,
          "--year=2028",
          "--party=Republican",
          "--file=nominee.json",
        ],
        NOW
      )
    ).toMatchObject({
      cycleId: CYCLE_ID,
      electionYear: 2028,
      party: "Republican",
      file: "nominee.json",
      dryRun: false,
    });

    expect(() =>
      parseManualPresidentialNomineeScriptArgs(
        [`--cycle-id=${CYCLE_ID}`, "--election-year=2028", "--party=Democratic", "--file=nominee.json", "--dry-run=true"],
        NOW
      )
    ).toThrow("Boolean flag must not include a value: --dry-run");
  });
});

describe("runManualPresidentialNomineeWrite", () => {
  it("resolves a nominee in dry-run mode without promoting", async () => {
    const promoteNominee = vi.fn();

    const result = await runManualPresidentialNomineeWrite({
      options: options({ dryRun: true }),
      rawPayload: payload(),
      pool: {} as Pool,
      deps: {
        loadCycle: loadCycle(),
        loadCandidates: vi.fn().mockResolvedValue(candidates()),
        promoteNominee,
      },
    });

    expect(result).toMatchObject({
      type: "manual_presidential_nominee_write",
      dryRun: true,
      cycleId: CYCLE_ID,
      electionYear: 2028,
      party: "Democratic",
      candidateCount: 1,
      resolution: {
        status: "matched",
        candidateId: CANDIDATE_ID,
        method: "exact_fec_id",
      },
      promotion: null,
      noAiProviderCall: true,
    });
    expect(promoteNominee).not.toHaveBeenCalled();
  });

  it("promotes only when nominee resolution is a clean match", async () => {
    const promoteNominee = vi.fn().mockResolvedValue({
      status: "promoted",
      primaryCycleId: CYCLE_ID,
      generalCycleId: GENERAL_CYCLE_ID,
      nomineeCandidateId: CANDIDATE_ID,
      party: "Democratic",
      sources: ["https://example.org/nominee"],
    });

    const result = await runManualPresidentialNomineeWrite({
      options: options(),
      rawPayload: payload(),
      pool: {} as Pool,
      deps: {
        loadCycle: loadCycle(),
        loadCandidates: vi.fn().mockResolvedValue(candidates()),
        promoteNominee,
      },
    });

    expect(result).toMatchObject({
      dryRun: false,
      resolution: {
        status: "matched",
        candidateId: CANDIDATE_ID,
      },
      promotion: {
        status: "promoted",
        generalCycleId: GENERAL_CYCLE_ID,
      },
    });
    expect(promoteNominee).toHaveBeenCalledWith({
      db: expect.any(Object),
      primaryCycleId: CYCLE_ID,
      electionYear: 2028,
      party: "Democratic",
      resolution: expect.objectContaining({
        status: "matched",
        candidateId: CANDIDATE_ID,
      }),
      confirmedAt: NOW,
    });
  });

  it("does not promote when the resolver returns unmatched", async () => {
    const promoteNominee = vi.fn();

    const result = await runManualPresidentialNomineeWrite({
      options: options(),
      rawPayload: {
        nominee_found: true,
        candidate_name: "Unknown Nominee",
        sources: ["https://example.org/nominee"],
      },
      pool: {} as Pool,
      deps: {
        loadCycle: loadCycle(),
        loadCandidates: vi.fn().mockResolvedValue(candidates()),
        promoteNominee,
      },
    });

    expect(result.resolution).toMatchObject({
      status: "unmatched",
      candidateName: "Unknown Nominee",
    });
    expect(result.promotion).toBeNull();
    expect(promoteNominee).not.toHaveBeenCalled();
  });

  it("does not promote when no nominee has been found", async () => {
    const promoteNominee = vi.fn();

    const result = await runManualPresidentialNomineeWrite({
      options: options(),
      rawPayload: {
        nominee_found: false,
        sources: ["https://example.org/no-nominee"],
      },
      pool: {} as Pool,
      deps: {
        loadCycle: loadCycle(),
        loadCandidates: vi.fn().mockResolvedValue([]),
        promoteNominee,
      },
    });

    expect(result.resolution).toEqual({
      status: "no_nominee_found",
      sources: ["https://example.org/no-nominee"],
    });
    expect(result.candidateCount).toBe(0);
    expect(result.promotion).toBeNull();
    expect(promoteNominee).not.toHaveBeenCalled();
  });

  it("rejects invalid payloads and empty active candidate sets", async () => {
    await expect(
      runManualPresidentialNomineeWrite({
        options: options(),
        rawPayload: { nominee_found: true, sources: ["https://example.org/nominee"] },
        pool: {} as Pool,
      })
    ).rejects.toThrow("Presidential nominee payload failed validation");

    await expect(
      runManualPresidentialNomineeWrite({
        options: options(),
        rawPayload: payload(),
        pool: {} as Pool,
        deps: {
          loadCycle: loadCycle(),
          loadCandidates: vi.fn().mockResolvedValue([]),
        },
      })
    ).rejects.toThrow("No active presidential primary candidates are available for nominee resolution");
  });

  it("rejects election year or party flags that do not match the cycle id", async () => {
    await expect(
      runManualPresidentialNomineeWrite({
        options: options({ electionYear: 2032 }),
        rawPayload: payload(),
        pool: {} as Pool,
        deps: {
          loadCycle: loadCycle(),
          loadCandidates: vi.fn().mockResolvedValue(candidates()),
        },
      })
    ).rejects.toThrow("--election-year (2032) does not match presidential cycle election_year (2028)");

    await expect(
      runManualPresidentialNomineeWrite({
        options: options({ party: "Republican" }),
        rawPayload: payload(),
        pool: {} as Pool,
        deps: {
          loadCycle: loadCycle(),
          loadCandidates: vi.fn().mockResolvedValue(candidates()),
        },
      })
    ).rejects.toThrow("--party (Republican) does not match presidential cycle party (Democratic)");
  });

  it("rejects cycle ids that do not point to a party primary cycle", async () => {
    await expect(
      runManualPresidentialNomineeWrite({
        options: options(),
        rawPayload: payload(),
        pool: {} as Pool,
        deps: {
          loadCycle: loadCycle({ stage: "general", party: null }),
          loadCandidates: vi.fn().mockResolvedValue(candidates()),
        },
      })
    ).rejects.toThrow("manual presidential nominee write requires a primary cycle; cycle stage is general");

    await expect(
      runManualPresidentialNomineeWrite({
        options: options(),
        rawPayload: payload(),
        pool: {} as Pool,
        deps: {
          loadCycle: loadCycle({ party: null }),
          loadCandidates: vi.fn().mockResolvedValue(candidates()),
        },
      })
    ).rejects.toThrow("manual presidential nominee write requires a primary cycle with a party");
  });
});
