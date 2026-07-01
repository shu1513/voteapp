import type { Pool } from "pg";
import { describe, expect, it, vi } from "vitest";

import {
  buildManualPresidentialRosterEnrichResult,
  parseManualPresidentialRosterScriptArgs,
  runManualPresidentialRosterWrite,
  toManualPresidentialRosterScriptOutput,
  type ManualPresidentialRosterScriptOptions,
} from "../../src/scripts/writeManualPresidentialRoster.js";
import type {
  PresidentialRosterEnricherInput,
  PresidentialRosterEnricherResult,
} from "../../src/pipeline/enrichers/presidentialRosterEnricher.js";

const CYCLE_ID = "00000000-0000-4000-8000-000000000001";
const NOW = new Date("2026-06-30T12:00:00.000Z");

function options(
  overrides: Partial<ManualPresidentialRosterScriptOptions> = {}
): ManualPresidentialRosterScriptOptions {
  return {
    cycleId: CYCLE_ID,
    electionYear: 2028,
    party: "Democratic",
    file: "roster.json",
    dryRun: true,
    runId: "manual-run",
    ...overrides,
  };
}

function payload() {
  return {
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
    ],
  };
}

function poolWithCycle(overrides: Record<string, unknown> = {}): Pool {
  return {
    query: vi.fn().mockResolvedValue({
      rows: [
        {
          id: CYCLE_ID,
          election_year: 2028,
          stage: "primary",
          party: "Democratic",
          ...overrides,
        },
      ],
      rowCount: 1,
    }),
  } as unknown as Pool;
}

describe("parseManualPresidentialRosterScriptArgs", () => {
  it("parses the required manual presidential roster flags", () => {
    expect(
      parseManualPresidentialRosterScriptArgs(
        [
          "--cycle-id",
          CYCLE_ID,
          "--election-year",
          "2028",
          "--party",
          "Democratic",
          "--file",
          "roster.json",
          "--dry-run",
          "--run-id",
          "manual-run",
        ],
        NOW
      )
    ).toEqual(options());
  });

  it("supports equals-style values and builds a run id", () => {
    const parsed = parseManualPresidentialRosterScriptArgs(
      [
        `--cycle-id=${CYCLE_ID}`,
        "--election-year=2028",
        "--party=Republican",
        "--file=roster.json",
      ],
      NOW
    );

    expect(parsed).toMatchObject({
      cycleId: CYCLE_ID,
      electionYear: 2028,
      party: "Republican",
      file: "roster.json",
      dryRun: false,
    });
    expect(parsed.runId).toBe("manual_presidential_roster:2028:primary:republican:2026-06-30T12:00:00.000Z");
  });

  it("rejects boolean flags with explicit values", () => {
    expect(() =>
      parseManualPresidentialRosterScriptArgs(
        [
          `--cycle-id=${CYCLE_ID}`,
          "--election-year=2028",
          "--party=Democratic",
          "--file=roster.json",
          "--dry-run=true",
        ],
        NOW
      )
    ).toThrow("Boolean flag must not include a value: --dry-run");
    expect(() =>
      parseManualPresidentialRosterScriptArgs(
        [
          `--cycle-id=${CYCLE_ID}`,
          "--election-year=2028",
          "--party=Democratic",
          "--file=roster.json",
          "--dry-run",
          "true",
        ],
        NOW
      )
    ).toThrow("Boolean flag must not include a value: --dry-run");
  });
});

describe("buildManualPresidentialRosterEnrichResult", () => {
  it("returns a manual provider result without calling an AI provider", () => {
    expect(
      buildManualPresidentialRosterEnrichResult({
        payload: payload(),
        file: "roster.json",
      })
    ).toMatchObject({
      ok: true,
      provider: "manual",
      model: "manual-research:codex",
      candidates: payload().candidates,
      aiRawDebug: {
        manual_research: true,
        no_ai_provider_call: true,
        source_file: "roster.json",
        candidate_count: 1,
      },
    });
  });
});

describe("runManualPresidentialRosterWrite", () => {
  it("validates the payload and injects manual roster/status providers into the existing enricher", async () => {
    let capturedInput: PresidentialRosterEnricherInput | null = null;
    const enrichRosterCycle = vi.fn(
      async (input: PresidentialRosterEnricherInput): Promise<PresidentialRosterEnricherResult> => {
        capturedInput = input;
        const rosterResult = await input.enrichRoster!(
          {
            cycleId: CYCLE_ID,
            electionYear: 2028,
            stage: "primary",
            party: "Democratic",
          },
          { timeoutMs: 1 }
        );
        const statusResult = await input.enrichRosterStatus!(
          {
            cycleId: CYCLE_ID,
            electionYear: 2028,
            stage: "primary",
            party: "Democratic",
            candidates: [
              {
                candidateId: "candidate-existing",
                displayName: "Existing Candidate",
                party: "Democratic",
                fecIds: ["P80000002"],
                sources: ["https://example.org/existing"],
              },
            ],
          },
          { timeoutMs: 1 }
        );

        expect(rosterResult).toMatchObject({
          ok: true,
          provider: "manual",
          model: "manual-research:codex",
          candidates: payload().candidates,
        });
        expect(statusResult).toMatchObject({
          ok: true,
          provider: "manual",
          model: "manual-research:codex",
          candidates: [
            {
              candidate_id: "candidate-existing",
              status: "active",
            },
          ],
        });
        expect(input.loadActiveCandidatesForReconciliation).toBeUndefined();

        return {
          ok: true,
          cycleId: CYCLE_ID,
          electionYear: 2028,
          stage: "primary",
          party: "Democratic",
          provider: "manual",
          model: "manual-research:codex",
          aiCandidateCount: 1,
          matchedCount: 1,
          ambiguousCount: 0,
          unmatchedCount: 0,
          withdrawnSkippedCount: 0,
          withdrawnDemotedCount: 0,
          emittedCount: 1,
          skippedCount: 0,
          dryRun: true,
          admissionPolicy: "fec_confirmed_only",
          statusVerification: {
            checkedCount: 0,
            withdrawnCount: 0,
            activeCount: 0,
            skippedCount: 0,
            demotedCount: 0,
            dryRun: true,
          },
          matches: [],
          aiRawDebug: null,
        };
      }
    );

    const result = await runManualPresidentialRosterWrite({
      options: options(),
      rawPayload: payload(),
      pool: poolWithCycle(),
      redis: { sendCommand: vi.fn() },
      enrichRosterCycle,
    });

    expect(result).toMatchObject({
      ok: true,
      matchedCount: 1,
      emittedCount: 1,
    });
    expect(enrichRosterCycle).toHaveBeenCalledTimes(1);
    expect(capturedInput).toMatchObject({
      cycleId: CYCLE_ID,
      electionYear: 2028,
      stage: "primary",
      party: "Democratic",
      runId: "manual-run",
      dryRun: true,
      aiConfig: { timeoutMs: 90000 },
    });
  });

  it("returns a schema failure when the payload contract rejects the file", async () => {
    const result = await runManualPresidentialRosterWrite({
      options: options(),
      rawPayload: {
        candidates: [
          {
            display_name: "FEC Only",
            party: "Democratic",
            fec_candidate_id: "P80000001",
            sources: ["https://www.fec.gov/data/candidate/P80000001"],
            qualification_evidence: [
              {
                kind: "official_campaign_website",
                source_url: "https://www.fec.gov/data/candidate/P80000001",
              },
            ],
            status: "active",
          },
        ],
      },
      pool: poolWithCycle(),
      redis: { sendCommand: vi.fn() },
      enrichRosterCycle: vi.fn(),
    });

    expect(result).toMatchObject({
      ok: false,
      errorCode: "SCHEMA_MISMATCH",
      error: expect.stringContaining("candidate.qualification_evidence"),
    });
  });

  it("rejects a cycle id that does not match the requested party", async () => {
    await expect(
      runManualPresidentialRosterWrite({
        options: options(),
        rawPayload: payload(),
        pool: poolWithCycle({ party: "Republican" }),
        redis: { sendCommand: vi.fn() },
        enrichRosterCycle: vi.fn(),
      })
    ).rejects.toThrow("--party (Democratic) does not match presidential cycle party (Republican)");
  });
});

describe("toManualPresidentialRosterScriptOutput", () => {
  it("reports matched, ambiguous, unmatched, and emitted counts", () => {
    const output = toManualPresidentialRosterScriptOutput({
      startedAt: NOW,
      options: options(),
      result: {
        ok: true,
        cycleId: CYCLE_ID,
        electionYear: 2028,
        stage: "primary",
        party: "Democratic",
        provider: "manual",
        model: "manual-research:codex",
        aiCandidateCount: 4,
        matchedCount: 2,
        ambiguousCount: 1,
        unmatchedCount: 1,
        withdrawnSkippedCount: 0,
        withdrawnDemotedCount: 0,
        emittedCount: 2,
        skippedCount: 0,
        dryRun: true,
        admissionPolicy: "fec_confirmed_only",
        statusVerification: {
          checkedCount: 0,
          withdrawnCount: 0,
          activeCount: 0,
          skippedCount: 0,
          demotedCount: 0,
          dryRun: true,
        },
        matches: [],
        aiRawDebug: null,
      },
    });

    expect(output).toMatchObject({
      type: "manual_presidential_roster_write",
      cycle_id: CYCLE_ID,
      no_ai_provider_call: true,
      summary: {
        ok: true,
        candidate_count: 4,
        matched_count: 2,
        ambiguous_count: 1,
        unmatched_count: 1,
        emitted_count: 2,
      },
    });
  });
});
