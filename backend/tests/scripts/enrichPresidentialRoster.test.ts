import { describe, expect, it } from "vitest";

import {
  parseEnrichPresidentialRosterScriptArgs,
  toEnrichPresidentialRosterScriptOutput,
} from "../../src/scripts/enrichPresidentialRoster.js";

describe("parseEnrichPresidentialRosterScriptArgs", () => {
  const now = new Date("2026-06-12T12:00:00.000Z");

  it("parses primary roster enrichment flags", () => {
    expect(
      parseEnrichPresidentialRosterScriptArgs(
        ["--year", "2028", "--party", "Democratic", "--dry-run", "--run-id", "manual-run"],
        now
      )
    ).toEqual({
      electionYear: 2028,
      party: "Democratic",
      stage: "primary",
      dryRun: true,
      runId: "manual-run",
    });
  });

  it("supports equals-style flags and builds a run id", () => {
    const parsed = parseEnrichPresidentialRosterScriptArgs(["--year=2028", "--party=Republican"], now);

    expect(parsed).toMatchObject({
      electionYear: 2028,
      party: "Republican",
      stage: "primary",
      dryRun: false,
    });
    expect(parsed.runId).toBe("presidential_roster:2028:primary:republican:2026-06-12T12:00:00.000Z");
  });

  it("rejects missing party for primary cycles", () => {
    expect(() => parseEnrichPresidentialRosterScriptArgs(["--year=2028"], now)).toThrow(/--party/);
  });

  it("rejects unsupported general-cycle use for v1", () => {
    expect(() =>
      parseEnrichPresidentialRosterScriptArgs(["--year=2028", "--stage=general", "--party=Democratic"], now)
    ).toThrow(/primary cycles only/);
  });

  it("rejects invalid election years", () => {
    expect(() => parseEnrichPresidentialRosterScriptArgs(["--year=2026", "--party=Democratic"], now)).toThrow(
      /Invalid --year/
    );
  });
});

describe("toEnrichPresidentialRosterScriptOutput", () => {
  const startedAt = new Date("2026-06-12T12:00:00.000Z");
  const options = {
    electionYear: 2028,
    party: "Democratic",
    stage: "primary" as const,
    dryRun: true,
    runId: "manual-run",
  };

  it("includes a compact success summary with reconciliation counts", () => {
    const output = toEnrichPresidentialRosterScriptOutput({
      startedAt,
      options,
      result: {
        ok: true,
        cycleId: "cycle-1",
        electionYear: 2028,
        stage: "primary",
        party: "Democratic",
        provider: "claude",
        model: "claude-sonnet-4-6",
        aiCandidateCount: 4,
        matchedCount: 2,
        ambiguousCount: 1,
        unmatchedCount: 1,
        withdrawnSkippedCount: 1,
        withdrawnDemotedCount: 1,
        emittedCount: 2,
        skippedCount: 0,
        dryRun: true,
        admissionPolicy: "fec_confirmed_only",
        statusVerification: {
          checkedCount: 3,
          withdrawnCount: 1,
          activeCount: 1,
          skippedCount: 2,
          demotedCount: 0,
          dryRun: true,
        },
        matches: [],
        aiRawDebug: null,
      },
    });

    expect(output).toMatchObject({
      type: "presidential_roster_enrichment",
      started_at: "2026-06-12T12:00:00.000Z",
      election_year: 2028,
      stage: "primary",
      party: "Democratic",
      dry_run: true,
      run_id: "manual-run",
      summary: {
        ok: true,
        ai_candidate_count: 4,
        matched_count: 2,
        ambiguous_count: 1,
        unmatched_count: 1,
        withdrawn_skipped_count: 1,
        withdrawn_demoted_count: 1,
        emitted_count: 2,
        skipped_count: 0,
        status_verification: {
          checked_count: 3,
          withdrawn_count: 1,
          active_count: 1,
          skipped_count: 2,
          demoted_count: 0,
        },
      },
    });
  });

  it("includes a compact failure summary", () => {
    const output = toEnrichPresidentialRosterScriptOutput({
      startedAt,
      options,
      result: {
        ok: false,
        cycleId: "cycle-1",
        electionYear: 2028,
        stage: "primary",
        party: "Democratic",
        error: "bad payload",
        retryable: false,
        errorCode: "SCHEMA_MISMATCH",
      },
    });

    expect(output).toMatchObject({
      summary: {
        ok: false,
        error_code: "SCHEMA_MISMATCH",
        retryable: false,
      },
    });
  });
});
