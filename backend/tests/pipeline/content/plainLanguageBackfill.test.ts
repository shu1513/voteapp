import { describe, expect, it, vi } from "vitest";
import type { Pool } from "pg";

import {
  mechanicalCheckFailure,
  runPlainLanguageBackfill,
  type PlainLanguageBackfillDeps,
} from "../../../src/pipeline/content/plainLanguageBackfill.js";

describe("mechanicalCheckFailure", () => {
  const original =
    "Announced he would not enforce the county COVID-19 vaccine mandate on department employees, out of step with county policy.";

  it("passes a faithful rewrite", () => {
    expect(
      mechanicalCheckFailure(
        "record_description",
        original,
        "Said he would not make department workers follow the county COVID-19 vaccine rule, which went against county policy."
      )
    ).toBeNull();
  });

  it("rejects an empty rewrite", () => {
    expect(mechanicalCheckFailure("record_description", original, "  ")).toContain("empty");
  });

  it("rejects a rewrite that is too short for its kind", () => {
    expect(mechanicalCheckFailure("record_description", original, "He said no.")).toContain("too short");
  });

  it("allows candidate summaries to shrink further than other kinds", () => {
    const summary =
      "Jane Doe served as county treasurer for a decade. She led the June primary with 42.04% and faces John Roe in the November runoff for Sheriff of Los Angeles County in the November 3, 2026 election.";
    const stripped = "Jane Doe served as county treasurer for ten years.";
    expect(mechanicalCheckFailure("candidate_summary", summary, stripped)).toBeNull();
    expect(mechanicalCheckFailure("record_description", summary, stripped)).toContain("too short");
  });

  it("rejects a rewrite that balloons past the upper bound", () => {
    expect(mechanicalCheckFailure("record_description", "Short claim.", "x".repeat(200))).toContain("too long");
  });

  it("rejects an introduced URL but allows kept URLs", () => {
    const withUrl = "See https://example.com/report for details.";
    expect(mechanicalCheckFailure("record_description", withUrl, "Read https://example.com/report for details.")).toBeNull();
    expect(
      mechanicalCheckFailure("record_description", "See the report for details on this.", "Read https://evil.example/x for details.")
    ).toContain("introduced a URL");
  });

  it("rejects an introduced number but allows dropped or reformatted numbers", () => {
    expect(
      mechanicalCheckFailure("record_description", "The bond is $11.25 billion in total funds.", "The bond totals $11.25 billion.")
    ).toBeNull();
    expect(
      mechanicalCheckFailure("record_description", "The bond is $11,250,000,000 in total funding.", "The bond totals $11250000000.")
    ).toBeNull();
    expect(
      mechanicalCheckFailure("candidate_summary", "She led the primary with 42.04% of the vote.", "She led the earlier vote.")
    ).toBeNull();
    expect(
      mechanicalCheckFailure("record_description", "The budget grew under his tenure a lot.", "The budget grew 300% under his tenure.")
    ).toContain("introduced a number");
  });

  it("licenses digits that spell out number words from the original", () => {
    expect(
      mechanicalCheckFailure(
        "record_description",
        "He was the first challenger in over a century to unseat an incumbent county sheriff.",
        "He was the first person in over 100 years to beat the sitting county sheriff in an election."
      )
    ).toBeNull();
    expect(
      mechanicalCheckFailure(
        "record_description",
        "The measure funds a thousand new housing units in the county over the years.",
        "The measure pays for 1000 new housing units in the county over the years."
      )
    ).toBeNull();
    expect(
      mechanicalCheckFailure(
        "record_description",
        "The budget grew sharply during his tenure at the department, records show.",
        "The budget grew 100-fold during his tenure at the department, records show."
      )
    ).toContain("introduced a number"); // no number word in the original licenses "100"
  });

  it("composes number phrases so a licensed word cannot excuse a changed quantity", () => {
    // "two million" licenses 2 and 2000000 — never a bare 1000000.
    expect(
      mechanicalCheckFailure(
        "record_description",
        "The settlement cost the county two million dollars, according to court records.",
        "The settlement cost the county 1,000,000 dollars, according to court records."
      )
    ).toContain("not in the original: 1000000");
    expect(
      mechanicalCheckFailure(
        "record_description",
        "The settlement cost the county two million dollars, according to court records.",
        "The settlement cost the county 2 million dollars, according to court records."
      )
    ).toBeNull();
    expect(
      mechanicalCheckFailure(
        "record_description",
        "The settlement cost the county two million dollars, according to court records.",
        "The settlement cost the county $2,000,000, according to court records."
      )
    ).toBeNull();
    // "half a million" licenses 500000 — never 50.
    expect(
      mechanicalCheckFailure(
        "record_description",
        "The program spent half a million dollars on outreach efforts in the county.",
        "The program spent 50 million dollars on outreach efforts in the county."
      )
    ).toContain("not in the original: 50");
    expect(
      mechanicalCheckFailure(
        "record_description",
        "The program spent half a million dollars on outreach efforts in the county.",
        "The program spent 500,000 dollars on outreach efforts in the county."
      )
    ).toBeNull();
    // Hyphenated compounds compose: "twenty-five" licenses 25 — never a bare 20.
    expect(
      mechanicalCheckFailure(
        "record_description",
        "He spent twenty-five years working in the county Public Defender's Office.",
        "He spent 25 years working in the county Public Defender's Office."
      )
    ).toBeNull();
    expect(
      mechanicalCheckFailure(
        "record_description",
        "He spent twenty-five years working in the county Public Defender's Office.",
        "He spent 20 years working in the county Public Defender's Office."
      )
    ).toContain("not in the original: 20");
    // Multi-scale chains compose: "two hundred thousand" -> 200000.
    expect(
      mechanicalCheckFailure(
        "record_description",
        "The fund distributed two hundred thousand dollars to local housing groups.",
        "The fund distributed 200,000 dollars to local housing groups."
      )
    ).toBeNull();
    // A bare "half" licenses 0.5 only, never 50.
    expect(
      mechanicalCheckFailure(
        "record_description",
        "More than half the commission members disagreed with the sheriff's decision.",
        "More than 50 of the commission members disagreed with the sheriff's decision."
      )
    ).toContain("not in the original: 50");
  });
});

type FakeQuery = { text: string; params: unknown[] | undefined };

function makeFakePool(options: {
  candidateRows?: Array<Record<string, unknown>>;
  measureRows?: Array<Record<string, unknown>>;
  processedMeasureRows?: Array<Record<string, unknown>>;
  recordRows?: Array<Record<string, unknown>>;
  auditCounts?: Array<{ status: string; count: string }>;
  /** rowCount returned for write statements; 0 simulates the staleness guard rejecting. */
  writeRowCount?: number;
}) {
  const writes: FakeQuery[] = [];
  const pool = {
    query: vi.fn(async (text: string, params?: unknown[]) => {
      if (text.includes("GROUP BY status")) {
        return { rows: options.auditCounts ?? [] };
      }
      if (text.includes("FROM public.candidates c")) {
        return { rows: options.candidateRows ?? [] };
      }
      if (text.includes("FROM public.ballot_measures")) {
        return { rows: options.measureRows ?? [] };
      }
      if (text.includes("WHERE target_table = 'ballot_measures'")) {
        return { rows: options.processedMeasureRows ?? [] };
      }
      if (text.includes("FROM public.candidate_records cr")) {
        return { rows: options.recordRows ?? [] };
      }
      writes.push({ text, params });
      return { rows: [], rowCount: options.writeRowCount ?? 1 };
    }),
  } as unknown as Pool;
  return { pool, writes };
}

function makeDeps(overrides: Partial<PlainLanguageBackfillDeps> = {}): PlainLanguageBackfillDeps {
  return {
    rewrite: vi.fn(async (input) => ({
      ok: true as const,
      provider: "gemini" as const,
      model: "gemini-2.5-flash-lite",
      rewrittenText: `plain: ${input.text}`,
    })),
    verify: vi.fn(async () => ({
      ok: true as const,
      provider: "openai" as const,
      model: "gpt-5.4-mini",
      verdict: "same_facts" as const,
      reason: null,
    })),
    aiConfig: { timeoutMs: 1000 },
    dryRun: false,
    log: () => {},
    ...overrides,
  };
}

const RECORD_TEXT = "Repeatedly rebuffed subpoenas to appear before the county oversight commission during hearings.";

function recordRow(id: string) {
  return { id, description: RECORD_TEXT, source_url: "https://example.com/report", event_date: "2022-12-02" };
}

describe("runPlainLanguageBackfill", () => {
  it("applies a verified rewrite atomically with staleness guard and recomputed record identity key", async () => {
    const { pool, writes } = makeFakePool({ recordRows: [recordRow("r1")] });
    const deps = makeDeps();

    const summary = await runPlainLanguageBackfill(pool, deps);

    expect(summary).toMatchObject({ processed: 1, applied: 1, flagged: 0, staleSkipped: 0, remaining: 0 });
    expect(writes).toHaveLength(1);
    expect(writes[0].text).toContain("SET description = $4");
    expect(writes[0].text).toContain("record_identity_key = $8");
    expect(writes[0].text).toContain("WHERE id = $2 AND description IS NOT DISTINCT FROM $5");
    expect(writes[0].text).toContain("RETURNING id");
    expect(writes[0].text).toContain("SELECT $1, $2, $3, 'applied', $5, $4, NULL, $6, $7 FROM updated");
    expect(writes[0].params?.slice(0, 7)).toEqual([
      "candidate_records",
      "r1",
      "description",
      `plain: ${RECORD_TEXT}`,
      RECORD_TEXT,
      "gemini",
      "gemini-2.5-flash-lite",
    ]);
    expect(writes[0].params?.[7]).toMatch(/^v3_[0-9a-f]{32}$/);
  });

  it("does not touch record_identity_key for non-record targets", async () => {
    const { pool, writes } = makeFakePool({
      measureRows: [
        {
          id: "m1",
          summary: null,
          what_yes_means: "Approves issuing the housing bonds described in the measure text.",
          what_no_means: "",
        },
      ],
    });

    await runPlainLanguageBackfill(pool, makeDeps());

    expect(writes).toHaveLength(1);
    expect(writes[0].text).not.toContain("record_identity_key");
    expect(writes[0].params).toHaveLength(7);
  });

  it("counts a stale row (text changed mid-run) without writing an audit row", async () => {
    const { pool, writes } = makeFakePool({ recordRows: [recordRow("r1")], writeRowCount: 0 });

    const summary = await runPlainLanguageBackfill(pool, makeDeps());

    expect(summary).toMatchObject({ processed: 1, applied: 0, flagged: 0, staleSkipped: 1 });
    expect(writes).toHaveLength(1); // the guarded statement ran but wrote nothing
  });

  it("passes the rewriter provider to the verifier", async () => {
    const { pool } = makeFakePool({ recordRows: [recordRow("r1")] });
    const deps = makeDeps();

    await runPlainLanguageBackfill(pool, deps);

    expect(deps.verify).toHaveBeenCalledWith(expect.anything(), expect.anything(), "gemini");
  });

  it("halts immediately when prior audit history already exceeds the flag rate", async () => {
    const { pool } = makeFakePool({
      recordRows: [recordRow("r1"), recordRow("r2")],
      auditCounts: [
        { status: "applied", count: "50" },
        { status: "flagged", count: "10" },
      ],
    });

    await expect(runPlainLanguageBackfill(pool, makeDeps())).rejects.toThrow("halting: flag rate");
  });

  it("flags a verifier mismatch, keeps the column untouched, and records the reason", async () => {
    const { pool, writes } = makeFakePool({ recordRows: [recordRow("r1")] });
    const deps = makeDeps({
      verify: vi.fn(async () => ({
        ok: true as const,
        provider: "openai" as const,
        model: "gpt-5.4-mini",
        verdict: "mismatch" as const,
        reason: "dropped the negation",
      })),
    });

    const summary = await runPlainLanguageBackfill(pool, deps);

    expect(summary).toMatchObject({ processed: 1, applied: 0, flagged: 1 });
    expect(writes).toHaveLength(1);
    expect(writes[0].text).not.toContain("UPDATE");
    expect(writes[0].text).toContain("'flagged'");
    expect(writes[0].params?.[5]).toBe("verifier mismatch: dropped the negation");
  });

  it("skips the verifier call entirely when the mechanical check fails", async () => {
    const { pool, writes } = makeFakePool({ recordRows: [recordRow("r1")] });
    const verify = vi.fn();
    const deps = makeDeps({
      rewrite: vi.fn(async () => ({
        ok: true as const,
        provider: "gemini" as const,
        model: "gemini-2.5-flash-lite",
        rewrittenText: "Too short.",
      })),
      verify: verify as unknown as PlainLanguageBackfillDeps["verify"],
    });

    const summary = await runPlainLanguageBackfill(pool, deps);

    expect(summary).toMatchObject({ applied: 0, flagged: 1 });
    expect(verify).not.toHaveBeenCalled();
    expect(writes[0].text).toContain("'flagged'");
  });

  it("aborts without an audit row when the rewrite provider call fails, so resume retries", async () => {
    const { pool, writes } = makeFakePool({ recordRows: [recordRow("r1")] });
    const deps = makeDeps({
      rewrite: vi.fn(async () => ({ ok: false as const, reason: "rate limited" })),
    });

    await expect(runPlainLanguageBackfill(pool, deps)).rejects.toThrow("rewrite call failed");
    expect(writes).toHaveLength(0);
  });

  it("writes nothing in dry-run mode but reports the would-be outcome", async () => {
    const { pool, writes } = makeFakePool({ recordRows: [recordRow("r1")] });
    const lines: string[] = [];
    const deps = makeDeps({ dryRun: true, log: (line) => lines.push(line) });

    const summary = await runPlainLanguageBackfill(pool, deps);

    expect(summary).toMatchObject({ processed: 1, applied: 1, dryRun: true });
    expect(writes).toHaveLength(0);
    expect(lines.join("\n")).toContain("would apply");
    expect(lines.join("\n")).toContain("before:");
    expect(lines.join("\n")).toContain("after:");
  });

  it("halts when the flag rate exceeds 5% after the minimum sample", async () => {
    const rows = Array.from({ length: 60 }, (_, index) => recordRow(`r${index}`));
    const { pool } = makeFakePool({ recordRows: rows });
    let calls = 0;
    const deps = makeDeps({
      verify: vi.fn(async () => {
        calls += 1;
        // Every 10th rewrite mismatches: by row 40 that is 4/40 = 10% > 5%.
        return calls % 10 === 0
          ? {
              ok: true as const,
              provider: "openai" as const,
              model: "gpt-5.4-mini",
              verdict: "mismatch" as const,
              reason: "changed a fact",
            }
          : {
              ok: true as const,
              provider: "openai" as const,
              model: "gpt-5.4-mini",
              verdict: "same_facts" as const,
              reason: null,
            };
      }),
    });

    await expect(runPlainLanguageBackfill(pool, deps)).rejects.toThrow("halting: flag rate");
  });

  it("respects --limit and reports the remainder", async () => {
    const rows = Array.from({ length: 5 }, (_, index) => recordRow(`r${index}`));
    const { pool, writes } = makeFakePool({ recordRows: rows });
    const deps = makeDeps({ limit: 2 });

    const summary = await runPlainLanguageBackfill(pool, deps);

    expect(summary).toMatchObject({ processed: 2, applied: 2, remaining: 3 });
    expect(writes).toHaveLength(2);
  });

  it("passes contest context through for candidate summaries and skips processed measure columns", async () => {
    const rewrite = vi.fn(async (input: { text: string }) => ({
      ok: true as const,
      provider: "gemini" as const,
      model: "gemini-2.5-flash-lite",
      rewrittenText: `plain: ${input.text}`,
    }));
    const { pool, writes } = makeFakePool({
      candidateRows: [
        {
          id: "c1",
          summary: "Jane Doe served as county treasurer for a decade before this campaign season began.",
          official_ballot_title: "Sheriff",
          district_name: "Los Angeles County, California",
          election_date: "2026-11-03",
        },
      ],
      measureRows: [
        {
          id: "m1",
          summary: "Authorizes bonds for affordable housing programs statewide.",
          what_yes_means: "Approves issuing the bonds described in the measure.",
          what_no_means: "Rejects the bond authorization described in the measure.",
        },
      ],
      processedMeasureRows: [
        { target_id: "m1", target_column: "summary" },
        { target_id: "m1", target_column: "what_yes_means" },
      ],
    });
    const deps = makeDeps({ rewrite: rewrite as unknown as PlainLanguageBackfillDeps["rewrite"] });

    const summary = await runPlainLanguageBackfill(pool, deps);

    // c1 summary + only the unprocessed measure column (what_no_means).
    expect(summary).toMatchObject({ processed: 2, applied: 2 });
    expect(rewrite).toHaveBeenCalledWith(
      expect.objectContaining({
        kind: "candidate_summary",
        contestContext: {
          officialBallotTitle: "Sheriff",
          districtName: "Los Angeles County, California",
          electionDate: "2026-11-03",
        },
      }),
      expect.anything()
    );
    expect(rewrite).toHaveBeenCalledWith(
      expect.objectContaining({ kind: "measure_what_no_means" }),
      expect.anything()
    );
    expect(
      writes.some(
        (write) => write.text.includes("UPDATE public.ballot_measures") && write.text.includes("SET what_no_means = $4")
      )
    ).toBe(true);
  });
});
