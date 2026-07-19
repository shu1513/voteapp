import { describe, expect, it } from "vitest";

import {
  COHORT_ENTRY_COUNT,
  readSweepEvidenceShape,
  runSweepConfirmationReset,
  type SweepConfirmationCohortRow,
  type SweepConfirmationResetClient,
} from "../../src/scripts/resetCandidateRecordSweepConfirmations.js";

const TEMPLATE_QUESTIONS = [
  "Major official actions / roll-call votes / sponsored legislation?",
  "Organizations/committees/advocacy?",
  "Court, ethics, or regulatory proceedings?",
  "Endorsements made/received?",
];

function templateEvidence(questionIds?: readonly string[]) {
  return {
    entries: TEMPLATE_QUESTIONS.map((question, index) => ({
      question,
      finding: "Nothing writeable found in accessible sources.",
      ...(questionIds?.[index] ? { question_id: questionIds[index] } : {}),
    })),
  };
}

function cohortRow(
  overrides: Partial<SweepConfirmationCohortRow> & { candidate_id: string }
): SweepConfirmationCohortRow {
  return {
    display_name: `Candidate ${overrides.candidate_id}`,
    confirmed_at: "2026-07-15 14:00:00-07",
    evidence: templateEvidence(),
    candidate_retired: false,
    active_claim: false,
    record_count: 0,
    ...overrides,
  };
}

// Answers the locking cohort SELECT with `rows`, the DELETE with one returned
// row per requested candidate id, the stamp-clear UPDATE likewise, and
// records every statement so assertions can pin the exact transaction shape.
function fakeClient(rows: SweepConfirmationCohortRow[]): {
  client: SweepConfirmationResetClient;
  statements: { text: string; values?: unknown[] }[];
} {
  const statements: { text: string; values?: unknown[] }[] = [];
  const client: SweepConfirmationResetClient = {
    async query<T>(text: string, values?: unknown[]): Promise<{ rows: T[] }> {
      statements.push({ text, values });
      if (text.includes("FOR UPDATE")) {
        return { rows: rows as T[] };
      }
      if (text.includes("DELETE FROM public.candidate_record_sweep_confirmations")) {
        const ids = values?.[0] as string[];
        return { rows: ids.map((candidate_id) => ({ candidate_id })) as T[] };
      }
      if (text.includes("UPDATE public.candidates")) {
        const ids = values?.[0] as string[];
        return { rows: ids.map((id) => ({ id })) as T[] };
      }
      return { rows: [] as T[] };
    },
  };
  return { client, statements };
}

function options(
  overrides: Partial<Parameters<typeof runSweepConfirmationReset>[1]> = {}
) {
  return {
    confirmedFrom: "2026-07-15",
    confirmedTo: "2026-07-16",
    expectedTotal: null,
    dryRun: true,
    ...overrides,
  };
}

function statementKinds(statements: { text: string }[]): string[] {
  return statements.map((statement) => statement.text.trim().split(/\s/)[0]!);
}

describe("readSweepEvidenceShape", () => {
  it("reads the raw entry count and question snippets", () => {
    const shape = readSweepEvidenceShape(templateEvidence());
    expect(shape.entryCount).toBe(COHORT_ENTRY_COUNT);
    expect(shape.hasQuestionIdTags).toBe(false);
    expect(shape.questions[0]).toBe(TEMPLATE_QUESTIONS[0]!.slice(0, 60));
  });

  it("detects question_id tags on any entry", () => {
    const tagged = templateEvidence(["career", "orgs_advocacy", "court_legal", "endorsements"]);
    expect(readSweepEvidenceShape(tagged).hasQuestionIdTags).toBe(true);
    const partiallyTagged = templateEvidence(["career"]);
    expect(readSweepEvidenceShape(partiallyTagged).hasQuestionIdTags).toBe(true);
  });

  it("treats unparseable evidence as no entries rather than throwing", () => {
    expect(readSweepEvidenceShape(null).entryCount).toBeNull();
    expect(readSweepEvidenceShape("nope").entryCount).toBeNull();
    expect(readSweepEvidenceShape({ entries: "nope" }).entryCount).toBeNull();
    expect(readSweepEvidenceShape({ entries: ["nope"] })).toEqual({
      entryCount: 1,
      hasQuestionIdTags: false,
      questions: ["(malformed entry)"],
    });
  });
});

describe("runSweepConfirmationReset", () => {
  it("dry-run classifies the cohort, reports signatures, and rolls back without writes", async () => {
    const rows = [
      cohortRow({ candidate_id: "a", record_count: 0 }),
      cohortRow({ candidate_id: "b", record_count: 3 }),
      cohortRow({ candidate_id: "c", record_count: 0 }),
    ];
    const { client, statements } = fakeClient(rows);

    const result = await runSweepConfirmationReset(client, options());

    expect(result).toMatchObject({
      dryRun: true,
      windowRowCount: 3,
      resettable: { total: 3, zeroRecordCount: 2, withRecordsCount: 1 },
      deletedConfirmations: 0,
      clearedStamps: 0,
    });
    expect(result.resettable.zeroRecordSample.map((c) => c.candidateId)).toEqual(["a", "c"]);
    expect(result.resettable.withRecordsSample.map((c) => c.candidateId)).toEqual(["b"]);
    expect(result.questionSignatures).toEqual([
      { questions: TEMPLATE_QUESTIONS.map((q) => q.slice(0, 60)), count: 3 },
    ]);
    expect(statementKinds(statements)).toEqual(["BEGIN", "SELECT", "ROLLBACK"]);
  });

  it("live run deletes all resettable confirmations but clears stamps only for zero-record candidates", async () => {
    const rows = [
      cohortRow({ candidate_id: "a", record_count: 0 }),
      cohortRow({ candidate_id: "b", record_count: 3 }),
      cohortRow({ candidate_id: "c", record_count: 0 }),
    ];
    const { client, statements } = fakeClient(rows);

    const result = await runSweepConfirmationReset(
      client,
      options({ dryRun: false, expectedTotal: 3 })
    );

    expect(result).toMatchObject({
      dryRun: false,
      deletedConfirmations: 3,
      clearedStamps: 2,
    });
    const deleteStatement = statements.find((s) => s.text.includes("DELETE FROM"));
    expect(deleteStatement?.values).toEqual([["a", "b", "c"]]);
    const updateStatement = statements.find((s) => s.text.includes("UPDATE public.candidates"));
    expect(updateStatement?.values).toEqual([["a", "c"]]);
    for (const column of [
      "last_records_searched_at = NULL",
      "last_records_researched_through = NULL",
    ]) {
      expect(updateStatement?.text).toContain(column);
    }
    expect(statementKinds(statements)).toEqual(["BEGIN", "SELECT", "DELETE", "UPDATE", "COMMIT"]);
  });

  it("skips the stamp-clear statement entirely when every resettable candidate has records", async () => {
    const rows = [cohortRow({ candidate_id: "b", record_count: 3 })];
    const { client, statements } = fakeClient(rows);

    const result = await runSweepConfirmationReset(
      client,
      options({ dryRun: false, expectedTotal: 1 })
    );

    expect(result).toMatchObject({ deletedConfirmations: 1, clearedStamps: 0 });
    expect(statementKinds(statements)).toEqual(["BEGIN", "SELECT", "DELETE", "COMMIT"]);
  });

  it("refuses a live run without --expected-total before touching the database", async () => {
    const { client, statements } = fakeClient([cohortRow({ candidate_id: "a" })]);

    await expect(
      runSweepConfirmationReset(client, options({ dryRun: false }))
    ).rejects.toThrow(/--expected-total is required for a live run/);
    expect(statements).toEqual([]);
  });

  it("refuses when --expected-total does not match the live resettable count", async () => {
    const { client, statements } = fakeClient([
      cohortRow({ candidate_id: "a" }),
      cohortRow({ candidate_id: "b" }),
    ]);

    await expect(
      runSweepConfirmationReset(client, options({ dryRun: false, expectedTotal: 5 }))
    ).rejects.toThrow(
      /--expected-total 5 does not match the live resettable count 2/
    );
    expect(statementKinds(statements)).toEqual(["BEGIN", "SELECT", "ROLLBACK"]);
  });

  it("excludes tagged and non-4-entry ledgers from the reset as shape mismatches", async () => {
    const tagged = cohortRow({
      candidate_id: "tagged",
      evidence: templateEvidence(["career", "orgs_advocacy", "court_legal", "endorsements"]),
    });
    const threeEntry = cohortRow({
      candidate_id: "three",
      evidence: {
        entries: TEMPLATE_QUESTIONS.slice(0, 3).map((question) => ({
          question,
          finding: "nothing",
        })),
      },
    });
    const malformed = cohortRow({ candidate_id: "malformed", evidence: null });
    const poisoned = cohortRow({ candidate_id: "poisoned" });
    const { client, statements } = fakeClient([tagged, threeEntry, malformed, poisoned]);

    const result = await runSweepConfirmationReset(
      client,
      options({ dryRun: false, expectedTotal: 1 })
    );

    expect(result.skipped.shapeMismatchCount).toBe(3);
    expect(result.skipped.shapeMismatchSample.map((c) => c.candidateId)).toEqual([
      "tagged",
      "three",
      "malformed",
    ]);
    const deleteStatement = statements.find((s) => s.text.includes("DELETE FROM"));
    expect(deleteStatement?.values).toEqual([["poisoned"]]);
  });

  it("skips retired candidates and active claims, reporting them separately", async () => {
    const retired = cohortRow({ candidate_id: "retired", candidate_retired: true });
    const claimed = cohortRow({ candidate_id: "claimed", active_claim: true });
    const poisoned = cohortRow({ candidate_id: "poisoned" });
    const { client, statements } = fakeClient([retired, claimed, poisoned]);

    const result = await runSweepConfirmationReset(
      client,
      options({ dryRun: false, expectedTotal: 1 })
    );

    expect(result.skipped).toMatchObject({
      retiredCandidateCount: 1,
      activeClaimCount: 1,
      shapeMismatchCount: 0,
    });
    const deleteStatement = statements.find((s) => s.text.includes("DELETE FROM"));
    expect(deleteStatement?.values).toEqual([["poisoned"]]);
  });

  it("rolls back and reports nothing to do when the window has no resettable rows", async () => {
    const { client, statements } = fakeClient([
      cohortRow({ candidate_id: "tagged", evidence: templateEvidence(["career"]) }),
    ]);

    const result = await runSweepConfirmationReset(
      client,
      options({ dryRun: false, expectedTotal: 0 })
    );

    expect(result).toMatchObject({
      resettable: { total: 0 },
      deletedConfirmations: 0,
      clearedStamps: 0,
    });
    expect(statementKinds(statements)).toEqual(["BEGIN", "SELECT", "ROLLBACK"]);
  });

  it("rolls back when the DELETE affects a different row count than classified", async () => {
    const rows = [cohortRow({ candidate_id: "a" }), cohortRow({ candidate_id: "b" })];
    const statements: { text: string; values?: unknown[] }[] = [];
    const client: SweepConfirmationResetClient = {
      async query<T>(text: string, values?: unknown[]): Promise<{ rows: T[] }> {
        statements.push({ text, values });
        if (text.includes("FOR UPDATE")) {
          return { rows: rows as T[] };
        }
        if (text.includes("DELETE FROM")) {
          return { rows: [{ candidate_id: "a" }] as T[] };
        }
        return { rows: [] as T[] };
      },
    };

    await expect(
      runSweepConfirmationReset(client, options({ dryRun: false, expectedTotal: 2 }))
    ).rejects.toThrow(/Deleted 1 confirmations but expected 2; rolled back/);
    expect(statements.at(-1)?.text).toBe("ROLLBACK");
  });

  it("refuses an inverted date window before opening a transaction", async () => {
    const { client, statements } = fakeClient([]);

    await expect(
      runSweepConfirmationReset(
        client,
        options({ confirmedFrom: "2026-07-16", confirmedTo: "2026-07-15" })
      )
    ).rejects.toThrow(/--confirmed-from 2026-07-16 is after --confirmed-to 2026-07-15/);
    expect(statements).toEqual([]);
  });

  it("passes the date window and claim lease to the locking cohort query", async () => {
    const { client, statements } = fakeClient([]);

    await runSweepConfirmationReset(client, options());

    const select = statements.find((s) => s.text.includes("FOR UPDATE"));
    expect(select?.values).toEqual(["2026-07-15", "2026-07-16", 2]);
    expect(select?.text).toContain("FOR UPDATE OF sc, c");
    expect(select?.text).toContain("sc.confirmed_at >= $1::date");
    expect(select?.text).toContain("sc.confirmed_at < $2::date + 1");
  });
});
