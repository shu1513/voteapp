import { describe, expect, it } from "vitest";

import {
  AUGUST_21_TEMPLATE_FINDINGS,
  COHORT_ENTRY_COUNT,
  isAugust21TemplateLedger,
  isRecordsRetiredOutLedger,
  isRouteCoverageGapLedger,
  matchesResetCohort,
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
    context_type: "election",
    context_id: `00000000-0000-0000-0000-${overrides.candidate_id.padStart(12, "0")}`,
    display_name: `Candidate ${overrides.candidate_id}`,
    confirmed_at: "2026-07-15 14:00:00-07",
    evidence: templateEvidence(),
    candidate_retired: false,
    active_claim: false,
    record_count: 0,
    retired_record_count: 0,
    covers_latest_search: true,
    confirmed_gap_ids: [],
    has_covering_no_records_claim: false,
    has_held_public_office: true,
    discovery_contest_family: "non_judicial_office",
    context_election_found: true,
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
    cohort: "july-15-untagged" as const,
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

  it("live run deletes all resettable confirmations and clears stamps for every one of them", async () => {
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
      resettable: { total: 3, zeroRecordCount: 2, withRecordsCount: 1 },
      deletedConfirmations: 3,
      clearedStamps: 3,
    });
    const deleteStatement = statements.find((s) => s.text.includes("DELETE FROM"));
    expect(deleteStatement?.values).toEqual([
      ["a", "b", "c"],
      ["election", "election", "election"],
      [
        "00000000-0000-0000-0000-00000000000a",
        "00000000-0000-0000-0000-00000000000b",
        "00000000-0000-0000-0000-00000000000c",
      ],
    ]);
    const updateStatement = statements.find((s) => s.text.includes("UPDATE public.candidates"));
    expect(updateStatement?.values).toEqual([["a", "b", "c"]]);
    for (const column of [
      "last_records_searched_at = NULL",
      "last_records_researched_through = NULL",
    ]) {
      expect(updateStatement?.text).toContain(column);
    }
    expect(statementKinds(statements)).toEqual(["BEGIN", "SELECT", "DELETE", "UPDATE", "COMMIT"]);
  });

  it("clears stamps for a records-holding candidate so it stays visible to repair", async () => {
    // Regression: with the confirmation deleted, a stamped candidate WITH
    // records is invisible to the audit suspect list (zero-record only), the
    // confirmation detectors, and the unstamped backlog — the stamp must
    // clear or the reset would end its repair permanently.
    const rows = [cohortRow({ candidate_id: "b", record_count: 3 })];
    const { client, statements } = fakeClient(rows);

    const result = await runSweepConfirmationReset(
      client,
      options({ dryRun: false, expectedTotal: 1 })
    );

    expect(result).toMatchObject({ deletedConfirmations: 1, clearedStamps: 1 });
    const updateStatement = statements.find((s) => s.text.includes("UPDATE public.candidates"));
    expect(updateStatement?.values).toEqual([["b"]]);
    expect(statementKinds(statements)).toEqual(["BEGIN", "SELECT", "DELETE", "UPDATE", "COMMIT"]);
  });

  it("deletes exact context rows but clears a duplicate candidate's stamp once", async () => {
    const rows = [
      cohortRow({
        candidate_id: "a",
        context_id: "11111111-1111-1111-1111-111111111111",
      }),
      cohortRow({
        candidate_id: "a",
        context_id: "22222222-2222-2222-2222-222222222222",
      }),
    ];
    const { client, statements } = fakeClient(rows);

    const result = await runSweepConfirmationReset(
      client,
      options({ dryRun: false, expectedTotal: 2 })
    );

    expect(result).toMatchObject({ deletedConfirmations: 2, clearedStamps: 1 });
    const deleteStatement = statements.find((s) => s.text.includes("DELETE FROM"));
    expect(deleteStatement?.values).toEqual([
      ["a", "a"],
      ["election", "election"],
      ["11111111-1111-1111-1111-111111111111", "22222222-2222-2222-2222-222222222222"],
    ]);
    const updateStatement = statements.find((s) => s.text.includes("UPDATE public.candidates"));
    expect(updateStatement?.values).toEqual([["a"]]);
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
    expect(deleteStatement?.values).toEqual([
      ["poisoned"],
      ["election"],
      ["00000000-0000-0000-0000-0000poisoned"],
    ]);
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
    expect(deleteStatement?.values).toEqual([
      ["poisoned"],
      ["election"],
      ["00000000-0000-0000-0000-0000poisoned"],
    ]);
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
    expect(select?.values).toEqual(["2026-07-15", "2026-07-16", 2, null]);
    expect(select?.text).toContain("FOR UPDATE OF sc, c");
    expect(select?.text).toContain("$4::uuid[] IS NULL");
    expect(select?.text).toContain("sc.confirmed_at >= $1::date");
    expect(select?.text).toContain("sc.confirmed_at < $2::date + 1");
  });
});

const AUGUST_FINDINGS = [...AUGUST_21_TEMPLATE_FINDINGS];

function augustEvidence(findings: readonly string[] = AUGUST_FINDINGS) {
  return {
    entries: findings.map((finding, index) => ({
      question: `Question ${index}`,
      question_id: `q${index}`,
      finding,
    })),
  };
}

describe("august-21-template cohort", () => {
  it("matches a ledger whose every finding is a template sentence", () => {
    expect(AUGUST_21_TEMPLATE_FINDINGS.size).toBe(7);
    expect(isAugust21TemplateLedger(augustEvidence())).toBe(true);
    expect(isAugust21TemplateLedger(augustEvidence(AUGUST_FINDINGS.map((f) => ` ${f} `)))).toBe(
      true
    );
  });

  it("keeps a ledger with any candidate-specific finding", () => {
    const mixed = augustEvidence([
      ...AUGUST_FINDINGS.slice(0, 6),
      "No additional dated substantive roll-call action found in the focused Volusia Council minutes search.",
    ]);
    expect(isAugust21TemplateLedger(mixed)).toBe(false);
  });

  it("rejects empty or malformed evidence", () => {
    for (const evidence of [
      null,
      "nope",
      {},
      { entries: [] },
      { entries: ["nope"] },
      { entries: [{ question: "q" }] },
    ]) {
      expect(isAugust21TemplateLedger(evidence)).toBe(false);
    }
  });

  it("keeps the two cohorts disjoint", () => {
    expect(matchesResetCohort(templateEvidence(), "august-21-template")).toBe(false);
    expect(matchesResetCohort(augustEvidence(), "july-15-untagged")).toBe(false);
    expect(matchesResetCohort(templateEvidence(), "july-15-untagged")).toBe(true);
    expect(matchesResetCohort(augustEvidence(), "august-21-template")).toBe(true);
  });

  it("live run resets only pure template ledgers and clears their stamps", async () => {
    const pure = cohortRow({ candidate_id: "pure", evidence: augustEvidence(), record_count: 2 });
    const mixed = cohortRow({
      candidate_id: "mixed",
      evidence: augustEvidence([...AUGUST_FINDINGS.slice(0, 6), "Board minutes reviewed; no vote found."]),
    });
    const july = cohortRow({ candidate_id: "july" });
    const { client, statements } = fakeClient([pure, mixed, july]);

    const result = await runSweepConfirmationReset(
      client,
      options({
        cohort: "august-21-template",
        confirmedFrom: "2026-08-21",
        confirmedTo: "2026-08-21",
        dryRun: false,
        expectedTotal: 1,
      })
    );

    expect(result).toMatchObject({
      cohort: "august-21-template",
      resettable: { total: 1, zeroRecordCount: 0, withRecordsCount: 1 },
      skipped: { shapeMismatchCount: 2 },
      deletedConfirmations: 1,
      clearedStamps: 1,
    });
    const deleteStatement = statements.find((s) => s.text.includes("DELETE FROM"));
    expect(deleteStatement?.values?.[0]).toEqual(["pure"]);
    const updateStatement = statements.find((s) => s.text.includes("UPDATE public.candidates"));
    expect(updateStatement?.values).toEqual([["pure"]]);
  });
});

describe("records-retired-out cohort", () => {
  const retiredOut = (overrides: Partial<SweepConfirmationCohortRow> & { candidate_id: string }) =>
    cohortRow({
      retired_record_count: 2,
      confirmed_gap_ids: ["candidate_records.only_general_labels"],
      ...overrides,
    });

  it("matches a covering ledger whose stored records were all retired", () => {
    expect(isRecordsRetiredOutLedger(retiredOut({ candidate_id: "a" }))).toBe(true);
    expect(isRecordsRetiredOutLedger(retiredOut({ candidate_id: "a", confirmed_gap_ids: [] }))).toBe(
      true
    );
  });

  it("keeps candidates with active records, no retired records, or an older ledger", () => {
    expect(isRecordsRetiredOutLedger(retiredOut({ candidate_id: "a", record_count: 1 }))).toBe(false);
    expect(
      isRecordsRetiredOutLedger(retiredOut({ candidate_id: "a", retired_record_count: 0 }))
    ).toBe(false);
    expect(
      isRecordsRetiredOutLedger(retiredOut({ candidate_id: "a", covers_latest_search: false }))
    ).toBe(false);
  });

  it("never matches a candidate with a covering no_records_found claim in any context", () => {
    const confirmedNull = retiredOut({
      candidate_id: "a",
      confirmed_gap_ids: ["candidate_records.no_records_found"],
      has_covering_no_records_claim: true,
    });
    expect(isRecordsRetiredOutLedger(confirmedNull)).toBe(false);
    // Weak election ledger, but a newer presidential ledger (no stamp
    // advance) confirmed the candidate null — the candidate is done.
    const siblingConfirmedNull = retiredOut({
      candidate_id: "a",
      has_covering_no_records_claim: true,
    });
    expect(isRecordsRetiredOutLedger(siblingConfirmedNull)).toBe(false);
  });

  it("live run resets only matching ledgers and clears their stamps", async () => {
    const match = retiredOut({ candidate_id: "match" });
    const confirmedNull = retiredOut({
      candidate_id: "null",
      confirmed_gap_ids: ["candidate_records.no_records_found"],
      has_covering_no_records_claim: true,
    });
    const withRecords = retiredOut({ candidate_id: "live", record_count: 3 });
    const { client, statements } = fakeClient([match, confirmedNull, withRecords]);

    const result = await runSweepConfirmationReset(
      client,
      options({
        cohort: "records-retired-out",
        confirmedFrom: "2026-07-10",
        confirmedTo: "2026-09-10",
        dryRun: false,
        expectedTotal: 1,
      })
    );

    expect(result).toMatchObject({
      cohort: "records-retired-out",
      resettable: { total: 1, zeroRecordCount: 1 },
      skipped: { shapeMismatchCount: 2 },
      deletedConfirmations: 1,
      clearedStamps: 1,
    });
    const deleteStatement = statements.find((s) => s.text.includes("DELETE FROM"));
    expect(deleteStatement?.values?.[0]).toEqual(["match"]);
    const updateStatement = statements.find((s) => s.text.includes("UPDATE public.candidates"));
    expect(updateStatement?.values).toEqual([["match"]]);
  });
});

describe("route-coverage-gap cohort", () => {
  const tagged = (questionIds: readonly string[]) => ({
    entries: questionIds.map((question_id) => ({
      question: `Question ${question_id}`,
      question_id,
      finding: "Nothing found.",
    })),
  });
  const JUDICIAL = ["cases", "discipline", "endorsements"];
  const OFFICEHOLDER = [
    "rollcalls",
    "sponsorship",
    "executive",
    "proceedings",
    "leadership",
    "outside_chamber",
    "endorsements",
  ];

  it("flags a ledger whose tags answer the other family's questions", () => {
    // A constable relabeled non-judicial still carries the judicial ledger.
    expect(
      isRouteCoverageGapLedger(cohortRow({ candidate_id: "a", evidence: tagged(JUDICIAL) }))
    ).toBe(true);
    // A justice of the peace relabeled judicial still carries the officeholder ledger.
    expect(
      isRouteCoverageGapLedger(
        cohortRow({
          candidate_id: "b",
          evidence: tagged(OFFICEHOLDER),
          discovery_contest_family: "judicial_office",
        })
      )
    ).toBe(true);
  });

  it("leaves a wrong-route ledger older than the candidate's latest search for the audit", () => {
    expect(
      isRouteCoverageGapLedger(
        cohortRow({ candidate_id: "a", evidence: tagged(JUDICIAL), covers_latest_search: false })
      )
    ).toBe(false);
  });

  it("keeps a ledger that covers its current route, even if it also covers another", () => {
    expect(
      isRouteCoverageGapLedger(cohortRow({ candidate_id: "a", evidence: tagged(OFFICEHOLDER) }))
    ).toBe(false);
    expect(
      isRouteCoverageGapLedger(
        cohortRow({
          candidate_id: "b",
          evidence: tagged([...JUDICIAL, ...OFFICEHOLDER]),
          discovery_contest_family: "judicial_office",
        })
      )
    ).toBe(false);
  });

  it("requires context ids and refuses them for other cohorts before touching the database", async () => {
    const { client, statements } = fakeClient([]);
    await expect(
      runSweepConfirmationReset(client, options({ cohort: "route-coverage-gap" }))
    ).rejects.toThrow(/requires --context-ids-file/);
    await expect(
      runSweepConfirmationReset(client, options({ contextIds: ["e-1"] }))
    ).rejects.toThrow(/applies only to --cohort route-coverage-gap/);
    expect(statements).toEqual([]);
  });

  it("live run reads only the listed contexts and resets only gap ledgers", async () => {
    const gap = cohortRow({ candidate_id: "gap", evidence: tagged(JUDICIAL) });
    const covered = cohortRow({ candidate_id: "covered", evidence: tagged(OFFICEHOLDER) });
    const { client, statements } = fakeClient([gap, covered]);

    const result = await runSweepConfirmationReset(
      client,
      options({
        cohort: "route-coverage-gap",
        contextIds: ["11111111-1111-1111-1111-111111111111"],
        confirmedFrom: "2026-01-01",
        confirmedTo: "2026-09-11",
        dryRun: false,
        expectedTotal: 1,
      })
    );

    expect(result).toMatchObject({
      cohort: "route-coverage-gap",
      resettable: { total: 1 },
      skipped: { shapeMismatchCount: 1 },
      deletedConfirmations: 1,
      clearedStamps: 1,
    });
    const select = statements.find((s) => s.text.includes("FOR UPDATE"));
    expect(select?.values?.[3]).toEqual(["11111111-1111-1111-1111-111111111111"]);
    const deleteStatement = statements.find((s) => s.text.includes("DELETE FROM"));
    expect(deleteStatement?.values?.[0]).toEqual(["gap"]);
  });
});
