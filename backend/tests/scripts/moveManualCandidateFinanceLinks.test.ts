import { describe, expect, it, vi } from "vitest";

import {
  findFinanceLinkIdentityKey,
  listLinkRowChildReferences,
  runMoveCandidateFinanceLinks,
} from "../../src/scripts/moveManualCandidateFinanceLinks.js";

const CANDIDATE_ID = "11111111-1111-1111-1111-111111111111";
const FROM_ELECTION = "22222222-2222-2222-2222-222222222222";
const TO_ELECTION = "33333333-3333-3333-3333-333333333333";

const DISTRICT = "dddddddd-dddd-dddd-dddd-dddddddddddd";
const OFFICE = "eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee";
const SRC_ROW = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa1";
const TGT_ROW = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa2";

function electionRows(
  overrides: { fromYear?: number; toYear?: number; toDistrict?: string; toOffice?: string | null } = {}
) {
  return [
    {
      id: FROM_ELECTION,
      official_ballot_title: "State Representative",
      election_year: overrides.fromYear ?? 2026,
      district_id: DISTRICT,
      office_id: OFFICE,
    },
    {
      id: TO_ELECTION,
      official_ballot_title: "State Representative District 6",
      election_year: overrides.toYear ?? 2026,
      district_id: overrides.toDistrict ?? DISTRICT,
      office_id: overrides.toOffice === undefined ? OFFICE : overrides.toOffice,
    },
  ];
}

// Query order: BEGIN, lock both elections, target roster link, FK-table
// catalog scan, then per table [row count, identity-key catalog lookup,
// child-FK catalog lookup, then either a bulk DELETE of duplicates (no
// children) or per-pair child counts + DELETE by id, UPDATE repoint],
// COMMIT/ROLLBACK. DELETE/UPDATE answer with rowCount like pg does.
function buildClient(responses: Record<string, unknown[][]>) {
  const calls: { text: string; values: unknown[] }[] = [];
  const queue = { ...responses };
  const query = vi.fn(async (text: string, values?: unknown[]) => {
    calls.push({ text, values: values ?? [] });
    for (const key of Object.keys(queue)) {
      if (text.includes(key)) {
        const rows = queue[key]!.shift();
        if (rows !== undefined) return { rows, rowCount: rows.length };
      }
    }
    return { rows: [], rowCount: 0 };
  });
  return { query, calls };
}

function happyResponses(overrides: Partial<Record<string, unknown[][]>> = {}) {
  return {
    "FROM public.elections": [electionRows()],
    "FROM public.candidate_elections": [[{ id: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa" }]],
    "confrelid = 'public.elections'::regclass": [
      [
        { table_name: "public.fl_candidate_finance_links", election_column: "election_id" },
        { table_name: "public.user_election_choices", election_column: "election_id" },
      ],
    ],
    "count(*)::text AS n FROM public.fl_candidate_finance_links": [[{ n: "1" }]],
    "contype IN ('u', 'p')": [
      [{ constraint_name: "fl_candidate_finance_links_unique", column_name: "committee_id" }],
    ],
    // No child tables: the twins are interchangeable, bulk path.
    "fa.attname = 'id'": [[]],
    // One duplicate dropped, one row repointed.
    "DELETE FROM public.fl_candidate_finance_links": [[{}]],
    "UPDATE public.fl_candidate_finance_links": [[{}]],
    ...overrides,
  };
}

const OPTIONS = { candidateId: CANDIDATE_ID, fromElectionId: FROM_ELECTION, toElectionId: TO_ELECTION, dryRun: false };

describe("runMoveCandidateFinanceLinks", () => {
  it("drops duplicates the target already holds and repoints the rest, table by table", async () => {
    const { query, calls } = buildClient(happyResponses());

    const result = await runMoveCandidateFinanceLinks({ query }, OPTIONS);

    expect(result.tables).toEqual([
      { table: "public.fl_candidate_finance_links", repointed: 1, duplicatesDeleted: 1, emptyTargetsReplaced: 0 },
    ]);
    expect(result.toElectionTitle).toBe("State Representative District 6");
    const del = calls.find((call) => call.text.includes("DELETE FROM public.fl_candidate_finance_links"));
    // Duplicate = same identity columns on the target election.
    expect(del?.text).toContain("AND tgt.committee_id = src.committee_id");
    expect(del?.values).toEqual([CANDIDATE_ID, FROM_ELECTION, TO_ELECTION]);
    const update = calls.find((call) => call.text.includes("UPDATE public.fl_candidate_finance_links"));
    expect(update?.text).toContain("SET election_id = $3::uuid");
    expect(update?.values).toEqual([CANDIDATE_ID, FROM_ELECTION, TO_ELECTION]);
    // The target roster link is held for the transaction so a concurrent
    // unlink cannot strand the repointed rows.
    const targetLink = calls.find((call) => call.text.includes("FROM public.candidate_elections"));
    expect(targetLink?.text).toContain("FOR KEY SHARE");
    // User tables are never touched, even when the catalog scan lists them.
    expect(calls.some((call) => call.text.includes("user_election_choices"))).toBe(false);
    expect(calls.at(-1)?.text).toBe("COMMIT");
  });

  it("dry-run executes the writes for real counts and rolls back", async () => {
    const { query, calls } = buildClient(happyResponses());

    const result = await runMoveCandidateFinanceLinks({ query }, { ...OPTIONS, dryRun: true });

    expect(result.dryRun).toBe(true);
    expect(result.tables[0]).toMatchObject({ repointed: 1, duplicatesDeleted: 1 });
    expect(calls.at(-1)?.text).toBe("ROLLBACK");
  });

  it("keeps the populated twin: a bare target is replaced by the from-row that holds the snapshot", async () => {
    const children = [
      { table_name: "public.fl_candidate_finance_summaries", column_name: "link_id" },
      { table_name: "public.fl_candidate_finance_direct_breakdowns", column_name: "link_id" },
    ];
    const populatedSource = buildClient(happyResponses({
      "fa.attname = 'id'": [children],
      "SELECT src.id AS src_id": [[{ src_id: SRC_ROW, tgt_id: TGT_ROW }]],
      // Source: 1 summary + 15 breakdowns; target: none.
      "FROM public.fl_candidate_finance_summaries WHERE": [[{ n: "1" }], [{ n: "0" }]],
      "FROM public.fl_candidate_finance_direct_breakdowns WHERE": [[{ n: "15" }], [{ n: "0" }]],
      "DELETE FROM public.fl_candidate_finance_links": [[{}]],
    }));

    const result = await runMoveCandidateFinanceLinks({ query: populatedSource.query }, OPTIONS);

    expect(result.tables[0]).toMatchObject({ repointed: 1, duplicatesDeleted: 0, emptyTargetsReplaced: 1 });
    const del = populatedSource.calls.find((call) => call.text.includes("DELETE FROM public.fl_candidate_finance_links"));
    expect(del?.values).toEqual([TGT_ROW]);

    // Both populated (the sibling-shell-synced-twice case): the target wins.
    const bothPopulated = buildClient(happyResponses({
      "fa.attname = 'id'": [children],
      "SELECT src.id AS src_id": [[{ src_id: SRC_ROW, tgt_id: TGT_ROW }]],
      "FROM public.fl_candidate_finance_summaries WHERE": [[{ n: "1" }], [{ n: "1" }]],
      "FROM public.fl_candidate_finance_direct_breakdowns WHERE": [[{ n: "15" }], [{ n: "15" }]],
      "DELETE FROM public.fl_candidate_finance_links": [[{}]],
    }));

    const tie = await runMoveCandidateFinanceLinks({ query: bothPopulated.query }, OPTIONS);

    expect(tie.tables[0]).toMatchObject({ duplicatesDeleted: 1, emptyTargetsReplaced: 0 });
    const tieDelete = bothPopulated.calls.find((call) => call.text.includes("DELETE FROM public.fl_candidate_finance_links"));
    expect(tieDelete?.values).toEqual([SRC_ROW]);
  });

  it("refuses a move that would change the stored district or office context", async () => {
    const crossDistrict = buildClient(happyResponses({
      "FROM public.elections": [electionRows({ toDistrict: "ffffffff-ffff-ffff-ffff-ffffffffffff" })],
    }));
    await expect(runMoveCandidateFinanceLinks({ query: crossDistrict.query }, OPTIONS)).rejects.toThrow(
      /differ in district or office/
    );

    const crossOffice = buildClient(happyResponses({
      "FROM public.elections": [electionRows({ toOffice: null })],
    }));
    await expect(runMoveCandidateFinanceLinks({ query: crossOffice.query }, OPTIONS)).rejects.toThrow(
      /differ in district or office/
    );
    expect(crossOffice.calls.some((call) => /^\s*(UPDATE|DELETE)\b/i.test(call.text))).toBe(false);
  });

  it("refuses a cross-year move", async () => {
    const { query } = buildClient(happyResponses({
      "FROM public.elections": [electionRows({ toYear: 2028 })],
    }));

    await expect(runMoveCandidateFinanceLinks({ query }, OPTIONS)).rejects.toThrow(/different years \(2026 vs 2028\)/);
  });

  it("refuses when the candidate is not on the target roster", async () => {
    const { query, calls } = buildClient(happyResponses({
      "FROM public.candidate_elections": [[]],
    }));

    await expect(runMoveCandidateFinanceLinks({ query }, OPTIONS)).rejects.toThrow(/not linked to election/);
    expect(calls.some((call) => /^\s*(UPDATE|DELETE)\b/i.test(call.text))).toBe(false);
  });

  it("refuses a table holding rows without a unique key on the pair, and a pair with no rows at all", async () => {
    const noKey = buildClient(happyResponses({ "contype IN ('u', 'p')": [[]] }));
    await expect(runMoveCandidateFinanceLinks({ query: noKey.query }, OPTIONS)).rejects.toThrow(
      /public\.fl_candidate_finance_links.*extend the guard/
    );

    const noRows = buildClient(happyResponses({
      "count(*)::text AS n FROM public.fl_candidate_finance_links": [[{ n: "0" }]],
    }));
    await expect(runMoveCandidateFinanceLinks({ query: noRows.query }, OPTIONS)).rejects.toThrow(/nothing to move/);
  });

  it("refuses missing elections and identical from/to ids", async () => {
    const missing = buildClient(happyResponses({ "FROM public.elections": [[electionRows()[0]!]] }));
    await expect(runMoveCandidateFinanceLinks({ query: missing.query }, OPTIONS)).rejects.toThrow(
      `Election not found: ${TO_ELECTION}`
    );

    await expect(
      runMoveCandidateFinanceLinks({ query: missing.query }, { ...OPTIONS, toElectionId: FROM_ELECTION })
    ).rejects.toThrow(/must differ/);
  });
});

describe("listLinkRowChildReferences", () => {
  it("reports the child column that references the link id, composite keys included", async () => {
    const { query, calls } = buildClient({
      "fa.attname = 'id'": [
        [
          { table_name: "public.fl_candidate_finance_summaries", column_name: "link_id" },
          { table_name: "public.fl_candidate_finance_direct_breakdowns", column_name: "link_id" },
        ],
      ],
    });
    expect(await listLinkRowChildReferences({ query }, "public.fl_candidate_finance_links")).toEqual([
      { table: "public.fl_candidate_finance_summaries", column: "link_id" },
      { table: "public.fl_candidate_finance_direct_breakdowns", column: "link_id" },
    ]);
    expect(calls[0]?.values).toEqual(["public.fl_candidate_finance_links"]);
  });
});

describe("findFinanceLinkIdentityKey", () => {
  it("reports the key's other columns, an empty list for a bare pair key, and null when there is none", async () => {
    const withColumns = buildClient({
      "contype IN ('u', 'p')": [
        [
          { constraint_name: "sfc_outside_unique", column_name: "spender_fppc_id" },
          { constraint_name: "sfc_outside_unique", column_name: "support_oppose" },
        ],
      ],
    });
    expect(await findFinanceLinkIdentityKey(withColumns, "public.sfc_candidate_finance_outside_committee_links")).toEqual({
      constraintName: "sfc_outside_unique",
      columns: ["spender_fppc_id", "support_oppose"],
    });

    const barePair = buildClient({
      "contype IN ('u', 'p')": [[{ constraint_name: "nyc_sync_attempts_pkey", column_name: null }]],
    });
    expect(await findFinanceLinkIdentityKey(barePair, "public.nyc_candidate_finance_sync_attempts")).toEqual({
      constraintName: "nyc_sync_attempts_pkey",
      columns: [],
    });

    const none = buildClient({ "contype IN ('u', 'p')": [[]] });
    expect(await findFinanceLinkIdentityKey(none, "public.user_election_choices")).toBeNull();
  });
});
