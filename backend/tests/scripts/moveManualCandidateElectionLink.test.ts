import { describe, expect, it, vi } from "vitest";

import { runMoveCandidateElectionLink } from "../../src/scripts/moveManualCandidateElectionLink.js";

const CANDIDATE_ID = "11111111-1111-1111-1111-111111111111";
const FROM_ELECTION = "22222222-2222-2222-2222-222222222222";
const TO_ELECTION = "33333333-3333-3333-3333-333333333333";
const LINK_ID = "44444444-4444-4444-4444-444444444444";
const MATE_ID = "55555555-5555-5555-5555-555555555555";

function linkRow(overrides: Partial<Record<string, unknown>> = {}) {
  return {
    id: LINK_ID,
    is_incumbent: false,
    status: "declared",
    running_mate_candidate_id: null,
    ...overrides,
  };
}

function electionRows(overrides: { fromDate?: string; toDate?: string; toDistrict?: string } = {}) {
  return [
    {
      id: FROM_ELECTION,
      district_id: "dddddddd-dddd-dddd-dddd-dddddddddddd",
      election_date: overrides.fromDate ?? "2026-11-03",
      official_ballot_title: "Governing Board",
    },
    {
      id: TO_ELECTION,
      district_id: overrides.toDistrict ?? "dddddddd-dddd-dddd-dddd-dddddddddddd",
      election_date: overrides.toDate ?? "2026-11-03",
      official_ballot_title: "Governing Board Member, Seat 3",
    },
  ];
}

// Query order: BEGIN, lock from-link, load elections, FK-table catalog scan,
// (per-table finance counts), target-link lock, [mate collision], write, COMMIT/ROLLBACK.
function buildClient(responses: Record<string, unknown[][]>) {
  const calls: { text: string; values: unknown[] }[] = [];
  const queue = { ...responses };
  const query = vi.fn(async (text: string, values?: unknown[]) => {
    calls.push({ text, values: values ?? [] });
    for (const key of Object.keys(queue)) {
      if (text.includes(key)) {
        const rows = queue[key]!.shift();
        if (rows !== undefined) return { rows };
      }
    }
    return { rows: [] };
  });
  return { query, calls };
}

function happyResponses(overrides: Partial<Record<string, unknown[][]>> = {}) {
  return {
    "FROM public.candidate_elections\n        WHERE candidate_id": [[linkRow()], []],
    "FROM public.elections": [electionRows()],
    "pg_constraint": [
      [{ table_name: "public.az_candidate_finance_links", election_column: "election_id" }],
    ],
    "count(*)::text AS n FROM public.az_candidate_finance_links": [[{ n: "0" }]],
    ...overrides,
  };
}

describe("runMoveCandidateElectionLink", () => {
  it("moves the link to the sibling shell and preserves the row id", async () => {
    const { query, calls } = buildClient(happyResponses());

    const result = await runMoveCandidateElectionLink(
      { query },
      { candidateId: CANDIDATE_ID, fromElectionId: FROM_ELECTION, toElectionId: TO_ELECTION, dryRun: false }
    );

    expect(result.action).toBe("moved");
    expect(result.toElectionTitle).toBe("Governing Board Member, Seat 3");
    // The sibling guard must validate against locked rows (deterministic
    // order so two concurrent movers cannot deadlock).
    const electionsLoad = calls.find((call) => call.text.includes("FROM public.elections"));
    expect(electionsLoad?.text).toContain("ORDER BY id");
    expect(electionsLoad?.text).toContain("FOR UPDATE");
    const update = calls.find((call) => call.text.includes("UPDATE public.candidate_elections"));
    expect(update?.text).toContain("SET election_id = $2::uuid");
    expect(update?.values).toEqual([LINK_ID, TO_ELECTION]);
    expect(calls.at(-1)?.text).toBe("COMMIT");
  });

  it("accepts uppercase UUID input by normalizing before row matching", async () => {
    const { query, calls } = buildClient(happyResponses());

    const result = await runMoveCandidateElectionLink(
      { query },
      {
        candidateId: CANDIDATE_ID.toUpperCase(),
        fromElectionId: FROM_ELECTION.toUpperCase(),
        toElectionId: TO_ELECTION.toUpperCase(),
        dryRun: false,
      }
    );

    expect(result.action).toBe("moved");
    const linkLock = calls.find((call) => call.text.includes("WHERE candidate_id"));
    expect(linkLock?.values).toEqual([CANDIDATE_ID, FROM_ELECTION]);
  });

  it("refuses when the from-election has persisted election results", async () => {
    const { query } = buildClient(happyResponses({
      "FROM public.election_results": [[{ id: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa" }]],
    }));

    await expect(
      runMoveCandidateElectionLink(
        { query },
        { candidateId: CANDIDATE_ID, fromElectionId: FROM_ELECTION, toElectionId: TO_ELECTION, dryRun: false }
      )
    ).rejects.toThrow(/persisted election_results rows whose winners reference/);
  });

  it("dry-run rolls back and writes nothing", async () => {
    const { query, calls } = buildClient(happyResponses());

    const result = await runMoveCandidateElectionLink(
      { query },
      { candidateId: CANDIDATE_ID, fromElectionId: FROM_ELECTION, toElectionId: TO_ELECTION, dryRun: true }
    );

    expect(result.dryRun).toBe(true);
    // Leading-whitespace-tolerant: the production UPDATE is a multiline
    // template literal starting with a newline.
    expect(calls.some((call) => /^\s*(UPDATE|DELETE)\b/i.test(call.text))).toBe(false);
    expect(calls.at(-1)?.text).toBe("ROLLBACK");
  });

  it("refuses when the link does not exist", async () => {
    const { query } = buildClient(happyResponses({
      "FROM public.candidate_elections\n        WHERE candidate_id": [[]],
    }));

    await expect(
      runMoveCandidateElectionLink(
        { query },
        { candidateId: CANDIDATE_ID, fromElectionId: FROM_ELECTION, toElectionId: TO_ELECTION, dryRun: false }
      )
    ).rejects.toThrow(/No candidate_elections link found/);
  });

  it("refuses cross-district and cross-date moves", async () => {
    const crossDistrict = buildClient(happyResponses({
      "FROM public.elections": [electionRows({ toDistrict: "eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee" })],
    }));
    await expect(
      runMoveCandidateElectionLink(
        { query: crossDistrict.query },
        { candidateId: CANDIDATE_ID, fromElectionId: FROM_ELECTION, toElectionId: TO_ELECTION, dryRun: false }
      )
    ).rejects.toThrow(/different districts/);

    const crossDate = buildClient(happyResponses({
      "FROM public.elections": [electionRows({ toDate: "2027-05-01" })],
    }));
    await expect(
      runMoveCandidateElectionLink(
        { query: crossDate.query },
        { candidateId: CANDIDATE_ID, fromElectionId: FROM_ELECTION, toElectionId: TO_ELECTION, dryRun: false }
      )
    ).rejects.toThrow(/different dates/);
  });

  it("refuses when finance rows on the from-election would be stranded", async () => {
    const { query } = buildClient(happyResponses({
      "count(*)::text AS n FROM public.az_candidate_finance_links": [[{ n: "2" }]],
    }));

    await expect(
      runMoveCandidateElectionLink(
        { query },
        { candidateId: CANDIDATE_ID, fromElectionId: FROM_ELECTION, toElectionId: TO_ELECTION, dryRun: false }
      )
    ).rejects.toThrow(/az_candidate_finance_links \(2\)/);
  });

  it("converges an identical duplicate link by deleting the from-link", async () => {
    const { query, calls } = buildClient(happyResponses({
      "FROM public.candidate_elections\n        WHERE candidate_id": [[linkRow()], [linkRow({ id: "99999999-9999-9999-9999-999999999999" })]],
    }));

    const result = await runMoveCandidateElectionLink(
      { query },
      { candidateId: CANDIDATE_ID, fromElectionId: FROM_ELECTION, toElectionId: TO_ELECTION, dryRun: false }
    );

    expect(result.action).toBe("merged_duplicate");
    const del = calls.find((call) => call.text.includes("DELETE FROM public.candidate_elections"));
    expect(del?.values).toEqual([LINK_ID]);
  });

  it("refuses to merge disagreeing duplicate links", async () => {
    const { query } = buildClient(happyResponses({
      "FROM public.candidate_elections\n        WHERE candidate_id": [
        [linkRow()],
        [linkRow({ id: "99999999-9999-9999-9999-999999999999", status: "withdrawn" })],
      ],
    }));

    await expect(
      runMoveCandidateElectionLink(
        { query },
        { candidateId: CANDIDATE_ID, fromElectionId: FROM_ELECTION, toElectionId: TO_ELECTION, dryRun: false }
      )
    ).rejects.toThrow(/links disagree/);
  });

  it("refuses when the moved running mate collides on the target shell", async () => {
    const { query } = buildClient(happyResponses({
      "FROM public.candidate_elections\n        WHERE candidate_id": [
        [linkRow({ running_mate_candidate_id: MATE_ID })],
        [],
      ],
      "running_mate_candidate_id = $2::uuid": [[{ id: "88888888-8888-8888-8888-888888888888" }]],
    }));

    await expect(
      runMoveCandidateElectionLink(
        { query },
        { candidateId: CANDIDATE_ID, fromElectionId: FROM_ELECTION, toElectionId: TO_ELECTION, dryRun: false }
      )
    ).rejects.toThrow(/already has a link carrying running mate/);
  });

  it("rejects identical from and to elections before touching the database", async () => {
    const { query } = buildClient({});
    await expect(
      runMoveCandidateElectionLink(
        { query },
        { candidateId: CANDIDATE_ID, fromElectionId: FROM_ELECTION, toElectionId: FROM_ELECTION, dryRun: false }
      )
    ).rejects.toThrow(/must differ/);
    expect(query).not.toHaveBeenCalled();
  });
});
