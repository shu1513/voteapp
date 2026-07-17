import { describe, expect, it, vi } from "vitest";

import { runWithdrawCandidateElection } from "../../src/scripts/withdrawManualCandidateElection.js";

const CANDIDATE_ID = "11111111-1111-4111-8111-111111111111";
const ELECTION_ID = "22222222-2222-4222-8222-222222222222";
const LINK_ID = "44444444-4444-4444-8444-444444444444";

function linkRow(overrides: Partial<Record<string, unknown>> = {}) {
  return { id: LINK_ID, status: "declared", ...overrides };
}

function electionRow(overrides: Partial<Record<string, unknown>> = {}) {
  return {
    id: ELECTION_ID,
    official_ballot_title: "Governor",
    election_date: "2026-11-03",
    is_upcoming: true,
    ...overrides,
  };
}

// Query order: BEGIN, lock link, load election, status update, stale
// future-election event delete, withdrawal event insert, COMMIT/ROLLBACK.
function buildClient(responses: Record<string, { rows: unknown[]; rowCount?: number }[]>) {
  const calls: { text: string; values: unknown[] }[] = [];
  const queue = { ...responses };
  const query = vi.fn(async (text: string, values?: unknown[]) => {
    calls.push({ text, values: values ?? [] });
    for (const key of Object.keys(queue)) {
      if (text.includes(key)) {
        const result = queue[key]!.shift();
        if (result !== undefined) return { rowCount: result.rows.length, ...result };
      }
    }
    return { rows: [], rowCount: 0 };
  });
  return { query, calls };
}

function happyResponses(overrides: Partial<Record<string, { rows: unknown[]; rowCount?: number }[]>> = {}) {
  return {
    "FROM public.candidate_elections": [{ rows: [linkRow()] }],
    "FROM public.elections": [{ rows: [electionRow()] }],
    "DELETE FROM public.user_candidate_follow_notification_events": [{ rows: [], rowCount: 2 }],
    "INSERT INTO public.user_candidate_follow_notification_events": [{ rows: [{ id: "e1" }], rowCount: 1 }],
    ...overrides,
  };
}

describe("runWithdrawCandidateElection", () => {
  it("marks the link withdrawn, purges unsent on-the-ballot events, and creates withdrawal events", async () => {
    const { query, calls } = buildClient(happyResponses());

    const result = await runWithdrawCandidateElection(
      { query },
      { candidateId: CANDIDATE_ID, electionId: ELECTION_ID, dryRun: false }
    );

    expect(result).toMatchObject({
      action: "withdrawn",
      dryRun: false,
      electionTitle: "Governor",
      staleFutureElectionEventsDeleted: 2,
      withdrawalEventsCreated: 1,
    });
    const lock = calls.find((call) => call.text.includes("FROM public.candidate_elections"));
    expect(lock?.text).toContain("FOR UPDATE");
    const update = calls.find((call) => call.text.includes("UPDATE public.candidate_elections"));
    expect(update?.text).toContain("SET status = 'withdrawn'");
    expect(update?.values).toEqual([LINK_ID]);
    // Unsent "on the ballot" events for the pair are wrong once the candidate
    // withdrew; one digest must not carry both lines.
    const purge = calls.find((call) => call.text.includes("DELETE FROM public.user_candidate_follow_notification_events"));
    expect(purge?.text).toContain("event_type = 'candidate_future_election'");
    expect(purge?.text).toContain("notified_at IS NULL");
    expect(calls.at(-1)?.text).toBe("COMMIT");
  });

  it("dry run executes the full transaction and rolls back", async () => {
    const { query, calls } = buildClient(happyResponses());

    const result = await runWithdrawCandidateElection(
      { query },
      { candidateId: CANDIDATE_ID, electionId: ELECTION_ID, dryRun: true }
    );

    // Counts are real because the writes ran before the rollback.
    expect(result).toMatchObject({
      dryRun: true,
      staleFutureElectionEventsDeleted: 2,
      withdrawalEventsCreated: 1,
    });
    expect(calls.at(-1)?.text).toBe("ROLLBACK");
  });

  it("accepts uppercase UUID input by normalizing before row matching", async () => {
    const { query, calls } = buildClient(happyResponses());

    await runWithdrawCandidateElection(
      { query },
      { candidateId: CANDIDATE_ID.toUpperCase(), electionId: ELECTION_ID.toUpperCase(), dryRun: false }
    );

    const lock = calls.find((call) => call.text.includes("FROM public.candidate_elections"));
    expect(lock?.values).toEqual([CANDIDATE_ID, ELECTION_ID]);
  });

  it("refuses when the link does not exist", async () => {
    const { query } = buildClient(happyResponses({ "FROM public.candidate_elections": [{ rows: [] }] }));

    await expect(
      runWithdrawCandidateElection({ query }, { candidateId: CANDIDATE_ID, electionId: ELECTION_ID, dryRun: false })
    ).rejects.toThrow(/No candidate_elections link found/);
  });

  it("refuses when the link is already withdrawn", async () => {
    const { query, calls } = buildClient(
      happyResponses({ "FROM public.candidate_elections": [{ rows: [linkRow({ status: "withdrawn" })] }] })
    );

    await expect(
      runWithdrawCandidateElection({ query }, { candidateId: CANDIDATE_ID, electionId: ELECTION_ID, dryRun: false })
    ).rejects.toThrow(/already withdrawn/);
    expect(calls.some((call) => call.text.includes("UPDATE public.candidate_elections"))).toBe(false);
    expect(calls.at(-1)?.text).toBe("ROLLBACK");
  });

  it("refuses to withdraw a link with a settled result status", async () => {
    const { query } = buildClient(
      happyResponses({ "FROM public.candidate_elections": [{ rows: [linkRow({ status: "won" })] }] })
    );

    await expect(
      runWithdrawCandidateElection({ query }, { candidateId: CANDIDATE_ID, electionId: ELECTION_ID, dryRun: false })
    ).rejects.toThrow(/settled result/);
  });

  it("refuses when the election already happened", async () => {
    const { query, calls } = buildClient(
      happyResponses({
        "FROM public.elections": [{ rows: [electionRow({ election_date: "2024-11-05", is_upcoming: false })] }],
      })
    );

    await expect(
      runWithdrawCandidateElection({ query }, { candidateId: CANDIDATE_ID, electionId: ELECTION_ID, dryRun: false })
    ).rejects.toThrow(/already happened/);
    expect(calls.some((call) => call.text.includes("UPDATE public.candidate_elections"))).toBe(false);
  });

  it("evaluates upcoming-ness with the US-local boundary in SQL", async () => {
    const { query, calls } = buildClient(happyResponses());

    await runWithdrawCandidateElection(
      { query },
      { candidateId: CANDIDATE_ID, electionId: ELECTION_ID, dryRun: false }
    );

    const electionLoad = calls.find((call) => call.text.includes("FROM public.elections"));
    expect(electionLoad?.text).toContain("(now() AT TIME ZONE 'Pacific/Honolulu')::date");
  });
});
