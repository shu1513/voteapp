import { describe, expect, it, vi } from "vitest";

import { listUserElectionChoices, setUserElectionChoice } from "../../../src/pipeline/users/userElectionChoices.js";

const userId = "11111111-1111-4111-8111-111111111111";
const electionId = "22222222-2222-4222-8222-222222222222";
const candidateId = "33333333-3333-4333-8333-333333333333";

function createMockTransactionalDb() {
  const client = {
    query: vi.fn(),
    release: vi.fn(),
  };
  const db = {
    connect: vi.fn().mockResolvedValue(client),
  };
  return { db, client };
}

describe("setUserElectionChoice", () => {
  it("keeps election catalog reads compatible with the SELECT-only API role", async () => {
    const { db, client } = createMockTransactionalDb();
    client.query
      .mockResolvedValueOnce({ rows: [] })
      .mockResolvedValueOnce({ rows: [{ id: userId }] })
      .mockResolvedValueOnce({
        rows: [
          {
            id: electionId,
            race_type: "office",
            election_date: "2026-08-18",
            seats_to_fill: 1,
            is_upcoming: true,
          },
        ],
      })
      .mockResolvedValueOnce({ rows: [{ candidate_id: candidateId }] })
      .mockResolvedValueOnce({ rows: [{ count: "0" }] })
      .mockResolvedValueOnce({ rows: [], rowCount: 1 })
      .mockResolvedValueOnce({
        rows: [
          {
            election_id: electionId,
            race_type: "office",
            official_ballot_title: "Commissioner of Agriculture",
            election_date: "2026-08-18",
            seats_to_fill: 1,
            candidate_id: candidateId,
            display_name: "Donald A. Prichard",
            candidacy_status: "declared",
            measure_position: null,
            measure_result: null,
            updated_at: "2026-08-02T17:00:00.000Z",
          },
        ],
      })
      .mockResolvedValueOnce({ rows: [] });

    await expect(
      setUserElectionChoice(db, userId, {
        electionId,
        candidateId,
        chosen: true,
      })
    ).resolves.toEqual({
      choice: {
        election_id: electionId,
        race_type: "office",
        official_ballot_title: "Commissioner of Agriculture",
        election_date: "2026-08-18",
        seats_to_fill: 1,
        picks: [
          {
            candidate_id: candidateId,
            display_name: "Donald A. Prichard",
            candidacy_status: "declared",
          },
        ],
        measure_position: null,
        measure_result: null,
        // The post-write read-back leaves the canonical-result fields at
        // their defaults; only the list read attaches them (writes are gated
        // to races the ballot still cards, where the summary carries them).
        current_result_outcome: null,
        current_result_winners: [],
        updated_at: "2026-08-02T17:00:00.000Z",
      },
    });

    expect(String(client.query.mock.calls[1]?.[0])).toContain("FOR UPDATE");
    expect(String(client.query.mock.calls[2]?.[0])).toContain("FROM public.elections");
    expect(String(client.query.mock.calls[2]?.[0])).not.toContain("FOR SHARE");
    expect(String(client.query.mock.calls[3]?.[0])).toContain("FROM public.candidate_elections");
    expect(String(client.query.mock.calls[3]?.[0])).not.toContain("FOR SHARE");
    const insertSql = String(client.query.mock.calls[5]?.[0]);
    expect(insertSql).toContain("INSERT INTO public.user_election_choices");
    // The write itself must carry the eligibility gate: candidacy status,
    // candidate liveness, and the election window are re-asserted in the
    // same statement as the INSERT, not just in the earlier plain reads.
    expect(insertSql).toContain("status NOT IN ('withdrawn', 'lost')");
    expect(insertSql).toContain("deleted_at IS NULL");
    expect(insertSql).toContain("merged_into_candidate_id IS NULL");
    expect(insertSql).toContain("election_date >=");
    expect(client.query.mock.calls[7]?.[0]).toBe("COMMIT");
    expect(client.release).toHaveBeenCalledOnce();
  });

  it("attaches the canonical result to the list read, so history keeps election-night calls", async () => {
    // Picks history outlives the ballot's just-finished window; without
    // this, an election-night "advanced" would vanish from history until
    // certification flips candidacy_status — weeks later.
    const query = vi
      .fn()
      .mockResolvedValueOnce({ rows: [{ id: userId }] })
      .mockResolvedValueOnce({
        rows: [
          {
            election_id: electionId,
            race_type: "office",
            official_ballot_title: "Governor",
            election_date: "2026-08-04",
            seats_to_fill: 1,
            candidate_id: candidateId,
            display_name: "Jocelyn Benson",
            candidacy_status: "declared",
            measure_position: null,
            measure_result: null,
            updated_at: "2026-08-02T17:00:00.000Z",
          },
        ],
      })
      .mockResolvedValueOnce({
        rows: [
          {
            election_id: electionId,
            outcome: "advanced",
            winners: [{ candidate_id: candidateId, candidate_name: "Jocelyn Benson", party: "Democratic" }],
          },
        ],
      });

    const result = await listUserElectionChoices({ query }, userId);

    expect(result.choices).toEqual([
      expect.objectContaining({
        election_id: electionId,
        current_result_outcome: "advanced",
        current_result_winners: [
          { candidate_id: candidateId, candidate_name: "Jocelyn Benson", party: "Democratic" },
        ],
      }),
    ]);
    // The canonical-result query is scoped to exactly the listed elections.
    expect(query).toHaveBeenCalledTimes(3);
    expect(query.mock.calls[2][1]).toEqual([[electionId]]);
  });

  it("skips the canonical-result query when the user has no choices", async () => {
    const query = vi
      .fn()
      .mockResolvedValueOnce({ rows: [{ id: userId }] })
      .mockResolvedValueOnce({ rows: [] });

    await expect(listUserElectionChoices({ query }, userId)).resolves.toEqual({ choices: [] });
    expect(query).toHaveBeenCalledTimes(2);
  });

  it("refuses the pick when the candidacy is withdrawn between validation and write", async () => {
    const { db, client } = createMockTransactionalDb();
    const election = {
      id: electionId,
      race_type: "office",
      election_date: "2026-08-18",
      seats_to_fill: 1,
      is_upcoming: true,
    };
    client.query
      .mockResolvedValueOnce({ rows: [] }) // BEGIN
      .mockResolvedValueOnce({ rows: [{ id: userId }] }) // user FOR UPDATE
      .mockResolvedValueOnce({ rows: [election] }) // readElection
      .mockResolvedValueOnce({ rows: [{ candidate_id: candidateId }] }) // pre-check passes
      .mockResolvedValueOnce({ rows: [{ count: "0" }] }) // seat-cap count
      .mockResolvedValueOnce({ rows: [], rowCount: 0 }) // gated INSERT refuses
      .mockResolvedValueOnce({ rows: [election] }) // disambiguating re-read: election still open
      .mockResolvedValueOnce({ rows: [] }); // ROLLBACK

    await expect(
      setUserElectionChoice(db, userId, { electionId, candidateId, chosen: true })
    ).rejects.toMatchObject({ code: "candidacy_not_available" });

    expect(client.query.mock.calls.map((call) => String(call[0])).at(-1)).toBe("ROLLBACK");
    expect(client.release).toHaveBeenCalledOnce();
  });
});
