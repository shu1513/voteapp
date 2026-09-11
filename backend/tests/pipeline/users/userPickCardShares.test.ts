import { describe, expect, it, vi } from "vitest";

import {
  deleteUserPickCardShare,
  listUserPickCardShares,
  lookupPublicPickCard,
} from "../../../src/pipeline/users/userPickCardShares.js";

const electionId = "22222222-2222-4222-8222-222222222222";
const candidateId = "33333333-3333-4333-8333-333333333333";
const token = "tok_abcdefghijklmnopqrstuvwxyz012345";

function cardRow(overrides: Record<string, unknown> = {}) {
  return {
    first_name: "Ava",
    election_date: "2026-08-04",
    election_id: electionId,
    official_ballot_title: "Governor",
    race_type: "office",
    district_name: "Michigan",
    candidate_id: candidateId,
    display_name: "Jocelyn Benson",
    candidacy_status: "declared",
    measure_position: null,
    measure_result: null,
    ...overrides,
  };
}

describe("lookupPublicPickCard", () => {
  it("attaches the canonical result so the card can flag election-night calls", async () => {
    const query = vi
      .fn()
      .mockResolvedValueOnce({ rows: [cardRow()] })
      .mockResolvedValueOnce({
        rows: [
          {
            election_id: electionId,
            outcome: "advanced",
            winners: [
              { candidate_id: candidateId, candidate_name: "Jocelyn Benson", party: "Democratic" },
              // Malformed / blank entries drop out instead of failing the card.
              { candidate_id: "  " },
              "not-an-object",
            ],
          },
        ],
      });

    const card = await lookupPublicPickCard({ query }, token);

    expect(card?.entries).toEqual([
      expect.objectContaining({
        election_id: electionId,
        current_result_outcome: "advanced",
        current_result_winners: [
          { candidate_id: candidateId, candidate_name: "Jocelyn Benson", party: "Democratic" },
        ],
      }),
    ]);
    // The canonical-result query is scoped to exactly the card's elections.
    expect(query).toHaveBeenCalledTimes(2);
    expect(query.mock.calls[1][1]).toEqual([[electionId]]);
  });

  it("leaves the result fields empty when no decisive result is recorded", async () => {
    const query = vi
      .fn()
      .mockResolvedValueOnce({ rows: [cardRow()] })
      .mockResolvedValueOnce({ rows: [] });

    const card = await lookupPublicPickCard({ query }, token);

    expect(card?.entries).toEqual([
      expect.objectContaining({
        current_result_outcome: null,
        current_result_winners: [],
      }),
    ]);
  });

  it("skips the result lookup entirely for a bare card with zero surviving picks", async () => {
    const query = vi
      .fn()
      // Main join: no rows (picks cleared since sharing).
      .mockResolvedValueOnce({ rows: [] })
      // Bare-share fallback: the token still resolves.
      .mockResolvedValueOnce({ rows: [{ first_name: "Ava", election_date: "2026-08-04" }] });

    const card = await lookupPublicPickCard({ query }, token);

    expect(card).toEqual({ first_name: "Ava", election_date: "2026-08-04", entries: [] });
    expect(query).toHaveBeenCalledTimes(2);
  });
});

describe("listUserPickCardShares", () => {
  const userId = "99999999-9999-4999-8999-999999999999";

  it("returns every live share for the user, scoped by user id", async () => {
    const query = vi.fn().mockResolvedValueOnce({
      rows: [
        { token, election_date: "2026-11-03" },
        { token: "tok_second_share_0123456789abcdefghij", election_date: "2026-08-04" },
      ],
    });

    const result = await listUserPickCardShares({ query }, userId);

    expect(result).toEqual({
      shares: [
        { token, election_date: "2026-11-03" },
        { token: "tok_second_share_0123456789abcdefghij", election_date: "2026-08-04" },
      ],
    });
    expect(query).toHaveBeenCalledTimes(1);
    expect(query.mock.calls[0][1]).toEqual([userId]);
  });

  it("rejects a malformed user id before touching the database", async () => {
    const query = vi.fn();
    await expect(listUserPickCardShares({ query }, "not-a-uuid")).rejects.toMatchObject({ code: "invalid_user_id" });
    expect(query).not.toHaveBeenCalled();
  });
});

describe("deleteUserPickCardShare", () => {
  const userId = "99999999-9999-4999-8999-999999999999";

  it("deletes the one (user, date) row and reports it", async () => {
    const query = vi.fn().mockResolvedValueOnce({ rowCount: 1, rows: [] });

    const result = await deleteUserPickCardShare({ query }, userId, " 2026-11-03 ");

    expect(result).toEqual({ deleted: true });
    expect(query).toHaveBeenCalledTimes(1);
    expect(query.mock.calls[0][0]).toMatch(/DELETE FROM public\.user_pick_card_shares/);
    expect(query.mock.calls[0][1]).toEqual([userId, "2026-11-03"]);
  });

  it("is idempotent: a missing share reports deleted=false instead of failing", async () => {
    const query = vi.fn().mockResolvedValueOnce({ rowCount: 0, rows: [] });
    await expect(deleteUserPickCardShare({ query }, userId, "2026-11-03")).resolves.toEqual({ deleted: false });
  });

  it("rejects a non-calendar date before touching the database", async () => {
    const query = vi.fn();
    await expect(deleteUserPickCardShare({ query }, userId, "2026-02-30")).rejects.toMatchObject({
      code: "invalid_election_date",
    });
    expect(query).not.toHaveBeenCalled();
  });
});
