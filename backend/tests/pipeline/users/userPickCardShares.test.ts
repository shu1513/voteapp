import { describe, expect, it, vi } from "vitest";

import { lookupPublicPickCard } from "../../../src/pipeline/users/userPickCardShares.js";

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
