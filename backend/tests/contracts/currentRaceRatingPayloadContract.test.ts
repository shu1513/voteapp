import { describe, expect, it } from "vitest";

import {
  parseCurrentRaceRatingPayload,
  type CurrentRaceRatingContext,
} from "../../src/contracts/currentRaceRatingPayloadContract.js";

const ELECTION_A = "11111111-1111-4111-8111-111111111111";
const ELECTION_B = "22222222-2222-4222-8222-222222222222";

function context(overrides: Partial<CurrentRaceRatingContext> = {}): CurrentRaceRatingContext {
  return {
    electionId: ELECTION_A,
    isDcDelegate: false,
    ...overrides,
  };
}

function ieObservation(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    outlet: "inside_elections",
    raw_rating: "Tilt Democrat",
    favored: "D",
    intensity: 2,
    as_of: "2026-08-06",
    url: "https://insideelections.com/ratings/senate",
    ...overrides,
  };
}

function sabatoObservation(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    outlet: "sabato",
    raw_rating: "Leans Democratic",
    favored: "D",
    intensity: 3,
    as_of: "2026-07-30",
    url: "https://centerforpolitics.org/crystalball/2026-senate",
    ...overrides,
  };
}

function ratedRow(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    election_id: ELECTION_A,
    method: "outlet_consensus",
    evidence_status: "rated",
    observations: [ieObservation(), sabatoObservation()],
    source_url: "https://en.wikipedia.org/wiki/2026_United_States_Senate_elections",
    ...overrides,
  };
}

function parse(rows: unknown[], contexts: CurrentRaceRatingContext[] = [context()]) {
  return parseCurrentRaceRatingPayload({ ratings: rows }, { contexts });
}

describe("parseCurrentRaceRatingPayload", () => {
  it("derives label, confidence, and feed-level as_of for a valid rated payload", () => {
    const result = parse([ratedRow()]);
    expect(result.ok).toBe(true);
    if (!result.ok) {
      return;
    }
    const row = result.payload.ratings[0]!;
    expect(row.competitiveness_label).toBe("competitive");
    expect(row.confidence).toBe("high");
    // as_of is the newest feed snapshot; each observation's own date stays in evidence.
    expect(row.as_of).toBe("2026-08-06");
    expect(row.evidence_status).toBe("rated");
    expect(row.method).toBe("outlet_consensus");
    expect(row.decisive_round).toBeNull();
    expect(row.evidence).toMatchObject({
      mean_intensity: 2.5,
      observations: [
        { outlet: "inside_elections", as_of: "2026-08-06" },
        { outlet: "sabato", as_of: "2026-07-30" },
      ],
    });
  });

  it("accepts a none_found row with null derived fields", () => {
    const result = parse([
      {
        election_id: ELECTION_A,
        method: "outlet_consensus",
        evidence_status: "none_found",
        source_url: "https://en.wikipedia.org/wiki/2026_United_States_Senate_elections",
        notes: "No outlet rates this race.",
      },
    ]);
    expect(result.ok).toBe(true);
    if (!result.ok) {
      return;
    }
    const row = result.payload.ratings[0]!;
    expect(row.competitiveness_label).toBeNull();
    expect(row.confidence).toBeNull();
    expect(row.as_of).toBeNull();
    expect(row.evidence).toEqual({ observations: [], notes: "No outlet rates this race." });
  });

  it("rejects payload rows that carry derived fields", () => {
    for (const field of ["competitiveness_label", "confidence", "as_of"]) {
      const result = parse([ratedRow({ [field]: "toss_up" })]);
      expect(result.ok).toBe(false);
      if (!result.ok) {
        expect(result.reason).toContain(`${field} is derived`);
      }
    }
  });

  it("rejects an election_id outside the provided context", () => {
    const result = parse([ratedRow({ election_id: ELECTION_B })]);
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.reason).toContain("outside provided context");
    }
  });

  it("rejects duplicate election ids and requires every context election", () => {
    const contexts = [context(), context({ electionId: ELECTION_B })];
    const duplicate = parse([ratedRow(), ratedRow()], contexts);
    expect(duplicate.ok).toBe(false);
    if (!duplicate.ok) {
      expect(duplicate.reason).toContain("duplicate election_id");
    }

    const missing = parse([ratedRow()], contexts);
    expect(missing.ok).toBe(false);
    if (!missing.ok) {
      expect(missing.reason).toContain(`missing election_id: ${ELECTION_B}`);
    }
  });

  it("rejects mayoral_rubric payloads until v1.1", () => {
    const result = parse([ratedRow({ method: "mayoral_rubric" })]);
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.reason).toContain("not supported until v1.1");
    }
  });

  it("rejects a rated row for the DC delegate race", () => {
    const result = parse([ratedRow()], [context({ isDcDelegate: true })]);
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.reason).toContain("DC delegate");
    }
  });

  it("still accepts a none_found row for the DC delegate race", () => {
    const result = parse(
      [
        {
          election_id: ELECTION_A,
          method: "outlet_consensus",
          evidence_status: "none_found",
          source_url: "https://en.wikipedia.org/wiki/District_of_Columbia%27s_at-large_congressional_district",
        },
      ],
      [context({ isDcDelegate: true })]
    );
    expect(result.ok).toBe(true);
  });

  it("rejects duplicate outlets in one election's observations", () => {
    const result = parse([ratedRow({ observations: [ieObservation(), ieObservation()] })]);
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.reason).toContain("duplicate outlet");
    }
  });

  it("rejects intensity values off the distance ladder", () => {
    const result = parse([ratedRow({ observations: [ieObservation({ intensity: 1 })] })]);
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.reason).toContain("intensity must be one of 0, 2, 3, 4, 5");
    }
  });

  it("rejects favored/intensity mismatches in both directions", () => {
    const tossUpWithSide = parse([
      ratedRow({ observations: [ieObservation({ intensity: 0, favored: "D", raw_rating: "Toss-up" })] }),
    ]);
    expect(tossUpWithSide.ok).toBe(false);
    if (!tossUpWithSide.ok) {
      expect(tossUpWithSide.reason).toContain("must use favored=none");
    }

    const leanWithoutSide = parse([ratedRow({ observations: [ieObservation({ favored: "none" })] })]);
    expect(leanWithoutSide.ok).toBe(false);
    if (!leanWithoutSide.ok) {
      expect(leanWithoutSide.reason).toContain("must name a favored side");
    }
  });

  it("rejects an observation url on the wrong outlet's domain", () => {
    const result = parse([
      ratedRow({
        observations: [ieObservation({ url: "https://en.wikipedia.org/wiki/2026_Senate" })],
      }),
    ]);
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.reason).toContain("must be on insideelections.com");
    }
  });

  it("rejects cookpolitical.com anywhere in the payload", () => {
    const result = parse([ratedRow({ source_url: "https://www.cookpolitical.com/ratings/senate" })]);
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.reason).toContain("banned as a rating source");
    }
  });

  it("rejects blocked source domains", () => {
    const result = parse([ratedRow({ source_url: "https://twitter.com/some-analyst/status/1" })]);
    expect(result.ok).toBe(false);
  });

  it("rejects malformed observation dates", () => {
    const result = parse([ratedRow({ observations: [ieObservation({ as_of: "2026-02-30" })] })]);
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.reason).toContain("valid YYYY-MM-DD");
    }
  });

  it("rejects a rated row without observations and a none_found row with them", () => {
    const noObservations = parse([ratedRow({ observations: [] })]);
    expect(noObservations.ok).toBe(false);
    if (!noObservations.ok) {
      expect(noObservations.reason).toContain("at least one observation");
    }

    const noneFoundWithObservations = parse([
      {
        election_id: ELECTION_A,
        method: "outlet_consensus",
        evidence_status: "none_found",
        observations: [ieObservation()],
        source_url: "https://en.wikipedia.org/wiki/2026_United_States_Senate_elections",
      },
    ]);
    expect(noneFoundWithObservations.ok).toBe(false);
    if (!noneFoundWithObservations.ok) {
      expect(noneFoundWithObservations.reason).toContain("must not include observations");
    }
  });

  it("rejects empty and oversized context lists", () => {
    expect(parseCurrentRaceRatingPayload({ ratings: [] }, { contexts: [] }).ok).toBe(false);
    const contexts = Array.from({ length: 11 }, (_, index) =>
      context({ electionId: `${index}0000000-0000-4000-8000-000000000000` })
    );
    expect(parseCurrentRaceRatingPayload({ ratings: [] }, { contexts }).ok).toBe(false);
  });
});
