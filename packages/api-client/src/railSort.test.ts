import { describe, expect, it } from "vitest";

import { railSortsOffered, sortRailEntries, type RailSortEntry } from "./railSort";
import { buildResearchAreaWeights } from "./researchAreaScoring";

function entry(id: string, overrides: Partial<RailSortEntry> = {}): RailSortEntry {
  return {
    id,
    title: id,
    race_type: "office",
    vote_power_score: null,
    election_date: "2026-11-03",
    research_area_ids: [],
    ...overrides,
  };
}

describe("railSortsOffered", () => {
  const KEYED = [entry("a"), entry("b")];

  it("offers everything but my_areas without saved areas", () => {
    expect(railSortsOffered(KEYED, false)).toEqual(["vote_power", "soonest", "alphabetical"]);
    expect(railSortsOffered(KEYED, true)).toEqual([
      "my_areas",
      "vote_power",
      "soonest",
      "alphabetical",
    ]);
  });

  it("offers nothing on an unkeyed (pre-deploy) snapshot or a single entry", () => {
    expect(railSortsOffered([{ id: "a", title: "A" }, { id: "b", title: "B" }], true)).toEqual([]);
    expect(railSortsOffered([entry("a")], true)).toEqual([]);
  });

  it("withholds only my_areas when area ids are missing from an entry", () => {
    const noAreas = [entry("a"), { ...entry("b"), research_area_ids: undefined }];
    expect(railSortsOffered(noAreas, true)).toEqual(["vote_power", "soonest", "alphabetical"]);
  });
});

describe("sortRailEntries", () => {
  it("vote_power: higher score first, unknown scores last, title tiebreak", () => {
    const sorted = sortRailEntries(
      [
        entry("low", { vote_power_score: 1 }),
        entry("unknown", { vote_power_score: null }),
        entry("high", { vote_power_score: 9 }),
      ],
      "vote_power"
    );
    expect(sorted.map((e) => e.id)).toEqual(["high", "low", "unknown"]);
  });

  it("soonest: earliest date first", () => {
    const sorted = sortRailEntries(
      [
        entry("later", { election_date: "2026-11-03" }),
        entry("sooner", { election_date: "2026-08-18" }),
      ],
      "soonest"
    );
    expect(sorted.map((e) => e.id)).toEqual(["sooner", "later"]);
  });

  it("alphabetical is numeric-aware: Proposition 4 before Proposition 33", () => {
    const sorted = sortRailEntries(
      [
        entry("p33", { title: "Proposition 33: Rent" }),
        entry("p4", { title: "Proposition 4: Bonds" }),
      ],
      "alphabetical"
    );
    expect(sorted.map((e) => e.id)).toEqual(["p4", "p33"]);
  });

  it("my_areas: summed matched weights first, best rank breaks ties, vote power after", () => {
    // rank 1 → weight 7, rank 2 → weight 6, rank 3 → weight 5.
    const weights = buildResearchAreaWeights([
      { research_area_id: "a-1", slug: "s1", name: "n1", description: null, rank: 1 },
      { research_area_id: "a-2", slug: "s2", name: "n2", description: null, rank: 2 },
      { research_area_id: "a-3", slug: "s3", name: "n3", description: null, rank: 3 },
    ]);
    const sorted = sortRailEntries(
      [
        entry("none", { vote_power_score: 99 }),
        entry("second-and-third", { research_area_ids: ["a-2", "a-3"] }), // 11
        entry("top-only", { research_area_ids: ["a-1"] }), // 7
        entry("top-and-third", { research_area_ids: ["a-1", "a-3"] }), // 12
      ],
      "my_areas",
      weights
    );
    expect(sorted.map((e) => e.id)).toEqual(["top-and-third", "second-and-third", "top-only", "none"]);
  });

  it("my_areas without weights degrades to vote_power order", () => {
    const sorted = sortRailEntries(
      [
        entry("weak", { vote_power_score: 1, research_area_ids: ["a-1"] }),
        entry("strong", { vote_power_score: 5 }),
      ],
      "my_areas"
    );
    expect(sorted.map((e) => e.id)).toEqual(["strong", "weak"]);
  });

  it("keeps the awaiting-candidates tail sunk under every sort", () => {
    const entries = [
      entry("awaiting-high", { vote_power_score: 99, awaiting_candidates: true, title: "AAA" }),
      entry("readable", { vote_power_score: 1, title: "ZZZ" }),
    ];
    for (const sort of ["my_areas", "vote_power", "soonest", "alphabetical"] as const) {
      expect(
        sortRailEntries(entries, sort).map((e) => e.id),
        sort
      ).toEqual(["readable", "awaiting-high"]);
    }
  });

  it("does not mutate the input", () => {
    const entries = [entry("b"), entry("a")];
    sortRailEntries(entries, "alphabetical");
    expect(entries.map((e) => e.id)).toEqual(["b", "a"]);
  });
});
