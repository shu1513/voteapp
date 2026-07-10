import { afterEach, describe, expect, it, vi } from "vitest";

import { loadVermontCandidateFinanceSummariesByCandidateElection } from "../../../src/pipeline/vermontFinance/vermontBallotLookupFinanceLoader.js";

afterEach(() => {
  vi.unstubAllEnvs();
});

const CANDIDATE_ID = "11111111-1111-4111-8111-111111111111";
const ELECTION_ID = "22222222-2222-4222-8222-222222222222";

describe("vermontBallotLookupFinanceLoader", () => {
  // Pins the loader-internal flag gate the Phase 3 registry relies on
  // (adapters carry no enabled-checks; every loader must self-gate). The
  // enabled path is exercised through ballotLookup.test.ts.
  it("returns an empty map without querying when Vermont campaign finance is disabled", async () => {
    const query = vi.fn();

    const result = await loadVermontCandidateFinanceSummariesByCandidateElection(
      { query },
      [{ candidate_id: CANDIDATE_ID, election_id: ELECTION_ID }],
      [{ election_id: ELECTION_ID, state: "VT", office_scope: "statewide", office_canonical_name: "Governor" }]
    );

    expect(result.size).toBe(0);
    expect(query).not.toHaveBeenCalled();
  });
});
