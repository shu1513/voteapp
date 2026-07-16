import { afterEach, describe, expect, it, vi } from "vitest";

import type { BallotLookupFinanceSummary } from "../../../src/pipeline/address/ballotLookupFinanceShared.js";

// Finance summaries enrich the ballot lookup; a broken finance module must
// degrade to "no summaries from that source" instead of failing the whole
// lookup. These tests mock three loader modules directly — every other
// adapter self-disables because its feature flag is unset here.
vi.mock("../../../src/pipeline/wisconsinFinance/wisconsinBallotLookupFinanceLoader.js", () => ({
  loadWisconsinCandidateFinanceSummariesByCandidateElection: vi.fn(),
}));
vi.mock("../../../src/pipeline/massachusettsFinance/massachusettsBallotLookupFinanceLoader.js", () => ({
  loadMassachusettsCandidateFinanceSummariesByCandidateElection: vi.fn(),
}));
vi.mock("../../../src/pipeline/finance/fecBallotLookupFinanceLoader.js", () => ({
  loadFecCandidateFinanceSummariesByCandidateElection: vi.fn(),
}));
// Fault-isolated failures are Sentry capture points of their own (the API
// error middleware never sees swallowed errors), so pin the capture calls.
vi.mock("../../../src/observability/sentry.js", () => ({
  captureError: vi.fn(),
}));

import { captureError } from "../../../src/observability/sentry.js";
import { loadCandidateFinanceSummariesByCandidateElection } from "../../../src/pipeline/address/ballotLookup.js";
import { loadFecCandidateFinanceSummariesByCandidateElection } from "../../../src/pipeline/finance/fecBallotLookupFinanceLoader.js";
import { loadMassachusettsCandidateFinanceSummariesByCandidateElection } from "../../../src/pipeline/massachusettsFinance/massachusettsBallotLookupFinanceLoader.js";
import { loadWisconsinCandidateFinanceSummariesByCandidateElection } from "../../../src/pipeline/wisconsinFinance/wisconsinBallotLookupFinanceLoader.js";

const CANDIDATE_ID = "11111111-1111-4111-8111-111111111111";
const ELECTION_ID = "22222222-2222-4222-8222-222222222222";
const KEY = `${CANDIDATE_ID}\u0000${ELECTION_ID}`;

const SUMMARY: BallotLookupFinanceSummary = {
  source: "MASSACHUSETTS_OCPF",
  cycle: 2026,
  fec_candidate_id: null,
  controlled_committee_id: "C123",
  last_synced_at: "2026-07-01T00:00:00Z",
  direct_campaign: {
    total_raised: 1000,
    total_spent: 500,
    cash_on_hand: 500,
    debts_owed: null,
    top_occupations: [],
    top_employers: [],
    top_industries: [],
    contribution_size_buckets: [],
  },
  outside_spending: {
    support_total: null,
    oppose_total: null,
    top_supporting_groups: [],
    top_opposing_groups: [],
    top_supporting_industries: [],
    top_opposing_industries: [],
  },
  backing_summary: {
    top_direct_donor_occupations: [],
    top_outside_supporting_industries: [],
  },
};

const wisconsinLoad = vi.mocked(loadWisconsinCandidateFinanceSummariesByCandidateElection);
const massachusettsLoad = vi.mocked(loadMassachusettsCandidateFinanceSummariesByCandidateElection);
const fecLoad = vi.mocked(loadFecCandidateFinanceSummariesByCandidateElection);

function arrange(input: { wisconsin: "ok" | "fail"; massachusetts: "ok" | "fail"; fec: "ok" | "fail" }) {
  wisconsinLoad.mockImplementation(async () => {
    if (input.wisconsin === "fail") throw new Error("wisconsin finance exploded");
    return new Map();
  });
  massachusettsLoad.mockImplementation(async () => {
    if (input.massachusetts === "fail") throw new Error("massachusetts finance exploded");
    return new Map([[KEY, SUMMARY]]);
  });
  fecLoad.mockImplementation(async () => {
    if (input.fec === "fail") throw new Error("fec finance exploded");
    return new Map();
  });
}

async function run() {
  return loadCandidateFinanceSummariesByCandidateElection(
    { query: vi.fn() },
    [{ candidate_id: CANDIDATE_ID, election_id: ELECTION_ID, fec_ids: null }],
    [
      {
        election_id: ELECTION_ID,
        district_id: "33333333-3333-4333-8333-333333333333",
        district_type: "state",
        geoid_compact: "25",
        district_name: "Massachusetts",
        state: "MA",
        state_fips: "25",
        representation_power_score: null,
        race_type: "regular",
        official_ballot_title: "Governor",
        election_date: "2026-11-03",
        election_stage: null,
        is_partisan: null,
        discovery_contest_family: null,
        sources: null,
      },
    ]
  );
}

afterEach(() => {
  vi.restoreAllMocks();
});

describe("ballot lookup finance fault isolation", () => {
  it("a throwing state adapter is skipped and the remaining sources still load", async () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    arrange({ wisconsin: "fail", massachusetts: "ok", fec: "ok" });

    const merged = await run();

    expect(merged.get(KEY)).toEqual(SUMMARY);
    expect(merged.size).toBe(1);
    expect(massachusettsLoad).toHaveBeenCalledTimes(1);
    expect(fecLoad).toHaveBeenCalledTimes(1);
    expect(warn).toHaveBeenCalledTimes(1);
    expect(warn).toHaveBeenCalledWith(
      "candidate finance summaries failed; continuing without this source:",
      expect.objectContaining({
        source: "WI",
        // describeError logs the scrubbed stack string, never the raw object
        reason: expect.stringContaining("wisconsin finance exploded"),
      })
    );
    expect(captureError).toHaveBeenCalledTimes(1);
    expect(captureError).toHaveBeenCalledWith(
      expect.objectContaining({ message: "wisconsin finance exploded" }),
      expect.objectContaining({ finance_source: "WI" })
    );
  });

  it("a throwing FEC loader keeps the state summaries already merged", async () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    arrange({ wisconsin: "ok", massachusetts: "ok", fec: "fail" });

    const merged = await run();

    expect(merged.get(KEY)).toEqual(SUMMARY);
    expect(merged.size).toBe(1);
    expect(warn).toHaveBeenCalledTimes(1);
    expect(warn).toHaveBeenCalledWith(
      "candidate finance summaries failed; continuing without this source:",
      expect.objectContaining({
        source: "FEC",
        reason: expect.stringContaining("fec finance exploded"),
      })
    );
    expect(captureError).toHaveBeenCalledWith(
      expect.objectContaining({ message: "fec finance exploded" }),
      expect.objectContaining({ finance_source: "FEC" })
    );
  });

  it("every source failing degrades to an empty map instead of throwing", async () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    arrange({ wisconsin: "fail", massachusetts: "fail", fec: "fail" });

    const merged = await run();

    expect(merged.size).toBe(0);
    expect(warn).toHaveBeenCalledTimes(3);
    expect(captureError).toHaveBeenCalledTimes(3);
  });

  it("no warning or capture happens when every source succeeds", async () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    arrange({ wisconsin: "ok", massachusetts: "ok", fec: "ok" });

    const merged = await run();

    expect(merged.get(KEY)).toEqual(SUMMARY);
    expect(warn).not.toHaveBeenCalled();
    expect(captureError).not.toHaveBeenCalled();
  });
});
