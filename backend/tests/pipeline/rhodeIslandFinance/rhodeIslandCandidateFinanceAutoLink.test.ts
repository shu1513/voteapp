import { describe, expect, it, vi } from "vitest";

import {
  autoLinkMissingRhodeIslandCandidateFinanceLinks,
  buildRhodeIslandFinanceLinkCompletenessReport,
  listRhodeIslandCandidateElectionsMissingFinanceLinks,
  tallyRhodeIslandFinanceAutoLinkResults,
  type RhodeIslandFinanceAutoLinkCandidateElection,
  type RhodeIslandFinanceAutoLinkResult,
} from "../../../src/pipeline/rhodeIslandFinance/rhodeIslandCandidateFinanceAutoLink.js";
import type { RhodeIslandCandidateCommitteeResolution } from "../../../src/pipeline/rhodeIslandFinance/rhodeIslandCandidateCommitteeResolver.js";
import type { ErtsTransport } from "../../../src/pipeline/rhodeIslandFinance/rhodeIslandErtsClient.js";
import { RHODE_ISLAND_FINANCE_ELIGIBLE_OFFICE_KEYS } from "../../../src/pipeline/rhodeIslandFinance/rhodeIslandFinanceEligibleOffices.js";

const NOW = new Date("2026-08-13T00:00:00.000Z");
const CANDIDATE_ID = "11111111-1111-4111-8111-111111111111";
const ELECTION_ID = "22222222-2222-4222-8222-222222222222";

const NOOP_TRANSPORT: ErtsTransport = {
  fetch: async () => {
    throw new Error("transport should not be used when a resolver stub is injected");
  },
};

function candidateElection(
  overrides: Partial<RhodeIslandFinanceAutoLinkCandidateElection> = {}
): RhodeIslandFinanceAutoLinkCandidateElection {
  return {
    candidateId: CANDIDATE_ID,
    electionId: ELECTION_ID,
    candidateName: "Daniel McKee",
    electionYear: 2026,
    officeScope: "statewide",
    officeName: "Governor",
    district: null,
    ...overrides,
  };
}

function matchedResolution(): RhodeIslandCandidateCommitteeResolution {
  return {
    status: "matched",
    orgId: "2235",
    organizationName: "DANIEL J MCKEE",
    searchLastName: "MCKEE",
    confidence: "exact",
    source: "erts_organization_search",
    sourceUrl: "https://www.ricampaignfinance.com/RIPublic/Contributions.aspx",
    inactiveMatchCount: 0,
  };
}

// db.query stub covering both statements the linked path issues: the
// duplicate-claim guard SELECT (no claim) and the writer's link upsert.
function linkableDb(claimRows: Array<{ candidate_id: string; election_id: string }> = []) {
  return {
    query: vi.fn().mockImplementation(async (sql: string) => {
      if (sql.includes("link.committee_id = $1")) return { rows: claimRows };
      return { rows: [{ id: "link-1" }] };
    }),
  };
}

describe("listRhodeIslandCandidateElectionsMissingFinanceLinks", () => {
  it("queries RI candidate elections without an active link, bound to eligible offices", async () => {
    const db = { query: vi.fn().mockResolvedValue({ rows: [] }) };
    await listRhodeIslandCandidateElectionsMissingFinanceLinks(db, {
      now: NOW,
      maxCandidates: 25,
      electionLookbackDays: 30,
      electionLookaheadDays: 730,
    });
    const [sql, params] = db.query.mock.calls[0]!;
    expect(sql).toContain("district.state = 'RI'");
    expect(sql).toContain("public.ri_candidate_finance_links");
    expect(sql).toContain("link.link_status = 'active'");
    expect(params).toEqual([NOW.toISOString(), 25, 30, 730, [...RHODE_ISLAND_FINANCE_ELIGIBLE_OFFICE_KEYS]]);
  });
});

describe("autoLinkMissingRhodeIslandCandidateFinanceLinks", () => {
  it("writes an active erts_portal link for a matched resolution", async () => {
    const db = linkableDb();
    const results = await autoLinkMissingRhodeIslandCandidateFinanceLinks({
      db,
      transport: NOOP_TRANSPORT,
      now: NOW,
      maxCandidates: 10,
      electionLookbackDays: 30,
      electionLookaheadDays: 730,
      candidateElections: [candidateElection()],
      resolveCandidateCommittee: async () => matchedResolution(),
    });
    expect(results).toEqual([
      { candidateId: CANDIDATE_ID, electionId: ELECTION_ID, status: "linked", committeeId: "2235" },
    ]);
    const upsert = db.query.mock.calls.find(([sql]) => sql.includes("ri_candidate_finance_links") && sql.includes("INSERT"));
    expect(upsert).toBeDefined();
    expect(upsert![1]).toEqual(
      expect.arrayContaining(["2235", "DANIEL J MCKEE", "active", "erts_portal", "DANIEL MCKEE"])
    );
  });

  it("passes the RI cycle window to the resolver", async () => {
    const db = linkableDb();
    const seen: Array<{ cycleBeginUs: string; cycleEndUs: string }> = [];
    await autoLinkMissingRhodeIslandCandidateFinanceLinks({
      db,
      transport: NOOP_TRANSPORT,
      now: NOW,
      maxCandidates: 10,
      electionLookbackDays: 30,
      electionLookaheadDays: 730,
      candidateElections: [candidateElection()],
      resolveCandidateCommittee: async (input) => {
        seen.push({ cycleBeginUs: input.cycleBeginUs, cycleEndUs: input.cycleEndUs });
        return matchedResolution();
      },
    });
    expect(seen).toEqual([{ cycleBeginUs: "01/01/2025", cycleEndUs: "12/31/2026" }]);
  });

  it("refuses a committee already actively linked to another candidate in the cycle", async () => {
    // Name is the ONLY evidence in RI, so two same-named candidates in
    // different districts would both resolve to the one registered
    // organization — the claim guard is the backstop.
    const db = linkableDb([{ candidate_id: "other-candidate", election_id: "other-election" }]);
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    try {
      const results = await autoLinkMissingRhodeIslandCandidateFinanceLinks({
        db,
        transport: NOOP_TRANSPORT,
        now: NOW,
        maxCandidates: 10,
        electionLookbackDays: 30,
        electionLookaheadDays: 730,
        candidateElections: [candidateElection()],
        resolveCandidateCommittee: async () => matchedResolution(),
      });
      expect(results).toEqual([
        {
          candidateId: CANDIDATE_ID,
          electionId: ELECTION_ID,
          status: "needs_review",
          reason: "committee_linked_to_another_candidate",
          committeeId: "2235",
        },
      ]);
      const upsert = db.query.mock.calls.find(([sql]) => sql.includes("INSERT"));
      expect(upsert).toBeUndefined();
      expect(warn).toHaveBeenCalled();
    } finally {
      warn.mockRestore();
    }
  });

  it("reports ambiguous resolutions without writing anything", async () => {
    const db = { query: vi.fn() };
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    try {
      const results = await autoLinkMissingRhodeIslandCandidateFinanceLinks({
        db,
        transport: NOOP_TRANSPORT,
        now: NOW,
        maxCandidates: 10,
        electionLookbackDays: 30,
        electionLookaheadDays: 730,
        candidateElections: [candidateElection()],
        resolveCandidateCommittee: async () => ({
          status: "ambiguous",
          reason: "multiple_active_organization_matches",
          candidateNameNormalized: "DANIEL MCKEE",
          matches: [
            { organizationName: "DANIEL J MCKEE", postbackTarget: "a", status: "Active" },
            { organizationName: "DANIEL J MCKEE", postbackTarget: "b", status: "Active" },
          ],
        }),
      });
      expect(results).toEqual([
        {
          candidateId: CANDIDATE_ID,
          electionId: ELECTION_ID,
          status: "ambiguous",
          reason: "multiple_active_organization_matches",
        },
      ]);
      expect(db.query).not.toHaveBeenCalled();
      expect(warn).toHaveBeenCalled();
    } finally {
      warn.mockRestore();
    }
  });

  it("reports unmatched resolutions without writing anything", async () => {
    const db = { query: vi.fn() };
    const results = await autoLinkMissingRhodeIslandCandidateFinanceLinks({
      db,
      transport: NOOP_TRANSPORT,
      now: NOW,
      maxCandidates: 10,
      electionLookbackDays: 30,
      electionLookaheadDays: 730,
      candidateElections: [candidateElection()],
      resolveCandidateCommittee: async () => ({
        status: "unmatched",
        reason: "no_organization_match",
        candidateNameNormalized: "DANIEL MCKEE",
      }),
    });
    expect(results[0]).toMatchObject({ status: "unmatched", reason: "no_organization_match" });
    expect(db.query).not.toHaveBeenCalled();
  });

  it("captures per-candidate failures and continues — including an odd election year", async () => {
    const db = linkableDb();
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    try {
      const oddYear = candidateElection({ electionYear: 2025 });
      const succeeding = candidateElection({
        candidateId: "33333333-3333-4333-8333-333333333333",
      });
      const results = await autoLinkMissingRhodeIslandCandidateFinanceLinks({
        db,
        transport: NOOP_TRANSPORT,
        now: NOW,
        maxCandidates: 10,
        electionLookbackDays: 30,
        electionLookaheadDays: 730,
        candidateElections: [oddYear, succeeding],
        resolveCandidateCommittee: async () => matchedResolution(),
      });
      expect(results[0]).toMatchObject({
        status: "error",
        reason: "auto_link_failed",
        error: expect.stringContaining("even election year"),
      });
      expect(results[1]).toMatchObject({ status: "linked" });
    } finally {
      warn.mockRestore();
    }
  });
});

describe("completeness report", () => {
  const results: RhodeIslandFinanceAutoLinkResult[] = [
    { candidateId: "a", electionId: "e1", status: "linked", committeeId: "2235" },
    { candidateId: "b", electionId: "e2", status: "unmatched", reason: "no_organization_match" },
    { candidateId: "c", electionId: "e3", status: "unmatched", reason: "no_organization_match" },
    { candidateId: "d", electionId: "e4", status: "unmatched", reason: "paginated_search_results" },
    { candidateId: "e", electionId: "e5", status: "ambiguous", reason: "multiple_active_organization_matches" },
    { candidateId: "f", electionId: "e6", status: "needs_review", reason: "committee_linked_to_another_candidate" },
    { candidateId: "g", electionId: "e7", status: "error", reason: "auto_link_failed", error: "boom" },
  ];

  it("tallies a run into the zero/one/multi-match split", () => {
    expect(tallyRhodeIslandFinanceAutoLinkResults(results)).toEqual({
      attempted: 7,
      linked: 1,
      ambiguous: 1,
      needsReview: 1,
      errors: 1,
      unmatchedByReason: {
        no_organization_match: 2,
        paginated_search_results: 1,
      },
    });
  });

  it("combines coverage counts with the run tally and a linked percentage", async () => {
    const db = {
      query: vi.fn().mockResolvedValue({ rows: [{ eligible_count: "40", linked_count: "25" }] }),
    };
    const report = await buildRhodeIslandFinanceLinkCompletenessReport(db, {
      now: NOW,
      electionLookbackDays: 30,
      electionLookaheadDays: 730,
      autoLinkResults: results,
    });
    expect(report).toMatchObject({
      eligibleCandidateElectionCount: 40,
      activeLinkedCandidateElectionCount: 25,
      linkedPercentage: 62.5,
    });
    expect(report.autoLink.attempted).toBe(7);
    const [sql, params] = db.query.mock.calls[0]!;
    expect(sql).toContain("district.state = 'RI'");
    expect(params).toEqual([NOW.toISOString(), 30, 730, [...RHODE_ISLAND_FINANCE_ELIGIBLE_OFFICE_KEYS]]);
  });

  it("reports null percentage over an empty population", async () => {
    const db = {
      query: vi.fn().mockResolvedValue({ rows: [{ eligible_count: "0", linked_count: "0" }] }),
    };
    const report = await buildRhodeIslandFinanceLinkCompletenessReport(db, {
      now: NOW,
      electionLookbackDays: 30,
      electionLookaheadDays: 730,
      autoLinkResults: [],
    });
    expect(report.linkedPercentage).toBeNull();
  });
});
