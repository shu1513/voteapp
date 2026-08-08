import { describe, expect, it, vi } from "vitest";

import {
  autoLinkMissingGeorgiaCandidateFinanceLinks,
  listGeorgiaCandidateElectionsMissingFinanceLinks,
  type GeorgiaFinanceAutoLinkCandidateElection,
} from "../../../src/pipeline/georgiaFinance/georgiaCandidateFinanceAutoLink.js";
import type { GeorgiaCandidateCommitteeResolution } from "../../../src/pipeline/georgiaFinance/georgiaCandidateCommitteeResolver.js";
import type { GeorgiaEthicsTransport } from "../../../src/pipeline/georgiaFinance/georgiaEthicsClient.js";
import { GEORGIA_FINANCE_ELIGIBLE_OFFICE_KEYS } from "../../../src/pipeline/georgiaFinance/georgiaFinanceEligibleOffices.js";

const NOW = new Date("2026-08-07T00:00:00.000Z");
const CANDIDATE_ID = "11111111-1111-4111-8111-111111111111";
const ELECTION_ID = "22222222-2222-4222-8222-222222222222";

const NOOP_TRANSPORT: GeorgiaEthicsTransport = {
  postJson: async () => {
    throw new Error("transport should not be used when a resolver stub is injected");
  },
};

function candidateElection(
  overrides: Partial<GeorgiaFinanceAutoLinkCandidateElection> = {}
): GeorgiaFinanceAutoLinkCandidateElection {
  return {
    candidateId: CANDIDATE_ID,
    electionId: ELECTION_ID,
    candidateName: "Christopher Carr",
    electionYear: 2026,
    officeScope: "statewide",
    officeName: "Governor",
    district: null,
    ...overrides,
  };
}

function matchedResolution(): GeorgiaCandidateCommitteeResolution {
  return {
    status: "matched",
    filerEntityId: "100035",
    registrationGuid: "d973ab3b-54c2-416e-81ce-f5b1ee9a6f57",
    committeeName: "Carr for Georgia, Inc.",
    filerName: "Carr, Christopher M.",
    office: "Governor",
    districtName: null,
    filerStatusCode: "FACT",
    confidence: "exact",
    source: "peachfile_candidate_index",
    sourceUrl: "https://ethics.ga.gov/records-search-all/",
    matchedRowCount: 1,
  };
}

describe("listGeorgiaCandidateElectionsMissingFinanceLinks", () => {
  it("queries GA candidate elections without an active link, bound to eligible offices", async () => {
    const db = { query: vi.fn().mockResolvedValue({ rows: [] }) };
    await listGeorgiaCandidateElectionsMissingFinanceLinks(db, {
      now: NOW,
      maxCandidates: 25,
      electionLookbackDays: 30,
      electionLookaheadDays: 730,
    });
    const [sql, params] = db.query.mock.calls[0]!;
    expect(sql).toContain("district.state = 'GA'");
    expect(sql).toContain("public.ga_candidate_finance_links");
    expect(sql).toContain("link.link_status = 'active'");
    expect(params).toEqual([NOW.toISOString(), 25, 30, 730, [...GEORGIA_FINANCE_ELIGIBLE_OFFICE_KEYS]]);
  });
});

describe("autoLinkMissingGeorgiaCandidateFinanceLinks", () => {
  it("writes an active peachfile_api link for an exact match", async () => {
    const db = { query: vi.fn().mockResolvedValue({ rows: [{ id: "link-1" }] }) };
    const results = await autoLinkMissingGeorgiaCandidateFinanceLinks({
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
      { candidateId: CANDIDATE_ID, electionId: ELECTION_ID, status: "linked", committeeId: "100035" },
    ]);
    const [sql, params] = db.query.mock.calls[0]!;
    expect(sql).toContain("ga_candidate_finance_links");
    expect(params).toEqual(
      expect.arrayContaining(["100035", "Carr for Georgia, Inc.", "active", "peachfile_api", "CHRISTOPHER CARR"])
    );
  });

  it("reports ambiguous resolutions without writing anything", async () => {
    const db = { query: vi.fn() };
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    try {
      const results = await autoLinkMissingGeorgiaCandidateFinanceLinks({
        db,
        transport: NOOP_TRANSPORT,
        now: NOW,
        maxCandidates: 10,
        electionLookbackDays: 30,
        electionLookaheadDays: 730,
        candidateElections: [candidateElection()],
        resolveCandidateCommittee: async () => ({
          status: "ambiguous",
          reason: "multiple_matching_registrations",
          candidateNameNormalized: "CHRISTOPHER CARR",
          officeNameNormalized: "GOVERNOR",
          matches: [matchedResolution(), matchedResolution()].map(({ status: _status, ...match }) => match),
        }),
      });
      expect(results).toEqual([
        {
          candidateId: CANDIDATE_ID,
          electionId: ELECTION_ID,
          status: "ambiguous",
          reason: "multiple_matching_registrations",
        },
      ]);
      // The ga_candidate_finance_links vocabulary is active/inactive only —
      // ambiguity must never reach the DB.
      expect(db.query).not.toHaveBeenCalled();
      expect(warn).toHaveBeenCalled();
    } finally {
      warn.mockRestore();
    }
  });

  it("reports unmatched resolutions without writing anything", async () => {
    const db = { query: vi.fn() };
    const results = await autoLinkMissingGeorgiaCandidateFinanceLinks({
      db,
      transport: NOOP_TRANSPORT,
      now: NOW,
      maxCandidates: 10,
      electionLookbackDays: 30,
      electionLookaheadDays: 730,
      candidateElections: [candidateElection()],
      resolveCandidateCommittee: async () => ({
        status: "unmatched",
        reason: "no_candidate_committee_match",
        candidateNameNormalized: "CHRISTOPHER CARR",
        officeNameNormalized: "GOVERNOR",
      }),
    });
    expect(results[0]).toMatchObject({ status: "unmatched", reason: "no_candidate_committee_match" });
    expect(db.query).not.toHaveBeenCalled();
  });

  it("captures per-candidate failures and continues", async () => {
    const db = { query: vi.fn().mockResolvedValue({ rows: [{ id: "link-1" }] }) };
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    try {
      const failing = candidateElection();
      const succeeding = candidateElection({
        candidateId: "33333333-3333-4333-8333-333333333333",
        candidateName: "Tanya Miller",
      });
      let callIndex = 0;
      const results = await autoLinkMissingGeorgiaCandidateFinanceLinks({
        db,
        transport: NOOP_TRANSPORT,
        now: NOW,
        maxCandidates: 10,
        electionLookbackDays: 30,
        electionLookaheadDays: 730,
        candidateElections: [failing, succeeding],
        resolveCandidateCommittee: async () => {
          callIndex += 1;
          if (callIndex === 1) {
            throw new Error("portal unreachable");
          }
          return matchedResolution();
        },
      });
      expect(results[0]).toMatchObject({ status: "error", reason: "auto_link_failed", error: "portal unreachable" });
      expect(results[1]).toMatchObject({ status: "linked" });
    } finally {
      warn.mockRestore();
    }
  });
});
