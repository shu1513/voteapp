import { describe, expect, it, vi } from "vitest";

import {
  autoLinkMissingDelawareCandidateFinanceLinks,
  type DelawareFinanceAutoLinkCandidateElection,
} from "../../../src/pipeline/delawareFinance/delawareCandidateFinanceAutoLink.js";
import {
  delawareCandidateNameTokens,
  normalizeDelawareCandidateNameForStorage,
} from "../../../src/pipeline/delawareFinance/delawareCandidateCommitteeResolver.js";

const LINK_ID = "33333333-3333-4333-8333-333333333333";

function candidateElection(overrides: Partial<DelawareFinanceAutoLinkCandidateElection> = {}): DelawareFinanceAutoLinkCandidateElection {
  return {
    candidateId: "11111111-1111-4111-8111-111111111111",
    electionId: "22222222-2222-4222-8222-222222222222",
    candidateName: "Jane Example",
    electionDate: "2026-11-03",
    electionYear: 2026,
    officeScope: "statewide",
    officeName: "Attorney General",
    districtName: "Delaware",
    legislativeDistrict: null,
    ...overrides,
  };
}

function linkWritingDb() {
  return {
    query: vi.fn((sql: unknown) =>
      String(sql).includes("INSERT INTO public.de_candidate_finance_links")
        ? Promise.resolve({ rows: [{ id: LINK_ID }], rowCount: 1 })
        : Promise.resolve({ rows: [], rowCount: 0 })
    ),
  };
}

describe("delaware candidate name helpers", () => {
  it("normalizes names and extracts tokens with suffixes stripped", () => {
    expect(normalizeDelawareCandidateNameForStorage("Colm  Ó'Néill-Smith")).toBe("COLM O NEILL SMITH");
    expect(delawareCandidateNameTokens("Jane Q. Example Jr.")).toEqual({ first: "JANE", last: "EXAMPLE" });
    expect(delawareCandidateNameTokens("Cher")).toBeNull();
  });
});

describe("autoLinkMissingDelawareCandidateFinanceLinks", () => {
  it("writes exactly-matched committees as cfrs_portal links", async () => {
    const db = linkWritingDb();
    const results = await autoLinkMissingDelawareCandidateFinanceLinks({
      db,
      now: new Date("2026-08-28T00:00:00.000Z"),
      maxCandidates: 5,
      electionLookbackDays: 30,
      electionLookaheadDays: 730,
      candidateElections: [candidateElection()],
      resolveCandidateCommittee: async () => ({
        status: "matched",
        cfId: "01009999",
        memberId: 600001,
        committeeName: "Jane Example for Delaware",
        confidence: "office_filtered_name_match",
        sourceUrl: "https://cfrs.elections.delaware.gov/Public/ViewCommittees",
        matchedCommitteeCount: 1,
      }),
    });
    expect(results).toEqual([
      {
        candidateId: candidateElection().candidateId,
        electionId: candidateElection().electionId,
        status: "linked",
        cfId: "01009999",
        committeeName: "Jane Example for Delaware",
      },
    ]);
    const insert = db.query.mock.calls.find((call) => String(call[0]).includes("INSERT INTO public.de_candidate_finance_links"));
    expect(insert?.[1]).toContain("cfrs_portal");
    expect(insert?.[1]).toContain("01009999");
  });

  it("never writes on ambiguity and isolates per-candidate failures", async () => {
    const db = linkWritingDb();
    const results = await autoLinkMissingDelawareCandidateFinanceLinks({
      db,
      now: new Date("2026-08-28T00:00:00.000Z"),
      maxCandidates: 5,
      electionLookbackDays: 30,
      electionLookaheadDays: 730,
      candidateElections: [
        candidateElection(),
        candidateElection({ candidateId: "44444444-4444-4444-8444-444444444444", candidateName: "Broken Case" }),
      ],
      resolveCandidateCommittee: async (input) => {
        if (input.candidateName === "Broken Case") {
          throw new Error("portal exploded");
        }
        return {
          status: "ambiguous",
          reason: "multiple_matching_committees",
          matches: [
            { cfId: "01000001", committeeName: "Example One" },
            { cfId: "01000002", committeeName: "Example Two" },
          ],
        };
      },
    });
    expect(results[0]).toMatchObject({ status: "ambiguous", reason: "multiple_matching_committees" });
    expect(results[1]).toMatchObject({ status: "error", reason: "auto_link_failed", error: "portal exploded" });
    expect(db.query.mock.calls.some((call) => String(call[0]).includes("INSERT INTO"))).toBe(false);
  });

  it("resolves each office+district race once", async () => {
    const db = linkWritingDb();
    const resolver = vi.fn(async () => ({
      status: "unmatched" as const,
      reason: "no_candidate_committee_match" as const,
    }));
    await autoLinkMissingDelawareCandidateFinanceLinks({
      db,
      now: new Date("2026-08-28T00:00:00.000Z"),
      maxCandidates: 5,
      electionLookbackDays: 30,
      electionLookaheadDays: 730,
      candidateElections: [candidateElection(), candidateElection({ electionId: "55555555-5555-4555-8555-555555555555" })],
      resolveCandidateCommittee: resolver,
    });
    expect(resolver).toHaveBeenCalledTimes(1);
  });
});
