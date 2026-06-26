import { describe, expect, it, vi } from "vitest";

import {
  autoLinkTennesseeCandidateFinanceForCandidateElection,
  listTennesseeCandidateElectionsMissingFinanceLinks,
  type TennesseeFinanceAutoLinkCandidateElection,
} from "../../../src/pipeline/tennesseeFinance/tennesseeCandidateFinanceAutoLink.js";

const candidateElection: TennesseeFinanceAutoLinkCandidateElection = {
  candidateId: "11111111-1111-4111-8111-111111111111",
  electionId: "22222222-2222-4222-8222-222222222222",
  candidateName: "Jane Doe",
  electionYear: 2026,
  officeScope: "statewide",
  officeName: "Governor",
  district: null,
};

describe("tennesseeCandidateFinanceAutoLink", () => {
  it("does not list candidate elections with active or ambiguous Tennessee links as missing", async () => {
    const db = {
      query: vi.fn().mockResolvedValue({ rows: [], rowCount: 0 }),
    };

    await listTennesseeCandidateElectionsMissingFinanceLinks(db, {
      now: new Date("2026-06-01T00:00:00.000Z"),
      maxCandidates: 10,
      electionLookbackDays: 1,
      electionLookaheadDays: 730,
    });

    expect(String(db.query.mock.calls[0]?.[0])).toContain("link.link_status IN ('active', 'ambiguous')");
  });

  it("stores ambiguous CAMP matches so auto-link does not retry them indefinitely", async () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    const db = {
      query: vi.fn().mockResolvedValue({ rows: [{ id: "33333333-3333-4333-8333-333333333333" }], rowCount: 1 }),
    };

    const result = await autoLinkTennesseeCandidateFinanceForCandidateElection({
      db,
      candidateElection,
      now: new Date("2026-06-01T00:00:00.000Z"),
      resolveCandidateCommittee: vi.fn().mockResolvedValue({
        status: "ambiguous",
        reason: "multiple_matching_committees",
        candidateNameNormalized: "JANE DOE",
        officeNameNormalized: "GOVERNOR",
        matches: [
          {
            campCandidateId: "100",
            ownerName: "DOE, JANE",
            candidateName: "DOE, JANE",
            officeSought: "Governor",
            district: null,
            confidence: "exact",
            source: "tncamp_search",
            sourceUrl: "https://apps.tn.gov/tncamp/public/cpsearch.htm",
            reportListUrl: "https://apps.tn.gov/tncamp/public/replist.htm?id=100",
            matchedRowCount: 1,
          },
          {
            campCandidateId: "101",
            ownerName: "DOE, JANE",
            candidateName: "DOE, JANE",
            officeSought: "Governor",
            district: null,
            confidence: "exact",
            source: "tncamp_search",
            sourceUrl: "https://apps.tn.gov/tncamp/public/cpsearch.htm",
            reportListUrl: "https://apps.tn.gov/tncamp/public/replist.htm?id=101",
            matchedRowCount: 1,
          },
        ],
      }),
    });

    expect(result).toMatchObject({ status: "ambiguous", reason: "multiple_matching_committees" });
    const upsertCall = db.query.mock.calls.find((call) =>
      String(call[0]).includes("INSERT INTO public.tn_candidate_finance_links")
    );
    expect(upsertCall?.[1]).toEqual(
      expect.arrayContaining(["AMBIGUOUS", "Jane Doe", "Ambiguous Tennessee CAMP match", "ambiguous"])
    );
    expect(warn).toHaveBeenCalledWith(
      "Tennessee finance auto-link found ambiguous CAMP candidate matches:",
      expect.objectContaining({ candidateId: candidateElection.candidateId, electionId: candidateElection.electionId })
    );
    warn.mockRestore();
  });
});
