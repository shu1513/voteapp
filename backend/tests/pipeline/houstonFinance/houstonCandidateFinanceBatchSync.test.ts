import { describe, expect, it, vi } from "vitest";
import { listDueHoustonCandidateFinanceRows, syncDueHoustonCandidateFinance } from "../../../src/pipeline/houstonFinance/houstonCandidateFinanceBatchSync.js";
import { listHoustonCandidateElectionsMissingFinanceLinks } from "../../../src/pipeline/houstonFinance/houstonCandidateFinanceAutoLink.js";

describe("Houston candidate finance batch selection", () => {
  it("uses the real merged-candidate column and passes force to stale selection", async () => {
    const query = vi.fn().mockResolvedValue({ rows: [] });
    await listDueHoustonCandidateFinanceRows({ db: { query }, now: new Date("2026-01-01T00:00:00Z"), maxCandidates: 10, staleAfterDays: 1, lookbackDays: 1, lookaheadDays: 730, force: true });
    expect(query.mock.calls[0]?.[0]).toContain("merged_into_candidate_id");
    expect(query.mock.calls[0]?.[1]).toEqual(expect.arrayContaining([true]));
  });

  it("still writes outside data when direct report retrieval fails", async () => {
    const due = { candidate_id: "11111111-1111-4111-8111-111111111111", election_id: "22222222-2222-4222-8222-222222222222", candidate_name: "Jane Doe", first_name: "Jane", last_name: "Doe", election_year: 2027, election_date: "2027-11-02", committee_id: "efile:1", committee_name: "Jane Doe", source_url: null };
    const db = { query: vi.fn().mockResolvedValue({ rows: [due] }) };
    const syncFn = vi.fn().mockResolvedValue({ candidateId: due.candidate_id, electionId: due.election_id, electionYear: 2027, dryRun: false, directContributionTotal: null, outsideSupportTotal: 10, outsideOpposeTotal: 0, directBreakdownsWritten: 0, outsideGroupsWritten: 1, outsideGroupBreakdownsWritten: 1 });
    const result = await syncDueHoustonCandidateFinance({
      db: db as never, now: new Date("2026-01-01T00:00:00Z"), autoLink: false,
      tecData: { sourceUrl: "https://example.test", purposeRows: [], candidateRows: [], expenditureRows: [], contributionRows: [], politicalCommitteeNames: new Set() },
      loadReportsFn: vi.fn().mockRejectedValue(new Error("PDF unavailable")), syncFn,
    });
    expect(result.syncedCandidateCount).toBe(1);
    expect(syncFn).toHaveBeenCalledWith(expect.objectContaining({ reports: undefined, purposeRows: [] }));
  });

  it("selects only Houston place GEOID Mayor elections for auto-link", async () => {
    const query = vi.fn().mockResolvedValue({ rows: [] });
    await listHoustonCandidateElectionsMissingFinanceLinks({ db: { query }, now: new Date("2026-01-01T00:00:00Z"), maxCandidates: 10, lookbackDays: 1, lookaheadDays: 730 });
    expect(query.mock.calls[0]?.[0]).toContain("district.geoid_compact = $5");
    expect(query.mock.calls[0]?.[1]?.[4]).toBe("4835000");
  });
});
