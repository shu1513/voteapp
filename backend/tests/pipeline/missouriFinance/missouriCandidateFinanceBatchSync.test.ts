import { afterEach, describe, expect, it, vi } from "vitest";

import { syncDueMissouriCandidateFinance } from "../../../src/pipeline/missouriFinance/missouriCandidateFinanceBatchSync.js";

afterEach(() => vi.unstubAllEnvs());

function due(candidate: string, committee: string) {
  return {
    candidate_id: candidate, election_id: `e-${candidate}`, candidate_name: `Candidate ${candidate}`,
    election_year: 2026, election_date: "2026-11-03", office_scope: "state_lower",
    office_name: "State Lower Chamber Legislator", district: "1", committee_id: committee,
    committee_name: `Committee ${committee}`, link_source: "mec_portal", source_url: "https://example.test",
    last_synced_at: null, total_due_rows: "2",
  };
}

describe("syncDueMissouriCandidateFinance", () => {
  it("refreshes then syncs each due candidate independently", async () => {
    vi.stubEnv("MISSOURI_CAMPAIGN_FINANCE_ENABLED", "true");
    vi.stubEnv("MISSOURI_CAMPAIGN_FINANCE_RAW_DATA_REFRESH_ENABLED", "true");
    const query = vi.fn().mockResolvedValue({ rows: [due("c1", "C263985"), due("c2", "A233052")] });
    const acquire = vi.fn().mockResolvedValue({});
    const sync = vi.fn()
      .mockResolvedValueOnce({ candidateId: "c1" })
      .mockRejectedValueOnce(new Error("bad amendment lineage"));
    const result = await syncDueMissouriCandidateFinance({
      db: { query, connect: vi.fn() } as never,
      now: new Date("2026-08-19T00:00:00Z"),
      autoLinkMissingLinks: false,
      acquireArtifactsFn: acquire as never,
      syncCandidateFn: sync as never,
      session: {} as never,
    });
    expect(acquire).toHaveBeenCalledTimes(2);
    expect(sync).toHaveBeenCalledTimes(2);
    expect(query.mock.calls[0]?.[1]?.[3]).toBe(38);
    expect(sync).toHaveBeenCalledWith(expect.objectContaining({ officeScope: "state_lower" }));
    expect(result).toMatchObject({ dueCandidateCount: 2, selectedCandidateCount: 2, syncedCandidateCount: 1, failedCandidateCount: 1 });
    expect(result.results[1]).toMatchObject({ ok: false, error: "bad amendment lineage" });
  });
});
