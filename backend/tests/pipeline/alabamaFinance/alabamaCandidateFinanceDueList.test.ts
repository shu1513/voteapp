import { describe, expect, it, vi } from "vitest";

import { listDueAlabamaCandidateFinanceSyncRows } from "../../../src/pipeline/alabamaFinance/alabamaCandidateFinanceDueList.js";
import { ALABAMA_FINANCE_ELIGIBLE_OFFICE_KEYS } from "../../../src/pipeline/alabamaFinance/alabamaFinanceEligibleOffices.js";

const INPUT = {
  now: new Date("2026-09-05T00:00:00.000Z"),
  staleAfterDays: 7,
  maxCandidates: 25,
  electionLookbackDays: 30,
  electionLookaheadDays: 730,
};

describe("listDueAlabamaCandidateFinanceSyncRows", () => {
  it("queries active general-election links with the ballot title and maps rows", async () => {
    const db = {
      query: vi.fn().mockResolvedValue({
        rows: [
          {
            candidate_id: "candidate-1",
            election_id: "election-1",
            candidate_name: "Jane Example",
            election_year: 2026,
            office_scope: "statewide",
            office_name: "Supreme Court Justice",
            ballot_title: "Associate Justice of the Supreme Court, Place 1",
            district: null,
            committee_id: "12345",
            committee_name: "Friends of Jane Example",
            fcpa_committee_number: "PCC-2026-001",
            link_source: "fcpa_portal",
            source_url: null,
            last_synced_at: null,
            total_due_rows: "3",
          },
        ],
      }),
    };

    const result = await listDueAlabamaCandidateFinanceSyncRows(db, INPUT);

    const [sql, params] = db.query.mock.calls[0]!;
    expect(String(sql)).toContain("al_candidate_finance_links");
    expect(String(sql)).toContain("al_candidate_finance_summaries");
    expect(String(sql)).toContain("election.election_stage = 'general'");
    expect(String(sql)).toContain("district.state = 'AL'");
    expect(String(sql)).toContain("election.official_ballot_title AS ballot_title");
    expect(String(sql)).toContain("link.fcpa_committee_number");
    expect(String(sql)).not.toContain("election_date::text");
    expect(params).toEqual(["2026-09-05T00:00:00.000Z", 7, 25, 30, 730, [...ALABAMA_FINANCE_ELIGIBLE_OFFICE_KEYS]]);

    expect(result.totalDueRows).toBe(3);
    expect(result.rows).toEqual([
      {
        candidateId: "candidate-1",
        electionId: "election-1",
        candidateName: "Jane Example",
        electionYear: 2026,
        officeScope: "statewide",
        officeName: "Supreme Court Justice",
        ballotTitle: "Associate Justice of the Supreme Court, Place 1",
        district: null,
        internalCommitteeId: 12345,
        committeeName: "Friends of Jane Example",
        fcpaCommitteeNumber: "PCC-2026-001",
        linkSource: "fcpa_portal",
        sourceUrl: null,
        lastSyncedAt: null,
      },
    ]);
  });

  it("rejects a corrupted stored internal committee id instead of syncing the wrong committee", async () => {
    const db = {
      query: vi.fn().mockResolvedValue({
        rows: [
          {
            candidate_id: "candidate-1",
            election_id: "election-1",
            candidate_name: "X",
            election_year: 2026,
            office_scope: "statewide",
            office_name: "Governor",
            ballot_title: "Governor",
            district: null,
            committee_id: "0",
            committee_name: "Y",
            fcpa_committee_number: null,
            link_source: "manual",
            source_url: null,
            last_synced_at: null,
            total_due_rows: 1,
          },
        ],
      }),
    };
    await expect(listDueAlabamaCandidateFinanceSyncRows(db, INPUT)).rejects.toThrow(
      "Invalid stored Alabama internal committee id: 0"
    );
  });

  it("returns zero totals for an empty due list", async () => {
    const db = { query: vi.fn().mockResolvedValue({ rows: [] }) };
    expect(await listDueAlabamaCandidateFinanceSyncRows(db, INPUT)).toEqual({ rows: [], totalDueRows: 0 });
  });
});
