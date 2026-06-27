import { describe, expect, it, vi } from "vitest";

import {
  syncDueMarylandCandidateFinance,
  type MarylandCandidateFinanceDueRow,
} from "../../../src/pipeline/marylandFinance/marylandCandidateFinanceBatchSync.js";
import type { MarylandCandidateFinanceSyncResult } from "../../../src/pipeline/marylandFinance/marylandCandidateFinanceSync.js";
import type {
  MarylandCfsCommitteeRow,
  MarylandCfsContributionRow,
  MarylandCfsExpenditureRow,
} from "../../../src/pipeline/marylandFinance/marylandCfsArtifactReader.js";

const SOURCE_URL = "https://campaignfinance.maryland.gov/public/cf/downloads";

function createMockDb(rows: MarylandCandidateFinanceDueRow[]) {
  const query = vi.fn().mockResolvedValue({
    rows: rows.map((row, index) => ({
      candidate_id: row.candidateId,
      election_id: row.electionId,
      candidate_name: row.candidateName,
      election_year: row.electionYear,
      office_scope: row.officeScope,
      office_name: row.officeName,
      district: row.district,
      committee_id: row.committeeId,
      committee_name: row.committeeName,
      source_url: row.sourceUrl,
      last_synced_at: row.lastSyncedAt,
      total_due_rows: index === 0 ? rows.length : undefined,
    })),
  });
  return {
    query,
    connect: vi.fn(),
  };
}

function dueRow(overrides: Partial<MarylandCandidateFinanceDueRow> = {}): MarylandCandidateFinanceDueRow {
  return {
    candidateId: "11111111-1111-4111-8111-111111111111",
    electionId: "22222222-2222-4222-8222-222222222222",
    candidateName: "Justin Gallucci",
    electionYear: 2026,
    officeScope: "state_upper",
    officeName: "State Senator",
    district: "1",
    committeeId: "16018290",
    committeeName: "Gallucci, Justin Friends of",
    sourceUrl: SOURCE_URL,
    lastSyncedAt: null,
    ...overrides,
  };
}

function syncResult(row: MarylandCandidateFinanceDueRow): MarylandCandidateFinanceSyncResult {
  return {
    candidateId: row.candidateId,
    electionId: row.electionId,
    electionYear: row.electionYear,
    dryRun: false,
    resolution: {
      status: "matched",
      committeeId: row.committeeId,
      committeeName: row.committeeName,
      confidence: "exact",
      source: "cfs_public_export",
      sourceUrl: row.sourceUrl,
      matchedCommitteeRowCount: 0,
    },
    linkWritten: true,
    summaryWritten: true,
    directBreakdownsWritten: 1,
    outsideGroupsWritten: 1,
    outsideGroupBreakdownsWritten: 2,
    totalReceipts: 250,
    directContributionTotal: 250,
    outsideSupportTotal: 7500,
    outsideOpposeTotal: 0,
    matchedContributionRowCount: 1,
    includedContributionRowCount: 1,
    skippedContributionRowCount: 0,
    matchedExpenditureRowCount: 1,
    includedExpenditureRowCount: 1,
    skippedExpenditureRowCount: 0,
    matchedOutsideContributionRowCount: 1,
    includedOutsideContributionRowCount: 1,
    skippedOutsideContributionRowCount: 0,
  };
}

describe("marylandCandidateFinanceBatchSync", () => {
  it("syncs due linked candidates with reused yearly committee, contribution, and expenditure artifacts", async () => {
    const rows = [
      dueRow(),
      dueRow({
        candidateId: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        electionId: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
        candidateName: "Renee Smith",
        committeeId: "16099999",
        committeeName: "Smith, Renee Friends of",
      }),
    ];
    const db = createMockDb(rows);
    const candidateContributionRow = {
      "Filing Entity Id": "16018290",
    } as MarylandCfsContributionRow;
    const outsideContributionRow = {
      "Filing Entity Id": "16020184",
    } as MarylandCfsContributionRow;
    const expenditureRow = {
      "Filing Entity Id": "16020184",
      "Candidate/Ballot Issue": "Gallucci, Justin",
    } as MarylandCfsExpenditureRow;
    const syncFn = vi.fn(async (input) => syncResult(rows.find((row) => row.candidateId === input.candidateId) ?? rows[0]));

    const result = await syncDueMarylandCandidateFinance({
      db,
      now: new Date("2026-07-08T09:10:11.000Z"),
      autoLinkMissingLinks: false,
      committeeDataByYear: new Map([
        [
          2026,
          {
            year: 2026,
            filePath: "/tmp/TCMD_2026.csv",
            sourceUrl: SOURCE_URL,
            rows: [{ "Filing Entity Id": "16018290" } as MarylandCfsCommitteeRow],
          },
        ],
      ]),
      contributionDataByYear: new Map([
        [
          2026,
          {
            year: 2026,
            filePath: "/tmp/TCON_2026.csv",
            sourceUrl: SOURCE_URL,
            rowsByCommitteeId: new Map([
              ["16018290", [candidateContributionRow]],
              ["16020184", [outsideContributionRow]],
            ]),
          },
        ],
      ]),
      expenditureDataByYear: new Map([
        [
          2026,
          {
            year: 2026,
            filePath: "/tmp/TEXP_2026.csv",
            sourceUrl: SOURCE_URL,
            rows: [expenditureRow],
          },
        ],
      ]),
      syncMarylandCandidateFinanceFn: syncFn,
    });

    expect(result).toMatchObject({
      dryRun: false,
      now: "2026-07-08T09:10:11.000Z",
      dueCandidateCount: 2,
      selectedCandidateCount: 2,
      syncedCandidateCount: 2,
      failedCandidateCount: 0,
    });
    expect(db.query).toHaveBeenCalledTimes(1);
    expect(syncFn).toHaveBeenCalledTimes(2);
    expect(syncFn.mock.calls[0]?.[0]).toMatchObject({
      candidateId: rows[0].candidateId,
      committeeRows: [{ "Filing Entity Id": "16018290" }],
      contributionRows: [candidateContributionRow, outsideContributionRow],
      expenditureRows: [expenditureRow],
      trustedCommittee: {
        committeeId: "16018290",
        committeeName: "Gallucci, Justin Friends of",
        sourceUrl: SOURCE_URL,
      },
    });
    expect(syncFn.mock.calls[1]?.[0]).toMatchObject({
      candidateId: rows[1].candidateId,
      contributionRows: [candidateContributionRow, outsideContributionRow],
      expenditureRows: [expenditureRow],
      trustedCommittee: {
        committeeId: "16099999",
        committeeName: "Smith, Renee Friends of",
        sourceUrl: SOURCE_URL,
      },
    });
  });

  it("records per-candidate failures without aborting the batch", async () => {
    const rows = [
      dueRow(),
      dueRow({
        candidateId: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        electionId: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
        committeeId: "16099999",
      }),
    ];
    const db = createMockDb(rows);
    const syncFn = vi
      .fn()
      .mockResolvedValueOnce(syncResult(rows[0]))
      .mockRejectedValueOnce(new Error("sync failed"));

    const result = await syncDueMarylandCandidateFinance({
      db,
      now: new Date("2026-07-08T09:10:11.000Z"),
      autoLinkMissingLinks: false,
      committeeDataByYear: new Map([[2026, { year: 2026, filePath: "/tmp/TCMD.csv", sourceUrl: SOURCE_URL, rows: [] }]]),
      contributionDataByYear: new Map([
        [2026, { year: 2026, filePath: "/tmp/TCON.csv", sourceUrl: SOURCE_URL, rowsByCommitteeId: new Map() }],
      ]),
      expenditureDataByYear: new Map([[2026, { year: 2026, filePath: "/tmp/TEXP.csv", sourceUrl: SOURCE_URL, rows: [] }]]),
      syncMarylandCandidateFinanceFn: syncFn,
    });

    expect(result.syncedCandidateCount).toBe(1);
    expect(result.failedCandidateCount).toBe(1);
    expect(result.results).toEqual([
      expect.objectContaining({ candidateId: rows[0].candidateId, ok: true }),
      expect.objectContaining({ candidateId: rows[1].candidateId, ok: false, error: "sync failed" }),
    ]);
  });
});
