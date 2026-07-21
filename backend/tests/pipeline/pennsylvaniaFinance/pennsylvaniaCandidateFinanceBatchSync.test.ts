import { describe, expect, it, vi } from "vitest";

import {
  listPennsylvaniaCandidateFinanceOutsideGroupsForLinks,
  listDuePennsylvaniaCandidateFinanceSyncRows,
  syncDuePennsylvaniaCandidateFinance,
} from "../../../src/pipeline/pennsylvaniaFinance/pennsylvaniaCandidateFinanceBatchSync.js";

const LINK_ID = "33333333-3333-3333-3333-333333333333";
const CANDIDATE_ID = "11111111-1111-1111-1111-111111111111";
const ELECTION_ID = "22222222-2222-2222-2222-222222222222";
const SOURCE_URL = "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/resources/voting-and-elections/campaign-finance/campaign-finance-data/2026.zip";

function dueRow() {
  return {
    link_id: LINK_ID,
    candidate_id: CANDIDATE_ID,
    election_id: ELECTION_ID,
    candidate_name: "Jane Doe",
    election_year: 2026,
    office_scope: "statewide",
    office_name: "Governor",
    district: null,
    filer_id: "12345",
    filer_name: "JANE DOE FOR GOVERNOR",
    source_url: SOURCE_URL,
    last_synced_at: null,
    total_due_rows: "1",
  };
}

describe("pennsylvaniaCandidateFinanceBatchSync", () => {
  it("lists due active PA finance links for eligible offices", async () => {
    const db = {
      query: vi.fn().mockResolvedValue({ rows: [dueRow()] }),
    };

    const result = await listDuePennsylvaniaCandidateFinanceSyncRows(db, {
      now: new Date("2026-01-01T00:00:00.000Z"),
      staleAfterDays: 7,
      maxCandidates: 25,
      electionLookbackDays: 1,
      electionLookaheadDays: 730,
    });

    expect(result).toEqual({
      rows: [
        {
          linkId: LINK_ID,
          candidateId: CANDIDATE_ID,
          electionId: ELECTION_ID,
          candidateName: "Jane Doe",
          electionYear: 2026,
          officeScope: "statewide",
          officeName: "Governor",
          district: null,
          filerId: "12345",
          filerName: "JANE DOE FOR GOVERNOR",
          sourceUrl: SOURCE_URL,
          lastSyncedAt: null,
        },
      ],
      totalDueRows: 1,
    });
    expect(String(db.query.mock.calls[0]?.[0])).toContain("FROM public.pa_candidate_finance_links AS link");
    expect(String(db.query.mock.calls[0]?.[0])).toContain("link.id::text AS link_id");
    expect(String(db.query.mock.calls[0]?.[0])).toContain("district.state = 'PA'");
    expect(db.query.mock.calls[0]?.[1]?.[5]).toContain("statewide::Governor");
  });

  it("lists existing outside groups for selected PA finance links", async () => {
    const db = {
      query: vi.fn().mockResolvedValue({
        rows: [
          {
            link_id: LINK_ID,
            group_id: "pac123",
            group_name: "PENNSYLVANIANS FOR ACTION",
            support_oppose: "support",
            amount: "100000.00",
            source_url: SOURCE_URL,
          },
        ],
      }),
    };

    const result = await listPennsylvaniaCandidateFinanceOutsideGroupsForLinks(db, [LINK_ID]);

    expect(result.get(LINK_ID)).toEqual([
      {
        groupId: "PAC123",
        groupName: "PENNSYLVANIANS FOR ACTION",
        supportOppose: "support",
        amount: 100000,
        sourceUrl: SOURCE_URL,
      },
    ]);
    expect(String(db.query.mock.calls[0]?.[0])).toContain("FROM public.pa_candidate_finance_outside_groups");
    expect(db.query.mock.calls[0]?.[1]).toEqual([[LINK_ID]]);
  });

  it("syncs due linked PA candidates with injected yearly data", async () => {
    const db = {
      query: vi
        .fn()
        .mockResolvedValueOnce({ rows: [dueRow()] })
        .mockResolvedValueOnce({
          rows: [
            {
              link_id: LINK_ID,
              group_id: "PAC123",
              group_name: "PENNSYLVANIANS FOR ACTION",
              support_oppose: "support",
              amount: "100000.00",
              source_url: SOURCE_URL,
            },
          ],
        }),
    };
    const syncFn = vi.fn().mockResolvedValue({
      candidateId: CANDIDATE_ID,
      electionId: ELECTION_ID,
      electionYear: 2026,
      dryRun: false,
      resolution: {
        status: "matched",
        filerId: "12345",
        filerName: "JANE DOE FOR GOVERNOR",
        filerType: null,
        confidence: "exact",
        source: "pa_bulk",
        sourceUrl: SOURCE_URL,
        matchedFilerRowCount: 0,
      },
      linkWritten: true,
      summaryWritten: true,
      directBreakdownsWritten: 1,
      outsideGroupsWritten: 1,
      outsideGroupBreakdownsWritten: 2,
      totalReceipts: 100,
      directContributionTotal: 100,
      outsideSupportTotal: 100000,
      outsideOpposeTotal: 0,
      matchedContributionRowCount: 1,
      includedContributionEventCount: 1,
      skippedContributionEventCount: 0,
      matchedOutsideContributionRowCount: 1,
      includedOutsideContributionEventCount: 1,
      skippedOutsideContributionEventCount: 0,
    });
    const financeIndustryClassifier = vi.fn();

    const result = await syncDuePennsylvaniaCandidateFinance({
      db,
      now: new Date("2026-01-01T00:00:00.000Z"),
      autoLinkMissingLinks: false,
      paDataByYear: new Map([
        [
          2025,
          {
            year: 2025,
            extractedDir: "/tmp/pa-2025",
            sourceUrl: SOURCE_URL.replace("2026.zip", "2025.zip"),
            filerRows: [{ FILERID: "12345", FILERNAME: "JANE DOE FOR GOVERNOR" }] as never,
            contributionRows: [{ FilerID: "12345", CONTDATE1: "20250101" }] as never,
          },
        ],
        [
          2026,
          {
            year: 2026,
            extractedDir: "/tmp/pa-2026",
            sourceUrl: SOURCE_URL,
            filerRows: [{ FILERID: "12345", FILERNAME: "JANE DOE FOR GOVERNOR" }] as never,
            contributionRows: [{ FilerID: "12345", CONTDATE1: "20260101" }] as never,
          },
        ],
      ]),
      financeIndustryClassifier,
      aiClassificationMinAmount: 50000,
      syncPennsylvaniaCandidateFinanceFn: syncFn,
    });

    expect(result).toMatchObject({
      dryRun: false,
      autoLinkAttemptedCount: 0,
      autoLinkLinkedCount: 0,
      dueCandidateCount: 1,
      selectedCandidateCount: 1,
      syncedCandidateCount: 1,
      failedCandidateCount: 0,
    });
    expect(syncFn).toHaveBeenCalledWith(
      expect.objectContaining({
        candidateId: CANDIDATE_ID,
        electionId: ELECTION_ID,
        candidateName: "Jane Doe",
        electionYear: 2026,
        trustedFiler: {
          filerId: "12345",
          filerName: "JANE DOE FOR GOVERNOR",
          sourceUrl: SOURCE_URL,
        },
        outsideGroups: [
          {
            groupId: "PAC123",
            groupName: "PENNSYLVANIANS FOR ACTION",
            supportOppose: "support",
            amount: 100000,
            sourceUrl: SOURCE_URL,
          },
        ],
        contributionRows: expect.any(Array),
        filerRows: expect.any(Array),
        financeIndustryClassifier,
        aiClassificationMinAmount: 50000,
      })
    );
    expect(syncFn.mock.calls[0]?.[0]).toMatchObject({
      sourceUrl: SOURCE_URL,
      contributionSourceUrl: SOURCE_URL,
    });
    expect(syncFn.mock.calls[0]?.[0].contributionRows).toHaveLength(2);
    expect(syncFn.mock.calls[0]?.[0].filerRows).toHaveLength(2);
  });
});
