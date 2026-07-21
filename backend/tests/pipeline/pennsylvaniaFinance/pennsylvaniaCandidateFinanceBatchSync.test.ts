import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";

import {
  listPennsylvaniaCandidateFinanceOutsideGroupsForLinks,
  listDuePennsylvaniaCandidateFinanceSyncRows,
  syncDuePennsylvaniaCandidateFinance,
} from "../../../src/pipeline/pennsylvaniaFinance/pennsylvaniaCandidateFinanceBatchSync.js";
import { PENNSYLVANIA_CAMPAIGN_FINANCE_FILER_COLUMNS } from "../../../src/pipeline/pennsylvaniaFinance/pennsylvaniaCampaignFinanceReader.js";

const LINK_ID = "33333333-3333-3333-3333-333333333333";
const CANDIDATE_ID = "11111111-1111-1111-1111-111111111111";
const ELECTION_ID = "22222222-2222-2222-2222-222222222222";
const tempDirs: string[] = [];

afterEach(async () => {
  await Promise.all(tempDirs.splice(0).map((dir) => rm(dir, { recursive: true, force: true })));
});

async function makeTempDir(): Promise<string> {
  const dir = await mkdtemp(join(tmpdir(), "voteapp-pa-batch-sync-"));
  tempDirs.push(dir);
  return dir;
}

function filerCsvLine(overrides: Record<string, string>): string {
  return PENNSYLVANIA_CAMPAIGN_FINANCE_FILER_COLUMNS.map((column) => overrides[column] ?? "").join(",");
}

async function writeFilerFile(input: {
  cacheDir: string;
  year: number;
  rows: readonly Record<string, string>[];
}): Promise<void> {
  const extractedDir = join(input.cacheDir, String(input.year));
  await mkdir(extractedDir, { recursive: true });
  const csv = [PENNSYLVANIA_CAMPAIGN_FINANCE_FILER_COLUMNS.join(","), ...input.rows.map(filerCsvLine)].join("\n") + "\n";
  await writeFile(join(extractedDir, `filer_${input.year}.txt`), csv, "utf8");
}
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

  it("auto-links a candidate whose filer registration exists only in the prior-year archive", async () => {
    // PA registrations for an election-year race are often exported only in
    // the prior year's file; the cycle loader must read filer_<year>.txt for
    // BOTH cycle years (each year selects a different file — no duplication).
    const cacheDir = await makeTempDir();
    await writeFilerFile({
      cacheDir,
      year: 2025,
      rows: [
        {
          CampaignfinanceID: "100",
          FILERID: "12345",
          EYEAR: "2026",
          FILERTYPE: "1",
          FILERNAME: "JANE DOE FOR GOVERNOR",
          OFFICE: "GOV",
          DISTRICT: "",
        },
      ],
    });
    await writeFilerFile({
      cacheDir,
      year: 2026,
      rows: [
        {
          CampaignfinanceID: "200",
          FILERID: "99999",
          EYEAR: "2026",
          FILERTYPE: "1",
          FILERNAME: "SOMEBODY ELSE FOR AUDITOR GENERAL",
          OFFICE: "AUD",
          DISTRICT: "",
        },
      ],
    });
    const db = {
      query: vi
        .fn()
        // 1: missing-links enumeration
        .mockResolvedValueOnce({
          rows: [
            {
              candidate_id: CANDIDATE_ID,
              election_id: ELECTION_ID,
              candidate_name: "Jane Doe",
              election_year: 2026,
              office_scope: "statewide",
              office_name: "Governor",
              district: null,
            },
          ],
        })
        // 2: link upsert
        .mockResolvedValueOnce({ rows: [{ id: LINK_ID }], rowCount: 1 })
        // 3: due query (empty — this test focuses on the auto-link path)
        .mockResolvedValueOnce({ rows: [] }),
    };
    const syncFn = vi.fn();

    const result = await syncDuePennsylvaniaCandidateFinance({
      db,
      now: new Date("2026-07-21T12:00:00.000Z"),
      rawDataCacheDir: cacheDir,
      syncPennsylvaniaCandidateFinanceFn: syncFn,
    });

    expect(result).toMatchObject({
      autoLinkAttemptedCount: 1,
      autoLinkLinkedCount: 1,
      dueCandidateCount: 0,
      syncedCandidateCount: 0,
    });
    const insertCall = db.query.mock.calls[1];
    expect(String(insertCall?.[0])).toContain("INSERT INTO public.pa_candidate_finance_links");
    expect(insertCall?.[1]).toContain("12345");
    // The auto-link enumeration must not be capped: unmatched candidates
    // would otherwise pin a stably-ordered LIMIT prefix and starve the tail.
    const enumerationParams = db.query.mock.calls[0]?.[1] as unknown[];
    expect(enumerationParams?.[1]).toBeNull();
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
