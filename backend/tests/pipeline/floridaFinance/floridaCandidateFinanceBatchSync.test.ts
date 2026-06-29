import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";

import {
  listDueFloridaCandidateFinanceSyncRows,
  syncDueFloridaCandidateFinance,
  syncFloridaCandidateFinanceBatch,
  type FloridaCandidateFinanceBatchSyncItemInput,
  type FloridaCandidateFinanceDueContributionData,
} from "../../../src/pipeline/floridaFinance/floridaCandidateFinanceBatchSync.js";
import {
  readFloridaContributionExportArtifact,
  writeFloridaContributionExportArtifact,
} from "../../../src/pipeline/floridaFinance/floridaCampaignFinanceArtifactCache.js";
import type { FloridaContributionExportRowsResult } from "../../../src/pipeline/floridaFinance/floridaCampaignFinanceClient.js";
import type { FloridaContributionRow } from "../../../src/pipeline/floridaFinance/floridaCampaignFinanceRows.js";
import { FLORIDA_FINANCE_ELIGIBLE_OFFICE_KEYS } from "../../../src/pipeline/floridaFinance/floridaFinanceEligibleOffices.js";

const tempDirs: string[] = [];
const CANDIDATE_ID = "11111111-1111-4111-8111-111111111111";
const CANDIDATE_ELECTION_ID = "22222222-2222-4222-8222-222222222222";
const ELECTION_ID = "33333333-3333-4333-8333-333333333333";

const CONTRIBUTION_ROW: FloridaContributionRow = {
  recipientName: "Friends of Jane Doe",
  contributionDate: "9/15/2026",
  amount: "100.00",
  transactionType: "CHE",
  contributorName: "Smith, Pat",
  address: "1 Main St",
  city: "Tallahassee",
  state: "FL",
  zip: "32301",
  occupation: "Attorney",
  inKindDescription: "",
  electionCode: "20261103-GEN",
  sourceUrl: "https://example.test/fl.tsv",
};

const SAMPLE_TSV = [
  "Candidate/Committee\tDate\tAmount\tTyp\tContributor Name\tAddress\tCity\tState\tZip\tOccupation\tInkind Desc",
  "Friends of Jane Doe\t9/15/2026\t100\tCHE\tSmith, Pat\t1 Main St\tTallahassee\tFL\t32301\tAttorney\t",
].join("\n");

async function makeTempDir(): Promise<string> {
  const dir = await mkdtemp(join(tmpdir(), "voteapp-fl-batch-sync-"));
  tempDirs.push(dir);
  return dir;
}

function createMockDb(rows: unknown[] = []) {
  return {
    query: vi.fn().mockResolvedValue({ rows }),
  };
}

function baseInput(
  overrides: Partial<FloridaCandidateFinanceBatchSyncItemInput> = {}
): FloridaCandidateFinanceBatchSyncItemInput {
  return {
    candidateId: "candidate-1",
    candidateElectionId: "candidate-election-1",
    electionId: "election-1",
    candidateName: "Jane Doe",
    electionYear: 2026,
    officeName: "Governor",
    trustedCommittee: {
      committeeId: "FRIENDS_OF_JANE_DOE",
      committeeName: "Friends of Jane Doe",
    },
    contributionRows: [CONTRIBUTION_ROW],
    sourceUrl: "https://example.test/source",
    ...overrides,
  };
}

function exportResult(cacheKey: string): FloridaContributionExportRowsResult {
  return {
    query: {
      searchType: "candidate_detail",
      electionCode: "20261103-GEN",
      candidateFirstName: "Jane",
      candidateLastName: "Doe",
      committeeName: null,
      committeeType: null,
      dateFrom: null,
      dateTo: null,
      rowLimit: 10000,
    },
    searchPageUrl: "https://dos.elections.myflorida.com/campaign-finance/contributions/",
    exportUrl: "https://dos.elections.myflorida.com/cgi-bin/contrib.exe",
    sourceUrl: "https://dos.elections.myflorida.com/cgi-bin/contrib.exe?download=1",
    cacheKey,
    retrievedAt: new Date("2026-06-20T20:00:00.000Z"),
    rowCount: 1,
    formData: {
      search_on: "2",
      queryformat: "2",
      rowlimit: "10000",
      Election: "20261103-GEN",
      CanFName: "Jane",
      CanLName: "Doe",
    },
    tsv: SAMPLE_TSV,
    rows: [],
  };
}

afterEach(async () => {
  await Promise.all(tempDirs.splice(0).map((dir) => rm(dir, { recursive: true, force: true })));
});

describe("floridaCandidateFinanceBatchSync", () => {
  it("lists due Florida finance sync rows from missing and stale snapshots", async () => {
    const db = createMockDb([
      {
        candidate_id: CANDIDATE_ID,
        candidate_election_id: CANDIDATE_ELECTION_ID,
        election_id: ELECTION_ID,
        candidate_name: "Jane Doe",
        election_year: 2026,
        office_name: "Governor",
        district: null,
        committee_id: "FRIENDS_OF_JANE_DOE",
        committee_name: "Friends of Jane Doe",
        source_url: "https://dos.elections.myflorida.com/campaign-finance/contributions/",
        last_synced_at: null,
        total_due_rows: "2",
      },
      {
        candidate_id: "44444444-4444-4444-8444-444444444444",
        candidate_election_id: "55555555-5555-4555-8555-555555555555",
        election_id: "66666666-6666-4666-8666-666666666666",
        candidate_name: "Alex Example",
        election_year: 2026,
        office_name: "State Senator",
        district: "12",
        committee_id: "ALEX_EXAMPLE_CAMPAIGN",
        committee_name: "Alex Example Campaign",
        source_url: null,
        last_synced_at: "2026-01-01 00:00:00+00",
        total_due_rows: "2",
      },
    ]);

    const result = await listDueFloridaCandidateFinanceSyncRows(db, {
      now: new Date("2026-06-01T00:00:00.000Z"),
      staleAfterDays: 7,
      maxCandidates: 25,
      electionLookbackDays: 30,
      electionLookaheadDays: 730,
    });

    expect(result).toEqual({
      totalDueRows: 2,
      rows: [
        {
          candidateId: CANDIDATE_ID,
          candidateElectionId: CANDIDATE_ELECTION_ID,
          electionId: ELECTION_ID,
          candidateName: "Jane Doe",
          electionYear: 2026,
          officeName: "Governor",
          district: null,
          committeeId: "FRIENDS_OF_JANE_DOE",
          committeeName: "Friends of Jane Doe",
          sourceUrl: "https://dos.elections.myflorida.com/campaign-finance/contributions/",
          lastSyncedAt: null,
        },
        {
          candidateId: "44444444-4444-4444-8444-444444444444",
          candidateElectionId: "55555555-5555-4555-8555-555555555555",
          electionId: "66666666-6666-4666-8666-666666666666",
          candidateName: "Alex Example",
          electionYear: 2026,
          officeName: "State Senator",
          district: "12",
          committeeId: "ALEX_EXAMPLE_CAMPAIGN",
          committeeName: "Alex Example Campaign",
          sourceUrl: null,
          lastSyncedAt: "2026-01-01 00:00:00+00",
        },
      ],
    });

    const sql = String(db.query.mock.calls[0]?.[0]);
    expect(sql).toContain("FROM public.fl_candidate_finance_links AS link");
    expect(sql).toContain("LEFT JOIN public.fl_candidate_finance_summaries AS summary");
    expect(sql).toContain("link.link_status = 'active'");
    expect(sql).toContain("district.state = 'FL'");
    expect(sql).toContain("candidate_election.status NOT IN ('withdrawn', 'lost')");
    expect(sql).toContain("summary.last_synced_at IS NULL");
    expect(sql).toContain("summary.last_synced_at < ($1::timestamptz - make_interval(days => $2::int))");
    expect(db.query.mock.calls[0]?.[1]).toEqual([
      "2026-06-01T00:00:00.000Z",
      7,
      25,
      30,
      730,
      [...FLORIDA_FINANCE_ELIGIBLE_OFFICE_KEYS],
    ]);
  });

  it("syncs selected due links through the existing batch path and fails rows without contribution data", async () => {
    const db = createMockDb([
      {
        candidate_id: CANDIDATE_ID,
        candidate_election_id: CANDIDATE_ELECTION_ID,
        election_id: ELECTION_ID,
        candidate_name: "Jane Doe",
        election_year: 2026,
        office_name: "Governor",
        district: null,
        committee_id: "FRIENDS_OF_JANE_DOE",
        committee_name: "Friends of Jane Doe",
        source_url: "https://dos.elections.myflorida.com/campaign-finance/contributions/",
        last_synced_at: null,
        total_due_rows: "2",
      },
      {
        candidate_id: "44444444-4444-4444-8444-444444444444",
        candidate_election_id: "55555555-5555-4555-8555-555555555555",
        election_id: "66666666-6666-4666-8666-666666666666",
        candidate_name: "Alex Example",
        election_year: 2026,
        office_name: "State Senator",
        district: "12",
        committee_id: "ALEX_EXAMPLE_CAMPAIGN",
        committee_name: "Alex Example Campaign",
        source_url: null,
        last_synced_at: "2026-01-01 00:00:00+00",
        total_due_rows: "2",
      },
    ]);
    const syncFloridaCandidateFinanceFn = vi.fn(async (input) => ({
      candidateId: input.candidateId,
      electionId: input.electionId,
      electionYear: input.electionYear,
      dryRun: input.dryRun === true,
      resolution: {
        status: "matched" as const,
        committeeId: input.trustedCommittee.committeeId,
        committeeName: input.trustedCommittee.committeeName,
        recipientNames: [input.trustedCommittee.committeeName],
        confidence: "exact" as const,
        source: "manual" as const,
        sourceUrl: input.trustedCommittee.sourceUrl ?? null,
      },
      linkWritten: true,
      summaryWritten: true,
      directBreakdownsWritten: 1,
      outsideGroupsWritten: 0,
      outsideGroupBreakdownsWritten: 0,
      outsideGroupSupportLinksWritten: 0,
      resolvedOutsideGroupCount: 0,
      outsideGroupSupportEvidenceCount: 0,
      heuristicOutsideGroupCount: 0,
      totalReceipts: 100,
      directContributionTotal: 100,
      outsideSupportTotal: null,
      outsideOpposeTotal: null,
      matchedContributionRowCount: input.contributionRows.length,
      includedContributionRowCount: input.contributionRows.length,
      skippedContributionRowCount: 0,
      matchedOutsideContributionRowCount: 0,
      includedOutsideContributionRowCount: 0,
      skippedOutsideContributionRowCount: 0,
    }));
    const contributionData: FloridaCandidateFinanceDueContributionData = {
      contributionRows: [CONTRIBUTION_ROW],
      contributionSourceUrl: "https://dos.elections.myflorida.com/cgi-bin/contrib.exe",
    };

    const result = await syncDueFloridaCandidateFinance({
      db,
      syncFloridaCandidateFinanceFn,
      now: new Date("2026-06-01T00:00:00.000Z"),
      maxCandidates: 2,
      staleAfterDays: 3,
      electionLookbackDays: 30,
      contributionDataByCommitteeId: new Map([["friends_of_jane_doe", contributionData]]),
      autoLinkMissingLinks: false,
      fetchMissingContributionData: false,
    });

    expect(result).toMatchObject({
      dryRun: false,
      now: "2026-06-01T00:00:00.000Z",
      staleAfterDays: 3,
      maxCandidates: 2,
      dueCandidateCount: 2,
      selectedCandidateCount: 2,
      syncedCandidateCount: 1,
      failedCandidateCount: 1,
    });
    expect(syncFloridaCandidateFinanceFn).toHaveBeenCalledTimes(1);
    expect(result.results).toEqual([
      expect.objectContaining({
        ok: true,
        committeeId: "FRIENDS_OF_JANE_DOE",
      }),
      {
        candidateId: "44444444-4444-4444-8444-444444444444",
        electionId: "66666666-6666-4666-8666-666666666666",
        electionYear: 2026,
        committeeId: "ALEX_EXAMPLE_CAMPAIGN",
        ok: false,
        error: "Florida contribution data not provided for committee: ALEX_EXAMPLE_CAMPAIGN",
      },
    ]);
    expect(syncFloridaCandidateFinanceFn).toHaveBeenCalledWith(
      expect.objectContaining({
        candidateId: CANDIDATE_ID,
        candidateElectionId: CANDIDATE_ELECTION_ID,
        electionId: ELECTION_ID,
        candidateName: "Jane Doe",
        electionYear: 2026,
        officeName: "Governor",
        trustedCommittee: expect.objectContaining({
          committeeId: "FRIENDS_OF_JANE_DOE",
          committeeName: "Friends of Jane Doe",
        }),
        contributionRows: [CONTRIBUTION_ROW],
        contributionSourceUrl: "https://dos.elections.myflorida.com/cgi-bin/contrib.exe",
        includeOutsideGroupFinance: false,
      })
    );
    expect(db.query.mock.calls[0]?.[1]).toEqual([
      "2026-06-01T00:00:00.000Z",
      3,
      2,
      30,
      730,
      [...FLORIDA_FINANCE_ELIGIBLE_OFFICE_KEYS],
    ]);
  });

  it("accepts zero-day due sync windows", async () => {
    const db = createMockDb([]);

    const result = await syncDueFloridaCandidateFinance({
      db,
      now: new Date("2026-06-01T00:00:00.000Z"),
      maxCandidates: 1,
      staleAfterDays: 0,
      electionLookbackDays: 0,
      electionLookaheadDays: 0,
      autoLinkMissingLinks: false,
      fetchMissingContributionData: false,
    });

    expect(result).toMatchObject({
      staleAfterDays: 0,
      dueCandidateCount: 0,
      selectedCandidateCount: 0,
    });
    expect(db.query.mock.calls[0]?.[1]).toEqual([
      "2026-06-01T00:00:00.000Z",
      0,
      1,
      0,
      0,
      [...FLORIDA_FINANCE_ELIGIBLE_OFFICE_KEYS],
    ]);
  });

  it("does not query Florida export with a suffix-only candidate last name", async () => {
    const db = createMockDb([]);
    const exportFloridaContributionRowsFn = vi.fn();

    await syncDueFloridaCandidateFinance({
      db,
      now: new Date("2026-06-01T00:00:00.000Z"),
      maxCandidates: 1,
      autoLinkCandidateElections: [
        {
          candidateId: CANDIDATE_ID,
          electionId: ELECTION_ID,
          candidateName: "Michael Jr",
          electionYear: 2026,
          officeScope: "statewide",
          officeName: "Governor",
          district: null,
        },
      ],
      exportFloridaContributionRowsFn,
      autoLinkMissingLinks: true,
      fetchMissingContributionData: false,
    });

    expect(exportFloridaContributionRowsFn).not.toHaveBeenCalled();
    expect(db.query).toHaveBeenCalledTimes(1);
  });

  it("fetches missing due contribution rows with the Florida export client and caches the artifact", async () => {
    const cacheDir = await makeTempDir();
    const db = createMockDb([
      {
        candidate_id: CANDIDATE_ID,
        candidate_election_id: CANDIDATE_ELECTION_ID,
        election_id: ELECTION_ID,
        candidate_name: "Jane Doe",
        election_year: 2026,
        office_name: "Governor",
        district: null,
        committee_id: "FRIENDS_OF_JANE_DOE",
        committee_name: "Friends of Jane Doe",
        source_url: null,
        last_synced_at: null,
        total_due_rows: "1",
      },
    ]);
    const liveCacheKey = "fl-contrib-committee-all-friends-of-jane-doe-live";
    const exportFloridaContributionRowsFn = vi.fn(async (input) => ({
      query: {
        searchType: "committee_detail" as const,
        electionCode: null,
        candidateFirstName: null,
        candidateLastName: null,
        committeeName: "Friends of Jane Doe",
        committeeType: null,
        dateFrom: null,
        dateTo: null,
        rowLimit: 10000,
      },
      searchPageUrl: "https://dos.elections.myflorida.com/campaign-finance/contributions/",
      exportUrl: "https://dos.elections.myflorida.com/cgi-bin/contrib.exe",
      sourceUrl: "https://dos.elections.myflorida.com/cgi-bin/contrib.exe?download=1",
      cacheKey: liveCacheKey,
      retrievedAt: new Date("2026-06-20T20:00:00.000Z"),
      rowCount: 1,
      formData: {
        search_on: "4",
        queryformat: "2",
        rowlimit: "10000",
        ComName: "Friends of Jane Doe",
      },
      tsv: SAMPLE_TSV,
      rows: [CONTRIBUTION_ROW],
    }));
    const syncFloridaCandidateFinanceFn = vi.fn(async (input) => ({
      candidateId: input.candidateId,
      electionId: input.electionId,
      electionYear: input.electionYear,
      dryRun: false,
      resolution: {
        status: "matched" as const,
        committeeId: input.trustedCommittee.committeeId,
        committeeName: input.trustedCommittee.committeeName,
        recipientNames: [input.trustedCommittee.committeeName],
        confidence: "exact" as const,
        source: "dos_export" as const,
        sourceUrl: null,
      },
      linkWritten: true,
      summaryWritten: true,
      directBreakdownsWritten: 1,
      outsideGroupsWritten: 0,
      outsideGroupBreakdownsWritten: 0,
      outsideGroupSupportLinksWritten: 0,
      resolvedOutsideGroupCount: 0,
      outsideGroupSupportEvidenceCount: 0,
      heuristicOutsideGroupCount: 0,
      totalReceipts: 100,
      directContributionTotal: 100,
      outsideSupportTotal: null,
      outsideOpposeTotal: null,
      matchedContributionRowCount: input.contributionRows.length,
      includedContributionRowCount: input.contributionRows.length,
      skippedContributionRowCount: 0,
      matchedOutsideContributionRowCount: 0,
      includedOutsideContributionRowCount: 0,
      skippedOutsideContributionRowCount: 0,
    }));

    const result = await syncDueFloridaCandidateFinance({
      db,
      now: new Date("2026-06-01T00:00:00.000Z"),
      maxCandidates: 1,
      staleAfterDays: 7,
      electionLookbackDays: 30,
      autoLinkMissingLinks: false,
      defaultArtifactCacheDir: cacheDir,
      exportFloridaContributionRowsFn,
      exportMinIntervalMs: 0,
      syncFloridaCandidateFinanceFn,
    });

    expect(result).toMatchObject({
      dueCandidateCount: 1,
      selectedCandidateCount: 1,
      syncedCandidateCount: 1,
      failedCandidateCount: 0,
    });
    expect(exportFloridaContributionRowsFn).toHaveBeenCalledWith(
      expect.objectContaining({
        searchType: "committee_detail",
        committeeName: "Friends of Jane Doe",
        rowLimit: 10000,
      })
    );
    expect(syncFloridaCandidateFinanceFn).toHaveBeenCalledWith(
      expect.objectContaining({
        contributionRows: [CONTRIBUTION_ROW],
        contributionArtifact: { cacheDir, cacheKey: liveCacheKey },
        contributionSourceUrl: "https://dos.elections.myflorida.com/cgi-bin/contrib.exe?download=1",
      })
    );
    await expect(readFloridaContributionExportArtifact({ cacheDir, cacheKey: liveCacheKey })).resolves.toMatchObject({
      metadata: {
        cacheKey: liveCacheKey,
        rowCount: 1,
      },
    });
  });

  it("auto-links missing Florida finance links from injected DOS rows before selecting due links", async () => {
    const db = {
      query: vi
        .fn()
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
          rowCount: 1,
        })
        .mockResolvedValueOnce({ rows: [{ id: "link-1" }], rowCount: 1 })
        .mockResolvedValueOnce({ rows: [], rowCount: 0 }),
    };

    const result = await syncDueFloridaCandidateFinance({
      db,
      now: new Date("2026-06-01T00:00:00.000Z"),
      maxCandidates: 25,
      staleAfterDays: 7,
      electionLookbackDays: 30,
      autoLinkContributionRowsByYear: new Map([[2026, [CONTRIBUTION_ROW]]]),
      autoLinkSourceUrlByYear: new Map([
        [2026, "https://dos.elections.myflorida.com/campaign-finance/contributions/"],
      ]),
    });

    expect(result).toMatchObject({
      dryRun: false,
      dueCandidateCount: 0,
      selectedCandidateCount: 0,
      syncedCandidateCount: 0,
      failedCandidateCount: 0,
    });
    expect(db.query).toHaveBeenCalledTimes(3);
    expect(String(db.query.mock.calls[0]?.[0])).toContain("FROM public.fl_candidate_finance_links AS link");
    expect(String(db.query.mock.calls[1]?.[0])).toContain("INSERT INTO public.fl_candidate_finance_links");
    expect(String(db.query.mock.calls[2]?.[0])).toContain("FROM public.fl_candidate_finance_links AS link");
    expect(db.query.mock.calls[1]?.[1]).toEqual([
      CANDIDATE_ID,
      ELECTION_ID,
      2026,
      "JANE DOE",
      "Governor",
      null,
      "FRIENDS_OF_JANE_DOE",
      "Friends of Jane Doe",
      "active",
      "dos_export",
      "https://dos.elections.myflorida.com/campaign-finance/contributions/",
      "2026-06-01T00:00:00.000Z",
    ]);
  });

  it("runs explicit trusted sync inputs and honors maxCandidates", async () => {
    const syncFloridaCandidateFinanceFn = vi.fn(async (input) => ({
      candidateId: input.candidateId,
      electionId: input.electionId,
      electionYear: input.electionYear,
      dryRun: input.dryRun === true,
      resolution: {
        status: "matched" as const,
        committeeId: input.trustedCommittee.committeeId,
        committeeName: input.trustedCommittee.committeeName,
        recipientNames: [input.trustedCommittee.committeeName],
        confidence: "exact" as const,
        source: "manual" as const,
        sourceUrl: null,
      },
      linkWritten: true,
      summaryWritten: true,
      directBreakdownsWritten: 1,
      outsideGroupsWritten: 0,
      outsideGroupBreakdownsWritten: 0,
      outsideGroupSupportLinksWritten: 0,
      resolvedOutsideGroupCount: 0,
      outsideGroupSupportEvidenceCount: 0,
      heuristicOutsideGroupCount: 0,
      totalReceipts: 100,
      directContributionTotal: 100,
      outsideSupportTotal: null,
      outsideOpposeTotal: null,
      matchedContributionRowCount: input.contributionRows.length,
      includedContributionRowCount: input.contributionRows.length,
      skippedContributionRowCount: 0,
      matchedOutsideContributionRowCount: 0,
      includedOutsideContributionRowCount: 0,
      skippedOutsideContributionRowCount: 0,
    }));

    const result = await syncFloridaCandidateFinanceBatch({
      db: { query: vi.fn() },
      now: new Date("2026-06-01T00:00:00.000Z"),
      dryRun: true,
      maxCandidates: 1,
      syncInputs: [baseInput(), baseInput({ candidateId: "candidate-2", electionId: "election-2" })],
      syncFloridaCandidateFinanceFn,
    });

    expect(result).toMatchObject({
      dryRun: true,
      maxCandidates: 1,
      dueCandidateCount: 2,
      selectedCandidateCount: 1,
      syncedCandidateCount: 1,
      failedCandidateCount: 0,
    });
    expect(syncFloridaCandidateFinanceFn).toHaveBeenCalledTimes(1);
    expect(syncFloridaCandidateFinanceFn).toHaveBeenCalledWith(
      expect.objectContaining({
        candidateId: "candidate-1",
        dryRun: true,
        contributionRows: [CONTRIBUTION_ROW],
      })
    );
  });

  it("loads contribution rows from cached export artifacts", async () => {
    const cacheDir = await makeTempDir();
    const cacheKey = "fl-contrib-candidate-20261103-gen-doe-jane-batchtest";
    await writeFloridaContributionExportArtifact({ cacheDir, result: exportResult(cacheKey) });
    const syncFloridaCandidateFinanceFn = vi.fn(async (input) => ({
      candidateId: input.candidateId,
      electionId: input.electionId,
      electionYear: input.electionYear,
      dryRun: false,
      resolution: {
        status: "matched" as const,
        committeeId: input.trustedCommittee.committeeId,
        committeeName: input.trustedCommittee.committeeName,
        recipientNames: [input.trustedCommittee.committeeName],
        confidence: "exact" as const,
        source: "manual" as const,
        sourceUrl: null,
      },
      linkWritten: true,
      summaryWritten: true,
      directBreakdownsWritten: 1,
      outsideGroupsWritten: 0,
      outsideGroupBreakdownsWritten: 0,
      outsideGroupSupportLinksWritten: 0,
      resolvedOutsideGroupCount: 0,
      outsideGroupSupportEvidenceCount: 0,
      heuristicOutsideGroupCount: 0,
      totalReceipts: 100,
      directContributionTotal: 100,
      outsideSupportTotal: null,
      outsideOpposeTotal: null,
      matchedContributionRowCount: input.contributionRows.length,
      includedContributionRowCount: input.contributionRows.length,
      skippedContributionRowCount: 0,
      matchedOutsideContributionRowCount: 0,
      includedOutsideContributionRowCount: 0,
      skippedOutsideContributionRowCount: 0,
    }));

    await syncFloridaCandidateFinanceBatch({
      db: { query: vi.fn() },
      now: new Date("2026-06-01T00:00:00.000Z"),
      defaultArtifactCacheDir: cacheDir,
      syncInputs: [
        baseInput({
          contributionRows: undefined,
          contributionArtifact: { cacheKey },
        }),
      ],
      syncFloridaCandidateFinanceFn,
    });

    expect(syncFloridaCandidateFinanceFn).toHaveBeenCalledWith(
      expect.objectContaining({
        contributionRows: [
          expect.objectContaining({
            contributorName: "Smith, Pat",
            electionCode: "20261103-GEN",
          }),
        ],
      })
    );
  });

  it("records item failures without failing the whole batch", async () => {
    const result = await syncFloridaCandidateFinanceBatch({
      db: { query: vi.fn() },
      now: new Date("2026-06-01T00:00:00.000Z"),
      syncInputs: [baseInput()],
      syncFloridaCandidateFinanceFn: vi.fn(async () => {
        throw new Error("sync failed");
      }),
    });

    expect(result).toMatchObject({
      dueCandidateCount: 1,
      selectedCandidateCount: 1,
      syncedCandidateCount: 0,
      failedCandidateCount: 1,
      results: [
        {
          candidateId: "candidate-1",
          electionId: "election-1",
          electionYear: 2026,
          committeeId: "FRIENDS_OF_JANE_DOE",
          ok: false,
          error: "sync failed",
        },
      ],
    });
  });
});
