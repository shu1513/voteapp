import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";

import {
  syncFloridaCandidateFinanceBatch,
  type FloridaCandidateFinanceBatchSyncItemInput,
} from "../../../src/pipeline/floridaFinance/floridaCandidateFinanceBatchSync.js";
import { writeFloridaContributionExportArtifact } from "../../../src/pipeline/floridaFinance/floridaCampaignFinanceArtifactCache.js";
import type { FloridaContributionExportRowsResult } from "../../../src/pipeline/floridaFinance/floridaCampaignFinanceClient.js";
import type { FloridaContributionRow } from "../../../src/pipeline/floridaFinance/floridaCampaignFinanceRows.js";

const tempDirs: string[] = [];

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
