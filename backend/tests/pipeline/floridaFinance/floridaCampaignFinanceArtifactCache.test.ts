import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";

import {
  getFloridaContributionExportArtifactPaths,
  readFloridaContributionExportArtifact,
  readFloridaContributionExportArtifactMetadata,
  writeFloridaContributionExportArtifact,
} from "../../../src/pipeline/floridaFinance/floridaCampaignFinanceArtifactCache.js";
import type { FloridaContributionExportRowsResult } from "../../../src/pipeline/floridaFinance/floridaCampaignFinanceClient.js";

const tempDirs: string[] = [];

const SAMPLE_TSV = [
  "Candidate/Committee\tDate\tAmount\tTyp\tContributor Name\tAddress\tCity\tState\tZip\tOccupation\tInkind Desc",
  "Friends of Jane Doe\t9/15/2026\t100\tCHE\tSmith, Pat\t1 Main St\tTallahassee\tFL\t32301\tAttorney\t",
].join("\n");

async function makeTempDir(): Promise<string> {
  const dir = await mkdtemp(join(tmpdir(), "voteapp-fl-dos-cache-"));
  tempDirs.push(dir);
  return dir;
}

function exportResult(
  overrides: Partial<FloridaContributionExportRowsResult> = {}
): FloridaContributionExportRowsResult {
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
    cacheKey: "fl-contrib-candidate-20261103-gen-doe-jane-abcdef123456",
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
    ...overrides,
  };
}

afterEach(async () => {
  await Promise.all(tempDirs.splice(0).map((dir) => rm(dir, { recursive: true, force: true })));
});

describe("floridaCampaignFinanceArtifactCache", () => {
  it("writes and reads Florida contribution export artifacts", async () => {
    const cacheDir = await makeTempDir();
    const result = exportResult();

    const metadata = await writeFloridaContributionExportArtifact({ cacheDir, result });
    const metadataFromDisk = await readFloridaContributionExportArtifactMetadata(metadata.metadataPath);
    const artifact = await readFloridaContributionExportArtifact({ cacheDir, cacheKey: result.cacheKey });

    expect(metadataFromDisk).toEqual(metadata);
    expect(artifact?.metadata).toEqual(metadata);
    expect(artifact?.tsv).toBe(SAMPLE_TSV);
    expect(artifact?.rows).toHaveLength(1);
    expect(artifact?.rows[0]).toMatchObject({
      contributorName: "Smith, Pat",
      electionCode: "20261103-GEN",
      sourceUrl: "https://dos.elections.myflorida.com/cgi-bin/contrib.exe?download=1",
    });
    expect(metadata).toMatchObject({
      version: 1,
      cacheKey: result.cacheKey,
      request: result.query,
      retrievedAt: "2026-06-20T20:00:00.000Z",
      rowCount: 1,
      formData: result.formData,
    });
  });

  it("returns null when the cache artifact is missing", async () => {
    await expect(
      readFloridaContributionExportArtifact({
        cacheDir: await makeTempDir(),
        cacheKey: "fl-contrib-candidate-20261103-gen-doe-jane-abcdef123456",
      })
    ).resolves.toBeNull();
  });

  it("warns and returns null when metadata cannot be parsed", async () => {
    const cacheDir = await makeTempDir();
    const metadataPath = join(cacheDir, "bad.metadata.json");
    const warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);

    await writeFile(metadataPath, "{", "utf8");

    await expect(readFloridaContributionExportArtifactMetadata(metadataPath)).resolves.toBeNull();
    expect(warn).toHaveBeenCalledWith(
      `Failed to read Florida contribution export metadata at ${metadataPath}`,
      expect.any(SyntaxError)
    );
    warn.mockRestore();
  });

  it("rejects unsafe cache keys", () => {
    expect(() =>
      getFloridaContributionExportArtifactPaths({
        cacheDir: "/tmp/florida",
        cacheKey: "../escape",
      })
    ).toThrow("Invalid Florida contribution export cache key");
  });
});
