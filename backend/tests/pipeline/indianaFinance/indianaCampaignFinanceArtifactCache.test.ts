import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";

import { afterEach, describe, expect, it, vi } from "vitest";

import {
  buildIndianaCampaignFinanceArtifactUrl,
  getIndianaCampaignFinanceArtifactCachePaths,
  readIndianaCampaignFinanceArtifactCacheMetadata,
  refreshIndianaCampaignFinanceArtifactCache,
} from "../../../src/pipeline/indianaFinance/indianaCampaignFinanceArtifactCache.js";

let tempDirs: string[] = [];

async function tempDir(): Promise<string> {
  const dir = await mkdtemp(path.join(tmpdir(), "in-finance-cache-"));
  tempDirs.push(dir);
  return dir;
}

describe("indianaCampaignFinanceArtifactCache", () => {
  afterEach(async () => {
    const dirs = tempDirs;
    tempDirs = [];
    await Promise.all(dirs.map((dir) => rm(dir, { recursive: true, force: true })));
    vi.restoreAllMocks();
  });

  it("builds official contribution and expenditure artifact URLs", () => {
    expect(buildIndianaCampaignFinanceArtifactUrl({ year: 2026, artifactKind: "contribution" })).toBe(
      "https://campaignfinance.in.gov/PublicSite/Docs/BulkDataDownloads/2026_ContributionData.csv.zip"
    );
    expect(buildIndianaCampaignFinanceArtifactUrl({ year: 2026, artifactKind: "expenditure" })).toBe(
      "https://campaignfinance.in.gov/PublicSite/Docs/BulkDataDownloads/2026_ExpenditureData.csv.zip"
    );
  });

  it("reuses cached artifacts when remote metadata matches", async () => {
    const cacheDir = await tempDir();
    const paths = getIndianaCampaignFinanceArtifactCachePaths({ cacheDir, year: 2026, artifactKind: "contribution" });
    await writeFile(paths.zipPath, "zip-bytes", "utf8");
    await writeFile(
      paths.metadataPath,
      `${JSON.stringify({
        version: 1,
        artifact: { year: 2026, artifactKind: "contribution" },
        zipPath: paths.zipPath,
        metadataPath: paths.metadataPath,
        downloadedAt: "2026-01-01T00:00:00.000Z",
        remote: {
          year: 2026,
          artifactKind: "contribution",
          url: "https://example.test/2026_ContributionData.csv.zip",
          contentLength: 9,
          contentType: "application/zip",
          etag: '"abc"',
          lastModified: "Thu, 01 Jan 2026 00:00:00 GMT",
        },
        bytesWritten: 9,
      })}\n`,
      "utf8"
    );

    const fetchImpl = vi.fn().mockResolvedValue(
      new Response(null, {
        status: 200,
        headers: {
          "content-length": "9",
          "content-type": "application/zip",
          etag: '"abc"',
          "last-modified": "Thu, 01 Jan 2026 00:00:00 GMT",
        },
      })
    ) as unknown as typeof fetch;

    await expect(
      refreshIndianaCampaignFinanceArtifactCache({
        cacheDir,
        year: 2026,
        artifactKind: "contribution",
        url: "https://example.test/2026_ContributionData.csv.zip",
        fetchImpl,
      })
    ).resolves.toMatchObject({ status: "unchanged", zipPath: paths.zipPath });
    expect(fetchImpl).toHaveBeenCalledTimes(1);
    expect(fetchImpl).toHaveBeenCalledWith(
      "https://example.test/2026_ContributionData.csv.zip",
      expect.objectContaining({
        method: "HEAD",
        headers: expect.any(Headers),
      })
    );
    const requestHeaders = fetchImpl.mock.calls[0]?.[1]?.headers as Headers;
    expect(requestHeaders.get("accept")).toContain("application/zip");
    expect(requestHeaders.get("user-agent")).toContain("Mozilla/5.0");
  });

  it("returns null for missing or invalid cache metadata", async () => {
    const cacheDir = await tempDir();
    const paths = getIndianaCampaignFinanceArtifactCachePaths({ cacheDir, year: 2026, artifactKind: "expenditure" });
    const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});

    await expect(readIndianaCampaignFinanceArtifactCacheMetadata(paths.metadataPath)).resolves.toBeNull();
    await writeFile(paths.metadataPath, "{", "utf8");
    await expect(readIndianaCampaignFinanceArtifactCacheMetadata(paths.metadataPath)).resolves.toBeNull();
    expect(warnSpy).toHaveBeenCalledTimes(1);
  });
});
