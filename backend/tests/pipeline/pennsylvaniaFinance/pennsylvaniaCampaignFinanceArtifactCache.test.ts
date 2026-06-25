import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";

import {
  buildPennsylvaniaCampaignFinanceExportUrl,
  getPennsylvaniaCampaignFinanceExportCachePaths,
  parsePennsylvaniaCampaignFinanceHttpsUrl,
  readPennsylvaniaCampaignFinanceExportCacheMetadata,
  refreshPennsylvaniaCampaignFinanceExportCache,
  type PennsylvaniaCampaignFinanceExportExtractor,
} from "../../../src/pipeline/pennsylvaniaFinance/pennsylvaniaCampaignFinanceArtifactCache.js";

const tempDirs: string[] = [];

async function makeTempDir(): Promise<string> {
  const dir = await mkdtemp(join(tmpdir(), "voteapp-pa-cf-cache-"));
  tempDirs.push(dir);
  return dir;
}

function response(body: string | null, headers: Record<string, string> = {}): Response {
  return new Response(body, {
    status: 200,
    headers,
  });
}

function fakeExtractor(): PennsylvaniaCampaignFinanceExportExtractor {
  return async ({ extractedDir, year }) => {
    await mkdir(extractedDir, { recursive: true });
    await writeFile(join(extractedDir, `filer_${year}.txt`), "ok", "utf8");
  };
}

afterEach(async () => {
  vi.restoreAllMocks();
  await Promise.all(tempDirs.splice(0).map((dir) => rm(dir, { recursive: true, force: true })));
});

describe("Pennsylvania campaign finance export artifact cache", () => {
  it("builds official yearly export URLs and rejects non-HTTPS URLs", () => {
    expect(buildPennsylvaniaCampaignFinanceExportUrl({ year: 2026 })).toBe(
      "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/resources/voting-and-elections/campaign-finance/campaign-finance-data/2026.zip"
    );
    expect(parsePennsylvaniaCampaignFinanceHttpsUrl("https://www.pa.gov/example.zip")).toBe(
      "https://www.pa.gov/example.zip"
    );
    expect(() => parsePennsylvaniaCampaignFinanceHttpsUrl("http://www.pa.gov/example.zip", "--url")).toThrow(
      "Only https is allowed"
    );
    expect(() => buildPennsylvaniaCampaignFinanceExportUrl({ year: 1999 })).toThrow(
      "Invalid Pennsylvania campaign finance export year"
    );
  });

  it("builds stable cache paths", async () => {
    const cacheDir = await makeTempDir();
    const paths = getPennsylvaniaCampaignFinanceExportCachePaths({ cacheDir, year: 2024 });

    expect(paths.cacheDir).toBe(cacheDir);
    expect(paths.archivePath).toBe(join(cacheDir, "2024.zip"));
    expect(paths.extractedDir).toBe(join(cacheDir, "2024"));
    expect(paths.metadataPath).toBe(join(cacheDir, "2024.metadata.json"));
  });

  it("downloads, extracts, and writes metadata when the export is missing", async () => {
    const cacheDir = await makeTempDir();
    const extractArchive = vi.fn(fakeExtractor());
    const fetchImpl = vi.fn<typeof fetch>(async (_url, init) => {
      if (init?.method === "HEAD") {
        return response(null, {
          "content-length": "8",
          "content-type": "application/zip",
          etag: "\"pa-2026-a\"",
          "last-modified": "Mon, 22 Jun 2026 13:57:46 GMT",
        });
      }
      return response("zip-data", {
        "content-length": "8",
        "content-type": "application/zip",
        etag: "\"pa-2026-a\"",
        "last-modified": "Mon, 22 Jun 2026 13:57:46 GMT",
      });
    });

    const result = await refreshPennsylvaniaCampaignFinanceExportCache({
      year: 2026,
      cacheDir,
      fetchImpl,
      extractArchive,
      now: new Date("2026-06-25T12:00:00.000Z"),
    });

    expect(result.status).toBe("downloaded");
    expect(fetchImpl).toHaveBeenCalledTimes(2);
    expect(extractArchive).toHaveBeenCalledWith(
      expect.objectContaining({
        archivePath: expect.stringContaining(`${result.archivePath}.tmp-`),
        extractedDir: expect.stringContaining(`${result.extractedDir}.tmp-`),
        year: 2026,
      })
    );
    expect(await readFile(result.archivePath, "utf8")).toBe("zip-data");
    await expect(readFile(join(result.extractedDir, "filer_2026.txt"), "utf8")).resolves.toBe("ok");
    await expect(readPennsylvaniaCampaignFinanceExportCacheMetadata(result.metadataPath)).resolves.toMatchObject({
      version: 1,
      year: 2026,
      archivePath: result.archivePath,
      extractedDir: result.extractedDir,
      downloadedAt: "2026-06-25T12:00:00.000Z",
      bytesWritten: 8,
      remote: {
        year: 2026,
        etag: "\"pa-2026-a\"",
      },
    });
  });

  it("extracts a matching cached export when extracted files are missing", async () => {
    const cacheDir = await makeTempDir();
    const paths = getPennsylvaniaCampaignFinanceExportCachePaths({ cacheDir, year: 2024 });
    await writeFile(paths.archivePath, "cached-zip", "utf8");
    await writeFile(
      paths.metadataPath,
      `${JSON.stringify(
        {
          version: 1,
          year: 2024,
          archivePath: paths.archivePath,
          metadataPath: paths.metadataPath,
          downloadedAt: "2026-06-24T12:00:00.000Z",
          bytesWritten: 10,
          remote: {
            year: 2024,
            url: buildPennsylvaniaCampaignFinanceExportUrl({ year: 2024 }),
            contentLength: 10,
            contentType: "application/zip",
            etag: "\"same\"",
            lastModified: "Tue, 14 Oct 2025 15:42:13 GMT",
          },
        },
        null,
        2
      )}\n`,
      "utf8"
    );
    const extractArchive = vi.fn(fakeExtractor());
    const fetchImpl = vi.fn<typeof fetch>(async () =>
      response(null, {
        "content-length": "10",
        "content-type": "application/zip",
        etag: "\"same\"",
        "last-modified": "Tue, 14 Oct 2025 15:42:13 GMT",
      })
    );

    const result = await refreshPennsylvaniaCampaignFinanceExportCache({
      year: 2024,
      cacheDir,
      fetchImpl,
      extractArchive,
    });

    expect(result.status).toBe("extracted");
    expect(fetchImpl).toHaveBeenCalledTimes(1);
    expect(extractArchive).toHaveBeenCalledTimes(1);
    await expect(readFile(join(result.extractedDir, "filer_2024.txt"), "utf8")).resolves.toBe("ok");
  });
});
