import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";

import {
  TEXAS_TEC_CSV_DATABASE_URL,
  getTexasTecCsvDatabaseArtifactCachePaths,
  parseTexasTecHttpsUrl,
  readTexasTecCsvDatabaseArtifactCacheMetadata,
  refreshTexasTecCsvDatabaseArtifactCache,
} from "../../../src/pipeline/texasFinance/texasTecCsvDatabaseArtifactCache.js";

const tempDirs: string[] = [];

async function makeTempDir(): Promise<string> {
  const dir = await mkdtemp(join(tmpdir(), "voteapp-tx-tec-cache-"));
  tempDirs.push(dir);
  return dir;
}

function response(body: string | null, headers: Record<string, string> = {}): Response {
  return new Response(body, {
    status: 200,
    headers,
  });
}

afterEach(async () => {
  vi.restoreAllMocks();
  await Promise.all(tempDirs.splice(0).map((dir) => rm(dir, { recursive: true, force: true })));
});

describe("Texas TEC CSV database artifact cache", () => {
  it("uses the official TEC CSV ZIP URL and rejects non-HTTPS URLs", () => {
    expect(TEXAS_TEC_CSV_DATABASE_URL).toBe(
      "https://prd.tecprd.ethicsefile.com/public/cf/public/TEC_CF_CSV.zip"
    );
    expect(parseTexasTecHttpsUrl(TEXAS_TEC_CSV_DATABASE_URL)).toBe(TEXAS_TEC_CSV_DATABASE_URL);
    expect(() => parseTexasTecHttpsUrl("http://example.com/TEC_CF_CSV.zip", "--url")).toThrow(
      "Only https is allowed"
    );
  });

  it("builds stable cache paths", async () => {
    const cacheDir = await makeTempDir();
    const paths = getTexasTecCsvDatabaseArtifactCachePaths(cacheDir);

    expect(paths.cacheDir).toBe(cacheDir);
    expect(paths.zipPath).toBe(join(cacheDir, "TEC_CF_CSV.zip"));
    expect(paths.metadataPath).toBe(join(cacheDir, "TEC_CF_CSV.metadata.json"));
  });

  it("rejects invalid refresh timestamps before remote fetches", async () => {
    const cacheDir = await makeTempDir();
    const fetchImpl = vi.fn<typeof fetch>();

    await expect(
      refreshTexasTecCsvDatabaseArtifactCache({
        cacheDir,
        fetchImpl,
        now: new Date("not a date"),
      })
    ).rejects.toThrow("Invalid Texas TEC CSV database artifact refresh timestamp");

    expect(fetchImpl).not.toHaveBeenCalled();
  });

  it("downloads and writes cache metadata when the artifact is missing", async () => {
    const cacheDir = await makeTempDir();
    const fetchImpl = vi.fn<typeof fetch>(async (_url, init) => {
      if (init?.method === "HEAD") {
        return response(null, {
          "content-length": "8",
          "content-type": "application/zip",
          etag: "\"tx-tec-a\"",
          "last-modified": "Sun, 21 Jun 2026 04:00:00 GMT",
        });
      }
      return response("zip-data", {
        "content-length": "8",
        "content-type": "application/zip",
        etag: "\"tx-tec-a\"",
        "last-modified": "Sun, 21 Jun 2026 04:00:00 GMT",
      });
    });

    const result = await refreshTexasTecCsvDatabaseArtifactCache({
      cacheDir,
      fetchImpl,
      now: new Date("2026-06-21T12:00:00.000Z"),
    });

    expect(result.status).toBe("downloaded");
    expect(fetchImpl).toHaveBeenCalledTimes(2);
    expect(await readFile(result.zipPath, "utf8")).toBe("zip-data");
    const metadata = await readTexasTecCsvDatabaseArtifactCacheMetadata(result.metadataPath);
    expect(metadata).toMatchObject({
      version: 1,
      downloadedAt: "2026-06-21T12:00:00.000Z",
      bytesWritten: 8,
      remote: {
        url: TEXAS_TEC_CSV_DATABASE_URL,
        etag: "\"tx-tec-a\"",
        lastModified: "Sun, 21 Jun 2026 04:00:00 GMT",
      },
    });
  });

  it("skips download when cached metadata still matches", async () => {
    const cacheDir = await makeTempDir();
    const paths = getTexasTecCsvDatabaseArtifactCachePaths(cacheDir);
    await writeFile(paths.zipPath, "cached-zip", "utf8");
    await writeFile(
      paths.metadataPath,
      `${JSON.stringify(
        {
          version: 1,
          zipPath: paths.zipPath,
          metadataPath: paths.metadataPath,
          downloadedAt: "2026-06-20T12:00:00.000Z",
          bytesWritten: 10,
          remote: {
            url: TEXAS_TEC_CSV_DATABASE_URL,
            contentLength: 10,
            contentType: "application/zip",
            etag: "\"same\"",
            lastModified: "Sun, 21 Jun 2026 04:00:00 GMT",
          },
        },
        null,
        2
      )}\n`,
      "utf8"
    );

    const fetchImpl = vi.fn<typeof fetch>(async () =>
      response(null, {
        "content-length": "10",
        "content-type": "application/zip",
        etag: "\"same\"",
        "last-modified": "Sun, 21 Jun 2026 04:00:00 GMT",
      })
    );

    const result = await refreshTexasTecCsvDatabaseArtifactCache({
      cacheDir,
      fetchImpl,
    });

    expect(result.status).toBe("unchanged");
    expect(fetchImpl).toHaveBeenCalledTimes(1);
    expect(await readFile(paths.zipPath, "utf8")).toBe("cached-zip");
  });

  it("warns and returns null when cache metadata cannot be parsed", async () => {
    const cacheDir = await makeTempDir();
    const paths = getTexasTecCsvDatabaseArtifactCachePaths(cacheDir);
    await writeFile(paths.metadataPath, "{not-json", "utf8");
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});

    await expect(readTexasTecCsvDatabaseArtifactCacheMetadata(paths.metadataPath)).resolves.toBeNull();
    expect(warn).toHaveBeenCalledWith(
      expect.stringContaining("Unexpected error reading Texas TEC CSV database cache metadata"),
      expect.any(Error)
    );
  });

  it("redownloads matching metadata when force is true", async () => {
    const cacheDir = await makeTempDir();
    const paths = getTexasTecCsvDatabaseArtifactCachePaths(cacheDir);
    await writeFile(paths.zipPath, "old", "utf8");
    await writeFile(
      paths.metadataPath,
      `${JSON.stringify(
        {
          version: 1,
          zipPath: paths.zipPath,
          metadataPath: paths.metadataPath,
          downloadedAt: "2026-06-20T12:00:00.000Z",
          bytesWritten: 3,
          remote: {
            url: TEXAS_TEC_CSV_DATABASE_URL,
            contentLength: 3,
            contentType: "application/zip",
            etag: "\"same\"",
            lastModified: null,
          },
        },
        null,
        2
      )}\n`,
      "utf8"
    );
    const fetchImpl = vi.fn<typeof fetch>(async (_url, init) => {
      if (init?.method === "HEAD") {
        return response(null, {
          "content-length": "7",
          etag: "\"same\"",
        });
      }
      return response("new-zip", {
        "content-length": "7",
        etag: "\"same\"",
      });
    });

    const result = await refreshTexasTecCsvDatabaseArtifactCache({
      cacheDir,
      fetchImpl,
      force: true,
    });

    expect(result.status).toBe("downloaded");
    expect(fetchImpl).toHaveBeenCalledTimes(2);
    expect(await readFile(paths.zipPath, "utf8")).toBe("new-zip");
  });
});
