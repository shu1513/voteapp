import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";

import {
  buildNebraskaNadcArtifactUrl,
  getNebraskaNadcArtifactCachePaths,
  parseNebraskaNadcHttpsUrl,
  readNebraskaNadcArtifactCacheMetadata,
  refreshNebraskaNadcArtifactCache,
} from "../../../src/pipeline/nebraskaFinance/nebraskaNadcArtifactCache.js";

const tempDirs: string[] = [];

async function makeTempDir(): Promise<string> {
  const dir = await mkdtemp(join(tmpdir(), "voteapp-ne-nadc-cache-"));
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

describe("Nebraska NADC artifact cache", () => {
  it("builds official yearly contribution and expenditure ZIP URLs", () => {
    expect(buildNebraskaNadcArtifactUrl({ year: 2026, artifactKind: "contribution_loan" })).toBe(
      "https://nadc-e.nebraska.gov/PublicSite/Docs/BulkDataDownloads/2026_ContributionLoanExtract.csv.zip"
    );
    expect(buildNebraskaNadcArtifactUrl({ year: 2026, artifactKind: "expenditure" })).toBe(
      "https://nadc-e.nebraska.gov/PublicSite/Docs/BulkDataDownloads/2026_ExpenditureExtract.csv.zip"
    );
  });

  it("rejects non-HTTPS URLs", () => {
    expect(() => parseNebraskaNadcHttpsUrl("http://example.com/file.zip", "--url")).toThrow("Only https is allowed");
    expect(() =>
      buildNebraskaNadcArtifactUrl({
        year: 2026,
        artifactKind: "contribution_loan",
        baseUrl: "http://example.com",
      })
    ).toThrow("Only https is allowed");
  });

  it("rejects invalid years", () => {
    expect(() => buildNebraskaNadcArtifactUrl({ year: 2020, artifactKind: "contribution_loan" })).toThrow(
      "Invalid Nebraska NADC artifact year"
    );
    expect(() => buildNebraskaNadcArtifactUrl({ year: 2026.5, artifactKind: "expenditure" })).toThrow(
      "Invalid Nebraska NADC artifact year"
    );
  });

  it("downloads and writes cache metadata when the artifact is missing", async () => {
    const cacheDir = await makeTempDir();
    const fetchImpl = vi.fn<typeof fetch>(async (_url, init) => {
      if (init?.method === "HEAD") {
        return response(null, {
          "content-length": "8",
          "content-type": "application/zip",
          etag: '"ne-2026-a"',
          "last-modified": "Sat, 20 Jun 2026 07:26:14 GMT",
        });
      }
      return response("zip-data", {
        "content-length": "8",
        "content-type": "application/zip",
        etag: '"ne-2026-a"',
        "last-modified": "Sat, 20 Jun 2026 07:26:14 GMT",
      });
    });

    const result = await refreshNebraskaNadcArtifactCache({
      year: 2026,
      artifactKind: "contribution_loan",
      cacheDir,
      fetchImpl,
      now: new Date("2026-06-20T12:00:00.000Z"),
    });

    expect(result.status).toBe("downloaded");
    expect(fetchImpl).toHaveBeenCalledTimes(2);
    expect(await readFile(result.zipPath, "utf8")).toBe("zip-data");
    const metadata = await readNebraskaNadcArtifactCacheMetadata(result.metadataPath);
    expect(metadata).toMatchObject({
      version: 1,
      artifact: {
        year: 2026,
        artifactKind: "contribution_loan",
      },
      downloadedAt: "2026-06-20T12:00:00.000Z",
      bytesWritten: 8,
      remote: {
        year: 2026,
        artifactKind: "contribution_loan",
        etag: '"ne-2026-a"',
        lastModified: "Sat, 20 Jun 2026 07:26:14 GMT",
      },
    });
  });

  it("skips download when cached metadata still matches", async () => {
    const cacheDir = await makeTempDir();
    const paths = getNebraskaNadcArtifactCachePaths({ cacheDir, year: 2026, artifactKind: "expenditure" });
    await writeFile(paths.zipPath, "cached-zip", "utf8");
    await writeFile(
      paths.metadataPath,
      `${JSON.stringify(
        {
          version: 1,
          artifact: {
            year: 2026,
            artifactKind: "expenditure",
          },
          zipPath: paths.zipPath,
          metadataPath: paths.metadataPath,
          downloadedAt: "2026-06-19T12:00:00.000Z",
          bytesWritten: 10,
          remote: {
            year: 2026,
            artifactKind: "expenditure",
            url: buildNebraskaNadcArtifactUrl({ year: 2026, artifactKind: "expenditure" }),
            contentLength: 10,
            contentType: "application/zip",
            etag: '"same"',
            lastModified: "Sat, 20 Jun 2026 07:26:15 GMT",
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
        etag: '"same"',
        "last-modified": "Sat, 20 Jun 2026 07:26:15 GMT",
      })
    );

    const result = await refreshNebraskaNadcArtifactCache({
      year: 2026,
      artifactKind: "expenditure",
      cacheDir,
      fetchImpl,
    });

    expect(result.status).toBe("unchanged");
    expect(fetchImpl).toHaveBeenCalledTimes(1);
    expect(await readFile(paths.zipPath, "utf8")).toBe("cached-zip");
  });

  it("warns and returns null when cache metadata cannot be parsed", async () => {
    const cacheDir = await makeTempDir();
    const paths = getNebraskaNadcArtifactCachePaths({ cacheDir, year: 2026, artifactKind: "contribution_loan" });
    await writeFile(paths.metadataPath, "{not-json", "utf8");
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});

    await expect(readNebraskaNadcArtifactCacheMetadata(paths.metadataPath)).resolves.toBeNull();
    expect(warn).toHaveBeenCalledWith(
      expect.stringContaining("Unexpected error reading Nebraska NADC cache metadata"),
      expect.any(Error)
    );
  });

  it("redownloads matching metadata when force is true", async () => {
    const cacheDir = await makeTempDir();
    const paths = getNebraskaNadcArtifactCachePaths({ cacheDir, year: 2026, artifactKind: "contribution_loan" });
    await writeFile(paths.zipPath, "old", "utf8");
    await writeFile(
      paths.metadataPath,
      `${JSON.stringify(
        {
          version: 1,
          artifact: {
            year: 2026,
            artifactKind: "contribution_loan",
          },
          zipPath: paths.zipPath,
          metadataPath: paths.metadataPath,
          downloadedAt: "2026-06-19T12:00:00.000Z",
          bytesWritten: 3,
          remote: {
            year: 2026,
            artifactKind: "contribution_loan",
            url: buildNebraskaNadcArtifactUrl({ year: 2026, artifactKind: "contribution_loan" }),
            contentLength: 3,
            contentType: "application/zip",
            etag: '"same"',
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
          etag: '"same"',
        });
      }
      return response("new-zip", {
        "content-length": "7",
        etag: '"same"',
      });
    });

    const result = await refreshNebraskaNadcArtifactCache({
      year: 2026,
      artifactKind: "contribution_loan",
      cacheDir,
      fetchImpl,
      force: true,
    });

    expect(result.status).toBe("downloaded");
    expect(fetchImpl).toHaveBeenCalledTimes(2);
    expect(await readFile(paths.zipPath, "utf8")).toBe("new-zip");
  });
});
