import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";

import {
  buildColoradoTracerContributionZipUrl,
  getColoradoTracerContributionArtifactCachePaths,
  parseColoradoTracerHttpsUrl,
  readColoradoTracerContributionArtifactCacheMetadata,
  refreshColoradoTracerContributionArtifactCache,
} from "../../../src/pipeline/coloradoFinance/coloradoTracerContributionArtifactCache.js";

const tempDirs: string[] = [];

async function makeTempDir(): Promise<string> {
  const dir = await mkdtemp(join(tmpdir(), "voteapp-co-tracer-cache-"));
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

describe("Colorado TRACER contribution artifact cache", () => {
  it("builds the official yearly contribution ZIP URL", () => {
    expect(buildColoradoTracerContributionZipUrl({ year: 2024 })).toBe(
      "https://tracer.sos.colorado.gov/PublicSite/Docs/BulkDataDownloads/2024_ContributionData.csv.zip"
    );
  });

  it("rejects non-HTTPS URLs", () => {
    expect(() => parseColoradoTracerHttpsUrl("http://example.com/file.zip", "--url")).toThrow(
      "Only https is allowed"
    );
    expect(() => buildColoradoTracerContributionZipUrl({ year: 2024, baseUrl: "http://example.com" })).toThrow(
      "Only https is allowed"
    );
  });

  it("rejects invalid years", () => {
    expect(() => buildColoradoTracerContributionZipUrl({ year: 1999 })).toThrow(
      "Invalid Colorado TRACER contribution year"
    );
    expect(() => buildColoradoTracerContributionZipUrl({ year: 2024.5 })).toThrow(
      "Invalid Colorado TRACER contribution year"
    );
  });

  it("downloads and writes cache metadata when the artifact is missing", async () => {
    const cacheDir = await makeTempDir();
    const fetchImpl = vi.fn<typeof fetch>(async (_url, init) => {
      if (init?.method === "HEAD") {
        return response(null, {
          "content-length": "8",
          "content-type": "application/zip",
          etag: "\"co-2024-a\"",
          "last-modified": "Fri, 19 Jun 2026 09:00:49 GMT",
        });
      }
      return response("zip-data", {
        "content-length": "8",
        "content-type": "application/zip",
        etag: "\"co-2024-a\"",
        "last-modified": "Fri, 19 Jun 2026 09:00:49 GMT",
      });
    });

    const result = await refreshColoradoTracerContributionArtifactCache({
      year: 2024,
      cacheDir,
      fetchImpl,
      now: new Date("2026-06-19T12:00:00.000Z"),
    });

    expect(result.status).toBe("downloaded");
    expect(fetchImpl).toHaveBeenCalledTimes(2);
    expect(await readFile(result.zipPath, "utf8")).toBe("zip-data");
    const metadata = await readColoradoTracerContributionArtifactCacheMetadata(result.metadataPath);
    expect(metadata).toMatchObject({
      version: 1,
      year: 2024,
      downloadedAt: "2026-06-19T12:00:00.000Z",
      bytesWritten: 8,
      remote: {
        year: 2024,
        etag: "\"co-2024-a\"",
        lastModified: "Fri, 19 Jun 2026 09:00:49 GMT",
      },
    });
  });

  it("skips download when cached metadata still matches", async () => {
    const cacheDir = await makeTempDir();
    const paths = getColoradoTracerContributionArtifactCachePaths({ cacheDir, year: 2024 });
    await writeFile(paths.zipPath, "cached-zip", "utf8");
    await writeFile(
      paths.metadataPath,
      `${JSON.stringify(
        {
          version: 1,
          year: 2024,
          zipPath: paths.zipPath,
          metadataPath: paths.metadataPath,
          downloadedAt: "2026-06-18T12:00:00.000Z",
          bytesWritten: 10,
          remote: {
            year: 2024,
            url: buildColoradoTracerContributionZipUrl({ year: 2024 }),
            contentLength: 10,
            contentType: "application/zip",
            etag: "\"same\"",
            lastModified: "Fri, 19 Jun 2026 09:00:49 GMT",
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
        "last-modified": "Fri, 19 Jun 2026 09:00:49 GMT",
      })
    );

    const result = await refreshColoradoTracerContributionArtifactCache({
      year: 2024,
      cacheDir,
      fetchImpl,
    });

    expect(result.status).toBe("unchanged");
    expect(fetchImpl).toHaveBeenCalledTimes(1);
    expect(await readFile(paths.zipPath, "utf8")).toBe("cached-zip");
  });

  it("redownloads matching metadata when force is true", async () => {
    const cacheDir = await makeTempDir();
    const paths = getColoradoTracerContributionArtifactCachePaths({ cacheDir, year: 2024 });
    await writeFile(paths.zipPath, "old", "utf8");
    await writeFile(
      paths.metadataPath,
      `${JSON.stringify(
        {
          version: 1,
          year: 2024,
          zipPath: paths.zipPath,
          metadataPath: paths.metadataPath,
          downloadedAt: "2026-06-18T12:00:00.000Z",
          bytesWritten: 3,
          remote: {
            year: 2024,
            url: buildColoradoTracerContributionZipUrl({ year: 2024 }),
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

    const result = await refreshColoradoTracerContributionArtifactCache({
      year: 2024,
      cacheDir,
      fetchImpl,
      force: true,
    });

    expect(result.status).toBe("downloaded");
    expect(fetchImpl).toHaveBeenCalledTimes(2);
    expect(await readFile(paths.zipPath, "utf8")).toBe("new-zip");
  });
});
