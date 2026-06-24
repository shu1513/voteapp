import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";

import {
  buildMichiganMitnLegacyArchiveUrl,
  fetchMichiganMitnLegacyArchiveMetadata,
  getMichiganMitnLegacyArchiveCachePaths,
  type MichiganMitnLegacyArchiveExtractor,
  parseMichiganMitnHttpsUrl,
  readMichiganMitnLegacyArchiveCacheMetadata,
  refreshMichiganMitnLegacyArchiveCache,
} from "../../../src/pipeline/michiganFinance/michiganMitnLegacyArtifactCache.js";

const tempDirs: string[] = [];

async function makeTempDir(): Promise<string> {
  const dir = await mkdtemp(join(tmpdir(), "voteapp-mi-mitn-cache-"));
  tempDirs.push(dir);
  return dir;
}

function response(body: string | null, headers: Record<string, string> = {}): Response {
  return new Response(body, {
    status: 200,
    headers,
  });
}

function fakeExtractor(): MichiganMitnLegacyArchiveExtractor {
  return async ({ extractedDir }) => {
    await mkdir(extractedDir, { recursive: true });
    await writeFile(join(extractedDir, "extracted.txt"), "ok", "utf8");
  };
}

afterEach(async () => {
  vi.restoreAllMocks();
  await Promise.all(tempDirs.splice(0).map((dir) => rm(dir, { recursive: true, force: true })));
});

describe("Michigan MiTN legacy archive artifact cache", () => {
  it("builds the official yearly legacy archive URL", () => {
    expect(buildMichiganMitnLegacyArchiveUrl({ year: 2022 })).toBe(
      "https://www.michigan.gov/sos/-/media/Project/Websites/sos/Elections/Disclosure/MiTN/Legacy-Data/2022_mi_cfr.7z"
    );
  });

  it("rejects non-HTTPS URLs", () => {
    expect(() => parseMichiganMitnHttpsUrl("http://example.com/2022_mi_cfr.7z", "--url")).toThrow(
      "Only https is allowed"
    );
    expect(() =>
      buildMichiganMitnLegacyArchiveUrl({ year: 2022, baseUrl: "http://example.com" })
    ).toThrow("Only https is allowed");
  });

  it("rejects unsupported years", () => {
    expect(() => buildMichiganMitnLegacyArchiveUrl({ year: 2019 })).toThrow(
      "Invalid Michigan MiTN legacy archive year"
    );
    expect(() => buildMichiganMitnLegacyArchiveUrl({ year: 2022.5 })).toThrow(
      "Invalid Michigan MiTN legacy archive year"
    );
  });

  it("builds stable cache paths", async () => {
    const cacheDir = await makeTempDir();
    const paths = getMichiganMitnLegacyArchiveCachePaths({ cacheDir, year: 2022 });

    expect(paths.cacheDir).toBe(cacheDir);
    expect(paths.archivePath).toBe(join(cacheDir, "2022_mi_cfr.7z"));
    expect(paths.extractedDir).toBe(join(cacheDir, "2022_mi_cfr"));
    expect(paths.metadataPath).toBe(join(cacheDir, "2022_mi_cfr.metadata.json"));
  });

  it("rejects invalid refresh timestamps before remote fetches", async () => {
    const cacheDir = await makeTempDir();
    const fetchImpl = vi.fn<typeof fetch>();

    await expect(
      refreshMichiganMitnLegacyArchiveCache({
        year: 2022,
        cacheDir,
        fetchImpl,
        now: new Date("not a date"),
      })
    ).rejects.toThrow("Invalid Michigan MiTN legacy archive refresh timestamp");

    expect(fetchImpl).not.toHaveBeenCalled();
  });

  it("downloads and writes cache metadata when the archive is missing", async () => {
    const cacheDir = await makeTempDir();
    const extractArchive = vi.fn(fakeExtractor());
    const fetchImpl = vi.fn<typeof fetch>(async (_url, init) => {
      if (init?.method === "HEAD") {
        return response(null, {
          "content-length": "8",
          "content-type": "application/x-7z-compressed",
          etag: "\"mi-2022-a\"",
          "last-modified": "Fri, 19 Jun 2026 09:00:49 GMT",
        });
      }
      return response("7z-bytes", {
        "content-length": "8",
        "content-type": "application/x-7z-compressed",
        etag: "\"mi-2022-a\"",
        "last-modified": "Fri, 19 Jun 2026 09:00:49 GMT",
      });
    });

    const result = await refreshMichiganMitnLegacyArchiveCache({
      year: 2022,
      cacheDir,
      fetchImpl,
      extractArchive,
      now: new Date("2026-06-19T12:00:00.000Z"),
    });

    expect(result.status).toBe("downloaded");
    expect(fetchImpl).toHaveBeenCalledTimes(2);
    expect(extractArchive).toHaveBeenCalledWith(
      expect.objectContaining({
        archivePath: expect.stringContaining(`${result.archivePath}.tmp-`),
        extractedDir: expect.stringContaining(`${result.extractedDir}.tmp-`),
        year: 2022,
      })
    );
    expect(await readFile(result.archivePath, "utf8")).toBe("7z-bytes");
    await expect(readFile(join(result.extractedDir, "extracted.txt"), "utf8")).resolves.toBe("ok");
    const metadata = await readMichiganMitnLegacyArchiveCacheMetadata(result.metadataPath);
    expect(metadata).toMatchObject({
      version: 1,
      year: 2022,
      downloadedAt: "2026-06-19T12:00:00.000Z",
      bytesWritten: 8,
      extractedDir: result.extractedDir,
      remote: {
        year: 2022,
        etag: "\"mi-2022-a\"",
        lastModified: "Fri, 19 Jun 2026 09:00:49 GMT",
      },
    });
  });

  it("serializes concurrent refreshes for the same year and cache directory", async () => {
    const cacheDir = await makeTempDir();
    let headCalls = 0;
    let getCalls = 0;
    const extractArchive = vi.fn(fakeExtractor());
    const fetchImpl = vi.fn<typeof fetch>(async (_url, init) => {
      if (init?.method === "HEAD") {
        headCalls += 1;
        return response(null, {
          "content-length": "8",
          "content-type": "application/x-7z-compressed",
          etag: "\"mi-2022-concurrent\"",
          "last-modified": "Fri, 19 Jun 2026 09:00:49 GMT",
        });
      }
      getCalls += 1;
      return response("7z-bytes", {
        "content-length": "8",
        "content-type": "application/x-7z-compressed",
        etag: "\"mi-2022-concurrent\"",
        "last-modified": "Fri, 19 Jun 2026 09:00:49 GMT",
      });
    });

    const [first, second] = await Promise.all([
      refreshMichiganMitnLegacyArchiveCache({
        year: 2022,
        cacheDir,
        fetchImpl,
        extractArchive,
        now: new Date("2026-06-19T12:00:00.000Z"),
      }),
      refreshMichiganMitnLegacyArchiveCache({
        year: 2022,
        cacheDir,
        fetchImpl,
        extractArchive,
        now: new Date("2026-06-19T12:00:00.000Z"),
      }),
    ]);

    expect(first.status).toBe("downloaded");
    expect(second.status).toBe("unchanged");
    expect(headCalls).toBe(2);
    expect(getCalls).toBe(1);
    expect(extractArchive).toHaveBeenCalledTimes(1);
    await expect(readFile(join(first.extractedDir, "extracted.txt"), "utf8")).resolves.toBe("ok");
  });

  it("discovers the current archive URL from the public page when the bare archive URL is rejected", async () => {
    const discoveredUrl =
      "https://www.michigan.gov/sos/-/media/Project/Websites/sos/Elections/Disclosure/MiTN/Legacy-Data/2022_mi_cfr.7z?rev=abc&amp;hash=def";
    const fetchImpl = vi.fn<typeof fetch>(async (url, init) => {
      if (init?.method === "HEAD" && String(url) === buildMichiganMitnLegacyArchiveUrl({ year: 2022 })) {
        return new Response(null, { status: 403, statusText: "Forbidden" });
      }
      if (init?.method === "GET") {
        return response(`<a href="${discoveredUrl}">2022_mi_cfr.7z</a>`, {
          "content-type": "text/html",
        });
      }
      return response(null, {
        "content-length": "8",
        "content-type": "application/octet-stream",
        etag: "\"mi-2022-discovered\"",
      });
    });

    await expect(
      fetchMichiganMitnLegacyArchiveMetadata({
        year: 2022,
        fetchImpl,
      })
    ).resolves.toMatchObject({
      year: 2022,
      url:
        "https://www.michigan.gov/sos/-/media/Project/Websites/sos/Elections/Disclosure/MiTN/Legacy-Data/2022_mi_cfr.7z?rev=abc&hash=def",
      etag: "\"mi-2022-discovered\"",
    });
    expect(fetchImpl).toHaveBeenCalledTimes(3);
  });

  it("rejects downloads whose byte count does not match content-length", async () => {
    const cacheDir = await makeTempDir();
    const paths = getMichiganMitnLegacyArchiveCachePaths({ cacheDir, year: 2022 });
    const fetchImpl = vi.fn<typeof fetch>(async (_url, init) => {
      if (init?.method === "HEAD") {
        return response(null, {
          "content-length": "100",
          "content-type": "application/x-7z-compressed",
          etag: "\"mi-2022-short\"",
        });
      }
      return response("short", {
        "content-length": "100",
        "content-type": "application/x-7z-compressed",
        etag: "\"mi-2022-short\"",
      });
    });

    await expect(
      refreshMichiganMitnLegacyArchiveCache({
        year: 2022,
        cacheDir,
        fetchImpl,
        now: new Date("2026-06-19T12:00:00.000Z"),
      })
    ).rejects.toThrow("Michigan MiTN legacy archive download size mismatch");

    await expect(readFile(paths.archivePath, "utf8")).rejects.toThrow();
  });

  it("skips download when cached metadata still matches", async () => {
    const cacheDir = await makeTempDir();
    const paths = getMichiganMitnLegacyArchiveCachePaths({ cacheDir, year: 2022 });
    await writeFile(paths.archivePath, "cached-archive", "utf8");
    await mkdir(paths.extractedDir, { recursive: true });
    await writeFile(
      paths.metadataPath,
      `${JSON.stringify(
        {
          version: 1,
            year: 2022,
            archivePath: paths.archivePath,
            extractedDir: paths.extractedDir,
            metadataPath: paths.metadataPath,
          downloadedAt: "2026-06-18T12:00:00.000Z",
          bytesWritten: 14,
          remote: {
            year: 2022,
            url: buildMichiganMitnLegacyArchiveUrl({ year: 2022 }),
            contentLength: 14,
            contentType: "application/x-7z-compressed",
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
        "content-length": "14",
        "content-type": "application/x-7z-compressed",
        etag: "\"same\"",
        "last-modified": "Fri, 19 Jun 2026 09:00:49 GMT",
      })
    );

    const result = await refreshMichiganMitnLegacyArchiveCache({
      year: 2022,
      cacheDir,
      fetchImpl,
    });

    expect(result.status).toBe("unchanged");
    expect(fetchImpl).toHaveBeenCalledTimes(1);
    expect(await readFile(paths.archivePath, "utf8")).toBe("cached-archive");
  });

  it("extracts a matching cached archive when the extracted directory is missing", async () => {
    const cacheDir = await makeTempDir();
    const paths = getMichiganMitnLegacyArchiveCachePaths({ cacheDir, year: 2022 });
    await writeFile(paths.archivePath, "cached-archive", "utf8");
    await writeFile(
      paths.metadataPath,
      `${JSON.stringify(
        {
          version: 1,
          year: 2022,
          archivePath: paths.archivePath,
          extractedDir: paths.extractedDir,
          metadataPath: paths.metadataPath,
          downloadedAt: "2026-06-18T12:00:00.000Z",
          bytesWritten: 14,
          remote: {
            year: 2022,
            url: buildMichiganMitnLegacyArchiveUrl({ year: 2022 }),
            contentLength: 14,
            contentType: "application/x-7z-compressed",
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
        "content-length": "14",
        "content-type": "application/x-7z-compressed",
        etag: "\"same\"",
        "last-modified": "Fri, 19 Jun 2026 09:00:49 GMT",
      })
    );
    const extractArchive = vi.fn(fakeExtractor());

    const result = await refreshMichiganMitnLegacyArchiveCache({
      year: 2022,
      cacheDir,
      fetchImpl,
      extractArchive,
    });

    expect(result.status).toBe("extracted");
    expect(fetchImpl).toHaveBeenCalledTimes(1);
    expect(extractArchive).toHaveBeenCalledTimes(1);
    await expect(readFile(join(result.extractedDir, "extracted.txt"), "utf8")).resolves.toBe("ok");
  });

  it("warns and returns null when cache metadata cannot be parsed", async () => {
    const cacheDir = await makeTempDir();
    const paths = getMichiganMitnLegacyArchiveCachePaths({ cacheDir, year: 2022 });
    await writeFile(paths.metadataPath, "{not-json", "utf8");
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});

    await expect(readMichiganMitnLegacyArchiveCacheMetadata(paths.metadataPath)).resolves.toBeNull();
    expect(warn).toHaveBeenCalledWith(
      expect.stringContaining("Unexpected error reading Michigan MiTN legacy archive cache metadata"),
      expect.any(Error)
    );
  });

  it("redownloads matching metadata when force is true", async () => {
    const cacheDir = await makeTempDir();
    const paths = getMichiganMitnLegacyArchiveCachePaths({ cacheDir, year: 2022 });
    await writeFile(paths.archivePath, "old", "utf8");
    await mkdir(paths.extractedDir, { recursive: true });
    await writeFile(
      paths.metadataPath,
      `${JSON.stringify(
        {
          version: 1,
            year: 2022,
            archivePath: paths.archivePath,
            extractedDir: paths.extractedDir,
            metadataPath: paths.metadataPath,
          downloadedAt: "2026-06-18T12:00:00.000Z",
          bytesWritten: 3,
          remote: {
            year: 2022,
            url: buildMichiganMitnLegacyArchiveUrl({ year: 2022 }),
            contentLength: 3,
            contentType: "application/x-7z-compressed",
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
          "content-length": "9",
          etag: "\"same\"",
        });
      }
      return response("new-7z-ok", {
        "content-length": "9",
        etag: "\"same\"",
      });
    });
    const extractArchive = vi.fn(fakeExtractor());

    const result = await refreshMichiganMitnLegacyArchiveCache({
      year: 2022,
      cacheDir,
      fetchImpl,
      extractArchive,
      force: true,
    });

    expect(result.status).toBe("downloaded");
    expect(fetchImpl).toHaveBeenCalledTimes(2);
    expect(extractArchive).toHaveBeenCalledTimes(1);
    expect(await readFile(paths.archivePath, "utf8")).toBe("new-7z-ok");
  });
});
