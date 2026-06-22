import { mkdtemp, readdir, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";

import {
  buildNewMexicoCfisArtifactUrl,
  buildNewMexicoCfisDataCatalogUrl,
  getNewMexicoCfisArtifactCachePaths,
  newMexicoCfisTransactionType,
  parseNewMexicoCfisHttpsUrl,
  readNewMexicoCfisArtifactCacheMetadata,
  refreshNewMexicoCfisArtifactCache,
} from "../../../src/pipeline/newMexicoFinance/newMexicoCfisArtifactCache.js";

const tempDirs: string[] = [];

async function makeTempDir(): Promise<string> {
  const dir = await mkdtemp(join(tmpdir(), "voteapp-nm-cfis-cache-"));
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

describe("New Mexico CFIS artifact cache", () => {
  it("builds official yearly contribution and expenditure CSV URLs", () => {
    expect(buildNewMexicoCfisArtifactUrl({ year: 2026, artifactKind: "contributions" })).toBe(
      "https://login.cfis.sos.state.nm.us/api/DataDownload/GetCSVDownloadReport?year=2026&transactionType=CON&reportFormat=csv&fileName=CON_2026.csv"
    );
    expect(buildNewMexicoCfisArtifactUrl({ year: 2026, artifactKind: "expenditures" })).toBe(
      "https://login.cfis.sos.state.nm.us/api/DataDownload/GetCSVDownloadReport?year=2026&transactionType=EXP&reportFormat=csv&fileName=EXP_2026.csv"
    );
    expect(newMexicoCfisTransactionType("contributions")).toBe("CON");
    expect(newMexicoCfisTransactionType("expenditures")).toBe("EXP");
  });

  it("builds the data catalog URL used to discover available CFIS files", () => {
    expect(buildNewMexicoCfisDataCatalogUrl()).toBe(
      "https://login.cfis.sos.state.nm.us/api/DataDownload/GetCheckDatadownload?pageNumber=1&pageSize=100&sortDir=asc&sortedBy=Year"
    );
    expect(buildNewMexicoCfisDataCatalogUrl({ pageNumber: 2, pageSize: 50, sortDir: "desc", sortedBy: "FileName" })).toBe(
      "https://login.cfis.sos.state.nm.us/api/DataDownload/GetCheckDatadownload?pageNumber=2&pageSize=50&sortDir=desc&sortedBy=FileName"
    );
  });

  it("rejects invalid URLs, invalid years, invalid catalog paging, and invalid artifact kinds", () => {
    expect(() => parseNewMexicoCfisHttpsUrl("http://example.com/file.csv", "--url")).toThrow(
      "Only https is allowed"
    );
    expect(() =>
      buildNewMexicoCfisArtifactUrl({
        year: 2026,
        artifactKind: "contributions",
        baseUrl: "http://example.com/download",
      })
    ).toThrow("Only https is allowed");
    expect(() => buildNewMexicoCfisArtifactUrl({ year: 2019, artifactKind: "contributions" })).toThrow(
      "Invalid New Mexico CFIS artifact year"
    );
    expect(() =>
      buildNewMexicoCfisArtifactUrl({ year: 2026, artifactKind: "bad" as never })
    ).toThrow("Invalid New Mexico CFIS artifact kind");
    expect(() => buildNewMexicoCfisDataCatalogUrl({ pageNumber: 0 })).toThrow(
      "Invalid New Mexico CFIS catalog pageNumber"
    );
    expect(() => buildNewMexicoCfisDataCatalogUrl({ pageSize: 0 })).toThrow(
      "Invalid New Mexico CFIS catalog pageSize"
    );
  });

  it("rejects invalid refresh timestamps before remote fetches", async () => {
    const cacheDir = await makeTempDir();
    const fetchImpl = vi.fn<typeof fetch>();

    await expect(
      refreshNewMexicoCfisArtifactCache({
        year: 2026,
        artifactKind: "contributions",
        cacheDir,
        fetchImpl,
        now: new Date("not a date"),
      })
    ).rejects.toThrow("Invalid New Mexico CFIS artifact refresh timestamp");

    expect(fetchImpl).not.toHaveBeenCalled();
  });

  it("downloads and writes cache metadata when the artifact is missing", async () => {
    const cacheDir = await makeTempDir();
    const fetchImpl = vi.fn<typeof fetch>(async (_url, init) => {
      if (init?.method === "HEAD") {
        return response(null, {
          "content-length": "24",
          "content-type": "text/csv",
          etag: '"nm-2026-con-a"',
          "last-modified": "Sat, 20 Jun 2026 19:13:00 GMT",
        });
      }
      return response("OrgID,Amount\n1,23.00\n", {
        "content-length": "21",
        "content-type": "text/csv",
        etag: '"nm-2026-con-a"',
        "last-modified": "Sat, 20 Jun 2026 19:13:00 GMT",
      });
    });

    const result = await refreshNewMexicoCfisArtifactCache({
      year: 2026,
      artifactKind: "contributions",
      cacheDir,
      fetchImpl,
      now: new Date("2026-06-20T20:00:00.000Z"),
    });

    expect(result.status).toBe("downloaded");
    expect(fetchImpl).toHaveBeenCalledTimes(2);
    expect(await readFile(result.filePath, "utf8")).toBe("OrgID,Amount\n1,23.00\n");
    const metadata = await readNewMexicoCfisArtifactCacheMetadata(result.metadataPath);
    expect(metadata).toMatchObject({
      version: 1,
      artifact: {
        year: 2026,
        artifactKind: "contributions",
      },
      downloadedAt: "2026-06-20T20:00:00.000Z",
      bytesWritten: 21,
      remote: {
        year: 2026,
        artifactKind: "contributions",
        etag: '"nm-2026-con-a"',
        lastModified: "Sat, 20 Jun 2026 19:13:00 GMT",
      },
    });
  });

  it("falls back to GET metadata when CFIS rejects HEAD", async () => {
    const fetchImpl = vi.fn<typeof fetch>(async (_url, init) => {
      if (init?.method === "HEAD") {
        return new Response(null, { status: 405, statusText: "Method Not Allowed" });
      }
      return response("OrgID,Amount\n", {
        "content-length": "13",
        "content-type": "text/csv",
        "last-modified": "Sat, 20 Jun 2026 19:13:00 GMT",
      });
    });

    const metadata = await refreshNewMexicoCfisArtifactCache({
      year: 2026,
      artifactKind: "contributions",
      cacheDir: await makeTempDir(),
      fetchImpl,
      now: new Date("2026-06-20T20:00:00.000Z"),
    });

    expect(metadata.status).toBe("downloaded");
    expect(fetchImpl).toHaveBeenCalledTimes(3);
    expect(fetchImpl.mock.calls[0]?.[1]).toMatchObject({ method: "HEAD" });
    expect(fetchImpl.mock.calls[1]?.[1]).toMatchObject({ method: "GET" });
    expect(fetchImpl.mock.calls[2]?.[1]).toMatchObject({ method: "GET" });
    expect(metadata.remote).toMatchObject({
      year: 2026,
      artifactKind: "contributions",
      contentLength: 13,
      contentType: "text/csv",
    });
  });

  it("skips download when cached metadata still matches", async () => {
    const cacheDir = await makeTempDir();
    const paths = getNewMexicoCfisArtifactCachePaths({ cacheDir, year: 2026, artifactKind: "expenditures" });
    await writeFile(paths.filePath, "cached-csv", "utf8");
    await writeFile(
      paths.metadataPath,
      `${JSON.stringify(
        {
          version: 1,
          artifact: {
            year: 2026,
            artifactKind: "expenditures",
          },
          filePath: paths.filePath,
          metadataPath: paths.metadataPath,
          downloadedAt: "2026-06-19T12:00:00.000Z",
          bytesWritten: 10,
          remote: {
            year: 2026,
            artifactKind: "expenditures",
            url: buildNewMexicoCfisArtifactUrl({ year: 2026, artifactKind: "expenditures" }),
            contentLength: 10,
            contentType: "text/csv",
            etag: '"same"',
            lastModified: "Sat, 20 Jun 2026 19:13:00 GMT",
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
        "content-type": "text/csv",
        etag: '"same"',
        "last-modified": "Sat, 20 Jun 2026 19:13:00 GMT",
      })
    );

    const result = await refreshNewMexicoCfisArtifactCache({
      year: 2026,
      artifactKind: "expenditures",
      cacheDir,
      fetchImpl,
    });

    expect(result.status).toBe("unchanged");
    expect(fetchImpl).toHaveBeenCalledTimes(1);
    expect(await readFile(paths.filePath, "utf8")).toBe("cached-csv");
  });

  it("warns and returns null when cache metadata cannot be parsed", async () => {
    const cacheDir = await makeTempDir();
    const paths = getNewMexicoCfisArtifactCachePaths({ cacheDir, year: 2026, artifactKind: "contributions" });
    await writeFile(paths.metadataPath, "{not-json", "utf8");
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});

    await expect(readNewMexicoCfisArtifactCacheMetadata(paths.metadataPath)).resolves.toBeNull();
    expect(warn).toHaveBeenCalledWith(
      expect.stringContaining("Unexpected error reading New Mexico CFIS cache metadata"),
      expect.any(Error)
    );
  });

  it("redownloads matching metadata when force is true", async () => {
    const cacheDir = await makeTempDir();
    const paths = getNewMexicoCfisArtifactCachePaths({ cacheDir, year: 2026, artifactKind: "contributions" });
    await writeFile(paths.filePath, "old", "utf8");
    await writeFile(
      paths.metadataPath,
      `${JSON.stringify(
        {
          version: 1,
          artifact: {
            year: 2026,
            artifactKind: "contributions",
          },
          filePath: paths.filePath,
          metadataPath: paths.metadataPath,
          downloadedAt: "2026-06-19T12:00:00.000Z",
          bytesWritten: 3,
          remote: {
            year: 2026,
            artifactKind: "contributions",
            url: buildNewMexicoCfisArtifactUrl({ year: 2026, artifactKind: "contributions" }),
            contentLength: 3,
            contentType: "text/csv",
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
      return response("new-csv", {
        "content-length": "7",
        etag: '"same"',
      });
    });

    const result = await refreshNewMexicoCfisArtifactCache({
      year: 2026,
      artifactKind: "contributions",
      cacheDir,
      fetchImpl,
      force: true,
    });

    expect(result.status).toBe("downloaded");
    expect(fetchImpl).toHaveBeenCalledTimes(2);
    expect(await readFile(paths.filePath, "utf8")).toBe("new-csv");
  });

  it("cleans up temporary CSV files when download fails", async () => {
    const cacheDir = await makeTempDir();
    const fetchImpl = vi.fn<typeof fetch>(async (_url, init) => {
      if (init?.method === "HEAD") {
        return response(null, {
          "content-length": "8",
          "content-type": "text/csv",
          etag: '"nm-2026-a"',
        });
      }
      return new Response(null, { status: 500, statusText: "Server Error" });
    });

    await expect(
      refreshNewMexicoCfisArtifactCache({
        year: 2026,
        artifactKind: "contributions",
        cacheDir,
        fetchImpl,
      })
    ).rejects.toThrow("Failed to download New Mexico CFIS artifact: 500 Server Error");

    const entries = await readdir(cacheDir).catch(() => []);
    expect(entries.filter((entry) => entry.includes(".tmp-"))).toEqual([]);
  });
});
