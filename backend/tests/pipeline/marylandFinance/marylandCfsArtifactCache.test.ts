import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";

import {
  getMarylandCfsArtifactCachePaths,
  readMarylandCfsArtifactCacheMetadata,
  refreshMarylandCfsArtifactCache,
} from "../../../src/pipeline/marylandFinance/marylandCfsArtifactCache.js";
import {
  MARYLAND_CFS_PUBLIC_EXPORT_API_URL,
  buildMarylandCfsPublicExportRequestBody,
  marylandCfsTransactionTypeCode,
  parseMarylandCfsHttpsUrl,
} from "../../../src/pipeline/marylandFinance/marylandCfsClient.js";

const tempDirs: string[] = [];

async function makeTempDir(): Promise<string> {
  const dir = await mkdtemp(join(tmpdir(), "voteapp-md-cfs-cache-"));
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

describe("Maryland CFS artifact cache", () => {
  it("maps artifact kinds to official public-export transaction type codes", () => {
    expect(marylandCfsTransactionTypeCode("contributions")).toBe("TCON");
    expect(marylandCfsTransactionTypeCode("expenditures")).toBe("TEXP");
    expect(marylandCfsTransactionTypeCode("committees")).toBe("TCMD");
    expect(buildMarylandCfsPublicExportRequestBody({ filingYear: 2026, artifactKind: "committees" })).toEqual({
      Type: "CSV",
      TransactionTypeCode: "TCMD",
      FilingYear: 2026,
    });
  });

  it("rejects invalid URLs, years, and artifact kinds", () => {
    expect(() => parseMarylandCfsHttpsUrl("http://example.com/file.csv", "--url")).toThrow(
      "Only https is allowed"
    );
    expect(() => buildMarylandCfsPublicExportRequestBody({ filingYear: 1999, artifactKind: "committees" })).toThrow(
      "Invalid Maryland CFS filing year"
    );
    expect(() =>
      buildMarylandCfsPublicExportRequestBody({ filingYear: 2026, artifactKind: "bad" as never })
    ).toThrow("Invalid Maryland CFS artifact kind");
  });

  it("builds stable cache paths", async () => {
    const cacheDir = await makeTempDir();

    expect(getMarylandCfsArtifactCachePaths({ cacheDir, filingYear: 2026, artifactKind: "contributions" })).toEqual({
      cacheDir,
      filePath: join(cacheDir, "TCON_2026.csv"),
      metadataPath: join(cacheDir, "TCON_2026.metadata.json"),
    });
    expect(getMarylandCfsArtifactCachePaths({ cacheDir, filingYear: 2026, artifactKind: "expenditures" }).filePath).toBe(
      join(cacheDir, "TEXP_2026.csv")
    );
    expect(getMarylandCfsArtifactCachePaths({ cacheDir, filingYear: 2026, artifactKind: "committees" }).filePath).toBe(
      join(cacheDir, "TCMD_2026.csv")
    );
  });

  it("rejects invalid refresh timestamps before remote fetches", async () => {
    const cacheDir = await makeTempDir();
    const fetchImpl = vi.fn<typeof fetch>();

    await expect(
      refreshMarylandCfsArtifactCache({
        filingYear: 2026,
        artifactKind: "contributions",
        cacheDir,
        fetchImpl,
        now: new Date("not a date"),
      })
    ).rejects.toThrow("Invalid Maryland CFS artifact refresh timestamp");

    expect(fetchImpl).not.toHaveBeenCalled();
  });

  it("downloads and writes cache metadata when the artifact is missing", async () => {
    const cacheDir = await makeTempDir();
    const fetchImpl = vi.fn<typeof fetch>(async (_url, init) => {
      expect(init?.method).toBe("POST");
      expect(init?.body).toBe(JSON.stringify({ Type: "CSV", TransactionTypeCode: "TCON", FilingYear: 2026 }));
      if (fetchImpl.mock.calls.length === 1) {
        return response("metadata-probe", {
          "content-length": "24",
          "content-type": "text/csv",
          etag: '"md-2026-con-a"',
          "last-modified": "Thu, 25 Jun 2026 08:03:00 GMT",
        });
      }
      return response("Filing Entity Id\n5007501\n", {
        "content-length": "25",
        "content-type": "text/csv",
        etag: '"md-2026-con-a"',
        "last-modified": "Thu, 25 Jun 2026 08:03:00 GMT",
      });
    });

    const result = await refreshMarylandCfsArtifactCache({
      filingYear: 2026,
      artifactKind: "contributions",
      cacheDir,
      fetchImpl,
      now: new Date("2026-06-25T12:00:00.000Z"),
    });

    expect(result.status).toBe("downloaded");
    expect(fetchImpl).toHaveBeenCalledTimes(2);
    expect(await readFile(result.filePath, "utf8")).toBe("Filing Entity Id\n5007501\n");
    const metadata = await readMarylandCfsArtifactCacheMetadata(result.metadataPath);
    expect(metadata).toMatchObject({
      version: 1,
      artifact: {
        filingYear: 2026,
        artifactKind: "contributions",
      },
      downloadedAt: "2026-06-25T12:00:00.000Z",
      bytesWritten: 25,
      remote: {
        filingYear: 2026,
        artifactKind: "contributions",
        url: MARYLAND_CFS_PUBLIC_EXPORT_API_URL,
        requestBody: {
          Type: "CSV",
          TransactionTypeCode: "TCON",
          FilingYear: 2026,
        },
        etag: '"md-2026-con-a"',
        lastModified: "Thu, 25 Jun 2026 08:03:00 GMT",
      },
    });
  });

  it("rejects downloads whose byte count does not match content-length", async () => {
    const cacheDir = await makeTempDir();
    const paths = getMarylandCfsArtifactCachePaths({ cacheDir, filingYear: 2026, artifactKind: "committees" });
    const fetchImpl = vi.fn<typeof fetch>(async (_url, init) => {
      if (fetchImpl.mock.calls.length === 1) {
        return response("metadata-probe", { "content-length": "100" });
      }
      return response("short", { "content-length": "100" });
    });

    await expect(
      refreshMarylandCfsArtifactCache({
        filingYear: 2026,
        artifactKind: "committees",
        cacheDir,
        fetchImpl,
        now: new Date("2026-06-25T12:00:00.000Z"),
      })
    ).rejects.toThrow("Maryland CFS artifact download size mismatch");

    await expect(readFile(paths.filePath, "utf8")).rejects.toThrow();
  });

  it("skips download when cached metadata still matches", async () => {
    const cacheDir = await makeTempDir();
    const paths = getMarylandCfsArtifactCachePaths({ cacheDir, filingYear: 2026, artifactKind: "expenditures" });
    await writeFile(paths.filePath, "cached-csv", "utf8");
    await writeFile(
      paths.metadataPath,
      `${JSON.stringify(
        {
          version: 1,
          artifact: {
            filingYear: 2026,
            artifactKind: "expenditures",
          },
          filePath: paths.filePath,
          metadataPath: paths.metadataPath,
          downloadedAt: "2026-06-24T12:00:00.000Z",
          bytesWritten: 10,
          remote: {
            filingYear: 2026,
            artifactKind: "expenditures",
            url: MARYLAND_CFS_PUBLIC_EXPORT_API_URL,
            requestBody: {
              Type: "CSV",
              TransactionTypeCode: "TEXP",
              FilingYear: 2026,
            },
            contentLength: 10,
            contentType: "text/csv",
            etag: '"same"',
            lastModified: "Thu, 25 Jun 2026 08:03:00 GMT",
          },
        },
        null,
        2
      )}\n`,
      "utf8"
    );

    const fetchImpl = vi.fn<typeof fetch>(async () =>
      response("metadata-probe", {
        "content-length": "10",
        "content-type": "text/csv",
        etag: '"same"',
        "last-modified": "Thu, 25 Jun 2026 08:03:00 GMT",
      })
    );

    const result = await refreshMarylandCfsArtifactCache({
      filingYear: 2026,
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
    const paths = getMarylandCfsArtifactCachePaths({ cacheDir, filingYear: 2026, artifactKind: "contributions" });
    await writeFile(paths.metadataPath, "{not-json", "utf8");
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});

    await expect(readMarylandCfsArtifactCacheMetadata(paths.metadataPath)).resolves.toBeNull();
    expect(warn).toHaveBeenCalledWith(
      expect.stringContaining("Unexpected error reading Maryland CFS cache metadata"),
      expect.any(Error)
    );
  });
});
