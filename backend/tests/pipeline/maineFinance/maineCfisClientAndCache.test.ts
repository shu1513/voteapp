import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";

import {
  getMaineCfisArtifactCachePaths,
  readMaineCfisArtifactCacheMetadata,
  refreshMaineCfisArtifactCache,
} from "../../../src/pipeline/maineFinance/maineCfisArtifactCache.js";
import {
  MAINE_CFIS_CSV_DOWNLOAD_API_URL,
  buildMaineCfisCsvDownloadRequestBody,
  fetchMaineCfisDownloadList,
  maineCfisTransactionType,
  parseMaineCfisHttpsUrl,
} from "../../../src/pipeline/maineFinance/maineCfisClient.js";

const tempDirs: string[] = [];

async function makeTempDir(): Promise<string> {
  const dir = await mkdtemp(join(tmpdir(), "voteapp-me-cfis-cache-"));
  tempDirs.push(dir);
  return dir;
}

function response(body: BodyInit | null, headers: Record<string, string> = {}): Response {
  return new Response(body, {
    status: 200,
    headers,
  });
}

afterEach(async () => {
  vi.restoreAllMocks();
  await Promise.all(tempDirs.splice(0).map((dir) => rm(dir, { recursive: true, force: true })));
});

describe("Maine CFIS client and artifact cache", () => {
  it("maps artifact kinds to Maine transaction types and request bodies", () => {
    expect(maineCfisTransactionType("contributions")).toBe("CON");
    expect(maineCfisTransactionType("expenditures")).toBe("EXP");
    expect(buildMaineCfisCsvDownloadRequestBody({ filingYear: 2026, artifactKind: "contributions" })).toEqual({
      year: 2026,
      transactionType: "CON",
    });
  });

  it("rejects invalid URLs, years, and artifact kinds", () => {
    expect(() => parseMaineCfisHttpsUrl("http://example.com/file.csv", "--url")).toThrow("Only https is allowed");
    expect(() => parseMaineCfisHttpsUrl("https://example.com/file.csv", "--url")).toThrow("Invalid --url host");
    expect(() => buildMaineCfisCsvDownloadRequestBody({ filingYear: 1999, artifactKind: "contributions" })).toThrow(
      "Invalid Maine CFIS filing year"
    );
    expect(() =>
      buildMaineCfisCsvDownloadRequestBody({ filingYear: 2026, artifactKind: "bad" as never })
    ).toThrow("Invalid Maine CFIS artifact kind");
  });

  it("fetches and filters the public download list", async () => {
    const fetchImpl = vi.fn<typeof fetch>(async () =>
      response(
        JSON.stringify([
          {
            TransactionKey: "Contributions and Loans",
            ElectionYear: 2026,
            NameOfFile: "CON_2026.csv",
            TransactionType: "CON",
          },
          {
            TransactionKey: "Bad",
            ElectionYear: 2026,
            NameOfFile: "BAD_2026.csv",
            TransactionType: "BAD",
          },
        ]),
        { "content-type": "application/json" }
      )
    );

    await expect(fetchMaineCfisDownloadList({ fetchImpl })).resolves.toEqual([
      {
        TransactionKey: "Contributions and Loans",
        ElectionYear: 2026,
        NameOfFile: "CON_2026.csv",
        TransactionType: "CON",
      },
    ]);
  });

  it("builds stable cache paths", async () => {
    const cacheDir = await makeTempDir();

    expect(getMaineCfisArtifactCachePaths({ cacheDir, filingYear: 2026, artifactKind: "contributions" })).toEqual({
      cacheDir,
      filePath: join(cacheDir, "CON_2026.csv"),
      metadataPath: join(cacheDir, "CON_2026.metadata.json"),
    });
    expect(getMaineCfisArtifactCachePaths({ cacheDir, filingYear: 2026, artifactKind: "expenditures" }).filePath).toBe(
      join(cacheDir, "EXP_2026.csv")
    );
  });

  it("rejects invalid refresh timestamps before remote fetches", async () => {
    const cacheDir = await makeTempDir();
    const fetchImpl = vi.fn<typeof fetch>();

    await expect(
      refreshMaineCfisArtifactCache({
        filingYear: 2026,
        artifactKind: "contributions",
        cacheDir,
        fetchImpl,
        now: new Date("not a date"),
      })
    ).rejects.toThrow("Invalid Maine CFIS artifact refresh timestamp");

    expect(fetchImpl).not.toHaveBeenCalled();
  });

  it("downloads and writes cache metadata when the artifact is missing", async () => {
    const cacheDir = await makeTempDir();
    const fetchImpl = vi.fn<typeof fetch>(async (_url, init) => {
      expect(init?.method).toBe("POST");
      expect(init?.body).toBe(JSON.stringify({ year: 2026, transactionType: "CON" }));
      if (fetchImpl.mock.calls.length === 1) {
        return response("metadata-probe", {
          "content-length": "25",
          "content-type": "application/octet-stream",
          "content-disposition": "attachment; filename=CON_2026.csv",
          etag: '"me-2026-con-a"',
          "last-modified": "Thu, 25 Jun 2026 08:03:00 GMT",
        });
      }
      return response("OrgID,Committee Name\n1,A\n", {
        "content-length": "25",
        "content-type": "application/octet-stream",
        "content-disposition": "attachment; filename=CON_2026.csv",
        etag: '"me-2026-con-a"',
        "last-modified": "Thu, 25 Jun 2026 08:03:00 GMT",
      });
    });

    const result = await refreshMaineCfisArtifactCache({
      filingYear: 2026,
      artifactKind: "contributions",
      cacheDir,
      fetchImpl,
      now: new Date("2026-06-25T12:00:00.000Z"),
    });

    expect(result.status).toBe("downloaded");
    expect(fetchImpl).toHaveBeenCalledTimes(2);
    expect(await readFile(result.filePath, "utf8")).toBe("OrgID,Committee Name\n1,A\n");
    await expect(readMaineCfisArtifactCacheMetadata(result.metadataPath)).resolves.toMatchObject({
      version: 1,
      artifact: {
        filingYear: 2026,
        artifactKind: "contributions",
      },
      downloadedAt: "2026-06-25T12:00:00.000Z",
      remote: {
        url: MAINE_CFIS_CSV_DOWNLOAD_API_URL,
        requestBody: {
          year: 2026,
          transactionType: "CON",
        },
        contentDisposition: "attachment; filename=CON_2026.csv",
      },
    });
  });

  it("rejects downloads whose byte count does not match content-length", async () => {
    const cacheDir = await makeTempDir();
    const paths = getMaineCfisArtifactCachePaths({ cacheDir, filingYear: 2026, artifactKind: "expenditures" });
    const fetchImpl = vi.fn<typeof fetch>(async () => {
      if (fetchImpl.mock.calls.length === 1) {
        return response("metadata-probe", { "content-length": "100" });
      }
      return response("short", { "content-length": "100" });
    });

    await expect(
      refreshMaineCfisArtifactCache({
        filingYear: 2026,
        artifactKind: "expenditures",
        cacheDir,
        fetchImpl,
      })
    ).rejects.toThrow("Maine CFIS artifact download size mismatch");

    await expect(readFile(paths.filePath, "utf8")).rejects.toThrow();
  });

  it("skips download when cached metadata still matches", async () => {
    const cacheDir = await makeTempDir();
    const paths = getMaineCfisArtifactCachePaths({ cacheDir, filingYear: 2026, artifactKind: "expenditures" });
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
            url: MAINE_CFIS_CSV_DOWNLOAD_API_URL,
            requestBody: {
              year: 2026,
              transactionType: "EXP",
            },
            contentLength: 10,
            contentType: "application/octet-stream",
            contentDisposition: "attachment; filename=EXP_2026.csv",
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
        "content-type": "application/octet-stream",
        "content-disposition": "attachment; filename=EXP_2026.csv",
        etag: '"same"',
        "last-modified": "Thu, 25 Jun 2026 08:03:00 GMT",
      })
    );

    const result = await refreshMaineCfisArtifactCache({
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
    const paths = getMaineCfisArtifactCachePaths({ cacheDir, filingYear: 2026, artifactKind: "contributions" });
    await writeFile(paths.metadataPath, "{not-json", "utf8");
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});

    await expect(readMaineCfisArtifactCacheMetadata(paths.metadataPath)).resolves.toBeNull();
    expect(warn).toHaveBeenCalledWith(
      expect.stringContaining("Unexpected error reading Maine CFIS cache metadata"),
      expect.any(Error)
    );
  });
});
