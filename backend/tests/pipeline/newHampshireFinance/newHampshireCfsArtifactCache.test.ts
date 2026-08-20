import { createHash } from "node:crypto";
import { chmod, mkdtemp, readFile, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { afterEach, describe, expect, it, vi } from "vitest";

import {
  getNewHampshireCfsArtifactCachePaths,
  readNewHampshireCfsArtifactCacheMetadata,
  refreshNewHampshireCfsArtifactCache,
} from "../../../src/pipeline/newHampshireFinance/newHampshireCfsArtifactCache.js";

const receiptHeader =
  "Filing Entity ID,Candidate Name,Committee Name,Committee Subtype,Transaction Type,Transaction Sub Type,Election Period,Election year,Date of Receipt,Amount of receipt,Contributor Type,Contributor Name,Contributor Address Line 1,Contributor Address Line 2,Contributor City,Contributor State,Contributor Zip Code,Contributor occupation,Contributor Employer,Contributor Principle place of Business,Description,Timed Report\n";
const artifactA = `${receiptHeader}101,,Committee A,,Contribution,Monetary,General,2026,01/01/2026,10.00,Individual,Donor,,,,NH,,,,,,No\n`;
const artifactB = `${receiptHeader}101,,Committee A,,Contribution,Monetary,General,2026,01/01/2026,25.00,Individual,Donor,,,,NH,,,,,,No\n`;
const tempDirs: string[] = [];

async function tempDir(): Promise<string> {
  const path = await mkdtemp(join(tmpdir(), "nh-cfs-cache-test-"));
  tempDirs.push(path);
  return path;
}

async function permissions(path: string): Promise<number> {
  return (await stat(path)).mode & 0o777;
}

function csvResponse(body: string): Response {
  return new Response(body, {
    status: 200,
    headers: {
      "Content-Type": "text/csv; charset=utf-8",
      "Content-Disposition": "attachment; filename=csv",
      "Content-Encoding": "gzip",
      Date: "Wed, 19 Aug 2026 12:00:00 GMT",
    },
  });
}

afterEach(async () => {
  await Promise.all(tempDirs.splice(0).map((path) => rm(path, { recursive: true, force: true })));
});

describe("New Hampshire CFS artifact cache", () => {
  it("streams the exact official POST into stable year-keyed paths and records integrity metadata", async () => {
    const cacheDir = await tempDir();
    await chmod(cacheDir, 0o755);
    const fetchImpl = vi.fn().mockImplementation(async () => csvResponse(artifactA)) as unknown as typeof fetch;
    const result = await refreshNewHampshireCfsArtifactCache({
      filingYear: 2026,
      artifactKind: "contributions",
      cacheDir,
      fetchImpl,
      now: new Date("2026-08-19T12:00:00.000Z"),
    });

    expect(result.status).toBe("downloaded");
    expect(result.filePath).toBe(join(cacheDir, "CON_2026.csv"));
    expect(result.metadataPath).toBe(join(cacheDir, "CON_2026.metadata.json"));
    expect(await readFile(result.filePath, "utf8")).toBe(artifactA);
    expect(await permissions(cacheDir)).toBe(0o700);
    expect(await permissions(result.filePath)).toBe(0o600);
    expect(await permissions(result.metadataPath)).toBe(0o600);
    expect(result.current).toMatchObject({
      downloadedAt: "2026-08-19T12:00:00.000Z",
      bytesWritten: Buffer.byteLength(artifactA),
      sha256: createHash("sha256").update(artifactA).digest("hex"),
      remote: {
        requestBody: { type: "CSV", filingYear: 2026, transactionTypeCode: "TCON" },
        contentType: "text/csv; charset=utf-8",
      },
    });
    expect(await readNewHampshireCfsArtifactCacheMetadata(result.metadataPath)).toEqual(result.current);

    const [url, init] = vi.mocked(fetchImpl).mock.calls[0]!;
    expect(url).toBe("https://cfsapi.sos.nh.gov/api/ExportData/GetExportPublicDownloadData");
    expect(init?.method).toBe("POST");
    expect(JSON.parse(String(init?.body))).toEqual({
      type: "CSV",
      filingYear: 2026,
      transactionTypeCode: "TCON",
    });
    expect(new Headers(init?.headers).get("origin")).toBe("https://cfs.sos.nh.gov");
  });

  it("downloads once to compare hashes, then preserves unchanged metadata and artifact", async () => {
    const cacheDir = await tempDir();
    const fetchImpl = vi.fn().mockImplementation(async () => csvResponse(artifactA)) as unknown as typeof fetch;
    const first = await refreshNewHampshireCfsArtifactCache({
      filingYear: 2026,
      artifactKind: "contributions",
      cacheDir,
      fetchImpl,
      now: new Date("2026-08-19T12:00:00.000Z"),
    });
    await chmod(cacheDir, 0o755);
    await chmod(first.filePath, 0o644);
    await chmod(first.metadataPath, 0o644);
    const second = await refreshNewHampshireCfsArtifactCache({
      filingYear: 2026,
      artifactKind: "contributions",
      cacheDir,
      fetchImpl,
      now: new Date("2026-08-20T12:00:00.000Z"),
    });

    expect(fetchImpl).toHaveBeenCalledTimes(2);
    expect(second.status).toBe("unchanged");
    expect(second.current).toEqual(first.current);
    expect(await readFile(second.filePath, "utf8")).toBe(artifactA);
    expect(await permissions(cacheDir)).toBe(0o700);
    expect(await permissions(second.filePath)).toBe(0o600);
    expect(await permissions(second.metadataPath)).toBe(0o600);
  });

  it("replaces changed content and repairs a cache whose file no longer matches metadata", async () => {
    const cacheDir = await tempDir();
    const fetchImpl = vi
      .fn()
      .mockResolvedValueOnce(csvResponse(artifactA))
      .mockResolvedValueOnce(csvResponse(artifactB))
      .mockResolvedValueOnce(csvResponse(artifactB)) as unknown as typeof fetch;
    const first = await refreshNewHampshireCfsArtifactCache({
      filingYear: 2026,
      artifactKind: "contributions",
      cacheDir,
      fetchImpl,
    });
    const changed = await refreshNewHampshireCfsArtifactCache({
      filingYear: 2026,
      artifactKind: "contributions",
      cacheDir,
      fetchImpl,
    });
    expect(changed.status).toBe("downloaded");
    expect(changed.current.sha256).not.toBe(first.current.sha256);
    expect(await readFile(changed.filePath, "utf8")).toBe(artifactB);

    await writeFile(changed.filePath, "corrupt", "utf8");
    const repaired = await refreshNewHampshireCfsArtifactCache({
      filingYear: 2026,
      artifactKind: "contributions",
      cacheDir,
      fetchImpl,
    });
    expect(repaired.status).toBe("downloaded");
    expect(await readFile(repaired.filePath, "utf8")).toBe(artifactB);
  });

  it("keeps the last good artifact when a refresh response is invalid", async () => {
    const cacheDir = await tempDir();
    const goodFetch = vi.fn().mockResolvedValue(csvResponse(artifactA)) as unknown as typeof fetch;
    const first = await refreshNewHampshireCfsArtifactCache({
      filingYear: 2026,
      artifactKind: "contributions",
      cacheDir,
      fetchImpl: goodFetch,
    });
    const badFetch = vi.fn().mockResolvedValue(
      new Response("<html>denied</html>", { status: 200, headers: { "Content-Type": "text/html" } })
    ) as unknown as typeof fetch;

    await expect(
      refreshNewHampshireCfsArtifactCache({
        filingYear: 2026,
        artifactKind: "contributions",
        cacheDir,
        fetchImpl: badFetch,
      })
    ).rejects.toThrow("unexpected content type");

    const badHeaderFetch = vi.fn().mockResolvedValue(
      new Response("not,a,real,cfs,csv\n", {
        status: 200,
        headers: { "Content-Type": "text/csv" },
      })
    ) as unknown as typeof fetch;
    await expect(
      refreshNewHampshireCfsArtifactCache({
        filingYear: 2026,
        artifactKind: "contributions",
        cacheDir,
        fetchImpl: badHeaderFetch,
      })
    ).rejects.toThrow("header changed");

    const badRowFetch = vi.fn().mockResolvedValue(
      csvResponse(`${receiptHeader}101,too,few\n`)
    ) as unknown as typeof fetch;
    await expect(
      refreshNewHampshireCfsArtifactCache({
        filingYear: 2026,
        artifactKind: "contributions",
        cacheDir,
        fetchImpl: badRowFetch,
      })
    ).rejects.toThrow("has 3 columns; expected 22");

    expect(await readFile(first.filePath, "utf8")).toBe(artifactA);
    expect((await readNewHampshireCfsArtifactCacheMetadata(first.metadataPath))?.sha256).toBe(
      first.current.sha256
    );
  });

  it("normalizes paths and rejects unsupported identities", () => {
    expect(
      getNewHampshireCfsArtifactCachePaths({
        cacheDir: "/cache",
        filingYear: 2024,
        artifactKind: "expenditures",
      }).filePath
    ).toBe("/cache/EXP_2024.csv");
    expect(() =>
      getNewHampshireCfsArtifactCachePaths({
        cacheDir: "/cache",
        filingYear: 2015,
        artifactKind: "contributions",
      })
    ).toThrow("Invalid New Hampshire CFS filing year");
  });
});
