import { createHash } from "node:crypto";
import { mkdtemp, readdir, readFile, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { afterEach, describe, expect, it, vi } from "vitest";

import {
  getWestVirginiaArtifactCachePaths,
  readWestVirginiaApiContributionsArtifact,
  readWestVirginiaArtifactCacheMetadata,
  readWestVirginiaBulkArtifact,
  refreshWestVirginiaApiContributionsArtifact,
  refreshWestVirginiaBulkArtifact,
} from "../../../src/pipeline/westVirginiaFinance/westVirginiaCfrsArtifactCache.js";
import type { WestVirginiaDataDownloadCatalogRow } from "../../../src/pipeline/westVirginiaFinance/westVirginiaCfrsClient.js";

const CON_HEADER =
  "RegistrantID,CommitteeName,CandidateName,TransactionType,TransactionCategory,TransactionDate,TransactionAmount,ContributorPayeeType,ContributorPayeeName,ContributorAddress,EmployerName,FiledDate";
const conCsvA = `${CON_HEADER}\n1010003610,Committee to Elect Dean Jeffries,Warren "Dean" Jeffries,Contributions,Monetary,2026-03-01,500.0000,Individual,Jane Doe,2 Oak St,Acme,2026-04-07\n`;
const conCsvB = `${CON_HEADER}\n1010003610,Committee to Elect Dean Jeffries,Warren "Dean" Jeffries,Contributions,Monetary,2026-03-01,750.0000,Individual,Jane Doe,2 Oak St,Acme,2026-04-07\n`;

const CATALOG: WestVirginiaDataDownloadCatalogRow[] = [
  { id: 54, dataType: "Contributions", year: "2026", s3ReportFilePath: "bulk/Contributions_2026_20260901.csv" },
  { id: 55, dataType: "Expenditures", year: "2026", s3ReportFilePath: "bulk/Expenditure_2026_20260901.csv" },
];

const PRESIGNED = "https://s3.example.test/bulk/Contributions_2026.csv?X-Amz-Security-Token=secret";

function jsonResponse(body: unknown): Response {
  return new Response(JSON.stringify(body), { status: 200, headers: { "Content-Type": "application/json" } });
}

function apiRow(transactionID: number, amount: number) {
  return {
    transactionID,
    entityID: "1010003610",
    orgID: 3610,
    transactionAmount: amount,
    transactionDate: "2026-03-01T00:00:00",
    entityTypeDesc: "Individual",
    transactionCategoryDesc: "Monetary",
    employerOccupation: "Retired",
  };
}

/** Routes the client's calls: download-URL mint, presigned body, API pages. */
function fetchImplServing(input: { csv?: string | Uint8Array; apiRows?: unknown[] }): typeof fetch {
  return vi.fn().mockImplementation(async (url: string, init?: RequestInit) => {
    if (url.includes("/AccessReport/getDataDownloadfile/")) {
      return jsonResponse({ isSuccess: true, responseData: { fileUrl: PRESIGNED } });
    }
    if (url.startsWith("https://s3.example.test/")) {
      const body = typeof input.csv === "string" ? Buffer.from(input.csv, "latin1") : Buffer.from(input.csv ?? "");
      return new Response(body, { status: 200, headers: { "Content-Type": "text/csv" } });
    }
    if (url.includes("/CommitteeTransactions/getAllPublicTransactionDataList")) {
      const request = JSON.parse(String(init?.body)) as { pageNumber: number };
      const rows = input.apiRows ?? [];
      return jsonResponse({
        isSuccess: true,
        responseData: { totalRecords: rows.length, data: request.pageNumber === 1 ? rows : [] },
      });
    }
    throw new Error(`unexpected fetch: ${url}`);
  }) as unknown as typeof fetch;
}

const tempDirs: string[] = [];
async function tempDir(): Promise<string> {
  const path = await mkdtemp(join(tmpdir(), "wv-cfrs-cache-test-"));
  tempDirs.push(path);
  return path;
}
afterEach(async () => {
  await Promise.all(tempDirs.splice(0).map((path) => rm(path, { recursive: true, force: true })));
});

async function refreshBulk(input: { cacheDir: string; csv: string | Uint8Array; force?: boolean }) {
  return refreshWestVirginiaBulkArtifact({
    kind: "contributions",
    year: 2026,
    cacheDir: input.cacheDir,
    catalog: CATALOG,
    force: input.force,
    clientOptions: { fetchImpl: fetchImplServing({ csv: input.csv }) },
    now: new Date("2026-09-01T12:00:00.000Z"),
  });
}

describe("refreshWestVirginiaBulkArtifact", () => {
  it("stores the raw bytes with integrity metadata under kind/year-keyed paths", async () => {
    const cacheDir = await tempDir();
    const result = await refreshBulk({ cacheDir, csv: conCsvA });
    const sha = createHash("sha256").update(Buffer.from(conCsvA, "latin1")).digest("hex");
    expect(result.status).toBe("downloaded");
    expect(result.filePath).toBe(join(cacheDir, `CON_2026.${sha.slice(0, 12)}.csv`));
    expect(result.metadataPath).toBe(join(cacheDir, "CON_2026.metadata.json"));
    expect((await readFile(result.filePath, "latin1"))).toBe(conCsvA);
    expect((await stat(cacheDir)).mode & 0o777).toBe(0o700);
    expect((await stat(result.filePath)).mode & 0o777).toBe(0o600);
    expect(result.current).toMatchObject({
      version: 1,
      artifact: { kind: "contributions", year: 2026 },
      downloadedAt: "2026-09-01T12:00:00.000Z",
      source: { catalogId: 54, s3ReportFilePath: "bulk/Contributions_2026_20260901.csv", dataType: "Contributions" },
      bytes: Buffer.byteLength(conCsvA, "latin1"),
      sha256: sha,
      recordCount: 1,
      recoveredRowCount: 0,
    });
    expect(await readWestVirginiaArtifactCacheMetadata(result.metadataPath)).toEqual(result.current);
    // No staging leftovers, and the presigned query never lands on disk.
    expect((await readdir(cacheDir)).sort()).toEqual([`CON_2026.${sha.slice(0, 12)}.csv`, "CON_2026.metadata.json"]);
    expect(JSON.stringify(result.current)).not.toContain("X-Amz");
  });

  it("reports unchanged on identical bytes and replaces on changed content", async () => {
    const cacheDir = await tempDir();
    const first = await refreshBulk({ cacheDir, csv: conCsvA });
    const second = await refreshBulk({ cacheDir, csv: conCsvA });
    expect(second.status).toBe("unchanged");
    expect(second.current).toEqual(first.current);
    const third = await refreshBulk({ cacheDir, csv: conCsvB });
    expect(third.status).toBe("downloaded");
    expect(third.filePath).not.toBe(first.filePath);
    const read = await readWestVirginiaBulkArtifact({ kind: "contributions", year: 2026, cacheDir });
    expect(read.csvText).toBe(conCsvB);
  });

  it("keeps the last good artifact when the download does not parse", async () => {
    const cacheDir = await tempDir();
    await refreshBulk({ cacheDir, csv: conCsvA });
    await expect(refreshBulk({ cacheDir, csv: conCsvA.replace("RegistrantID,", "Registrant,") })).rejects.toThrow(/header drift/);
    await expect(refreshBulk({ cacheDir, csv: `${CON_HEADER}\n1,2,3\n` })).rejects.toThrow(/1 row errors/);
    await expect(refreshBulk({ cacheDir, csv: "<html>blocked</html>" })).rejects.toThrow(/header drift/);
    const read = await readWestVirginiaBulkArtifact({ kind: "contributions", year: 2026, cacheDir });
    expect(read.csvText).toBe(conCsvA);
    expect((await readdir(cacheDir)).filter((name) => name.includes(".download-"))).toEqual([]);
  });

  it("refuses to replace a populated artifact with a header-only file unless forced", async () => {
    const cacheDir = await tempDir();
    await refreshBulk({ cacheDir, csv: conCsvA });
    await expect(refreshBulk({ cacheDir, csv: `${CON_HEADER}\n` })).rejects.toThrow(/returned 0 data rows; keeping the last good artifact \(1 rows\)/);
    expect((await readWestVirginiaBulkArtifact({ kind: "contributions", year: 2026, cacheDir })).csvText).toBe(conCsvA);
    const forced = await refreshBulk({ cacheDir, csv: `${CON_HEADER}\n`, force: true });
    expect(forced.status).toBe("downloaded");
    expect(forced.current.recordCount).toBe(0);
  });

  it("fails when the catalog has no entry for the kind and year", async () => {
    const cacheDir = await tempDir();
    await expect(
      refreshWestVirginiaBulkArtifact({
        kind: "reporting_schedules",
        year: 2026,
        cacheDir,
        catalog: CATALOG,
        clientOptions: { fetchImpl: fetchImplServing({ csv: conCsvA }) },
      })
    ).rejects.toThrow(/catalog has no Reporting Schedules 2026 entry/);
  });
});

describe("refreshWestVirginiaApiContributionsArtifact", () => {
  it("stores the harvest as JSON sorted by transactionID and reads it back", async () => {
    const cacheDir = await tempDir();
    const result = await refreshWestVirginiaApiContributionsArtifact({
      year: 2026,
      cacheDir,
      clientOptions: { fetchImpl: fetchImplServing({ apiRows: [apiRow(20, 100), apiRow(10, 50)] }) },
      now: new Date("2026-09-01T12:00:00.000Z"),
    });
    expect(result.status).toBe("downloaded");
    expect(result.filePath).toMatch(/APICON_2026\.[a-f0-9]{12}\.json$/);
    expect(result.current).toMatchObject({ artifact: { kind: "api_contributions", year: 2026 }, recordCount: 2 });
    const read = await readWestVirginiaApiContributionsArtifact({ year: 2026, cacheDir });
    expect(read.rows.map((row) => row.transactionID)).toEqual([10, 20]);
    expect(read.rows[0]).toMatchObject({ entityID: "1010003610", transactionAmount: 50, employerOccupation: "Retired" });

    // Same rows in another order hash identically.
    const again = await refreshWestVirginiaApiContributionsArtifact({
      year: 2026,
      cacheDir,
      clientOptions: { fetchImpl: fetchImplServing({ apiRows: [apiRow(10, 50), apiRow(20, 100)] }) },
    });
    expect(again.status).toBe("unchanged");
  });
});

describe("read helpers", () => {
  it("throw on missing metadata, corrupted content, and a different kind at the same path", async () => {
    const cacheDir = await tempDir();
    await expect(readWestVirginiaBulkArtifact({ kind: "contributions", year: 2026, cacheDir })).rejects.toThrow(/no cached metadata/);
    const result = await refreshBulk({ cacheDir, csv: conCsvA });
    await writeFile(result.filePath, "corrupted");
    await expect(readWestVirginiaBulkArtifact({ kind: "contributions", year: 2026, cacheDir })).rejects.toThrow(/missing or corrupt/);
    const paths = getWestVirginiaArtifactCachePaths({ cacheDir, kind: "expenditures", year: 2026 });
    await writeFile(paths.metadataPath, JSON.stringify(result.current));
    await expect(readWestVirginiaBulkArtifact({ kind: "expenditures", year: 2026, cacheDir })).rejects.toThrow(/missing or corrupt/);
  });
});
