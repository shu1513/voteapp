import { createHash } from "node:crypto";
import { mkdtemp, readFile, rm, stat, utimes, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { strToU8, zipSync } from "fflate";
import { afterEach, describe, expect, it, vi } from "vitest";

import type { AlabamaExtractCatalogRow } from "../../../src/pipeline/alabamaFinance/alabamaFcpaClient.js";
import {
  getAlabamaFcpaArtifactCachePaths,
  normalizeAlabamaExtractKind,
  normalizeAlabamaExtractYear,
  readAlabamaFcpaArtifact,
  readAlabamaFcpaArtifactCacheMetadata,
  refreshAlabamaFcpaArtifactCache,
  SWEEP_MIN_AGE_MS,
} from "../../../src/pipeline/alabamaFinance/alabamaFcpaArtifactCache.js";

const CASH_HEADER =
  "CommitteeId,ContributionAmount,ContributionDate,LastName,FirstName,MI,Suffix,Address1,City,State,Zip," +
  "ContributionID,FiledDate,ContributionType,ContributorType,CommitteeType,CommitteeName,CandidateName,Amended";

const cashCsvA = `${CASH_HEADER}\n32837,500.00,01/05/2026,Doe,Jane,,,1 Main St,Selma,AL,36701,1386065,01/06/2026,Cash (Itemized),Individual,Principal Campaign Committee,,DOUG JONES,N\n`;
const cashCsvB = `${CASH_HEADER}\n32837,750.00,01/07/2026,Doe,Jane,,,1 Main St,Selma,AL,36701,1386099,01/08/2026,Cash (Itemized),Individual,Principal Campaign Committee,,DOUG JONES,N\n`;

const CATALOG: AlabamaExtractCatalogRow[] = [
  {
    DATATYPE: "Cash Contribution",
    YEAR: 2026,
    LASTUPDATED: "08/28/2026",
    LASTUPDATEDRAW: "08/28/2026 02:32 AM",
    DOWNLOAD: 54,
  },
  {
    DATATYPE: "Expenditure",
    YEAR: 2026,
    LASTUPDATED: "08/28/2026",
    LASTUPDATEDRAW: "08/28/2026 02:32 AM",
    DOWNLOAD: 55,
  },
];

function cashZip(csv: string, level: 0 | 9 = 9): Uint8Array {
  return zipSync({ "CashContributionsExtract_2026.csv": strToU8(csv) }, { level });
}

function zipResponse(bytes: Uint8Array): Response {
  return new Response(Buffer.from(bytes), {
    status: 200,
    headers: { "Content-Type": "application/zip" },
  });
}

const tempDirs: string[] = [];

async function tempDir(): Promise<string> {
  const path = await mkdtemp(join(tmpdir(), "al-fcpa-cache-test-"));
  tempDirs.push(path);
  return path;
}

afterEach(async () => {
  await Promise.all(tempDirs.splice(0).map((path) => rm(path, { recursive: true, force: true })));
});

function fetchImplServing(bytes: Uint8Array): typeof fetch {
  return vi.fn().mockImplementation(async () => zipResponse(bytes)) as unknown as typeof fetch;
}

async function refresh(input: {
  cacheDir: string;
  bytes: Uint8Array;
  force?: boolean;
  now?: Date;
}) {
  return refreshAlabamaFcpaArtifactCache({
    kind: "cash",
    year: 2026,
    cacheDir: input.cacheDir,
    catalog: CATALOG,
    force: input.force,
    clientOptions: { fetchImpl: fetchImplServing(input.bytes) },
    now: input.now ?? new Date("2026-08-28T12:00:00.000Z"),
  });
}

describe("normalizers", () => {
  it("accepts known kinds and years, rejects the rest", () => {
    expect(normalizeAlabamaExtractKind("Cash Contribution")).toBe("cash");
    expect(normalizeAlabamaExtractKind("expenditures")).toBe("expenditure");
    expect(() => normalizeAlabamaExtractKind("In-Kind")).toThrow(/Invalid Alabama FCPA extract kind/);
    expect(normalizeAlabamaExtractYear(2013)).toBe(2013);
    expect(() => normalizeAlabamaExtractYear(2012)).toThrow(/Invalid Alabama FCPA extract year/);
    expect(() => normalizeAlabamaExtractYear(2026.5)).toThrow(/Invalid Alabama FCPA extract year/);
  });
});

describe("refreshAlabamaFcpaArtifactCache", () => {
  it("stores the raw zip with integrity metadata under kind/year-keyed paths", async () => {
    const cacheDir = await tempDir();
    const bytes = cashZip(cashCsvA);
    const result = await refresh({ cacheDir, bytes });

    const csvShaA = createHash("sha256").update(cashCsvA).digest("hex");
    expect(result.status).toBe("downloaded");
    expect(result.filePath).toBe(join(cacheDir, `CASH_2026.${csvShaA.slice(0, 12)}.zip`));
    expect(result.metadataPath).toBe(join(cacheDir, "CASH_2026.metadata.json"));
    expect(Buffer.from(await readFile(result.filePath)).equals(Buffer.from(bytes))).toBe(true);
    expect((await stat(cacheDir)).mode & 0o777).toBe(0o700);
    expect((await stat(result.filePath)).mode & 0o777).toBe(0o600);
    expect(result.current).toMatchObject({
      version: 1,
      artifact: { kind: "cash", year: 2026 },
      downloadedAt: "2026-08-28T12:00:00.000Z",
      source: { dataType: "Cash Contribution", downloadId: 54, lastUpdatedRaw: "08/28/2026 02:32 AM" },
      fileName: "CashContributionsExtract_2026.csv",
      zipBytes: bytes.byteLength,
      zipSha256: createHash("sha256").update(bytes).digest("hex"),
      csvSha256: createHash("sha256").update(cashCsvA).digest("hex"),
      recordCount: 1,
      quarantinedCount: 0,
    });
    expect(await readAlabamaFcpaArtifactCacheMetadata(result.metadataPath)).toEqual(result.current);
  });

  it("reports unchanged on identical CSV content even when the zip bytes recompress", async () => {
    const cacheDir = await tempDir();
    const first = await refresh({ cacheDir, bytes: cashZip(cashCsvA, 9) });
    // Same CSV, different compression level: different zip bytes.
    const recompressed = cashZip(cashCsvA, 0);
    expect(Buffer.from(recompressed).equals(Buffer.from(cashZip(cashCsvA, 9)))).toBe(false);

    const second = await refresh({ cacheDir, bytes: recompressed });
    expect(second.status).toBe("unchanged");
    expect(second.current).toEqual(first.current);
    // The stored artifact is still the first zip.
    expect(Buffer.from(await readFile(second.filePath)).equals(Buffer.from(cashZip(cashCsvA, 9)))).toBe(true);

    const forced = await refresh({ cacheDir, bytes: recompressed, force: true });
    expect(forced.status).toBe("downloaded");
  });

  it("replaces the artifact when the CSV content changes, sweeping only aged-out zip versions", async () => {
    const cacheDir = await tempDir();
    const first = await refresh({ cacheDir, bytes: cashZip(cashCsvA) });
    const result = await refresh({ cacheDir, bytes: cashZip(cashCsvB) });
    expect(result.status).toBe("downloaded");
    expect(result.filePath).not.toBe(first.filePath);
    expect(result.previous?.csvSha256).not.toBe(result.current.csvSha256);
    const read = await readAlabamaFcpaArtifact({ kind: "cash", year: 2026, cacheDir });
    expect(read.csvText).toBe(cashCsvB);
    // The superseded zip is minutes old — a concurrent refresh could still
    // reference it, so the sweep leaves it alone…
    await expect(stat(first.filePath)).resolves.toBeDefined();
    // …and removes it once it has aged past the sweep threshold.
    const agedOut = new Date(Date.now() - SWEEP_MIN_AGE_MS - 60_000);
    await utimes(first.filePath, agedOut, agedOut);
    const cashCsvC = cashCsvB.replace("750.00", "900.00");
    const third = await refresh({ cacheDir, bytes: cashZip(cashCsvC) });
    expect(third.status).toBe("downloaded");
    await expect(stat(first.filePath)).rejects.toMatchObject({ code: "ENOENT" });
  });

  it("refuses to replace a populated artifact with a header-only extract unless forced", async () => {
    const cacheDir = await tempDir();
    await refresh({ cacheDir, bytes: cashZip(cashCsvA) });
    const emptyCsv = `${CASH_HEADER}\n`;
    await expect(refresh({ cacheDir, bytes: cashZip(emptyCsv) })).rejects.toThrow(
      /returned 0 data rows; keeping the last good artifact \(1 rows\)/
    );
    const read = await readAlabamaFcpaArtifact({ kind: "cash", year: 2026, cacheDir });
    expect(read.csvText).toBe(cashCsvA);

    const forced = await refresh({ cacheDir, bytes: cashZip(emptyCsv), force: true });
    expect(forced.status).toBe("downloaded");
    expect(forced.current.recordCount).toBe(0);
  });

  it("accepts a header-only extract when there is no populated artifact to protect", async () => {
    const cacheDir = await tempDir();
    const emptyCsv = `${CASH_HEADER}\n`;
    const result = await refresh({ cacheDir, bytes: cashZip(emptyCsv) });
    expect(result.status).toBe("downloaded");
    expect(result.current.recordCount).toBe(0);
    expect((await readAlabamaFcpaArtifact({ kind: "cash", year: 2026, cacheDir })).csvText).toBe(emptyCsv);
  });

  it("keeps the last good pair readable when a crash lands a zip without its metadata commit", async () => {
    const cacheDir = await tempDir();
    await refresh({ cacheDir, bytes: cashZip(cashCsvA) });
    // Simulate a refresh that wrote the new content-addressed zip and died
    // before the metadata pointer committed it.
    const csvShaB = createHash("sha256").update(cashCsvB).digest("hex");
    await writeFile(join(cacheDir, `CASH_2026.${csvShaB.slice(0, 12)}.zip`), cashZip(cashCsvB));
    const read = await readAlabamaFcpaArtifact({ kind: "cash", year: 2026, cacheDir });
    expect(read.csvText).toBe(cashCsvA);
    // And the next successful refresh still converges on the new content.
    const next = await refresh({ cacheDir, bytes: cashZip(cashCsvB) });
    expect(next.status).toBe("downloaded");
    expect((await readAlabamaFcpaArtifact({ kind: "cash", year: 2026, cacheDir })).csvText).toBe(cashCsvB);
  });

  it("rejects an HTML body and keeps the last good artifact", async () => {
    const cacheDir = await tempDir();
    await refresh({ cacheDir, bytes: cashZip(cashCsvA) });
    await expect(
      refresh({ cacheDir, bytes: strToU8("<html><body>System Exception</body></html>") })
    ).rejects.toThrow(/not a zip archive/);
    const read = await readAlabamaFcpaArtifact({ kind: "cash", year: 2026, cacheDir });
    expect(read.csvText).toBe(cashCsvA);
  });

  it("rejects an empty archive and a changed header, keeping the last good artifact", async () => {
    const cacheDir = await tempDir();
    await refresh({ cacheDir, bytes: cashZip(cashCsvA) });
    await expect(refresh({ cacheDir, bytes: zipSync({}) })).rejects.toThrow(/0 CSV entries/);
    const changedHeader = cashCsvA.replace("CommitteeId,", "CommitteeNumber,");
    await expect(refresh({ cacheDir, bytes: cashZip(changedHeader) })).rejects.toThrow(
      /header changed/
    );
    const read = await readAlabamaFcpaArtifact({ kind: "cash", year: 2026, cacheDir });
    expect(read.csvText).toBe(cashCsvA);
  });

  it("fails when the catalog has no entry for the kind and year", async () => {
    const cacheDir = await tempDir();
    await expect(
      refreshAlabamaFcpaArtifactCache({
        kind: "cash",
        year: 2025,
        cacheDir,
        catalog: CATALOG,
        clientOptions: { fetchImpl: fetchImplServing(cashZip(cashCsvA)) },
      })
    ).rejects.toThrow(/catalog has no Cash Contribution 2025 entry/);
  });
});

describe("readAlabamaFcpaArtifact", () => {
  it("throws on missing metadata and on a corrupted artifact file", async () => {
    const cacheDir = await tempDir();
    await expect(readAlabamaFcpaArtifact({ kind: "cash", year: 2026, cacheDir })).rejects.toThrow(
      /no cached metadata/
    );

    const result = await refresh({ cacheDir, bytes: cashZip(cashCsvA) });
    await writeFile(result.filePath, strToU8("PK corrupted"));
    await expect(readAlabamaFcpaArtifact({ kind: "cash", year: 2026, cacheDir })).rejects.toThrow(
      /missing or corrupt/
    );
  });

  it("rejects metadata for a different kind at the same path", async () => {
    const cacheDir = await tempDir();
    const result = await refresh({ cacheDir, bytes: cashZip(cashCsvA) });
    const paths = getAlabamaFcpaArtifactCachePaths({ cacheDir, kind: "expenditure", year: 2026 });
    await writeFile(paths.metadataPath, JSON.stringify(result.current));
    await expect(
      readAlabamaFcpaArtifact({ kind: "expenditure", year: 2026, cacheDir })
    ).rejects.toThrow(/missing or corrupt/);
  });
});
