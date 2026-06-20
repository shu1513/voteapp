import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";

import {
  buildConnecticutEcrisArtifactUrl,
  defaultConnecticutEcrisArtifactFormat,
  getConnecticutEcrisArtifactCachePaths,
  parseConnecticutEcrisHttpsUrl,
  readConnecticutEcrisArtifactCacheMetadata,
  refreshConnecticutEcrisArtifactCache,
} from "../../../src/pipeline/connecticutFinance/connecticutEcrisArtifactCache.js";

const tempDirs: string[] = [];

async function makeTempDir(): Promise<string> {
  const dir = await mkdtemp(join(tmpdir(), "voteapp-ct-ecris-cache-"));
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

describe("Connecticut eCRIS artifact cache", () => {
  it("builds official candidate/exploratory and party/PAC artifact URLs", () => {
    expect(
      buildConnecticutEcrisArtifactUrl({
        year: 2026,
        transactionType: "receipts",
        committeeType: "candidate_exploratory",
        period: "election",
      })
    ).toBe(
      "https://seec.ct.gov/ecrisreporting/Data/eCrisDownloads/exportdatafiles/Receipts2026ElectionYearCandidateExploratoryCommittees.csv"
    );
    expect(
      buildConnecticutEcrisArtifactUrl({
        year: 2026,
        transactionType: "disbursements",
        committeeType: "party_pac",
        period: "calendar",
      })
    ).toBe(
      "https://seec.ct.gov/ecrisreporting/Data/eCrisDownloads/exportdatafiles/Disbursements2026CalendarYearPartyPACCommittees.csv"
    );
    expect(
      buildConnecticutEcrisArtifactUrl({
        year: 2024,
        transactionType: "receipts",
        committeeType: "candidate_exploratory",
        period: "election",
      })
    ).toBe(
      "https://seec.ct.gov/ecrisreporting/Data/eCrisDownloads/exportdatafiles/Receipts2024ElectionYearCandidateExploratoryCommittees.xlsx"
    );
  });

  it("chooses the known current/previous-year default formats", () => {
    expect(defaultConnecticutEcrisArtifactFormat(2026)).toBe("csv");
    expect(defaultConnecticutEcrisArtifactFormat(2024)).toBe("xlsx");
    expect(defaultConnecticutEcrisArtifactFormat(2021)).toBe("csv");
  });

  it("rejects invalid URLs, invalid years, and nonexistent artifact pairings", () => {
    expect(() => parseConnecticutEcrisHttpsUrl("http://example.com/file.csv", "--url")).toThrow(
      "Only https is allowed"
    );
    expect(() =>
      buildConnecticutEcrisArtifactUrl({
        year: 2007,
        transactionType: "receipts",
        committeeType: "candidate_exploratory",
        period: "election",
      })
    ).toThrow("Invalid Connecticut eCRIS artifact year");
    expect(() =>
      buildConnecticutEcrisArtifactUrl({
        year: 2026,
        transactionType: "receipts",
        committeeType: "candidate_exploratory",
        period: "calendar",
      })
    ).toThrow("candidate/exploratory artifacts use election-year files");
    expect(() =>
      buildConnecticutEcrisArtifactUrl({
        year: 2026,
        transactionType: "receipts",
        committeeType: "party_pac",
        period: "election",
      })
    ).toThrow("party/PAC artifacts use calendar-year files");
  });

  it("downloads and writes cache metadata when the artifact is missing", async () => {
    const cacheDir = await makeTempDir();
    const fetchImpl = vi.fn<typeof fetch>(async (_url, init) => {
      if (init?.method === "HEAD") {
        return response(null, {
          "content-length": "12",
          "content-type": "text/csv",
          etag: '"ct-2026-a"',
          "last-modified": "Fri, 19 Jun 2026 09:00:49 GMT",
        });
      }
      return response("csv-contents", {
        "content-length": "12",
        "content-type": "text/csv",
        etag: '"ct-2026-a"',
        "last-modified": "Fri, 19 Jun 2026 09:00:49 GMT",
      });
    });

    const result = await refreshConnecticutEcrisArtifactCache({
      year: 2026,
      transactionType: "receipts",
      committeeType: "candidate_exploratory",
      period: "election",
      cacheDir,
      fetchImpl,
      now: new Date("2026-06-19T12:00:00.000Z"),
    });

    expect(result.status).toBe("downloaded");
    expect(fetchImpl).toHaveBeenCalledTimes(2);
    expect(await readFile(result.filePath, "utf8")).toBe("csv-contents");
    const metadata = await readConnecticutEcrisArtifactCacheMetadata(result.metadataPath);
    expect(metadata).toMatchObject({
      version: 1,
      artifact: {
        year: 2026,
        transactionType: "receipts",
        committeeType: "candidate_exploratory",
        period: "election",
        format: "csv",
      },
      downloadedAt: "2026-06-19T12:00:00.000Z",
      bytesWritten: 12,
      remote: {
        year: 2026,
        etag: '"ct-2026-a"',
        lastModified: "Fri, 19 Jun 2026 09:00:49 GMT",
      },
    });
  });

  it("skips download when cached metadata still matches", async () => {
    const cacheDir = await makeTempDir();
    const artifact = {
      year: 2026,
      transactionType: "receipts" as const,
      committeeType: "candidate_exploratory" as const,
      period: "election" as const,
    };
    const paths = getConnecticutEcrisArtifactCachePaths({ cacheDir, ...artifact });
    const url = buildConnecticutEcrisArtifactUrl(artifact);
    await writeFile(paths.filePath, "cached-csv", "utf8");
    await writeFile(
      paths.metadataPath,
      `${JSON.stringify(
        {
          version: 1,
          artifact: { ...artifact, format: "csv" },
          filePath: paths.filePath,
          metadataPath: paths.metadataPath,
          downloadedAt: "2026-06-18T12:00:00.000Z",
          bytesWritten: 10,
          remote: {
            ...artifact,
            format: "csv",
            url,
            contentLength: 10,
            contentType: "text/csv",
            etag: '"same"',
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
        "content-type": "text/csv",
        etag: '"same"',
        "last-modified": "Fri, 19 Jun 2026 09:00:49 GMT",
      })
    );

    const result = await refreshConnecticutEcrisArtifactCache({
      ...artifact,
      cacheDir,
      fetchImpl,
    });

    expect(result.status).toBe("unchanged");
    expect(fetchImpl).toHaveBeenCalledTimes(1);
    expect(await readFile(paths.filePath, "utf8")).toBe("cached-csv");
  });

  it("warns and returns null when cache metadata cannot be parsed", async () => {
    const cacheDir = await makeTempDir();
    const paths = getConnecticutEcrisArtifactCachePaths({
      cacheDir,
      year: 2026,
      transactionType: "receipts",
      committeeType: "candidate_exploratory",
      period: "election",
    });
    await writeFile(paths.metadataPath, "{not-json", "utf8");
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});

    await expect(readConnecticutEcrisArtifactCacheMetadata(paths.metadataPath)).resolves.toBeNull();
    expect(warn).toHaveBeenCalledWith(
      expect.stringContaining("Unexpected error reading Connecticut eCRIS cache metadata"),
      expect.any(Error)
    );
  });

  it("redownloads matching metadata when force is true", async () => {
    const cacheDir = await makeTempDir();
    const artifact = {
      year: 2026,
      transactionType: "receipts" as const,
      committeeType: "candidate_exploratory" as const,
      period: "election" as const,
    };
    const paths = getConnecticutEcrisArtifactCachePaths({ cacheDir, ...artifact });
    const url = buildConnecticutEcrisArtifactUrl(artifact);
    await writeFile(paths.filePath, "old", "utf8");
    await writeFile(
      paths.metadataPath,
      `${JSON.stringify(
        {
          version: 1,
          artifact: { ...artifact, format: "csv" },
          filePath: paths.filePath,
          metadataPath: paths.metadataPath,
          downloadedAt: "2026-06-18T12:00:00.000Z",
          bytesWritten: 3,
          remote: {
            ...artifact,
            format: "csv",
            url,
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

    const result = await refreshConnecticutEcrisArtifactCache({
      ...artifact,
      cacheDir,
      fetchImpl,
      force: true,
    });

    expect(result.status).toBe("downloaded");
    expect(fetchImpl).toHaveBeenCalledTimes(2);
    expect(await readFile(paths.filePath, "utf8")).toBe("new-csv");
  });
});
