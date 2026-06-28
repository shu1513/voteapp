import { createHash } from "node:crypto";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";

import {
  DEFAULT_LOUISIANA_CAMPAIGN_FINANCE_CACHE_DIR,
  LOUISIANA_CAMPAIGN_FINANCE_DOWNLOAD_PAGE_URL,
  buildLouisianaCampaignFinanceDownloadFileName,
  buildLouisianaCampaignFinanceDownloadUrl,
  fetchLouisianaCampaignFinanceDownloadMetadata,
  getLouisianaCampaignFinanceArtifactCachePaths,
  refreshLouisianaCampaignFinanceArtifactCache,
} from "../../../src/pipeline/louisianaFinance/louisianaCampaignFinanceArtifactCache.js";

const tempDirs: string[] = [];

async function makeTempDir(): Promise<string> {
  const dir = await mkdtemp(join(tmpdir(), "voteapp-la-cf-cache-"));
  tempDirs.push(dir);
  return dir;
}

function response(body: string | null, headers: Record<string, string> = {}): Response {
  return new Response(body, {
    status: 200,
    headers,
  });
}

function sha256(value: string): string {
  return createHash("sha256").update(value, "utf8").digest("hex");
}

afterEach(async () => {
  vi.restoreAllMocks();
  await Promise.all(tempDirs.splice(0).map((dir) => rm(dir, { recursive: true, force: true })));
});

describe("Louisiana campaign finance artifact cache", () => {
  it("exposes official bulk CSV URLs and stable cache paths", async () => {
    const cacheDir = await makeTempDir();
    const paths = getLouisianaCampaignFinanceArtifactCachePaths(cacheDir);

    expect(DEFAULT_LOUISIANA_CAMPAIGN_FINANCE_CACHE_DIR).toBe("scratch/louisiana-campaign-finance");
    expect(LOUISIANA_CAMPAIGN_FINANCE_DOWNLOAD_PAGE_URL).toBe(
      "https://www.ethics.la.gov/campaignfinancesearch/ShowPremadereports.aspx"
    );
    expect(buildLouisianaCampaignFinanceDownloadFileName("contributions")).toBe("Contributions_2024_to_2027.csv");
    expect(buildLouisianaCampaignFinanceDownloadFileName("expenditures")).toBe("Expenditures_2024_to_2027.csv");
    expect(buildLouisianaCampaignFinanceDownloadUrl("contributions")).toBe(
      "https://www.ethics.la.gov/Pub/CampFinan/DataDownload/ContributionReports/Contributions_2024_to_2027.csv"
    );
    expect(buildLouisianaCampaignFinanceDownloadUrl("expenditures")).toBe(
      "https://www.ethics.la.gov/Pub/CampFinan/DataDownload/ExpenditureReports/Expenditures_2024_to_2027.csv"
    );
    expect(paths.cacheDir).toBe(cacheDir);
    expect(paths.metadataPath).toBe(join(cacheDir, "louisiana_campaign_finance_2024_to_2027.metadata.json"));
    expect(paths.downloads).toMatchObject({
      contributions: join(cacheDir, "Contributions_2024_to_2027.csv"),
      expenditures: join(cacheDir, "Expenditures_2024_to_2027.csv"),
    });
  });

  it("builds alternate year ranges without changing the default phase-one URLs", () => {
    expect(buildLouisianaCampaignFinanceDownloadUrl("contributions", { startYear: 2020, endYear: 2023 })).toBe(
      "https://www.ethics.la.gov/Pub/CampFinan/DataDownload/ContributionReports/Contributions_2020_to_2023.csv"
    );
    expect(() => buildLouisianaCampaignFinanceDownloadUrl("expenditures", { startYear: 2027, endYear: 2024 })).toThrow(
      "Invalid Louisiana campaign finance year range"
    );
  });

  it("fetches remote metadata with HEAD", async () => {
    const fetchImpl = vi.fn<typeof fetch>(async (_url, init) => {
      expect(String(_url)).toBe(
        "https://www.ethics.la.gov/Pub/CampFinan/DataDownload/ContributionReports/Contributions_2024_to_2027.csv"
      );
      expect(init?.method).toBe("HEAD");
      return response(null, {
        "content-length": "66043026",
        "content-type": "application/octet-stream",
        "last-modified": "Fri, 26 Jun 2026 06:15:12 GMT",
      });
    });

    await expect(fetchLouisianaCampaignFinanceDownloadMetadata("contributions", {}, { fetchImpl })).resolves.toEqual({
      key: "contributions",
      label: "E-filed Contributions",
      sourcePageUrl: LOUISIANA_CAMPAIGN_FINANCE_DOWNLOAD_PAGE_URL,
      url: "https://www.ethics.la.gov/Pub/CampFinan/DataDownload/ContributionReports/Contributions_2024_to_2027.csv",
      filename: "Contributions_2024_to_2027.csv",
      contentLength: 66043026,
      contentType: "application/octet-stream",
      contentDisposition: null,
      lastModified: "Fri, 26 Jun 2026 06:15:12 GMT",
    });
  });

  it("falls back to a ranged GET when metadata HEAD is forbidden", async () => {
    const fetchImpl = vi.fn<typeof fetch>(async (_url, init) => {
      expect(String(_url)).toBe(
        "https://www.ethics.la.gov/Pub/CampFinan/DataDownload/ContributionReports/Contributions_2024_to_2027.csv"
      );
      if (init?.method === "HEAD") {
        return new Response(null, { status: 403, statusText: "Forbidden" });
      }
      expect(init?.method).toBe("GET");
      expect(new Headers(init?.headers).get("range")).toBe("bytes=0-0");
      return new Response("x", {
        status: 206,
        headers: {
        "content-length": "1",
        "content-range": "bytes 0-0/66043026",
        "content-type": "application/octet-stream",
        "last-modified": "Fri, 26 Jun 2026 06:15:12 GMT",
        },
      });
    });

    await expect(fetchLouisianaCampaignFinanceDownloadMetadata("contributions", {}, { fetchImpl })).resolves.toMatchObject({
      key: "contributions",
      filename: "Contributions_2024_to_2027.csv",
      contentLength: 66043026,
      contentType: "application/octet-stream",
      lastModified: "Fri, 26 Jun 2026 06:15:12 GMT",
    });
    expect(fetchImpl).toHaveBeenCalledTimes(2);
  });

  it("downloads both official CSVs and writes cache metadata", async () => {
    const cacheDir = await makeTempDir();
    const fetchImpl = vi.fn<typeof fetch>(async (_url, init) => {
      const url = String(_url);
      if (init?.method === "HEAD" && url.includes("ContributionReports")) {
        return response(null, {
          "content-length": "9",
          "content-type": "application/octet-stream",
          "last-modified": "Fri, 26 Jun 2026 06:15:12 GMT",
        });
      }
      if (init?.method === "HEAD" && url.includes("ExpenditureReports")) {
        return response(null, {
          "content-length": "7",
          "content-type": "application/octet-stream",
          "last-modified": "Fri, 26 Jun 2026 06:05:54 GMT",
        });
      }
      if (init?.method === "GET" && url.includes("ContributionReports")) {
        return response("contrib-a", {
          "content-length": "9",
          "content-type": "text/csv",
          "content-disposition": 'attachment; filename="Contributions_2024_to_2027.csv"',
          "last-modified": "Fri, 26 Jun 2026 06:15:12 GMT",
        });
      }
      if (init?.method === "GET" && url.includes("ExpenditureReports")) {
        return response("expense", {
          "content-length": "7",
          "content-type": "text/csv",
          "content-disposition": 'attachment; filename="Expenditures_2024_to_2027.csv"',
          "last-modified": "Fri, 26 Jun 2026 06:05:54 GMT",
        });
      }
      throw new Error(`Unexpected fetch url: ${url}`);
    });

    const result = await refreshLouisianaCampaignFinanceArtifactCache({
      cacheDir,
      fetchImpl,
      now: new Date("2026-06-27T12:00:00.000Z"),
    });

    expect(result.status).toBe("downloaded");
    expect(fetchImpl).toHaveBeenCalledTimes(4);
    expect(await readFile(join(cacheDir, "Contributions_2024_to_2027.csv"), "utf8")).toBe("contrib-a");
    expect(await readFile(join(cacheDir, "Expenditures_2024_to_2027.csv"), "utf8")).toBe("expense");
    expect(result.current).toMatchObject({
      version: 1,
      cacheDir,
      metadataPath: join(cacheDir, "louisiana_campaign_finance_2024_to_2027.metadata.json"),
      downloadedAt: "2026-06-27T12:00:00.000Z",
      sourcePageUrl: LOUISIANA_CAMPAIGN_FINANCE_DOWNLOAD_PAGE_URL,
      yearRange: { startYear: 2024, endYear: 2027 },
      downloads: {
        contributions: {
          outputPath: join(cacheDir, "Contributions_2024_to_2027.csv"),
          bytesWritten: 9,
          sha256: sha256("contrib-a"),
          remote: {
            key: "contributions",
            url: "https://www.ethics.la.gov/Pub/CampFinan/DataDownload/ContributionReports/Contributions_2024_to_2027.csv",
            lastModified: "Fri, 26 Jun 2026 06:15:12 GMT",
          },
        },
        expenditures: {
          outputPath: join(cacheDir, "Expenditures_2024_to_2027.csv"),
          bytesWritten: 7,
          sha256: sha256("expense"),
          remote: {
            key: "expenditures",
            url: "https://www.ethics.la.gov/Pub/CampFinan/DataDownload/ExpenditureReports/Expenditures_2024_to_2027.csv",
            lastModified: "Fri, 26 Jun 2026 06:05:54 GMT",
          },
        },
      },
    });
  });

  it("returns unchanged when remote metadata still matches local files", async () => {
    const cacheDir = await makeTempDir();
    const paths = getLouisianaCampaignFinanceArtifactCachePaths(cacheDir);
    await writeFile(paths.downloads.contributions, "contrib-a", "utf8");
    await writeFile(paths.downloads.expenditures, "expense", "utf8");
    await writeFile(
      paths.metadataPath,
      `${JSON.stringify(
        {
          version: 1,
          cacheDir,
          metadataPath: paths.metadataPath,
          downloadedAt: "2026-06-26T12:00:00.000Z",
          sourcePageUrl: LOUISIANA_CAMPAIGN_FINANCE_DOWNLOAD_PAGE_URL,
          yearRange: { startYear: 2024, endYear: 2027 },
          downloads: {
            contributions: {
              outputPath: paths.downloads.contributions,
              bytesWritten: 9,
              sha256: sha256("contrib-a"),
              remote: {
                key: "contributions",
                label: "E-filed Contributions",
                sourcePageUrl: LOUISIANA_CAMPAIGN_FINANCE_DOWNLOAD_PAGE_URL,
                url: buildLouisianaCampaignFinanceDownloadUrl("contributions"),
                filename: "Contributions_2024_to_2027.csv",
                contentLength: 9,
                contentType: "application/octet-stream",
                contentDisposition: null,
                lastModified: "Fri, 26 Jun 2026 06:15:12 GMT",
              },
            },
            expenditures: {
              outputPath: paths.downloads.expenditures,
              bytesWritten: 7,
              sha256: sha256("expense"),
              remote: {
                key: "expenditures",
                label: "E-filed Expenditures",
                sourcePageUrl: LOUISIANA_CAMPAIGN_FINANCE_DOWNLOAD_PAGE_URL,
                url: buildLouisianaCampaignFinanceDownloadUrl("expenditures"),
                filename: "Expenditures_2024_to_2027.csv",
                contentLength: 7,
                contentType: "application/octet-stream",
                contentDisposition: null,
                lastModified: "Fri, 26 Jun 2026 06:05:54 GMT",
              },
            },
          },
        },
        null,
        2
      )}\n`,
      "utf8"
    );

    const fetchImpl = vi.fn<typeof fetch>(async (_url, init) => {
      const url = String(_url);
      expect(init?.method).toBe("HEAD");
      if (url.includes("ContributionReports")) {
        return response(null, {
          "content-length": "9",
          "content-type": "application/octet-stream",
          "last-modified": "Fri, 26 Jun 2026 06:15:12 GMT",
        });
      }
      if (url.includes("ExpenditureReports")) {
        return response(null, {
          "content-length": "7",
          "content-type": "application/octet-stream",
          "last-modified": "Fri, 26 Jun 2026 06:05:54 GMT",
        });
      }
      throw new Error(`Unexpected fetch url: ${url}`);
    });

    const result = await refreshLouisianaCampaignFinanceArtifactCache({
      cacheDir,
      fetchImpl,
      now: new Date("2026-06-27T12:00:00.000Z"),
    });

    expect(result.status).toBe("unchanged");
    expect(result.current).toEqual(result.previous);
    expect(fetchImpl).toHaveBeenCalledTimes(2);
  });

  it("redownloads when remote metadata has no usable freshness validators", async () => {
    const cacheDir = await makeTempDir();
    const paths = getLouisianaCampaignFinanceArtifactCachePaths(cacheDir);
    await writeFile(paths.downloads.contributions, "old-contrib", "utf8");
    await writeFile(paths.downloads.expenditures, "old-expense", "utf8");
    await writeFile(
      paths.metadataPath,
      `${JSON.stringify(
        {
          version: 1,
          cacheDir,
          metadataPath: paths.metadataPath,
          downloadedAt: "2026-06-26T12:00:00.000Z",
          sourcePageUrl: LOUISIANA_CAMPAIGN_FINANCE_DOWNLOAD_PAGE_URL,
          yearRange: { startYear: 2024, endYear: 2027 },
          downloads: {
            contributions: {
              outputPath: paths.downloads.contributions,
              bytesWritten: 11,
              sha256: sha256("old-contrib"),
              remote: {
                key: "contributions",
                label: "E-filed Contributions",
                sourcePageUrl: LOUISIANA_CAMPAIGN_FINANCE_DOWNLOAD_PAGE_URL,
                url: buildLouisianaCampaignFinanceDownloadUrl("contributions"),
                filename: "Contributions_2024_to_2027.csv",
                contentLength: null,
                contentType: "application/octet-stream",
                contentDisposition: null,
                lastModified: null,
              },
            },
            expenditures: {
              outputPath: paths.downloads.expenditures,
              bytesWritten: 11,
              sha256: sha256("old-expense"),
              remote: {
                key: "expenditures",
                label: "E-filed Expenditures",
                sourcePageUrl: LOUISIANA_CAMPAIGN_FINANCE_DOWNLOAD_PAGE_URL,
                url: buildLouisianaCampaignFinanceDownloadUrl("expenditures"),
                filename: "Expenditures_2024_to_2027.csv",
                contentLength: null,
                contentType: "application/octet-stream",
                contentDisposition: null,
                lastModified: null,
              },
            },
          },
        },
        null,
        2
      )}\n`,
      "utf8"
    );

    const fetchImpl = vi.fn<typeof fetch>(async (_url, init) => {
      const url = String(_url);
      if (init?.method === "HEAD") {
        return response(null, { "content-type": "application/octet-stream" });
      }
      return response(url.includes("ContributionReports") ? "new-contrib" : "new-expense", {
        "content-type": "text/csv",
      });
    });

    const result = await refreshLouisianaCampaignFinanceArtifactCache({
      cacheDir,
      fetchImpl,
      now: new Date("2026-06-27T12:00:00.000Z"),
    });

    expect(result.status).toBe("downloaded");
    expect(fetchImpl).toHaveBeenCalledTimes(4);
    expect(await readFile(paths.downloads.contributions, "utf8")).toBe("new-contrib");
    expect(await readFile(paths.downloads.expenditures, "utf8")).toBe("new-expense");
  });

  it("redownloads when a matching cached file fails integrity checks", async () => {
    const cacheDir = await makeTempDir();
    const paths = getLouisianaCampaignFinanceArtifactCachePaths(cacheDir);
    await writeFile(paths.downloads.contributions, "truncated", "utf8");
    await writeFile(paths.downloads.expenditures, "expense", "utf8");
    await writeFile(
      paths.metadataPath,
      `${JSON.stringify(
        {
          version: 1,
          cacheDir,
          metadataPath: paths.metadataPath,
          downloadedAt: "2026-06-26T12:00:00.000Z",
          sourcePageUrl: LOUISIANA_CAMPAIGN_FINANCE_DOWNLOAD_PAGE_URL,
          yearRange: { startYear: 2024, endYear: 2027 },
          downloads: {
            contributions: {
              outputPath: paths.downloads.contributions,
              bytesWritten: 9,
              sha256: sha256("contrib-a"),
              remote: {
                key: "contributions",
                label: "E-filed Contributions",
                sourcePageUrl: LOUISIANA_CAMPAIGN_FINANCE_DOWNLOAD_PAGE_URL,
                url: buildLouisianaCampaignFinanceDownloadUrl("contributions"),
                filename: "Contributions_2024_to_2027.csv",
                contentLength: 9,
                contentType: "application/octet-stream",
                contentDisposition: null,
                lastModified: "Fri, 26 Jun 2026 06:15:12 GMT",
              },
            },
            expenditures: {
              outputPath: paths.downloads.expenditures,
              bytesWritten: 7,
              sha256: sha256("expense"),
              remote: {
                key: "expenditures",
                label: "E-filed Expenditures",
                sourcePageUrl: LOUISIANA_CAMPAIGN_FINANCE_DOWNLOAD_PAGE_URL,
                url: buildLouisianaCampaignFinanceDownloadUrl("expenditures"),
                filename: "Expenditures_2024_to_2027.csv",
                contentLength: 7,
                contentType: "application/octet-stream",
                contentDisposition: null,
                lastModified: "Fri, 26 Jun 2026 06:05:54 GMT",
              },
            },
          },
        },
        null,
        2
      )}\n`,
      "utf8"
    );

    const fetchImpl = vi.fn<typeof fetch>(async (_url, init) => {
      const url = String(_url);
      if (init?.method === "HEAD") {
        return response(null, {
          "content-length": url.includes("ContributionReports") ? "9" : "7",
          "content-type": "application/octet-stream",
          "last-modified": url.includes("ContributionReports")
            ? "Fri, 26 Jun 2026 06:15:12 GMT"
            : "Fri, 26 Jun 2026 06:05:54 GMT",
        });
      }
      return response(url.includes("ContributionReports") ? "contrib-a" : "expense", {
        "content-type": "text/csv",
      });
    });

    const result = await refreshLouisianaCampaignFinanceArtifactCache({ cacheDir, fetchImpl });

    expect(result.status).toBe("downloaded");
    expect(fetchImpl).toHaveBeenCalledTimes(4);
  });

  it("rejects download size mismatches", async () => {
    const cacheDir = await makeTempDir();
    const fetchImpl = vi.fn<typeof fetch>(async (_url, init) => {
      const url = String(_url);
      if (init?.method === "HEAD") {
        return response(null, {
          "content-length": url.includes("ContributionReports") ? "10" : "7",
          "content-type": "application/octet-stream",
          "last-modified": "Fri, 26 Jun 2026 06:15:12 GMT",
        });
      }
      if (url.includes("ContributionReports")) {
        return response("short", {
          "content-length": "10",
          "content-type": "text/csv",
        });
      }
      return response("expense", {
        "content-length": "7",
        "content-type": "text/csv",
      });
    });

    await expect(refreshLouisianaCampaignFinanceArtifactCache({ cacheDir, fetchImpl })).rejects.toThrow(
      "download size mismatch"
    );
  });
});
