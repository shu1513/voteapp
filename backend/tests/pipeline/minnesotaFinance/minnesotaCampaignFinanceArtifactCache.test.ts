import { createHash } from "node:crypto";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";

import {
  DEFAULT_MINNESOTA_CAMPAIGN_FINANCE_CACHE_DIR,
  MINNESOTA_CAMPAIGN_FINANCE_DOWNLOAD_PAGE_URL,
  discoverMinnesotaCampaignFinanceDownloadUrls,
  getMinnesotaCampaignFinanceArtifactCachePaths,
  readMinnesotaCampaignFinanceArtifactCacheMetadata,
  refreshMinnesotaCampaignFinanceArtifactCache,
} from "../../../src/pipeline/minnesotaFinance/minnesotaCampaignFinanceArtifactCache.js";

const tempDirs: string[] = [];

async function makeTempDir(): Promise<string> {
  const dir = await mkdtemp(join(tmpdir(), "voteapp-mn-cf-cache-"));
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

describe("Minnesota campaign finance artifact cache", () => {
  it("exposes the official downloads page and stable cache paths", async () => {
    const cacheDir = await makeTempDir();
    const paths = getMinnesotaCampaignFinanceArtifactCachePaths(cacheDir);

    expect(DEFAULT_MINNESOTA_CAMPAIGN_FINANCE_CACHE_DIR).toBe("scratch/minnesota-campaign-finance");
    expect(MINNESOTA_CAMPAIGN_FINANCE_DOWNLOAD_PAGE_URL).toBe(
      "https://register.cfb.mn.gov/reports-and-data/self-help/data-downloads/campaign-finance/"
    );
    expect(paths.cacheDir).toBe(cacheDir);
    expect(paths.metadataPath).toBe(join(cacheDir, "minnesota_campaign_finance.metadata.json"));
    expect(paths.downloads).toMatchObject({
      contributions_received: join(cacheDir, "all_contributions_received.csv"),
      independent_expenditures: join(cacheDir, "all_independent_expenditures.csv"),
      independent_expenditure_contributions: join(cacheDir, "ie_committee_contributions.csv"),
    });
  });

  it("discovers the required download urls from the public page", async () => {
    const fetchImpl = vi.fn<typeof fetch>(async (_url, init) => {
      expect(init?.method).toBe("GET");
      return response(
        [
          "<table>",
          `<tr><td><a href="/downloads/contrib.csv">All Contributions received by all entities - 2015 to present Download</a></td></tr>`,
          `<tr><td><a href="/downloads/ie.csv">All Independent expenditures by all entities - 2015 to present Download</a></td></tr>`,
          `<tr><td><a href="/downloads/ie-contrib.csv">Independent expenditure committees and funds Contributions received by independent expenditure committees and funds only - 2015 to present Download</a></td></tr>`,
          "</table>",
        ].join(""),
        {
          "content-type": "text/html; charset=utf-8",
        }
      );
    });

    await expect(discoverMinnesotaCampaignFinanceDownloadUrls({ fetchImpl })).resolves.toEqual({
      contributions_received: "https://register.cfb.mn.gov/downloads/contrib.csv",
      independent_expenditures: "https://register.cfb.mn.gov/downloads/ie.csv",
      independent_expenditure_contributions: "https://register.cfb.mn.gov/downloads/ie-contrib.csv",
    });
    expect(fetchImpl).toHaveBeenCalledTimes(1);
  });

  it("downloads and writes cache metadata when the cache is missing", async () => {
    const cacheDir = await makeTempDir();
    const fetchImpl = vi.fn<typeof fetch>(async (_url, init) => {
      if (init?.method === "GET") {
        const url = String(_url);
        if (url === MINNESOTA_CAMPAIGN_FINANCE_DOWNLOAD_PAGE_URL) {
          return response(
            [
              "<table>",
              `<tr><td><a href="/downloads/contrib.csv">All Contributions received by all entities - 2015 to present Download</a></td></tr>`,
              `<tr><td><a href="/downloads/ie.csv">All Independent expenditures by all entities - 2015 to present Download</a></td></tr>`,
              `<tr><td><a href="/downloads/ie-contrib.csv">Independent expenditure committees and funds Contributions received by independent expenditure committees and funds only - 2015 to present Download</a></td></tr>`,
              "</table>",
            ].join(""),
            {
              "content-type": "text/html; charset=utf-8",
            }
          );
        }
        if (url.endsWith("/downloads/contrib.csv")) {
          return response("contrib-a", {
            "content-length": "9",
            "content-type": "text/csv",
          });
        }
        if (url.endsWith("/downloads/ie.csv")) {
          return response("ie-data", {
            "content-length": "7",
            "content-type": "text/csv",
          });
        }
        if (url.endsWith("/downloads/ie-contrib.csv")) {
          return response("ie-contrib", {
            "content-length": "10",
            "content-type": "text/csv",
          });
        }
      }

      if (init?.method === "HEAD") {
        return response(
          [
            "<table>",
            `<tr><td><a href="/downloads/contrib.csv">All Contributions received by all entities - 2015 to present Download</a></td></tr>`,
            `<tr><td><a href="/downloads/ie.csv">All Independent expenditures by all entities - 2015 to present Download</a></td></tr>`,
            `<tr><td><a href="/downloads/ie-contrib.csv">Independent expenditure committees and funds Contributions received by independent expenditure committees and funds only - 2015 to present Download</a></td></tr>`,
            "</table>",
          ].join(""),
          {
            "content-type": "text/html; charset=utf-8",
          }
        );
      }

      const url = String(_url);
      if (url.endsWith("/downloads/contrib.csv")) {
        return response("contrib-a", {
          "content-length": "9",
          "content-type": "text/csv",
          "content-disposition": 'attachment; filename="contrib.csv"',
        });
      }
      if (url.endsWith("/downloads/ie.csv")) {
        return response("ie-data", {
          "content-length": "7",
          "content-type": "text/csv",
          "content-disposition": 'attachment; filename="ie.csv"',
        });
      }
      if (url.endsWith("/downloads/ie-contrib.csv")) {
        return response("ie-contrib", {
          "content-length": "10",
          "content-type": "text/csv",
          "content-disposition": 'attachment; filename="ie-contrib.csv"',
        });
      }

      throw new Error(`Unexpected fetch url: ${url}`);
    });

    const result = await refreshMinnesotaCampaignFinanceArtifactCache({
      cacheDir,
      fetchImpl,
      now: new Date("2026-06-25T12:00:00.000Z"),
    });

    expect(result.status).toBe("downloaded");
    expect(fetchImpl).toHaveBeenCalledTimes(4);
    expect(await readFile(join(cacheDir, "all_contributions_received.csv"), "utf8")).toBe("contrib-a");
    expect(await readFile(join(cacheDir, "all_independent_expenditures.csv"), "utf8")).toBe("ie-data");
    expect(await readFile(join(cacheDir, "ie_committee_contributions.csv"), "utf8")).toBe("ie-contrib");

    const metadata = await readMinnesotaCampaignFinanceArtifactCacheMetadata(result.metadataPath);
    expect(metadata).toMatchObject({
      version: 1,
      cacheDir,
      metadataPath: result.metadataPath,
      downloadedAt: "2026-06-25T12:00:00.000Z",
      sourcePageUrl: MINNESOTA_CAMPAIGN_FINANCE_DOWNLOAD_PAGE_URL,
      downloads: {
        contributions_received: {
          outputPath: join(cacheDir, "all_contributions_received.csv"),
          bytesWritten: 9,
          remote: {
            url: "https://register.cfb.mn.gov/downloads/contrib.csv",
          },
        },
        independent_expenditures: {
          outputPath: join(cacheDir, "all_independent_expenditures.csv"),
          bytesWritten: 7,
          remote: {
            url: "https://register.cfb.mn.gov/downloads/ie.csv",
          },
        },
        independent_expenditure_contributions: {
          outputPath: join(cacheDir, "ie_committee_contributions.csv"),
          bytesWritten: 10,
          remote: {
            url: "https://register.cfb.mn.gov/downloads/ie-contrib.csv",
          },
        },
      },
    });
  });

  it("rejects refreshes when a required download label is missing", async () => {
    const cacheDir = await makeTempDir();
    const fetchImpl = vi.fn<typeof fetch>(async (_url, init) => {
      if (init?.method === "GET") {
        return response(
          [
            "<table>",
            `<tr><td><a href="/downloads/contrib.csv">All Contributions received by all entities - 2015 to present Download</a></td></tr>`,
            `<tr><td><a href="/downloads/ie.csv">All Independent expenditures by all entities - 2015 to present Download</a></td></tr>`,
            "</table>",
          ].join(""),
          {
            "content-type": "text/html; charset=utf-8",
          }
        );
      }
      return response("ignored", {
        "content-length": "7",
        "content-type": "text/csv",
      });
    });

    await expect(
      refreshMinnesotaCampaignFinanceArtifactCache({
        cacheDir,
        fetchImpl,
        now: new Date("2026-06-25T12:00:00.000Z"),
      })
    ).rejects.toThrow("Missing Minnesota campaign finance download link for independent_expenditure_contributions");
    expect(fetchImpl).toHaveBeenCalledTimes(1);
  });

  it("returns unchanged when the metadata still matches the downloaded files", async () => {
    const cacheDir = await makeTempDir();
    const paths = getMinnesotaCampaignFinanceArtifactCachePaths(cacheDir);
    await writeFile(paths.downloads.contributions_received, "contrib-a", "utf8");
    await writeFile(paths.downloads.independent_expenditures, "ie-data", "utf8");
    await writeFile(paths.downloads.independent_expenditure_contributions, "ie-contrib", "utf8");
    await writeFile(
      paths.metadataPath,
      `${JSON.stringify(
        {
          version: 1,
          cacheDir: paths.cacheDir,
          metadataPath: paths.metadataPath,
          downloadedAt: "2026-06-24T12:00:00.000Z",
          sourcePageUrl: MINNESOTA_CAMPAIGN_FINANCE_DOWNLOAD_PAGE_URL,
          downloads: {
            contributions_received: {
              outputPath: paths.downloads.contributions_received,
              bytesWritten: 9,
              sha256: sha256("contrib-a"),
              remote: {
                key: "contributions_received",
                label: "All Contributions received by all entities - 2015 to present",
                sourcePageUrl: MINNESOTA_CAMPAIGN_FINANCE_DOWNLOAD_PAGE_URL,
                url: "https://register.cfb.mn.gov/downloads/contrib.csv",
                filename: "contrib.csv",
                contentLength: 9,
                contentType: "text/csv",
                contentDisposition: 'attachment; filename="contrib.csv"',
              },
            },
            independent_expenditures: {
              outputPath: paths.downloads.independent_expenditures,
              bytesWritten: 7,
              sha256: sha256("ie-data"),
              remote: {
                key: "independent_expenditures",
                label: "All Independent expenditures by all entities - 2015 to present",
                sourcePageUrl: MINNESOTA_CAMPAIGN_FINANCE_DOWNLOAD_PAGE_URL,
                url: "https://register.cfb.mn.gov/downloads/ie.csv",
                filename: "ie.csv",
                contentLength: 7,
                contentType: "text/csv",
                contentDisposition: 'attachment; filename="ie.csv"',
              },
            },
            independent_expenditure_contributions: {
              outputPath: paths.downloads.independent_expenditure_contributions,
              bytesWritten: 10,
              sha256: sha256("ie-contrib"),
              remote: {
                key: "independent_expenditure_contributions",
                label:
                  "Independent expenditure committees and funds Contributions received by independent expenditure committees and funds only - 2015 to present",
                sourcePageUrl: MINNESOTA_CAMPAIGN_FINANCE_DOWNLOAD_PAGE_URL,
                url: "https://register.cfb.mn.gov/downloads/ie-contrib.csv",
                filename: "ie-contrib.csv",
                contentLength: 10,
                contentType: "text/csv",
                contentDisposition: 'attachment; filename="ie-contrib.csv"',
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
      if (init?.method === "GET" && url === MINNESOTA_CAMPAIGN_FINANCE_DOWNLOAD_PAGE_URL) {
        return response(
          [
            "<table>",
            `<tr><td><a href="/downloads/contrib.csv">All Contributions received by all entities - 2015 to present Download</a></td></tr>`,
            `<tr><td><a href="/downloads/ie.csv">All Independent expenditures by all entities - 2015 to present Download</a></td></tr>`,
            `<tr><td><a href="/downloads/ie-contrib.csv">Independent expenditure committees and funds Contributions received by independent expenditure committees and funds only - 2015 to present Download</a></td></tr>`,
            "</table>",
          ].join(""),
          {
            "content-type": "text/html; charset=utf-8",
          }
        );
      }
      if (url.endsWith("/downloads/contrib.csv")) {
        return response("contrib-a", {
          "content-length": "9",
          "content-type": "text/csv",
        });
      }
      if (url.endsWith("/downloads/ie.csv")) {
        return response("ie-data", {
          "content-length": "7",
          "content-type": "text/csv",
        });
      }
      if (url.endsWith("/downloads/ie-contrib.csv")) {
        return response("ie-contrib", {
          "content-length": "10",
          "content-type": "text/csv",
        });
      }

      throw new Error(`Unexpected fetch url: ${url}`);
    });

    const result = await refreshMinnesotaCampaignFinanceArtifactCache({
      cacheDir,
      fetchImpl,
      now: new Date("2026-06-25T12:00:00.000Z"),
    });

    expect(result.status).toBe("unchanged");
    expect(result.current).toEqual(result.previous);
    expect(fetchImpl).toHaveBeenCalledTimes(4);
  });
});
