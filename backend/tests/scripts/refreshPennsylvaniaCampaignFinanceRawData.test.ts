import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";

import {
  parseRefreshPennsylvaniaCampaignFinanceRawDataScriptArgs as parseArgs,
  runRefreshPennsylvaniaCampaignFinanceRawDataScript,
} from "../../src/scripts/refreshPennsylvaniaCampaignFinanceRawData.js";

const tempDirs: string[] = [];

async function makeTempDir(): Promise<string> {
  const dir = await mkdtemp(join(tmpdir(), "voteapp-pa-cf-script-"));
  tempDirs.push(dir);
  return dir;
}

afterEach(async () => {
  await Promise.all(tempDirs.splice(0).map((dir) => rm(dir, { recursive: true, force: true })));
});

describe("refreshPennsylvaniaCampaignFinanceRawData script", () => {
  it("parses Pennsylvania raw-data refresh options", () => {
    expect(
      parseArgs([
        "--year=2022",
        "--url=https://example.test/2022.zip",
        "--cache-dir=/cache",
        "--timeout-ms=5000",
        "--force",
      ])
    ).toEqual({
      year: 2022,
      url: "https://example.test/2022.zip",
      cacheDir: "/cache",
      timeoutMs: 5000,
      force: true,
    });
  });

  it("defaults to the current-year Pennsylvania export", () => {
    const year = new Date().getUTCFullYear();
    expect(parseArgs([])).toMatchObject({
      year,
      url:
        "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/resources/voting-and-elections/" +
        `campaign-finance/campaign-finance-data/${year}.zip`,
      force: false,
      timeoutMs: 900_000,
    });
    expect(parseArgs([]).cacheDir).toContain("scratch/pennsylvania-campaign-finance/exports");
  });

  it("rejects malformed values", () => {
    expect(() => parseArgs(["--timeout-ms=5x"])).toThrow("Invalid --timeout-ms value: 5x");
    expect(() => parseArgs(["--year=1999"])).toThrow("Invalid Pennsylvania campaign finance export year: 1999");
    expect(() => parseArgs(["--url=http://example.test/2022.zip"])).toThrow("Only https is allowed");
  });

  it("rejects missing or duplicate values", () => {
    expect(() => parseArgs(["--url"])).toThrow("Missing value for --url");
    expect(() => parseArgs(["--url="])).toThrow("Missing value for --url");
    expect(() => parseArgs(["--cache-dir", "   "])).toThrow("Missing value for --cache-dir");
    expect(() => parseArgs(["--year=2022", "--year=2024"])).toThrow("Provide --year at most once");
  });

  it("formats refresh output from a mocked Pennsylvania artifact response", async () => {
    const cacheDir = await makeTempDir();
    const extractArchive = async ({ extractedDir }: { extractedDir: string }) => {
      await mkdir(extractedDir, { recursive: true });
      await writeFile(join(extractedDir, "extracted.txt"), "ok", "utf8");
    };
    const fetchImpl = async (_url: string | URL | Request, init?: RequestInit) => {
      if (init?.method === "HEAD") {
        return new Response(null, {
          status: 200,
          headers: {
            "content-length": "7",
            "content-type": "application/zip",
            etag: '"pa-a"',
            "last-modified": "Sun, 21 Jun 2026 04:00:00 GMT",
          },
        });
      }
      return new Response("zipdata", {
        status: 200,
        headers: {
          "content-length": "7",
          "content-type": "application/zip",
          etag: '"pa-a"',
          "last-modified": "Sun, 21 Jun 2026 04:00:00 GMT",
        },
      });
    };

    const output = await runRefreshPennsylvaniaCampaignFinanceRawDataScript({
      options: parseArgs([
        "--year=2022",
        "--url=https://example.test/2022.zip",
        `--cache-dir=${cacheDir}`,
        "--force",
      ]),
      fetchImpl,
      extractArchive,
      now: new Date("2026-06-21T12:00:00.000Z"),
    });

    expect(output).toMatchObject({
      type: "pennsylvania_campaign_finance_raw_data_refresh",
      started_at: "2026-06-21T12:00:00.000Z",
      year: 2022,
      status: "downloaded",
      remote: {
        year: 2022,
        url: "https://example.test/2022.zip",
        etag: '"pa-a"',
      },
      current: {
        year: 2022,
        downloadedAt: "2026-06-21T12:00:00.000Z",
        bytesWritten: 7,
      },
    });
    expect(output.archive_path).toContain("2022.zip");
    expect(output.extracted_dir).toContain("2022");
    await expect(readFile(join(output.extracted_dir, "extracted.txt"), "utf8")).resolves.toBe("ok");
    expect(typeof output.ts).toBe("string");
  });
});
