import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";

import {
  parseRefreshMinnesotaCampaignFinanceRawDataScriptArgs as parseArgs,
  runRefreshMinnesotaCampaignFinanceRawDataScript,
} from "../../src/scripts/refreshMinnesotaCampaignFinanceRawData.js";

const tempDirs: string[] = [];

async function makeTempDir(): Promise<string> {
  const dir = await mkdtemp(join(tmpdir(), "voteapp-mn-cf-script-"));
  tempDirs.push(dir);
  return dir;
}

afterEach(async () => {
  vi.unstubAllEnvs();
  await Promise.all(tempDirs.splice(0).map((dir) => rm(dir, { recursive: true, force: true })));
});

describe("refreshMinnesotaCampaignFinanceRawData script", () => {
  it("parses Minnesota raw-data refresh options", () => {
    expect(parseArgs(["--cache-dir=/cache", "--timeout-ms=5000", "--force"])).toEqual({
      cacheDir: "/cache",
      timeoutMs: 5000,
      force: true,
    });
  });

  it("defaults to the shared Minnesota cache", () => {
    vi.stubEnv("MINNESOTA_CAMPAIGN_FINANCE_CACHE_DIR", "");
    const options = parseArgs([]);

    expect(options).toMatchObject({
      force: false,
      timeoutMs: 900_000,
    });
    expect(options.cacheDir).toContain("scratch/minnesota-campaign-finance");
  });

  it("rejects malformed and unknown arguments", () => {
    expect(() => parseArgs(["--timeout-ms=5x"])).toThrow("Invalid --timeout-ms value: 5x");
    expect(() => parseArgs(["--timeout-ms=900001"])).toThrow("Invalid --timeout-ms value: 900001");
    expect(() => parseArgs([`--timeout-ms=${Number.MAX_SAFE_INTEGER}0`])).toThrow(
      `Invalid --timeout-ms value: ${Number.MAX_SAFE_INTEGER}0`
    );
    expect(() => parseArgs(["--cache-dir"])).toThrow("Missing value for --cache-dir");
    expect(() => parseArgs(["--cache-dir=/a", "--cache-dir=/b"])).toThrow("Provide --cache-dir at most once");
    expect(() => parseArgs(["--bogus"])).toThrow("Unknown Minnesota campaign finance raw data refresh flag");
    expect(() => parseArgs(["extra"])).toThrow("Unexpected Minnesota campaign finance raw data refresh argument");
  });

  it("refreshes all three Minnesota artifacts with mocked network data", async () => {
    const cacheDir = await makeTempDir();
    const fetchImpl = async (urlInput: string | URL | Request) => {
      const url = String(urlInput);
      if (url.endsWith("/campaign-finance/")) {
        return new Response(
          [
            '<table><tr><td>All</td><td>Contributions received by all entities - 2015 to present</td><td><a href="/contrib.csv">Download</a></td></tr>',
            '<tr><td>All</td><td>Independent expenditures by all entities - 2015 to present</td><td><a href="/ie.csv">Download</a></td></tr>',
            '<tr><td>Independent expenditure committees and funds</td><td>Contributions received by independent expenditure committees and funds only - 2015 to present</td><td><a href="/ie-contrib.csv">Download</a></td></tr></table>',
          ].join(""),
          { headers: { "content-type": "text/html" } }
        );
      }
      const body = url.endsWith("/contrib.csv") ? "contrib" : url.endsWith("/ie.csv") ? "ie" : "ie-contrib";
      return new Response(body, {
        headers: {
          "content-length": String(body.length),
          "content-type": "text/csv",
        },
      });
    };

    const output = await runRefreshMinnesotaCampaignFinanceRawDataScript({
      options: parseArgs([`--cache-dir=${cacheDir}`, "--force"]),
      fetchImpl,
      now: new Date("2026-07-24T12:00:00.000Z"),
    });

    expect(output).toMatchObject({
      type: "minnesota_campaign_finance_raw_data_refresh",
      started_at: "2026-07-24T12:00:00.000Z",
      cache_dir: cacheDir,
      status: "downloaded",
    });
    await expect(readFile(join(cacheDir, "all_contributions_received.csv"), "utf8")).resolves.toBe("contrib");
    await expect(readFile(join(cacheDir, "all_independent_expenditures.csv"), "utf8")).resolves.toBe("ie");
    await expect(readFile(join(cacheDir, "ie_committee_contributions.csv"), "utf8")).resolves.toBe("ie-contrib");
  });
});
