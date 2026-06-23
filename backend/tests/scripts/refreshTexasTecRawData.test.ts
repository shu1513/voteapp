import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach } from "vitest";
import { describe, expect, it } from "vitest";

import {
  parseRefreshTexasTecRawDataScriptArgs as parseArgs,
  runRefreshTexasTecRawDataScript,
} from "../../src/scripts/refreshTexasTecRawData.js";

const tempDirs: string[] = [];

async function makeTempDir(): Promise<string> {
  const dir = await mkdtemp(join(tmpdir(), "voteapp-tx-tec-script-"));
  tempDirs.push(dir);
  return dir;
}

afterEach(async () => {
  await Promise.all(tempDirs.splice(0).map((dir) => rm(dir, { recursive: true, force: true })));
});

describe("refreshTexasTecRawData script", () => {
  it("parses TEC raw-data refresh options", () => {
    expect(
      parseArgs([
        "--url=https://example.test/TEC_CF_CSV.zip",
        "--cache-dir=/cache",
        "--timeout-ms=5000",
        "--force",
      ])
    ).toEqual({
      url: "https://example.test/TEC_CF_CSV.zip",
      cacheDir: "/cache",
      timeoutMs: 5000,
      force: true,
    });
  });

  it("defaults to the official TEC CSV database ZIP", () => {
    expect(parseArgs([])).toMatchObject({
      url: "https://prd.tecprd.ethicsefile.com/public/cf/public/TEC_CF_CSV.zip",
      force: false,
      timeoutMs: 30_000,
    });
    expect(parseArgs([]).cacheDir).toContain("scratch/texas-campaign-finance/tec");
  });

  it("rejects malformed values", () => {
    expect(() => parseArgs(["--timeout-ms=5x"])).toThrow("Invalid --timeout-ms value: 5x");
    expect(() => parseArgs(["--url=http://example.test/TEC_CF_CSV.zip"])).toThrow("Only https is allowed");
  });

  it("rejects missing or duplicate values", () => {
    expect(() => parseArgs(["--url"])).toThrow("Missing value for --url");
    expect(() => parseArgs(["--url="])).toThrow("Missing value for --url");
    expect(() => parseArgs(["--cache-dir", "   "])).toThrow("Missing value for --cache-dir");
    expect(() => parseArgs(["--url=https://example.test/a.zip", "--url=https://example.test/b.zip"])).toThrow(
      "Provide --url at most once"
    );
  });

  it("formats refresh output from a mocked TEC artifact response", async () => {
    const cacheDir = await makeTempDir();
    const fetchImpl = async (_url: string | URL | Request, init?: RequestInit) => {
      if (init?.method === "HEAD") {
        return new Response(null, {
          status: 200,
          headers: {
            "content-length": "8",
            "content-type": "application/zip",
            etag: "\"tx-a\"",
            "last-modified": "Sun, 21 Jun 2026 04:00:00 GMT",
          },
        });
      }
      return new Response("zip-data", {
        status: 200,
        headers: {
          "content-length": "8",
          "content-type": "application/zip",
          etag: "\"tx-a\"",
          "last-modified": "Sun, 21 Jun 2026 04:00:00 GMT",
        },
      });
    };

    const output = await runRefreshTexasTecRawDataScript({
      options: parseArgs([
        "--url=https://example.test/TEC_CF_CSV.zip",
        `--cache-dir=${cacheDir}`,
        "--force",
      ]),
      fetchImpl,
      now: new Date("2026-06-21T12:00:00.000Z"),
    });

    expect(output).toMatchObject({
      type: "texas_tec_raw_data_refresh",
      started_at: "2026-06-21T12:00:00.000Z",
      status: "downloaded",
      remote: {
        url: "https://example.test/TEC_CF_CSV.zip",
        etag: "\"tx-a\"",
      },
      current: {
        downloadedAt: "2026-06-21T12:00:00.000Z",
        bytesWritten: 8,
      },
    });
    expect(typeof output.ts).toBe("string");
  });
});
