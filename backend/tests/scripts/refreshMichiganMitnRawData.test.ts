import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach } from "vitest";
import { describe, expect, it } from "vitest";

import {
  parseRefreshMichiganMitnRawDataScriptArgs as parseArgs,
  runRefreshMichiganMitnRawDataScript,
} from "../../src/scripts/refreshMichiganMitnRawData.js";

const tempDirs: string[] = [];

async function makeTempDir(): Promise<string> {
  const dir = await mkdtemp(join(tmpdir(), "voteapp-mi-mitn-script-"));
  tempDirs.push(dir);
  return dir;
}

afterEach(async () => {
  await Promise.all(tempDirs.splice(0).map((dir) => rm(dir, { recursive: true, force: true })));
});

describe("refreshMichiganMitnRawData script", () => {
  it("parses MiTN raw-data refresh options", () => {
    expect(
      parseArgs([
        "--year=2022",
        "--url=https://example.test/2022_mi_cfr.7z",
        "--cache-dir=/cache",
        "--timeout-ms=5000",
        "--force",
      ])
    ).toEqual({
      year: 2022,
      url: "https://example.test/2022_mi_cfr.7z",
      cacheDir: "/cache",
      timeoutMs: 5000,
      force: true,
    });
  });

  it("defaults to the latest supported MiTN legacy archive", () => {
    expect(parseArgs([])).toMatchObject({
      year: 2025,
      url: "https://www.michigan.gov/sos/-/media/Project/Websites/sos/Elections/Disclosure/MiTN/Legacy-Data/2025_mi_cfr.7z",
      force: false,
      timeoutMs: 900_000,
    });
    expect(parseArgs([]).cacheDir).toContain("scratch/michigan-campaign-finance/mitn");
  });

  it("rejects malformed values", () => {
    expect(() => parseArgs(["--timeout-ms=5x"])).toThrow("Invalid --timeout-ms value: 5x");
    expect(() => parseArgs(["--year=2019"])).toThrow("Invalid Michigan MiTN legacy archive year: 2019");
    expect(() => parseArgs(["--url=http://example.test/2022_mi_cfr.7z"])).toThrow("Only https is allowed");
  });

  it("rejects missing or duplicate values", () => {
    expect(() => parseArgs(["--url"])).toThrow("Missing value for --url");
    expect(() => parseArgs(["--url="])).toThrow("Missing value for --url");
    expect(() => parseArgs(["--cache-dir", "   "])).toThrow("Missing value for --cache-dir");
    expect(() => parseArgs(["--year=2022", "--year=2024"])).toThrow("Provide --year at most once");
  });

  it("formats refresh output from a mocked MiTN artifact response", async () => {
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
            "content-type": "application/x-7z-compressed",
            etag: '"mi-a"',
            "last-modified": "Sun, 21 Jun 2026 04:00:00 GMT",
          },
        });
      }
      return new Response("7z-data", {
        status: 200,
        headers: {
          "content-length": "7",
          "content-type": "application/x-7z-compressed",
          etag: '"mi-a"',
          "last-modified": "Sun, 21 Jun 2026 04:00:00 GMT",
        },
      });
    };

    const output = await runRefreshMichiganMitnRawDataScript({
      options: parseArgs([
        "--year=2022",
        "--url=https://example.test/2022_mi_cfr.7z",
        `--cache-dir=${cacheDir}`,
        "--force",
      ]),
      fetchImpl,
      extractArchive,
      now: new Date("2026-06-21T12:00:00.000Z"),
    });

    expect(output).toMatchObject({
      type: "michigan_mitn_raw_data_refresh",
      started_at: "2026-06-21T12:00:00.000Z",
      year: 2022,
      status: "downloaded",
      remote: {
        year: 2022,
        url: "https://example.test/2022_mi_cfr.7z",
        etag: '"mi-a"',
      },
      current: {
        year: 2022,
        downloadedAt: "2026-06-21T12:00:00.000Z",
        bytesWritten: 7,
      },
    });
    expect(output.archive_path).toContain("2022_mi_cfr.7z");
    expect(output.extracted_dir).toContain("2022_mi_cfr");
    await expect(readFile(join(output.extracted_dir, "extracted.txt"), "utf8")).resolves.toBe("ok");
    expect(typeof output.ts).toBe("string");
  });
});
