import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";

import { afterEach, describe, expect, it, vi } from "vitest";

import {
  CAL_ACCESS_RAW_DATA_CACHE_METADATA_FILE_NAME,
  CAL_ACCESS_RAW_DATA_CACHE_ZIP_FILE_NAME,
  CAL_ACCESS_RAW_DATA_ZIP_URL,
} from "../../src/pipeline/californiaFinance/calAccessRawDataArtifactCache.js";
import {
  DEFAULT_CAL_ACCESS_RAW_DATA_CACHE_DIR,
  parseRefreshCaliforniaCampaignFinanceRawDataScriptArgs,
  runRefreshCaliforniaCampaignFinanceRawDataScript,
} from "../../src/scripts/refreshCaliforniaCampaignFinanceRawData.js";

let tempDirs: string[] = [];

async function createTempDir(prefix = "calaccess-refresh-"): Promise<string> {
  const dir = await mkdtemp(path.join(tmpdir(), prefix));
  tempDirs.push(dir);
  return dir;
}

function zipResponse(body: Buffer, init: ResponseInit = {}): Response {
  return new Response(body, {
    status: 200,
    statusText: "OK",
    headers: {
      "content-type": "application/x-zip-compressed",
      "content-length": String(body.length),
      etag: '"etag-1"',
      "last-modified": "Thu, 18 Jun 2026 08:40:47 GMT",
    },
    ...init,
  });
}

describe("refreshCaliforniaCampaignFinanceRawData script", () => {
  afterEach(async () => {
    vi.restoreAllMocks();
    const dirs = tempDirs;
    tempDirs = [];
    await Promise.all(dirs.map((dir) => rm(dir, { recursive: true, force: true })));
  });

  it("parses default refresh options", () => {
    expect(parseRefreshCaliforniaCampaignFinanceRawDataScriptArgs([])).toEqual({
      url: CAL_ACCESS_RAW_DATA_ZIP_URL,
      cacheDir: path.resolve(DEFAULT_CAL_ACCESS_RAW_DATA_CACHE_DIR),
      force: false,
      validateManifest: false,
      timeoutMs: 30_000,
    });
  });

  it("rejects non-HTTPS URLs and malformed timeout values", () => {
    expect(() => parseRefreshCaliforniaCampaignFinanceRawDataScriptArgs(["--url=http://example.test/db.zip"])).toThrow(
      "Invalid --url protocol: http:. Only https is allowed."
    );
    expect(() => parseRefreshCaliforniaCampaignFinanceRawDataScriptArgs(["--timeout-ms=10abc"])).toThrow(
      "Invalid --timeout-ms value: 10abc"
    );
    expect(() => parseRefreshCaliforniaCampaignFinanceRawDataScriptArgs(["--url", "--force"])).toThrow(
      "Missing --url value"
    );
  });

  it("downloads a changed artifact into the cache and writes metadata", async () => {
    const cacheDir = await createTempDir();
    const zip = Buffer.from("fake zip");
    const fetchImpl = vi
      .fn()
      .mockResolvedValueOnce(zipResponse(Buffer.alloc(0)))
      .mockResolvedValueOnce(zipResponse(zip)) as unknown as typeof fetch;

    const output = await runRefreshCaliforniaCampaignFinanceRawDataScript({
      options: parseRefreshCaliforniaCampaignFinanceRawDataScriptArgs([
        `--cache-dir=${cacheDir}`,
        "--url=https://example.test/dbwebexport.zip",
      ]),
      fetchImpl,
      now: new Date("2026-06-19T12:00:00.000Z"),
    });

    expect(output).toMatchObject({
      type: "cal_access_raw_data_refresh",
      status: "downloaded",
      cache_dir: cacheDir,
      zip_path: path.join(cacheDir, CAL_ACCESS_RAW_DATA_CACHE_ZIP_FILE_NAME),
      metadata_path: path.join(cacheDir, CAL_ACCESS_RAW_DATA_CACHE_METADATA_FILE_NAME),
      previous: null,
      current: {
        downloadedAt: "2026-06-19T12:00:00.000Z",
        bytesWritten: zip.length,
      },
    });
    expect(fetchImpl).toHaveBeenCalledTimes(2);
    await expect(readFile(path.join(cacheDir, CAL_ACCESS_RAW_DATA_CACHE_ZIP_FILE_NAME), "utf8")).resolves.toBe("fake zip");

    const metadata = JSON.parse(
      await readFile(path.join(cacheDir, CAL_ACCESS_RAW_DATA_CACHE_METADATA_FILE_NAME), "utf8")
    );
    expect(metadata).toMatchObject({
      version: 1,
      downloadedAt: "2026-06-19T12:00:00.000Z",
      remote: {
        url: "https://example.test/dbwebexport.zip",
        etag: '"etag-1"',
      },
      bytesWritten: zip.length,
    });
  });

  it("skips download when cached metadata still matches the remote ETag", async () => {
    const cacheDir = await createTempDir();
    const zipPath = path.join(cacheDir, CAL_ACCESS_RAW_DATA_CACHE_ZIP_FILE_NAME);
    const metadataPath = path.join(cacheDir, CAL_ACCESS_RAW_DATA_CACHE_METADATA_FILE_NAME);
    await writeFile(zipPath, "existing zip", "utf8");
    await writeFile(
      metadataPath,
      JSON.stringify({
        version: 1,
        zipPath,
        metadataPath,
        downloadedAt: "2026-06-18T12:00:00.000Z",
        remote: {
          url: "https://example.test/dbwebexport.zip",
          contentLength: 8,
          contentType: "application/x-zip-compressed",
          etag: '"etag-1"',
          lastModified: "Thu, 18 Jun 2026 08:40:47 GMT",
        },
        bytesWritten: 12,
      }),
      "utf8"
    );
    const fetchImpl = vi.fn().mockResolvedValue(zipResponse(Buffer.alloc(0))) as unknown as typeof fetch;

    const output = await runRefreshCaliforniaCampaignFinanceRawDataScript({
      options: parseRefreshCaliforniaCampaignFinanceRawDataScriptArgs([
        `--cache-dir=${cacheDir}`,
        "--url=https://example.test/dbwebexport.zip",
      ]),
      fetchImpl,
      now: new Date("2026-06-19T12:00:00.000Z"),
    });

    expect(output).toMatchObject({
      status: "unchanged",
      previous: {
        downloadedAt: "2026-06-18T12:00:00.000Z",
      },
      current: {
        downloadedAt: "2026-06-18T12:00:00.000Z",
      },
    });
    expect(fetchImpl).toHaveBeenCalledTimes(1);
    await expect(readFile(zipPath, "utf8")).resolves.toBe("existing zip");
  });

  it("force-refreshes even when cached metadata matches", async () => {
    const cacheDir = await createTempDir();
    const zipPath = path.join(cacheDir, CAL_ACCESS_RAW_DATA_CACHE_ZIP_FILE_NAME);
    const metadataPath = path.join(cacheDir, CAL_ACCESS_RAW_DATA_CACHE_METADATA_FILE_NAME);
    await writeFile(zipPath, "old zip", "utf8");
    await writeFile(
      metadataPath,
      JSON.stringify({
        version: 1,
        zipPath,
        metadataPath,
        downloadedAt: "2026-06-18T12:00:00.000Z",
        remote: {
          url: "https://example.test/dbwebexport.zip",
          contentLength: 7,
          contentType: "application/x-zip-compressed",
          etag: '"etag-1"',
          lastModified: "Thu, 18 Jun 2026 08:40:47 GMT",
        },
        bytesWritten: 7,
      }),
      "utf8"
    );
    const fetchImpl = vi
      .fn()
      .mockResolvedValueOnce(zipResponse(Buffer.alloc(0)))
      .mockResolvedValueOnce(zipResponse(Buffer.from("new zip"))) as unknown as typeof fetch;

    const output = await runRefreshCaliforniaCampaignFinanceRawDataScript({
      options: parseRefreshCaliforniaCampaignFinanceRawDataScriptArgs([
        `--cache-dir=${cacheDir}`,
        "--url=https://example.test/dbwebexport.zip",
        "--force",
      ]),
      fetchImpl,
      now: new Date("2026-06-19T12:00:00.000Z"),
    });

    expect(output.status).toBe("downloaded");
    expect(fetchImpl).toHaveBeenCalledTimes(2);
    await expect(readFile(zipPath, "utf8")).resolves.toBe("new zip");
  });
});
