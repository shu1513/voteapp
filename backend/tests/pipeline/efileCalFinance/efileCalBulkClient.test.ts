import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";

import { afterEach, describe, expect, it, vi } from "vitest";

import {
  downloadEfileCalWorkbook,
  type EfileCalAgencyConfig,
  fetchEfileCalBulkExportUrl,
  getEfileCalWorkbookArtifactCachePaths,
  readEfileCalWorkbookArtifactCacheMetadata,
  refreshEfileCalWorkbookArtifactCache,
} from "../../../src/pipeline/efileCalFinance/efileCalBulkClient.js";
import { buildEfileCalExportWorkbook } from "./efileCalExportFixture.js";

const CONFIG: EfileCalAgencyConfig = {
  agencyKey: "csj",
  portalBaseUrl: "https://efile.example.gov",
  allowedExportHosts: ["exports.example-s3.test"],
};

const EXPORT_URL = "https://exports.example-s3.test/csj/export/City_CAL_2026_most_recent.xlsx";

let tempDirs: string[] = [];

async function tempDir(): Promise<string> {
  const dir = await mkdtemp(path.join(tmpdir(), "efile-cal-cache-"));
  tempDirs.push(dir);
  return dir;
}

function exportUrlResponse(url: string = EXPORT_URL): Response {
  return new Response(JSON.stringify({ success: true, data: url }), {
    status: 200,
    headers: { "content-type": "application/json" },
  });
}

function workbookResponse(data: Uint8Array, headers: Record<string, string> = {}): Response {
  return new Response(Buffer.from(data), {
    status: 200,
    headers: {
      "content-type": "binary/octet-stream",
      "content-length": String(data.byteLength),
      etag: '"v1"',
      "last-modified": "Mon, 10 Aug 2026 16:24:28 GMT",
      ...headers,
    },
  });
}

/** Routes portal + S3 requests the way the live services answer them. */
function routedFetch(input: { workbook: Uint8Array; etag?: string; onRequest?: (url: string, method: string) => void }) {
  return vi.fn(async (url: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
    const target = String(url);
    const method = init?.method ?? "GET";
    input.onRequest?.(target, method);
    if (target.startsWith("https://efile.example.gov/")) {
      return exportUrlResponse();
    }
    const headers = input.etag ? { etag: input.etag } : {};
    if (method === "HEAD") {
      return new Response(null, {
        status: 200,
        headers: {
          "content-length": String(input.workbook.byteLength),
          "content-type": "binary/octet-stream",
          etag: '"v1"',
          "last-modified": "Mon, 10 Aug 2026 16:24:28 GMT",
          ...headers,
        },
      });
    }
    return workbookResponse(input.workbook, headers);
  }) as unknown as typeof fetch;
}

describe("efileCalBulkClient", () => {
  afterEach(async () => {
    const dirs = tempDirs;
    tempDirs = [];
    await Promise.all(dirs.map((dir) => rm(dir, { recursive: true, force: true })));
    vi.restoreAllMocks();
  });

  it("resolves the bulk export URL with the documented query parameters", async () => {
    const seen: string[] = [];
    const fetchImpl = vi.fn(async (url: RequestInfo | URL): Promise<Response> => {
      seen.push(String(url));
      return exportUrlResponse();
    }) as unknown as typeof fetch;

    await expect(
      fetchEfileCalBulkExportUrl(CONFIG, { year: 2026, mostRecentOnly: true }, { fetchImpl })
    ).resolves.toBe(EXPORT_URL);
    expect(seen).toEqual([
      "https://efile.example.gov/api/v1/public/campaign-bulk-export-url?year=2026&most_recent_only=true",
    ]);
  });

  it("refuses export URLs outside the host allowlist and non-https portals", async () => {
    const offAllowlist = vi.fn(async () =>
      exportUrlResponse("https://evil.example.test/export.xlsx")
    ) as unknown as typeof fetch;
    await expect(
      fetchEfileCalBulkExportUrl(CONFIG, { year: 2026, mostRecentOnly: true }, { fetchImpl: offAllowlist })
    ).rejects.toThrow("is not in the allowlist");

    await expect(
      fetchEfileCalBulkExportUrl(
        { ...CONFIG, portalBaseUrl: "http://efile.example.gov" },
        { year: 2026, mostRecentOnly: true },
        { fetchImpl: offAllowlist }
      )
    ).rejects.toThrow("Only https is allowed");
  });

  it("fails closed on malformed export-url responses", async () => {
    const notSuccess = vi.fn(async () =>
      new Response(JSON.stringify({ success: false, message: "down" }), { status: 200 })
    ) as unknown as typeof fetch;
    await expect(
      fetchEfileCalBulkExportUrl(CONFIG, { year: 2026, mostRecentOnly: true }, { fetchImpl: notSuccess })
    ).rejects.toThrow("response is malformed");

    const notJson = vi.fn(async () => new Response("<html>err</html>", { status: 200 })) as unknown as typeof fetch;
    await expect(
      fetchEfileCalBulkExportUrl(CONFIG, { year: 2026, mostRecentOnly: true }, { fetchImpl: notJson })
    ).rejects.toThrow("response is not JSON");
  });

  it("downloads a workbook, enforcing the size cap and the XLSX signature", async () => {
    const workbook = buildEfileCalExportWorkbook();
    const okFetch = vi.fn(async () => workbookResponse(workbook)) as unknown as typeof fetch;
    const downloaded = await downloadEfileCalWorkbook(EXPORT_URL, CONFIG, { fetchImpl: okFetch });
    expect(downloaded.data).toEqual(workbook);
    expect(downloaded.remote.etag).toBe('"v1"');

    const declaredTooBig = vi.fn(async () =>
      workbookResponse(workbook, { "content-length": "999999999" })
    ) as unknown as typeof fetch;
    await expect(
      downloadEfileCalWorkbook(EXPORT_URL, CONFIG, { fetchImpl: declaredTooBig })
    ).rejects.toThrow("over the");

    const lyingLength = vi.fn(async () =>
      workbookResponse(workbook, { "content-length": "10" })
    ) as unknown as typeof fetch;
    await expect(
      downloadEfileCalWorkbook(EXPORT_URL, CONFIG, { fetchImpl: lyingLength, maxBytes: 10 })
    ).rejects.toThrow("exceeded the 10-byte cap while streaming");

    const htmlBody = vi.fn(async () =>
      workbookResponse(new TextEncoder().encode("<html>maintenance</html>"))
    ) as unknown as typeof fetch;
    await expect(downloadEfileCalWorkbook(EXPORT_URL, CONFIG, { fetchImpl: htmlBody })).rejects.toThrow(
      "is not an XLSX file"
    );

    await expect(
      downloadEfileCalWorkbook("https://evil.example.test/export.xlsx", CONFIG, { fetchImpl: okFetch })
    ).rejects.toThrow("is not in the allowlist");
  });

  it("downloads on first refresh, skips when remote metadata is unchanged, and re-downloads on change or force", async () => {
    const cacheDir = await tempDir();
    const workbook = buildEfileCalExportWorkbook();
    const requests: string[] = [];
    const fetchImpl = routedFetch({ workbook, onRequest: (url, method) => requests.push(`${method} ${url}`) });

    const first = await refreshEfileCalWorkbookArtifactCache({
      config: CONFIG,
      year: 2026,
      mostRecentOnly: true,
      cacheDir,
      fetchImpl,
    });
    expect(first.status).toBe("downloaded");
    expect(first.current.bytesWritten).toBe(workbook.byteLength);
    const paths = getEfileCalWorkbookArtifactCachePaths({
      cacheDir,
      agencyKey: "csj",
      year: 2026,
      mostRecentOnly: true,
    });
    expect(first.workbookPath).toBe(paths.workbookPath);
    expect(new Uint8Array(await readFile(paths.workbookPath))).toEqual(workbook);
    await expect(readEfileCalWorkbookArtifactCacheMetadata(paths.metadataPath)).resolves.toMatchObject({
      version: 1,
      agencyKey: "csj",
      year: 2026,
      mostRecentOnly: true,
      remote: { etag: '"v1"' },
    });

    const second = await refreshEfileCalWorkbookArtifactCache({
      config: CONFIG,
      year: 2026,
      mostRecentOnly: true,
      cacheDir,
      fetchImpl,
    });
    expect(second.status).toBe("unchanged");
    // Unchanged refreshes must never GET the workbook again.
    expect(requests.filter((entry) => entry.startsWith("GET https://exports."))).toHaveLength(1);

    const changed = await refreshEfileCalWorkbookArtifactCache({
      config: CONFIG,
      year: 2026,
      mostRecentOnly: true,
      cacheDir,
      fetchImpl: routedFetch({ workbook, etag: '"v2"' }),
    });
    expect(changed.status).toBe("downloaded");
    expect(changed.current.remote.etag).toBe('"v2"');

    const forced = await refreshEfileCalWorkbookArtifactCache({
      config: CONFIG,
      year: 2026,
      mostRecentOnly: true,
      cacheDir,
      fetchImpl: routedFetch({ workbook, etag: '"v2"' }),
      force: true,
    });
    expect(forced.status).toBe("downloaded");
  });
});
