import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";

import {
  buildNewYorkCityCfbArtifactUrl,
  refreshNewYorkCityCfbArtifact,
} from "../../../src/pipeline/newYorkCityFinance/newYorkCityCfbArtifactCache.js";

const tempDirs: string[] = [];
afterEach(async () => {
  await Promise.all(tempDirs.splice(0).map((path) => rm(path, { recursive: true, force: true })));
});

describe("newYorkCityCfbArtifactCache", () => {
  it("builds official stable URLs", () => {
    expect(buildNewYorkCityCfbArtifactUrl({ electionYear: 2025, kind: "contributions" }))
      .toBe("https://www.nyccfb.info/datalibrary/2025_Contributions.csv");
    expect(buildNewYorkCityCfbArtifactUrl({ electionYear: 2025, kind: "financial_analysis" }))
      .toBe("https://www.nyccfb.info/datalibrary/EC2025_FinancialAnalysis.csv");
  });

  it("downloads atomically, then uses conditional request", async () => {
    const cacheDir = await mkdtemp(join(tmpdir(), "nyc-cfb-cache-"));
    tempDirs.push(cacheDir);
    const csv = "ELECTION,OFFICECD,RECIPID\n2025,1,123\n";
    const fetchImpl = vi.fn<typeof fetch>()
      .mockResolvedValueOnce(new Response(csv, { status: 200, headers: { etag: '"v1"', "content-type": "text/csv" } }))
      .mockResolvedValueOnce(new Response(null, { status: 304 }));

    const first = await refreshNewYorkCityCfbArtifact({ cacheDir, electionYear: 2025, kind: "contributions", fetchImpl });
    expect(first.status).toBe("downloaded");
    if (first.status !== "downloaded") throw new Error("expected download");
    expect(await readFile(first.current.filePath, "utf8")).toBe(csv);

    const second = await refreshNewYorkCityCfbArtifact({ cacheDir, electionYear: 2025, kind: "contributions", fetchImpl });
    expect(second.status).toBe("unchanged");
    expect(new Headers(fetchImpl.mock.calls[1]?.[1]?.headers).get("if-none-match")).toBe('"v1"');
  });

  it("reports unpublished future artifacts without throwing", async () => {
    const cacheDir = await mkdtemp(join(tmpdir(), "nyc-cfb-cache-"));
    tempDirs.push(cacheDir);
    const result = await refreshNewYorkCityCfbArtifact({
      cacheDir, electionYear: 2029, kind: "financial_analysis", fetchImpl: vi.fn<typeof fetch>().mockResolvedValue(new Response(null, { status: 404 })),
    });
    expect(result.status).toBe("not_yet_published");
    if (result.status === "not_yet_published") {
      expect(Date.parse(result.nextCheckAt)).toBeGreaterThan(Date.parse(result.checkedAt));
    }
  });

  it("fails loudly when an expected published-cycle artifact returns 404", async () => {
    const cacheDir = await mkdtemp(join(tmpdir(), "nyc-cfb-cache-"));
    tempDirs.push(cacheDir);
    await expect(refreshNewYorkCityCfbArtifact({
      cacheDir, electionYear: 2025, kind: "contributions",
      fetchImpl: vi.fn<typeof fetch>().mockResolvedValue(new Response(null, { status: 404 })),
    })).rejects.toThrow("artifact missing for published election year 2025");
  });

  it("rejects HTML challenge bodies", async () => {
    const cacheDir = await mkdtemp(join(tmpdir(), "nyc-cfb-cache-"));
    tempDirs.push(cacheDir);
    await expect(refreshNewYorkCityCfbArtifact({
      cacheDir, electionYear: 2025, kind: "contributions",
      fetchImpl: vi.fn<typeof fetch>().mockResolvedValue(new Response("<!doctype html><title>Blocked</title>", { status: 200 })),
    })).rejects.toThrow("HTML, not CSV");
  });

  it("accepts a quoted header containing a comma", async () => {
    const cacheDir = await mkdtemp(join(tmpdir(), "nyc-cfb-cache-"));
    tempDirs.push(cacheDir);
    const csv = '"IGNORED,HEADER",RECIPID\nvalue,123\n';
    const result = await refreshNewYorkCityCfbArtifact({
      cacheDir, electionYear: 2025, kind: "contributions",
      fetchImpl: vi.fn<typeof fetch>().mockResolvedValue(new Response(csv, { status: 200 })),
    });
    expect(result.status).toBe("downloaded");
  });

  it("applies the request timeout while streaming the response body", async () => {
    const cacheDir = await mkdtemp(join(tmpdir(), "nyc-cfb-cache-"));
    tempDirs.push(cacheDir);
    const body = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(new TextEncoder().encode("ELECTION,OFFICECD,RECIPID\n"));
      },
    });
    await expect(refreshNewYorkCityCfbArtifact({
      cacheDir, electionYear: 2025, kind: "contributions", timeoutMs: 20,
      fetchImpl: vi.fn<typeof fetch>().mockResolvedValue(new Response(body, { status: 200 })),
    })).rejects.toThrow("request timed out after 20ms");
  });
});
