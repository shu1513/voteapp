import { mkdtemp, readFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";

import { acquireMontanaCersCandidateFinanceArtifacts } from "../../../src/pipeline/montanaFinance/montanaCersArtifactAcquisition.js";
import { readMontanaCersCandidateFinanceArtifacts } from "../../../src/pipeline/montanaFinance/montanaCersArtifactCache.js";
import type { MontanaCersFetchFn } from "../../../src/pipeline/montanaFinance/montanaCersClient.js";

const fixtures = new URL("../../fixtures/montanaFinance/", import.meta.url);

async function fixture(name: string): Promise<string> {
  return readFile(new URL(name, fixtures), "utf8");
}

function html(marker: string): Response {
  return new Response(`<html><head><title>Campaign Electronic Reporting System (${marker})</title></head></html>`, {
    status: 200,
    headers: { "content-type": "text/html" },
  });
}

async function buildFakeCers(options: { bounceFinancialSearch?: boolean } = {}) {
  const inventoryBody = await fixture("report-inventory-sanitized.json");
  const detailFixture = JSON.parse(await fixture("report-detail-sanitized.json")) as {
    lists: Record<string, unknown[]>;
  };
  const contrBody = await fixture("contributions-export-sanitized.csv");
  const expendBody = await fixture("expenditures-export-sanitized.csv");
  let financialMode: "CONTR" | "EXPEND" | null = null;
  const requests: string[] = [];

  const fetchImpl: MontanaCersFetchFn = (url, init) => {
    const path = new URL(url).pathname + (new URL(url).search ? "?" : "");
    requests.push(`${init.method} ${path}`);
    const body = init.body ?? "";
    if (url.includes("/search/candidateSearch")) {
      return Promise.resolve(html("search"));
    }
    if (url.includes("/publicReportList/retrieveCampaignReports")) {
      return Promise.resolve(new Response(null, { status: 302, headers: { location: "/CampaignTracker/public/publicReportList" } }));
    }
    if (url.includes("/publicReportList/listFinanceReports")) {
      return Promise.resolve(new Response(inventoryBody, { status: 200, headers: { "content-type": "application/json" } }));
    }
    if (url.includes("/publicReportList")) {
      return Promise.resolve(html("publicReportList"));
    }
    if (url.includes("/viewFinanceReport/retrieveReport")) {
      return Promise.resolve(new Response(null, { status: 302, headers: { location: "/CampaignTracker/public/viewFinanceReport" } }));
    }
    if (url.includes("/viewFinanceReport/financeRepDetailList")) {
      const listName = new URLSearchParams(body).get("listName") ?? "";
      const rows = detailFixture.lists[listName] ?? [];
      return Promise.resolve(new Response(JSON.stringify(rows), { status: 200, headers: { "content-type": "application/json" } }));
    }
    if (url.includes("/searchResults/searchFinancials")) {
      financialMode = new URLSearchParams(body).get("financialSearchType") as "CONTR" | "EXPEND";
      return Promise.resolve(html(options.bounceFinancialSearch ? "search" : "searchResults"));
    }
    if (url.includes("/searchResults/prepareDownloadFile")) {
      return Promise.resolve(new Response(JSON.stringify({ fileName: `${financialMode}.csv` }), { status: 200, headers: { "content-type": "application/json" } }));
    }
    if (url.includes("/searchResults/downloadFile")) {
      return Promise.resolve(new Response(financialMode === "CONTR" ? contrBody : expendBody, { status: 200, headers: { "content-type": "text/plain" } }));
    }
    throw new Error(`Unexpected CERS request in test: ${url}`);
  };

  return { fetchImpl, requests };
}

const sessionBasics = { sleep: () => Promise.resolve(), spacingMs: 0 };

describe("acquireMontanaCersCandidateFinanceArtifacts", () => {
  it("harvests inventory, canonical details, and both exports into a same-vintage bundle", async () => {
    const cacheDir = await mkdtemp(join(tmpdir(), "mt-cers-acquire-"));
    const { fetchImpl } = await buildFakeCers();
    const result = await acquireMontanaCersCandidateFinanceArtifacts({
      candidateId: 21020,
      year: 2026,
      cacheDir,
      now: new Date("2026-08-28T00:00:00.000Z"),
      sessionOptions: { fetchImpl, ...sessionBasics },
    });
    expect(result).toMatchObject({
      candidateId: 21020,
      year: 2026,
      reportCount: 5,
      canonicalReportCount: 4,
      detailReportIds: [75674, 76083, 76535, 77491],
      contributionRowCount: 5,
      expenditureRowCount: 2,
    });
    // The stored bundle reads back as one vintage.
    const bundle = await readMontanaCersCandidateFinanceArtifacts({ cacheDir, candidateId: 21020, year: 2026 });
    expect(bundle.inventory).toHaveLength(5);
    expect(bundle.contributionRows).toHaveLength(5);
  });

  it("stores only the inventory when no canonical C5 exists", async () => {
    const cacheDir = await mkdtemp(join(tmpdir(), "mt-cers-acquire-"));
    const { fetchImpl, requests } = await buildFakeCers();
    const inventory = JSON.parse(await fixture("report-inventory-sanitized.json")) as {
      aaData: { formTypeCode: string; statusCode: string }[];
      iTotalRecords: number;
      iTotalDisplayRecords: number;
    };
    // Every row incorporated C7: registered entity with nothing countable.
    inventory.aaData = inventory.aaData.map((row) => ({ ...row, formTypeCode: "C7", statusCode: "INCRP" }));
    const inventoryOnlyFetch: MontanaCersFetchFn = (url, init) =>
      url.includes("/publicReportList/listFinanceReports")
        ? Promise.resolve(
            new Response(JSON.stringify(inventory), { status: 200, headers: { "content-type": "application/json" } })
          )
        : fetchImpl(url, init);
    const result = await acquireMontanaCersCandidateFinanceArtifacts({
      candidateId: 21020,
      year: 2026,
      cacheDir,
      sessionOptions: { fetchImpl: inventoryOnlyFetch, ...sessionBasics },
    });
    expect(result.canonicalReportCount).toBe(0);
    expect(requests.some((line) => line.includes("searchFinancials"))).toBe(false);
    await expect(
      readMontanaCersCandidateFinanceArtifacts({ cacheDir, candidateId: 21020, year: 2026 })
    ).rejects.toThrow("Missing Montana CERS artifact");
  });

  it("fails closed when the financial search silently bounces", async () => {
    const cacheDir = await mkdtemp(join(tmpdir(), "mt-cers-acquire-"));
    const { fetchImpl } = await buildFakeCers({ bounceFinancialSearch: true });
    await expect(
      acquireMontanaCersCandidateFinanceArtifacts({
        candidateId: 21020,
        year: 2026,
        cacheDir,
        sessionOptions: { fetchImpl, ...sessionBasics },
      })
    ).rejects.toThrow("silently bounced");
  });
});
