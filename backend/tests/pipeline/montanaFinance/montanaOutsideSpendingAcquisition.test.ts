import { mkdtemp, readFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";

import { readMontanaCersOutsideSpendingArtifacts } from "../../../src/pipeline/montanaFinance/montanaCersArtifactCache.js";
import type { MontanaCersFetchFn } from "../../../src/pipeline/montanaFinance/montanaCersClient.js";
import { acquireMontanaCersOutsideSpendingArtifacts } from "../../../src/pipeline/montanaFinance/montanaOutsideSpendingAcquisition.js";

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

function json(body: unknown): Response {
  return new Response(JSON.stringify(body), { status: 200, headers: { "content-type": "application/json" } });
}

const REGISTRATION_BODY = {
  sEcho: 1,
  iTotalRecords: 1,
  iTotalDisplayRecords: 1,
  aaData: [
    {
      candidateId: 21020,
      personDTO: { lastName: "Bedey", firstName: "David", middleInitial: "F." },
      electionYear: "2026",
      officeTitle: "Senate District No. 43",
      officeCode: "236",
      partyDescr: "Republican",
      candidateStatusDescr: "Active",
      resCountyDescr: "Ravalli",
    },
  ],
};

const EMPTY_LIST = { sEcho: 1, iTotalRecords: 0, iTotalDisplayRecords: 0, aaData: [] };

async function buildFakeCers(options: { bounce?: boolean; wrongResultCount?: boolean } = {}) {
  const committeeBody = await fixture("ie-committee-results-sanitized.json");
  const transactionsBody = await fixture("ie-transactions-sanitized.json");
  const requests: string[] = [];
  // Server-side session state the real portal keeps: which entity the last
  // viewFinancialEntities POST selected.
  let viewedCommitteeId: string | null = null;

  const fetchImpl: MontanaCersFetchFn = (url, init) => {
    requests.push(`${init.method} ${new URL(url).pathname}`);
    const body = init.body ?? "";
    if (url.includes("/search/candidateSearch")) {
      return Promise.resolve(html("search"));
    }
    if (url.includes("/searchResults/searchFinancials")) {
      return Promise.resolve(html(options.bounce ? "search" : "searchResults"));
    }
    if (url.includes("/searchResults/listFinancialCommitteeResults")) {
      return Promise.resolve(new Response(committeeBody, { status: 200, headers: { "content-type": "application/json" } }));
    }
    if (url.includes("/searchResults/viewFinancialEntities")) {
      viewedCommitteeId = new URLSearchParams(body).get("committeeId");
      const resultCount = viewedCommitteeId === "100" ? (options.wrongResultCount ? 5 : 3) : 0;
      return Promise.resolve(json({ resultCount }));
    }
    if (url.includes("/searchResults/listViewFinancialEntityResults")) {
      return Promise.resolve(
        viewedCommitteeId === "100"
          ? new Response(transactionsBody, { status: 200, headers: { "content-type": "application/json" } })
          : json(EMPTY_LIST)
      );
    }
    if (url.includes("/searchResults/searchCandidates")) {
      return Promise.resolve(html("searchResults"));
    }
    if (url.includes("/searchResults/listCandidateResults")) {
      return Promise.resolve(json(REGISTRATION_BODY));
    }
    throw new Error(`Unexpected CERS request in test: ${url}`);
  };
  return { fetchImpl, requests };
}

const sessionBasics = { sleep: () => Promise.resolve(), spacingMs: 0 };

describe("acquireMontanaCersOutsideSpendingArtifacts", () => {
  it("sweeps every committee on a fresh session and stores a same-vintage bundle", async () => {
    const cacheDir = await mkdtemp(join(tmpdir(), "mt-cers-ie-"));
    const { fetchImpl, requests } = await buildFakeCers();
    const result = await acquireMontanaCersOutsideSpendingArtifacts({
      year: 2026,
      cacheDir,
      now: new Date("2026-08-28T00:00:00.000Z"),
      sessionOptions: { fetchImpl, ...sessionBasics },
    });
    expect(result).toEqual({ year: 2026, committeeCount: 2, transactionRowCount: 3, registrationRowCount: 1 });
    // One session seed for the committee list, one per committee (rows carry
    // no committee identity — the session's selection is the binding), one
    // for the registration list.
    expect(requests.filter((line) => line === "GET /CampaignTracker/public/search/candidateSearch")).toHaveLength(4);
    const bundle = await readMontanaCersOutsideSpendingArtifacts({ cacheDir, year: 2026 });
    expect(bundle.sweep.transactionsByCommitteeId.get(100)).toHaveLength(3);
    expect(bundle.sweep.transactionsByCommitteeId.get(200)).toEqual([]);
    expect(bundle.registrationRows).toHaveLength(1);
  });

  it("fails closed on a silent search bounce, storing nothing", async () => {
    const cacheDir = await mkdtemp(join(tmpdir(), "mt-cers-ie-"));
    const { fetchImpl } = await buildFakeCers({ bounce: true });
    await expect(
      acquireMontanaCersOutsideSpendingArtifacts({ year: 2026, cacheDir, sessionOptions: { fetchImpl, ...sessionBasics } })
    ).rejects.toThrow("silently bounced");
    await expect(readMontanaCersOutsideSpendingArtifacts({ cacheDir, year: 2026 })).rejects.toThrow(
      "Missing Montana CERS artifact"
    );
  });

  it("fails closed when a committee's rows disagree with viewFinancialEntities resultCount", async () => {
    const cacheDir = await mkdtemp(join(tmpdir(), "mt-cers-ie-"));
    const { fetchImpl } = await buildFakeCers({ wrongResultCount: true });
    await expect(
      acquireMontanaCersOutsideSpendingArtifacts({ year: 2026, cacheDir, sessionOptions: { fetchImpl, ...sessionBasics } })
    ).rejects.toThrow("viewFinancialEntities said 5");
    await expect(readMontanaCersOutsideSpendingArtifacts({ cacheDir, year: 2026 })).rejects.toThrow(
      "Missing Montana CERS artifact"
    );
  });
});
