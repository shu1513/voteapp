import { describe, expect, it } from "vitest";

import {
  MontanaCersClientError,
  assertMontanaCersPageTitle,
  buildMontanaCersDataTablesQuery,
  buildMontanaCersUrl,
  createMontanaCersSession,
  extractMontanaCersPageTitleMarker,
  type MontanaCersFetchFn,
} from "../../../src/pipeline/montanaFinance/montanaCersClient.js";

function jsonResponse(body: string, init: { status?: number; headers?: Record<string, string> } = {}): Response {
  return new Response(body, {
    status: init.status ?? 200,
    headers: { "content-type": "application/json", ...init.headers },
  });
}

describe("montanaCersClient URLs", () => {
  it("builds public URLs with the required DataTables params", () => {
    const url = buildMontanaCersUrl("searchResults/listCandidateResults", buildMontanaCersDataTablesQuery());
    expect(url).toContain("https://cers-ext.mt.gov/CampaignTracker/public/searchResults/listCandidateResults");
    // iSortCol_0 is load-bearing: the server 500s without it.
    expect(url).toContain("iSortCol_0=1");
    expect(url).toContain("sSortDir_0=asc");
  });

  it("refuses non-CERS URLs", async () => {
    const session = createMontanaCersSession({ fetchImpl: () => Promise.resolve(jsonResponse("{}")) });
    await expect(session.get("https://example.com/CampaignTracker/public/x")).rejects.toMatchObject({
      code: "invalid_request",
    });
  });
});

describe("montanaCersClient title markers", () => {
  const searchResults = "<html><head><title>Campaign Electronic Reporting System (searchResults)</title></head></html>";
  const bounced = "<html><head><title>Campaign Electronic Reporting System (search) </title></head></html>";

  it("extracts the parenthesized page marker", () => {
    expect(extractMontanaCersPageTitleMarker(searchResults)).toBe("searchResults");
    expect(extractMontanaCersPageTitleMarker(bounced)).toBe("search");
    expect(extractMontanaCersPageTitleMarker("<html>no title</html>")).toBeNull();
  });

  it("fails closed on the silent validation bounce", () => {
    expect(() => assertMontanaCersPageTitle(searchResults, "searchResults", "CONTR search")).not.toThrow();
    expect(() => assertMontanaCersPageTitle(bounced, "searchResults", "CONTR search")).toThrow(
      "silently bounced"
    );
    try {
      assertMontanaCersPageTitle(bounced, "searchResults", "CONTR search");
    } catch (error) {
      expect((error as MontanaCersClientError).code).toBe("validation_bounce");
    }
  });
});

describe("montanaCersClient session", () => {
  it("keeps the JSESSIONID cookie and spaces sequential requests", async () => {
    const seenCookies: (string | undefined)[] = [];
    const sleeps: number[] = [];
    let calls = 0;
    const fetchImpl: MontanaCersFetchFn = (url, init) => {
      seenCookies.push(init.headers.Cookie);
      calls += 1;
      return Promise.resolve(
        jsonResponse("{}", calls === 1 ? { headers: { "set-cookie": "JSESSIONID=abc123; Path=/" } } : {})
      );
    };
    const session = createMontanaCersSession({
      fetchImpl,
      sleep: (ms) => {
        sleeps.push(ms);
        return Promise.resolve();
      },
      spacingMs: 250,
    });
    await session.get(buildMontanaCersUrl("search/candidateSearch"));
    await session.postForm(buildMontanaCersUrl("searchResults/searchCandidates"), { lastName: "Bedey" });
    expect(seenCookies[0]).toBeUndefined();
    expect(seenCookies[1]).toBe("JSESSIONID=abc123");
    expect(sleeps).toEqual([250]);
  });

  it("retries transient failures and gives up on the rest", async () => {
    let attempts = 0;
    const flaky: MontanaCersFetchFn = () => {
      attempts += 1;
      return Promise.resolve(jsonResponse("oops", { status: attempts < 3 ? 503 : 200 }));
    };
    const session = createMontanaCersSession({ fetchImpl: flaky, sleep: () => Promise.resolve() });
    const response = await session.get(buildMontanaCersUrl("publicReportList"));
    expect(response.status).toBe(200);
    expect(attempts).toBe(3);

    const notFound: MontanaCersFetchFn = () => Promise.resolve(jsonResponse("gone", { status: 404 }));
    const session2 = createMontanaCersSession({ fetchImpl: notFound, sleep: () => Promise.resolve() });
    await expect(session2.get(buildMontanaCersUrl("publicReportList"))).rejects.toMatchObject({
      code: "http_error",
      status: 404,
    });
  });

  it("returns 302 redirects as data without following them", async () => {
    const redirect: MontanaCersFetchFn = () =>
      Promise.resolve(
        new Response(null, {
          status: 302,
          headers: { location: "https://cers-ext.mt.gov/CampaignTracker/public/publicReportList" },
        })
      );
    const session = createMontanaCersSession({ fetchImpl: redirect, sleep: () => Promise.resolve() });
    const response = await session.postForm(buildMontanaCersUrl("publicReportList/retrieveCampaignReports"), {
      candidateId: "21020",
    });
    expect(response.status).toBe(302);
    expect(response.redirectLocation).toContain("/public/publicReportList");
  });

  it("rejects oversize responses", async () => {
    const big: MontanaCersFetchFn = () => Promise.resolve(jsonResponse("x".repeat(2_048)));
    const session = createMontanaCersSession({ fetchImpl: big, sleep: () => Promise.resolve(), maxResponseBytes: 1_024 });
    await expect(session.get(buildMontanaCersUrl("publicReportList"))).rejects.toMatchObject({
      code: "bad_response",
    });
  });
});
