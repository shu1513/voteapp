import { describe, expect, it, vi } from "vitest";

import {
  getOregonOrestarCandidateSearchRows,
  getOregonOrestarCommitteeTransactionDetails,
  getOregonOrestarSearchForm,
  getOregonOrestarTransactionDetail,
  getOregonOrestarTransactionDetailsFromSourceUrl,
} from "../../../src/pipeline/oregonFinance/oregonOrestarClient.js";

function htmlResponse(html: string, overrides: Partial<{ ok: boolean; status: number; statusText: string }> = {}) {
  return {
    ok: overrides.ok ?? true,
    status: overrides.status ?? 200,
    statusText: overrides.statusText ?? "OK",
    text: async () => html,
  };
}

describe("oregonOrestarClient", () => {
  it("fetches and parses the public search form", async () => {
    const fetchFn = vi.fn(async () =>
      htmlResponse(`
        <form name="cneSearchForm" action="/orestar/cneSearch.do;JSESSIONID_ORESTAR=abc123">
          <input type="hidden" name="OWASP_CSRFTOKEN" value="csrf-token-1">
        </form>
      `)
    );

    await expect(getOregonOrestarSearchForm({ fetchFn })).resolves.toEqual({
      actionUrl: "https://secure.sos.state.or.us/orestar/cneSearch.do;JSESSIONID_ORESTAR=abc123",
      csrfToken: "csrf-token-1",
      cookieHeader: null,
      sessionId: "abc123",
    });
    expect(fetchFn.mock.calls[0]?.[0]).toBe("https://secure.sos.state.or.us/orestar/gotoPublicTransactionSearch.do");
  });

  it("fetches the CSRFGuard token when the live search form omits the hidden token", async () => {
    const fetchFn = vi.fn(async (url: string, init?: RequestInit) => {
      if (url.endsWith("/orestar/JavaScriptServlet")) {
        expect(init?.method).toBe("POST");
        expect(init?.headers).toMatchObject({
          "FETCH-CSRF-TOKEN": "1",
          cookie: "JSESSIONID_ORESTAR=abc123",
        });
        return htmlResponse("OWASP_CSRFTOKEN:csrf:token:from-script");
      }
      return {
        ...htmlResponse(`
          <form name="cneSearchForm" action="/orestar/gotoPublicTransactionSearchResults.do;JSESSIONID_ORESTAR=abc123">
            <input type="hidden" name="cneSearchButtonName" value="">
          </form>
        `),
        headers: {
          get: (name: string) => (name.toLowerCase() === "set-cookie" ? "JSESSIONID_ORESTAR=abc123; Path=/orestar" : null),
        },
      };
    });

    await expect(getOregonOrestarSearchForm({ fetchFn })).resolves.toEqual({
      actionUrl: "https://secure.sos.state.or.us/orestar/gotoPublicTransactionSearchResults.do;JSESSIONID_ORESTAR=abc123",
      csrfToken: "csrf:token:from-script",
      cookieHeader: "JSESSIONID_ORESTAR=abc123",
      sessionId: "abc123",
    });
    expect(fetchFn).toHaveBeenCalledTimes(2);
  });

  it("fetches and parses a transaction detail URL", async () => {
    const fetchFn = vi.fn(async () =>
      htmlResponse(`
        <table>
          <tr><td>Transaction Detail</td><td>Friends of Tina Kotek (4792)</td></tr>
          <tr><td>Transaction ID</td><td>:</td><td>4458653</td></tr>
          <tr><td>Transaction Date</td><td>:</td><td>10/12/2022</td></tr>
          <tr><td>Transaction Type</td><td>:</td><td>Contribution</td></tr>
          <tr><td>Amount</td><td>:</td><td>$10,000.00</td></tr>
        </table>
      `)
    );

    await expect(
      getOregonOrestarTransactionDetail({
        url: "https://secure.sos.state.or.us/orestar/gotoPublicTransactionDetail.do?tranRsn=4458653",
        options: { fetchFn },
      })
    ).resolves.toMatchObject({
      transactionId: "4458653",
      amount: 10_000,
      sourceUrl: "https://secure.sos.state.or.us/orestar/gotoPublicTransactionDetail.do?tranRsn=4458653",
    });
  });

  it("rejects transaction detail URLs outside the ORESTAR origin before fetching", async () => {
    const fetchFn = vi.fn(async () => htmlResponse("<html></html>"));

    await expect(
      getOregonOrestarTransactionDetail({
        url: "https://example.test/orestar/gotoPublicTransactionDetail.do?tranRsn=4458653",
        options: { fetchFn },
      })
    ).rejects.toThrow("Oregon ORESTAR URL must use https://secure.sos.state.or.us");
    expect(fetchFn).not.toHaveBeenCalled();
  });

  it("loads detail pages referenced by a populated search-result URL", async () => {
    const fetchFn = vi.fn(async (url: string) => {
      if (url.includes("cneSearch.do")) {
        return htmlResponse(`
          <div>Results : 1 record found</div>
          <table>
            <tr><th>Tran ID</th><th>Date</th><th>Status</th><th>Filer/Committee</th><th>Contributor/Payee</th><th>Sub Type</th><th>Amount</th></tr>
            <tr>
              <td><a href="/orestar/gotoPublicTransactionDetail.do?tranRsn=4458653">4458653</a></td>
              <td>10/12/2022</td><td>Original</td>
              <td><a href="/orestar/sooDetail.do?cneCommitteeId=4792">Friends of Tina Kotek</a></td>
              <td>John Ramsbacher</td><td>Cash Contribution</td><td>$10,000.00</td>
            </tr>
          </table>
        `);
      }
      return htmlResponse(`
        <table>
          <tr><td>Transaction Detail</td><td>Friends of Tina Kotek (4792)</td></tr>
          <tr><td>Transaction ID</td><td>:</td><td>4458653</td></tr>
          <tr><td>Transaction Date</td><td>:</td><td>10/12/2022</td></tr>
          <tr><td>Transaction Type</td><td>:</td><td>Contribution</td></tr>
          <tr><td>Amount</td><td>:</td><td>$10,000.00</td></tr>
        </table>
      `);
    });

    await expect(
      getOregonOrestarTransactionDetailsFromSourceUrl({
        sourceUrl: "https://secure.sos.state.or.us/orestar/cneSearch.do?candidate=4792",
        options: { fetchFn },
      })
    ).resolves.toHaveLength(1);
    expect(fetchFn).toHaveBeenCalledTimes(2);
  });

  it("follows next result pages until the detail cap is reached", async () => {
    const fetchFn = vi.fn(async (url: string) => {
      if (url.includes("page=2")) {
        return htmlResponse(`
          <div>Results : 2 records found</div>
          <table>
            <tr><th>Tran ID</th><th>Date</th><th>Status</th><th>Filer/Committee</th><th>Contributor/Payee</th><th>Sub Type</th><th>Amount</th></tr>
            <tr>
              <td><a href="/orestar/gotoPublicTransactionDetail.do?tranRsn=4459000">4459000</a></td>
              <td>10/13/2022</td><td>Original</td>
              <td><a href="/orestar/sooDetail.do?cneCommitteeId=4792">Friends of Tina Kotek</a></td>
              <td>Jane Donor</td><td>Cash Contribution</td><td>$250.00</td>
            </tr>
          </table>
        `);
      }
      if (url.includes("cneSearch.do")) {
        return htmlResponse(`
          <a href="/orestar/cneSearch.do?page=2">Next</a>
          <table>
            <tr><th>Tran ID</th><th>Date</th><th>Status</th><th>Filer/Committee</th><th>Contributor/Payee</th><th>Sub Type</th><th>Amount</th></tr>
            <tr>
              <td><a href="/orestar/gotoPublicTransactionDetail.do?tranRsn=4458653">4458653</a></td>
              <td>10/12/2022</td><td>Original</td>
              <td><a href="/orestar/sooDetail.do?cneCommitteeId=4792">Friends of Tina Kotek</a></td>
              <td>John Ramsbacher</td><td>Cash Contribution</td><td>$10,000.00</td>
            </tr>
          </table>
        `);
      }
      const transactionId = new URL(url).searchParams.get("tranRsn") ?? "unknown";
      return htmlResponse(`
        <table>
          <tr><td>Transaction Detail</td><td>Friends of Tina Kotek (4792)</td></tr>
          <tr><td>Transaction ID</td><td>:</td><td>${transactionId}</td></tr>
          <tr><td>Transaction Date</td><td>:</td><td>10/12/2022</td></tr>
          <tr><td>Transaction Type</td><td>:</td><td>Contribution</td></tr>
          <tr><td>Amount</td><td>:</td><td>$10.00</td></tr>
        </table>
      `);
    });

    await expect(
      getOregonOrestarTransactionDetailsFromSourceUrl({
        sourceUrl: "https://secure.sos.state.or.us/orestar/cneSearch.do?candidate=4792",
        maxDetails: 2,
        options: { fetchFn },
      })
    ).resolves.toMatchObject([{ transactionId: "4458653" }, { transactionId: "4459000" }]);
    expect(fetchFn).toHaveBeenCalledTimes(4);
    expect(fetchFn.mock.calls[2]?.[0]).toBe("https://secure.sos.state.or.us/orestar/cneSearch.do?page=2");
  });

  it("passes an abort signal with the configured timeout to ORESTAR requests", async () => {
    const fetchFn = vi.fn(async (_url: string, init?: RequestInit) => {
      expect(init?.signal).toBeInstanceOf(AbortSignal);
      return htmlResponse(`
        <form name="cneSearchForm" action="/orestar/cneSearch.do;JSESSIONID_ORESTAR=abc123">
          <input type="hidden" name="OWASP_CSRFTOKEN" value="csrf-token-1">
        </form>
      `);
    });

    await getOregonOrestarSearchForm({ fetchFn, timeoutMs: 5000 });

    expect(fetchFn).toHaveBeenCalledWith(
      "https://secure.sos.state.or.us/orestar/gotoPublicTransactionSearch.do",
      expect.objectContaining({
        signal: expect.any(AbortSignal),
      })
    );
  });

  it("rejects generic search URLs because they do not identify a result set", async () => {
    await expect(
      getOregonOrestarTransactionDetailsFromSourceUrl({
        sourceUrl: "https://secure.sos.state.or.us/orestar/gotoPublicTransactionSearch.do",
      })
    ).rejects.toThrow("Oregon ORESTAR source URL must point to a transaction detail or populated search result");
  });

  it("rejects source URLs outside the ORESTAR origin before pagination", async () => {
    const fetchFn = vi.fn(async () => htmlResponse("<html></html>"));

    await expect(
      getOregonOrestarTransactionDetailsFromSourceUrl({
        sourceUrl: "https://example.test/orestar/cneSearch.do?candidate=4792",
        options: { fetchFn },
      })
    ).rejects.toThrow("Oregon ORESTAR URL must use https://secure.sos.state.or.us");
    expect(fetchFn).not.toHaveBeenCalled();
  });

  it("searches candidate transactions across the full two-year cycle window", async () => {
    const fetchFn = vi.fn(async (url: string, init?: RequestInit) => {
      if (url.includes("gotoPublicTransactionSearchResults.do")) {
        const body = String(init?.body);
        expect(body).toContain("cneSearchFilerCommitteeTxt=Tina+Kotek");
        expect(body).toContain("cneSearchTranStartDate=01%2F01%2F2025");
        expect(body).toContain("cneSearchTranEndDate=12%2F31%2F2026");
        return htmlResponse(`
          <div>Results : 1 record found</div>
          <table>
            <tr><th>Tran ID</th><th>Date</th><th>Status</th><th>Filer/Committee</th><th>Contributor/Payee</th><th>Sub Type</th><th>Amount</th></tr>
            <tr>
              <td><a href="/orestar/gotoPublicTransactionDetail.do?tranRsn=5500001">5500001</a></td>
              <td>05/05/2025</td><td>Original</td>
              <td><a href="/orestar/sooDetail.do?cneCommitteeId=4792">Friends of Tina Kotek</a></td>
              <td>Jane Donor</td><td>Cash Contribution</td><td>$100.00</td>
            </tr>
          </table>
        `);
      }
      return htmlResponse(`
        <form name="cneSearchForm" action="/orestar/gotoPublicTransactionSearchResults.do;JSESSIONID_ORESTAR=abc123">
          <input type="hidden" name="OWASP_CSRFTOKEN" value="csrf-token-1">
        </form>
      `);
    });

    await expect(
      getOregonOrestarCandidateSearchRows({
        candidateName: "Tina Kotek",
        electionYear: 2026,
        options: { fetchFn },
      })
    ).resolves.toMatchObject([{ transactionId: "5500001", transactionDate: "05/05/2025" }]);
  });

  it("pages committee transactions by re-POSTing with an incremented page index", async () => {
    const searchRow = (transactionId: string, date: string) => `
      <tr>
        <td><a href="/orestar/gotoPublicTransactionDetail.do?tranRsn=${transactionId}">${transactionId}</a></td>
        <td>${date}</td><td>Original</td>
        <td><a href="/orestar/sooDetail.do?cneCommitteeId=4792">Friends of Tina Kotek</a></td>
        <td>Jane Donor</td><td>Cash Contribution</td><td>$100.00</td>
      </tr>
    `;
    const header = `<tr><th>Tran ID</th><th>Date</th><th>Status</th><th>Filer/Committee</th><th>Contributor/Payee</th><th>Sub Type</th><th>Amount</th></tr>`;
    const searchBodies: string[] = [];
    const fetchFn = vi.fn(async (url: string, init?: RequestInit) => {
      if (url.includes("gotoPublicTransactionSearchResults.do")) {
        const body = String(init?.body);
        searchBodies.push(body);
        const pageIdx = new URLSearchParams(body).get("cneSearchPageIdx");
        if (pageIdx === "0") {
          return htmlResponse(`
            <div>Results : 3 records found</div>
            <table>${header}${searchRow("5500001", "10/12/2025")}${searchRow("5500002", "11/01/2025")}</table>
          `);
        }
        return htmlResponse(`
          <div>Results : 3 records found</div>
          <table>${header}${searchRow("5500003", "02/03/2026")}</table>
        `);
      }
      if (url.includes("gotoPublicTransactionDetail.do")) {
        const transactionId = new URL(url).searchParams.get("tranRsn") ?? "unknown";
        return htmlResponse(`
          <table>
            <tr><td>Transaction Detail</td><td>Friends of Tina Kotek (4792)</td></tr>
            <tr><td>Transaction ID</td><td>:</td><td>${transactionId}</td></tr>
            <tr><td>Transaction Date</td><td>:</td><td>10/12/2025</td></tr>
            <tr><td>Transaction Type</td><td>:</td><td>Contribution</td></tr>
            <tr><td>Amount</td><td>:</td><td>$100.00</td></tr>
          </table>
        `);
      }
      return htmlResponse(`
        <form name="cneSearchForm" action="/orestar/gotoPublicTransactionSearchResults.do;JSESSIONID_ORESTAR=abc123">
          <input type="hidden" name="OWASP_CSRFTOKEN" value="csrf-token-1">
        </form>
      `);
    });

    await expect(
      getOregonOrestarCommitteeTransactionDetails({
        committeeId: "4792",
        electionYear: 2026,
        options: { fetchFn },
      })
    ).resolves.toMatchObject([{ transactionId: "5500001" }, { transactionId: "5500002" }, { transactionId: "5500003" }]);

    expect(searchBodies).toHaveLength(2);
    for (const body of searchBodies) {
      expect(body).toContain("cneSearchFilerCommitteeId=4792");
      expect(body).toContain("cneSearchTranStartDate=01%2F01%2F2025");
      expect(body).toContain("cneSearchTranEndDate=12%2F31%2F2026");
    }
    expect(new URLSearchParams(searchBodies[0]).get("cneSearchButtonName")).toBe("search");
    expect(new URLSearchParams(searchBodies[1]).get("cneSearchButtonName")).toBe("next");
    // 1 form fetch + 2 search pages + 3 detail fetches; paging stops once
    // resultCount is reached instead of trusting the pager controls.
    expect(fetchFn).toHaveBeenCalledTimes(6);
  });

  it("fails closed when the committee search returns an empty page without a result count", async () => {
    const fetchFn = vi.fn(async (url: string) => {
      if (url.includes("gotoPublicTransactionSearchResults.do")) {
        // Soft-blocked portal responses render the page but omit both the
        // result rows and the "Results : N records found" marker.
        return htmlResponse("<html><body>Transaction search</body></html>");
      }
      return htmlResponse(`
        <form name="cneSearchForm" action="/orestar/gotoPublicTransactionSearchResults.do;JSESSIONID_ORESTAR=abc123">
          <input type="hidden" name="OWASP_CSRFTOKEN" value="csrf-token-1">
        </form>
      `);
    });

    await expect(
      getOregonOrestarCommitteeTransactionDetails({
        committeeId: "4792",
        electionYear: 2026,
        options: { fetchFn },
      })
    ).rejects.toThrow("ORESTAR returned an empty search page for committee 4792");
  });

  it("refuses committees larger than maxDetails and rejects non-numeric committee ids", async () => {
    await expect(
      getOregonOrestarCommitteeTransactionDetails({
        committeeId: "not-a-committee",
        electionYear: 2026,
      })
    ).rejects.toThrow("Oregon ORESTAR committee transaction search requires a numeric committeeId");

    const searchRow = (transactionId: string) => `
      <tr>
        <td><a href="/orestar/gotoPublicTransactionDetail.do?tranRsn=${transactionId}">${transactionId}</a></td>
        <td>10/12/2025</td><td>Original</td>
        <td><a href="/orestar/sooDetail.do?cneCommitteeId=4792">Friends of Tina Kotek</a></td>
        <td>Jane Donor</td><td>Cash Contribution</td><td>$100.00</td>
      </tr>
    `;
    const header = `<tr><th>Tran ID</th><th>Date</th><th>Status</th><th>Filer/Committee</th><th>Contributor/Payee</th><th>Sub Type</th><th>Amount</th></tr>`;
    const fetchFn = vi.fn(async (url: string) => {
      if (url.includes("gotoPublicTransactionSearchResults.do")) {
        return htmlResponse(`
          <div>Results : 9 records found</div>
          <table>${header}${searchRow("5500001")}${searchRow("5500002")}${searchRow("5500003")}</table>
        `);
      }
      return htmlResponse(`
        <form name="cneSearchForm" action="/orestar/gotoPublicTransactionSearchResults.do;JSESSIONID_ORESTAR=abc123">
          <input type="hidden" name="OWASP_CSRFTOKEN" value="csrf-token-1">
        </form>
      `);
    });

    // A committee whose result count exceeds maxDetails must throw up front —
    // persisting the first N transactions would silently understate totals.
    await expect(
      getOregonOrestarCommitteeTransactionDetails({
        committeeId: "4792",
        electionYear: 2026,
        maxDetails: 2,
        options: { fetchFn },
      })
    ).rejects.toThrow("has 9 transactions, more than maxDetails=2");
    // 1 form fetch + 1 search page; no detail fetches are wasted.
    expect(fetchFn).toHaveBeenCalledTimes(2);
  });

  it("throws instead of returning a partial crawl when pagination stalls short of the result count", async () => {
    const searchRow = (transactionId: string) => `
      <tr>
        <td><a href="/orestar/gotoPublicTransactionDetail.do?tranRsn=${transactionId}">${transactionId}</a></td>
        <td>10/12/2025</td><td>Original</td>
        <td><a href="/orestar/sooDetail.do?cneCommitteeId=4792">Friends of Tina Kotek</a></td>
        <td>Jane Donor</td><td>Cash Contribution</td><td>$100.00</td>
      </tr>
    `;
    const header = `<tr><th>Tran ID</th><th>Date</th><th>Status</th><th>Filer/Committee</th><th>Contributor/Payee</th><th>Sub Type</th><th>Amount</th></tr>`;
    const fetchFn = vi.fn(async (url: string) => {
      if (url.includes("gotoPublicTransactionSearchResults.do")) {
        // Every page repeats the same two rows even though four exist — the
        // portal degrading mid-crawl must not truncate silently.
        return htmlResponse(`
          <div>Results : 4 records found</div>
          <table>${header}${searchRow("5500001")}${searchRow("5500002")}</table>
        `);
      }
      if (url.includes("gotoPublicTransactionDetail.do")) {
        const transactionId = new URL(url).searchParams.get("tranRsn") ?? "unknown";
        return htmlResponse(`
          <table>
            <tr><td>Transaction ID</td><td>:</td><td>${transactionId}</td></tr>
            <tr><td>Amount</td><td>:</td><td>$100.00</td></tr>
          </table>
        `);
      }
      return htmlResponse(`
        <form name="cneSearchForm" action="/orestar/gotoPublicTransactionSearchResults.do;JSESSIONID_ORESTAR=abc123">
          <input type="hidden" name="OWASP_CSRFTOKEN" value="csrf-token-1">
        </form>
      `);
    });

    await expect(
      getOregonOrestarCommitteeTransactionDetails({
        committeeId: "4792",
        electionYear: 2026,
        options: { fetchFn },
      })
    ).rejects.toThrow("ORESTAR returned 2 of 4 transactions for committee 4792");
  });
});
