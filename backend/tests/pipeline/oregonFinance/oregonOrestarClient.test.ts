import { describe, expect, it, vi } from "vitest";

import {
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
      sessionId: "abc123",
    });
    expect(fetchFn.mock.calls[0]?.[0]).toBe("https://secure.sos.state.or.us/orestar/gotoPublicTransactionSearch.do");
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

  it("rejects generic search URLs because they do not identify a result set", async () => {
    await expect(
      getOregonOrestarTransactionDetailsFromSourceUrl({
        sourceUrl: "https://secure.sos.state.or.us/orestar/gotoPublicTransactionSearch.do",
      })
    ).rejects.toThrow("Oregon ORESTAR source URL must point to a transaction detail or populated search result");
  });
});
