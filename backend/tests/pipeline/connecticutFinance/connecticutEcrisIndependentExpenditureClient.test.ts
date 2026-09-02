import { describe, expect, it, vi } from "vitest";

import {
  CONNECTICUT_ECRIS_INDEPENDENT_EXPENDITURE_PAGE_SIZE,
  fetchConnecticutEcrisIndependentExpenditures,
} from "../../../src/pipeline/connecticutFinance/connecticutEcrisIndependentExpenditureClient.js";
import {
  CONNECTICUT_ECRIS_INDEPENDENT_EXPENDITURE_COLUMNS,
  CONNECTICUT_ECRIS_INDEPENDENT_EXPENDITURE_SEARCH_URL,
  CONNECTICUT_ECRIS_NO_DOCUMENTS_MESSAGE,
} from "../../../src/pipeline/connecticutFinance/connecticutEcrisIndependentExpenditureParsers.js";

const FORM_PAGE = `
<html><body><form id="aspnetForm">
<input type="hidden" name="__VIEWSTATE" id="__VIEWSTATE" value="STATE==" />
<input type="hidden" name="__VIEWSTATEGENERATOR" id="__VIEWSTATEGENERATOR" value="GEN" />
<input type="hidden" name="__VIEWSTATEENCRYPTED" id="__VIEWSTATEENCRYPTED" value="" />
</form></body></html>`;

type RowSpec = { receivedDate: string; committee?: string; formTag?: string; fileYear?: number; amount?: string };

function resultsPage(rows: readonly RowSpec[]): string {
  const header = CONNECTICUT_ECRIS_INDEPENDENT_EXPENDITURE_COLUMNS.map((column) => `<th scope="col">${column}</th>`).join("");
  const body = rows
    .map(
      (row, index) =>
        `<tr><td class="HideColumn">0</td>` +
        `<td align="left"> <a id="ctl00_ContentPlaceHolder1_gvSearchResult_ctl${index}_hlDocFile" href="Data/Attachment/Unassigned/SEEC40_July_10_Filing_${index}.PDF" target="_blank">${row.committee ?? "Nutmeg Forward"}</a> <span id="x_lblDocName">(${row.formTag ?? "SEEC40"})</span> </td>` +
        `<td>July 10 Filing</td><td>Original</td><td>Vendor</td><td><span>${row.receivedDate}</span></td><td>${row.fileYear ?? 2026}</td>` +
        `<td><span>04/01/2026</span></td><td><span>06/30/2026</span></td><td><span>${row.amount ?? "$10.00"}</span></td>` +
        `<td>G. Expenses Paid by Committee</td><td>Jane Q Doe</td><td>State Representative</td><td>&nbsp;</td><td>&nbsp;</td><td>eFile</td></tr>`
    )
    .join("");
  return `<html><body><table id="ctl00_ContentPlaceHolder1_gvSearchResult"><tr>${header}</tr>${body}</table></body></html>`;
}

const NO_DOCUMENTS_PAGE = `<html><body><span>${CONNECTICUT_ECRIS_NO_DOCUMENTS_MESSAGE}</span></body></html>`;

type Call = { method: string; headers: Record<string, string>; params: URLSearchParams | null };

/** Fake eCRIS: answers GET with the form and POST via `answer(params)`. */
function fakeEcris(answer: (params: URLSearchParams) => string) {
  const calls: Call[] = [];
  const fetchImpl = vi.fn(async (url: string | URL | Request, init?: RequestInit): Promise<Response> => {
    expect(String(url)).toBe(CONNECTICUT_ECRIS_INDEPENDENT_EXPENDITURE_SEARCH_URL);
    const method = init?.method ?? "GET";
    const headers = Object.fromEntries(new Headers(init?.headers).entries());
    if (method === "GET") {
      calls.push({ method, headers, params: null });
      return new Response(FORM_PAGE, { status: 200, headers: { "set-cookie": "ASP.NET_SessionId=abc123; path=/; HttpOnly" } });
    }
    const params = new URLSearchParams(String(init?.body));
    calls.push({ method, headers, params });
    return new Response(answer(params), { status: 200 });
  });
  return { fetchImpl: fetchImpl as unknown as typeof fetch, calls };
}

function windowOf(params: URLSearchParams): string {
  return `${params.get("ctl00$ContentPlaceHolder1$ReceivedStartDate")}..${params.get("ctl00$ContentPlaceHolder1$ReceivedEndDate")}`;
}

describe("connecticutEcrisIndependentExpenditureClient", () => {
  it("posts the form's hidden fields with the Form 40 filter and forwards the session cookie", async () => {
    const { fetchImpl, calls } = fakeEcris(() => resultsPage([{ receivedDate: "07/30/2026" }]));

    const result = await fetchConnecticutEcrisIndependentExpenditures({ year: 2026, fetchImpl });

    expect(result).toMatchObject({
      year: 2026,
      sourceUrl: CONNECTICUT_ECRIS_INDEPENDENT_EXPENDITURE_SEARCH_URL,
      searchWindows: [{ startDate: "2026-01-01", endDate: "2026-12-31", rowCount: 1 }],
    });
    expect(result.rows).toHaveLength(1);
    expect(result.rows[0]).toMatchObject({ committeeName: "Nutmeg Forward", receivedDate: "2026-07-30", amountCents: 1000 });

    expect(calls.map((call) => call.method)).toEqual(["GET", "POST"]);
    expect(calls[0]?.headers["user-agent"]).toMatch(/^Mozilla\/5\.0/);
    const post = calls[1]!;
    expect(post.headers.cookie).toBe("ASP.NET_SessionId=abc123");
    expect(post.headers["content-type"]).toBe("application/x-www-form-urlencoded");
    expect(Object.fromEntries(post.params!.entries())).toEqual({
      __VIEWSTATE: "STATE==",
      __VIEWSTATEGENERATOR: "GEN",
      __VIEWSTATEENCRYPTED: "",
      "ctl00$ContentPlaceHolder1$lstFILE_YEAR": "2026",
      "ctl00$ContentPlaceHolder1$lstNoOfRecords": String(CONNECTICUT_ECRIS_INDEPENDENT_EXPENDITURE_PAGE_SIZE),
      "ctl00$ContentPlaceHolder1$rblShowHistory": "0",
      "ctl00$ContentPlaceHolder1$ReceivedStartDate": "01/01/2026",
      "ctl00$ContentPlaceHolder1$ReceivedEndDate": "12/31/2026",
      "ctl00$ContentPlaceHolder1$chkFormName$4": "on",
      "ctl00$ContentPlaceHolder1$btnSearch": "Search",
    });
  });

  it("splits a window that fills the page until every window is under the cap", async () => {
    const pageSize = 3;
    const many = (dates: string[]) => resultsPage(dates.map((receivedDate) => ({ receivedDate })));
    const { fetchImpl, calls } = fakeEcris((params) => {
      switch (windowOf(params)) {
        case "01/01/2026..12/31/2026":
          return many(["01/05/2026", "03/09/2026", "11/20/2026"]);
        case "01/01/2026..07/02/2026":
          return many(["01/05/2026", "03/09/2026"]);
        case "07/03/2026..12/31/2026":
          return many(["11/20/2026"]);
        default:
          throw new Error(`unexpected window ${windowOf(params)}`);
      }
    });

    const result = await fetchConnecticutEcrisIndependentExpenditures({ year: 2026, fetchImpl, pageSize });

    expect(result.rows.map((row) => row.receivedDate)).toEqual(["2026-01-05", "2026-03-09", "2026-11-20"]);
    expect(result.searchWindows).toEqual([
      { startDate: "2026-01-01", endDate: "2026-07-02", rowCount: 2 },
      { startDate: "2026-07-03", endDate: "2026-12-31", rowCount: 1 },
    ]);
    expect(calls.filter((call) => call.method === "POST")).toHaveLength(3);
  });

  it("treats a no-documents window as empty", async () => {
    const { fetchImpl } = fakeEcris(() => NO_DOCUMENTS_PAGE);

    const result = await fetchConnecticutEcrisIndependentExpenditures({ year: 2026, fetchImpl });

    expect(result.rows).toEqual([]);
    expect(result.searchWindows).toEqual([{ startDate: "2026-01-01", endDate: "2026-12-31", rowCount: 0 }]);
  });

  it("fails closed when one day still fills the page", async () => {
    const { fetchImpl } = fakeEcris(() => resultsPage([{ receivedDate: "05/05/2026" }, { receivedDate: "05/05/2026" }]));

    await expect(fetchConnecticutEcrisIndependentExpenditures({ year: 2026, fetchImpl, pageSize: 2 })).rejects.toThrow(
      "the day cannot be split further"
    );
  });

  it("rejects rows from another file year or another form", async () => {
    const wrongYear = fakeEcris(() => resultsPage([{ receivedDate: "05/05/2026", fileYear: 2025 }]));
    await expect(fetchConnecticutEcrisIndependentExpenditures({ year: 2026, fetchImpl: wrongYear.fetchImpl })).rejects.toThrow(
      "returned a 2025 row"
    );

    const wrongForm = fakeEcris(() => resultsPage([{ receivedDate: "05/05/2026", formTag: "SEEC20" }]));
    await expect(fetchConnecticutEcrisIndependentExpenditures({ year: 2026, fetchImpl: wrongForm.fetchImpl })).rejects.toThrow(
      "returned a SEEC20 document; only SEEC40 was requested"
    );
  });

  it("surfaces HTTP failures and a form without view state", async () => {
    const failing = vi.fn(async () => new Response("down", { status: 503, statusText: "Service Unavailable" }));
    await expect(
      fetchConnecticutEcrisIndependentExpenditures({ year: 2026, fetchImpl: failing as unknown as typeof fetch })
    ).rejects.toThrow("search form answered 503 Service Unavailable");

    const noState = vi.fn(async () => new Response("<html></html>", { status: 200 }));
    await expect(
      fetchConnecticutEcrisIndependentExpenditures({ year: 2026, fetchImpl: noState as unknown as typeof fetch })
    ).rejects.toThrow("has no __VIEWSTATE");
  });
});
