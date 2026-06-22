import { describe, expect, it, vi } from "vitest";

import {
  buildOklahomaGuardianIeReportSearchRequest,
  buildOklahomaGuardianIeReportViewRequest,
  extractOklahomaGuardianWebFormHiddenFields,
  parseOklahomaGuardianIeReportPdfArtifacts,
  parseOklahomaGuardianIeReportSearchRows,
  probeOklahomaGuardianIeReportDocument,
  searchOklahomaGuardianIeReports,
} from "../../../src/pipeline/oklahomaFinance/oklahomaGuardianIeReportClient.js";

const SEARCH_PAGE_HTML = `
<form name="aspnetForm" method="post">
  <input type="hidden" name="ctl00_ToolkitScriptManager1_HiddenField" value="" />
  <input type="hidden" name="__VIEWSTATE" id="__VIEWSTATE" value="VIEWSTATE_VALUE" />
  <input type="hidden" name="__VIEWSTATEGENERATOR" id="__VIEWSTATEGENERATOR" value="7EAEBAAE" />
  <input type="hidden" name="__EVENTVALIDATION" id="__EVENTVALIDATION" value="EVENTVALIDATION_VALUE" />
</form>
`;

const RESULT_PAGE_HTML = `
<form name="aspnetForm" method="post">
  <input type="hidden" name="__VIEWSTATE" id="__VIEWSTATE" value="RESULT_VIEWSTATE_VALUE" />
  <input type="hidden" name="__EVENTVALIDATION" id="__EVENTVALIDATION" value="RESULT_EVENTVALIDATION_VALUE" />
</form>
<table class="frmDataGrid" cellspacing="0" rules="all" border="1">
  <tr>
    <th scope="col" abbr="FilerName"><a>Filer Name</a></th>
    <th scope="col" abbr="Description"><a>Report</a></th>
    <th scope="col" abbr="PeriodBegin"><a>Period Begin</a></th>
    <th scope="col" abbr="PeriodEnd"><a>Period End</a></th>
    <th scope="col" abbr="FiledDate"><a>Filed Date</a></th>
    <th scope="col" abbr="ViewReport">&nbsp;</th>
  </tr>
  <tr>
    <td>AMERICAN CONSERVATIVE UNION</td>
    <td>Public IE EC SQ Report - PRE-ELECTION</td>
    <td>6/14/2022</td>
    <td>6/28/2022</td>
    <td>6/15/2022</td>
    <td><a href="javascript:__doPostBack(&#39;ctl00$Content$abc$IEEC_SearchManager$Search_Export$mgrReportViewer$Search_Output$ctl01$ctl02$ViewReportNotAmended00&#39;,&#39;&#39;)">View Report</a></td>
  </tr>
  <tr>
    <td>COMMON SENSE CONSERVATIVES, LLC</td>
    <td>IE EC SQ Report</td>
    <td>10/25/2022</td>
    <td>10/31/2022</td>
    <td>11/1/2022</td>
    <td><a href="javascript:__doPostBack(&#39;ctl00$Content$abc$IEEC_SearchManager$Search_Export$mgrReportViewer$Search_Output$ctl01$ctl03$ViewReportNotAmended00&#39;,&#39;&#39;)">View Report</a></td>
  </tr>
</table>
`;

const REPORT_PAGE_HTML = `
<html>
  <body>
    <a href="data:application/pdf;base64,JVBERi0xLjMKJQ==">PDF</a>
  </body>
</html>
`;

function response(body: string, init: ResponseInit = {}): Response {
  return new Response(body, { status: 200, statusText: "OK", ...init });
}

describe("Oklahoma Guardian IE report client", () => {
  it("extracts WebForms hidden fields", () => {
    const fields = extractOklahomaGuardianWebFormHiddenFields(SEARCH_PAGE_HTML);

    expect(fields.get("__VIEWSTATE")).toBe("VIEWSTATE_VALUE");
    expect(fields.get("__EVENTVALIDATION")).toBe("EVENTVALIDATION_VALUE");
  });

  it("builds the Guardian IE report search POST request", () => {
    const request = buildOklahomaGuardianIeReportSearchRequest({
      searchPageHtml: SEARCH_PAGE_HTML,
      search: {
        candidateName: "Kevin Stitt",
        electionYear: 2022,
        candidateSearchMode: "contains",
      },
    });

    expect(request.method).toBe("POST");
    expect(request.body.get("__VIEWSTATE")).toBe("VIEWSTATE_VALUE");
    expect(
      request.body.get(
        "ctl00$Content$3ff86095-94e1-4145-a34f-248a7c4b4540$IEEC_SearchManager$IEEC_SearchParams$AssocCandidateName$ctl01"
      )
    ).toBe("Kevin Stitt");
    expect(
      request.body.get(
        "ctl00$Content$3ff86095-94e1-4145-a34f-248a7c4b4540$IEEC_SearchManager$IEEC_SearchParams$DateFrom$ctl01"
      )
    ).toBe("01/01/2022");
    expect(
      request.body.get(
        "ctl00$Content$3ff86095-94e1-4145-a34f-248a7c4b4540$IEEC_SearchManager$IEEC_SearchParams$DateThrough$ctl01"
      )
    ).toBe("12/31/2022");
    expect(
      request.body.get(
        "ctl00$Content$3ff86095-94e1-4145-a34f-248a7c4b4540$IEEC_SearchManager$IEEC_SearchParams$DisbursementCodeHook$ctl01"
      )
    ).toBe("A7BC217D-68C2-4601-BA0A-0B68C9BF66EA");
  });

  it("parses Guardian IE report search rows", () => {
    expect(parseOklahomaGuardianIeReportSearchRows(RESULT_PAGE_HTML)).toEqual([
      {
        filerName: "AMERICAN CONSERVATIVE UNION",
        reportDescription: "Public IE EC SQ Report - PRE-ELECTION",
        periodBegin: "6/14/2022",
        periodEnd: "6/28/2022",
        filedDate: "6/15/2022",
        viewReportPostbackTarget:
          "ctl00$Content$abc$IEEC_SearchManager$Search_Export$mgrReportViewer$Search_Output$ctl01$ctl02$ViewReportNotAmended00",
      },
      {
        filerName: "COMMON SENSE CONSERVATIVES, LLC",
        reportDescription: "IE EC SQ Report",
        periodBegin: "10/25/2022",
        periodEnd: "10/31/2022",
        filedDate: "11/1/2022",
        viewReportPostbackTarget:
          "ctl00$Content$abc$IEEC_SearchManager$Search_Export$mgrReportViewer$Search_Output$ctl01$ctl03$ViewReportNotAmended00",
      },
    ]);
  });

  it("builds a Guardian View Report postback request from search results", () => {
    const request = buildOklahomaGuardianIeReportViewRequest({
      searchResultsHtml: RESULT_PAGE_HTML,
      viewReportPostbackTarget:
        "ctl00$Content$abc$IEEC_SearchManager$Search_Export$mgrReportViewer$Search_Output$ctl01$ctl02$ViewReportNotAmended00",
    });

    expect(request.method).toBe("POST");
    expect(request.body.get("__VIEWSTATE")).toBe("RESULT_VIEWSTATE_VALUE");
    expect(request.body.get("__EVENTVALIDATION")).toBe("RESULT_EVENTVALIDATION_VALUE");
    expect(request.body.get("__EVENTTARGET")).toBe(
      "ctl00$Content$abc$IEEC_SearchManager$Search_Export$mgrReportViewer$Search_Output$ctl01$ctl02$ViewReportNotAmended00"
    );
  });

  it("parses embedded Guardian IE report PDF data artifacts", () => {
    expect(
      parseOklahomaGuardianIeReportPdfArtifacts(`
        <a href="DATA:APPLICATION/PDF;BASE64,JVBERi0xLjMKJQ==">PDF</a>
      `)
    ).toEqual([
      {
        mimeType: "application/pdf",
        dataUrl: "data:application/pdf;base64,JVBERi0xLjMKJQ==",
        base64Length: 16,
        byteLength: 10,
      },
    ]);
  });

  it("preserves Guardian cookies while probing a selected report document", async () => {
    const fetchImpl = vi
      .fn<typeof fetch>()
      .mockResolvedValueOnce(
        response(SEARCH_PAGE_HTML, {
          headers: { "set-cookie": "ASP.NET_SessionId=session-1; path=/; HttpOnly" },
        })
      )
      .mockImplementationOnce((_url, init) => {
        expect(new Headers(init?.headers).get("cookie")).toBe("ASP.NET_SessionId=session-1");
        return Promise.resolve(response(RESULT_PAGE_HTML));
      })
      .mockImplementationOnce((_url, init) => {
        expect(new Headers(init?.headers).get("cookie")).toBe("ASP.NET_SessionId=session-1");
        return Promise.resolve(response(REPORT_PAGE_HTML));
      });

    const result = await probeOklahomaGuardianIeReportDocument(
      { candidateName: "Stitt", electionYear: 2022 },
      { fetchImpl }
    );

    expect(fetchImpl).toHaveBeenCalledTimes(3);
    expect(result.selectedRow.filerName).toBe("AMERICAN CONSERVATIVE UNION");
    expect(result.pdfArtifacts).toEqual([
      expect.objectContaining({
        mimeType: "application/pdf",
        byteLength: 10,
      }),
    ]);
  });

  it("fetches the search page, submits the search, and parses rows", async () => {
    const fetchImpl = vi
      .fn<typeof fetch>()
      .mockResolvedValueOnce(
        response(SEARCH_PAGE_HTML, {
          headers: { "set-cookie": "ASP.NET_SessionId=session-2; path=/; HttpOnly" },
        })
      )
      .mockImplementationOnce((_url, init) => {
        expect(new Headers(init?.headers).get("cookie")).toBe("ASP.NET_SessionId=session-2");
        return Promise.resolve(response(RESULT_PAGE_HTML));
      });

    const result = await searchOklahomaGuardianIeReports(
      { candidateName: "Stitt", electionYear: 2022 },
      { fetchImpl }
    );

    expect(fetchImpl).toHaveBeenCalledTimes(2);
    expect(fetchImpl.mock.calls[0]?.[1]).toMatchObject({ method: "GET" });
    expect(fetchImpl.mock.calls[1]?.[1]).toMatchObject({ method: "POST" });
    expect(result).toMatchObject({
      candidateName: "Stitt",
      dateFrom: "01/01/2022",
      dateThrough: "12/31/2022",
      expenditureType: "independent_expenditure",
      rows: expect.arrayContaining([expect.objectContaining({ filerName: "AMERICAN CONSERVATIVE UNION" })]),
    });
  });

  it("rejects a search page without WebForms state", () => {
    expect(() => extractOklahomaGuardianWebFormHiddenFields("<html></html>")).toThrow(
      "required WebForms hidden fields"
    );
  });
});
