import { describe, expect, it, vi } from "vitest";

import { discoverOklahomaGuardianIeOutsideSpendingReports } from "../../../src/pipeline/oklahomaFinance/oklahomaGuardianIeOutsideSpendingDiscovery.js";

const SEARCH_PAGE_HTML = `
<form name="aspnetForm" method="post">
  <input type="hidden" name="__VIEWSTATE" id="__VIEWSTATE" value="VIEWSTATE_VALUE" />
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
    <th scope="col" abbr="FilerName">Filer Name</th>
    <th scope="col" abbr="Description">Report</th>
    <th scope="col" abbr="PeriodBegin">Period Begin</th>
    <th scope="col" abbr="PeriodEnd">Period End</th>
    <th scope="col" abbr="FiledDate">Filed Date</th>
    <th scope="col" abbr="ViewReport">&nbsp;</th>
  </tr>
  <tr>
    <td>HOMETOWN FREEDOM ACTION NETWORK</td>
    <td>IE EC SQ Report</td>
    <td>10/25/2022</td>
    <td>10/31/2022</td>
    <td>11/1/2022</td>
    <td><a href="javascript:__doPostBack(&#39;target$ViewReportNotAmended00&#39;,&#39;&#39;)">View Report</a></td>
  </tr>
  <tr>
    <td>AMERICAN CONSERVATIVE UNION</td>
    <td>Public IE EC SQ Report - PRE-ELECTION</td>
    <td>6/14/2022</td>
    <td>6/28/2022</td>
    <td>6/15/2022</td>
    <td><a href="javascript:__doPostBack(&#39;target$ViewReportNotAmended01&#39;,&#39;&#39;)">View Report</a></td>
  </tr>
</table>
`;

const SINGLE_CANDIDATE_TEXT = `
AMENDED:
NO
Full Name of Committee or Person Making Expenditure
HOMETOWN FREEDOM ACTION NETWORK
Type of Report
IE EC SQ Report
Reporting Period:
10/25/2022 - 10/31/2022
TOTAL EXPENDITURES:
$12,345.67
STITT, KEVIN
, GOVERNOR
(OPPOSE)
`;

const MULTI_CANDIDATE_TEXT = `
AMENDED:
NO
Full Name of Committee or Person Making Expenditure
Acronym
AMERICAN CONSERVATIVE UNION
ACU
Type of Report
Public IE EC SQ Report - PRE-ELECTION
Reporting Period:
06/14/2022 - 06/28/2022
TOTAL EXPENDITURES:
$6,000.00
O'CONNOR, JOHN
, ATTORNEY
GENERAL
(SUPPORT)
STITT, KEVIN
, GOVERNOR
(SUPPORT)
`;

function response(body: string, init: ResponseInit = {}): Response {
  return new Response(body, { status: 200, statusText: "OK", ...init });
}

function simplePdfWithText(content: string): Buffer {
  const stream = content
    .split("\n")
    .map((line) => `BT /F1 12 Tf 0 0 Td (${line.replace(/([()\\])/g, "\\$1")}) Tj ET`)
    .join("\n");
  return Buffer.from(`%PDF-1.3\n1 0 obj\n<< /Length ${stream.length} >>\nstream\n${stream}\nendstream\nendobj\n%%EOF`, "latin1");
}

function reportPageHtml(text: string): string {
  return `<a href="data:application/pdf;base64,${simplePdfWithText(text).toString("base64")}">PDF</a>`;
}

describe("Oklahoma Guardian IE outside-spending discovery", () => {
  it("returns only strict candidate-specific usable reports and structured skips", async () => {
    const fetchImpl = vi
      .fn<typeof fetch>()
      .mockResolvedValueOnce(response(SEARCH_PAGE_HTML))
      .mockResolvedValueOnce(response(RESULT_PAGE_HTML))
      .mockResolvedValueOnce(response(SEARCH_PAGE_HTML))
      .mockResolvedValueOnce(response(RESULT_PAGE_HTML))
      .mockResolvedValueOnce(response(reportPageHtml(SINGLE_CANDIDATE_TEXT)))
      .mockResolvedValueOnce(response(SEARCH_PAGE_HTML))
      .mockResolvedValueOnce(response(RESULT_PAGE_HTML))
      .mockResolvedValueOnce(response(reportPageHtml(MULTI_CANDIDATE_TEXT)));

    const result = await discoverOklahomaGuardianIeOutsideSpendingReports(
      { candidateName: "Kevin Stitt", electionYear: 2022, maxReports: 2 },
      { fetchImpl }
    );

    expect(fetchImpl).toHaveBeenCalledTimes(8);
    expect(result.reportsExamined).toBe(2);
    expect(result.usableReports).toEqual([
      expect.objectContaining({
        rowIndex: 0,
        spenderName: "HOMETOWN FREEDOM ACTION NETWORK",
        candidateName: "STITT, KEVIN",
        officeName: "GOVERNOR",
        supportOppose: "oppose",
        amount: 12345.67,
        reportingPeriodBegin: "10/25/2022",
        reportingPeriodEnd: "10/31/2022",
      }),
    ]);
    expect(result.skippedReports).toEqual([
      expect.objectContaining({
        rowIndex: 1,
        reason: "multiple_candidate_stances",
        matchingCandidateStances: [
          { candidateName: "STITT, KEVIN", officeName: "GOVERNOR", supportOppose: "support" },
        ],
      }),
    ]);
  });

  it("validates maxReports", async () => {
    await expect(
      discoverOklahomaGuardianIeOutsideSpendingReports(
        { candidateName: "Kevin Stitt", electionYear: 2022, maxReports: 0 },
        { fetchImpl: vi.fn<typeof fetch>() }
      )
    ).rejects.toThrow("Invalid Oklahoma Guardian IE maxReports");
  });
});
