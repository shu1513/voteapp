import { mkdtemp } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it, vi } from "vitest";

import { acquireMissouriMecCandidateFinanceArtifacts } from "../../../src/pipeline/missouriFinance/missouriMecArtifactAcquisition.js";
import type { MissouriMecResponse, MissouriMecSession } from "../../../src/pipeline/missouriFinance/missouriMecClient.js";
import { MISSOURI_MEC_CONTRIBUTION_EXPORT_HEADER, MISSOURI_MEC_EXPENDITURE_EXPORT_HEADER } from "../../../src/pipeline/missouriFinance/missouriMecParsers.js";

const state = `<input type="hidden" name="__VIEWSTATE" value="state"><input type="hidden" name="__EVENTVALIDATION" value="validation">`;
const committee = `${state}<span id="x_lblMECID">C263985</span><span id="x_lblCommName">Jane for Missouri</span><span id="x_lblCandName">Jane Doe</span>
  <span id="x_gvElecHistory_lblElecYear_0">11/3/2026</span><span id="x_gvElecHistory_lblElectionType_0">General Election</span>
  <span id="x_gvElecHistory_lblSub_0">State Representative</span><span id="x_gvElecHistory_lblPolSub_0">Missouri House</span>`;
const reports = `${state}<span id="x_grvReportOutside_lblYear_0">2026</span>`;
const inventory = `${state}<a id="x_grvReports_0_hlink_0" data-CPID="1"></a><span id="x_grvReports_0_lblReport_0">July Quarterly Report</span><span id="x_grvReports_0_lblDateReceived_0">7/15/2026</span>`;
const table = (header: readonly string[]) => `<table><tr>${header.map((value) => `<th>${value}</th>`).join("")}</tr></table>`;

function response(body: string, contentType = "text/html"): MissouriMecResponse {
  return { status: 200, contentType, contentDisposition: null, redirectLocation: null, body: Buffer.from(body), text: () => body };
}

describe("acquireMissouriMecCandidateFinanceArtifacts", () => {
  it("walks WebForms reports + exact-MECID exports and installs a complete cache", async () => {
    const cacheDir = await mkdtemp(join(tmpdir(), "mo-acquire-"));
    const get = vi.fn()
      .mockResolvedValueOnce(response(committee))
      .mockResolvedValueOnce(response(state))
      .mockResolvedValueOnce(response(state))
      .mockResolvedValueOnce(response(state))
      .mockResolvedValueOnce(response(state));
    const postForm = vi.fn()
      .mockResolvedValueOnce(response(reports))
      .mockResolvedValueOnce(response(inventory))
      .mockResolvedValueOnce(response(state))
      .mockResolvedValueOnce(response(state))
      .mockResolvedValueOnce(response("CF12_ContrExpendResults.aspx"))
      .mockResolvedValueOnce(response(table(MISSOURI_MEC_CONTRIBUTION_EXPORT_HEADER), "application/vnd.ms-excel"))
      .mockResolvedValueOnce(response(state))
      .mockResolvedValueOnce(response(state))
      .mockResolvedValueOnce(response("CF12_ContrExpendResults.aspx"))
      .mockResolvedValueOnce(response(table(MISSOURI_MEC_EXPENDITURE_EXPORT_HEADER), "application/vnd.ms-excel"));
    const result = await acquireMissouriMecCandidateFinanceArtifacts({
      mecid: "c263985", year: 2026, cacheDir, session: { get, postForm } as MissouriMecSession,
      now: new Date("2026-08-19T00:00:00Z"),
    });
    expect(result).toEqual({ mecid: "C263985", year: 2026, reportCount: 1, contributionCount: 0, expenditureCount: 0 });
    expect(postForm).toHaveBeenCalledWith(expect.any(String), expect.objectContaining({
      "ctl00$ctl00$ContentPlaceHolder$ContentPlaceHolder1$txtCommID": "C263985",
      "ctl00$ctl00$ContentPlaceHolder$ContentPlaceHolder1$ddYear": "2026",
    }), expect.any(Object));
    expect(postForm).toHaveBeenCalledWith(expect.any(String), expect.objectContaining({
      "ctl00$ctl00$ContentPlaceHolder$ContentPlaceHolder1$grvReportOutside$ctl02$ImgRptRight.x": "1",
    }), expect.any(Object));
  });
});
