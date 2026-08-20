import { mkdtemp } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it, vi } from "vitest";

import {
  acquireMissouriMecCandidateFinanceArtifacts,
  acquireMissouriMecOutsideSpendingArtifacts,
  acquireMissouriMecOutsideSpenderContributionArtifacts,
} from "../../../src/pipeline/missouriFinance/missouriMecArtifactAcquisition.js";
import {
  readMissouriMecOutsideSpenderContributionArtifacts,
  readMissouriMecOutsideSpendingArtifacts,
} from "../../../src/pipeline/missouriFinance/missouriMecArtifactCache.js";
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

function redirect(location: string): MissouriMecResponse {
  return { status: 302, contentType: "text/html", contentDisposition: null, redirectLocation: location, body: Buffer.alloc(0), text: () => "" };
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

describe("acquireMissouriMecOutsideSpendingArtifacts", () => {
  it("proves grid/export ordering and stores link-derived spender MECIDs", async () => {
    const cacheDir = await mkdtemp(join(tmpdir(), "mo-outside-acquire-"));
    const search = state;
    const results = `${state}<span>1 records found</span>
      <span id="x_grvExpenditures_lblName_0">Jane Doe 10 Private St</span>
      <span id="x_grvExpenditures_lblSought_0">State Representative</span>
      <span id="x_grvExpenditures_lblSupp_0">Support</span>
      <span id="x_grvExpenditures_lblDate_0">10/20/2026</span>
      <span id="x_grvExpenditures_lblAmount_0">$25.00</span>
      <a id="x_grvExpenditures_lbtnCommittee_0" href="javascript:__doPostBack(&#39;ctl00$grid$ctl02$lbtnCommittee&#39;,&#39;&#39;)">Example PAC</a>
      <span id="x_grvExpenditures_lblReport_0">8 Day Before General Election</span>`;
    const outsideExport = `<table><tr>${[
      "Candidates Name and Address", "Office Sought", "Support/Oppose", "Date", "Amount",
      "Reporting Committee", "Report",
    ].map((value) => `<th>${value}</th>`).join("")}</tr><tr>${[
      "Jane Doe 10 Private St", "State Representative", "Support", "10/20/2026", "$25.00",
      "Example PAC", "8 Day Before General Election",
    ].map((value) => `<td>${value}</td>`).join("")}</tr></table>`;
    const get = vi.fn().mockResolvedValueOnce(response(search));
    const postForm = vi.fn()
      .mockResolvedValueOnce(response(results))
      .mockResolvedValueOnce(response(outsideExport, "application/vnd.ms-excel"))
      .mockResolvedValueOnce(redirect("/MEC/Campaign_Finance/CommInfo.aspx?mecid=C123456"));
    await expect(acquireMissouriMecOutsideSpendingArtifacts({
      year: 2026, cacheDir, session: { get, postForm } as MissouriMecSession,
      now: new Date("2026-08-19T00:00:00Z"),
    })).resolves.toEqual({ year: 2026, rowCount: 1, spenderCount: 1, unresolvedSpenderCount: 0 });
    expect(postForm).toHaveBeenCalledWith(expect.any(String), expect.objectContaining({
      __EVENTTARGET: "ctl00$grid$ctl02$lbtnCommittee",
    }), expect.any(Object));
    await expect(readMissouriMecOutsideSpendingArtifacts({ cacheDir, year: 2026 })).resolves.toMatchObject({
      identities: [{ reportingCommittee: "Example PAC", mecid: "C123456" }],
    });
  });

  it("uses unique case-folded MECID/name activity evidence when paged grid links are broken", async () => {
    const cacheDir = await mkdtemp(join(tmpdir(), "mo-outside-fallback-"));
    const gridRow = (name: string, committeeName: string, page: number, next = false) => `${state}
      <span id="x_grvExpenditures_lblName_0">${name} 10 Private St</span>
      <span id="x_grvExpenditures_lblSought_0">State Representative</span>
      <span id="x_grvExpenditures_lblSupp_0">Support</span>
      <span id="x_grvExpenditures_lblDate_0">10/20/2026</span>
      <span id="x_grvExpenditures_lblAmount_0">$25.00</span>
      <a id="x_grvExpenditures_lbtnCommittee_0" href="javascript:__doPostBack(&#39;ctl00$grid$ctl02$lbtnCommittee&#39;,&#39;&#39;)">${committeeName}</a>
      <span id="x_grvExpenditures_lblReport_0">8 Day Before General Election</span>
      <span id="x_grvExpenditures_CurrentPage"><font>${page}</font></span>
      ${next ? '<a id="x_grvExpenditures_lbtnNextPage" href="javascript:__doPostBack(&#39;ctl00$grid$ctl28$lbtnNextPage&#39;,&#39;&#39;)">Next</a>' : ""}`;
    const firstPage = `<span>2 records found</span>${gridRow("Jane Doe", "First PAC", 1, true)}`;
    const secondPage = gridRow("Jane Roe", "Second PAC", 2);
    const outsideExport = `<table><tr>${[
      "Candidates Name and Address", "Office Sought", "Support/Oppose", "Date", "Amount", "Reporting Committee", "Report",
    ].map((value) => `<th>${value}</th>`).join("")}</tr>${[
      ["Jane Doe 10 Private St", "First PAC"], ["Jane Roe 10 Private St", "Second PAC"],
    ].map(([name, committeeName]) => `<tr>${[
      name, "State Representative", "Support", "10/20/2026", "$25.00", committeeName, "8 Day Before General Election",
    ].map((value) => `<td>${value}</td>`).join("")}</tr>`).join("")}</table>`;
    const activity = `${state}<table id="x_gvAdvanced"><tr>${[
      "Status Date", "MECID", "Committee Name", "Committee Type", "Committee Candidate", "Committee Status",
    ].map((value) => `<th>${value}</th>`).join("")}</tr><tr>${[
      "8/19/2026", "C654321", "Second Pac", "Political Action", "", "Active",
    ].map((value) => `<td>${value}</td>`).join("")}</tr></table>`;
    const get = vi.fn().mockResolvedValueOnce(response(state)).mockResolvedValueOnce(response(state));
    const postForm = vi.fn()
      .mockResolvedValueOnce(response(firstPage))
      .mockResolvedValueOnce(response(outsideExport, "application/vnd.ms-excel"))
      .mockResolvedValueOnce(redirect("/MEC/Campaign_Finance/CommInfo.aspx?mecid=C123456"))
      .mockResolvedValueOnce(response(secondPage))
      .mockResolvedValueOnce(response(state))
      .mockResolvedValueOnce(response(activity));
    await expect(acquireMissouriMecOutsideSpendingArtifacts({
      year: 2026, cacheDir, session: { get, postForm } as MissouriMecSession,
      now: new Date("2026-08-19T00:00:00Z"),
    })).resolves.toEqual({ year: 2026, rowCount: 2, spenderCount: 2, unresolvedSpenderCount: 0 });
    await expect(readMissouriMecOutsideSpendingArtifacts({ cacheDir, year: 2026 })).resolves.toMatchObject({
      identities: expect.arrayContaining([
        { reportingCommittee: "First PAC", mecid: "C123456" },
        { reportingCommittee: "Second PAC", mecid: "C654321" },
      ]),
    });
    expect(postForm.mock.calls.filter((call) => call[1]?.__EVENTTARGET === "ctl00$grid$ctl02$lbtnCommittee")).toHaveLength(1);
  });

  it("does not apply one row's direct MECID to later rows that only share its committee name", async () => {
    const cacheDir = await mkdtemp(join(tmpdir(), "mo-outside-row-scope-"));
    const grid = (candidate: string, page: number, next: boolean) => `${state}
      <span id="x_grvExpenditures_lblName_0">${candidate} 10 Private St</span>
      <span id="x_grvExpenditures_lblSought_0">State Representative</span>
      <span id="x_grvExpenditures_lblSupp_0">Support</span>
      <span id="x_grvExpenditures_lblDate_0">10/20/2026</span>
      <span id="x_grvExpenditures_lblAmount_0">$25.00</span>
      <a id="x_grvExpenditures_lbtnCommittee_0" href="javascript:__doPostBack(&#39;ctl00$grid$ctl02$lbtnCommittee&#39;,&#39;&#39;)">Reused PAC</a>
      <span id="x_grvExpenditures_lblReport_0">8 Day Before General Election</span>
      <span id="x_grvExpenditures_CurrentPage"><font>${page}</font></span>
      ${next ? '<a id="x_grvExpenditures_lbtnNextPage" href="javascript:__doPostBack(&#39;ctl00$grid$ctl28$lbtnNextPage&#39;,&#39;&#39;)">Next</a>' : ""}`;
    const firstPage = `<span>2 records found</span>${grid("Jane Doe", 1, true)}`;
    const secondPage = grid("Jane Roe", 2, false);
    const outsideExport = `<table><tr>${[
      "Candidates Name and Address", "Office Sought", "Support/Oppose", "Date", "Amount", "Reporting Committee", "Report",
    ].map((value) => `<th>${value}</th>`).join("")}</tr>${["Jane Doe", "Jane Roe"].map((candidate) => `<tr>${[
      `${candidate} 10 Private St`, "State Representative", "Support", "10/20/2026", "$25.00", "Reused PAC", "8 Day Before General Election",
    ].map((value) => `<td>${value}</td>`).join("")}</tr>`).join("")}</table>`;
    const activityNames = ["Reused Pac", "REUSED PAC"];
    const activity = `${state}<table id="x_gvAdvanced"><tr>${[
      "Status Date", "MECID", "Committee Name", "Committee Type", "Committee Candidate", "Committee Status",
    ].map((value) => `<th>${value}</th>`).join("")}</tr>${["C123456", "C654321"].map((mecid, index) => `<tr>${[
      "8/19/2026", mecid, activityNames[index], "Political Action", "", "Active",
    ].map((value) => `<td>${value}</td>`).join("")}</tr>`).join("")}</table>`;
    const get = vi.fn().mockResolvedValueOnce(response(state)).mockResolvedValueOnce(response(state));
    const postForm = vi.fn()
      .mockResolvedValueOnce(response(firstPage))
      .mockResolvedValueOnce(response(outsideExport, "application/vnd.ms-excel"))
      .mockResolvedValueOnce(redirect("/MEC/Campaign_Finance/CommInfo.aspx?mecid=C123456"))
      .mockResolvedValueOnce(response(secondPage))
      .mockResolvedValueOnce(response(state))
      .mockResolvedValueOnce(response(activity));
    await expect(acquireMissouriMecOutsideSpendingArtifacts({
      year: 2026, cacheDir, session: { get, postForm } as MissouriMecSession,
      now: new Date("2026-08-19T00:00:00Z"),
    })).resolves.toEqual({ year: 2026, rowCount: 2, spenderCount: 1, unresolvedSpenderCount: 1 });
    await expect(readMissouriMecOutsideSpendingArtifacts({ cacheDir, year: 2026 })).resolves.toMatchObject({
      identities: [{ reportingCommittee: "Reused PAC", mecid: null }],
    });
  });
});

describe("acquireMissouriMecOutsideSpenderContributionArtifacts", () => {
  it("caches PAC report inventory and CF12 contributions without requiring candidate history", async () => {
    const cacheDir = await mkdtemp(join(tmpdir(), "mo-spender-acquire-"));
    const pac = `${state}<span id="x_lblMECID">C123456</span><span id="x_lblCommName">Example PAC</span>`;
    const get = vi.fn()
      .mockResolvedValueOnce(response(pac))
      .mockResolvedValueOnce(response(state))
      .mockResolvedValueOnce(response(state));
    const postForm = vi.fn()
      .mockResolvedValueOnce(response(reports))
      .mockResolvedValueOnce(response(inventory))
      .mockResolvedValueOnce(response(state))
      .mockResolvedValueOnce(response(state))
      .mockResolvedValueOnce(response("CF12_ContrExpendResults.aspx"))
      .mockResolvedValueOnce(response(table(MISSOURI_MEC_CONTRIBUTION_EXPORT_HEADER), "application/vnd.ms-excel"));
    await expect(acquireMissouriMecOutsideSpenderContributionArtifacts({
      mecid: "c123456", year: 2026, cacheDir, session: { get, postForm } as MissouriMecSession,
      now: new Date("2026-08-19T00:00:00Z"),
    })).resolves.toEqual({
      mecid: "C123456", year: 2026, committeeName: "Example PAC", reportCount: 1, contributionCount: 0,
    });
    await expect(readMissouriMecOutsideSpenderContributionArtifacts({
      cacheDir, mecid: "C123456", year: 2026,
    })).resolves.toMatchObject({ inventory: [{ reportId: "1" }], contributionRows: [] });
  });
});
