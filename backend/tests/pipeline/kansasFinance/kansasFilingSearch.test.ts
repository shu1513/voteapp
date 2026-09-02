import { describe, expect, it, vi } from "vitest";

import { kansasCfrOfficeForRace } from "../../../src/pipeline/kansasFinance/kansasFinanceEligibleOffices.js";
import {
  createKansasFilingPoolLoader,
  KANSAS_CFR_FILER_SEARCHES,
  kansasGridOfficeMatches,
  searchKansasFilings,
  type KansasSearchedFiling,
} from "../../../src/pipeline/kansasFinance/kansasFilingSearch.js";

const NOW = new Date("2026-09-02T12:00:00.000Z");
const house = kansasCfrOfficeForRace({ officeScope: "state_lower", officeCanonicalName: "State Lower Chamber Legislator" })!;

const ENTRY = "https://sos.ks.gov/elections/cfr_viewer/cfr_examiner_entry.aspx";
const FORM = "https://sos.ks.gov/elections/cfr_viewer/cfr_examiner.aspx";
const RESULTS = "https://sos.ks.gov/elections/cfr_viewer/cfr_examiner_search_results.aspx";
const COVER = "https://sos.ks.gov/elections/cfr_viewer/reports/exp_report_main.aspx";
const hidden = (state: string) =>
  `<input type="hidden" name="__VIEWSTATE" value="${state}" /><input type="hidden" name="__EVENTVALIDATION" value="ev" />`;

// Synthetic filer names on purpose (25-4154(d) posture).
const RESULTS_HTML = `<span id="lblRecordCount">2</span>${hidden("results")}
  <span id="grdviewCfrResults_lblOriginalDate_0">07/27/2026</span>
  <span id="grdviewCfrResults_lblAmendmentDate_0"></span>
  <a id="grdviewCfrResults_lnkbtnName_0" href="javascript:__doPostBack(&#39;grdviewCfrResults$ctl02$lnkbtnName&#39;,&#39;&#39;)">HOLLOWAY MARGARET</a>
  <a id="grdviewCfrResults_LinkButton1_0" href="javascript:__doPostBack(&#39;grdviewCfrResults$ctl02$LinkButton1&#39;,&#39;&#39;)"></a>
  <span id="grdviewCfrResults_labelOfficeSought_0">State Representative</span>
  <span id="grdviewCfrResults_lblDistrictNumber_0">/ 85</span>
  <span id="grdviewCfrResults_lblOriginalDate_1">07/27/2026</span>
  <a id="grdviewCfrResults_lnkbtnName_1" href="javascript:__doPostBack(&#39;grdviewCfrResults$ctl03$lnkbtnName&#39;,&#39;&#39;)">MUIR DANIEL</a>
  <img id="grdviewCfrResults_paper_1" title="Paper Filing" src="../../images/pdficon_small.gif" />
  <span id="grdviewCfrResults_labelOfficeSought_1">State Representative</span>
  <span id="grdviewCfrResults_lblDistrictNumber_1">/ 2</span>`;

function fakeViewer() {
  const posts: { url: string; body: URLSearchParams }[] = [];
  const fetchImpl = vi.fn(async (url: string, init: { method: string; body?: string }) => {
    if (init.method === "POST") {
      const body = new URLSearchParams(init.body ?? "");
      posts.push({ url, body });
      const target = url === ENTRY ? FORM : body.get("__EVENTTARGET") ? COVER : RESULTS;
      return new Response(null, { status: 302, headers: { location: target } });
    }
    if (url === ENTRY) return new Response(`<form>${hidden("entry")}</form>`, { status: 200 });
    if (url === FORM) return new Response(`<form>${hidden("form")}</form>`, { status: 200 });
    if (url === RESULTS) return new Response(RESULTS_HTML, { status: 200 });
    if (url === COVER) return new Response(`<span id="lblFileStartDate">1/1/2026</span>`, { status: 200 });
    return new Response("nope", { status: 404 });
  });
  return { posts, fetchImpl };
}

describe("kansasGridOfficeMatches", () => {
  it("compares office text case- and whitespace-insensitively", () => {
    expect(kansasGridOfficeMatches(house, "STATE  REPRESENTATIVE")).toBe(true);
    expect(kansasGridOfficeMatches(house, "State Representative")).toBe(true);
    expect(kansasGridOfficeMatches(house, "Governor")).toBe(false);
  });
});

describe("searchKansasFilings", () => {
  it("returns every grid row with a handle that opens the report from its own results page", async () => {
    const viewer = fakeViewer();
    const filings = await searchKansasFilings({
      office: house,
      filingType: "Receipts and Expenditures Report",
      gridId: "grdviewCfrResults",
      startDate: "01/01/2025",
      endDate: "09/02/2026",
      sessionOptions: { fetchImpl: viewer.fetchImpl as never, spacingMs: 0 },
    });
    expect(filings.map((filing) => [filing.row.name, filing.row.channel])).toEqual([
      ["HOLLOWAY MARGARET", "efile"],
      ["MUIR DANIEL", "paper"],
    ]);
    const search = viewer.posts.find((post) => post.url === FORM)!;
    expect(Object.fromEntries(search.body)).toMatchObject({
      drpdownOffice: "7",
      txtDistrictNo: "",
      drpdownFilingType: "Receipts and Expenditures Report",
      txtStartDate: "01/01/2025",
      txtEndDate: "09/02/2026",
      btnSearch: "Submit Search",
    });

    const cover = await filings[0]!.openReport();
    expect(cover.url).toBe(COVER);
    expect(cover.html).toContain("lblFileStartDate");
    const postback = viewer.posts.find((post) => post.body.get("__EVENTTARGET"))!;
    expect(postback.url).toBe(RESULTS);
    expect(Object.fromEntries(postback.body)).toMatchObject({
      __VIEWSTATE: "results",
      __EVENTTARGET: "grdviewCfrResults$ctl02$lnkbtnName",
      __EVENTARGUMENT: "",
    });
  });

  it("refuses to open a paper row (its name link answers 500 live)", async () => {
    const viewer = fakeViewer();
    const filings = await searchKansasFilings({
      office: house,
      filingType: "Receipts and Expenditures Report",
      gridId: "grdviewCfrResults",
      startDate: "01/01/2025",
      endDate: "09/02/2026",
      sessionOptions: { fetchImpl: viewer.fetchImpl as never, spacingMs: 0 },
    });
    const postsBefore = viewer.posts.length;
    await expect(filings[1]!.openReport()).rejects.toThrow('no HTML report for paper filing "MUIR DANIEL"');
    expect(viewer.posts).toHaveLength(postsBefore);
  });
});

describe("createKansasFilingPoolLoader", () => {
  const searched = (name: string, officeSought: string): KansasSearchedFiling => ({
    row: {
      index: 0,
      fileDate: "07/27/2026",
      amendmentDate: "",
      amendmentNo: "",
      name,
      officeSought,
      district: "85",
      channel: "efile",
      postbackTarget: null,
    },
    openReport: () => Promise.reject(new Error("not opened")),
  });

  it("runs the three searches once per office + cycle, tags kinds, and drops other-office rows", async () => {
    const onSkippedRows = vi.fn();
    const search = vi.fn(async (input: { filingType: string }) => [
      searched("HOLLOWAY MARGARET", input.filingType === "Appointment of Treasurer" ? "STATE REPRESENTATIVE" : "State Representative"),
      searched("STRAY ROW", "Governor"),
    ]);
    const load = createKansasFilingPoolLoader({ now: NOW, search, onSkippedRows });
    const [first, second] = await Promise.all([load(house, 2026), load(house, 2026)]);
    expect(first).toBe(second);
    expect(search).toHaveBeenCalledTimes(3);
    expect(search.mock.calls.map(([input]) => [input.filingType, input.gridId])).toEqual(
      KANSAS_CFR_FILER_SEARCHES.map((entry) => [entry.filingType, entry.gridId])
    );
    expect(search.mock.calls[0]![0]).toMatchObject({ office: house, startDate: "01/01/2025", endDate: "09/02/2026" });
    expect(first.map((filing) => [filing.row.name, filing.filingKind])).toEqual([
      ["HOLLOWAY MARGARET", "report"],
      ["HOLLOWAY MARGARET", "appointment_of_treasurer"],
      ["HOLLOWAY MARGARET", "affidavit"],
    ]);
    expect(onSkippedRows).toHaveBeenCalledWith(house, 3);
  });

  it("keys a special cycle start separately and searches from its January 1", async () => {
    const search = vi.fn(async () => []);
    const load = createKansasFilingPoolLoader({ now: NOW, search });
    await load(house, 2026);
    await load(house, 2026, 2025);
    await load(house, 2026, 2026);
    expect(search).toHaveBeenCalledTimes(9);
    expect(search.mock.calls[3]![0]).toMatchObject({ startDate: "01/01/2025" });
    expect(search.mock.calls[6]![0]).toMatchObject({ startDate: "01/01/2026" });
  });
});
