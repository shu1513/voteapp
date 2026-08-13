import { readFile } from "node:fs/promises";
import path from "node:path";

import { describe, expect, it } from "vitest";

import {
  ERTS_CONTRIBUTION_EXPORT_COLUMNS,
  ERTS_CONTRIBUTION_TYPE_CODES,
  cf2SummaryValues,
  classifyContributionSearchResult,
  ertsContributionReportUrl,
  ertsExpenditureReportUrl,
  ertsHiddenFields,
  ertsSelectDefaults,
  parseCf8IndexPage,
  parseCf8Pager,
  parseCsv,
  parseMoneyToCents,
  parseSummaryGroupings,
} from "../../src/scripts/probeRhodeIslandCandidateFinance.js";

// Fixtures are trimmed captures of the live ERTS pages taken during the PR 3
// acquisition spike (2026-08-13). They pin the shapes the spike proved so a
// silent portal change shows up as a test failure rather than as wrong money.
const FIXTURE_DIR = path.join(import.meta.dirname, "../fixtures/rhodeIslandFinance");

function fixture(name: string): Promise<string> {
  return readFile(path.join(FIXTURE_DIR, name), "utf8");
}

describe("parseMoneyToCents", () => {
  it("reads the portal's and the CF-2's money formats", () => {
    expect(parseMoneyToCents("$ 1,019,993.37")).toBe(101_999_337);
    expect(parseMoneyToCents("202,028.63")).toBe(20_202_863);
    expect(parseMoneyToCents("0")).toBe(0);
    // CF-2 page 1 prints negative amounts in parentheses.
    expect(parseMoneyToCents("(3,500.00)")).toBe(-350_000);
    // The detail export prints four decimals.
    expect(parseMoneyToCents("250.0000")).toBe(25_000);
    expect(parseMoneyToCents("1063.5000")).toBe(106_350);
  });

  it("rejects sub-cent precision rather than rounding it away", () => {
    expect(parseMoneyToCents("250.0050")).toBeNull();
  });

  it("returns null for anything that is not an amount", () => {
    expect(parseMoneyToCents("")).toBeNull();
    expect(parseMoneyToCents("Total")).toBeNull();
    expect(parseMoneyToCents("04/01/2026")).toBeNull();
  });
});

describe("parseSummaryGroupings", () => {
  it("reads the contribution report's official per-type totals", async () => {
    const totals = parseSummaryGroupings(await fixture("contribution-report-summary.html"), "dgrReport");
    expect(Object.fromEntries(totals)).toEqual({
      Individual: 24_126_429,
      PAC: 1_245_000,
      "Interest Received": 511_677,
      "In-Kind - Individual": 350_800,
      "Other Receipt": 11_395,
    });
  });

  it("reads the expenditure report's total", async () => {
    const totals = parseSummaryGroupings(await fixture("expenditure-report-summary.html"), "dgrExpenditureSummary");
    expect(Object.fromEntries(totals)).toEqual({ "Campaign Expenditure": 94_543_457 });
  });
});

describe("parseCsv", () => {
  it("parses the detail export with its pinned column list", async () => {
    const rows = parseCsv(await fixture("contribution-export-sample.csv"));
    expect(rows[0]).toEqual([...ERTS_CONTRIBUTION_EXPORT_COLUMNS]);
    expect(rows).toHaveLength(5);
    const contDesc = rows[0].indexOf("ContDesc");
    expect(rows.slice(1).map((row) => row[contDesc])).toEqual([
      "Individual",
      "PAC",
      "Interest Received",
      "In-Kind - Individual",
    ]);
    // Every itemized type the export can carry must exist in the pinned
    // vocabulary, or decision 13's mapping table has a hole.
    for (const row of rows.slice(1)) {
      expect(ERTS_CONTRIBUTION_TYPE_CODES[row[contDesc]]).toBeTypeOf("number");
    }
  });

  it("keeps commas inside quoted fields", () => {
    expect(parseCsv('a,b\n1,"Murray, Paul S."\n')).toEqual([
      ["a", "b"],
      ["1", "Murray, Paul S."],
    ]);
  });
});

describe("cf2SummaryValues", () => {
  // CF-2 page 1 is a two-column form: the ending-balance amount sits on the
  // same baseline as an unrelated left-column label, so "nearest amount to the
  // right" is what makes the mapping correct.
  const items = [
    { text: "7. Interest Received", x: 48, y: 327 },
    { text: "3,373.87", x: 254, y: 327 },
    { text: "$ 1,110,487.44", x: 526, y: 327 },
    { text: "6. Report of In-Kind Contributions", x: 300, y: 357 },
    { text: "31.00", x: 559, y: 357 },
    { text: "1. Beginning Cash Balance", x: 24, y: 471 },
    { text: "$ 1,019,993.37", x: 232, y: 471 },
  ];

  it("binds each label to the nearest amount on its baseline", () => {
    const values = cf2SummaryValues(items, [
      "1. Beginning Cash Balance",
      "7. Interest Received",
      "6. Report of In-Kind Contributions",
    ]);
    expect(values.get("7. Interest Received")).toBe(337_387);
    expect(values.get("6. Report of In-Kind Contributions")).toBe(3_100);
    expect(values.get("1. Beginning Cash Balance")).toBe(101_999_337);
  });

  it("omits labels the page does not carry", () => {
    expect(cf2SummaryValues(items, ["11. Matching Public Funds"]).size).toBe(0);
  });
});

describe("parseCf8IndexPage", () => {
  it("reads the Other Filings grid and absolutizes the scan links", async () => {
    const rows = parseCf8IndexPage(await fixture("cf8-index-page1.html"));
    expect(rows.length).toBeGreaterThan(0);
    expect(rows.every((row) => row.scannedUrl?.startsWith("https://www.ricampaignfinance.com/ReportsScanned/"))).toBe(
      true
    );
    // Filing type drives decision 6's gate: only INDEPENDENT EXPENDITURE rows
    // can ever produce candidate outside rows.
    expect(rows.some((row) => row.filingType === "INDEPENDENT EXPENDITURE")).toBe(true);
    expect(rows.every((row) => row.filedDate !== "" && row.organizationName !== "")).toBe(true);
  });
});

describe("classifyContributionSearchResult", () => {
  it("recognizes a result grid, the portal's no-rows message, and nothing else", () => {
    expect(classifyContributionSearchResult('<table id="dgrContribution"><tr></tr></table>')).toBe("rows");
    expect(
      classifyContributionSearchResult("<p>No Contributions were found for the Search criteria you entered</p>")
    ).toBe("no_rows");
    // A Cloudflare challenge or error page must never read as "no rows" —
    // that verdict is what lets gate 2 accept a type's absence from the
    // export.
    expect(classifyContributionSearchResult("<html><body>Checking your browser…</body></html>")).toBe("unreadable");
    expect(classifyContributionSearchResult("")).toBe("unreadable");
  });
});

describe("parseCf8Pager", () => {
  it("reads the current page and the numbered pager links", async () => {
    const pager = parseCf8Pager(await fixture("cf8-index-page1.html"));
    expect(pager.currentPage).toBe(1);
    expect(pager.links.map((link) => link.label)).toEqual(["2", "3", "4", "5", "6", "7", "8", "9", "10", "..."]);
    // Control ids are positional, so page "2" is ctl01 — following ids in
    // order would walk backwards into page 1.
    expect(pager.links[0].target).toBe("dgdCF8FilingList$ctl14$ctl01");
  });
});

describe("report URLs", () => {
  it("builds the portal's own contribution report URL", () => {
    const url = new URL(ertsContributionReportUrl({ orgId: "2235", begin: "04/01/2026", end: "06/30/2026" }));
    expect(url.pathname).toBe("/RIPublic/Reporting/TransactionReport.aspx");
    expect(url.searchParams.get("OrgID")).toBe("2235");
    expect(url.searchParams.get("ContType")).toBe("0");
    expect(url.searchParams.get("ReportType")).toBe("Contrib");
  });

  it("passes a contribution-type filter through", () => {
    const url = new URL(
      ertsContributionReportUrl({
        orgId: "2235",
        begin: "04/01/2026",
        end: "06/30/2026",
        contributionTypeCode: ERTS_CONTRIBUTION_TYPE_CODES["Other Receipt"],
      })
    );
    expect(url.searchParams.get("ContType")).toBe("17");
  });

  it("rejects a non-numeric organization key and a malformed date", () => {
    expect(() => ertsContributionReportUrl({ orgId: "2235; DROP", begin: "04/01/2026", end: "06/30/2026" })).toThrow(
      /Invalid ERTS organization key/
    );
    expect(() => ertsExpenditureReportUrl({ orgId: "2235", begin: "2026-04-01", end: "06/30/2026" })).toThrow(
      /Invalid BeginDate/
    );
  });
});

describe("WebForms field capture", () => {
  const html =
    '<input type="hidden" name="__VIEWSTATE" value="a&amp;b" />' +
    '<input type="hidden" name="__EVENTVALIDATION" value="" />' +
    '<select name="lstContributionType"><option value="0">All</option>' +
    '<option value="2" selected>Individual</option></select>' +
    '<select name="lstSort1"><option value="ReceiptDate">Date</option></select>';

  it("captures hidden fields with entities decoded", () => {
    expect(ertsHiddenFields(html)).toEqual({ __VIEWSTATE: "a&b", __EVENTVALIDATION: "" });
  });

  it("carries each dropdown's selected option, falling back to the first", () => {
    expect(ertsSelectDefaults(html)).toEqual({ lstContributionType: "2", lstSort1: "ReceiptDate" });
  });
});
