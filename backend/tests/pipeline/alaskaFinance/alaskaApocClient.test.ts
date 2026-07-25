import { describe, expect, it, vi } from "vitest";

import {
  ALASKA_APOC_CAMPAIGN_INCOME_URL,
  ALASKA_APOC_IE_CONTRIBUTIONS_URL,
  ALASKA_APOC_IE_EXPENDITURES_URL,
  fetchAlaskaApocCsv,
  fetchAlaskaApocExportCsv,
  fetchAlaskaApocFinanceCsvBundle,
  parseAlaskaApocAmount,
  parseAlaskaApocCampaignIncomeCsv,
  parseAlaskaApocDateYear,
  parseAlaskaApocIndependentContributionCsv,
  parseAlaskaApocIndependentExpenditureCsv,
} from "../../../src/pipeline/alaskaFinance/alaskaApocClient.js";

// Minimal stand-in for an APOC WebForms report page: the export CSV is only
// served after the search and export postbacks have run in the same session.
export function createApocExportChainFetch(csvByPageUrl: Record<string, string>) {
  const prefix = "M$C$csfFilter$";
  const formPage = (extra = ""): string =>
    [
      '<form name="aspnetForm" method="post">',
      '<input type="hidden" name="__VIEWSTATE" value="vs-token" />',
      '<input type="hidden" name="__VIEWSTATEGENERATOR" value="gen" />',
      `<select name="${prefix}ddlReportYear"><option value="2025">2025</option><option selected="selected" value="2026">2026</option></select>`,
      `<input name="${prefix}txtName" type="text" value="" />`,
      `<input type="submit" name="${prefix}btnSearch" value="Search" />`,
      `<input type="submit" name="${prefix}btnExport" value="Export" />`,
      extra,
      "</form>",
    ].join("");

  const searched = new Set<string>();
  return vi.fn(async (url: string, init?: RequestInit) => {
    const body = typeof init?.body === "string" ? init.body : "";
    const pageUrl = url.split("?")[0] ?? url;

    const decoded = decodeURIComponent(body.replace(/\+/g, " "));
    if (decoded.includes(`${prefix}btnSearch=Search`)) {
      searched.add(pageUrl);
      return new Response(formPage(), { status: 200, headers: { "content-type": "text/html" } });
    }
    if (decoded.includes(`${prefix}btnExport=Export`)) {
      if (!searched.has(pageUrl)) {
        return new Response(formPage(), { status: 200, headers: { "content-type": "text/html" } });
      }
      const href = `${new URL(pageUrl).pathname}?exportAll=True&amp;exportFormat=CSV&amp;isExport=True`;
      return new Response(
        formPage(`<a id="M_C_csfFilter_ExportDialog_hlAllCSV" href="${href}">.CSV</a>`),
        { status: 200, headers: { "content-type": "text/html" } }
      );
    }
    if (url.includes("isExport=True")) {
      const csv = csvByPageUrl[pageUrl];
      if (csv === undefined) {
        return new Response("missing fixture", { status: 404 });
      }
      return new Response(csv, {
        status: 200,
        headers: { "content-type": "text/comma-separated-values; charset=utf-8" },
      });
    }
    return new Response(formPage(), { status: 200, headers: { "content-type": "text/html" } });
  });
}

describe("alaskaApocClient", () => {
  it("parses APOC campaign income CSV exports", () => {
    const rows = parseAlaskaApocCampaignIncomeCsv(
      [
        "Filer,Filer Type,Name,Date,Type,Contributor/Vendor,Address,City,State,Zip,Country,Payment Type,Payment Detail,Occupation,Employer,Purpose,Amount,Submitted,Status",
        "\"Doe, Jane\",Candidate,\"Doe, Jane\",10/01/2026,Income,\"Smith, Pat\",\"1 Main St\",Juneau,AK,99801,USA,Check,1001,Attorney,\"Law Firm, LLP\",Contribution,\"$1,200.50\",10/02/2026,\"Complete, Not Amended\"",
        "\"Doe, Jane\",Candidate,\"Doe, Jane\",10/02/2026,Income,Bad Amount,,,,,,,,,,,bad,,Complete",
      ].join("\n"),
      { sourceUrl: ALASKA_APOC_CAMPAIGN_INCOME_URL }
    );

    expect(rows).toEqual([
      expect.objectContaining({
        filerName: "Doe, Jane",
        name: "Doe, Jane",
        contributor: "Smith, Pat",
        occupation: "Attorney",
        employer: "Law Firm, LLP",
        amount: 1200.5,
        reportYear: 2026,
        status: "Complete, Not Amended",
        sourceUrl: ALASKA_APOC_CAMPAIGN_INCOME_URL,
      }),
    ]);
  });

  // Header rows below are copied from real APOC "Export All Pages / .CSV"
  // downloads, which use different column names than the legacy fixtures above.
  it("parses the official APOC campaign income export column names", () => {
    const rows = parseAlaskaApocCampaignIncomeCsv(
      [
        "Result,Date,Transaction Type,Payment Type,Payment Detail,Amount,Last/Business Name,First Name,Address,City,State,Zip,Country,Occupation,Employer,Purpose of Expenditure,--------,Report Type,Election Name,Election Type,Municipality,Office,Filer Type,Name,Report Year,Submitted",
        "1,12/24/2024,Income,Check,2408,\"$1,000.00\",\"Public Employees Local 71\",,\"2510 Arctic Blvd\",Anchorage,Alaska,99503,USA,PAC,PAC,,,\"Previous Year Start Report\",\"2026 - Anchorage Municipal Election\",\"Anchorage Municipal\",\"Anchorage, City and Borough\",Assembly,Candidate,\"Dave Donley\",2026,2/9/2025",
        "2,1/15/2026,Income,Check,101,$50.00,Smith,Pat,\"1 Main St\",Juneau,Alaska,99801,USA,Attorney,\"Law Firm\",,,\"Year Start Report\",\"2026 - State General\",State,,Governor,Candidate,\"Dave Donley\",2026,2/1/2026",
      ].join("\n")
    );

    expect(rows).toEqual([
      expect.objectContaining({
        // The export has no Filer ID or Filer Name column; "Name" carries the
        // filing candidate and the resolver keys off it.
        filerId: "",
        filerName: "Dave Donley",
        name: "Dave Donley",
        filerType: "Candidate",
        type: "Income",
        contributor: "Public Employees Local 71",
        occupation: "PAC",
        amount: 1000,
        reportYear: 2026,
      }),
      expect.objectContaining({
        // Split individual-contributor columns rejoin as "Last, First".
        contributor: "Smith, Pat",
        occupation: "Attorney",
        amount: 50,
      }),
    ]);
  });

  it("parses the official APOC independent expenditure export column names", () => {
    const rows = parseAlaskaApocIndependentExpenditureCsv(
      [
        "Result,Date,Payment Type,Payment Detail,Amount,Recipient,Recipient Address,Recipient City,Recipient State,Recipient Zip,Recipient Country,Election Year,Election Name,Candidate/Proposition,Position,--------,Report Year,Filer Type,Filer Name,Submitted,Status",
        "1,5/28/2026,\"Debit Card\",,\"$3,150.00\",\"A.T. Publishing\",\"1720 Abbott Rd\",Anchorage,Alaska,99507,USA,2026,\"State General\",\"2 - Repeal\",Supports,,2026,\"Registered Group\",\"2026 - Repeal Now\",6/1/2026,Complete",
      ].join("\n")
    );

    expect(rows).toEqual([
      expect.objectContaining({
        filerName: "2026 - Repeal Now",
        recipient: "A.T. Publishing",
        city: "Anchorage",
        state: "Alaska",
        zip: "99507",
        election: "State General",
        candidateProposition: "2 - Repeal",
        position: "Supports",
        amount: 3150,
        reportYear: 2026,
      }),
    ]);
  });

  it("parses APOC independent expenditure CSV exports", () => {
    const rows = parseAlaskaApocIndependentExpenditureCsv(
      [
        "Filer Name,Filer,Filer Type,Report Year,Business Phone,Business Type,Type,Date,Recipient,Address,City,State,Zip,Country,Position,Candidate/Proposition,Description,Report Type,Election,Payment Type,Payment Detail,Amount,Submitted,Status",
        "Alaska Future PAC,8001,Group,2026,907-555-0100,Super PAC,Expenditure,09/15/2026,Vendor,1 Main,Anchorage,AK,99501,USA,Support,Jane Doe,Mailers,24-hour,General,Card,ad buy,\"$25,000.00\",09/16/2026,Complete",
      ].join("\n"),
      { sourceUrl: ALASKA_APOC_IE_EXPENDITURES_URL }
    );

    expect(rows).toEqual([
      expect.objectContaining({
        filerId: "8001",
        filerName: "Alaska Future PAC",
        reportYear: 2026,
        position: "Support",
        candidateProposition: "Jane Doe",
        amount: 25000,
        sourceUrl: ALASKA_APOC_IE_EXPENDITURES_URL,
      }),
    ]);
  });

  it("parses APOC independent expenditure contribution CSV exports", () => {
    const rows = parseAlaskaApocIndependentContributionCsv(
      [
        "Filer Name,Filer,Filer Type,Report Year,Business Phone,Business Type,Type,Date,Contributor,Contributor Address,Contributor City,Contributor State,Contributor Zip,Contributor Country,Employer,Occupation,Report Type,Election,Officers,Amount,Submitted,Status",
        "Alaska Future PAC,8001,Group,2026,907-555-0100,Super PAC,Contribution,09/01/2026,Energy Transfer LLC,2 Energy Rd,Dallas,TX,75001,USA,,,24-hour,General,,\"$30,000.00\",09/02/2026,Complete",
      ].join("\n"),
      { sourceUrl: ALASKA_APOC_IE_CONTRIBUTIONS_URL }
    );

    expect(rows).toEqual([
      expect.objectContaining({
        filerId: "8001",
        filerName: "Alaska Future PAC",
        contributor: "Energy Transfer LLC",
        amount: 30000,
        sourceUrl: ALASKA_APOC_IE_CONTRIBUTIONS_URL,
      }),
    ]);
  });

  it("parses APOC amount and date primitives conservatively", () => {
    expect(parseAlaskaApocAmount("$1,234.56")).toBe(1234.56);
    expect(parseAlaskaApocAmount("($25.00)")).toBe(-25);
    expect(parseAlaskaApocAmount("bad")).toBeNull();
    expect(parseAlaskaApocDateYear("10/01/2026")).toBe(2026);
    expect(parseAlaskaApocDateYear("2026-10-01T00:00:00")).toBe(2026);
    expect(parseAlaskaApocDateYear("")).toBeNull();
  });

  it("rejects blank APOC CSV exports instead of treating them as valid empty data", () => {
    expect(() => parseAlaskaApocCampaignIncomeCsv(" \n \n")).toThrow(
      "Alaska APOC CSV export is missing a header row"
    );
  });

  it("fetches APOC CSV exports with retry and timeout options", async () => {
    const fetchFn = vi
      .fn()
      .mockResolvedValueOnce(new Response("temporary", { status: 503 }))
      .mockResolvedValueOnce(new Response("Name,Amount\nJane,$1.00\n", { status: 200 }));

    await expect(
      fetchAlaskaApocCsv(ALASKA_APOC_CAMPAIGN_INCOME_URL, {
        fetchFn,
        timeoutMs: 1000,
        retryCount: 1,
        retryDelayMs: 0,
      })
    ).resolves.toBe("Name,Amount\nJane,$1.00\n");
    expect(fetchFn).toHaveBeenCalledTimes(2);
  });

  it("rejects non-HTTPS APOC CSV URLs", async () => {
    const fetchFn = vi.fn();

    await expect(
      fetchAlaskaApocCsv("http://aws.state.ak.us/ApocReports/CampaignDisclosure/CDIncome.aspx", {
        fetchFn,
        timeoutMs: 1000,
        retryCount: 0,
      })
    ).rejects.toThrow("Only https is allowed");
    expect(fetchFn).not.toHaveBeenCalled();
  });

  it("rejects APOC HTML report pages instead of treating them as empty CSV exports", async () => {
    const fetchFn = vi.fn().mockResolvedValue(
      new Response("<!doctype html><html><body><form><table><tr><td>No CSV here</td></tr></table></form></body></html>", {
        status: 200,
        headers: { "content-type": "text/html; charset=utf-8" },
      })
    );

    await expect(
      fetchAlaskaApocCsv(ALASKA_APOC_CAMPAIGN_INCOME_URL, {
        fetchFn,
        timeoutMs: 1000,
        retryCount: 0,
      })
    ).rejects.toThrow("returned an HTML report page instead of a CSV export");
  });

  it("does not reject CSV fields that contain HTML-like text", async () => {
    const fetchFn = vi.fn().mockResolvedValue(
      new Response("Name,Amount\n\"<form value>\",$1.00\n", {
        status: 200,
        headers: { "content-type": "text/csv" },
      })
    );

    await expect(
      fetchAlaskaApocCsv(ALASKA_APOC_CAMPAIGN_INCOME_URL, {
        fetchFn,
        timeoutMs: 1000,
        retryCount: 0,
      })
    ).resolves.toBe("Name,Amount\n\"<form value>\",$1.00\n");
  });

  it("fetches an APOC CSV bundle with source URL provenance", async () => {
    const fetchFn = createApocExportChainFetch({
      [ALASKA_APOC_CAMPAIGN_INCOME_URL]: "Name,Amount\nJane,$1.00\n",
      [ALASKA_APOC_IE_EXPENDITURES_URL]: "Name,Amount\nPAC,$2.00\n",
    });

    const bundle = await fetchAlaskaApocFinanceCsvBundle({
      fetchFn,
      retryDelayMs: 0,
      requestSpacingMs: 0,
      includeIndependentContributions: false,
    });

    expect(bundle).toMatchObject({
      incomeCsv: "Name,Amount\nJane,$1.00\n",
      independentExpenditureCsv: "Name,Amount\nPAC,$2.00\n",
      incomeSourceUrl: ALASKA_APOC_CAMPAIGN_INCOME_URL,
      independentExpenditureSourceUrl: ALASKA_APOC_IE_EXPENDITURES_URL,
      independentContributionSourceUrl: null,
    });
    // Four requests per report page: load, search, export dialog, CSV.
    expect(fetchFn).toHaveBeenCalledTimes(8);
  });

  it("runs the search and export postbacks before downloading the CSV", async () => {
    const fetchFn = createApocExportChainFetch({
      [ALASKA_APOC_CAMPAIGN_INCOME_URL]: "Name,Amount\nJane,$1.00\n",
    });

    await expect(
      fetchAlaskaApocExportCsv(ALASKA_APOC_CAMPAIGN_INCOME_URL, {
        reportYear: 2026,
        fetchFn,
        retryDelayMs: 0,
      })
    ).resolves.toBe("Name,Amount\nJane,$1.00\n");

    const bodies = fetchFn.mock.calls.map(([, init]) =>
      typeof init?.body === "string" ? decodeURIComponent(init.body.replace(/\+/g, " ")) : ""
    );
    expect(bodies[0]).toBe("");
    expect(bodies[1]).toContain("btnSearch=Search");
    // The selected report year and the page's own form state are posted back.
    expect(bodies[1]).toContain("ddlReportYear=2026");
    expect(bodies[1]).toContain("__VIEWSTATE=vs-token");
    expect(bodies[2]).toContain("btnExport=Export");
    expect(fetchFn.mock.calls[3]?.[0]).toContain("isExport=True");
    // A browser user agent is required; the WAF rejects requests without one.
    const headers = fetchFn.mock.calls[3]?.[1]?.headers as Record<string, string>;
    expect(headers["user-agent"]).toContain("Chrome/");
    expect(headers.referer).toBe(ALASKA_APOC_CAMPAIGN_INCOME_URL);
  });

  it("fails loudly when the export dialog offers no CSV download", async () => {
    const fetchFn = vi.fn(
      async () => new Response("<form><input name=\"M$C$csfFilter$btnSearch\" /></form>", { status: 200 })
    );

    await expect(
      fetchAlaskaApocExportCsv(ALASKA_APOC_CAMPAIGN_INCOME_URL, {
        reportYear: 2026,
        fetchFn,
        retryCount: 0,
        retryDelayMs: 0,
      })
    ).rejects.toThrow("Alaska APOC export dialog did not offer a CSV download");
  });

  it("rejects an invalid APOC report year before making any request", async () => {
    const fetchFn = vi.fn();

    await expect(
      fetchAlaskaApocExportCsv(ALASKA_APOC_CAMPAIGN_INCOME_URL, { reportYear: 1999, fetchFn })
    ).rejects.toThrow("Invalid Alaska APOC report year: 1999");
    expect(fetchFn).not.toHaveBeenCalled();
  });

  it("does not validate disabled independent expenditure or contribution URLs", async () => {
    const fetchFn = createApocExportChainFetch({
      [ALASKA_APOC_CAMPAIGN_INCOME_URL]: "Name,Amount\nJane,$1.00\n",
    });

    const bundle = await fetchAlaskaApocFinanceCsvBundle({
      fetchFn,
      retryDelayMs: 0,
      requestSpacingMs: 0,
      includeIndependentExpenditures: false,
      includeIndependentContributions: false,
      independentExpenditureUrl: "not a url",
      independentContributionUrl: "also not a url",
    });

    expect(bundle).toMatchObject({
      independentExpenditureSourceUrl: null,
      independentContributionSourceUrl: null,
    });
    // Only the income page is fetched, via its four-step export chain.
    expect(fetchFn).toHaveBeenCalledTimes(4);
  });
});
