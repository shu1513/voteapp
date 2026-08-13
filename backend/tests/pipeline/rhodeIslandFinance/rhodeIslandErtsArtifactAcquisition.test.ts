import { readFileSync } from "node:fs";
import { mkdtemp } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

import { beforeEach, describe, expect, it } from "vitest";

import {
  acquireErtsCf8IndexArtifacts,
  acquireErtsOrganizationArtifacts,
  ertsCycleWindowForYear,
  ertsPdfGuidFromUrl,
  reconcileErtsContributionExport,
  selectErtsCycleCf2Filings,
} from "../../../src/pipeline/rhodeIslandFinance/rhodeIslandErtsArtifactAcquisition.js";
import { getErtsArtifactStatus } from "../../../src/pipeline/rhodeIslandFinance/rhodeIslandErtsArtifactCache.js";
import { createErtsTransport, type ErtsHttpResponse } from "../../../src/pipeline/rhodeIslandFinance/rhodeIslandErtsClient.js";
import { parseErtsFilingListPage } from "../../../src/pipeline/rhodeIslandFinance/rhodeIslandErtsParsers.js";
import { makeMinimalPdf } from "../../helpers/minimalPdf.js";

const NOW = new Date("2026-08-13T18:00:00Z");

function fixture(name: string): string {
  return readFileSync(fileURLToPath(new URL(`../../fixtures/rhodeIslandFinance/${name}`, import.meta.url)), "utf8");
}

const FILINGS_HTML = fixture("organization-filings.html");
const FILING_ROWS = parseErtsFilingListPage(FILINGS_HTML);
const EXPORT_CSV = fixture("contribution-export-sample.csv");

// Summary groupings matching the fixture export's per-type sums:
// Individual 250.00 + PAC 500.00 + Interest Received 1,149.53 +
// In-Kind - Individual 508.00. `Other Receipt` is summary-only and needs the
// typed-search proof.
const CONTRIBUTION_REPORT_HTML = `
<input type="hidden" name="__VIEWSTATE" value="vs" />
<table id="dgrReport">
  <tr><td>Summary Groupings</td><td>Total</td></tr>
  <tr><td>Individual</td><td>$250.00</td></tr>
  <tr><td>PAC</td><td>$500.00</td></tr>
  <tr><td>Interest Received</td><td>$1,149.53</td></tr>
  <tr><td>In-Kind - Individual</td><td>$508.00</td></tr>
  <tr><td>Other Receipt</td><td>$113.95</td></tr>
</table>
<table id="dgrContribution"><tr><td>rows</td></tr></table>`;

const EXPENDITURE_REPORT_HTML = `
<table id="dgrExpenditureSummary">
  <tr><td>Summary Groupings</td><td>Total</td></tr>
  <tr><td>Campaign Expenditure</td><td>$945,434.57</td></tr>
</table>
<table id="dgrExpenditure"><tr><td>rows</td></tr></table>`;

const NO_ROWS_CONTRIBUTIONS = "<p>No Contributions were found for the Search criteria you entered.</p>";
const NO_ROWS_EXPENDITURES = "<p>No Expenditures were found for the Search criteria you entered.</p>";

const PDF_BY_FILING: Record<string, { guid: string; pdf: Uint8Array }> = {
  // Filing 230557's latest version GUID comes from the committed fixture.
  "230557": {
    guid: "c3881961-09d8-4b9c-8eda-4ead34a1f842",
    pdf: makeMinimalPdf([{ text: "2. Individuals", x: 48, y: 700 }]),
  },
  "239295": {
    guid: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
    pdf: makeMinimalPdf([{ text: "5. Ending Cash Balance", x: 48, y: 700 }]),
  },
};

function versionsHtml(filingId: string): string {
  const { guid } = PDF_BY_FILING[filingId];
  return `
<table id="grdAmendments">
  <tr><td>Amendment</td><td>Desc</td><td>Date Filed</td><td></td></tr>
  <tr><td></td><td><a href="https://ricampaignfinance.com/ExportDocs/2235-RICF2-${filingId}-${guid}.pdf">report</a></td><td>Aug 12 2026 7:48PM</td><td></td></tr>
</table>`;
}

type FakePortalOptions = {
  // Serve a different filing list on the second Filings.aspx read (the
  // stability gate must then discard the bundle).
  filingsAfterHtml?: string;
  // Replace the rows-classified expenditure page (summary-drift scenarios).
  expenditureHtml?: string;
};

function makeFakePortal(options: FakePortalOptions = {}) {
  const log: string[] = [];
  let filingsReads = 0;
  const html = (body: string, finalUrl?: string): ErtsHttpResponse => ({
    status: 200,
    finalUrl: finalUrl ?? "https://www.ricampaignfinance.com/RIPublic/",
    contentType: "text/html",
    body: new TextEncoder().encode(body),
  });
  const fetch = async (url: string, body?: URLSearchParams): Promise<ErtsHttpResponse> => {
    log.push(`${body ? "POST" : "GET"} ${url}${body?.get("__EVENTTARGET") ? ` [${body.get("__EVENTTARGET")}]` : ""}`);
    const parsed = new URL(url);

    if (parsed.pathname.endsWith("/Contributions.aspx")) {
      if (!body) return html('<input type="hidden" name="__VIEWSTATE" value="vs" />');
      if (body.has("lnkSearchOrg")) return html('<input name="txtOrgLastName" /><input type="hidden" name="__VIEWSTATE" value="vs" />');
      if (body.has("lnkSubSearchOrg"))
        return html(
          '<table id="dgdOrgSearchResults"><tr><td>Name</td></tr>' +
            "<tr><td><a href=\"javascript:__doPostBack('dgdOrgSearchResults$ctl02$ctl00','')\">DANIEL J MCKEE</a></td></tr>" +
            "<tr><td><a href=\"javascript:__doPostBack('dgdOrgSearchResults$ctl03$ctl00','')\">FRIENDS OF MCKEE</a></td></tr>" +
            '</table><input type="hidden" name="__VIEWSTATE" value="vs" />'
        );
      if (body.get("__EVENTTARGET")?.startsWith("dgdOrgSearchResults"))
        return html('<input type="hidden" name="__VIEWSTATE" value="vs" />');
      if (body.has("btnSearch"))
        return html("<html>report</html>", "https://www.ricampaignfinance.com/RIPublic/Reporting/TransactionReport.aspx?OrgID=2235");
      throw new Error(`Unexpected Contributions.aspx post: ${body.toString()}`);
    }

    if (parsed.pathname.endsWith("/Filings.aspx")) {
      filingsReads += 1;
      return html(filingsReads > 1 && options.filingsAfterHtml ? options.filingsAfterHtml : FILINGS_HTML);
    }

    if (parsed.pathname.endsWith("/TransactionReport.aspx")) {
      const begin = parsed.searchParams.get("BeginDate");
      const contType = parsed.searchParams.get("ContType");
      if (body?.get("__EVENTTARGET") === "lnkExport")
        return html("<script>txtPage = 'https://www.ricampaignfinance.com/RIPublic/Reporting/DownloadFile.aspx?path=x&file=y.csv';</script>");
      if (contType === "17") return html(NO_ROWS_CONTRIBUTIONS);
      if (contType !== "0") throw new Error(`Unexpected typed search: ContType=${contType}`);
      return html(begin === "07/01/2026" ? CONTRIBUTION_REPORT_HTML : NO_ROWS_CONTRIBUTIONS);
    }

    if (parsed.pathname.endsWith("/DownloadFile.aspx")) {
      if (body?.get("__EVENTTARGET") === "hypFileDownload") return html(EXPORT_CSV);
      return html('<a id="hypFileDownload">download</a><input type="hidden" name="__VIEWSTATE" value="vs" />');
    }

    if (parsed.pathname.endsWith("/ExpenditureReport.aspx")) {
      const begin = parsed.searchParams.get("BeginDate");
      return html(begin === "07/01/2026" ? (options.expenditureHtml ?? EXPENDITURE_REPORT_HTML) : NO_ROWS_EXPENDITURES);
    }

    if (parsed.pathname.endsWith("/FilingAmendmentSelect.aspx")) {
      return html(versionsHtml(parsed.searchParams.get("FilingID") as string));
    }

    if (parsed.pathname.startsWith("/ExportDocs/")) {
      const filingId = /RICF2-(\d+)-/.exec(parsed.pathname)?.[1] as string;
      return { status: 200, finalUrl: url, contentType: "application/pdf", body: PDF_BY_FILING[filingId].pdf };
    }

    throw new Error(`Unexpected portal request: ${url}`);
  };
  return { fetch, log };
}

function makeTransport(portal: ReturnType<typeof makeFakePortal>) {
  return createErtsTransport({ fetch: portal.fetch, sleep: async () => {} });
}

const MCKEE = { orgId: "2235", searchLastName: "McKee", organizationName: "DANIEL J MCKEE" };
const CYCLE = ertsCycleWindowForYear(2026);

let cacheDir: string;

beforeEach(async () => {
  cacheDir = await mkdtemp(join(tmpdir(), "ri-erts-acq-"));
});

describe("ertsCycleWindowForYear", () => {
  it("spans odd-year January through even-year December", () => {
    expect(CYCLE).toEqual({ beginUs: "01/01/2025", endUs: "12/31/2026" });
    expect(() => ertsCycleWindowForYear(2025)).toThrow(/even election year/);
  });
});

describe("ertsPdfGuidFromUrl", () => {
  it("reads the version GUID and rejects URLs without one", () => {
    expect(
      ertsPdfGuidFromUrl("https://ricampaignfinance.com/ExportDocs/2235-RICF2-230557-C3881961-09D8-4B9C-8EDA-4EAD34A1F842.pdf")
    ).toBe("c3881961-09d8-4b9c-8eda-4ead34a1f842");
    expect(() => ertsPdfGuidFromUrl("https://ricampaignfinance.com/ExportDocs/report.pdf")).toThrow(/no GUID/);
  });
});

describe("selectErtsCycleCf2Filings", () => {
  it("keeps filed in-cycle CF-2 filings and counts everything else", () => {
    const selection = selectErtsCycleCf2Filings({
      rows: FILING_ROWS,
      cycleBeginIso: "2025-01-01",
      cycleEndIso: "2026-12-31",
    });
    expect(selection.selected.map((row) => row.filingId).sort()).toEqual(["230557", "239295"]);
    expect(selection.unfiledRowCount).toBe(1);
    expect(selection.nonCf2FiledRowCount).toBe(1);
    expect(selection.outOfCycleRowCount).toBe(0);
  });

  it("excludes a CF-2 whose period predates the cycle", () => {
    const selection = selectErtsCycleCf2Filings({
      rows: FILING_ROWS,
      cycleBeginIso: "2027-01-01",
      cycleEndIso: "2028-12-31",
    });
    expect(selection.selected).toEqual([]);
    expect(selection.outOfCycleRowCount).toBe(2);
  });
});

describe("reconcileErtsContributionExport", () => {
  const summaryOf = (entries: Record<string, number>) => new Map(Object.entries(entries));
  const baseSummary = {
    Individual: 25_000,
    PAC: 50_000,
    "Interest Received": 114_953,
    "In-Kind - Individual": 50_800,
  };

  it("passes when every grouping is cent-exact or proven summary-only", async () => {
    const portal = makeFakePortal();
    const result = await reconcileErtsContributionExport({
      transport: makeTransport(portal),
      orgId: "2235",
      begin: "07/01/2026",
      end: "08/11/2026",
      summary: summaryOf({ ...baseSummary, "Other Receipt": 11_395 }),
      csvText: EXPORT_CSV,
    });
    expect(result.exportRowCount).toBe(4);
    expect(result.centExactTypeCount).toBe(4);
    expect(result.confirmedSummaryOnlyLabels).toEqual(["Other Receipt"]);
  });

  it("fails on a cent mismatch — the only silent-truncation control", async () => {
    const portal = makeFakePortal();
    await expect(
      reconcileErtsContributionExport({
        transport: makeTransport(portal),
        orgId: "2235",
        begin: "07/01/2026",
        end: "08/11/2026",
        summary: summaryOf({ ...baseSummary, Individual: 99_999 }),
        csvText: EXPORT_CSV,
      })
    ).rejects.toThrow(/Individual: export 25000 cents != summary 99999 cents/);
  });

  it("fails on a summary label outside the pinned search vocabulary", async () => {
    const portal = makeFakePortal();
    await expect(
      reconcileErtsContributionExport({
        transport: makeTransport(portal),
        orgId: "2235",
        begin: "07/01/2026",
        end: "08/11/2026",
        summary: summaryOf({ ...baseSummary, "NSF Check": -10_000 }),
        csvText: EXPORT_CSV,
      })
    ).rejects.toThrow(/NSF Check: absent from the export and outside the pinned search vocabulary/);
  });

  it("fails on an export-only type missing from the summary", async () => {
    const portal = makeFakePortal();
    await expect(
      reconcileErtsContributionExport({
        transport: makeTransport(portal),
        orgId: "2235",
        begin: "07/01/2026",
        end: "08/11/2026",
        summary: summaryOf({ Individual: 25_000, PAC: 50_000, "Interest Received": 114_953 }),
        csvText: EXPORT_CSV,
      })
    ).rejects.toThrow(/In-Kind - Individual: in the export but missing from the summary groupings/);
  });
});

describe("acquireErtsOrganizationArtifacts", () => {
  it("fetches, gates and installs one organization's cycle bundle", async () => {
    const portal = makeFakePortal();
    const result = await acquireErtsOrganizationArtifacts({
      transport: makeTransport(portal),
      cacheDir,
      organization: MCKEE,
      cycle: CYCLE,
      retrievedAt: NOW,
    });

    expect(result.selectedFilingCount).toBe(2);
    expect(result.periods).toHaveLength(2);
    const withRows = result.periods.find((period) => period.beginIso === "2026-07-01");
    expect(withRows).toMatchObject({
      contributionClassification: "rows",
      expenditureClassification: "rows",
      exportRowCount: 4,
      confirmedSummaryOnlyLabels: ["Other Receipt"],
    });
    const noRows = result.periods.find((period) => period.beginIso === "2025-10-01");
    expect(noRows).toMatchObject({
      contributionClassification: "no_rows",
      expenditureClassification: "no_rows",
      exportRowCount: null,
    });
    expect(result.fetchedPdfCount).toBe(2);
    expect(result.skippedPdfCount).toBe(0);

    // The bundle is installed and ready.
    for (const key of [
      { type: "organization_search", query: "McKee" } as const,
      { type: "organization_filings", orgId: "2235" } as const,
      { type: "contribution_report", orgId: "2235", beginIso: "2026-07-01", endIso: "2026-08-11" } as const,
      { type: "contribution_export", orgId: "2235", beginIso: "2026-07-01", endIso: "2026-08-11" } as const,
      { type: "expenditure_report", orgId: "2235", beginIso: "2026-07-01", endIso: "2026-08-11" } as const,
      { type: "contribution_report", orgId: "2235", beginIso: "2025-10-01", endIso: "2025-12-31" } as const,
      { type: "filing_versions", filingId: "230557" } as const,
      { type: "filing_pdf", filingId: "230557", guid: PDF_BY_FILING["230557"].guid } as const,
      { type: "filing_pdf", filingId: "239295", guid: PDF_BY_FILING["239295"].guid } as const,
    ]) {
      expect((await getErtsArtifactStatus({ cacheDir, key })).status, JSON.stringify(key)).toBe("ready");
    }
    // The no-rows period has no export artifact.
    expect(
      (
        await getErtsArtifactStatus({
          cacheDir,
          key: { type: "contribution_export", orgId: "2235", beginIso: "2025-10-01", endIso: "2025-12-31" },
        })
      ).status
    ).toBe("missing");
  });

  it("skips re-fetching an immutable version PDF on the next run", async () => {
    await acquireErtsOrganizationArtifacts({
      transport: makeTransport(makeFakePortal()),
      cacheDir,
      organization: MCKEE,
      cycle: CYCLE,
      retrievedAt: NOW,
    });
    const portal = makeFakePortal();
    const rerun = await acquireErtsOrganizationArtifacts({
      transport: makeTransport(portal),
      cacheDir,
      organization: MCKEE,
      cycle: CYCLE,
      retrievedAt: NOW,
    });
    expect(rerun.fetchedPdfCount).toBe(0);
    expect(rerun.skippedPdfCount).toBe(2);
    expect(portal.log.some((line) => line.includes("/ExportDocs/"))).toBe(false);
  });

  it("refuses to fetch under a mismatched identity", async () => {
    await expect(
      acquireErtsOrganizationArtifacts({
        transport: makeTransport(makeFakePortal()),
        cacheDir,
        organization: { ...MCKEE, orgId: "9999" },
        cycle: CYCLE,
      })
    ).rejects.toThrow(/resolved to OrgID 2235, expected 9999/);
    expect((await getErtsArtifactStatus({ cacheDir, key: { type: "organization_filings", orgId: "9999" } })).status).toBe(
      "missing"
    );
  });

  it("fails the organization when an expenditure page has rows but no summary block", async () => {
    // The expenditure leg has no export reconciliation behind it — this
    // guard is its only shield against a zero-disbursements read.
    const portal = makeFakePortal({ expenditureHtml: '<table id="dgrExpenditure"><tr><td>rows</td></tr></table>' });
    await expect(
      acquireErtsOrganizationArtifacts({
        transport: makeTransport(portal),
        cacheDir,
        organization: MCKEE,
        cycle: CYCLE,
      })
    ).rejects.toThrow(/itemized rows but no readable summary groupings/);
    expect((await getErtsArtifactStatus({ cacheDir, key: { type: "organization_filings", orgId: "2235" } })).status).toBe(
      "missing"
    );
  });

  it("discards the whole bundle when the filing list changes mid-fetch", async () => {
    // The after-snapshot drops the amended flag — the filed set changed.
    const changed = FILINGS_HTML.replace(
      /Yes(\s*<\/td><td>\s*<a id="grdSearchResults_ctl17)/,
      "No$1"
    );
    expect(changed).not.toBe(FILINGS_HTML);
    const portal = makeFakePortal({ filingsAfterHtml: changed });
    await expect(
      acquireErtsOrganizationArtifacts({
        transport: makeTransport(portal),
        cacheDir,
        organization: MCKEE,
        cycle: CYCLE,
      })
    ).rejects.toThrow(/changed mid-fetch/);
    expect((await getErtsArtifactStatus({ cacheDir, key: { type: "organization_filings", orgId: "2235" } })).status).toBe(
      "missing"
    );
  });
});

describe("acquireErtsCf8IndexArtifacts", () => {
  function cf8Row(date: string, type: string, org: string): string {
    return (
      `<tr><td>${date}</td><td>x</td><td>${type}</td>` +
      `<td><a href="/ReportsScanned/${org.replace(/\s+/g, "")}.pdf">${org}</a></td></tr>`
    );
  }
  function cf8Page(rows: string[], current: number, nextLabels: string[]): string {
    // Control ids are positional (page N renders as ctl0{N-1}), matching the
    // live pager the spike pinned.
    const links = nextLabels
      .map((label) => `<a href="javascript:__doPostBack('dgdCF8FilingList$ctl14$ctl0${Number(label) - 1}','')">${label}</a>`)
      .join(" ");
    return (
      '<input type="hidden" name="__VIEWSTATE" value="vs" />' +
      `<table id="dgdCF8FilingList"><tr><td>Filed Date</td><td></td><td>Type</td><td>Org</td></tr>` +
      rows.join("") +
      `<tr><td colspan="4"><span>${current}</span> ${links}</td></tr></table>`
    );
  }

  function makeCf8Portal(pages: Record<number, string>) {
    const log: string[] = [];
    const fetch = async (url: string, body?: URLSearchParams): Promise<ErtsHttpResponse> => {
      log.push(`${body ? "POST" : "GET"} ${url}`);
      const target = body?.get("__EVENTTARGET");
      const page = target ? Number(target.slice(-1)) + 1 : 1;
      return {
        status: 200,
        finalUrl: url,
        contentType: "text/html",
        body: new TextEncoder().encode(pages[page]),
      };
    };
    return { fetch, log };
  }

  it("traverses to the boundary and installs one vintage of pages", async () => {
    const pages = {
      1: cf8Page([cf8Row("Jul 30 2026", "INDEPENDENT EXPENDITURE", "UNITE HERE"), cf8Row("Jul 3 2025", "COVERED TRANSFER", "Org B")], 1, ["2"]),
      2: cf8Page([cf8Row("Mar 1 2024", "INDEPENDENT EXPENDITURE", "Old Org")], 2, []),
    };
    const transport = createErtsTransport({ fetch: makeCf8Portal(pages).fetch, sleep: async () => {} });
    const result = await acquireErtsCf8IndexArtifacts({ transport, cacheDir, cycle: CYCLE, retrievedAt: NOW });
    expect(result).toEqual({
      pageCount: 2,
      rowCount: 3,
      cycleRowCount: 2,
      independentExpenditureRowCount: 1,
      missingScanLinkCount: 0,
    });
    for (const page of [1, 2]) {
      const status = await getErtsArtifactStatus({ cacheDir, key: { type: "cf8_index_page", page } });
      expect(status.status).toBe("ready");
      expect(status.manifest?.source?.cf8PageCount).toBe(2);
    }
  });

  it("installs nothing when the traversal does not descend", async () => {
    const pages = {
      1: cf8Page([cf8Row("Jul 3 2025", "COVERED TRANSFER", "Org B")], 1, ["2"]),
      // Page 2 is NEWER than page 1 — the pager lied; nothing may install.
      2: cf8Page([cf8Row("Jul 30 2026", "INDEPENDENT EXPENDITURE", "UNITE HERE")], 2, []),
    };
    const transport = createErtsTransport({ fetch: makeCf8Portal(pages).fetch, sleep: async () => {} });
    await expect(acquireErtsCf8IndexArtifacts({ transport, cacheDir, cycle: CYCLE })).rejects.toThrow(
      /not trustworthy/
    );
    expect((await getErtsArtifactStatus({ cacheDir, key: { type: "cf8_index_page", page: 1 } })).status).toBe("missing");
  });
});
