import { describe, expect, it } from "vitest";

import {
  parseKansasCfrGridCurrentPage,
  parseKansasCfrGridRows,
  parseKansasContributionExportRows,
  parseKansasHiddenFields,
  parseKansasMoneyCents,
  parseKansasOcrMoneyCents,
  parseKansasRecordCount,
  parseKansasReportCover,
  parseKansasScheduleATotals,
  parseKansasScheduleCTotals,
  reconcileKansasCoverArithmetic,
} from "../../../src/pipeline/kansasFinance/kansasCfrViewerParsers.js";

describe("parseKansasHiddenFields", () => {
  it("collects hidden inputs including empty and entity-encoded values", () => {
    const html = `
      <input type="hidden" name="__VIEWSTATE" id="__VIEWSTATE" value="AbC&#43;dE=" />
      <input type="hidden" name="__VIEWSTATEENCRYPTED" id="__VIEWSTATEENCRYPTED" value="" />
      <input type="hidden" name="__SCROLLPOSITIONX" id="__SCROLLPOSITIONX" value="0" />
      <input type="text" name="txtLastName" value="ignored" />`;
    expect(parseKansasHiddenFields(html)).toEqual({
      __VIEWSTATE: "AbC+dE=",
      __VIEWSTATEENCRYPTED: "",
      __SCROLLPOSITIONX: "0",
    });
  });
});

describe("parseKansasMoneyCents", () => {
  it.each([
    ["$3,077.59", 307759],
    ["$ 4350.00", 435000],
    ["$0", 0],
    ["$ 0", 0],
    ["1600.00", 160000],
    ["$412,630.21", 41263021],
    // Live Schmidt 2026 credit-card refund rows use accounting negatives.
    ["($4,000.00)", -400000],
    ["( $4,000.00 )", -400000],
  ])("parses %s", (raw, cents) => {
    expect(parseKansasMoneyCents(raw)).toBe(cents);
  });

  it.each([[""], ["view/print"], ["$1,23.45"], ["$1.2"], ["$1,000.001"], ["($1.2)"], ["()"]])(
    "rejects %s",
    (raw) => {
      expect(parseKansasMoneyCents(raw)).toBeNull();
    }
  );
});

describe("parseKansasOcrMoneyCents", () => {
  it("parses clean OCR amounts as certain", () => {
    expect(parseKansasOcrMoneyCents("$ 359,633.00")).toEqual({ cents: 35963300, uncertain: false });
  });

  it("normalizes a comma-as-decimal read (live: '$ 138,270 ,00')", () => {
    expect(parseKansasOcrMoneyCents("$ 138,270 ,00")).toEqual({ cents: 13827000, uncertain: false });
  });

  it("marks trailing border-artifact digits as uncertain (live: '$2,550.001')", () => {
    expect(parseKansasOcrMoneyCents("$2,550.001")).toEqual({ cents: 255000, uncertain: true });
  });

  it("returns null when the decimal point cannot be located", () => {
    expect(parseKansasOcrMoneyCents("$58.741.00")).toBeNull();
    expect(parseKansasOcrMoneyCents("$")).toBeNull();
  });
});

describe("parseKansasRecordCount", () => {
  it("reads the lblRecordCount span (the phrase sits outside the span)", () => {
    const html = `<strong>\n<span id="lblRecordCount">4285</span>\nrecord(s) found.\n</strong>`;
    expect(parseKansasRecordCount(html)).toBe(4285);
    expect(parseKansasRecordCount('<span id="lblRecordCount">1,234</span>')).toBe(1234);
    expect(parseKansasRecordCount("<p>nothing here</p>")).toBeNull();
  });
});

describe("parseKansasContributionExportRows", () => {
  // Synthetic contributor identities on the live markup shape: K.S.A.
  // 25-4154(d) restricts reuse of names copied from Kansas filings, so no
  // real contributor name or address is committed.
  it("groups id-stamped spans into rows", () => {
    const html = `
      <span id="lblCandName_0">Example Candidate</span>
      <span id="lblContributor_0">Sample Individual</span>
      <span id="lblCity_0">Anytown</span>
      <span id="lblState_0">KS</span>
      <span id="lblZip_0">00000</span>
      <span id="lblOccupation_0">Not Employed</span>
      <span id="lblIndustry_0"></span>
      <span id="lblDate_0">02/08/2026</span>
      <span id="lblTypeofTender_0">Credit Card</span>
      <span id="lblAmount_0">$250.00</span>
      <span id="lblInKindAmount_0"></span>
      <span id="lblInKindDescription_0"></span>
      <span id="lblStartDate_0">01/01/2026</span>
      <span id="lblEndDate_0">07/23/2026</span>
      <span id="lblCandName_1">Example Candidate</span>
      <span id="lblContributor_1">Sample Trade Association PAC</span>
      <span id="lblCity_1">Anytown</span>
      <span id="lblState_1">KS</span>
      <span id="lblZip_1">00000</span>
      <span id="lblOccupation_1"></span>
      <span id="lblIndustry_1"></span>
      <span id="lblDate_1">07/23/2026</span>
      <span id="lblTypeofTender_1">Check</span>
      <span id="lblAmount_1"></span>
      <span id="lblInKindAmount_1">$1,000.00</span>
      <span id="lblInKindDescription_1">Mailer</span>
      <span id="lblStartDate_1">01/01/2026</span>
      <span id="lblEndDate_1">07/23/2026</span>`;
    const rows = parseKansasContributionExportRows(html);
    expect(rows).toHaveLength(2);
    expect(rows[0]).toMatchObject({
      contributorName: "Sample Individual",
      occupation: "Not Employed",
      amountCents: 25000,
      inKindAmountCents: null,
    });
    expect(rows[1]).toMatchObject({
      contributorName: "Sample Trade Association PAC",
      occupation: "",
      amountCents: null,
      inKindAmountCents: 100000,
      inKindDescription: "Mailer",
    });
  });
});

const COVER_HTML = `
  <input id="chkAmended" type="checkbox" name="chkAmended" disabled="disabled" />
  <input id="chkTermination" type="checkbox" name="chkTermination" disabled="disabled" checked="checked" />
  <span id="lblCandOrgName">Dale R Helwig</span>
  <span id="lblOfficeSoughtName">State Representative</span>
  <span id="lblDistrictNo">1</span>
  <span id="lblFileStartDate">1/1/2026</span>
  <span id="lblFileEndDate">7/23/2026</span>
  <span id="lblCashBeginning">$3,077.59</span>
  <span id="lblTotalContributions">$4,350.00</span>
  <span id="lblCashThisPeriod">$7,427.59</span>
  <span id="lblTotalExpenditures">$1,860.65</span>
  <span id="lblCashOnHandClose">$5,566.94</span>
  <span id="lblInKindContributions">$0.00</span>
  <span id="lblOtherTransactions">1600.00</span>
  <span id="lblElectronicSignature">Electronically filed on:  7/27/2026 8:54:06 AM</span>`;

describe("parseKansasReportCover", () => {
  it("parses the live cover shape and its checkboxes", () => {
    const cover = parseKansasReportCover(COVER_HTML);
    expect(cover).toMatchObject({
      candidateName: "Dale R Helwig",
      officeSought: "State Representative",
      district: "1",
      amended: false,
      termination: true,
      electronicallyFiledOn: "7/27/2026 8:54:06 AM",
      cashBeginningCents: 307759,
      totalContributionsCents: 435000,
      cashAvailableCents: 742759,
      totalExpendituresCents: 186065,
      cashCloseCents: 556694,
      inKindCents: 0,
      otherTransactionsCents: 160000,
    });
    expect(reconcileKansasCoverArithmetic(cover)).toBe(true);
  });

  it("fails arithmetic when a line is off by one cent", () => {
    const cover = parseKansasReportCover(
      COVER_HTML.replace("$5,566.94", "$5,566.95")
    );
    expect(reconcileKansasCoverArithmetic(cover)).toBe(false);
  });
});

describe("schedule totals", () => {
  it("parses Schedule A totals (live '$ 4350.00' / '$ 0' shapes)", () => {
    const html = `
      <span id="lblTotalItemized">$ 4350.00</span>
      <span id="lblTotalUnitemized">$ 0</span>
      <span id="lblPoliticalMaterials">$ 0</span>
      <span id="lblContributorUnknown">$ 0</span>
      <span id="lblTotalReceipts">$ 4350.00</span>`;
    expect(parseKansasScheduleATotals(html)).toEqual({
      totalItemizedCents: 435000,
      totalUnitemizedCents: 0,
      politicalMaterialsCents: 0,
      contributorUnknownCents: 0,
      totalReceiptsCents: 435000,
    });
  });

  it("parses Schedule C totals", () => {
    const html = `
      <span id="lblTotalItemizedExpenditures">$ 1860.65</span>
      <span id="lblTotalUnitemized">$ 0</span>
      <span id="lblTotalExpenditures">$ 1860.65</span>`;
    expect(parseKansasScheduleCTotals(html)).toEqual({
      totalItemizedCents: 186065,
      totalUnitemizedCents: 0,
      totalExpendituresCents: 186065,
    });
  });
});

describe("parseKansasCfrGridRows", () => {
  it("classifies e-filed and paper rows (live grid shapes)", () => {
    const html = `
      <span id="grdviewCfrResults_lblDate_0">07/27/2026</span>
      <span id="grdviewCfrResults_lblAmendmentDate_0"></span>
      <a id="grdviewCfrResults_lnkbtnLastName_0" href="javascript:__doPostBack(&#39;grdviewCfrResults$ctl02$lnkbtnLastName&#39;,&#39;&#39;)">Helwig</a>
      <a id="grdviewCfrResults_lnkbtnFirstName_0" href="javascript:__doPostBack(&#39;grdviewCfrResults$ctl02$lnkbtnFirstName&#39;,&#39;&#39;)">Dale</a>
      <span id="grdviewCfrResults_labelOfficeSought_0">STATE REPRESENTATIVE</span>
      <span id="grdviewCfrResults_lblDistrictNumber_0">/ 1</span>
      <span id="grdviewCfrResults_lblOriginalDate_1">07/27/2026</span>
      <span id="grdviewCfrResults_lblAmendmentDate_1">08/06/2026</span>
      <a id="grdviewCfrResults_LinkButton1_1" title="open filing with Adobe Acrobat in a new window" href="javascript:__doPostBack(&#39;grdviewCfrResults$ctl03$LinkButton1&#39;,&#39;&#39;)">HENDERSON FRANK</a>
      <img id="grdviewCfrResults_paper_1" title="Paper Filing" src="../../images/pdficon_small.gif" />`;
    const rows = parseKansasCfrGridRows(html, "grdviewCfrResults");
    expect(rows).toHaveLength(2);
    expect(rows[0]).toMatchObject({
      fileDate: "07/27/2026",
      name: "Helwig Dale",
      officeSought: "STATE REPRESENTATIVE",
      district: "1",
      channel: "efile",
      postbackTarget: "grdviewCfrResults$ctl02$lnkbtnLastName",
    });
    expect(rows[1]).toMatchObject({
      fileDate: "07/27/2026",
      amendmentDate: "08/06/2026",
      name: "HENDERSON FRANK",
      channel: "paper",
      postbackTarget: "grdviewCfrResults$ctl03$LinkButton1",
    });
  });
});

describe("parseKansasCfrGridCurrentPage", () => {
  // Live pager markup 2026-08-28: the current page is the only bare
  // <td><span>N</span></td>; every other page is a Page$N postback link.
  const pagerRow = (cells: string) =>
    `<tr><td colspan="5"><table><tr>${cells}</tr></table></td></tr>`;
  const link = (page: number) =>
    `<td><a href="javascript:__doPostBack(&#39;grdviewCfrResults&#39;,&#39;Page$${page}&#39;)">${page}</a></td>`;

  it("reads the current page from live-shaped pager rows", () => {
    const page1 = pagerRow(`<td><span>1</span></td>${link(2)}${link(3)}${link(11)}`);
    expect(parseKansasCfrGridCurrentPage(page1, "grdviewCfrResults")).toBe(1);
    const page2 = pagerRow(`${link(1)}<td><span>2</span></td>${link(3)}`);
    expect(parseKansasCfrGridCurrentPage(page2, "grdviewCfrResults")).toBe(2);
  });

  it("returns null when no pager is rendered (single-page results)", () => {
    expect(
      parseKansasCfrGridCurrentPage('<span id="grdviewCfrResults_lblDate_0">07/27/2026</span>', "grdviewCfrResults")
    ).toBeNull();
  });

  it("returns null on an ambiguous pager instead of guessing", () => {
    const ambiguous = pagerRow(`<td><span>1</span></td><td><span>2</span></td>${link(3)}`);
    expect(parseKansasCfrGridCurrentPage(ambiguous, "grdviewCfrResults")).toBeNull();
  });

  it("ignores another grid's pager", () => {
    const other = pagerRow(`<td><span>4</span></td>${link(5)}`).replaceAll(
      "grdviewCfrResults",
      "gvIndividualEntity"
    );
    expect(parseKansasCfrGridCurrentPage(other, "gvIndividualEntity")).toBe(4);
    expect(parseKansasCfrGridCurrentPage(other, "grdviewCfrResults")).toBeNull();
  });
});
