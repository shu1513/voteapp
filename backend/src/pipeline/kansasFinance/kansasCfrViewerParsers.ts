// Parsers for Kansas SOS CFR viewer pages (plan-kansas-finance.md).
//
// Every shape below was captured live 2026-08-26. The viewer renders values
// in id-stamped <span>s, which keeps parsing structural:
// - report cover (reports/exp_report_main.aspx): lblCashBeginning,
//   lblTotalContributions, lblCashThisPeriod, lblTotalExpenditures,
//   lblCashOnHandClose, lblInKindContributions, lblOtherTransactions, plus
//   identity spans and the disabled chkAmended/chkTermination checkboxes.
// - schedule totals: Schedule A lblTotalItemized/lblTotalUnitemized/
//   lblPoliticalMaterials/lblContributorUnknown/lblTotalReceipts; Schedule C
//   lblTotalItemizedExpenditures/lblTotalUnitemized/lblTotalExpenditures;
//   Schedule B lblTotalItemized/lblTotalUnitemized/lblTotalInKind. Itemized
//   ROW parsing is deliberately absent — Phase 0 needs only the totals.
// - contribution export: one <span id="lblField_N"> set per row.
// - candidate-filings grids: e-filed rows carry lblDate_N and postback name
//   links; paper rows carry lblOriginalDate_N, an <img id="..._paper_N"
//   title="Paper Filing">, and their links open the scanned PDF.
//
// All money is parsed to integer cents. The strict parser rejects anything
// it does not fully understand; the OCR-tolerant parser (for scanned KPDC
// PDFs) may return an `uncertain` read that callers accept only when an
// arithmetic cross-check (cover lines 1+2=3, 3-4=5; IE running totals)
// validates it.

const HTML_ENTITY_MAP: Record<string, string> = {
  "&amp;": "&",
  "&lt;": "<",
  "&gt;": ">",
  "&quot;": '"',
  "&nbsp;": " ",
};

export function decodeKansasHtmlText(value: string): string {
  return value.replace(
    /&(?:#x([0-9a-fA-F]+)|#(\d+)|amp|lt|gt|quot|nbsp);/g,
    (entity, hex?: string, decimal?: string) => {
      if (hex !== undefined) return String.fromCodePoint(Number.parseInt(hex, 16));
      if (decimal !== undefined) return String.fromCodePoint(Number.parseInt(decimal, 10));
      return HTML_ENTITY_MAP[entity] ?? entity;
    }
  );
}

/**
 * Every hidden input (name -> value) of a WebForms page. The next POST must
 * echo them all: omitting __VIEWSTATEENCRYPTED produced a 500 live.
 */
export function parseKansasHiddenFields(html: string): Record<string, string> {
  const fields: Record<string, string> = {};
  for (const tag of html.matchAll(/<input[^>]*type="hidden"[^>]*>/g)) {
    const name = /name="([^"]+)"/.exec(tag[0]);
    if (!name) continue;
    const value = /value="([^"]*)"/.exec(tag[0]);
    fields[decodeKansasHtmlText(name[1]!)] = decodeKansasHtmlText(value?.[1] ?? "");
  }
  return fields;
}

/**
 * Strict money parse to integer cents. Accepts the viewer's live variants:
 * "$3,077.59", "$ 4350.00", "$0", "$ 0", and the cover's bare "1600.00".
 * Anything else (including blanks) is null.
 */
export function parseKansasMoneyCents(raw: string): number | null {
  const text = decodeKansasHtmlText(raw).trim();
  const match = /^\$?\s*(\d{1,3}(?:,\d{3})*|\d+)(?:\.(\d{2}))?$/.exec(text);
  if (!match) return null;
  const whole = Number.parseInt(match[1]!.replace(/,/g, ""), 10);
  const cents = match[2] === undefined ? 0 : Number.parseInt(match[2], 10);
  return whole * 100 + cents;
}

export type KansasOcrMoney = { cents: number; uncertain: boolean };

/**
 * OCR-tolerant money parse for scanned KPDC PDFs. Live artifacts:
 * "$ 359,633.00" (spaces), "$ 138,270 ,00" (comma as decimal point with a
 * stray space), "$2,550.001" (a table border read as a trailing digit).
 * A read that required dropping trailing artifact digits is `uncertain` and
 * must only be used when an arithmetic cross-check validates it. A string
 * whose decimal point cannot be located confidently returns null.
 */
export function parseKansasOcrMoneyCents(raw: string): KansasOcrMoney | null {
  let text = raw.replace(/\s+/g, " ").trim().replace(/^\$\s*/, "");
  if (!text) return null;
  // Normalize "138,270 ,00" / "138,270, 00" -> "138,270.00".
  text = text.replace(/\s*[,.]\s*(\d{2})$/, ".$1");
  const exact = /^(\d{1,3}(?:,\d{3})*|\d+)\.(\d{2})$/.exec(text);
  if (exact) {
    return {
      cents: Number.parseInt(exact[1]!.replace(/,/g, ""), 10) * 100 + Number.parseInt(exact[2]!, 10),
      uncertain: false,
    };
  }
  // Trailing artifact digits after a well-formed .XX (e.g. "2,550.001").
  const artifact = /^(\d{1,3}(?:,\d{3})*|\d+)\.(\d{2})\d{1,2}$/.exec(text);
  if (artifact) {
    return {
      cents:
        Number.parseInt(artifact[1]!.replace(/,/g, ""), 10) * 100 + Number.parseInt(artifact[2]!, 10),
      uncertain: true,
    };
  }
  return null;
}

/**
 * Results-page record count. Rendered as
 * `<span id="lblRecordCount">4285</span>\n record(s) found.` (the phrase sits
 * outside the span), so the span is the anchor.
 */
export function parseKansasRecordCount(html: string): number | null {
  const match = /<span id="lblRecordCount"[^>]*>([\d,]+)<\/span>/.exec(html);
  if (!match) return null;
  return Number.parseInt(match[1]!.replace(/,/g, ""), 10);
}

// ---------------------------------------------------------------------------
// Contribution/expenditure export (HTML table served as Contributions.xls).

export type KansasContributionExportRow = {
  candidateName: string;
  contributorName: string;
  city: string;
  state: string;
  zip: string;
  occupation: string;
  /** Always blank in live data; kept so drift is visible. */
  industry: string;
  date: string;
  tenderType: string;
  amountCents: number | null;
  inKindAmountCents: number | null;
  inKindDescription: string;
  periodStart: string;
  periodEnd: string;
};

function collectIndexedSpans(html: string, prefix: string): Map<number, string> {
  const values = new Map<number, string>();
  const pattern = new RegExp(`<span id="${prefix}_(\\d+)">([^<]*)</span>`, "g");
  for (const match of html.matchAll(pattern)) {
    values.set(Number.parseInt(match[1]!, 10), decodeKansasHtmlText(match[2]!).trim());
  }
  return values;
}

export function parseKansasContributionExportRows(html: string): KansasContributionExportRow[] {
  const byField = {
    candidateName: collectIndexedSpans(html, "lblCandName"),
    contributorName: collectIndexedSpans(html, "lblContributor"),
    city: collectIndexedSpans(html, "lblCity"),
    state: collectIndexedSpans(html, "lblState"),
    zip: collectIndexedSpans(html, "lblZip"),
    occupation: collectIndexedSpans(html, "lblOccupation"),
    industry: collectIndexedSpans(html, "lblIndustry"),
    date: collectIndexedSpans(html, "lblDate"),
    tenderType: collectIndexedSpans(html, "lblTypeofTender"),
    amount: collectIndexedSpans(html, "lblAmount"),
    inKindAmount: collectIndexedSpans(html, "lblInKindAmount"),
    inKindDescription: collectIndexedSpans(html, "lblInKindDescription"),
    periodStart: collectIndexedSpans(html, "lblStartDate"),
    periodEnd: collectIndexedSpans(html, "lblEndDate"),
  };
  const rows: KansasContributionExportRow[] = [];
  const indexes = [...byField.contributorName.keys()].sort((a, b) => a - b);
  for (const index of indexes) {
    const amountText = byField.amount.get(index) ?? "";
    const inKindText = byField.inKindAmount.get(index) ?? "";
    rows.push({
      candidateName: byField.candidateName.get(index) ?? "",
      contributorName: byField.contributorName.get(index) ?? "",
      city: byField.city.get(index) ?? "",
      state: byField.state.get(index) ?? "",
      zip: byField.zip.get(index) ?? "",
      occupation: byField.occupation.get(index) ?? "",
      industry: byField.industry.get(index) ?? "",
      date: byField.date.get(index) ?? "",
      tenderType: byField.tenderType.get(index) ?? "",
      amountCents: amountText === "" ? null : parseKansasMoneyCents(amountText),
      inKindAmountCents: inKindText === "" ? null : parseKansasMoneyCents(inKindText),
      inKindDescription: byField.inKindDescription.get(index) ?? "",
      periodStart: byField.periodStart.get(index) ?? "",
      periodEnd: byField.periodEnd.get(index) ?? "",
    });
  }
  return rows;
}

// ---------------------------------------------------------------------------
// Report cover.

export type KansasReportCover = {
  candidateName: string;
  officeSought: string;
  district: string;
  periodStart: string;
  periodEnd: string;
  amended: boolean;
  termination: boolean;
  electronicallyFiledOn: string | null;
  cashBeginningCents: number | null;
  totalContributionsCents: number | null;
  cashAvailableCents: number | null;
  totalExpendituresCents: number | null;
  cashCloseCents: number | null;
  inKindCents: number | null;
  otherTransactionsCents: number | null;
};

function spanValue(html: string, id: string): string {
  const match = new RegExp(`<span id="${id}"[^>]*>([^<]*)</span>`).exec(html);
  return match ? decodeKansasHtmlText(match[1]!).trim() : "";
}

function checkboxChecked(html: string, id: string): boolean {
  const match = new RegExp(`<input[^>]*id="${id}"[^>]*>`).exec(html);
  return match ? /\bchecked\b/.test(match[0]) : false;
}

export function parseKansasReportCover(html: string): KansasReportCover {
  const filedText = spanValue(html, "lblElectronicSignature");
  const filedMatch = /Electronically filed on:\s*(.+)$/.exec(filedText);
  return {
    candidateName: spanValue(html, "lblCandOrgName"),
    officeSought: spanValue(html, "lblOfficeSoughtName"),
    district: spanValue(html, "lblDistrictNo"),
    periodStart: spanValue(html, "lblFileStartDate"),
    periodEnd: spanValue(html, "lblFileEndDate"),
    amended: checkboxChecked(html, "chkAmended"),
    termination: checkboxChecked(html, "chkTermination"),
    electronicallyFiledOn: filedMatch ? filedMatch[1]!.trim() : filedText.trim() || null,
    cashBeginningCents: parseKansasMoneyCents(spanValue(html, "lblCashBeginning")),
    totalContributionsCents: parseKansasMoneyCents(spanValue(html, "lblTotalContributions")),
    cashAvailableCents: parseKansasMoneyCents(spanValue(html, "lblCashThisPeriod")),
    totalExpendituresCents: parseKansasMoneyCents(spanValue(html, "lblTotalExpenditures")),
    cashCloseCents: parseKansasMoneyCents(spanValue(html, "lblCashOnHandClose")),
    inKindCents: parseKansasMoneyCents(spanValue(html, "lblInKindContributions")),
    otherTransactionsCents: parseKansasMoneyCents(spanValue(html, "lblOtherTransactions")),
  };
}

/**
 * Cover arithmetic self-check (K.S.A. 25-4148 form): 1+2=3 and 3-4=5, exact
 * to the cent. A cover that fails is quarantined, never published.
 */
export function reconcileKansasCoverArithmetic(cover: {
  cashBeginningCents: number | null;
  totalContributionsCents: number | null;
  cashAvailableCents: number | null;
  totalExpendituresCents: number | null;
  cashCloseCents: number | null;
}): boolean {
  const { cashBeginningCents, totalContributionsCents, cashAvailableCents, totalExpendituresCents, cashCloseCents } =
    cover;
  if (
    cashBeginningCents === null ||
    totalContributionsCents === null ||
    cashAvailableCents === null ||
    totalExpendituresCents === null ||
    cashCloseCents === null
  ) {
    return false;
  }
  return (
    cashBeginningCents + totalContributionsCents === cashAvailableCents &&
    cashAvailableCents - totalExpendituresCents === cashCloseCents
  );
}

// ---------------------------------------------------------------------------
// Schedule totals (Phase 0 reconciles totals only; rows are Phase 2).

export type KansasScheduleATotals = {
  totalItemizedCents: number | null;
  totalUnitemizedCents: number | null;
  politicalMaterialsCents: number | null;
  contributorUnknownCents: number | null;
  totalReceiptsCents: number | null;
};

export function parseKansasScheduleATotals(html: string): KansasScheduleATotals {
  return {
    totalItemizedCents: parseKansasMoneyCents(spanValue(html, "lblTotalItemized")),
    totalUnitemizedCents: parseKansasMoneyCents(spanValue(html, "lblTotalUnitemized")),
    politicalMaterialsCents: parseKansasMoneyCents(spanValue(html, "lblPoliticalMaterials")),
    contributorUnknownCents: parseKansasMoneyCents(spanValue(html, "lblContributorUnknown")),
    totalReceiptsCents: parseKansasMoneyCents(spanValue(html, "lblTotalReceipts")),
  };
}

export type KansasScheduleCTotals = {
  totalItemizedCents: number | null;
  totalUnitemizedCents: number | null;
  totalExpendituresCents: number | null;
};

export function parseKansasScheduleCTotals(html: string): KansasScheduleCTotals {
  return {
    totalItemizedCents: parseKansasMoneyCents(spanValue(html, "lblTotalItemizedExpenditures")),
    totalUnitemizedCents: parseKansasMoneyCents(spanValue(html, "lblTotalUnitemized")),
    totalExpendituresCents: parseKansasMoneyCents(spanValue(html, "lblTotalExpenditures")),
  };
}

// ---------------------------------------------------------------------------
// Candidate-filings grids.

export type KansasCfrGridRow = {
  index: number;
  /** lblDate_N (e-filed rows) or lblOriginalDate_N (paper rows). */
  fileDate: string;
  amendmentDate: string;
  name: string;
  officeSought: string;
  district: string;
  /** Explicit marker: <img id="<grid>_paper_N" title="Paper Filing">. */
  channel: "efile" | "paper";
  /** First row postback target, e.g. "grdviewCfrResults$ctl02$lnkbtnLastName". */
  postbackTarget: string | null;
};

/**
 * Rows of a viewer results grid (`grdviewCfrResults`, `grdviewLookupResults`,
 * `gvIndividualEntity`, ...). Handles both live shapes: e-filed rows
 * (lblDate_N + lnkbtn name links that open the HTML report) and paper rows
 * (lblOriginalDate_N + a "Paper Filing" pdf icon; their links open the
 * scanned PDF in a new window).
 */
export function parseKansasCfrGridRows(html: string, gridId: string): KansasCfrGridRow[] {
  const spanPattern = new RegExp(`<span id="${gridId}_(\\w+?)_(\\d+)"[^>]*>([^<]*)</span>`, "g");
  const fields = new Map<number, Map<string, string>>();
  for (const match of html.matchAll(spanPattern)) {
    const index = Number.parseInt(match[2]!, 10);
    if (!fields.has(index)) fields.set(index, new Map());
    fields.get(index)!.set(match[1]!, decodeKansasHtmlText(match[3]!).trim());
  }

  const anchorPattern = new RegExp(
    `<a id="${gridId}_\\w+?_(\\d+)"[^>]*href="javascript:__doPostBack\\(&#39;([^&]+)&#39;`,
    "g"
  );
  const anchors = new Map<number, string[]>();
  const anchorText = new Map<number, string[]>();
  const anchorFullPattern = new RegExp(
    `<a id="${gridId}_\\w+?_(\\d+)"[^>]*href="javascript:__doPostBack\\(&#39;([^&]+)&#39;[^>]*>([^<]*)</a>`,
    "g"
  );
  for (const match of html.matchAll(anchorPattern)) {
    const index = Number.parseInt(match[1]!, 10);
    if (!anchors.has(index)) anchors.set(index, []);
    anchors.get(index)!.push(match[2]!);
  }
  for (const match of html.matchAll(anchorFullPattern)) {
    const index = Number.parseInt(match[1]!, 10);
    if (!anchorText.has(index)) anchorText.set(index, []);
    const text = decodeKansasHtmlText(match[3]!).trim();
    if (text) anchorText.get(index)!.push(text);
  }

  const paperPattern = new RegExp(`<img id="${gridId}_paper_(\\d+)"`, "g");
  const paperRows = new Set<number>();
  for (const match of html.matchAll(paperPattern)) {
    paperRows.add(Number.parseInt(match[1]!, 10));
  }

  const indexes = new Set<number>([...fields.keys(), ...anchors.keys()]);
  return [...indexes]
    .sort((a, b) => a - b)
    .map((index) => {
      const rowFields = fields.get(index) ?? new Map<string, string>();
      const names = anchorText.get(index) ?? [];
      return {
        index,
        fileDate: rowFields.get("lblDate") ?? rowFields.get("lblOriginalDate") ?? "",
        amendmentDate: rowFields.get("lblAmendmentDate") ?? "",
        name: names.join(" "),
        officeSought: rowFields.get("labelOfficeSought") ?? "",
        district: (rowFields.get("lblDistrictNumber") ?? "").replace(/^\/\s*/, ""),
        channel: paperRows.has(index) ? "paper" : "efile",
        postbackTarget: anchors.get(index)?.[0] ?? null,
      };
    });
}
