// Kansas Phase 0 probe (plan-kansas-finance.md). Validates the SOS CFR
// viewer acquisition contracts and the OCR/fixture ground truth. No
// database, cache, scheduler, or published snapshot.
//
// Steps (each skippable):
//   1 contribution export per statewide candidate (+ occupation coverage)
//   2 expenditure export per candidate (first exercise of that flow)
//   3 export-cap test: wide no-name search, export count vs record count
//   4 candidate-filings enumeration: House + Senate-special R&E rows,
//     e-file vs paper channel tally
//   5 e-filed report walk: Helwig cover + Schedule A/C cent-exact checks
//   6 OCR cover recovery over scanned KPDC PDFs
//   7 two parallel viewer sessions
//   8 fixtures: Perry amendment pair, Kansas Comeback IE running totals
//     (oppose total $522,213.63), Koch GA unallocated statement

import { pathToFileURL } from "node:url";

import {
  buildKansasCfrUrl,
  createKansasCfrSession,
  exportKansasSearchResults,
  getKansasReportSchedule,
  KANSAS_CFR_OFFICE_CODES,
  KANSAS_CFR_VIEWER_PAGES,
  openKansasCfrCategory,
  postAndFollow,
  postbackAndFollow,
  DEFAULT_KANSAS_CFR_USER_AGENT,
  type KansasCfrPage,
  type KansasCfrSession,
} from "../pipeline/kansasFinance/kansasCfrViewerClient.js";
import {
  parseKansasCfrGridRows,
  parseKansasContributionExportRows,
  parseKansasRecordCount,
  parseKansasReportCover,
  parseKansasScheduleATotals,
  parseKansasScheduleCTotals,
} from "../pipeline/kansasFinance/kansasCfrViewerParsers.js";
import {
  checkKansasCoverAgainstSchedules,
  reconcileKansasIeStatements,
  recoverKansasOcrCoverFromText,
  summarizeKansasContributionExport,
  type KansasIeStatement,
} from "../pipeline/kansasFinance/kansasPhaseZero.js";
import {
  extractKansasPdfPages,
  kansasPdfFullText,
} from "../pipeline/kansasFinance/kansasFinancePdfText.js";

const KPDC_SCAN_BASE = "https://www.kansas.gov/ethics/CFAScanned/";

/** Scanned KPDC artifacts for the OCR cover-recovery rate (2026 House 202607). */
const DEFAULT_OCR_COVER_PDFS = [
  // Not marked "Electronically filed" in their OCR layer (paper-leaning):
  "House/2026ElecCycle/202607/H006FH_202607.pdf",
  "House/2026ElecCycle/202607/H007TA_202607.pdf",
  "House/2026ElecCycle/202607/H007TM_202607.pdf",
  // E-filed but print-then-scanned by KPDC (same artifact class):
  "House/2026ElecCycle/202607/H001DH_202607.pdf",
  "House/2026ElecCycle/202607/H002KC_202607.pdf",
];

const PERRY_ORIGINAL_PDF = "House/2026ElecCycle/202607/H003DP_202607.pdf";
const PERRY_AMENDED_PDF = "House/2026ElecCycle/202607/H003DP_amend2607.pdf";
const KOCH_GA_PDF = "Others/2026ElecCycle/202607/IE_KG_2607.pdf";

// Kansas Comeback PAC fixture: rows and stated totals hand-transcribed from
// the four statements and verified 2026-08-26. "Total this Period" is
// cumulative WITHIN a reporting period and resets at the boundary; ordered
// by filing date (KC2 6/30, KC1 7/2, KC3 7/7, then KC4 opens 7/24-10/22).
const KANSAS_COMEBACK_STATEMENTS: (KansasIeStatement & { pdfPath: string })[] = [
  {
    label: "IE_KC2_2607",
    pdfPath: "Others/2026ElecCycle/202607/IE_KC2_2607.pdf",
    periodKey: "2026-07",
    rowAmountsCents: [359_633_00, 10_810_63],
    totalThisPeriodCents: 370_443_63,
  },
  {
    label: "IE_KC1_2607",
    pdfPath: "Others/2026ElecCycle/202607/IE_KC1_2607.pdf",
    periodKey: "2026-07",
    rowAmountsCents: [8_500_00],
    totalThisPeriodCents: 378_943_63,
  },
  {
    label: "IE_KC3_2607",
    pdfPath: "Others/2026ElecCycle/202607/IE_KC3_2607.pdf",
    periodKey: "2026-07",
    rowAmountsCents: [5_000_00],
    totalThisPeriodCents: 383_943_63,
  },
  {
    label: "IE_KC4_2607",
    pdfPath: "Others/2026ElecCycle/202607/IE_KC4_2607.pdf",
    periodKey: "2026-10",
    rowAmountsCents: [138_270_00],
    totalThisPeriodCents: 138_270_00,
  },
];
const KANSAS_COMEBACK_EXPECTED_OPPOSE_CENTS = 522_213_63;

export type KansasPhaseZeroArgs = {
  candidates: string[];
  capStartDate: string;
  capEndDate: string;
  filedStartDate: string;
  filedEndDate: string;
  timeoutMs: number;
  spacingMs: number;
  skipCapTest: boolean;
  skipConcurrency: boolean;
};

const DEFAULT_ARGS: KansasPhaseZeroArgs = {
  candidates: ["Holscher", "Schmidt"],
  capStartDate: "01/01/2026",
  capEndDate: "07/23/2026",
  filedStartDate: "07/01/2026",
  filedEndDate: "08/26/2026",
  timeoutMs: 120_000,
  spacingMs: 1_500,
  skipCapTest: false,
  skipConcurrency: false,
};

export function parseKansasPhaseZeroArgs(args: readonly string[]): KansasPhaseZeroArgs {
  const result = { ...DEFAULT_ARGS, candidates: [...DEFAULT_ARGS.candidates] };
  for (let index = 0; index < args.length; index += 1) {
    const arg = args[index]!;
    const next = () => {
      const value = args[index + 1];
      if (!value || value.startsWith("--")) throw new Error(`Missing value for ${arg}`);
      index += 1;
      return value;
    };
    // NaN here is not harmless: setTimeout(…, NaN) fires immediately (every
    // fetch aborts) and `spacingMs > 0` goes false (request spacing off).
    // Number(), not parseInt(): "500ms" / "1.5" / "10junk" must reject, not
    // silently truncate to their numeric prefix.
    const nextNonNegativeInt = () => {
      const raw = next();
      const value = Number(raw);
      if (!Number.isInteger(value) || value < 0) throw new Error(`Invalid value for ${arg}: ${raw}`);
      return value;
    };
    switch (arg) {
      case "--candidates":
        result.candidates = next().split(",").map((name) => name.trim()).filter(Boolean);
        break;
      case "--cap-start":
        result.capStartDate = next();
        break;
      case "--cap-end":
        result.capEndDate = next();
        break;
      case "--filed-start":
        result.filedStartDate = next();
        break;
      case "--filed-end":
        result.filedEndDate = next();
        break;
      case "--timeout-ms":
        result.timeoutMs = nextNonNegativeInt();
        break;
      case "--spacing-ms":
        result.spacingMs = nextNonNegativeInt();
        break;
      case "--skip-cap":
        result.skipCapTest = true;
        break;
      case "--skip-concurrency":
        result.skipConcurrency = true;
        break;
      default:
        throw new Error(`Unknown argument: ${arg}`);
    }
  }
  if (result.candidates.length === 0) throw new Error("--candidates must name at least one candidate");
  return result;
}

// --- KPDC PDF fetch (plain fetch: kansas.gov 302s to www.kansas.gov). -----

async function fetchKpdcPdf(path: string, timeoutMs: number): Promise<Uint8Array> {
  const url = `${KPDC_SCAN_BASE}${path}`;
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(url, {
      headers: { "User-Agent": DEFAULT_KANSAS_CFR_USER_AGENT },
      redirect: "follow",
      signal: controller.signal,
    });
    if (!response.ok) throw new Error(`GET ${url} answered ${response.status}`);
    const bytes = new Uint8Array(await response.arrayBuffer());
    if (bytes.length < 4 || String.fromCharCode(...bytes.slice(0, 4)) !== "%PDF") {
      throw new Error(`GET ${url} did not answer a PDF`);
    }
    return bytes;
  } finally {
    clearTimeout(timer);
  }
}

/** True when the OCR text contains the amount, tolerant of OCR separators. */
export function ocrTextContainsAmountCents(text: string, cents: number): boolean {
  const whole = Math.floor(cents / 100).toLocaleString("en-US");
  const fraction = String(cents % 100).padStart(2, "0");
  // Digit-boundary guards: without them "$21,544.08" contains "1,544.08" and a
  // transcribed amount reads as present when the document states a larger one.
  const pattern = new RegExp(
    `(?<![\\d,.])${whole.replace(/,/g, "[,. ]{1,3}")}\\s*[,.]\\s*${fraction}(?!\\d)`
  );
  return pattern.test(text);
}

// --- Viewer search flows. --------------------------------------------------

async function runItemizedExport(
  session: KansasCfrSession,
  category: "Contribution" | "Expenditure",
  candidateName: string,
  startDate: string,
  endDate: string
) {
  const form = await openKansasCfrCategory(session, category);
  const fields: Record<string, string> =
    category === "Contribution"
      ? {
          txtContributorName: "",
          txtCandidateName: candidateName,
          txtContributorCity: "",
          ddlStates: "",
          ddlContributionType: "",
          txtCashAmount: "",
          txtStartDate: startDate,
          txtEndDate: endDate,
          btnSubmit: "Submit",
        }
      : {
          txtEntity: "",
          txtCandidateName: candidateName,
          txtCity: "",
          ddlStates: "",
          ddlExpenditureType: "",
          txtAmount: "",
          txtStartDate: startDate,
          txtEndDate: endDate,
          btnSubmit: "Submit",
        };
  const results = await postAndFollow(session, form, fields);
  const recordCount = parseKansasRecordCount(results.html);
  const exported = await exportKansasSearchResults(session, results);
  const exportHtml = exported.body.toString("utf8");
  const rows = parseKansasContributionExportRows(exportHtml);
  return { recordCount, rows, exportBytes: exported.body.byteLength, contentDisposition: exported.contentDisposition };
}

async function enumerateFilings(
  session: KansasCfrSession,
  officeCode: string,
  filedStart: string,
  filedEnd: string
) {
  const form = await openKansasCfrCategory(session, "Candidate");
  const results = await postAndFollow(session, form, {
    txtFirstName: "",
    txtLastName: "",
    drpdownOffice: officeCode,
    txtDistrictNo: "",
    drpdownFilingType: "Receipts and Expenditures Report",
    txtStartDate: filedStart,
    txtEndDate: filedEnd,
    btnSearch: "Submit Search",
  });
  const recordCount = parseKansasRecordCount(results.html);
  const rows = parseKansasCfrGridRows(results.html, "grdviewCfrResults");
  return { recordCount, rows, page: results };
}

async function walkHelwigReport(session: KansasCfrSession) {
  const form = await openKansasCfrCategory(session, "Candidate");
  const results = await postAndFollow(session, form, {
    txtFirstName: "Dale",
    txtLastName: "Helwig",
    drpdownOffice: KANSAS_CFR_OFFICE_CODES.stateRepresentative,
    txtDistrictNo: "1",
    drpdownFilingType: "Receipts and Expenditures Report",
    txtStartDate: "07/25/2026",
    txtEndDate: "07/28/2026",
    btnSearch: "Submit Search",
  });
  const rows = parseKansasCfrGridRows(results.html, "grdviewCfrResults");
  const efileRow = rows.find((row) => row.channel === "efile" && row.postbackTarget !== null);
  if (!efileRow) throw new Error("Helwig fixture: no e-filed R&E row found in 07/25-07/28/2026");
  const coverPage: KansasCfrPage = await postbackAndFollow(session, results, efileRow.postbackTarget!);
  if (!coverPage.url.endsWith(KANSAS_CFR_VIEWER_PAGES.reportCover)) {
    throw new Error(`Helwig fixture: row postback landed on ${coverPage.url}`);
  }
  const cover = parseKansasReportCover(coverPage.html);
  const scheduleA = parseKansasScheduleATotals((await getKansasReportSchedule(session, "A")).html);
  const scheduleC = parseKansasScheduleCTotals((await getKansasReportSchedule(session, "C")).html);
  return { cover, scheduleA, scheduleC, checks: checkKansasCoverAgainstSchedules(cover, scheduleA, scheduleC) };
}

// --- Main probe. -----------------------------------------------------------

export async function runKansasPhaseZeroProbe(args: KansasPhaseZeroArgs) {
  const sessionOptions = { timeoutMs: args.timeoutMs, spacingMs: args.spacingMs };
  const failures: string[] = [];
  const note = (ok: boolean, message: string) => {
    if (!ok) failures.push(message);
  };

  // 1+2: itemized exports per candidate.
  const exports: Record<string, unknown>[] = [];
  for (const candidate of args.candidates) {
    for (const category of ["Contribution", "Expenditure"] as const) {
      const session = createKansasCfrSession(sessionOptions);
      const result = await runItemizedExport(session, category, candidate, args.capStartDate, args.capEndDate);
      const summary = summarizeKansasContributionExport(result.rows);
      note(
        result.recordCount !== null && result.rows.length === result.recordCount,
        `${category} export for ${candidate}: parsed ${result.rows.length} rows but page reported ${result.recordCount}`
      );
      // Row-count equality alone would let a structurally-parsed row with an
      // unparseable amount undercount totals silently — fail-closed instead.
      note(
        summary.unparsedAmountRowCount === 0,
        `${category} export for ${candidate}: ${summary.unparsedAmountRowCount} rows with unparsed amounts`
      );
      exports.push({
        candidate,
        category,
        recordCount: result.recordCount,
        exportBytes: result.exportBytes,
        contentDisposition: result.contentDisposition,
        summary,
      });
    }
  }

  // 3: export cap test (no candidate name — the widest search we can ask for).
  let capTest: Record<string, unknown> | null = null;
  if (!args.skipCapTest) {
    const session = createKansasCfrSession(sessionOptions);
    const result = await runItemizedExport(session, "Contribution", "", args.capStartDate, args.capEndDate);
    const capSummary = summarizeKansasContributionExport(result.rows);
    note(
      result.recordCount !== null && result.rows.length === result.recordCount,
      `cap test: parsed ${result.rows.length} rows but page reported ${result.recordCount}`
    );
    note(
      capSummary.unparsedAmountRowCount === 0,
      `cap test: ${capSummary.unparsedAmountRowCount} rows with unparsed amounts`
    );
    capTest = {
      recordCount: result.recordCount,
      exportedRows: result.rows.length,
      exportBytes: result.exportBytes,
      distinctCandidates: new Set(result.rows.map((row) => row.candidateName)).size,
    };
  }

  // 4: filings enumeration + channel tally (House + Senate specials).
  const enumeration: Record<string, unknown>[] = [];
  for (const [officeName, officeCode] of [
    ["stateRepresentative", KANSAS_CFR_OFFICE_CODES.stateRepresentative],
    ["stateSenator", KANSAS_CFR_OFFICE_CODES.stateSenator],
  ] as const) {
    const session = createKansasCfrSession(sessionOptions);
    const result = await enumerateFilings(session, officeCode, args.filedStartDate, args.filedEndDate);
    const paper = result.rows.filter((row) => row.channel === "paper").length;
    const efile = result.rows.length - paper;
    note(
      result.rows.length > 0 || officeName === "stateSenator",
      `${officeName} enumeration returned no grid rows`
    );
    enumeration.push({
      office: officeName,
      recordCount: result.recordCount,
      gridRowsOnFirstPage: result.rows.length,
      efileRows: efile,
      paperRows: paper,
    });
  }

  // 5: e-filed walk (Helwig).
  const helwigSession = createKansasCfrSession(sessionOptions);
  const helwig = await walkHelwigReport(helwigSession);
  note(helwig.checks.coverArithmeticOk, "Helwig cover arithmetic failed");
  note(helwig.checks.scheduleAMatchesCover, "Helwig Schedule A total != cover line 2");
  note(helwig.checks.scheduleCMatchesCover, "Helwig Schedule C total != cover line 4");

  // 6: OCR cover recovery on scanned KPDC PDFs.
  const ocrCovers: Record<string, unknown>[] = [];
  let ocrRecovered = 0;
  for (const pdfPath of DEFAULT_OCR_COVER_PDFS) {
    const bytes = await fetchKpdcPdf(pdfPath, args.timeoutMs);
    const text = kansasPdfFullText(await extractKansasPdfPages(bytes));
    const recovery = recoverKansasOcrCoverFromText(text);
    if (recovery) ocrRecovered += 1;
    ocrCovers.push({
      pdf: pdfPath,
      recovered: recovery !== null,
      usedUncertainRead: recovery?.usedUncertainRead ?? null,
      receiptsCents: recovery?.receiptsCents ?? null,
      expendituresCents: recovery?.expendituresCents ?? null,
    });
  }

  // 7: parallel sessions.
  let concurrency: Record<string, unknown> | null = null;
  if (!args.skipConcurrency) {
    const attempts = await Promise.allSettled(
      [0, 1].map(async () => {
        const session = createKansasCfrSession(sessionOptions);
        const form = await openKansasCfrCategory(session, "Contribution");
        if (!form.url.endsWith(KANSAS_CFR_VIEWER_PAGES.contributionForm)) {
          throw new Error(`landed on ${form.url}`);
        }
      })
    );
    const succeeded = attempts.filter((attempt) => attempt.status === "fulfilled").length;
    // This step exists to PROVE parallel sessions safe — a failed session must
    // fail the probe, not just show up in the output.
    note(
      succeeded === attempts.length,
      `concurrency: only ${succeeded}/${attempts.length} parallel sessions succeeded`
    );
    concurrency = {
      parallelSessions: attempts.length,
      succeeded,
      errors: attempts.flatMap((attempt) =>
        attempt.status === "rejected" ? [String(attempt.reason)] : []
      ),
    };
  }

  // 8a: Perry amendment pair — both covers must recover and differ only as a
  // full replacement (same receipts, larger in-kind is visible via close/
  // expenditure lines staying consistent; the strict claim tested here is
  // that BOTH reconcile independently, so "take the latest" is safe).
  const perry: Record<string, unknown> = {};
  {
    const originalText = kansasPdfFullText(
      await extractKansasPdfPages(await fetchKpdcPdf(PERRY_ORIGINAL_PDF, args.timeoutMs))
    );
    const amendedText = kansasPdfFullText(
      await extractKansasPdfPages(await fetchKpdcPdf(PERRY_AMENDED_PDF, args.timeoutMs))
    );
    const original = recoverKansasOcrCoverFromText(originalText);
    const amended = recoverKansasOcrCoverFromText(amendedText);
    note(original !== null, "Perry original cover did not recover");
    note(amended !== null, "Perry amended cover did not recover");
    note(
      original !== null && amended !== null && original.receiptsCents === amended.receiptsCents,
      "Perry amendment fixture: receipts changed between original and amendment (expected a full replacement with identical receipts)"
    );
    perry.original = original;
    perry.amended = amended;
  }

  // 8b: Kansas Comeback IE running totals + artifact cross-check.
  const comebackReconciliation = reconcileKansasIeStatements(KANSAS_COMEBACK_STATEMENTS);
  note(comebackReconciliation.ok, `Comeback running totals: ${comebackReconciliation.failures.join("; ")}`);
  note(
    comebackReconciliation.totalRowCents === KANSAS_COMEBACK_EXPECTED_OPPOSE_CENTS,
    `Comeback oppose total ${comebackReconciliation.totalRowCents} != expected ${KANSAS_COMEBACK_EXPECTED_OPPOSE_CENTS}`
  );
  const comebackArtifacts: Record<string, unknown>[] = [];
  for (const statement of KANSAS_COMEBACK_STATEMENTS) {
    const text = kansasPdfFullText(
      await extractKansasPdfPages(await fetchKpdcPdf(statement.pdfPath, args.timeoutMs))
    );
    const rowAmountsPresent = statement.rowAmountsCents.every((cents) =>
      ocrTextContainsAmountCents(text, cents)
    );
    const totalPresent = ocrTextContainsAmountCents(text, statement.totalThisPeriodCents);
    const opposePresent = /Oppose/i.test(text) && /Masterson/i.test(text);
    note(rowAmountsPresent, `${statement.label}: transcribed row amounts not found in OCR text`);
    note(totalPresent, `${statement.label}: stated total not found in OCR text`);
    note(opposePresent, `${statement.label}: Oppose/Masterson not found in OCR text`);
    comebackArtifacts.push({ label: statement.label, rowAmountsPresent, totalPresent, opposePresent });
  }

  // 8c: Koch GA — multi-candidate unallocated spend.
  const kochText = kansasPdfFullText(
    await extractKansasPdfPages(await fetchKpdcPdf(KOCH_GA_PDF, args.timeoutMs))
  );
  const kochUnallocated = /34 candidates/.test(kochText) && !/[HS]D\s?\d{1,3}\b/.test(kochText);
  const kochTotalPresent = ocrTextContainsAmountCents(kochText, 1_544_08);
  note(kochUnallocated, "Koch GA fixture: expected an unallocated 34-candidate statement");
  note(kochTotalPresent, "Koch GA fixture: $1,544.08 not found in OCR text");

  return {
    type: "kansas_campaign_finance_phase_zero_probe" as const,
    ts: new Date().toISOString(),
    ok: failures.length === 0,
    failures,
    viewerEntryUrl: buildKansasCfrUrl(KANSAS_CFR_VIEWER_PAGES.entry),
    exports,
    capTest: capTest ?? "skipped",
    enumeration,
    helwig: { cover: helwig.cover, scheduleA: helwig.scheduleA, scheduleC: helwig.scheduleC, checks: helwig.checks },
    ocrCovers: { recovered: ocrRecovered, total: DEFAULT_OCR_COVER_PDFS.length, perPdf: ocrCovers },
    concurrency: concurrency ?? "skipped",
    perryAmendmentFixture: perry,
    kansasComeback: {
      expectedOpposeCents: KANSAS_COMEBACK_EXPECTED_OPPOSE_CENTS,
      reconciliation: comebackReconciliation,
      artifacts: comebackArtifacts,
    },
    kochGa: { unallocated: kochUnallocated, totalPresent: kochTotalPresent },
    publication: "disabled_phase_zero" as const,
  };
}

async function main(): Promise<void> {
  const output = await runKansasPhaseZeroProbe(parseKansasPhaseZeroArgs(process.argv.slice(2)));
  console.log(JSON.stringify(output, null, 2));
  if (!output.ok) process.exitCode = 1;
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    console.error(
      "Kansas campaign-finance Phase 0 probe failed:",
      error instanceof Error ? error.message : error
    );
    process.exitCode = 1;
  });
}
