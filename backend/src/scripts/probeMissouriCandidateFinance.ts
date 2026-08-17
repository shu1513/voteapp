// Phase 0 probe for the Missouri finance module (plan-missouri-finance.md).
// NO schema, NO database writes: exercises the MEC WebForms portals live
// through missouriMecClient and checks the plan's acquisition gates. Every
// pinned constant (paths, control names, export headers, grid markers) was
// derived by hand from live pages on 2026-08-12. A FAIL means the portal
// changed — re-verify by hand before building on top of it.
//
// Gates:
//   1. WAF pin: the www host serves full pages to the plain client. The
//      probe deliberately never requests the bare host live (one bare-host
//      hit flags the IP and the next www request gets challenged);
//      challenge-stub detection is fixture-tested in
//      missouriMecClient.test.ts against the captured bare-host response.
//   2. Contribution acquisition: search POST answers with the
//      window.open('CF12_ContrExpendResults.aspx') popup signal and the
//      session-bound results page renders with both report tabs.
//   3. Contribution export: HTML-table-as-.xls with the pinned 18-column
//      header; SEPARATE Employer and Occupation columns that demonstrably
//      populate independently — rows with employer-but-no-occupation AND
//      rows with occupation-but-no-employer must both exist in the current
//      cycle (hard fact 1 lands on publishing the occupation column — a
//      FAIL here reopens the suppression decision); every row's MECID and
//      amount parse.
//   4. No silent export cap: export data rows equal the Full Disclosure tab
//      count for the current cycle (2026), a larger multi-thousand-row
//      query (2024 — 2,197 rows live, past the common 1,000/2,000 caps),
//      and a historical query (2002, year floor — stable data). Counts are
//      observations, never contracts. Caps above ~2,200 rows stay unproven
//      here.
//   5. Outside-spending acquisition: inline results with a records-found
//      count, export rows equal it exactly, stance values are only
//      Support/Oppose, and the export still has NO MECID column (a FAIL
//      means MEC added one — good news; revisit the resolver plan).
//   6. Spender identity: the results grid's committee-link postback answers
//      302 -> CommInfo.aspx?mecid=... — deterministic MECID resolution
//      without name matching.
//   7. Office vocabulary: the election-search year -> date cascade enables
//      the closed Political Office list (captured verbatim for
//      missouriFinanceEligibleOffices.ts in a later phase).
//   8. Committee Info gold set: each pinned committee page echoes its MECID
//      and yields parseable Election History rows — date, election type,
//      office, and political subdivision must all parse with one value per
//      row (the auto-link backbone; the resolver keys on all four).
//   9. Report inventory (plan gate 3, REACHABLE half): the lbtnReports
//      postback + per-year expansion yields the report list — id, type,
//      filed date, amendment lineage. The gold set collectively shows the
//      required type families (quarterly / pre-election / limited-activity /
//      termination) and at least one amendment pair (AMENDED X + base X).
//
// Plan gates 3-5, cover-dependent half — NOT reachable, so NOT gated here.
// The report DOCUMENT (the cover: official totals, amendment replace-vs-delta,
// Limited Activity carry-forward, cash-on-hand, indebtedness) is fetched only
// through CommInfo.aspx/VerifyClick, a page-method that server-verifies a
// reCAPTCHA Enterprise token before returning the document redirect (probed
// live 2026-08-12: an empty/garbage token yields Successful:false, no
// redirect). The document id is never exposed client-side (data-DID="null";
// DMS/DOC/V/{reportId} 404s — the CPID is a report id, not the doc id), so
// there is no plain-client path to covers. Consequences, recorded in the plan:
// canonical totals CANNOT be cover-reconciled to the cent via the portal
// (unlike Georgia/NC); v1 totals come from itemized transactions (amendment
// replace-vs-delta is determined by comparing the AMENDED report's rows to the
// base report's rows — a Phase 3 data analysis, not a cover read) or from the
// MEC bulk extract (the § 130.057 request). Totals mapping (plan gate 5)
// likewise waits on the extract. Solving the reCAPTCHA is never an option.
//
// Artifacts: set MISSOURI_MEC_PROBE_ARTIFACT_DIR to save every fetched body
// for the report-inventory analysis.
// PII: contribution exports carry contributor street addresses — the
// artifact dir must stay out of git, and this script prints only aggregates,
// headers, MECIDs, and committee names, never contributor rows.

import { chmodSync, mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { pathToFileURL } from "node:url";

import {
  MISSOURI_MEC_PAGES,
  MISSOURI_MEC_RESULTS_FIELD_PREFIX,
  MISSOURI_MEC_SEARCH_FIELD_PREFIX,
  MissouriMecClientError,
  buildMissouriMecUrl,
  createMissouriMecSession,
  parseMissouriMecHiddenFields,
  type MissouriMecResponse,
  type MissouriMecSession,
} from "../pipeline/missouriFinance/missouriMecClient.js";

const SEARCH = MISSOURI_MEC_SEARCH_FIELD_PREFIX;
const RESULTS = MISSOURI_MEC_RESULTS_FIELD_PREFIX;

/** Pinned live 2026-08-12: the contribution export's exact header row. */
const CONTRIBUTION_EXPORT_HEADER = [
  "MECID",
  "Committee",
  "Report",
  "Contributor-Committee",
  "Contributor-Company",
  "Contributor-Last Name",
  "Contributor-First Name",
  "Address1",
  "Address2",
  "City",
  "State",
  "Zip",
  "Employer",
  "Occupation",
  "Contribution Date",
  "Contribution Amount",
  "Monetary/In-Kind",
  "Committee",
] as const;

/** Pinned live 2026-08-12: the outside-spending export's exact header row. */
const OUTSIDE_EXPORT_HEADER = [
  "Candidates Name and Address",
  "Office Sought",
  "Support/Oppose",
  "Date",
  "Amount",
  "Reporting Committee",
  "Report",
] as const;

/** Office names that must appear in the November-2026 vocabulary. */
const OFFICE_VOCABULARY_SENTINELS = ["State Representative", "State Senator", "Statewide Office", "Sheriff"];

/**
 * Gold committees for the Committee Info gate — extended as the live-run
 * analysis identifies the full plan set (statewide/legislative/county/
 * municipal/school-board, terminated, amended, Limited Activity, timely,
 * out-of-state, outside spender).
 */
const GOLD_COMMITTEES = [
  { mecid: "A222073", note: "municipal candidate — Alderperson Ward 3, City of Jackson (Seabaugh); Termination + Limited Activity + AMENDED lineage" },
  { mecid: "A171387", note: "county candidate — Baird For Jackson County" },
  { mecid: "C263985", note: "legislative candidate — State Senate District 34 (Barnes for Missouri); Quarterly + pre-election + AMENDED April Quarterly pair" },
] as const;

/**
 * Report-type families the gold set must collectively exhibit — the taxonomy
 * backbone (fact 2/3). Proven from report NAMES in the inventory; the dollar
 * semantics behind them (amendment replace-vs-delta, Limited Activity
 * carry-forward, cover reconciliation) live in the report DOCUMENT, which is
 * reCAPTCHA-gated and therefore out of Phase 0's reach — see the header note.
 */
const REQUIRED_REPORT_FAMILIES = ["quarterly", "pre_election", "limited_activity", "termination"] as const;

const MECID_PATTERN = /^[A-Z]?\d+$/;

type Gate = { name: string; pass: boolean; detail: string };

function decodeEntities(value: string): string {
  return value
    .replace(/&#x([0-9a-fA-F]+);/g, (_, hex: string) => String.fromCodePoint(Number.parseInt(hex, 16)))
    .replace(/&#(\d+);/g, (_, code: string) => String.fromCodePoint(Number.parseInt(code, 10)))
    .replace(/&nbsp;/g, " ")
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&quot;/g, '"');
}

function stripTags(html: string): string {
  return decodeEntities(html.replace(/<[^>]+>/g, " ")).replace(/\s+/g, " ").trim();
}

/** Cell texts for every <tr> of the FIRST <table> in an HTML fragment. */
function parseHtmlTableRows(html: string): string[][] {
  const table = /<table[^>]*>([\s\S]*?)<\/table>/.exec(html);
  if (table === null) {
    return [];
  }
  const rows: string[][] = [];
  for (const row of table[1]!.matchAll(/<tr[^>]*>([\s\S]*?)<\/tr>/g)) {
    const cells = [...row[1]!.matchAll(/<t[hd][^>]*>([\s\S]*?)<\/t[hd]>/g)].map((cell) => stripTags(cell[1]!));
    if (cells.length > 0) {
      rows.push(cells);
    }
  }
  return rows;
}

/** Values of <span id="...{gridId}_{label}_{n}">text</span> in row order. */
function parseSpanSeries(html: string, gridId: string, label: string): string[] {
  const pattern = new RegExp(`id="[^"]*${gridId}_${label}_(\\d+)">([\\s\\S]*?)</span>`, "g");
  const byIndex = new Map<number, string>();
  for (const match of html.matchAll(pattern)) {
    byIndex.set(Number.parseInt(match[1]!, 10), stripTags(match[2]!));
  }
  return [...byIndex.entries()].sort((a, b) => a[0] - b[0]).map((entry) => entry[1]);
}

function parseSelectOptions(html: string, controlName: string): { value: string; label: string }[] | null {
  const select = new RegExp(`<select[^>]*name="[^"]*${controlName.replace(/\$/g, "\\$")}"[^>]*>([\\s\\S]*?)</select>`).exec(
    html
  );
  if (select === null) {
    return null;
  }
  return [...select[1]!.matchAll(/<option[^>]*value="([^"]*)"[^>]*>([^<]*)/g)].map((match) => ({
    value: decodeEntities(match[1]!),
    label: decodeEntities(match[2]!).trim(),
  }));
}

/** "Full Disclosure Reports (833)" -> 833; null when the label is absent. */
function parseLabeledCount(html: string, label: string): number | null {
  const match = new RegExp(`${label}[^()]*\\((\\d[\\d,]*)\\)`).exec(stripTags(html));
  if (match === null) {
    return null;
  }
  return Number.parseInt(match[1]!.replace(/,/g, ""), 10);
}

function parseCurrency(value: string): number | null {
  const match = /^\(?-?\$[\d,]+\.\d{2}\)?$/.exec(value.trim());
  if (match === null) {
    return null;
  }
  const cents = Math.round(Number.parseFloat(value.replace(/[$,()]/g, "")) * 100);
  return value.trim().startsWith("(") ? -cents : cents;
}

function requireHiddenFields(page: string, url: string): Record<string, string> {
  const fields = parseMissouriMecHiddenFields(page);
  if (fields.__VIEWSTATE === undefined) {
    throw new MissouriMecClientError("bad_response", `no __VIEWSTATE on ${url} — not a WebForms page?`);
  }
  return fields;
}

/**
 * Report-inventory row from the CommInfo Reports tab (lbtnReports postback,
 * then per-year expansion). This is the ONLY report-level data the plain
 * client can reach — the report DOCUMENT (cover totals) sits behind a
 * reCAPTCHA Enterprise gate (see the gate-3/5 note in the header). The
 * inventory alone gives report id, type, filed date, and amendment lineage.
 */
type ReportInventoryRow = { cpid: string; report: string; dateFiled: string };

export type CommitteeReportRow = { mecid: string; report: string };

/** Coarse report-type family for the timely/period taxonomy (fact 2/3). */
type ReportTypeFamily =
  | "quarterly"
  | "pre_election"
  | "limited_activity"
  | "termination"
  | "timely_48h"
  | "timely_24h"
  | "other";

function classifyReportType(report: string): ReportTypeFamily {
  // Order matters: "AMENDED April Quarterly Report - Limited Activity" is a
  // limited-activity report; the AMENDED prefix is lineage, not a family.
  if (/limited activity/i.test(report)) return "limited_activity";
  if (/termination/i.test(report)) return "termination";
  if (/48[ -]?hour/i.test(report)) return "timely_48h";
  if (/24[ -]?hour/i.test(report)) return "timely_24h";
  if (/quarterly/i.test(report)) return "quarterly";
  if (/before (primary|general|election)|day before|pre-?election/i.test(report)) return "pre_election";
  return "other";
}

const AMENDED_PREFIX = /^AMENDED\s+/i;

function isAmendedReport(report: string): boolean {
  return AMENDED_PREFIX.test(report.trim());
}

function reportLineageKey(row: CommitteeReportRow): string {
  const normalizedReport = row.report.trim().replace(AMENDED_PREFIX, "").trim().toLocaleLowerCase("en-US");
  return `${row.mecid.trim().toLocaleUpperCase("en-US")}\u0000${normalizedReport}`;
}

/** Returns amended reports whose unamended base exists for the same committee. */
export function findSameCommitteeAmendmentPairs(rows: readonly CommitteeReportRow[]): CommitteeReportRow[] {
  const baseReports = new Set(
    rows.filter((row) => !isAmendedReport(row.report)).map((row) => reportLineageKey(row))
  );
  return rows.filter((row) => isAmendedReport(row.report) && baseReports.has(reportLineageKey(row)));
}

/**
 * Parses the expanded grvReports_0 table (one year) into inventory rows.
 * CPID lives in the report link's data-CPID attribute; the report name and
 * filed date are index-aligned spans (lblReport_N / lblDateReceived_N).
 */
function parseReportInventory(html: string): ReportInventoryRow[] {
  const cpids = new Map<number, string>();
  for (const match of html.matchAll(/grvReports_0_hlink_(\d+)"[^>]*data-CPID="(\d+)"/g)) {
    cpids.set(Number.parseInt(match[1]!, 10), match[2]!);
  }
  const names = new Map<number, string>();
  for (const match of html.matchAll(/grvReports_0_lblReport_(\d+)">([\s\S]*?)<\/span>/g)) {
    names.set(Number.parseInt(match[1]!, 10), stripTags(match[2]!));
  }
  const dates = new Map<number, string>();
  for (const match of html.matchAll(/grvReports_0_lblDateReceived_(\d+)">([\s\S]*?)<\/span>/g)) {
    dates.set(Number.parseInt(match[1]!, 10), stripTags(match[2]!));
  }
  const rows: ReportInventoryRow[] = [];
  for (const index of [...cpids.keys()].sort((a, b) => a - b)) {
    rows.push({ cpid: cpids.get(index)!, report: names.get(index) ?? "", dateFiled: dates.get(index) ?? "" });
  }
  return rows;
}

/**
 * CommInfo Reports tab -> expand the most recent report year (ctl02 = the
 * first grvReportOutside row) -> parse its inventory. Proves the lbtnReports
 * mechanism and yields the current-cycle report list. Phase 3 expands every
 * year; Phase 0 only needs the mechanism proven on the current cycle.
 */
async function fetchReportInventory(
  session: MissouriMecSession,
  infoUrl: string,
  infoHtml: string
): Promise<{ rows: ReportInventoryRow[]; expandedHtml: string }> {
  const tab = await session.postForm(
    infoUrl,
    { ...requireHiddenFields(infoHtml, infoUrl), __EVENTTARGET: `${SEARCH}lbtnReports`, __EVENTARGUMENT: "" },
    { referer: infoUrl }
  );
  const expand = await session.postForm(
    infoUrl,
    {
      ...requireHiddenFields(tab.text(), infoUrl),
      [`${SEARCH}grvReportOutside$ctl02$ImgRptRight.x`]: "1",
      [`${SEARCH}grvReportOutside$ctl02$ImgRptRight.y`]: "1",
    },
    { referer: infoUrl }
  );
  return { rows: parseReportInventory(expand.text()), expandedHtml: expand.text() };
}

function usd(cents: number): string {
  const sign = cents < 0 ? "-" : "";
  const abs = Math.abs(cents);
  return `${sign}$${Math.trunc(abs / 100).toLocaleString("en-US")}.${String(abs % 100).padStart(2, "0")}`;
}

type ArtifactSink = (name: string, body: Buffer | string) => void;

function createArtifactSink(): ArtifactSink {
  const dir = process.env.MISSOURI_MEC_PROBE_ARTIFACT_DIR;
  if (dir === undefined || dir === "") {
    return () => {};
  }
  // Owner-only modes: artifacts carry contributor street addresses. chmod
  // as well, since mkdir/write modes only apply when the path is created.
  mkdirSync(dir, { recursive: true, mode: 0o700 });
  chmodSync(dir, 0o700);
  console.log(`artifacts -> ${dir} (contains contributor street addresses — keep out of git)`);
  return (name, body) => {
    const artifactPath = join(dir, name);
    writeFileSync(artifactPath, body, { mode: 0o600 });
    chmodSync(artifactPath, 0o600);
  };
}

/** Search -> session popup -> results -> export, returning both pages. */
async function runContributionSearch(
  session: MissouriMecSession,
  searchPageHtml: string,
  year: string
): Promise<{ postBody: string; resultsHtml: string; exportResponse: MissouriMecResponse }> {
  const searchUrl = buildMissouriMecUrl(MISSOURI_MEC_PAGES.contributionSearch);
  const resultsUrl = buildMissouriMecUrl(MISSOURI_MEC_PAGES.contributionResults);
  const post = await session.postForm(
    searchUrl,
    {
      ...requireHiddenFields(searchPageHtml, searchUrl),
      [`${SEARCH}ddYear`]: year,
      [`${SEARCH}txtLName`]: "Smith",
      [`${SEARCH}txtFName`]: "",
      [`${SEARCH}btnSearch`]: "Search",
    },
    { referer: searchUrl }
  );
  const results = await session.get(resultsUrl, { referer: searchUrl });
  const exportResponse = await session.postForm(
    resultsUrl,
    {
      ...requireHiddenFields(results.text(), resultsUrl),
      [`${RESULTS}btnExport`]: "Export Results to Excel",
    },
    { referer: resultsUrl }
  );
  return { postBody: post.text(), resultsHtml: results.text(), exportResponse };
}

function checkContributionExport(
  exportResponse: MissouriMecResponse,
  fullDisclosureCount: number | null,
  label: string,
  gates: Gate[],
  options: {
    /**
     * Require rows where only Employer is filled AND rows where only
     * Occupation is filled — proof the columns populate independently, not
     * just that both headers exist. Demanded of the current-cycle query;
     * the 2002 sample has zero such rows (early e-filings mirrored one
     * value into both), so historical queries check row-count fidelity only.
     */
    requireIndependentEmployerOccupation: boolean;
  }
): void {
  const body = exportResponse.text();
  const rows = parseHtmlTableRows(body);
  const header = rows[0] ?? [];
  const dataRows = rows.slice(1);

  const badCells = dataRows.filter((row) => row.length !== CONTRIBUTION_EXPORT_HEADER.length).length;
  const badMecids = dataRows.filter((row) => !MECID_PATTERN.test(row[0] ?? "")).length;
  const amounts = dataRows.map((row) => parseCurrency(row[15] ?? ""));
  const badAmounts = amounts.filter((cents) => cents === null).length;
  const amendedRows = dataRows.filter((row) => (row[2] ?? "").startsWith("AMENDED")).length;
  const reportTypes = new Set(dataRows.map((row) => row[2] ?? ""));
  const monetaryKinds = new Set(dataRows.map((row) => row[16] ?? ""));
  const employerOnly = dataRows.filter((row) => (row[12] ?? "") !== "" && (row[13] ?? "") === "").length;
  const occupationOnly = dataRows.filter((row) => (row[12] ?? "") === "" && (row[13] ?? "") !== "").length;
  const totalCents = amounts.reduce<number>((total, cents) => total + (cents ?? 0), 0);

  console.log(
    `${label}: ${dataRows.length} rows, ${usd(totalCents)} summed, ${amendedRows} AMENDED rows, ` +
      `${reportTypes.size} report names, monetary/in-kind values [${[...monetaryKinds].sort().join(", ")}], ` +
      `employer-only ${employerOnly}, occupation-only ${occupationOnly}`
  );

  const independencePasses =
    !options.requireIndependentEmployerOccupation || (employerOnly > 0 && occupationOnly > 0);
  gates.push({
    name: `${label}: HTML-table .xls with the pinned 18-column header, separate Employer/Occupation${options.requireIndependentEmployerOccupation ? " (independently populated)" : ""}`,
    pass:
      (exportResponse.contentType ?? "").includes("application/vnd.ms-excel") &&
      (exportResponse.contentDisposition ?? "").includes("Contribution_Search.xls") &&
      body.trimStart().startsWith("<table") &&
      header.join("|") === CONTRIBUTION_EXPORT_HEADER.join("|") &&
      independencePasses,
    detail: `disposition=${exportResponse.contentDisposition}, header ${header.length} cols, employer-only ${employerOnly}, occupation-only ${occupationOnly}`,
  });
  gates.push({
    name: `${label}: every row parses (18 cells, MECID shape, currency amount)`,
    pass: dataRows.length > 0 && badCells === 0 && badMecids === 0 && badAmounts === 0,
    detail: `${dataRows.length} rows, ${badCells} bad cell counts, ${badMecids} bad MECIDs, ${badAmounts} bad amounts`,
  });
  gates.push({
    name: `${label}: export rows equal the Full Disclosure tab count (no silent cap)`,
    pass: fullDisclosureCount !== null && dataRows.length === fullDisclosureCount,
    detail: `export ${dataRows.length} vs tab ${fullDisclosureCount ?? "?"}`,
  });
}

async function main(): Promise<void> {
  if (process.argv.length > 2) {
    throw new Error(`Missouri finance probe takes no flags, got: ${process.argv.slice(2).join(" ")}`);
  }
  const gates: Gate[] = [];
  const saveArtifact = createArtifactSink();
  const session = createMissouriMecSession({ log: (message) => console.log(`  ${message}`) });

  // --- Gate 1: the www host serves full pages to the plain client. ---
  // The probe deliberately never touches the bare host live: one bare-host
  // hit (which answers with the Incapsula challenge) got the very next
  // www request challenged too, from a fresh cookie-free session on the
  // same IP (observed live 2026-08-12) — the flag is per-IP, not just
  // per-cookie. Production acquisition must never touch the bare host.
  // Challenge-stub detection is pinned against the captured bare-host
  // response in missouriMecClient.test.ts instead.
  const searchUrl = buildMissouriMecUrl(MISSOURI_MEC_PAGES.contributionSearch);
  const searchPage = await session.get(searchUrl);
  saveArtifact("contribution_search_page.html", searchPage.body);
  gates.push({
    name: "www host serves the full contribution search page to the plain client",
    pass: searchPage.status === 200 && searchPage.body.length > 30_000 && searchPage.text().includes("__VIEWSTATE"),
    detail: `status ${searchPage.status}, ${searchPage.body.length} bytes`,
  });

  // --- Gate 2: contribution search -> popup -> session-bound results (www). ---
  const large = await runContributionSearch(session, searchPage.text(), "2026");
  saveArtifact("contribution_results_2026.html", large.resultsHtml);
  saveArtifact("contribution_export_2026.xls.html", large.exportResponse.body);
  const fullDisclosure2026 = parseLabeledCount(large.resultsHtml, "Full Disclosure Reports");
  const fortyEightHour2026 = parseLabeledCount(large.resultsHtml, "48 Hour > \\$5000 Reports");
  console.log(
    `contribution results 2026 Smith: Full Disclosure (${fullDisclosure2026 ?? "?"}), 48 Hour (${fortyEightHour2026 ?? "?"})`
  );
  gates.push({
    name: "contribution search answers the popup signal and the session-bound results render",
    pass:
      large.postBody.includes("window.open('CF12_ContrExpendResults.aspx'") &&
      fullDisclosure2026 !== null &&
      fortyEightHour2026 !== null,
    detail: `popup signal ${large.postBody.includes("window.open('CF12_ContrExpendResults.aspx'") ? "present" : "MISSING"}, tabs (${fullDisclosure2026 ?? "?"} / ${fortyEightHour2026 ?? "?"})`,
  });

  // --- Gates 3+4a: 2026 export schema + cap check + column independence. ---
  checkContributionExport(large.exportResponse, fullDisclosure2026, "contribution export 2026", gates, {
    requireIndependentEmployerOccupation: true,
  });

  // --- Gate 4b: multi-thousand-row query (2024 — 2,197 rows live 2026-08-12,
  // past the common 1,000/2,000 export caps; caps beyond this size are
  // covered by per-committee report-cover reconciliation, not this probe). ---
  const bigger = await runContributionSearch(session, large.postBody, "2024");
  saveArtifact("contribution_results_2024.html", bigger.resultsHtml);
  saveArtifact("contribution_export_2024.xls.html", bigger.exportResponse.body);
  checkContributionExport(
    bigger.exportResponse,
    parseLabeledCount(bigger.resultsHtml, "Full Disclosure Reports"),
    "contribution export 2024",
    gates,
    { requireIndependentEmployerOccupation: false }
  );

  // --- Gate 4c: historical query (2002 = year floor, stable data). ---
  const small = await runContributionSearch(session, bigger.postBody, "2002");
  saveArtifact("contribution_results_2002.html", small.resultsHtml);
  saveArtifact("contribution_export_2002.xls.html", small.exportResponse.body);
  checkContributionExport(
    small.exportResponse,
    parseLabeledCount(small.resultsHtml, "Full Disclosure Reports"),
    "contribution export 2002",
    gates,
    { requireIndependentEmployerOccupation: false }
  );

  // --- Gate 5: outside-spending search + export. ---
  const outsideUrl = buildMissouriMecUrl(MISSOURI_MEC_PAGES.outsideSpendingSearch);
  const outsidePage = await session.get(outsideUrl);
  const outsideResults = await session.postForm(
    outsideUrl,
    {
      ...requireHiddenFields(outsidePage.text(), outsideUrl),
      [`${SEARCH}ddYear`]: "2026",
      [`${SEARCH}txtLastName`]: "",
      [`${SEARCH}txtOfficeSought`]: "",
      [`${SEARCH}SO`]: "",
      [`${SEARCH}btnSearch`]: "Search",
    },
    { referer: outsideUrl }
  );
  saveArtifact("outside_results_2026.html", outsideResults.body);
  const outsideHtml = outsideResults.text();
  const recordsFound = /([\d,]+) records found/.exec(stripTags(outsideHtml));
  const outsideCount = recordsFound === null ? null : Number.parseInt(recordsFound[1]!.replace(/,/g, ""), 10);

  const outsideExport = await session.postForm(
    outsideUrl,
    {
      ...requireHiddenFields(outsideHtml, outsideUrl),
      [`${SEARCH}ddYear`]: "2026",
      [`${SEARCH}btnExport`]: "Export Results to Excel",
    },
    { referer: outsideUrl }
  );
  saveArtifact("outside_export_2026.xls.html", outsideExport.body);
  const outsideRows = parseHtmlTableRows(outsideExport.text());
  const outsideHeader = outsideRows[0] ?? [];
  const outsideData = outsideRows.slice(1);
  const stances = new Set(outsideData.map((row) => row[2] ?? ""));
  const badOutsideAmounts = outsideData.filter((row) => parseCurrency(row[4] ?? "") === null).length;
  const outsideTotalCents = outsideData.reduce((total, row) => total + (parseCurrency(row[4] ?? "") ?? 0), 0);
  console.log(
    `outside spending 2026: ${outsideCount ?? "?"} records, export ${outsideData.length} rows, ${usd(outsideTotalCents)} summed, stances [${[...stances].sort().join(", ")}]`
  );
  gates.push({
    name: "outside export: pinned 7-column header, rows equal records-found, Support/Oppose only",
    pass:
      outsideCount !== null &&
      outsideData.length === outsideCount &&
      outsideHeader.join("|") === OUTSIDE_EXPORT_HEADER.join("|") &&
      [...stances].every((stance) => stance === "Support" || stance === "Oppose") &&
      badOutsideAmounts === 0,
    detail: `export ${outsideData.length} vs found ${outsideCount ?? "?"}, ${badOutsideAmounts} bad amounts`,
  });
  gates.push({
    name: "outside export still has NO MECID column (resolver must use the committee-link postback)",
    pass: !outsideHeader.includes("MECID"),
    detail: `header: ${outsideHeader.join(" | ")}`,
  });

  // --- Gate 6: spender MECID via the committee-link 302. ---
  const linkResponse = await session.postForm(
    outsideUrl,
    {
      ...requireHiddenFields(outsideHtml, outsideUrl),
      __EVENTTARGET: `${SEARCH}grvExpenditures$ctl02$lbtnCommittee`,
      __EVENTARGUMENT: "",
      [`${SEARCH}ddYear`]: "2026",
    },
    { referer: outsideUrl }
  );
  const mecidMatch = /CommInfo\.aspx\?mecid=([A-Z]?\d+)/i.exec(linkResponse.redirectLocation ?? "");
  gates.push({
    name: "committee-link postback resolves the spender MECID via 302 Location",
    pass: linkResponse.status === 302 && mecidMatch !== null,
    detail: `status ${linkResponse.status}, location ${linkResponse.redirectLocation ?? "none"}`,
  });

  // --- Gate 7: election-search cascade -> office vocabulary. ---
  const electionUrl = buildMissouriMecUrl(MISSOURI_MEC_PAGES.electionSearch);
  const electionPage = await session.get(electionUrl);
  const yearStep = await session.postForm(
    electionUrl,
    {
      ...requireHiddenFields(electionPage.text(), electionUrl),
      __EVENTTARGET: `${SEARCH}ddElectionYear`,
      __EVENTARGUMENT: "",
      [`${SEARCH}ddElectionYear`]: "2026",
    },
    { referer: electionUrl }
  );
  const dateStep = await session.postForm(
    electionUrl,
    {
      ...requireHiddenFields(yearStep.text(), electionUrl),
      __EVENTTARGET: `${SEARCH}ddElectionDate`,
      __EVENTARGUMENT: "",
      [`${SEARCH}ddElectionYear`]: "2026",
      [`${SEARCH}ddElectionDate`]: "11/03/2026",
    },
    { referer: electionUrl }
  );
  saveArtifact("election_search_offices.html", dateStep.body);
  const officeOptions = (parseSelectOptions(dateStep.text(), "ddPoliticalOffice") ?? []).filter(
    (option) => option.value !== "0"
  );
  console.log(`office vocabulary (11/03/2026): ${officeOptions.length} offices`);
  for (const option of officeOptions) {
    console.log(`  ${option.label}`);
  }
  gates.push({
    name: "election-search cascade yields the closed office vocabulary",
    pass:
      officeOptions.length >= 20 &&
      OFFICE_VOCABULARY_SENTINELS.every((sentinel) => officeOptions.some((option) => option.label === sentinel)),
    detail: `${officeOptions.length} offices, sentinels ${OFFICE_VOCABULARY_SENTINELS.every((sentinel) => officeOptions.some((option) => option.label === sentinel)) ? "present" : "MISSING"}`,
  });

  // --- Gate 8: Committee Info gold set + report inventory (gate 3, reachable). ---
  const allReportRows: CommitteeReportRow[] = [];
  for (const gold of GOLD_COMMITTEES) {
    const infoUrl = buildMissouriMecUrl(MISSOURI_MEC_PAGES.committeeInfo, { MECID: gold.mecid });
    const info = await session.get(infoUrl);
    saveArtifact(`comminfo_${gold.mecid}.html`, info.body);
    const html = info.text();
    const echo = parseSpanSeries(html, "ContentPlaceHolder1", "lblMECID")[0] ?? stripTags(/lblMECID[^>]*>([^<]*)/.exec(html)?.[1] ?? "");
    const dates = parseSpanSeries(html, "gvElecHistory", "lblElecYear");
    const types = parseSpanSeries(html, "gvElecHistory", "lblElectionType");
    const offices = parseSpanSeries(html, "gvElecHistory", "lblSub");
    const subdivisions = parseSpanSeries(html, "gvElecHistory", "lblPolSub");
    const committeeName = /lblCommName[^>]*>([^<]*)/.exec(html)?.[1] ?? "?";
    console.log(`CommInfo ${gold.mecid} (${gold.note}): ${committeeName}`);
    for (let index = 0; index < dates.length; index += 1) {
      console.log(
        `  ${dates[index]} ${types[index] ?? ""} -> ${offices[index] ?? "?"} ${subdivisions[index] ?? ""}`
      );
    }
    gates.push({
      name: `CommInfo ${gold.mecid}: MECID echoed, election history parses (date/type/office/subdivision per row)`,
      pass:
        echo === gold.mecid &&
        dates.length > 0 &&
        dates.length === offices.length &&
        dates.length === types.length &&
        dates.length === subdivisions.length &&
        dates.every((date) => /^\d{1,2}\/\d{1,2}\/\d{4}$/.test(date)),
      detail: `echo=${echo || "?"}, ${dates.length} rows (types ${types.length}, offices ${offices.length}, subdivisions ${subdivisions.length})`,
    });

    // Report inventory (gate 3, reachable half): the lbtnReports postback +
    // year expansion yields report id/type/filed-date/amendment-lineage.
    const inventory = await fetchReportInventory(session, infoUrl, html);
    saveArtifact(`comminfo_${gold.mecid}_reports.html`, inventory.expandedHtml);
    for (const row of inventory.rows) {
      allReportRows.push({ mecid: gold.mecid, report: row.report });
      console.log(`    report ${row.cpid} [${classifyReportType(row.report)}] ${row.report} (${row.dateFiled})`);
    }
    gates.push({
      name: `CommInfo ${gold.mecid}: report inventory parses (id/type/filed-date per row)`,
      pass:
        inventory.rows.length > 0 &&
        inventory.rows.every(
          (row) =>
            /^\d+$/.test(row.cpid) &&
            row.report !== "" &&
            /^\d{1,2}\/\d{1,2}\/\d{4}$/.test(row.dateFiled)
        ),
      detail: `${inventory.rows.length} reports, families [${[...new Set(inventory.rows.map((row) => classifyReportType(row.report)))].sort().join(", ")}]`,
    });
  }

  // --- Gate 3 (aggregate): the gold set exhibits the required report-type
  // families and at least one amendment pair (an "AMENDED X" whose base "X"
  // also filed) — the report-inventory backbone for fact 2/3. The DOLLAR
  // semantics of those reports (replace-vs-delta, carry-forward, cover cent
  // reconciliation) are NOT provable here: the report cover is reCAPTCHA
  // Enterprise-gated (VerifyClick), so it is out of scope for the plain
  // client (see the header note; totals must come from itemized transactions
  // or the MEC bulk extract). ---
  const seenFamilies = new Set(allReportRows.map((row) => classifyReportType(row.report)));
  const missingFamilies = REQUIRED_REPORT_FAMILIES.filter((family) => !seenFamilies.has(family));
  const amendmentPairs = findSameCommitteeAmendmentPairs(allReportRows);
  console.log(
    `\nreport-type families across gold set: [${[...seenFamilies].sort().join(", ")}]; ` +
      `amendment pairs: ${amendmentPairs.length}`
  );
  gates.push({
    name: "report inventory: required type families present and an amendment pair (AMENDED X + base X) exists",
    pass: missingFamilies.length === 0 && amendmentPairs.length > 0,
    detail: `missing families [${missingFamilies.join(", ") || "none"}], ${amendmentPairs.length} amendment pair(s)`,
  });

  // --- Summary. ---
  console.log("\n=== Phase 0 gates ===");
  let failures = 0;
  for (const gate of gates) {
    const status = gate.pass ? "PASS" : "FAIL";
    if (!gate.pass) failures += 1;
    console.log(`${status}  ${gate.name} — ${gate.detail}`);
  }
  if (failures > 0) {
    process.exitCode = 1;
    console.log(`\n${failures} gate(s) failed`);
  } else {
    console.log("\nall gates passed");
  }
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    const message = error instanceof Error ? error.message : String(error);
    console.error("Missouri candidate finance probe failed:", message);
    process.exitCode = 1;
  });
}
