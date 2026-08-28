// Phase 0 probe for the Delaware finance module (plan-delaware-finance.md).
// NO schema, NO database writes: exercises the CFRS portal live through
// delawareCfrsClient/delawareCfrsParsers and checks the plan's Phase 0 gates.
// Every pinned constant (paths, field sets, CSV headers, gold committees) was
// derived by hand from live pages on 2026-08-26. A FAIL means the portal
// changed — re-verify by hand before building on top of it.
//
// Gates (numbering follows the plan's Phase 0 list):
//   1. Acquisition: fresh session -> GET search page (warmup) -> full-field
//      search POST -> stored-search total -> CSV export parses with row
//      count == total. Registrant autocomplete resolves the gold MemberId.
//   2. Determinism: the same receipts flow on a SECOND fresh session yields
//      the identical parsed row multiset (count, signed sum, sorted hash).
//      Raw byte equality is reported but advisory.
//   3. Expenses flow: OtherSearch POST + CSV export, count == total.
//   4. Filed-report inventory: search POST + grid JSON; the View-Current
//      re-search yields a subset; amended filing periods are identified.
//   5. Cover extraction: every current, non-scanned report PDF yields a
//      STATEMENT OF ACCOUNT BALANCE whose cash identity holds (the extractor
//      enforces beginning + receipts − expenditures = ending).
//   6. Amendment semantics (the plan's #1 question): the receipts/expenses
//      CSV sums are compared against the CURRENT covers PER FILING PERIOD
//      (not one grand total, where cross-period differences could cancel).
//      Every period matching to the cent -> the transaction search returns
//      current-version rows only (Nevada behavior); any period over ->
//      original+amended rows coexist (Missouri behavior). The gate passes
//      only on a definitive verdict. As direct evidence, the receipts grid
//      JSON is paged for the stored search and the FileAmendedVersion
//      distribution of the amended periods' rows is reported (advisory —
//      the endpoint's shape is not a pinned contract).
//   7. TP registration stance: ShowReview (clean URL) parses the pinned
//      multi-affiliation table (CNDW) and the pinned empty table (DLGA).
//   8. Registry sweep: type-01 committee search pages through the grid JSON;
//      CF_ID completeness/uniqueness decides the plan's identity gate;
//      OfficeSought vocabulary is captured for delawareFinanceEligibleOffices.
//   9. Occupation non-blank rate: statewide 2024 receipts measure the legacy
//      Employer Name / Employer Occupation fill rate (hard-fact-1 baseline).
//      The export must reconcile against the grid total within a small bound
//      (the statewide-scale count quirk) before the rate counts as evidence.
//  10. Cycle-window evidence: Meyer's receipts split by filing-period year
//      must show both the 2022 (county-era) and 2024 (governor) money —
//      the § 8002(11) windowing input.
//  11. Office evidence for the resolver: registry rows carry no OfficeSought,
//      so the per-committee office source is the filed-report grid's Office
//      column and the receipts CSV's Office column — both must be populated
//      on every gold-committee row.
//
// Artifacts: set DELAWARE_CFRS_PROBE_ARTIFACT_DIR to save every fetched body.
// PII: receipt exports carry contributor street addresses — the artifact dir
// must stay out of git (0700/0600), and this script prints only aggregates,
// headers, committee names, and totals, never contributor rows.

import { createHash } from "node:crypto";
import { chmodSync, mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { pathToFileURL } from "node:url";

import {
  DELAWARE_CFRS_EXPORT_QUERY,
  DELAWARE_CFRS_PAGES,
  DELAWARE_CFRS_THEME_QUERY,
  buildDelawareCfrsUrl,
  buildDelawareCommitteeSearchFields,
  buildDelawareExpensesSearchFields,
  buildDelawareFiledReportsSearchFields,
  buildDelawareReceiptsSearchFields,
  createDelawareCfrsSession,
  looksLikeDelawareCfrsHtml,
  type DelawareCfrsSession,
} from "../pipeline/delawareFinance/delawareCfrsClient.js";
import {
  extractDelawareGridTotal,
  extractDelawareReportCover,
  parseDelawareAmountCents,
  parseDelawareCommitteeGridJson,
  parseDelawareExpensesCsv,
  parseDelawareFiledReportsHtml,
  parseDelawareReceiptsCsv,
  parseDelawareRegistrantSuggestions,
  parseDelawareTpAffiliations,
  type DelawareCommitteeGridRow,
  type DelawareFiledReportRow,
  type DelawareReceiptCsvRow,
} from "../pipeline/delawareFinance/delawareCfrsParsers.js";

/** Gold committees pinned live 2026-08-26. */
const GOLD = {
  meyer: {
    memberId: 558171,
    cfId: "01005311",
    name: "Meyer for Delaware",
    note: "Gov 2024 winner; committee spans the 2022 county-executive era too; has amended filings",
  },
  cndw: {
    memberId: 642221,
    cfId: "04006103",
    name: "Citizens for a New Delaware Way 3rd Party Advertiser",
    affiliations: [
      { candidateName: "Bethany Hall-Long", position: "Oppose" },
      { candidateName: "Matthew Meyer", position: "Support" },
    ],
  },
  dlga: { memberId: 643731, cfId: "04006142", name: "DLGA PAC", note: "registered TP advertiser with an EMPTY affiliation table" },
} as const;

/** Statewide occupancy-rate sample year (36,718 receipts observed live). */
const OCCUPATION_SAMPLE_YEAR = "2024";

type Gate = { name: string; pass: boolean; detail: string };

const artifactDir = process.env.DELAWARE_CFRS_PROBE_ARTIFACT_DIR ?? null;
if (artifactDir !== null) {
  mkdirSync(artifactDir, { recursive: true, mode: 0o700 });
  chmodSync(artifactDir, 0o700);
}

function saveArtifact(name: string, body: string | Buffer): void {
  if (artifactDir === null) {
    return;
  }
  // Artifact names can embed portal-derived strings (report file names) —
  // neutralize path separators and friends so a hostile or drifted portal
  // value can never escape artifactDir.
  const path = join(artifactDir, name.replace(/[^A-Za-z0-9._-]/g, "_"));
  writeFileSync(path, body, { mode: 0o600 });
}

function usd(cents: number): string {
  const sign = cents < 0 ? "-" : "";
  const abs = Math.abs(cents);
  return `${sign}$${Math.trunc(abs / 100).toLocaleString("en-US")}.${String(abs % 100).padStart(2, "0")}`;
}

function newSession(): DelawareCfrsSession {
  return createDelawareCfrsSession({ log: (message) => console.log(`  [client] ${message}`) });
}

/**
 * The receipts flow: warmup GET -> full-field search POST -> stored-search
 * total -> CSV export. Returns the parsed rows plus the raw CSV for the
 * determinism comparison.
 */
async function runReceiptsFlow(
  session: DelawareCfrsSession,
  overrides: Parameters<typeof buildDelawareReceiptsSearchFields>[0],
  label: string
): Promise<{ total: number | null; csvText: string; rows: DelawareReceiptCsvRow[]; malformed: number }> {
  const searchUrl = buildDelawareCfrsUrl(DELAWARE_CFRS_PAGES.receiptsSearch, { ...DELAWARE_CFRS_THEME_QUERY });
  const warmupUrl = buildDelawareCfrsUrl(DELAWARE_CFRS_PAGES.receiptsSearch);
  await session.get(warmupUrl);
  const searchResponse = await session.postForm(searchUrl, buildDelawareReceiptsSearchFields(overrides), {
    referer: warmupUrl,
  });
  const total = extractDelawareGridTotal(searchResponse.text());
  saveArtifact(`${label}_search.html`, searchResponse.body);
  const exportResponse = await session.get(
    buildDelawareCfrsUrl(DELAWARE_CFRS_PAGES.receiptsExportCsv, { ...DELAWARE_CFRS_EXPORT_QUERY }),
    { referer: searchUrl }
  );
  const csvText = exportResponse.text();
  saveArtifact(`${label}_receipts.csv`, exportResponse.body);
  if (looksLikeDelawareCfrsHtml(csvText)) {
    throw new Error(`${label}: receipts export answered HTML, not CSV`);
  }
  const parsed = parseDelawareReceiptsCsv(csvText);
  return { total, csvText, rows: parsed.rows, malformed: parsed.malformedRowCount };
}

function sumReceiptsCents(rows: readonly DelawareReceiptCsvRow[]): number {
  return rows.reduce((sum, row) => sum + parseDelawareAmountCents(row["Contribution Amount"]), 0);
}

/** Order-independent fingerprint of the parsed row multiset. */
function multisetHash(rows: readonly Record<string, string>[]): string {
  const hash = createHash("sha256");
  const lines = rows.map((row) => JSON.stringify(row)).sort();
  for (const line of lines) {
    hash.update(line);
    hash.update("\n");
  }
  return hash.digest("hex");
}

async function main(): Promise<void> {
  const gates: Gate[] = [];
  console.log("Delaware CFRS Phase 0 probe (plan-delaware-finance.md)");
  console.log(artifactDir === null ? "artifacts: NOT saved (set DELAWARE_CFRS_PROBE_ARTIFACT_DIR)" : `artifacts: ${artifactDir}`);

  // --- Gate 1: acquisition (registrant autocomplete + receipts flow). ---
  const session = newSession();
  const suggestionsResponse = await session.get(
    buildDelawareCfrsUrl(DELAWARE_CFRS_PAGES.findRegistrants, { q: "Meyer" })
  );
  const suggestions = parseDelawareRegistrantSuggestions(suggestionsResponse.text());
  const meyerSuggestion = suggestions.find((entry) => entry.memberId === GOLD.meyer.memberId);
  gates.push({
    name: "registrant autocomplete resolves the gold committee",
    pass: meyerSuggestion !== undefined && meyerSuggestion.name === GOLD.meyer.name,
    detail: `${suggestions.length} suggestions for "Meyer"; gold hit: ${meyerSuggestion?.name ?? "MISSING"}(${meyerSuggestion?.status ?? "?"})`,
  });

  const meyerOverrides = { txtReceivingRegistrant: GOLD.meyer.name, MemberId: String(GOLD.meyer.memberId) };
  const firstRun = await runReceiptsFlow(session, meyerOverrides, "meyer_run1");
  const firstSum = sumReceiptsCents(firstRun.rows);
  console.log(
    `receipts run 1: total=${firstRun.total}, rows=${firstRun.rows.length}, malformed=${firstRun.malformed}, sum=${usd(firstSum)}`
  );
  gates.push({
    name: "receipts acquisition: CSV parses and row count == stored-search total",
    pass: firstRun.total !== null && firstRun.rows.length === firstRun.total && firstRun.malformed === 0,
    detail: `total=${firstRun.total}, rows=${firstRun.rows.length}, malformed=${firstRun.malformed}`,
  });

  // --- Gate 2: determinism on a second fresh session. ---
  const secondRun = await runReceiptsFlow(newSession(), meyerOverrides, "meyer_run2");
  const secondSum = sumReceiptsCents(secondRun.rows);
  const bytesEqual = firstRun.csvText === secondRun.csvText;
  gates.push({
    name: "determinism: two fresh sessions yield the identical parsed row multiset",
    pass:
      firstRun.rows.length === secondRun.rows.length &&
      firstSum === secondSum &&
      multisetHash(firstRun.rows) === multisetHash(secondRun.rows),
    detail: `rows ${firstRun.rows.length}/${secondRun.rows.length}, sums ${usd(firstSum)}/${usd(secondSum)}, raw bytes ${bytesEqual ? "identical" : "differ (advisory)"}`,
  });

  // --- Gate 3: expenses flow (same session as run 1 — searches are typed slots). ---
  const expensesSearchUrl = buildDelawareCfrsUrl(DELAWARE_CFRS_PAGES.expensesSearchPost, {
    ...DELAWARE_CFRS_THEME_QUERY,
  });
  await session.get(buildDelawareCfrsUrl(DELAWARE_CFRS_PAGES.expensesSearch));
  const expensesSearch = await session.postForm(
    expensesSearchUrl,
    buildDelawareExpensesSearchFields({ txtRegistrant: GOLD.meyer.name, MemberId: String(GOLD.meyer.memberId) })
  );
  const expensesTotal = extractDelawareGridTotal(expensesSearch.text());
  const expensesExport = await session.get(
    buildDelawareCfrsUrl(DELAWARE_CFRS_PAGES.expensesExportCsv, { ...DELAWARE_CFRS_EXPORT_QUERY }),
    { referer: expensesSearchUrl }
  );
  saveArtifact("meyer_expenses.csv", expensesExport.body);
  const expensesParsed = parseDelawareExpensesCsv(expensesExport.text());
  const expensesSum = expensesParsed.rows.reduce(
    (sum, row) => sum + parseDelawareAmountCents(row["Amount($)"]),
    0
  );
  console.log(
    `expenses: total=${expensesTotal}, rows=${expensesParsed.rows.length}, malformed=${expensesParsed.malformedRowCount}, sum=${usd(expensesSum)}`
  );
  gates.push({
    name: "expenses acquisition: CSV parses and row count == stored-search total",
    pass:
      expensesTotal !== null &&
      expensesParsed.rows.length === expensesTotal &&
      expensesParsed.malformedRowCount === 0,
    detail: `total=${expensesTotal}, rows=${expensesParsed.rows.length}, malformed=${expensesParsed.malformedRowCount}`,
  });

  // --- Gate 4: filed-report inventory, all versions vs View Current. The
  // grid is server-operation-mode telerik: the "ajax" URL returns the FULL
  // page with the requested slice rendered; Grid-size=200 renders all rows
  // for a gold committee in one page. ---
  async function fetchFiledReports(viewCurrent: boolean): Promise<DelawareFiledReportRow[]> {
    const searchUrl = buildDelawareCfrsUrl(DELAWARE_CFRS_PAGES.filedReportsSearch, { ...DELAWARE_CFRS_THEME_QUERY });
    await session.get(buildDelawareCfrsUrl(DELAWARE_CFRS_PAGES.filedReportsSearch));
    const fields: Record<string, string> = buildDelawareFiledReportsSearchFields({
      txtCommitteeName: GOLD.meyer.name,
      MemberId: String(GOLD.meyer.memberId),
      ...(viewCurrent ? { hdnViewCurrent: "true" } : {}),
    });
    if (viewCurrent) {
      fields.chkViewCurrReport = "on";
    }
    await session.postForm(searchUrl, fields);
    const gridResponse = await session.get(
      buildDelawareCfrsUrl(DELAWARE_CFRS_PAGES.filedReportsGridJson, {
        ajax: "True",
        "Grid-page": "1",
        "Grid-orderBy": "~",
        "Grid-groupBy": "~",
        "Grid-filter": "~",
        "Grid-size": "200",
        theme: "vista",
      }),
      { referer: searchUrl }
    );
    saveArtifact(`meyer_filed_reports_${viewCurrent ? "current" : "all"}.html`, gridResponse.body);
    const parsed = parseDelawareFiledReportsHtml(gridResponse.text());
    if (parsed.total !== null && parsed.rows.length !== parsed.total) {
      throw new Error(
        `filed-reports slice incomplete: ${parsed.rows.length} rows rendered vs total ${parsed.total} — page the grid`
      );
    }
    return parsed.rows;
  }

  const allReports = await fetchFiledReports(false);
  const currentReports = await fetchFiledReports(true);
  const byPeriod = new Map<string, DelawareFiledReportRow[]>();
  for (const row of allReports) {
    const list = byPeriod.get(row.filingPeriodName) ?? [];
    list.push(row);
    byPeriod.set(row.filingPeriodName, list);
  }
  const amendedPeriods = [...byPeriod.entries()].filter(([, rows]) => rows.length > 1);
  console.log(`filed reports: all=${allReports.length}, current=${currentReports.length}, amended periods=${amendedPeriods.length}`);
  for (const row of allReports) {
    console.log(
      `  [${row.filingPeriodName}] ${row.reportName} filed=${row.dateFiled || "?"} pdf=${row.document === null ? "none" : "yes"}`
    );
  }
  gates.push({
    name: "filed-report inventory: rendered grid parses; amended lineage visible (View Current = latest report only)",
    pass:
      allReports.length > 0 &&
      currentReports.length === 1 &&
      amendedPeriods.length > 0 &&
      allReports.every(
        (row) =>
          row.cfId === GOLD.meyer.cfId &&
          (row.document === null || row.document.memberId === GOLD.meyer.memberId)
      ),
    detail: `all=${allReports.length}, current=${currentReports.length} (portal semantics: single most recent report), amended periods=${amendedPeriods.length}`,
  });

  // --- Gate 5: cover extraction on EVERY report PDF (all versions). The
  // View Current control only surfaces the single latest report, so the
  // canonical per-period version is selected from the PDFs themselves:
  // group by FilingCalendarID (the filing-period calendar entry) and take
  // the highest footer "Version:". ---
  type CoverRecord = { row: DelawareFiledReportRow; cover: Awaited<ReturnType<typeof extractDelawareReportCover>> };
  const covers: CoverRecord[] = [];
  let coversFailed = 0;
  let noDocumentSkipped = 0;
  for (const row of allReports) {
    if (row.document === null) {
      noDocumentSkipped += 1;
      continue;
    }
    const pdfResponse = await session.get(
      buildDelawareCfrsUrl(DELAWARE_CFRS_PAGES.filedReportPdf, {
        FileName: row.document.publicReportFileName,
        CommitteeID: String(row.document.memberId),
        FilingCalendarID: String(row.document.filingCalendarId),
      })
    );
    saveArtifact(
      `meyer_report_${row.document.filingCalendarId}_${row.document.publicReportFileName.slice(-16)}`,
      pdfResponse.body
    );
    if (!(pdfResponse.contentType ?? "").includes("pdf")) {
      coversFailed += 1;
      console.log(`  cover [${row.filingPeriodName}] not a PDF (${pdfResponse.contentType})`);
      continue;
    }
    try {
      const cover = await extractDelawareReportCover(new Uint8Array(pdfResponse.body));
      covers.push({ row, cover });
      console.log(
        `  cover [${row.filingPeriodName}] ${row.reportName} v${cover.documentVersion} ` +
          `${cover.reportingPeriodFrom ?? "?"}..${cover.reportingPeriodTo ?? "?"}: ` +
          `beg=${usd(cover.beginningBalanceCents)} rec=${usd(cover.receiptsCents)} exp=${usd(cover.expendituresCents)} end=${usd(cover.endingBalanceCents)} (${cover.method})`
      );
    } catch (error) {
      coversFailed += 1;
      console.log(
        `  cover [${row.filingPeriodName}] EXTRACTION FAILED: ${error instanceof Error ? error.message : String(error)}`
      );
    }
  }
  gates.push({
    name: "cover extraction: every report PDF yields a cash-identity-valid cover with version + period",
    pass:
      covers.length > 0 &&
      coversFailed === 0 &&
      covers.every(
        (record) =>
          record.cover.documentVersion !== null &&
          record.cover.reportingPeriodFrom !== null &&
          record.cover.reportingPeriodTo !== null
      ),
    detail: `extracted=${covers.length}, failed=${coversFailed}, no-document skipped=${noDocumentSkipped}`,
  });

  // --- Canonical per-period selection: max version per FilingCalendarID. ---
  const byCalendar = new Map<number, CoverRecord[]>();
  for (const record of covers) {
    const key = record.row.document!.filingCalendarId;
    const list = byCalendar.get(key) ?? [];
    list.push(record);
    byCalendar.set(key, list);
  }
  const canonical: CoverRecord[] = [];
  let ambiguousPeriods = 0;
  for (const [, records] of byCalendar) {
    const maxVersion = Math.max(...records.map((record) => record.cover.documentVersion ?? -1));
    const winners = records.filter((record) => (record.cover.documentVersion ?? -1) === maxVersion);
    if (winners.length > 1) {
      ambiguousPeriods += 1;
    }
    canonical.push(winners[0]!);
  }
  const parseUsDate = (value: string | null): number => {
    const match = value === null ? null : /^(\d{2})\/(\d{2})\/(\d{4})$/.exec(value);
    return match === null ? 0 : Number.parseInt(match[3]!, 10) * 10_000 + Number.parseInt(match[1]!, 10) * 100 + Number.parseInt(match[2]!, 10);
  };
  canonical.sort((a, b) => parseUsDate(a.cover.reportingPeriodFrom) - parseUsDate(b.cover.reportingPeriodFrom));
  let chainBreaks = 0;
  for (let index = 1; index < canonical.length; index += 1) {
    if (canonical[index]!.cover.beginningBalanceCents !== canonical[index - 1]!.cover.endingBalanceCents) {
      chainBreaks += 1;
      console.log(
        `  chain break: [${canonical[index - 1]!.row.filingPeriodName}] end=${usd(canonical[index - 1]!.cover.endingBalanceCents)} -> ` +
          `[${canonical[index]!.row.filingPeriodName}] beg=${usd(canonical[index]!.cover.beginningBalanceCents)}`
      );
    }
  }
  const coverReceiptsCents = canonical.reduce((sum, record) => sum + record.cover.receiptsCents, 0);
  const coverExpendituresCents = canonical.reduce((sum, record) => sum + record.cover.expendituresCents, 0);
  gates.push({
    name: "canonical selection: unambiguous max-version per filing period; balance chain continuous",
    pass: canonical.length > 0 && ambiguousPeriods === 0 && chainBreaks === 0,
    detail: `periods=${canonical.length}, ambiguous=${ambiguousPeriods}, chain breaks=${chainBreaks}`,
  });

  // --- Gate 6: amendment semantics — CSV vs canonical covers PER FILING
  // PERIOD. A grand-total-only comparison could hide cross-period
  // cancellation (stale rows in one period offset by adjustments in
  // another), so every period must reconcile to the cent on its own. ---
  // The three artifacts render the SAME filing period under different names
  // (receipts CSV "2024 2024  General Election 11/05/2024 30 Day", filed-
  // report grid "2024 30 Day 2024 General Election 11/05/2024", expenses CSV
  // "2024 30 Day General"; annuals differ only in whitespace) — verified by
  // the 1:1 amount pairing on the 2026-08-27 run. Normalize to
  // (year, report kind, election kind) before matching.
  const periodKeyOf = (name: string): string => {
    const year = /\b(?:19|20)\d{2}\b/.exec(name)?.[0] ?? "?";
    const day = /\b(30|8)\s*Day\b/i.exec(name);
    const kind = /annual/i.test(name) ? "Annual" : day === null ? "?" : `${day[1]} Day`;
    const election = /general/i.test(name)
      ? " General"
      : /primary/i.test(name)
        ? " Primary"
        : /special/i.test(name)
          ? " Special"
          : "";
    return `${year} ${kind}${election}`;
  };
  const coverByPeriodName = new Map<string, CoverRecord>();
  let duplicateCoverKeys = 0;
  for (const record of canonical) {
    const key = periodKeyOf(record.row.filingPeriodName);
    if (coverByPeriodName.has(key)) {
      duplicateCoverKeys += 1;
    }
    coverByPeriodName.set(key, record);
  }
  const sumByPeriod = (rows: readonly Record<string, string>[], amountColumn: string): Map<string, number> => {
    const sums = new Map<string, number>();
    for (const row of rows) {
      const period = periodKeyOf(row["Filing Period"] ?? "");
      sums.set(period, (sums.get(period) ?? 0) + parseDelawareAmountCents(row[amountColumn] ?? ""));
    }
    return sums;
  };
  const csvReceiptsByPeriod = sumByPeriod(firstRun.rows, "Contribution Amount");
  const csvExpensesByPeriod = sumByPeriod(expensesParsed.rows, "Amount($)");
  const periodNames = [
    ...new Set([...coverByPeriodName.keys(), ...csvReceiptsByPeriod.keys(), ...csvExpensesByPeriod.keys()]),
  ].sort();
  let periodMismatches = duplicateCoverKeys;
  for (const period of periodNames) {
    const cover = coverByPeriodName.get(period);
    const csvReceipts = csvReceiptsByPeriod.get(period) ?? 0;
    const csvExpenses = csvExpensesByPeriod.get(period) ?? 0;
    const receiptsOk = cover !== undefined && cover.cover.receiptsCents === csvReceipts;
    const expensesOk = cover !== undefined && cover.cover.expendituresCents === csvExpenses;
    if (!receiptsOk || !expensesOk) {
      periodMismatches += 1;
    }
    console.log(
      `  period [${period}] receipts ${usd(csvReceipts)} vs cover ${cover === undefined ? "NO COVER" : usd(cover.cover.receiptsCents)} (${receiptsOk ? "match" : "MISMATCH"}); ` +
        `expenses ${usd(csvExpenses)} vs cover ${cover === undefined ? "NO COVER" : usd(cover.cover.expendituresCents)} (${expensesOk ? "match" : "MISMATCH"})`
    );
  }
  const receiptsMatch = firstSum === coverReceiptsCents;
  const expensesMatch = expensesSum === coverExpendituresCents;
  const verdict =
    receiptsMatch && expensesMatch && periodMismatches === 0
      ? "CURRENT-ONLY (search returns current-version rows; every filing period reconciles to its canonical cover)"
      : firstSum > coverReceiptsCents || expensesSum > coverExpendituresCents || periodMismatches > 0
        ? "COEXISTING VERSIONS or coverage gap (CSV disagrees with canonical covers — needs lineage dedup)"
        : "CSV BELOW COVERS (missing itemization or unparsed covers — investigate)";
  console.log(
    `amendment semantics: CSV receipts=${usd(firstSum)} vs covers=${usd(coverReceiptsCents)}; CSV expenses=${usd(expensesSum)} vs covers=${usd(coverExpendituresCents)}; period mismatches=${periodMismatches}/${periodNames.length}`
  );
  console.log(`verdict: ${verdict}`);
  gates.push({
    name: "amendment semantics: per-period cent reconciliation delivers a definitive verdict",
    pass: coversFailed === 0 && receiptsMatch && expensesMatch && periodMismatches === 0,
    detail: `receipts ${usd(firstSum)} vs ${usd(coverReceiptsCents)} (${receiptsMatch ? "MATCH" : "DIFF"}); expenses ${usd(expensesSum)} vs ${usd(coverExpendituresCents)} (${expensesMatch ? "MATCH" : "DIFF"}); per-period mismatches=${periodMismatches}/${periodNames.length} (duplicate cover keys=${duplicateCoverKeys})`,
  });

  // Advisory direct evidence: page the receipts grid JSON for the session's
  // stored Meyer search and report the FileAmendedVersion distribution of
  // rows inside the amended filing periods. The endpoint's row shape is not
  // a pinned contract (the CSV is the source of truth), so failures here are
  // reported, never fatal — the per-period reconciliation above is the gate.
  try {
    // Telerik ajax mode: the results page's selectUrl is a GET with Grid-*
    // params (a bare form POST answers an empty data array — observed
    // 2026-08-27).
    const fetchJsonPage = async (page: number): Promise<Record<string, unknown>[]> => {
      const response = await session.get(
        buildDelawareCfrsUrl(DELAWARE_CFRS_PAGES.receiptsGridJson, {
          ajax: "True",
          "Grid-page": String(page),
          "Grid-orderBy": "~",
          "Grid-groupBy": "~",
          "Grid-filter": "~",
          "Grid-size": "500",
          theme: "vista",
        }),
        { xhr: true }
      );
      const envelope = JSON.parse(response.text()) as { data?: unknown };
      if (!Array.isArray(envelope.data)) {
        throw new Error("grid JSON response has no data array");
      }
      return envelope.data as Record<string, unknown>[];
    };
    const jsonRows: Record<string, unknown>[] = [];
    for (let page = 1; page <= 40; page += 1) {
      const pageRows = await fetchJsonPage(page);
      jsonRows.push(...pageRows);
      if (pageRows.length < 500) {
        break;
      }
    }
    if (jsonRows.length !== firstRun.rows.length) {
      console.log(`  [advisory] receipts grid JSON rows=${jsonRows.length} vs CSV rows=${firstRun.rows.length} (MISMATCH)`);
    }
    const sample = jsonRows[0] ?? {};
    const periodKey = ["FilingPeriodName", "FilingPeriod", "Filing_Period"].find((key) => key in sample);
    const versionKey = ["FileAmendedVersion", "FileAmendedversion"].find((key) => key in sample);
    if (periodKey === undefined || versionKey === undefined) {
      console.log(`  [advisory] grid JSON rows lack period/version keys; keys seen: ${Object.keys(sample).join(", ")}`);
    } else {
      const amendedNames = new Set(amendedPeriods.map(([name]) => periodKeyOf(name)));
      const distributions = new Map<string, Map<string, number>>();
      for (const row of jsonRows) {
        const period = periodKeyOf(String(row[periodKey] ?? ""));
        if (!amendedNames.has(period)) {
          continue;
        }
        const distribution = distributions.get(period) ?? new Map<string, number>();
        const version = String(row[versionKey] ?? "");
        distribution.set(version, (distribution.get(version) ?? 0) + 1);
        distributions.set(period, distribution);
      }
      if (distributions.size === 0) {
        console.log("  [advisory] no receipts grid JSON rows landed in an amended filing period");
      }
      for (const [period, distribution] of distributions) {
        console.log(
          `  [advisory] amended period [${period}] FileAmendedVersion distribution: ` +
            [...distribution.entries()].map(([version, count]) => `"${version}"×${count}`).join(", ")
        );
      }
    }
  } catch (error) {
    console.log(
      `  [advisory] receipts grid JSON inspection unavailable: ${error instanceof Error ? error.message : String(error)}`
    );
  }

  // --- Gate 7: TP registration stance (clean URLs, no portal-link spaces).
  // ShowReview requires committee-search session context: without a prior
  // /Public/Search POST it answers the alert-wrapped rejection. ---
  await session.get(buildDelawareCfrsUrl(DELAWARE_CFRS_PAGES.committeeSearch));
  await session.postForm(
    buildDelawareCfrsUrl(DELAWARE_CFRS_PAGES.committeeSearchPost, { ...DELAWARE_CFRS_THEME_QUERY }),
    buildDelawareCommitteeSearchFields({ CommitteeType: "04" })
  );
  const cndwResponse = await session.get(
    buildDelawareCfrsUrl(DELAWARE_CFRS_PAGES.statementOfOrganization, {
      memberID: String(GOLD.cndw.memberId),
      memVersID: "2",
      cTypeCode: "04",
      ftype: "SO",
      fpath: "",
    })
  );
  saveArtifact("cndw_showreview.html", cndwResponse.body);
  const cndwAffiliations = parseDelawareTpAffiliations(cndwResponse.text());
  const cndwMatches =
    cndwAffiliations.length === GOLD.cndw.affiliations.length &&
    GOLD.cndw.affiliations.every((expected) =>
      cndwAffiliations.some(
        (row) => row.candidateName === expected.candidateName && row.position === expected.position
      )
    );
  const dlgaResponse = await session.get(
    buildDelawareCfrsUrl(DELAWARE_CFRS_PAGES.statementOfOrganization, {
      memberID: String(GOLD.dlga.memberId),
      memVersID: "1",
      cTypeCode: "04",
      ftype: "SO",
      fpath: "",
    })
  );
  saveArtifact("dlga_showreview.html", dlgaResponse.body);
  const dlgaAffiliations = parseDelawareTpAffiliations(dlgaResponse.text());
  console.log(
    `TP stance: CNDW ${cndwAffiliations.map((row) => `${row.position} ${row.candidateName}`).join("; ")} | DLGA rows=${dlgaAffiliations.length}`
  );
  gates.push({
    name: "TP registration stance: pinned affiliation tables parse (multi-row + empty), clean URLs",
    pass: cndwMatches && dlgaAffiliations.length === 0,
    detail: `CNDW rows=${cndwAffiliations.length} (${cndwMatches ? "pinned rows found" : "MISMATCH"}), DLGA rows=${dlgaAffiliations.length}`,
  });

  // --- Gate 8: registry sweep (type 01), CF_ID completeness, office vocabulary. ---
  await session.get(buildDelawareCfrsUrl(DELAWARE_CFRS_PAGES.committeeSearch));
  await session.postForm(
    buildDelawareCfrsUrl(DELAWARE_CFRS_PAGES.committeeSearchPost, { ...DELAWARE_CFRS_THEME_QUERY }),
    buildDelawareCommitteeSearchFields({ CommitteeType: "01" })
  );
  const registryRows: DelawareCommitteeGridRow[] = [];
  const pageSize = 500;
  let registryTotal: number | null = null;
  for (let page = 1; page <= 40; page += 1) {
    const gridResponse = await session.postForm(
      buildDelawareCfrsUrl(DELAWARE_CFRS_PAGES.committeeGridJson, { ...DELAWARE_CFRS_THEME_QUERY }),
      { page: String(page), size: String(pageSize), orderBy: "", groupBy: "", filter: "" },
      { xhr: true }
    );
    const parsed = parseDelawareCommitteeGridJson(gridResponse.text());
    registryTotal = parsed.total;
    registryRows.push(...parsed.rows);
    if (parsed.rows.length < pageSize) {
      break;
    }
  }
  saveArtifact("registry_type01.json", JSON.stringify(registryRows, null, 1));
  // The registry emits one row per statement-of-organization VERSION/form —
  // MemberID repeats across rows. Identity is judged per committee: every
  // committee (MemberID) must carry exactly one distinct non-empty CF_ID,
  // and no CF_ID may span two committees.
  const byMember = new Map<number, Set<string>>();
  for (const row of registryRows) {
    const set = byMember.get(row.memberId) ?? new Set<string>();
    if (row.cfId !== "") {
      set.add(row.cfId);
    }
    byMember.set(row.memberId, set);
  }
  const membersWithoutCfId = [...byMember.values()].filter((set) => set.size === 0).length;
  const membersWithConflictingCfIds = [...byMember.values()].filter((set) => set.size > 1).length;
  const cfIdToMembers = new Map<string, Set<number>>();
  for (const row of registryRows) {
    if (row.cfId === "") continue;
    const set = cfIdToMembers.get(row.cfId) ?? new Set<number>();
    set.add(row.memberId);
    cfIdToMembers.set(row.cfId, set);
  }
  const cfIdsSpanningMembers = [...cfIdToMembers.values()].filter((set) => set.size > 1).length;
  const officeVocabulary = [...new Set(registryRows.map((row) => row.officeSought).filter((value) => value !== ""))].sort();
  console.log(
    `registry type 01: rows=${registryRows.length} (grid total=${registryTotal}; one row per statement version), ` +
      `committees=${byMember.size}, without CF_ID=${membersWithoutCfId}, conflicting CF_IDs=${membersWithConflictingCfIds}, CF_IDs spanning committees=${cfIdsSpanningMembers}`
  );
  console.log(
    officeVocabulary.length === 0
      ? "office vocabulary: EMPTY in registry rows — office data must come from committee detail or office-filtered searches"
      : `office vocabulary (${officeVocabulary.length}): ${officeVocabulary.join(" | ")}`
  );
  gates.push({
    name: "registry sweep: pages fully; one consistent CF_ID per committee (identity: CF_ID keyed, MemberID stored)",
    pass:
      registryRows.length > 0 &&
      (registryTotal === null || registryRows.length === registryTotal) &&
      membersWithConflictingCfIds === 0 &&
      cfIdsSpanningMembers === 0 &&
      membersWithoutCfId < byMember.size / 100,
    detail: `committees=${byMember.size}, without CF_ID=${membersWithoutCfId}, conflicting=${membersWithConflictingCfIds}, spanning=${cfIdsSpanningMembers}`,
  });

  // --- Gate 9: occupation non-blank rate, statewide sample year. ---
  const statewide = await runReceiptsFlow(newSession(), { FilingYear: OCCUPATION_SAMPLE_YEAR }, `statewide_${OCCUPATION_SAMPLE_YEAR}`);
  const statewideIndividuals = statewide.rows.filter((row) => row["Contributor Type"] === "Individual");
  const withOccupation = statewideIndividuals.filter((row) => row["Employer Occupation"].trim() !== "").length;
  const withEmployer = statewideIndividuals.filter((row) => row["Employer Name"].trim() !== "").length;
  const statewideDelta = statewide.total === null ? null : statewide.total - statewide.rows.length;
  console.log(
    `occupation baseline ${OCCUPATION_SAMPLE_YEAR}: rows=${statewide.rows.length} (grid total=${statewide.total}, delta=${statewideDelta}), individuals=${statewideIndividuals.length}, with occupation=${withOccupation}, with employer=${withEmployer}`
  );
  // Completeness controls: exact count reconciliation fails at statewide
  // scale (the known count quirk — and the year-only search page sometimes
  // renders total:0 while the export still streams the full year, observed
  // 2026-08-27). Two controls, either satisfies the gate:
  //   a) the grid total exists (>0) and the export's delta stays within
  //      0.5% — a truncated export blows through that bound; or
  //   b) the grid total is unusable, and the export covers every month of
  //      the sample year — a truncation would chop the tail months off.
  const statewideDeltaBounded =
    statewide.total !== null &&
    statewide.total > 0 &&
    statewideDelta !== null &&
    Math.abs(statewideDelta) <= Math.ceil(statewide.total * 0.005);
  const monthsCovered = new Set(
    statewide.rows
      .map((row) => /^(\d{1,2})\//.exec(row["Contribution Date"])?.[1] ?? "")
      .filter((month) => month !== "")
      .map((month) => Number.parseInt(month, 10))
  );
  const fullYearCovered = monthsCovered.size === 12;
  gates.push({
    name: "occupation baseline: statewide export passes a completeness control; non-blank rate measured",
    pass: statewide.rows.length > 10_000 && statewide.malformed === 0 && (statewideDeltaBounded || fullYearCovered),
    detail: `individuals=${statewideIndividuals.length}, occupation=${withOccupation} (${((withOccupation / Math.max(1, statewideIndividuals.length)) * 100).toFixed(2)}%), employer=${withEmployer}; grid-total delta=${statewideDelta} (bound ±0.5%${statewideDeltaBounded ? ", satisfied" : ", unusable/exceeded"}), months covered=${monthsCovered.size}/12`,
  });

  // --- Gate 10: cycle-window evidence from Meyer's filing-period years. ---
  const byYear = new Map<string, { rows: number; cents: number }>();
  for (const row of firstRun.rows) {
    const year = /^(\d{4})/.exec(row["Filing Period"])?.[1] ?? "????";
    const entry = byYear.get(year) ?? { rows: 0, cents: 0 };
    entry.rows += 1;
    entry.cents += parseDelawareAmountCents(row["Contribution Amount"]);
    byYear.set(year, entry);
  }
  for (const [year, entry] of [...byYear.entries()].sort()) {
    console.log(`  filing-period year ${year}: ${entry.rows} rows, ${usd(entry.cents)}`);
  }
  gates.push({
    name: "cycle-window evidence: the gold committee holds both 2022 (county-era) and 2024 (governor) money",
    pass: byYear.has("2022") && byYear.has("2024"),
    detail: `years: ${[...byYear.keys()].sort().join(", ")}`,
  });

  // --- Gate 11: office evidence for the resolver. The registry sweep's
  // OfficeSought/DistrictName are empty, so the per-committee office source
  // is the filed-report grid's Office column + the receipts CSV's Office
  // column — every gold-committee row must carry one. ---
  const reportOffices = [...new Set(allReports.map((row) => row.office))].sort();
  const csvOffices = [...new Set(firstRun.rows.map((row) => row.Office))].sort();
  const reportOfficeBlanks = allReports.filter((row) => row.office.trim() === "").length;
  const csvOfficeBlanks = firstRun.rows.filter((row) => row.Office.trim() === "").length;
  console.log(
    `office evidence: filed-report Office values [${reportOffices.join(" | ")}] (blank=${reportOfficeBlanks}); receipts CSV Office values [${csvOffices.join(" | ")}] (blank=${csvOfficeBlanks})`
  );
  gates.push({
    name: "office evidence: filed-report + receipts-CSV Office populated on every gold-committee row (registry OfficeSought is empty)",
    pass: allReports.length > 0 && reportOfficeBlanks === 0 && csvOfficeBlanks === 0,
    detail: `filed-report offices=[${reportOffices.join(" | ")}], CSV offices=[${csvOffices.join(" | ")}], blanks=${reportOfficeBlanks}/${csvOfficeBlanks}`,
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
    console.error("Delaware candidate finance probe failed:", message);
    process.exitCode = 1;
  });
}
