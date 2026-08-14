// PR 3 acquisition spike for the Rhode Island finance module
// (rhode_island_plan.md, "PR sequence" step 3).
//
// NO migration, NO database, NO writes: this script only reads the public ERTS
// portal (ricampaignfinance.com), writes every fetched artifact to the
// gitignored scratch directory as evidence, and checks the plan's spike gates.
// PR 4 lifted the transport, URL builders and parsers this probe pinned into
// `pipeline/rhodeIslandFinance/rhodeIslandErts{Client,Parsers}.ts` — the probe
// now consumes those modules, so a drift between the probe's proven shapes and
// the production acquisition is structurally impossible. The gates themselves
// stay here as the portal-change detector.
//
// Gates (each hand-derived live on 2026-08-13; a FAIL means the portal changed
// and the finding must be re-verified by hand before any build work):
//   1. Transaction report is a stateless GET. `Reporting/TransactionReport.aspx`
//      serves McKee's Q2 2026 contribution report with no cookie, no viewstate
//      and no prior search, and its summary groupings are cent-exact against
//      the plan's reconciliation fixture.
//   2. CSV export round-trip. `lnkExport` -> `DownloadFile.aspx` ->
//      `hypFileDownload` yields the detail CSV with the pinned column list,
//      and EVERY summary grouping is accounted for: either the export
//      reproduces it cent-exact, or a typed search proves the portal holds no
//      itemized rows for that type. An unexplained absence fails the gate —
//      the portal renders no exported-row count, so this reconciliation is
//      the only silent-truncation control.
//   3. The summary groupings are NOT reproducible from the export. Q2 2026
//      carries `Other Receipt $113.95` in the summary while the itemized search
//      for that type confirms no rows exist — so official totals must come from
//      the summary/CF-2 side, never from summing the export (the georgia
//      cover-arithmetic lesson, decision 2).
//   4. Organization discovery works and yields the numeric Board key: the
//      WebForms org search on `Contributions.aspx` ends in a redirect whose
//      `OrgID` query parameter is the canonical `committee_id`.
//   5. The org filing list exposes amendment state (`Amended` Yes/No) plus a
//      `FilingAmendmentSelect.aspx` link per amended family, and every version
//      is a generated (text-layer) PDF under `/ExportDocs/`.
//   6. Amendment semantics (decision 4, the release-gating question): for >= 5
//      CONCLUSIVE amended CF-2 families — both version PDFs parsed and at
//      least one comparable receipt field changed between original and latest
//      (identical totals match both versions and prove nothing) — the
//      date-bounded transaction search reproduces the LATEST version's
//      values, and therefore differs from the original's on the changed
//      fields. The public transaction data is current-ledger state.
//   7. The `dgdCF8FilingList` index paginates by WebForms pager postbacks with
//      dates descending page over page, and can be traversed to the cycle
//      boundary (decision 3c / decision 5's diff source).

import { mkdir, writeFile } from "node:fs/promises";
import path from "node:path";
import { pathToFileURL } from "node:url";

import {
  createErtsTransport,
  discoverErtsOrganization,
  ertsContributionReportUrl,
  fetchErtsContributionReport,
  fetchErtsExpenditureReport,
  fetchErtsFilingPdf,
  fetchErtsFilingVersions,
  fetchErtsOrganizationFilings,
  fetchErtsTransactionExportCsv,
  traverseErtsCf8Index,
} from "../pipeline/rhodeIslandFinance/rhodeIslandErtsClient.js";
import {
  extractErtsPdfPageItems,
  parseErtsCf8FiledDate,
  parseErtsCsv,
  parseErtsMoneyToCents,
  parseErtsSummaryGroupings,
  readErtsCf2SummaryValues,
  ERTS_CF2_SUMMARY_LABELS,
  ERTS_CONTRIBUTION_EXPORT_COLUMNS,
  ERTS_CONTRIBUTION_SUMMARY_GRID_ID,
  ERTS_CONTRIBUTION_TYPE_CODES,
  ERTS_EXPENDITURE_SUMMARY_GRID_ID,
} from "../pipeline/rhodeIslandFinance/rhodeIslandErtsParsers.js";

export const RHODE_ISLAND_FINANCE_PROBE_CACHE_DIR = "scratch/rhode-island-campaign-finance/erts";

// R.I. Gen. Laws § 17-25-3 cycle window used by the plan (decision 2).
const CYCLE_BEGIN = "01/01/2025";
const CYCLE_END = "12/31/2026";

// Reconciliation fixture: Daniel J. McKee, ERTS organization key 2235.
const MCKEE_ORG_ID = "2235";
const MCKEE_SEARCH_LAST_NAME = "McKee";
const MCKEE_ORGANIZATION_NAME = "DANIEL J MCKEE";

const Q2_2026 = { begin: "04/01/2026", end: "06/30/2026" } as const;

// Summary groupings of the Q2 2026 contribution report, hand-read from the
// portal on 2026-08-12 and again on 2026-08-13.
const EXPECTED_Q2_2026_CONTRIBUTION_SUMMARY: ReadonlyMap<string, number> = new Map([
  ["Individual", 24_126_429],
  ["PAC", 1_245_000],
  ["Interest Received", 511_677],
  ["In-Kind - Individual", 350_800],
  ["Other Receipt", 11_395],
]);

const EXPECTED_Q2_2026_EXPENDITURE_TOTAL_CENTS = 94_543_457;

// Cash receipts exclude in-kind; the CF-2 arithmetic in the plan is
// 1,355,115.78 + 258,945.01 - 945,434.57 = 668,626.22.
const EXPECTED_Q2_2026_CASH_RECEIPTS_CENTS = 25_894_501;

// How many amended CF-2 families the amendment gate must cover (decision 4).
const AMENDMENT_FAMILY_TARGET = 5;

type Gate = { name: string; pass: boolean; detail: string };

async function saveArtifact(name: string, body: Uint8Array): Promise<void> {
  const target = path.join(RHODE_ISLAND_FINANCE_PROBE_CACHE_DIR, name);
  await mkdir(path.dirname(target), { recursive: true });
  await writeFile(target, body);
}

function formatCents(cents: number): string {
  return `$${(cents / 100).toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
}

function compareTotals(actual: ReadonlyMap<string, number>, expected: ReadonlyMap<string, number>): string[] {
  const differences: string[] = [];
  for (const [label, cents] of expected) {
    const found = actual.get(label);
    if (found !== cents) differences.push(`${label}: expected ${formatCents(cents)}, got ${found === undefined ? "nothing" : formatCents(found)}`);
  }
  for (const label of actual.keys()) {
    if (!expected.has(label)) differences.push(`${label}: unexpected summary grouping ${formatCents(actual.get(label) ?? 0)}`);
  }
  return differences;
}

async function main(): Promise<void> {
  for (const arg of process.argv.slice(2)) {
    throw new Error(`Unknown Rhode Island finance probe flag: ${arg}`);
  }

  const transport = createErtsTransport({ log: (message) => console.log(`  ${message}`) });
  const gates: Gate[] = [];
  const encoder = new TextEncoder();

  // --- Gate 1: stateless contribution report + pinned summary groupings. ---
  const contribution = await fetchErtsContributionReport(transport, { orgId: MCKEE_ORG_ID, ...Q2_2026 });
  if (contribution.classification !== "rows") {
    throw new Error("ERTS contribution report for the fixture window carried no result grid");
  }
  await saveArtifact("mckee-q2-2026-contributions.html", encoder.encode(contribution.html));
  const summary = parseErtsSummaryGroupings(contribution.html, ERTS_CONTRIBUTION_SUMMARY_GRID_ID);
  const summaryDifferences = compareTotals(summary, EXPECTED_Q2_2026_CONTRIBUTION_SUMMARY);

  const expenditure = await fetchErtsExpenditureReport(transport, { orgId: MCKEE_ORG_ID, ...Q2_2026 });
  await saveArtifact("mckee-q2-2026-expenditures.html", encoder.encode(expenditure.html));
  const expenditureTotal = [...parseErtsSummaryGroupings(expenditure.html, ERTS_EXPENDITURE_SUMMARY_GRID_ID).values()].reduce(
    (total, cents) => total + cents,
    0
  );

  // Cash receipts exclude in-kind (the CF-2 reports in-kind on its own line).
  const cashReceipts = [...summary].reduce(
    (total, [label, cents]) => (/^In[- ]Kind/i.test(label) ? total : total + cents),
    0
  );
  const gate1Failures = [
    ...summaryDifferences,
    ...(expenditureTotal === EXPECTED_Q2_2026_EXPENDITURE_TOTAL_CENTS
      ? []
      : [`expenditures: expected ${formatCents(EXPECTED_Q2_2026_EXPENDITURE_TOTAL_CENTS)}, got ${formatCents(expenditureTotal)}`]),
    ...(cashReceipts === EXPECTED_Q2_2026_CASH_RECEIPTS_CENTS
      ? []
      : [`cash receipts: expected ${formatCents(EXPECTED_Q2_2026_CASH_RECEIPTS_CENTS)}, got ${formatCents(cashReceipts)}`]),
  ];
  gates.push({
    name: "1. stateless transaction reports reconcile",
    pass: gate1Failures.length === 0,
    detail:
      gate1Failures.length === 0
        ? `${summary.size} groupings cent-exact; cash receipts ${formatCents(cashReceipts)}; expenditures ${formatCents(expenditureTotal)}`
        : gate1Failures.join("; "),
  });

  // --- Gate 2: CSV export round-trip and per-type agreement. ---
  const exported = await fetchErtsTransactionExportCsv(transport, {
    reportUrl: contribution.url,
    reportHtml: contribution.html,
  });
  await saveArtifact("mckee-q2-2026-contributions.csv", encoder.encode(exported.csv));
  const csvRows = parseErtsCsv(exported.csv);
  const header = csvRows[0] ?? [];
  const headerMatches =
    header.length === ERTS_CONTRIBUTION_EXPORT_COLUMNS.length &&
    header.every((column, index) => column === ERTS_CONTRIBUTION_EXPORT_COLUMNS[index]);
  const typeIndex = header.indexOf("ContDesc");
  const amountIndex = header.indexOf("Amount");
  const exportTotals = new Map<string, number>();
  for (const row of csvRows.slice(1)) {
    const cents = parseErtsMoneyToCents(row[amountIndex] ?? "");
    if (cents === null) continue;
    exportTotals.set(row[typeIndex] ?? "", (exportTotals.get(row[typeIndex] ?? "") ?? 0) + cents);
  }
  // Every summary grouping must be accounted for in one of two ways: the
  // export reproduces it cent-exact, or a typed search proves the portal
  // itself holds no itemized rows for it. A type absent from the export
  // without that proof is evidence of silent truncation, not of a
  // summary-only type — and the portal renders no exported-row count
  // anywhere, so this reconciliation is the only truncation control.
  const exportMismatches: string[] = [];
  const confirmedSummaryOnly: string[] = [];
  for (const [label, cents] of summary) {
    const exportedCents = exportTotals.get(label);
    if (exportedCents === cents) continue;
    if (exportedCents !== undefined) {
      exportMismatches.push(`${label}: export ${formatCents(exportedCents)} != summary ${formatCents(cents)}`);
      continue;
    }
    const code = ERTS_CONTRIBUTION_TYPE_CODES[label];
    if (code === undefined) {
      exportMismatches.push(`${label}: absent from the export and not in the pinned type vocabulary`);
      continue;
    }
    let verdict: "rows" | "no_rows" | "unreadable";
    try {
      verdict = (
        await fetchErtsContributionReport(transport, { orgId: MCKEE_ORG_ID, ...Q2_2026, contributionTypeCode: code })
      ).classification;
    } catch {
      verdict = "unreadable";
    }
    if (verdict === "no_rows") {
      confirmedSummaryOnly.push(label);
    } else {
      exportMismatches.push(
        `${label}: absent from the export but the typed search ${verdict === "rows" ? "returned itemized rows — the export dropped them" : "was unreadable"}`
      );
    }
  }
  const exportOnlyTypes = [...exportTotals.keys()].filter((label) => !summary.has(label));
  const gate2Failures = [
    ...(headerMatches ? [] : [`export header changed: ${header.join(",")}`]),
    ...exportMismatches,
    ...exportOnlyTypes.map((label) => `${label}: in the export but missing from the summary groupings`),
    ...(exportTotals.size > 0 ? [] : ["export contained no parseable rows"]),
  ];
  gates.push({
    name: "2. every summary grouping is accounted for by the export",
    pass: gate2Failures.length === 0,
    detail:
      gate2Failures.length === 0
        ? `${csvRows.length - 1} rows; ${exportTotals.size} types cent-exact; ${confirmedSummaryOnly.length} confirmed summary-only`
        : gate2Failures.join("; "),
  });

  // --- Gate 3: summary-only receipt types (totals must not be summed). ---
  const summaryOnlyCents = confirmedSummaryOnly.reduce((total, label) => total + (summary.get(label) ?? 0), 0);
  gates.push({
    name: "3. official totals are not the export sum",
    pass: confirmedSummaryOnly.length > 0,
    detail:
      confirmedSummaryOnly.length > 0
        ? `${confirmedSummaryOnly.join(", ")} = ${formatCents(summaryOnlyCents)} in the summary; typed search confirms no itemized rows`
        : "every summary grouping was reproducible from the export — re-check decision 2 before trusting export sums",
  });

  // --- Gate 4: organization discovery yields the numeric Board key. ---
  const discovery = await discoverErtsOrganization(transport, {
    lastName: MCKEE_SEARCH_LAST_NAME,
    organizationName: MCKEE_ORGANIZATION_NAME,
    begin: CYCLE_BEGIN,
    end: CYCLE_END,
  });
  gates.push({
    name: "4. organization search yields the ERTS key",
    pass: discovery.orgId === MCKEE_ORG_ID,
    detail: `"${MCKEE_SEARCH_LAST_NAME}" matched ${discovery.candidates.length} organizations; ${MCKEE_ORGANIZATION_NAME} -> OrgID ${discovery.orgId}`,
  });

  // --- Gate 5: filing list exposes amendment state and version documents. ---
  const filings = await fetchErtsOrganizationFilings(transport);
  await saveArtifact("mckee-filings.html", encoder.encode(filings.html));
  const filedRows = filings.rows.filter((row) => row.filedAt !== "");
  const amendedFilings = filedRows.filter((row) => row.amended && row.filingId !== null && row.formName === "RICF2");
  gates.push({
    name: "5. filing list exposes amendment state",
    pass: filedRows.length > 0 && amendedFilings.length >= AMENDMENT_FAMILY_TARGET,
    detail: `${filedRows.length} filed reports, ${amendedFilings.length} amended CF-2 families with FilingIDs`,
  });

  // --- Gate 6: amendment semantics (decision 4). ---
  // A family only counts as evidence when it can actually discriminate
  // between the original and the latest version: both PDFs must parse, at
  // least one comparable receipt field must have CHANGED between the two
  // (identical totals match both versions and prove nothing), and the search
  // must equal the latest value on every comparable field — which, for a
  // changed field, also means it differs from the original. Extraction
  // failures and identical-total families are skipped as inconclusive, never
  // silently counted as agreement.
  const amendmentResults: string[] = [];
  let amendmentFailures = 0;
  let conclusiveFamilies = 0;
  let inconclusiveFamilies = 0;
  // Bound on portal traffic: each family costs 3-4 paced requests, and 5
  // conclusive families out of 12 would itself be a finding worth reading.
  const maxFamiliesFetched = 12;
  let familiesFetched = 0;

  // The CF-2 lines the transaction search can be held against. Each CF-2
  // line aggregates a SET of search types: line 6 is every in-kind type
  // (verified live on the 2022 window: In-Kind - Individual $3,049.67 +
  // In-Kind - Party $5,927.90 = line 6's $8,977.57), and the itemized+
  // aggregate pairs roll up the same way per decision 13's table.
  const amendmentChecks: { cf2Label: string; searchLabel: string; matches: (label: string) => boolean }[] = [
    {
      cf2Label: "2. Individuals",
      searchLabel: "Individual (+ Aggregate)",
      matches: (label) => label === "Individual" || label === "Aggregate - Individual",
    },
    {
      cf2Label: "4. Political Action Committees",
      searchLabel: "PAC (+ Aggregate)",
      matches: (label) => label === "PAC" || label === "Aggregate - PAC",
    },
    {
      cf2Label: "7. Interest Received",
      searchLabel: "Interest Received",
      matches: (label) => label === "Interest Received",
    },
    {
      cf2Label: "6. Report of In-Kind Contributions",
      searchLabel: "all In-Kind types",
      matches: (label) => /^In[- ]Kind/i.test(label),
    },
  ];

  for (const filing of amendedFilings) {
    if (conclusiveFamilies >= AMENDMENT_FAMILY_TARGET || familiesFetched >= maxFamiliesFetched) break;
    if (!/^\d{2}\/\d{2}\/\d{4}$/.test(filing.periodBegin) || !/^\d{2}\/\d{2}\/\d{4}$/.test(filing.periodEnd)) continue;
    familiesFetched += 1;
    const { versions } = await fetchErtsFilingVersions(transport, {
      filingId: filing.filingId as string,
      formName: filing.formName as string,
    });
    if (versions.length < 2) continue;

    // `grdAmendments` lists a family oldest-first (original, then each
    // amendment in filing order) — confirmed across the tested families,
    // including a three-version one: reading the last row as "latest" is what
    // makes the search comparison below agree.
    const latest = versions[versions.length - 1];
    const original = versions[0];
    const latestPdf = (await fetchErtsFilingPdf(transport, latest.pdfUrl)).pdf;
    const originalPdf = (await fetchErtsFilingPdf(transport, original.pdfUrl)).pdf;
    await saveArtifact(`cf2-${filing.filingId}-latest.pdf`, latestPdf);
    const latestValues = readErtsCf2SummaryValues(await extractErtsPdfPageItems(latestPdf), ERTS_CF2_SUMMARY_LABELS);
    const originalValues = readErtsCf2SummaryValues(await extractErtsPdfPageItems(originalPdf), ERTS_CF2_SUMMARY_LABELS);

    const comparable = amendmentChecks.filter(
      (check) => latestValues.has(check.cf2Label) && originalValues.has(check.cf2Label)
    );
    const changed = comparable.filter(
      (check) => latestValues.get(check.cf2Label) !== originalValues.get(check.cf2Label)
    );
    if (comparable.length === 0 || changed.length === 0) {
      inconclusiveFamilies += 1;
      amendmentResults.push(
        `${filing.reportType} (${filing.periodBegin}-${filing.periodEnd}, ${versions.length} versions): INCONCLUSIVE — ` +
          (comparable.length === 0 ? "CF-2 extraction yielded no comparable fields" : "versions identical on every comparable field")
      );
      continue;
    }

    const periodHtml = (
      await fetchErtsContributionReport(transport, {
        orgId: MCKEE_ORG_ID,
        begin: filing.periodBegin,
        end: filing.periodEnd,
      })
    ).html;
    const periodSummary = parseErtsSummaryGroupings(periodHtml, ERTS_CONTRIBUTION_SUMMARY_GRID_ID);

    const searchTotal = (check: (typeof amendmentChecks)[number]): number =>
      [...periodSummary].reduce((total, [label, cents]) => (check.matches(label) ? total + cents : total), 0);
    const mismatches = comparable.filter((check) => searchTotal(check) !== latestValues.get(check.cf2Label));
    conclusiveFamilies += 1;
    if (mismatches.length > 0) amendmentFailures += 1;
    amendmentResults.push(
      `${filing.reportType} (${filing.periodBegin}-${filing.periodEnd}, ${versions.length} versions, ` +
        `${changed.length}/${comparable.length} fields changed): ` +
        (mismatches.length === 0
          ? "search matches latest version (and so differs from the original on the changed fields)"
          : mismatches
              .map(
                (check) =>
                  `${check.searchLabel} ${formatCents(searchTotal(check))} vs ${check.cf2Label} ${formatCents(latestValues.get(check.cf2Label) ?? 0)}`
              )
              .join("; "))
    );
  }
  for (const line of amendmentResults) console.log(`  amendment: ${line}`);
  gates.push({
    name: "6. transaction search is current-ledger state",
    pass: conclusiveFamilies >= AMENDMENT_FAMILY_TARGET && amendmentFailures === 0,
    detail:
      `${conclusiveFamilies}/${AMENDMENT_FAMILY_TARGET} conclusive families (${inconclusiveFamilies} inconclusive of ` +
      `${familiesFetched} fetched), ${amendmentFailures} disagreed with the latest CF-2`,
  });

  // --- Gate 7: CF-8 index pagination to the cycle boundary. ---
  const cycleStart = Date.parse("2025-01-01T00:00:00Z");
  // Inclusive upper bound: without it, a 2027 re-run of this probe would
  // count next cycle's filings as this cycle's.
  const cycleEnd = Date.parse("2026-12-31T00:00:00Z");
  const cf8 = await traverseErtsCf8Index(transport, { cycleStartMs: cycleStart });
  const cycleRows = cf8.rows.filter((row) => {
    const filed = parseErtsCf8FiledDate(row.filedDate);
    return filed >= cycleStart && filed <= cycleEnd;
  });
  const independentExpenditures = cycleRows.filter((row) => /INDEPENDENT EXPENDITURE/i.test(row.filingType));
  const filingTypes = [...new Set(cycleRows.map((row) => row.filingType))];
  console.log(
    `\nCF-8 index: ${cf8.pages.length} pages, ${cf8.rows.length} rows scanned, ${cycleRows.length} in cycle, ` +
      `${independentExpenditures.length} independent expenditures; types: ${filingTypes.join(", ")}`
  );
  for (const row of independentExpenditures) {
    console.log(`  IE ${row.filedDate}  ${row.organizationName}  ${row.scannedUrl ?? "(no scan link)"}`);
  }
  gates.push({
    name: "7. CF-8 index paginates to the cycle boundary",
    pass: cf8.reachedBoundary && cf8.descending && cf8.rows.every((row) => row.scannedUrl !== null),
    detail: `${cf8.pages.length} pages traversed, dates ${cf8.descending ? "descend" : "DO NOT descend"}, boundary ${cf8.reachedBoundary ? "reached" : "NOT reached"}`,
  });

  // --- Summary. ---
  console.log("\n=== PR 3 acquisition-spike gates ===");
  let failures = 0;
  for (const gate of gates) {
    if (!gate.pass) failures += 1;
    console.log(`${gate.pass ? "PASS" : "FAIL"}  ${gate.name} — ${gate.detail}`);
  }
  console.log(`\nartifacts written to backend/${RHODE_ISLAND_FINANCE_PROBE_CACHE_DIR}`);
  if (failures > 0) {
    process.exitCode = 1;
    console.log(`${failures} gate(s) failed`);
  } else {
    console.log("all gates passed");
  }
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    console.error("Rhode Island candidate finance probe failed:", error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  });
}
