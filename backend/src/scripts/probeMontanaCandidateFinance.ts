// Live smoke probe for the Montana finance module (docs/plans/montana-finance.md).
// NO schema, NO database writes: exercises the CERS public flow through the
// Phase 1 module code (client + parsers + canonical selection + chain
// reconciliation) against one known candidate. Every pinned constant was
// verified live 2026-08-27 (Bedey, SD-43, candidateId 21020). A FAIL means
// the portal changed — re-verify by hand before building on top of it.
//
// Gates:
//   1. Candidate search flow: fresh session, searchCandidates POST, the
//      DataTables results GET parses, and the probe candidate is found.
//   2. Report inventory: retrieveCampaignReports 302 -> publicReportList ->
//      listFinanceReports parses; canonical selection yields C5 rows and
//      excludes Incorporated C7s.
//   3. Chain reconciliation: report-detail lists for the first two
//      consecutive canonical C5s harvest cleanly (a legitimately empty list
//      is `[]`, never an empty body) and the cash-begin link passes the
//      lump gate.
//   4. Contribution export: searchFinancials CONTR (title-marker check) ->
//      prepareDownloadFile -> downloadFile with the pinned 18-column pipe
//      header; every row parses; occupation fill on individual rows is
//      printed (Phase 0 measured 100%).
//
// PII: exports carry donor street addresses — this script prints only
// aggregates and headers, never contributor rows.

import { pathToFileURL } from "node:url";

import {
  MontanaCersClientError,
  assertMontanaCersPageTitle,
  buildMontanaCersDataTablesQuery,
  buildMontanaCersUrl,
  createMontanaCersSession,
  type MontanaCersSession,
} from "../pipeline/montanaFinance/montanaCersClient.js";
import {
  MONTANA_CERS_ALL_CANDIDATE_DETAIL_LISTS,
  parseMontanaCersCandidateSearchResults,
  parseMontanaCersContributionExport,
  parseMontanaCersFinanceRepDetailList,
  parseMontanaCersReportInventory,
  type MontanaCersDetailRow,
  type MontanaCersReportDetailArtifact,
} from "../pipeline/montanaFinance/montanaCersParsers.js";
import {
  computeMontanaReportCashFlows,
  reconcileMontanaCashBeginChain,
} from "../pipeline/montanaFinance/montanaChainReconciliation.js";
import { selectMontanaCanonicalReports } from "../pipeline/montanaFinance/montanaReportInventory.js";

// Probe fixture: Bedey, SD-43 (2026). Stable across sessions (Phase 0 Q7).
const PROBE_LAST_NAME = "Bedey";
const PROBE_ELECTION_YEAR = "2026";
const PROBE_CANDIDATE_ID = 21020;

function pass(gate: string, detail: string): void {
  console.log(`PASS ${gate}: ${detail}`);
}

async function fetchDetailArtifact(
  session: MontanaCersSession,
  candidateId: number,
  reportId: number
): Promise<MontanaCersReportDetailArtifact> {
  const retrieve = await session.postForm(buildMontanaCersUrl("viewFinanceReport/retrieveReport"), {
    candidateId: String(candidateId),
    reportId: String(reportId),
    searchPage: "public",
  });
  if (retrieve.status !== 200 && retrieve.status !== 302) {
    throw new Error(`retrieveReport answered HTTP ${retrieve.status} for report ${reportId}`);
  }
  const lists = {} as Record<(typeof MONTANA_CERS_ALL_CANDIDATE_DETAIL_LISTS)[number], MontanaCersDetailRow[]>;
  for (const listName of MONTANA_CERS_ALL_CANDIDATE_DETAIL_LISTS) {
    const response = await session.postForm(buildMontanaCersUrl("viewFinanceReport/financeRepDetailList"), {
      listName,
    });
    lists[listName] = parseMontanaCersFinanceRepDetailList(response.text());
  }
  return { reportId, lists };
}

async function main(): Promise<void> {
  if (process.argv.length > 2) {
    throw new Error(`Montana finance probe takes no flags, got: ${process.argv.slice(2).join(" ")}`);
  }

  // Gate 1: candidate search (fresh session; seed then POST then list GET).
  const session = createMontanaCersSession({ log: (message) => console.error(message) });
  await session.get(buildMontanaCersUrl("search/candidateSearch"));
  await session.postForm(buildMontanaCersUrl("searchResults/searchCandidates"), {
    lastName: PROBE_LAST_NAME,
    firstName: "",
    middleInitial: "",
    candidateTypeCode: "",
    officeCode: "",
    countyCode: "",
    partyCode: "",
    electionYear: PROBE_ELECTION_YEAR,
  });
  const candidateList = await session.get(
    buildMontanaCersUrl("searchResults/listCandidateResults", buildMontanaCersDataTablesQuery())
  );
  const candidates = parseMontanaCersCandidateSearchResults(candidateList.text());
  const probeCandidate = candidates.find((row) => row.candidateId === PROBE_CANDIDATE_ID);
  if (probeCandidate === undefined) {
    throw new Error(`Probe candidate ${PROBE_CANDIDATE_ID} not in search results (${candidates.length} rows)`);
  }
  pass("candidate-search", `${candidates.length} row(s); found candidateId ${PROBE_CANDIDATE_ID}`);

  // Gate 2: report inventory + canonical selection.
  const retrieveReports = await session.postForm(buildMontanaCersUrl("publicReportList/retrieveCampaignReports"), {
    candidateId: String(PROBE_CANDIDATE_ID),
    searchType: "",
    searchPage: "public",
  });
  if (retrieveReports.status !== 302) {
    throw new Error(`retrieveCampaignReports answered HTTP ${retrieveReports.status}, expected 302`);
  }
  await session.get(buildMontanaCersUrl("publicReportList"));
  const inventoryResponse = await session.get(
    buildMontanaCersUrl("publicReportList/listFinanceReports", buildMontanaCersDataTablesQuery())
  );
  const inventory = parseMontanaCersReportInventory(inventoryResponse.text());
  const selection = selectMontanaCanonicalReports(inventory);
  if (selection.reports.length < 2) {
    throw new Error(`Expected at least 2 canonical C5 reports, got ${selection.reports.length}`);
  }
  if (selection.hasOverlappingPeriods) {
    throw new Error("Canonical C5 periods overlap");
  }
  const incorporated = selection.diagnostics.filter((diagnostic) => diagnostic.reason === "incorporated").length;
  pass(
    "report-inventory",
    `${inventory.length} rows -> ${selection.reports.length} canonical C5, ${incorporated} incorporated excluded`
  );

  // Gate 3: chain link between the first two consecutive canonical C5s.
  const [first, second] = selection.reports;
  const chain = reconcileMontanaCashBeginChain([
    { inventory: first!, flows: computeMontanaReportCashFlows(await fetchDetailArtifact(session, PROBE_CANDIDATE_ID, first!.reportId)) },
    { inventory: second!, flows: computeMontanaReportCashFlows(await fetchDetailArtifact(session, PROBE_CANDIDATE_ID, second!.reportId)) },
  ]);
  if (!chain.ok) {
    const broken = chain.links.filter((link) => !link.ok);
    throw new Error(
      `Chain link failed: ${broken.map((link) => `${link.side} ${link.reportId}->${link.nextReportId} lump ${link.lumpCents}c (${link.failure})`).join("; ")}`
    );
  }
  pass(
    "chain-reconciliation",
    chain.links
      .map((link) => `${link.side} ${link.reportId}->${link.nextReportId} lump ${(link.lumpCents / 100).toFixed(2)}`)
      .join("; ")
  );

  // Gate 4: contribution CSV export (fresh session per entity rule — the
  // financial search flow resets the server-side search state anyway, but a
  // clean session mirrors production harvest behavior).
  const exportSession = createMontanaCersSession({ log: (message) => console.error(message) });
  await exportSession.get(buildMontanaCersUrl("search/candidateSearch"));
  const financialSearch = await exportSession.postForm(buildMontanaCersUrl("searchResults/searchFinancials"), {
    financialSearchType: "CONTR",
    contrSearchTypeCode: "CANDIDATE",
    lastName: PROBE_LAST_NAME,
    firstName: "",
    contrSearchFromDate: "",
    contrSearchToDate: "",
    electionYear: PROBE_ELECTION_YEAR,
  });
  assertMontanaCersPageTitle(financialSearch.text(), "searchResults", "CONTR search");
  const prepare = await exportSession.postForm(buildMontanaCersUrl("searchResults/prepareDownloadFile"), {
    candidateId: String(PROBE_CANDIDATE_ID),
    committeeId: "0",
  });
  const fileName: unknown = (JSON.parse(prepare.text()) as { fileName?: unknown }).fileName;
  if (typeof fileName !== "string" || fileName === "") {
    throw new Error(`prepareDownloadFile returned no fileName: ${prepare.text().slice(0, 200)}`);
  }
  const download = await exportSession.get(buildMontanaCersUrl("searchResults/downloadFile", { fileName }));
  const rows = parseMontanaCersContributionExport(download.text());
  if (rows.length === 0) {
    throw new Error("Contribution export parsed to zero rows");
  }
  const individualRows = rows.filter((row) => row.lineItem === "Individual Contributions");
  const withOccupation = individualRows.filter((row) => row.occupation !== null).length;
  pass(
    "contribution-export",
    `${rows.length} rows; occupation fill ${withOccupation}/${individualRows.length} individual rows`
  );

  console.log("Montana CERS probe: ALL GATES PASSED");
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error: unknown) => {
    const message =
      error instanceof MontanaCersClientError
        ? `${error.code}: ${error.message}`
        : error instanceof Error
          ? error.message
          : String(error);
    console.error(`Montana CERS probe FAILED — ${message}`);
    process.exitCode = 1;
  });
}
