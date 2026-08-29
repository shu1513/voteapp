// Montana CERS artifact acquisition (docs/plans/montana-finance.md, Phase 2a).
//
// Harvests one candidate's cycle into the artifact cache: report inventory,
// per-canonical-report detail lists, and the CONTR/EXPEND CSV exports. Flow
// pinned by the Phase 1 probe and re-verified live 2026-08-28:
// - FRESH session per flow (report flow, CONTR export, EXPEND export) — CERS
//   keeps search state server-side and a stale session silently serves the
//   previous entity's data. The cache layer's identity checks are the second
//   line of defense.
// - The financial-search POST works with an EMPTY lastName (verified live):
//   prepareDownloadFile's candidateId selects the entity, so acquisition
//   needs no name. The `(searchResults)` title assertion still guards the
//   silent validation bounce.
// - All artifacts of one run share one retrievedAt; the sync layer refuses
//   mixed-vintage bundles.

import {
  assertMontanaCersPageTitle,
  buildMontanaCersDataTablesQuery,
  buildMontanaCersUrl,
  createMontanaCersSession,
  type MontanaCersSession,
  type MontanaCersSessionOptions,
} from "./montanaCersClient.js";
import { storeMontanaCersArtifact } from "./montanaCersArtifactCache.js";
import {
  MONTANA_CERS_ALL_CANDIDATE_DETAIL_LISTS,
  parseMontanaCersContributionExport,
  parseMontanaCersExpenditureExport,
  parseMontanaCersFinanceRepDetailList,
  parseMontanaCersReportInventory,
} from "./montanaCersParsers.js";
import { selectMontanaCanonicalReports } from "./montanaReportInventory.js";

export type MontanaCersAcquisitionResult = {
  candidateId: number;
  year: number;
  reportCount: number;
  canonicalReportCount: number;
  detailReportIds: number[];
  contributionRowCount: number;
  expenditureRowCount: number;
};

function requirePositiveInt(value: number, label: string): void {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new Error(`Invalid Montana CERS acquisition ${label}: ${value}`);
  }
}

async function fetchReportInventoryBody(session: MontanaCersSession, candidateId: number): Promise<string> {
  await session.get(buildMontanaCersUrl("search/candidateSearch"));
  const retrieve = await session.postForm(buildMontanaCersUrl("publicReportList/retrieveCampaignReports"), {
    candidateId: String(candidateId),
    searchType: "",
    searchPage: "public",
  });
  if (retrieve.status !== 302) {
    throw new Error(`Montana CERS retrieveCampaignReports answered HTTP ${retrieve.status}, expected 302`);
  }
  await session.get(buildMontanaCersUrl("publicReportList"));
  const response = await session.get(
    buildMontanaCersUrl("publicReportList/listFinanceReports", buildMontanaCersDataTablesQuery())
  );
  return response.text();
}

async function fetchReportDetailBody(
  session: MontanaCersSession,
  candidateId: number,
  reportId: number
): Promise<string> {
  const retrieve = await session.postForm(buildMontanaCersUrl("viewFinanceReport/retrieveReport"), {
    candidateId: String(candidateId),
    reportId: String(reportId),
    searchPage: "public",
  });
  if (retrieve.status !== 200 && retrieve.status !== 302) {
    throw new Error(`Montana CERS retrieveReport answered HTTP ${retrieve.status} for report ${reportId}`);
  }
  const lists: Record<string, unknown> = {};
  for (const listName of MONTANA_CERS_ALL_CANDIDATE_DETAIL_LISTS) {
    const response = await session.postForm(buildMontanaCersUrl("viewFinanceReport/financeRepDetailList"), {
      listName,
    });
    const body = response.text();
    // Validate before embedding; the raw parsed JSON keeps every source
    // field, not just the ones the row parser models.
    parseMontanaCersFinanceRepDetailList(body);
    lists[listName] = JSON.parse(body);
  }
  return JSON.stringify({ reportId, lists });
}

async function fetchFinancialExportBody(input: {
  candidateId: number;
  year: number;
  mode: "CONTR" | "EXPEND";
  sessionOptions?: MontanaCersSessionOptions;
}): Promise<string> {
  const session = createMontanaCersSession(input.sessionOptions);
  await session.get(buildMontanaCersUrl("search/candidateSearch"));
  const searchFields: Record<string, string> =
    input.mode === "CONTR"
      ? {
          financialSearchType: "CONTR",
          contrSearchTypeCode: "CANDIDATE",
          lastName: "",
          firstName: "",
          contrSearchFromDate: "",
          contrSearchToDate: "",
          electionYear: String(input.year),
        }
      : {
          financialSearchType: "EXPEND",
          expendSearchTypeCode: "CANDIDATE",
          lastName: "",
          firstName: "",
          expendSearchFromDate: "",
          expendSearchToDate: "",
          electionYear: String(input.year),
        };
  const search = await session.postForm(buildMontanaCersUrl("searchResults/searchFinancials"), searchFields);
  assertMontanaCersPageTitle(search.text(), "searchResults", `${input.mode} search`);
  const prepare = await session.postForm(buildMontanaCersUrl("searchResults/prepareDownloadFile"), {
    candidateId: String(input.candidateId),
    committeeId: "0",
  });
  let fileName: unknown;
  try {
    fileName = (JSON.parse(prepare.text()) as { fileName?: unknown }).fileName;
  } catch {
    throw new Error(`Montana CERS prepareDownloadFile did not answer JSON for ${input.mode}`);
  }
  if (typeof fileName !== "string" || fileName === "") {
    throw new Error(`Montana CERS prepareDownloadFile returned no fileName for ${input.mode}`);
  }
  const download = await session.get(buildMontanaCersUrl("searchResults/downloadFile", { fileName }));
  return download.text();
}

/**
 * Acquires one candidate-cycle's artifacts. When the candidate has no
 * canonical C5 reports (sub-$500 filers, C7-only entities), only the
 * inventory artifact is stored and the exports are skipped — the sync layer
 * reads the inventory alone and reports "no_filed_reports" without touching
 * the bundle.
 */
export async function acquireMontanaCersCandidateFinanceArtifacts(input: {
  candidateId: number;
  year: number;
  cacheDir?: string;
  now?: Date;
  sessionOptions?: MontanaCersSessionOptions;
}): Promise<MontanaCersAcquisitionResult> {
  requirePositiveInt(input.candidateId, "candidateId");
  if (!Number.isSafeInteger(input.year) || input.year < 2020 || input.year > 2100) {
    throw new Error(`Invalid Montana CERS acquisition year: ${input.year}`);
  }
  const retrievedAt = input.now ?? new Date();
  if (Number.isNaN(retrievedAt.getTime())) {
    throw new Error("Invalid Montana CERS acquisition timestamp");
  }

  const reportSession = createMontanaCersSession(input.sessionOptions);
  const inventoryBody = await fetchReportInventoryBody(reportSession, input.candidateId);
  const inventory = parseMontanaCersReportInventory(inventoryBody);
  const selection = selectMontanaCanonicalReports(inventory);
  await storeMontanaCersArtifact({
    cacheDir: input.cacheDir,
    key: { type: "report_inventory", candidateId: input.candidateId, year: input.year },
    sourceUrl: buildMontanaCersUrl("publicReportList"),
    body: inventoryBody,
    retrievedAt,
  });
  if (selection.reports.length === 0) {
    return {
      candidateId: input.candidateId,
      year: input.year,
      reportCount: inventory.length,
      canonicalReportCount: 0,
      detailReportIds: [],
      contributionRowCount: 0,
      expenditureRowCount: 0,
    };
  }

  for (const report of selection.reports) {
    const detailBody = await fetchReportDetailBody(reportSession, input.candidateId, report.reportId);
    await storeMontanaCersArtifact({
      cacheDir: input.cacheDir,
      key: { type: "report_detail", candidateId: input.candidateId, year: input.year, reportId: report.reportId },
      sourceUrl: buildMontanaCersUrl("viewFinanceReport/financeRepDetailList"),
      body: detailBody,
      retrievedAt,
    });
  }

  const contributionBody = await fetchFinancialExportBody({
    candidateId: input.candidateId,
    year: input.year,
    mode: "CONTR",
    sessionOptions: input.sessionOptions,
  });
  await storeMontanaCersArtifact({
    cacheDir: input.cacheDir,
    key: { type: "contributions_export", candidateId: input.candidateId, year: input.year },
    sourceUrl: buildMontanaCersUrl("search/candidateSearch"),
    body: contributionBody,
    retrievedAt,
  });
  const expenditureBody = await fetchFinancialExportBody({
    candidateId: input.candidateId,
    year: input.year,
    mode: "EXPEND",
    sessionOptions: input.sessionOptions,
  });
  await storeMontanaCersArtifact({
    cacheDir: input.cacheDir,
    key: { type: "expenditures_export", candidateId: input.candidateId, year: input.year },
    sourceUrl: buildMontanaCersUrl("search/candidateSearch"),
    body: expenditureBody,
    retrievedAt,
  });

  return {
    candidateId: input.candidateId,
    year: input.year,
    reportCount: inventory.length,
    canonicalReportCount: selection.reports.length,
    detailReportIds: selection.reports.map((report) => report.reportId),
    contributionRowCount: parseMontanaCersContributionExport(contributionBody).length,
    expenditureRowCount: parseMontanaCersExpenditureExport(expenditureBody).length,
  };
}
