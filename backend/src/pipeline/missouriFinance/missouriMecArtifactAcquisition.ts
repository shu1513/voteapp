import {
  buildMissouriMecUrl,
  createMissouriMecSession,
  MISSOURI_MEC_PAGES,
  MISSOURI_MEC_RESULTS_FIELD_PREFIX,
  MISSOURI_MEC_SEARCH_FIELD_PREFIX,
  parseMissouriMecHiddenFields,
  type MissouriMecResponse,
  type MissouriMecSession,
} from "./missouriMecClient.js";
import { storeMissouriMecArtifact } from "./missouriMecArtifactCache.js";
import {
  parseMissouriMecCommitteeInfo,
  parseMissouriMecContributionExport,
  parseMissouriMecExpenditureExport,
  parseMissouriMecReportInventory,
  parseMissouriMecReportYears,
} from "./missouriMecParsers.js";

const SEARCH = MISSOURI_MEC_SEARCH_FIELD_PREFIX;
const RESULTS = MISSOURI_MEC_RESULTS_FIELD_PREFIX;

function hidden(html: string, url: string): Record<string, string> {
  const fields = parseMissouriMecHiddenFields(html);
  if (!fields.__VIEWSTATE) throw new Error(`Missouri MEC page has no __VIEWSTATE: ${url}`);
  return fields;
}

function requireHtml(response: MissouriMecResponse, label: string): string {
  if (response.status !== 200 || !(response.contentType ?? "text/html").includes("text/html")) {
    throw new Error(`Unexpected Missouri MEC ${label} response: HTTP ${response.status}, ${response.contentType}`);
  }
  return response.text();
}

async function fetchTransactionExport(input: {
  session: MissouriMecSession;
  mecid: string;
  year: number;
  mode: "contributions" | "expenditures";
}): Promise<{ body: string; sourceUrl: string }> {
  const searchUrl = buildMissouriMecUrl(MISSOURI_MEC_PAGES.contributionSearch);
  const resultsUrl = buildMissouriMecUrl(MISSOURI_MEC_PAGES.contributionResults);
  let page = requireHtml(await input.session.get(searchUrl), "transaction-search page");
  const modeControl = input.mode === "contributions" ? "lbtnContr" : "lbtnExpend";
  page = requireHtml(await input.session.postForm(searchUrl, {
    ...hidden(page, searchUrl),
    __EVENTTARGET: `${SEARCH}${modeControl}`,
    __EVENTARGUMENT: "",
  }, { referer: searchUrl }), `${input.mode} mode`);
  page = requireHtml(await input.session.postForm(searchUrl, {
    ...hidden(page, searchUrl),
    __EVENTTARGET: `${SEARCH}lbtnAdvanced`,
    __EVENTARGUMENT: "",
  }, { referer: searchUrl }), `${input.mode} advanced search`);
  const search = await input.session.postForm(searchUrl, {
    ...hidden(page, searchUrl),
    [`${SEARCH}ddYear`]: String(input.year),
    [`${SEARCH}txtCommID`]: input.mecid,
    [`${SEARCH}btnSearch`]: "Search",
  }, { referer: searchUrl });
  if (!search.text().includes("CF12_ContrExpendResults.aspx")) {
    throw new Error(`Missouri MEC ${input.mode} search did not open results`);
  }
  const results = requireHtml(await input.session.get(resultsUrl, { referer: searchUrl }), `${input.mode} results`);
  const exported = await input.session.postForm(resultsUrl, {
    ...hidden(results, resultsUrl),
    [`${RESULTS}btnExport`]: "Export Results to Excel",
  }, { referer: resultsUrl });
  if (!(exported.contentType ?? "").includes("application/vnd.ms-excel")) {
    throw new Error(`Unexpected Missouri MEC ${input.mode} export content type: ${exported.contentType}`);
  }
  const body = exported.text();
  if (input.mode === "contributions") parseMissouriMecContributionExport(body);
  else parseMissouriMecExpenditureExport(body);
  // Results are session-bound; the stable public search page is the useful
  // citation target after this acquisition session expires.
  return { body, sourceUrl: searchUrl };
}

export async function acquireMissouriMecCandidateFinanceArtifacts(input: {
  mecid: string;
  year: number;
  cacheDir?: string;
  session?: MissouriMecSession;
  now?: Date;
}): Promise<{ mecid: string; year: number; reportCount: number; contributionCount: number; expenditureCount: number }> {
  const mecid = input.mecid.trim().toUpperCase();
  if (!/^[A-Z]\d{6}$/.test(mecid)) throw new Error(`Invalid Missouri MECID: ${input.mecid}`);
  if (!Number.isSafeInteger(input.year) || input.year < 2002 || input.year > 2100) throw new Error(`Invalid Missouri MEC year: ${input.year}`);
  const session = input.session ?? createMissouriMecSession();
  const retrievedAt = input.now ?? new Date();
  if (Number.isNaN(retrievedAt.getTime())) throw new Error("Invalid Missouri MEC acquisition timestamp");
  const committeeUrl = buildMissouriMecUrl(MISSOURI_MEC_PAGES.committeeInfo, { MECID: mecid });
  const committeeHtml = requireHtml(await session.get(committeeUrl), "committee-info page");
  const committeeInfo = parseMissouriMecCommitteeInfo(committeeHtml);
  if (committeeInfo.mecid !== mecid) throw new Error(`Missouri MEC committee profile mismatch: expected ${mecid}, got ${committeeInfo.mecid}`);

  const reportsTab = requireHtml(await session.postForm(committeeUrl, {
    ...hidden(committeeHtml, committeeUrl),
    __EVENTTARGET: `${SEARCH}lbtnReports`,
    __EVENTARGUMENT: "",
  }, { referer: committeeUrl }), "reports tab");
  const reportYear = parseMissouriMecReportYears(reportsTab).find((row) => row.year === input.year);
  if (!reportYear) throw new Error(`Missouri MEC committee ${mecid} has no report inventory for ${input.year}`);
  const reportHtml = requireHtml(await session.postForm(committeeUrl, {
    ...hidden(reportsTab, committeeUrl),
    [`${SEARCH}${reportYear.expandControlName}.x`]: "1",
    [`${SEARCH}${reportYear.expandControlName}.y`]: "1",
  }, { referer: committeeUrl }), "expanded report inventory");
  const inventory = parseMissouriMecReportInventory(reportHtml);
  const contributions = await fetchTransactionExport({ session, mecid, year: input.year, mode: "contributions" });
  const expenditures = await fetchTransactionExport({ session, mecid, year: input.year, mode: "expenditures" });

  const artifacts = [
    { type: "committee_info" as const, body: committeeHtml, sourceUrl: committeeUrl },
    { type: "report_inventory" as const, body: reportHtml, sourceUrl: committeeUrl },
    { type: "contributions" as const, ...contributions },
    { type: "expenditures" as const, ...expenditures },
  ];
  await Promise.all(artifacts.map((artifact) => storeMissouriMecArtifact({
    cacheDir: input.cacheDir,
    key: { type: artifact.type, mecid, year: input.year },
    sourceUrl: artifact.sourceUrl,
    body: artifact.body,
    retrievedAt,
  })));
  return {
    mecid,
    year: input.year,
    reportCount: inventory.length,
    contributionCount: parseMissouriMecContributionExport(contributions.body).length,
    expenditureCount: parseMissouriMecExpenditureExport(expenditures.body).length,
  };
}
