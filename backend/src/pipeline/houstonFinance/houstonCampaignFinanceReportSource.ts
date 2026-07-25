import type { HoustonFinanceParsedReport, HoustonFinanceReportIndexRecord } from "./houstonFinanceTypes.js";
import {
  downloadHoustonEthicsEfileReportPdf,
  listHoustonEthicsEfileReports,
  type HoustonEthicsEfileClientOptions,
} from "./houstonEthicsEfileClient.js";
import {
  downloadHoustonLegacyReportPdf,
  searchHoustonLegacyCandidateReports,
  type HoustonLegacyClientOptions,
} from "./houstonLegacyCampaignFinanceClient.js";
import {
  cacheHoustonFinancePdf,
  DEFAULT_HOUSTON_FINANCE_PDF_CACHE_DIR,
  readCachedHoustonFinancePdf,
  validateHoustonFinancePdf,
} from "./houstonCampaignFinancePdfCache.js";
import { parseHoustonCandidateFinancePdf } from "./houstonCampaignFinancePdfParser.js";
import {
  houstonFinanceOfficeTargetsEqual,
  parseHoustonEfileOfficeTarget,
  type HoustonFinanceOfficeTarget,
} from "./houstonFinanceOfficeTargets.js";
import { normalizeTexasCandidateNameKeys } from "../texasFinance/texasCandidateCommitteeResolver.js";

const MAX_REPORTS_PER_CANDIDATE = 100;

function namesMatch(left: string, right: string): boolean {
  const rightKeys = normalizeTexasCandidateNameKeys(right);
  for (const key of normalizeTexasCandidateNameKeys(left)) if (rightKeys.has(key)) return true;
  return false;
}

async function cachedOrDownload(input: {
  cacheDir: string;
  report: HoustonFinanceReportIndexRecord;
  download: () => Promise<Uint8Array>;
}): Promise<Uint8Array> {
  const cached = await readCachedHoustonFinancePdf(input.cacheDir, input.report);
  if (cached) return cached;
  const data = await input.download();
  validateHoustonFinancePdf(data);
  await cacheHoustonFinancePdf(input.cacheDir, input.report, data);
  return data;
}

export async function loadHoustonCandidateFinanceReports(input: {
  candidateName: string;
  firstName: string;
  lastName: string;
  electionYear: number;
  officeTarget?: HoustonFinanceOfficeTarget;
  cacheDir?: string;
  efileReports?: readonly HoustonFinanceReportIndexRecord[];
  efileOptions?: HoustonEthicsEfileClientOptions;
  legacyOptions?: HoustonLegacyClientOptions;
}): Promise<HoustonFinanceParsedReport[]> {
  const cacheDir = input.cacheDir ?? (process.env.HOUSTON_CAMPAIGN_FINANCE_PDF_CACHE_DIR?.trim() || DEFAULT_HOUSTON_FINANCE_PDF_CACHE_DIR);
  const officeTarget = input.officeTarget ?? { officeName: "Mayor", seat: "Houston" };
  const allEfileReports = input.efileReports ?? await listHoustonEthicsEfileReports(input.efileOptions);
  const efileReports = allEfileReports.filter((report) =>
    report.filerType === "COH" &&
    namesMatch(input.candidateName, report.filerName) &&
    Number(report.periodEnd?.slice(0, 4) ?? report.receivedDate.slice(0, 4)) <= input.electionYear &&
    (() => {
      const indexedTarget = parseHoustonEfileOfficeTarget(report.officeDescription);
      return indexedTarget !== null && houstonFinanceOfficeTargetsEqual(indexedTarget, officeTarget);
    })()
  );
  const legacySession = await searchHoustonLegacyCandidateReports(
    { firstName: input.firstName, lastName: input.lastName },
    input.legacyOptions
  );
  const legacyReports = legacySession.reports.filter((report) =>
    ["COH", "CORCOH"].includes(report.filerType.trim().toUpperCase()) &&
    report.campaignYear === input.electionYear &&
    namesMatch(input.candidateName, report.filerName)
  );
  if (efileReports.length + legacyReports.length > MAX_REPORTS_PER_CANDIDATE) {
    throw new Error("Houston candidate report count exceeds safety limit");
  }
  const parsed: HoustonFinanceParsedReport[] = [];
  const failures: Array<{ report: HoustonFinanceReportIndexRecord; error: unknown }> = [];
  const loadReport = async (report: HoustonFinanceReportIndexRecord, download: () => Promise<Uint8Array>) => {
    try {
      const data = await cachedOrDownload({ cacheDir, report, download });
      const parsedReport = await parseHoustonCandidateFinancePdf({ data, index: report });
      if (
        parsedReport.electionDate.startsWith(`${input.electionYear}-`) &&
        houstonFinanceOfficeTargetsEqual(parsedReport.officeSought, officeTarget)
      ) parsed.push(parsedReport);
    } catch (error) {
      failures.push({ report, error });
      console.warn("Houston finance report skipped after download or parse failure:", {
        sourceSystem: report.sourceSystem,
        reportId: report.reportId,
        error: error instanceof Error ? error.message : String(error),
      });
    }
  };
  for (const report of efileReports) {
    await loadReport(report, () => downloadHoustonEthicsEfileReportPdf(report, input.efileOptions));
  }
  for (const report of legacyReports) {
    await loadReport(report, async () => {
      try {
        return await downloadHoustonLegacyReportPdf(legacySession, report, input.legacyOptions);
      } catch {
        const retrySession = await searchHoustonLegacyCandidateReports(
          { firstName: input.firstName, lastName: input.lastName },
          input.legacyOptions
        );
        const retryReport = retrySession.reports.find((candidate) => candidate.reportId === report.reportId);
        if (!retryReport) throw new Error(`Houston legacy report disappeared during retry: ${report.reportId}`);
        return downloadHoustonLegacyReportPdf(retrySession, retryReport, input.legacyOptions);
      }
    });
  }
  if (parsed.length === 0 && failures.length > 0) {
    throw new Error(`All ${failures.length} Houston candidate finance reports failed to load or parse`);
  }
  return parsed;
}
