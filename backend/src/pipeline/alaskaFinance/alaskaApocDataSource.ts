import { readFile } from "node:fs/promises";

import {
  ALASKA_APOC_CAMPAIGN_INCOME_URL,
  ALASKA_APOC_DEFAULT_REQUEST_SPACING_MS,
  ALASKA_APOC_DEFAULT_RETRY_COUNT,
  ALASKA_APOC_DEFAULT_RETRY_DELAY_MS,
  ALASKA_APOC_DEFAULT_TIMEOUT_MS,
  ALASKA_APOC_IE_CONTRIBUTIONS_URL,
  ALASKA_APOC_IE_EXPENDITURES_URL,
  defaultAlaskaApocReportYear,
  fetchAlaskaApocFinanceCsvBundle,
  parseAlaskaApocCampaignIncomeCsv,
  parseAlaskaApocIndependentContributionCsv,
  parseAlaskaApocIndependentExpenditureCsv,
  type AlaskaApocCsvFetchFn,
} from "./alaskaApocClient.js";
import type { AlaskaApocFinanceDataSet } from "./alaskaCandidateFinanceBatchSync.js";

export type AlaskaApocDataSourceMode = "csv" | "live";

export type AlaskaApocDataSourceConfig = {
  mode: AlaskaApocDataSourceMode;
  incomeCsvPath?: string;
  independentExpendituresCsvPath?: string;
  independentContributionsCsvPath?: string;
  incomeUrl?: string;
  independentExpendituresUrl?: string;
  independentContributionsUrl?: string;
  includeIndependentExpenditures?: boolean;
  includeIndependentContributions?: boolean;
  timeoutMs?: number;
  retryCount?: number;
  retryDelayMs?: number;
  requestSpacingMs?: number;
  reportYear?: number;
  exportTimeoutMs?: number;
};

export type AlaskaApocDataSourceMetadata = {
  mode: AlaskaApocDataSourceMode;
  income_source_url: string;
  independent_expenditure_source_url: string | null;
  independent_contribution_source_url: string | null;
  income_csv_path: string | null;
  independent_expenditures_csv_path: string | null;
  independent_contributions_csv_path: string | null;
  timeout_ms: number | null;
  retry_count: number | null;
  retry_delay_ms: number | null;
  request_spacing_ms: number | null;
  report_year: number | null;
};

export type LoadedAlaskaApocFinanceData = {
  apocData: AlaskaApocFinanceDataSet;
  metadata: AlaskaApocDataSourceMetadata;
};

export type LoadAlaskaApocFinanceDataOptions = {
  fetchFn?: AlaskaApocCsvFetchFn;
  logger?: Pick<typeof console, "warn">;
};

function normalizeOptionalPath(value: string | undefined): string | undefined {
  const trimmed = value?.trim();
  return trimmed || undefined;
}

function normalizeOptionalUrl(value: string | undefined): string | undefined {
  const trimmed = value?.trim();
  return trimmed || undefined;
}

async function readRequiredCsv(path: string | undefined, label: string): Promise<string> {
  const normalizedPath = normalizeOptionalPath(path);
  if (!normalizedPath) {
    throw new Error(`${label} is required for Alaska APOC CSV data source`);
  }
  return readFile(normalizedPath, "utf8");
}

async function readOptionalCsv(path: string | undefined): Promise<string | null> {
  const normalizedPath = normalizeOptionalPath(path);
  if (!normalizedPath) {
    return null;
  }
  return readFile(normalizedPath, "utf8");
}

function parseLoadedCsv(input: {
  incomeCsv: string;
  independentExpenditureCsv: string | null;
  independentContributionCsv: string | null;
  incomeSourceUrl: string;
  independentExpenditureSourceUrl: string | null;
  independentContributionSourceUrl: string | null;
}): AlaskaApocFinanceDataSet {
  return {
    incomeRows: parseAlaskaApocCampaignIncomeCsv(input.incomeCsv, { sourceUrl: input.incomeSourceUrl }),
    independentExpenditureRows: input.independentExpenditureCsv
      ? parseAlaskaApocIndependentExpenditureCsv(input.independentExpenditureCsv, {
          sourceUrl: input.independentExpenditureSourceUrl,
        })
      : [],
    independentContributionRows: input.independentContributionCsv
      ? parseAlaskaApocIndependentContributionCsv(input.independentContributionCsv, {
          sourceUrl: input.independentContributionSourceUrl,
        })
      : [],
    incomeSourceUrl: input.incomeSourceUrl,
    independentExpenditureSourceUrl: input.independentExpenditureSourceUrl,
    independentContributionSourceUrl: input.independentContributionSourceUrl,
  };
}

export async function loadAlaskaApocFinanceData(
  config: AlaskaApocDataSourceConfig,
  options: LoadAlaskaApocFinanceDataOptions = {}
): Promise<LoadedAlaskaApocFinanceData> {
  if (config.mode === "csv") {
    const [incomeCsv, independentExpenditureCsv, independentContributionCsv] = await Promise.all([
      readRequiredCsv(config.incomeCsvPath, "--income-csv"),
      readOptionalCsv(config.independentExpendituresCsvPath),
      readOptionalCsv(config.independentContributionsCsvPath),
    ]);
    const incomeSourceUrl = normalizeOptionalUrl(config.incomeUrl) ?? ALASKA_APOC_CAMPAIGN_INCOME_URL;
    const independentExpenditureSourceUrl =
      independentExpenditureCsv === null
        ? null
        : normalizeOptionalUrl(config.independentExpendituresUrl) ?? ALASKA_APOC_IE_EXPENDITURES_URL;
    const independentContributionSourceUrl =
      independentContributionCsv === null
        ? null
        : normalizeOptionalUrl(config.independentContributionsUrl) ?? ALASKA_APOC_IE_CONTRIBUTIONS_URL;

    return {
      apocData: parseLoadedCsv({
        incomeCsv,
        independentExpenditureCsv,
        independentContributionCsv,
        incomeSourceUrl,
        independentExpenditureSourceUrl,
        independentContributionSourceUrl,
      }),
      metadata: {
        mode: "csv",
        income_source_url: incomeSourceUrl,
        independent_expenditure_source_url: independentExpenditureSourceUrl,
        independent_contribution_source_url: independentContributionSourceUrl,
        income_csv_path: normalizeOptionalPath(config.incomeCsvPath) ?? null,
        independent_expenditures_csv_path: normalizeOptionalPath(config.independentExpendituresCsvPath) ?? null,
        independent_contributions_csv_path: normalizeOptionalPath(config.independentContributionsCsvPath) ?? null,
        timeout_ms: null,
        retry_count: null,
        retry_delay_ms: null,
        request_spacing_ms: null,
        report_year: null,
      },
    };
  }

  const reportYear = config.reportYear ?? defaultAlaskaApocReportYear();
  const bundle = await fetchAlaskaApocFinanceCsvBundle({
    incomeUrl: config.incomeUrl,
    independentExpenditureUrl: config.independentExpendituresUrl,
    independentContributionUrl: config.independentContributionsUrl,
    includeIndependentExpenditures: config.includeIndependentExpenditures,
    includeIndependentContributions: config.includeIndependentContributions,
    timeoutMs: config.timeoutMs,
    exportTimeoutMs: config.exportTimeoutMs,
    retryCount: config.retryCount,
    retryDelayMs: config.retryDelayMs,
    requestSpacingMs: config.requestSpacingMs,
    reportYear,
    fetchFn: options.fetchFn,
    logger: options.logger,
  });

  return {
    apocData: parseLoadedCsv({
      incomeCsv: bundle.incomeCsv,
      independentExpenditureCsv: bundle.independentExpenditureCsv,
      independentContributionCsv: bundle.independentContributionCsv,
      incomeSourceUrl: bundle.incomeSourceUrl,
      independentExpenditureSourceUrl: bundle.independentExpenditureSourceUrl,
      independentContributionSourceUrl: bundle.independentContributionSourceUrl,
    }),
    metadata: {
      mode: "live",
      income_source_url: bundle.incomeSourceUrl,
      independent_expenditure_source_url: bundle.independentExpenditureSourceUrl,
      independent_contribution_source_url: bundle.independentContributionSourceUrl,
      income_csv_path: null,
      independent_expenditures_csv_path: null,
      independent_contributions_csv_path: null,
      timeout_ms: config.timeoutMs ?? ALASKA_APOC_DEFAULT_TIMEOUT_MS,
      retry_count: config.retryCount ?? ALASKA_APOC_DEFAULT_RETRY_COUNT,
      retry_delay_ms: config.retryDelayMs ?? ALASKA_APOC_DEFAULT_RETRY_DELAY_MS,
      request_spacing_ms: config.requestSpacingMs ?? ALASKA_APOC_DEFAULT_REQUEST_SPACING_MS,
      report_year: reportYear,
    },
  };
}
