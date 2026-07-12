import type { HoustonFinanceReportIndexRecord } from "./houstonFinanceTypes.js";

export const HOUSTON_ETHICS_EFILE_CONFIG_URL =
  "https://reporting.ethicsefile.com/assets/config/app-config.json";
export const HOUSTON_ETHICS_EFILE_HOST = "reporting.cityofhouston.ethicsefile.com";
const DEFAULT_TIMEOUT_MS = 30_000;

type FetchLike = typeof fetch;

type HoustonHostConfig = {
  apiBaseUrl: string;
  rptBaseUrl: string;
  clients: string;
};

type HoustonConfigDocument = {
  hostConfigs?: Record<string, unknown>;
};

type RawReport = Record<string, unknown>;

export type HoustonEthicsEfileClientOptions = {
  fetchImpl?: FetchLike;
  timeoutMs?: number;
  configUrl?: string;
};

function nonempty(value: unknown): string | null {
  return typeof value === "string" && value.trim() ? value.trim() : null;
}

function isoDate(value: unknown): string | null {
  const text = nonempty(value)?.replaceAll("/", "-") ?? null;
  return text && /^\d{4}-\d{2}-\d{2}$/.test(text) ? text : null;
}

function assertAllowedConfig(config: HoustonHostConfig): HoustonHostConfig {
  if (config.clients.trim().toLowerCase() !== "cityofhouston") {
    throw new Error("Houston eFile configuration has the wrong client");
  }
  const api = new URL(config.apiBaseUrl);
  const reports = new URL(config.rptBaseUrl);
  if (
    api.protocol !== "https:" ||
    !/^[a-z0-9]+\.lambda-url\.us-east-1\.on\.aws$/.test(api.hostname) ||
    reports.protocol !== "https:" ||
    reports.hostname !== "cityofhouston.ethicsefile.com" ||
    !reports.pathname.startsWith("/public/cf")
  ) {
    throw new Error("Houston eFile configuration contains an unexpected endpoint");
  }
  return config;
}

async function fetchJson(url: string, options: HoustonEthicsEfileClientOptions): Promise<unknown> {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), options.timeoutMs ?? DEFAULT_TIMEOUT_MS);
  try {
    const response = await (options.fetchImpl ?? fetch)(url, {
      headers: { accept: "application/json" },
      signal: controller.signal,
    });
    if (!response.ok) {
      throw new Error(`Houston eFile request failed: ${response.status} ${response.statusText}`);
    }
    return await response.json();
  } finally {
    clearTimeout(timeout);
  }
}

export async function loadHoustonEthicsEfileConfig(
  options: HoustonEthicsEfileClientOptions = {}
): Promise<HoustonHostConfig> {
  const raw = (await fetchJson(options.configUrl ?? HOUSTON_ETHICS_EFILE_CONFIG_URL, options)) as HoustonConfigDocument;
  const selected = raw.hostConfigs?.[HOUSTON_ETHICS_EFILE_HOST];
  if (!selected || typeof selected !== "object") {
    throw new Error("Houston eFile configuration is missing the production Houston host");
  }
  const record = selected as Record<string, unknown>;
  const apiBaseUrl = nonempty(record.apiBaseUrl);
  const rptBaseUrl = nonempty(record.rptBaseUrl);
  const clients = nonempty(record.clients);
  if (!apiBaseUrl || !rptBaseUrl || !clients) {
    throw new Error("Houston eFile configuration is incomplete");
  }
  return assertAllowedConfig({ apiBaseUrl, rptBaseUrl, clients });
}

function reportPdfUrl(config: HoustonHostConfig, reportId: string, receivedDate: string): string {
  const year = receivedDate.slice(0, 4);
  if (!/^\d{4}$/.test(year) || !/^\d+$/.test(reportId)) {
    throw new Error("Houston eFile report has invalid PDF identity fields");
  }
  return `${config.rptBaseUrl.replace(/\/$/, "")}/${year}/pdfs/ScrubbedReport_${reportId}.PDF`;
}

function mapReport(raw: RawReport, config: HoustonHostConfig): HoustonFinanceReportIndexRecord | null {
  const reportId = nonempty(raw.report_info_ident);
  const filerId = nonempty(raw.filer_ident);
  const filerName = nonempty(raw.filer_name);
  const filerType = nonempty(raw.filer_type_cd);
  const reportType = nonempty(raw.report_type_cd);
  const receivedDate = isoDate(raw.received_dt);
  const filedAt = nonempty(raw.filed_ts);
  if (!reportId || !filerId || !filerName || !filerType || !reportType || !receivedDate || !filedAt) {
    return null;
  }
  return {
    sourceSystem: "ethics_efile",
    reportId,
    filerId,
    filerName,
    filerType,
    reportType,
    receivedDate,
    filedAt,
    periodStart: isoDate(raw.period_start_dt),
    periodEnd: isoDate(raw.period_end_dt),
    officeDescription: nonempty(raw.seek_office_descr),
    campaignYear: null,
    pdfUrl: reportPdfUrl(config, reportId, receivedDate),
  };
}

export async function listHoustonEthicsEfileReports(
  options: HoustonEthicsEfileClientOptions = {}
): Promise<HoustonFinanceReportIndexRecord[]> {
  const config = await loadHoustonEthicsEfileConfig(options);
  const apiUrl = new URL("all_reports", config.apiBaseUrl.endsWith("/") ? config.apiBaseUrl : `${config.apiBaseUrl}/`);
  apiUrl.searchParams.set("client", config.clients);
  const raw = await fetchJson(apiUrl.toString(), options);
  if (!Array.isArray(raw)) {
    throw new Error("Houston eFile report index was not an array");
  }
  return raw.flatMap((row) => {
    const mapped = row && typeof row === "object" ? mapReport(row as RawReport, config) : null;
    return mapped ? [mapped] : [];
  });
}

export async function downloadHoustonEthicsEfileReportPdf(
  report: HoustonFinanceReportIndexRecord,
  options: HoustonEthicsEfileClientOptions = {}
): Promise<Uint8Array> {
  if (report.sourceSystem !== "ethics_efile" || !report.pdfUrl) {
    throw new Error("Houston eFile PDF download requires an eFile report URL");
  }
  const url = new URL(report.pdfUrl);
  if (url.protocol !== "https:" || url.hostname !== "cityofhouston.ethicsefile.com") {
    throw new Error("Houston eFile report PDF has an unexpected host");
  }
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), options.timeoutMs ?? DEFAULT_TIMEOUT_MS);
  try {
    const response = await (options.fetchImpl ?? fetch)(url, {
      headers: { accept: "application/pdf" },
      signal: controller.signal,
    });
    if (!response.ok) throw new Error(`Houston eFile PDF request failed: ${response.status}`);
    return new Uint8Array(await response.arrayBuffer());
  } finally {
    clearTimeout(timeout);
  }
}
