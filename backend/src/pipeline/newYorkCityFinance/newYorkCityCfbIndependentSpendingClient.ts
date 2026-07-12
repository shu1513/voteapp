import { strFromU8, unzipSync } from "fflate";

import { parseCsvObjects, type CsvObject } from "../../utils/csvObjects.js";

export const NEW_YORK_CITY_CFB_INDEPENDENT_SPENDING_URL =
  "https://www.nyccfb.info/FTMSearch/IndependentSpenders/Expenditures";
export const NEW_YORK_CITY_CFB_INDEPENDENT_SPENDING_EXPORT_URL =
  "https://www.nyccfb.info/FTMSearch/IndependentSpenders/ExportIndependentSpendersExpendituresToExcelAjaxHandler";
export const NEW_YORK_CITY_CFB_INDEPENDENT_SPENDER_CONTRIBUTIONS_URL =
  "https://www.nyccfb.info/FTMSearch/IndependentSpenders/Contributions";
export const NEW_YORK_CITY_CFB_INDEPENDENT_SPENDER_CONTRIBUTIONS_EXPORT_URL =
  "https://www.nyccfb.info/FTMSearch/IndependentSpenders/ExportIndependentSpendersContributionsToExcelAjaxHandler";
export const NEW_YORK_CITY_CFB_ELECTION_CYCLES_URL =
  "https://www.nyccfb.info/FTMSearchWebAPI/api/Common/GetElectionCycle";
export const NEW_YORK_CITY_CFB_CANDIDATES_URL =
  "https://www.nyccfb.info/FTMSearchWebAPI/api/AutoComplete/GetCandidates";

const REQUIRED_HEADERS = [
  "ELECTION",
  "SPENDERID",
  "SPENDER_NAME",
  "COMMUNICATION_ID",
  "CANDID",
  "CANDNAME",
  "ALLOCATION",
  "POSITION",
] as const;
const DEFAULT_TIMEOUT_MS = 60_000;
const MAX_EXPORT_BYTES = 100 * 1024 * 1024;
const FUNDER_REQUIRED_HEADERS = ["ELECTION", "RECIPID", "SCHEDULE", "REFNO", "NAME", "C_CODE", "AMNT"] as const;
const KNOWN_FUNDER_TYPES = new Set(["IND", "LLC", "CORP", "OTHR", "EMPO", "PART"]);

export type NewYorkCityCfbIndependentSpendingRow = {
  electionYear: number;
  electionCycle: string;
  spenderId: string;
  spenderName: string;
  communicationId: string;
  candidateId: string;
  candidateName: string;
  allocation: number;
  supportOppose: "support" | "oppose";
};

export type NewYorkCityCfbIndependentSpendingExport = {
  rows: NewYorkCityCfbIndependentSpendingRow[];
  rawRowCount: number;
  malformedRowCount: number;
  ignoredPositionRowCount: number;
};

export type NewYorkCityCfbIndependentSpenderFunderRow = {
  electionYear: number;
  electionCycle: string;
  spenderId: string;
  transactionId: string;
  funderName: string;
  funderType: string;
  amount: number;
};

export type NewYorkCityCfbIndependentSpenderFunderExport = {
  rows: NewYorkCityCfbIndependentSpenderFunderRow[];
  rawRowCount: number;
  ignoredRowCount: number;
};

export type NewYorkCityCfbCandidateCycleResolution = {
  resolved: Map<string, string>;
  ambiguousCandidateIds: Set<string>;
  missingCandidateIds: Set<string>;
};

function normalizeElectionYear(value: number): number {
  if (!Number.isInteger(value) || value < 2001 || value > 2100) {
    throw new Error(`Invalid NYC CFB independent-spending election year: ${value}`);
  }
  return value;
}

function normalizeElectionCycle(value: string | undefined, electionYear: number): string {
  const cycle = value?.trim() || String(electionYear);
  if (!new RegExp(`^${electionYear}[A-Z]?$`).test(cycle)) {
    throw new Error(`Invalid NYC CFB election cycle for ${electionYear}: ${value}`);
  }
  return cycle;
}

function required(row: CsvObject, field: string): string {
  const value = row[field]?.trim() ?? "";
  if (!value) throw new Error(`NYC CFB independent-spending row missing ${field}`);
  return value;
}

function parseRow(
  row: CsvObject,
  electionYear: number,
  electionCycle: string
): NewYorkCityCfbIndependentSpendingRow | null {
  const rowCycle = required(row, "ELECTION").toUpperCase();
  if (rowCycle !== electionCycle) {
    throw new Error(`NYC CFB independent-spending export contained unexpected election cycle: ${row.ELECTION}`);
  }
  const position = required(row, "POSITION").toLowerCase();
  if (position === "not determined") return null;
  if (position !== "support" && position !== "oppose") {
    throw new Error(`NYC CFB independent-spending export contained unknown position: ${row.POSITION}`);
  }
  const allocation = Number(required(row, "ALLOCATION"));
  if (!Number.isFinite(allocation) || allocation < 0) {
    throw new Error(`NYC CFB independent-spending export contained invalid allocation: ${row.ALLOCATION}`);
  }
  return {
    electionYear,
    electionCycle,
    spenderId: required(row, "SPENDERID"),
    spenderName: required(row, "SPENDER_NAME"),
    communicationId: required(row, "COMMUNICATION_ID"),
    candidateId: required(row, "CANDID"),
    candidateName: required(row, "CANDNAME"),
    allocation,
    supportOppose: position,
  };
}

export function parseNewYorkCityCfbIndependentSpendingCsv(input: {
  csv: string;
  electionYear: number;
  electionCycle?: string;
}): NewYorkCityCfbIndependentSpendingExport {
  const electionYear = normalizeElectionYear(input.electionYear);
  const electionCycle = normalizeElectionCycle(input.electionCycle, electionYear);
  const parsed = parseCsvObjects({ text: input.csv, requiredHeaders: REQUIRED_HEADERS });
  if (parsed.malformedRowCount > 0) {
    throw new Error(`NYC CFB independent-spending export contained ${parsed.malformedRowCount} malformed CSV rows`);
  }
  const rows: NewYorkCityCfbIndependentSpendingRow[] = [];
  const seen = new Map<string, NewYorkCityCfbIndependentSpendingRow>();
  let ignoredPositionRowCount = 0;
  for (const raw of parsed.rows) {
    const row = parseRow(raw, electionYear, electionCycle);
    if (!row) {
      ignoredPositionRowCount += 1;
      continue;
    }
    const key = `${row.spenderId}\u0000${row.communicationId}\u0000${row.candidateId}`;
    const previous = seen.get(key);
    if (previous) {
      if (
        previous.spenderName !== row.spenderName ||
        previous.candidateName !== row.candidateName ||
        previous.supportOppose !== row.supportOppose ||
        Math.round(previous.allocation * 10_000) !== Math.round(row.allocation * 10_000)
      ) {
        throw new Error(`NYC CFB independent-spending export contained conflicting duplicate communication allocation: ${key}`);
      }
      continue;
    }
    seen.set(key, row);
    rows.push(row);
  }
  return {
    rows,
    rawRowCount: parsed.rows.length + parsed.malformedRowCount,
    malformedRowCount: 0,
    ignoredPositionRowCount,
  };
}

async function fetchWithTimeout(
  url: string,
  init: RequestInit,
  fetchImpl: typeof fetch,
  timeoutMs: number
): Promise<Response> {
  const signal = AbortSignal.timeout(timeoutMs);
  try {
    return await fetchImpl(url, { ...init, signal });
  } catch (error) {
    if (error instanceof Error && (error.name === "AbortError" || error.name === "TimeoutError")) {
      throw new Error(`NYC CFB independent-spending request timed out after ${timeoutMs}ms for ${url}`);
    }
    throw error;
  }
}

export async function resolveNewYorkCityCfbCandidateElectionCycles(input: {
  electionYear: number;
  candidateIds: ReadonlySet<string>;
  fetchImpl?: typeof fetch;
  timeoutMs?: number;
  electionCyclesUrl?: string;
  candidatesUrl?: string;
}): Promise<NewYorkCityCfbCandidateCycleResolution> {
  const electionYear = normalizeElectionYear(input.electionYear);
  const fetchImpl = input.fetchImpl ?? fetch;
  const timeoutMs = input.timeoutMs ?? DEFAULT_TIMEOUT_MS;
  const electionCyclesUrl = input.electionCyclesUrl ?? NEW_YORK_CITY_CFB_ELECTION_CYCLES_URL;
  const candidatesUrl = input.candidatesUrl ?? NEW_YORK_CITY_CFB_CANDIDATES_URL;
  if (new URL(electionCyclesUrl).protocol !== "https:" || new URL(candidatesUrl).protocol !== "https:") {
    throw new Error("NYC CFB cycle resolver URLs must use https");
  }
  if (input.candidateIds.size === 0) {
    return { resolved: new Map(), ambiguousCandidateIds: new Set(), missingCandidateIds: new Set() };
  }
  const cycleResponse = await fetchWithTimeout(electionCyclesUrl, { headers: { accept: "application/json" } }, fetchImpl, timeoutMs);
  if (!cycleResponse.ok) {
    throw new Error(`Failed to load NYC CFB election cycles: ${cycleResponse.status} ${cycleResponse.statusText}`);
  }
  const cyclePayload = await cycleResponse.json() as Array<{
    options?: Array<{ id?: unknown; label?: unknown }>;
  }>;
  const cycleIds = [...new Set(
    cyclePayload.flatMap((group) => group.options ?? []).flatMap((option) => {
      const id = typeof option.id === "string" ? option.id.trim().toUpperCase() : "";
      const label = typeof option.label === "string" ? option.label : "";
      if (!new RegExp(`^${electionYear}[A-Z]?$`).test(id) || /transition|inauguration/i.test(label)) return [];
      return [id];
    })
  )];
  if (cycleIds.length === 0 || cycleIds.length > 10) {
    throw new Error(`NYC CFB returned an invalid number of election cycles for ${electionYear}: ${cycleIds.length}`);
  }
  const cyclesByCandidate = new Map<string, string[]>();
  await Promise.all(cycleIds.map(async (electionCycle) => {
    const url = new URL(candidatesUrl);
    url.searchParams.set("ec", electionCycle);
    const response = await fetchWithTimeout(url.toString(), { headers: { accept: "application/json" } }, fetchImpl, timeoutMs);
    if (!response.ok) {
      throw new Error(`Failed to load NYC CFB candidates for ${electionCycle}: ${response.status} ${response.statusText}`);
    }
    const candidates = await response.json() as Array<{ id?: unknown }>;
    for (const candidate of candidates) {
      const candidateId = typeof candidate.id === "string" ? candidate.id.trim() : "";
      if (!input.candidateIds.has(candidateId)) continue;
      const cycles = cyclesByCandidate.get(candidateId) ?? [];
      cycles.push(electionCycle);
      cyclesByCandidate.set(candidateId, cycles);
    }
  }));
  const resolved = new Map<string, string>();
  const ambiguousCandidateIds = new Set<string>();
  const missingCandidateIds = new Set<string>();
  for (const candidateId of input.candidateIds) {
    const cycles = [...new Set(cyclesByCandidate.get(candidateId) ?? [])];
    if (cycles.length === 1) resolved.set(candidateId, cycles[0]!);
    else if (cycles.length > 1) ambiguousCandidateIds.add(candidateId);
    else missingCandidateIds.add(candidateId);
  }
  return { resolved, ambiguousCandidateIds, missingCandidateIds };
}

function buildSpendingDownloadUrl(exportPath: string, exportUrl: string): string {
  if (!/^\.\.[\\/]Temp[\\/]IndependentSpendersExpenditures[\\/]CFB-IE_[A-Za-z0-9_.-]+\.zip$/.test(exportPath)) {
    throw new Error(`NYC CFB independent-spending export returned an invalid path: ${exportPath}`);
  }
  return new URL(exportPath.replaceAll("\\", "/"), exportUrl).toString();
}

export async function fetchNewYorkCityCfbIndependentSpending(input: {
  electionYear: number;
  electionCycle?: string;
  fetchImpl?: typeof fetch;
  timeoutMs?: number;
  exportUrl?: string;
}): Promise<NewYorkCityCfbIndependentSpendingExport> {
  const electionYear = normalizeElectionYear(input.electionYear);
  const electionCycle = normalizeElectionCycle(input.electionCycle, electionYear);
  const fetchImpl = input.fetchImpl ?? fetch;
  const timeoutMs = input.timeoutMs ?? DEFAULT_TIMEOUT_MS;
  const exportUrl = input.exportUrl ?? NEW_YORK_CITY_CFB_INDEPENDENT_SPENDING_EXPORT_URL;
  const parsedExportUrl = new URL(exportUrl);
  if (parsedExportUrl.protocol !== "https:") {
    throw new Error("NYC CFB independent-spending export URL must use https");
  }
  const response = await fetchWithTimeout(
    exportUrl,
    {
      method: "POST",
      headers: { accept: "text/plain,*/*;q=0.1", "content-type": "application/x-www-form-urlencoded" },
      body: new URLSearchParams({ election_cycle: electionCycle, action_type: "search" }),
    },
    fetchImpl,
    timeoutMs
  );
  if (!response.ok) {
    throw new Error(`Failed to create NYC CFB independent-spending export: ${response.status} ${response.statusText}`);
  }
  const exportPath = (await response.text()).trim();
  if (exportPath === "NoData") {
    return { rows: [], rawRowCount: 0, malformedRowCount: 0, ignoredPositionRowCount: 0 };
  }
  const downloadUrl = buildSpendingDownloadUrl(exportPath, exportUrl);
  const download = await fetchWithTimeout(downloadUrl, { headers: { accept: "*/*" } }, fetchImpl, timeoutMs);
  if (!download.ok) {
    throw new Error(`Failed to download NYC CFB independent-spending export: ${download.status} ${download.statusText}`);
  }
  const bytes = new Uint8Array(await download.arrayBuffer());
  if (bytes.byteLength === 0 || bytes.byteLength > MAX_EXPORT_BYTES) {
    throw new Error(`NYC CFB independent-spending export had invalid size: ${bytes.byteLength}`);
  }
  let files: Record<string, Uint8Array>;
  try {
    files = unzipSync(bytes, {
      filter: (file) => {
        const isCommunicationCsv = /^CFB-IE-COMM_[A-Za-z0-9_.-]+\.csv$/.test(file.name);
        if (isCommunicationCsv && file.originalSize > MAX_EXPORT_BYTES) {
          throw new Error(`NYC CFB independent-spending communication CSV was too large: ${file.originalSize}`);
        }
        return isCommunicationCsv;
      },
    });
  } catch (error) {
    throw new Error("NYC CFB independent-spending export was not a valid ZIP", { cause: error });
  }
  const communicationFiles = Object.entries(files).filter(([name]) => /^CFB-IE-COMM_[A-Za-z0-9_.-]+\.csv$/.test(name));
  if (communicationFiles.length !== 1) {
    throw new Error(`NYC CFB independent-spending export contained ${communicationFiles.length} communication CSV files`);
  }
  return parseNewYorkCityCfbIndependentSpendingCsv({
    csv: strFromU8(communicationFiles[0]![1]),
    electionYear,
    electionCycle,
  });
}

export function buildNewYorkCityCfbIndependentSpendingSourceUrl(input: {
  electionYear: number;
  electionCycle?: string;
  candidateId: string;
}): string {
  const candidateId = input.candidateId.trim();
  if (!candidateId) throw new Error("NYC CFB independent-spending candidate id is required");
  const url = new URL(NEW_YORK_CITY_CFB_INDEPENDENT_SPENDING_URL);
  const electionYear = normalizeElectionYear(input.electionYear);
  url.searchParams.set("ec", normalizeElectionCycle(input.electionCycle, electionYear));
  url.searchParams.set("cand", candidateId);
  url.searchParams.set("viewMode", "list");
  return url.toString();
}

export function parseNewYorkCityCfbIndependentSpenderFunderCsv(input: {
  csv: string;
  electionYear: number;
  electionCycle?: string;
}): NewYorkCityCfbIndependentSpenderFunderExport {
  const electionYear = normalizeElectionYear(input.electionYear);
  const electionCycle = normalizeElectionCycle(input.electionCycle, electionYear);
  const parsed = parseCsvObjects({ text: input.csv, requiredHeaders: FUNDER_REQUIRED_HEADERS });
  if (parsed.malformedRowCount > 0) {
    throw new Error(`NYC CFB independent-spender funder export contained ${parsed.malformedRowCount} malformed CSV rows`);
  }
  const rows: NewYorkCityCfbIndependentSpenderFunderRow[] = [];
  const seen = new Map<string, NewYorkCityCfbIndependentSpenderFunderRow>();
  let ignoredRowCount = 0;
  for (const raw of parsed.rows) {
    const rowCycle = required(raw, "ELECTION").toUpperCase();
    if (rowCycle !== electionCycle) {
      throw new Error(`NYC CFB independent-spender funder export contained unexpected election cycle: ${raw.ELECTION}`);
    }
    const schedule = required(raw, "SCHEDULE").toUpperCase();
    if (schedule === "IEXPF") {
      ignoredRowCount += 1;
      continue;
    }
    if (schedule !== "ICONT" && schedule !== "IREF") {
      throw new Error(`NYC CFB independent-spender funder export contained unknown schedule: ${schedule}`);
    }
    const amount = Number(required(raw, "AMNT"));
    if (!Number.isFinite(amount) || (schedule === "ICONT" ? amount < 0 : amount > 0)) {
      throw new Error(`NYC CFB independent-spender funder export contained invalid ${schedule} amount: ${raw.AMNT}`);
    }
    const funderType = required(raw, "C_CODE").toUpperCase();
    if (!KNOWN_FUNDER_TYPES.has(funderType)) {
      throw new Error(`NYC CFB independent-spender funder export contained unknown contributor type: ${funderType}`);
    }
    const row: NewYorkCityCfbIndependentSpenderFunderRow = {
      electionYear,
      electionCycle,
      spenderId: required(raw, "RECIPID"),
      transactionId: `${schedule}:${required(raw, "REFNO")}`,
      funderName: required(raw, "NAME"),
      funderType,
      amount,
    };
    const key = `${row.spenderId}\u0000${row.transactionId}`;
    const previous = seen.get(key);
    if (previous) {
      if (
        previous.funderName !== row.funderName ||
        previous.funderType !== row.funderType ||
        Math.round(previous.amount * 100) !== Math.round(row.amount * 100)
      ) {
        throw new Error(`NYC CFB independent-spender funder export contained conflicting duplicate transaction: ${key}`);
      }
      continue;
    }
    seen.set(key, row);
    rows.push(row);
  }
  return { rows, rawRowCount: parsed.rows.length, ignoredRowCount };
}

export async function fetchNewYorkCityCfbIndependentSpenderFunders(input: {
  electionYear: number;
  electionCycle?: string;
  fetchImpl?: typeof fetch;
  timeoutMs?: number;
  exportUrl?: string;
}): Promise<NewYorkCityCfbIndependentSpenderFunderExport> {
  const electionYear = normalizeElectionYear(input.electionYear);
  const electionCycle = normalizeElectionCycle(input.electionCycle, electionYear);
  const fetchImpl = input.fetchImpl ?? fetch;
  const timeoutMs = input.timeoutMs ?? DEFAULT_TIMEOUT_MS;
  const exportUrl = input.exportUrl ?? NEW_YORK_CITY_CFB_INDEPENDENT_SPENDER_CONTRIBUTIONS_EXPORT_URL;
  const parsedExportUrl = new URL(exportUrl);
  if (parsedExportUrl.protocol !== "https:") {
    throw new Error("NYC CFB independent-spender funder export URL must use https");
  }
  const response = await fetchWithTimeout(
    exportUrl,
    {
      method: "POST",
      headers: { accept: "text/plain,*/*;q=0.1", "content-type": "application/x-www-form-urlencoded" },
      body: new URLSearchParams({ election_cycle: electionCycle, RecipientType: "ind", action_type: "search" }),
    },
    fetchImpl,
    timeoutMs
  );
  if (!response.ok) {
    throw new Error(`Failed to create NYC CFB independent-spender funder export: ${response.status} ${response.statusText}`);
  }
  const exportPath = (await response.text()).trim();
  if (exportPath === "NoData") return { rows: [], rawRowCount: 0, ignoredRowCount: 0 };
  if (!/^\.\.[\\/]Temp[\\/]IndependentSpendersContributions[\\/]CFB_[A-Za-z0-9_.-]+\.csv$/.test(exportPath)) {
    throw new Error(`NYC CFB independent-spender funder export returned an invalid path: ${exportPath}`);
  }
  const downloadUrl = new URL(exportPath.replaceAll("\\", "/"), exportUrl).toString();
  const download = await fetchWithTimeout(downloadUrl, { headers: { accept: "*/*" } }, fetchImpl, timeoutMs);
  if (!download.ok) {
    throw new Error(`Failed to download NYC CFB independent-spender funder export: ${download.status} ${download.statusText}`);
  }
  const contentLength = Number(download.headers.get("content-length"));
  if (Number.isFinite(contentLength) && contentLength > MAX_EXPORT_BYTES) {
    throw new Error(`NYC CFB independent-spender funder export was too large: ${contentLength}`);
  }
  const csv = await download.text();
  if (Buffer.byteLength(csv, "utf8") > MAX_EXPORT_BYTES) {
    throw new Error(`NYC CFB independent-spender funder export was too large: ${Buffer.byteLength(csv, "utf8")}`);
  }
  return parseNewYorkCityCfbIndependentSpenderFunderCsv({ csv, electionYear, electionCycle });
}

export function buildNewYorkCityCfbIndependentSpenderFunderSourceUrl(input: {
  electionYear: number;
  electionCycle?: string;
  spenderId: string;
}): string {
  const spenderId = input.spenderId.trim();
  if (!spenderId) throw new Error("NYC CFB independent spender id is required");
  const url = new URL(NEW_YORK_CITY_CFB_INDEPENDENT_SPENDER_CONTRIBUTIONS_URL);
  const electionYear = normalizeElectionYear(input.electionYear);
  url.searchParams.set("ec", normalizeElectionCycle(input.electionCycle, electionYear));
  url.searchParams.set("indId", spenderId);
  url.searchParams.set("rt", "ind");
  return url.toString();
}
