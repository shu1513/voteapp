import { readFile } from "node:fs/promises";
import { resolve } from "node:path";

import {
  type HistoricalContestSourceFormat,
  listVerifiedHistoricalContestSourcePresets,
  VERIFIED_HISTORICAL_CONTEST_SOURCE_BY_PRESET,
  type HistoricalContestSourceDownloadMode,
  type VerifiedHistoricalContestSourcePreset,
} from "../pipeline/competitiveness/historicalContestSources.js";

export type HistoricalContestMarginImportArgs = {
  inputKind: "file" | "url" | "preset";
  input: string;
  preset: HistoricalContestMarginImportPresetName | null;
  source: string;
  sourceUrl: string | null;
  format: HistoricalContestSourceFormat;
  dryRun: boolean;
  staleAfterRedistricting: boolean;
};

export type HistoricalContestMarginImportInput = {
  csv: string;
  inputLabel: string;
  sourceUrl: string | null;
};

export const HISTORICAL_CONTEST_MARGIN_IMPORT_PRESETS = VERIFIED_HISTORICAL_CONTEST_SOURCE_BY_PRESET;

export type HistoricalContestMarginImportPresetName = VerifiedHistoricalContestSourcePreset;

const HISTORICAL_CONTEST_CSV_FETCH_TIMEOUT_MS = 30_000;

export type DataverseGuestbookResponseInput = {
  name?: string;
  email?: string;
  institution?: string;
  position?: string;
};

export type HistoricalContestCsvFetchOptions = {
  downloadMode?: HistoricalContestSourceDownloadMode;
  dataverseGuestbookResponse?: DataverseGuestbookResponseInput;
};

function readValueFlag(args: readonly string[], name: string): string | undefined {
  const prefix = `${name}=`;
  const arg = args.find((token) => token.startsWith(prefix));
  return arg?.slice(prefix.length);
}

function parseHttpUrl(value: string, flagName: string): string {
  let parsed: URL;
  try {
    parsed = new URL(value);
  } catch {
    throw new Error(`Invalid ${flagName} URL: ${value}`);
  }
  if (parsed.protocol !== "https:") {
    throw new Error(`Invalid ${flagName} URL protocol: ${parsed.protocol}. Only https is allowed.`);
  }
  return parsed.toString();
}

function isAbortError(error: unknown): boolean {
  return (
    (error instanceof DOMException && error.name === "AbortError") ||
    (error instanceof Error && error.name === "AbortError")
  );
}

function readGuestbookResponseValue(
  input: DataverseGuestbookResponseInput | undefined,
  key: keyof DataverseGuestbookResponseInput,
  envName: string,
  fallback: string
): string {
  const value = input?.[key]?.trim() || process.env[envName]?.trim() || fallback;
  if (!value) {
    throw new Error(`Missing Dataverse guestbook response value: ${envName}`);
  }
  return value;
}

export function buildDataverseGuestbookResponse(input?: DataverseGuestbookResponseInput): Record<string, unknown> {
  return {
    name: readGuestbookResponseValue(
      input,
      "name",
      "DATAVERSE_GUESTBOOK_NAME",
      "VoteApp Historical Contest Importer"
    ),
    email: readGuestbookResponseValue(
      input,
      "email",
      "DATAVERSE_GUESTBOOK_EMAIL",
      "data-import@example.invalid"
    ),
    institution: readGuestbookResponseValue(input, "institution", "DATAVERSE_GUESTBOOK_INSTITUTION", "VoteApp"),
    position: readGuestbookResponseValue(input, "position", "DATAVERSE_GUESTBOOK_POSITION", "Data importer"),
    answers: [],
  };
}

function withDataverseSignedDownloadParam(url: string): string {
  const parsed = new URL(url);
  parsed.searchParams.set("signed", "true");
  return parsed.toString();
}

function extractDataverseSignedUrl(payload: unknown): string {
  if (typeof payload === "string" && payload.trim().length > 0) {
    return payload.trim();
  }
  if (typeof payload !== "object" || payload === null || Array.isArray(payload)) {
    throw new Error("Dataverse guestbook response did not include a signed URL");
  }

  const record = payload as Record<string, unknown>;
  const data = record.data;
  if (typeof data === "string" && data.trim().length > 0) {
    return data.trim();
  }
  if (typeof data === "object" && data !== null && !Array.isArray(data)) {
    const dataRecord = data as Record<string, unknown>;
    const signedUrl = dataRecord.signedUrl ?? dataRecord.url;
    if (typeof signedUrl === "string" && signedUrl.trim().length > 0) {
      return signedUrl.trim();
    }
  }

  throw new Error("Dataverse guestbook response did not include a signed URL");
}

function parsePreset(value: string | undefined): HistoricalContestMarginImportPresetName | null {
  const preset = value?.trim();
  if (!preset) {
    return null;
  }
  if (!Object.prototype.hasOwnProperty.call(HISTORICAL_CONTEST_MARGIN_IMPORT_PRESETS, preset)) {
    throw new Error(
      `Unknown historical contest import preset: ${preset}. ` +
        `Known presets: ${listVerifiedHistoricalContestSourcePresets().join(", ")}`
    );
  }
  return preset as HistoricalContestMarginImportPresetName;
}

function parseFormat(value: string | undefined): HistoricalContestSourceFormat | null {
  const format = value?.trim();
  if (!format) {
    return null;
  }
  if (format !== "medsl_aggregate_csv" && format !== "medsl_precinct_csv") {
    throw new Error(`Unknown historical contest import format: ${format}`);
  }
  return format;
}

export function parseHistoricalContestMarginImportArgs(args: readonly string[]): HistoricalContestMarginImportArgs {
  const file = readValueFlag(args, "--file")?.trim();
  const url = readValueFlag(args, "--url")?.trim();
  const preset = parsePreset(readValueFlag(args, "--preset"));
  const explicitFormat = parseFormat(readValueFlag(args, "--format"));
  const inputCount = [file, url, preset].filter(Boolean).length;
  if (inputCount !== 1) {
    throw new Error("Provide exactly one input flag: --file=..., --url=..., or --preset=...");
  }

  const presetConfig = preset ? HISTORICAL_CONTEST_MARGIN_IMPORT_PRESETS[preset] : null;
  if (
    presetConfig &&
    (("sourceFiles" in presetConfig && presetConfig.sourceFiles?.length) ||
      ("sourceFileDiscovery" in presetConfig && presetConfig.sourceFileDiscovery))
  ) {
    throw new Error(
      `Historical contest import preset ${presetConfig.preset} has multiple source files; use the verified import script.`
    );
  }
  const sourceUrl = readValueFlag(args, "--source-url")?.trim() || null;
  const normalizedUrl = url ? parseHttpUrl(url, "--url") : null;
  const presetUrl = presetConfig ? parseHttpUrl(presetConfig.sourceUrl, "--preset") : null;
  const source = readValueFlag(args, "--source")?.trim() || presetConfig?.source;
  if (!source) {
    throw new Error("Missing required flag: --source=...");
  }
  if (presetConfig && explicitFormat && explicitFormat !== presetConfig.format) {
    throw new Error(
      `Preset ${presetConfig.preset} uses format ${presetConfig.format}; received --format=${explicitFormat}`
    );
  }

  return {
    inputKind: preset ? "preset" : normalizedUrl ? "url" : "file",
    input: presetUrl ?? normalizedUrl ?? resolve(file as string),
    preset,
    source,
    sourceUrl: sourceUrl ? parseHttpUrl(sourceUrl, "--source-url") : presetUrl ?? normalizedUrl,
    format: explicitFormat ?? presetConfig?.format ?? "medsl_aggregate_csv",
    dryRun: args.includes("--dry-run"),
    staleAfterRedistricting: args.includes("--stale-after-redistricting"),
  };
}

async function fetchTextWithTimeout(url: string, init: RequestInit = {}): Promise<Response> {
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), HISTORICAL_CONTEST_CSV_FETCH_TIMEOUT_MS);
  const headers = new Headers(init.headers);
  if (!headers.has("accept")) {
    headers.set("accept", "text/csv,text/plain;q=0.9,*/*;q=0.1");
  }
  try {
    return await fetch(url, {
      ...init,
      headers,
      signal: controller.signal,
    });
  } catch (error) {
    if (isAbortError(error)) {
      throw new Error(
        `Failed to fetch historical contest CSV: request timed out after ${HISTORICAL_CONTEST_CSV_FETCH_TIMEOUT_MS}ms for ${url}`
      );
    }
    throw error;
  } finally {
    clearTimeout(timeoutId);
  }
}

async function fetchDataverseSignedCsv(
  url: string,
  options: HistoricalContestCsvFetchOptions
): Promise<string> {
  const response = await fetchTextWithTimeout(withDataverseSignedDownloadParam(url), {
    method: "POST",
    headers: {
      accept: "application/json",
      "content-type": "application/json",
    },
    body: JSON.stringify(buildDataverseGuestbookResponse(options.dataverseGuestbookResponse)),
  });
  const responseText = await response.text();
  if (!response.ok) {
    throw new Error(
      `Failed to submit Dataverse guestbook response: ${response.status} ${response.statusText}: ${responseText}`
    );
  }

  let payload: unknown;
  try {
    payload = JSON.parse(responseText);
  } catch {
    throw new Error("Dataverse guestbook response was not valid JSON");
  }

  const signedUrl = extractDataverseSignedUrl(payload);
  const signedResponse = await fetchTextWithTimeout(signedUrl);
  if (!signedResponse.ok) {
    throw new Error(
      `Failed to fetch signed Dataverse historical contest CSV: ${signedResponse.status} ${signedResponse.statusText}`
    );
  }
  return await signedResponse.text();
}

export async function fetchHistoricalContestCsv(
  url: string,
  options: HistoricalContestCsvFetchOptions = {}
): Promise<string> {
  if (options.downloadMode === "dataverse_guestbook") {
    return await fetchDataverseSignedCsv(url, options);
  }

  const response = await fetchTextWithTimeout(url);
  if (!response.ok) {
    throw new Error(`Failed to fetch historical contest CSV: ${response.status} ${response.statusText}`);
  }
  return await response.text();
}

export async function loadHistoricalContestMarginImportInput(
  args: HistoricalContestMarginImportArgs
): Promise<HistoricalContestMarginImportInput> {
  if (args.inputKind === "url" || args.inputKind === "preset") {
    return {
      csv: await fetchHistoricalContestCsv(args.input),
      inputLabel: args.input,
      sourceUrl: args.sourceUrl ?? args.input,
    };
  }

  return {
    csv: await readFile(args.input, "utf8"),
    inputLabel: args.input,
    sourceUrl: args.sourceUrl,
  };
}
