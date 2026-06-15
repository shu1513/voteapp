import { readFile } from "node:fs/promises";
import { resolve } from "node:path";

import {
  type HistoricalContestSourceFormat,
  listVerifiedHistoricalContestSourcePresets,
  VERIFIED_HISTORICAL_CONTEST_SOURCE_BY_PRESET,
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

export async function fetchHistoricalContestCsv(url: string): Promise<string> {
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), HISTORICAL_CONTEST_CSV_FETCH_TIMEOUT_MS);
  let response: Response;
  try {
    response = await fetch(url, {
      headers: {
        accept: "text/csv,text/plain;q=0.9,*/*;q=0.1",
      },
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
