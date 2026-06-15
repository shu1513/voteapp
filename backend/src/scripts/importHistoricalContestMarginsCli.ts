import { readFile } from "node:fs/promises";
import { resolve } from "node:path";

export type HistoricalContestMarginImportArgs = {
  inputKind: "file" | "url" | "preset";
  input: string;
  preset: HistoricalContestMarginImportPresetName | null;
  source: string;
  sourceUrl: string | null;
  dryRun: boolean;
  staleAfterRedistricting: boolean;
};

export type HistoricalContestMarginImportInput = {
  csv: string;
  inputLabel: string;
  sourceUrl: string | null;
};

export const HISTORICAL_CONTEST_MARGIN_IMPORT_PRESETS = {
  "medsl-2024-president-state": {
    url: "https://raw.githubusercontent.com/MEDSL/2024-elections-official/main/2024-president-state.csv",
    source: "MIT_2024",
  },
  "medsl-2024-senate-state": {
    url: "https://raw.githubusercontent.com/MEDSL/2024-elections-official/main/2024-senate-state.csv",
    source: "MIT_2024",
  },
} as const;

export type HistoricalContestMarginImportPresetName = keyof typeof HISTORICAL_CONTEST_MARGIN_IMPORT_PRESETS;

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
  if (!(preset in HISTORICAL_CONTEST_MARGIN_IMPORT_PRESETS)) {
    throw new Error(
      `Unknown historical contest import preset: ${preset}. ` +
        `Known presets: ${Object.keys(HISTORICAL_CONTEST_MARGIN_IMPORT_PRESETS).join(", ")}`
    );
  }
  return preset as HistoricalContestMarginImportPresetName;
}

export function parseHistoricalContestMarginImportArgs(args: readonly string[]): HistoricalContestMarginImportArgs {
  const file = readValueFlag(args, "--file")?.trim();
  const url = readValueFlag(args, "--url")?.trim();
  const preset = parsePreset(readValueFlag(args, "--preset"));
  const inputCount = [file, url, preset].filter(Boolean).length;
  if (inputCount !== 1) {
    throw new Error("Provide exactly one input flag: --file=..., --url=..., or --preset=...");
  }

  const presetConfig = preset ? HISTORICAL_CONTEST_MARGIN_IMPORT_PRESETS[preset] : null;
  const sourceUrl = readValueFlag(args, "--source-url")?.trim() || null;
  const normalizedUrl = url ? parseHttpUrl(url, "--url") : null;
  const presetUrl = presetConfig ? parseHttpUrl(presetConfig.url, "--preset") : null;
  const source = readValueFlag(args, "--source")?.trim() || presetConfig?.source;
  if (!source) {
    throw new Error("Missing required flag: --source=...");
  }

  return {
    inputKind: preset ? "preset" : normalizedUrl ? "url" : "file",
    input: presetUrl ?? normalizedUrl ?? resolve(file as string),
    preset,
    source,
    sourceUrl: sourceUrl ? parseHttpUrl(sourceUrl, "--source-url") : presetUrl ?? normalizedUrl,
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
