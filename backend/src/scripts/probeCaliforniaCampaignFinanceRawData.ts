import { resolve } from "node:path";
import { pathToFileURL } from "node:url";

import { loadProjectEnv } from "../config/env.js";
import {
  CAL_ACCESS_RAW_DATA_FETCH_TIMEOUT_MS,
  CAL_ACCESS_RAW_DATA_ZIP_URL,
  downloadCalAccessRawDataZip,
  fetchCalAccessRawDataZipMetadata,
  parseCalAccessHttpsUrl,
} from "../pipeline/californiaFinance/calAccessRawDataArtifactCache.js";
import {
  listCalAccessRawDataManifestFileNames,
  validateCalAccessRawDataManifest,
} from "../pipeline/californiaFinance/calAccessRawDataManifest.js";
import { probeCalAccessRawDataZip } from "../pipeline/californiaFinance/calAccessRawDataProbe.js";

export {
  CAL_ACCESS_RAW_DATA_FETCH_TIMEOUT_MS,
  CAL_ACCESS_RAW_DATA_ZIP_URL,
  downloadCalAccessRawDataZip,
  fetchCalAccessRawDataZipMetadata,
};

export type ProbeCaliforniaCampaignFinanceRawDataScriptOptions = {
  inputKind: "url" | "local_zip";
  url: string | null;
  localZip: string | null;
  outputPath: string | null;
  headOnly: boolean;
  sampleFileNames: string[];
  samplePatterns: string[];
  validateManifest: boolean;
  maxRowsPerFile: number;
  maxFiles: number;
  timeoutMs: number;
};

function readValueFlags(args: readonly string[], name: string): string[] {
  const values: string[] = [];
  const inlinePrefix = `${name}=`;

  for (let index = 0; index < args.length; index += 1) {
    const arg = args[index];
    if (arg?.startsWith(inlinePrefix)) {
      values.push(arg.slice(inlinePrefix.length));
      continue;
    }
    if (arg === name) {
      const next = args[index + 1];
      if (!next || next.startsWith("--")) {
        throw new Error(`Missing value for ${name}`);
      }
      values.push(next);
      index += 1;
    }
  }

  return values;
}

function readValueFlag(args: readonly string[], name: string): string | undefined {
  const values = readValueFlags(args, name);
  if (values.length > 1) {
    throw new Error(`Provide ${name} at most once`);
  }
  return values[0];
}

function parseHttpsUrl(value: string, flagName: string): string {
  return parseCalAccessHttpsUrl(value, `${flagName} URL`);
}

function parsePositiveInteger(value: string | undefined, fallback: number, flagName: string): number {
  if (!value) {
    return fallback;
  }
  if (!/^[1-9]\d*$/.test(value)) {
    throw new Error(`Invalid ${flagName} value: ${value}`);
  }
  return Number(value);
}

function parseLocalPath(value: string | undefined, flagName: string): string | null {
  const trimmed = value?.trim();
  return trimmed && trimmed.length > 0 ? resolve(trimmed) : null;
}

export function parseProbeCaliforniaCampaignFinanceRawDataScriptArgs(
  args: readonly string[]
): ProbeCaliforniaCampaignFinanceRawDataScriptOptions {
  const localZip = parseLocalPath(readValueFlag(args, "--local-zip"), "--local-zip");
  const explicitUrl = readValueFlag(args, "--url")?.trim();
  const outputPath = parseLocalPath(readValueFlag(args, "--output"), "--output");
  const headOnly = args.includes("--head-only");
  const validateManifest = args.includes("--manifest");

  if (localZip && explicitUrl) {
    throw new Error("Provide only one of --local-zip or --url");
  }
  if (localZip && headOnly) {
    throw new Error("--head-only can only be used with remote --url input");
  }
  if (!localZip && !headOnly && !outputPath) {
    throw new Error("Provide --output=... when downloading the CAL-ACCESS raw data ZIP");
  }
  if (headOnly && validateManifest) {
    throw new Error("--manifest cannot be used with --head-only because manifest validation requires ZIP samples");
  }

  const sampleFileNames = readValueFlags(args, "--sample-file")
    .map((value) => value.trim())
    .filter(Boolean);
  const samplePatterns = readValueFlags(args, "--sample-pattern")
    .map((value) => value.trim())
    .filter(Boolean);

  return {
    inputKind: localZip ? "local_zip" : "url",
    url: localZip ? null : parseHttpsUrl(explicitUrl || CAL_ACCESS_RAW_DATA_ZIP_URL, "--url"),
    localZip,
    outputPath,
    headOnly,
    sampleFileNames,
    samplePatterns,
    validateManifest,
    maxRowsPerFile: parsePositiveInteger(readValueFlag(args, "--max-rows"), 5, "--max-rows"),
    maxFiles: parsePositiveInteger(readValueFlag(args, "--max-files"), validateManifest ? 50 : 20, "--max-files"),
    timeoutMs: parsePositiveInteger(readValueFlag(args, "--timeout-ms"), CAL_ACCESS_RAW_DATA_FETCH_TIMEOUT_MS, "--timeout-ms"),
  };
}

function compileSamplePatterns(values: readonly string[]): RegExp[] {
  return values.map((value) => {
    try {
      return new RegExp(value);
    } catch {
      throw new Error(`Invalid --sample-pattern regular expression: ${value}`);
    }
  });
}

export async function runProbeCaliforniaCampaignFinanceRawDataScript(input: {
  options: ProbeCaliforniaCampaignFinanceRawDataScriptOptions;
  fetchImpl?: typeof fetch;
}) {
  const startedAt = new Date();
  const options = input.options;
  const remote =
    options.url !== null
      ? await fetchCalAccessRawDataZipMetadata(options.url, {
          fetchImpl: input.fetchImpl,
          timeoutMs: options.timeoutMs,
        })
      : null;

  if (options.headOnly) {
    return {
      type: "cal_access_raw_data_probe",
      ts: new Date().toISOString(),
      started_at: startedAt.toISOString(),
      input_kind: options.inputKind,
      head_only: true,
      remote,
      download: null,
      probe: null,
      manifest_validation: null,
    };
  }

  const download =
    options.inputKind === "url"
      ? await downloadCalAccessRawDataZip({
          url: options.url ?? undefined,
          outputPath: options.outputPath ?? "",
          fetchImpl: input.fetchImpl,
          timeoutMs: options.timeoutMs,
        })
      : null;
  const zipPath = options.localZip ?? download?.outputPath;
  if (!zipPath) {
    throw new Error("CAL-ACCESS raw data ZIP path could not be resolved");
  }
  const selectedFileNames = options.validateManifest
    ? [...new Set([...options.sampleFileNames, ...listCalAccessRawDataManifestFileNames()])]
    : options.sampleFileNames;

  const probe = await probeCalAccessRawDataZip({
    zipPath,
    selectedFileNames,
    selectedFileNamePatterns: compileSamplePatterns(options.samplePatterns),
    maxRowsPerFile: options.maxRowsPerFile,
    maxFiles: options.maxFiles,
  });
  const manifestValidation = options.validateManifest ? validateCalAccessRawDataManifest(probe) : null;

  return {
    type: "cal_access_raw_data_probe",
    ts: new Date().toISOString(),
    started_at: startedAt.toISOString(),
    input_kind: options.inputKind,
    head_only: false,
    remote,
    download,
    probe: {
      zip_path: probe.zipPath,
      entry_count: probe.entries.length,
      entries: probe.entries,
      samples: probe.samples,
      missing_file_names: probe.missingFileNames,
    },
    manifest_validation: manifestValidation,
  };
}

async function main(): Promise<void> {
  loadProjectEnv();
  const options = parseProbeCaliforniaCampaignFinanceRawDataScriptArgs(process.argv.slice(2));
  const output = await runProbeCaliforniaCampaignFinanceRawDataScript({ options });
  console.log(JSON.stringify(output, null, 2));
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    const message = error instanceof Error ? error.message : String(error);
    console.error("CAL-ACCESS raw data probe failed:", message);
    process.exitCode = 1;
  });
}
