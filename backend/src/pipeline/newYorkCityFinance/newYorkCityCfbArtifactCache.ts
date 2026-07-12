import { createReadStream, createWriteStream } from "node:fs";
import { chmod, mkdir, readFile, rename, stat, unlink, writeFile } from "node:fs/promises";
import { resolve } from "node:path";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";
import type { ReadableStream as NodeReadableStream } from "node:stream/web";
import { parseCsvRecord } from "../../utils/csvObjects.js";

export const NEW_YORK_CITY_CFB_DATA_LIBRARY_BASE_URL = "https://www.nyccfb.info/datalibrary";
export const DEFAULT_NEW_YORK_CITY_CFB_CACHE_DIR = "scratch/new-york-city-campaign-finance/cfb";
export const NEW_YORK_CITY_CFB_FETCH_TIMEOUT_MS = 60_000;

export type NewYorkCityCfbArtifactKind = "contributions" | "financial_analysis";

export type NewYorkCityCfbArtifactMetadata = {
  version: 1;
  electionYear: number;
  kind: NewYorkCityCfbArtifactKind;
  url: string;
  filePath: string;
  downloadedAt: string;
  bytes: number;
  etag: string | null;
  lastModified: string | null;
};

export type NewYorkCityCfbArtifactRefreshResult =
  | { status: "downloaded" | "unchanged"; current: NewYorkCityCfbArtifactMetadata }
  | {
      status: "not_yet_published";
      electionYear: number;
      kind: NewYorkCityCfbArtifactKind;
      url: string;
      checkedAt: string;
      nextCheckAt: string;
    };

function normalizeElectionYear(value: number): number {
  if (!Number.isInteger(value) || value < 2001 || value > 2100) {
    throw new Error(`Invalid NYC CFB election year: ${value}`);
  }
  return value;
}

export function buildNewYorkCityCfbArtifactUrl(input: {
  electionYear: number;
  kind: NewYorkCityCfbArtifactKind;
  baseUrl?: string;
}): string {
  const electionYear = normalizeElectionYear(input.electionYear);
  const baseUrl = (input.baseUrl ?? NEW_YORK_CITY_CFB_DATA_LIBRARY_BASE_URL).replace(/\/$/, "");
  const parsed = new URL(baseUrl);
  if (parsed.protocol !== "https:") {
    throw new Error("NYC CFB artifact base URL must use https");
  }
  const filename =
    input.kind === "contributions"
      ? `${electionYear}_Contributions.csv`
      : `EC${electionYear}_FinancialAnalysis.csv`;
  return `${baseUrl}/${filename}`;
}

export function getNewYorkCityCfbArtifactCachePaths(input: {
  cacheDir: string;
  electionYear: number;
  kind: NewYorkCityCfbArtifactKind;
}): { filePath: string; metadataPath: string } {
  const electionYear = normalizeElectionYear(input.electionYear);
  const cacheDir = resolve(input.cacheDir);
  const stem = `${electionYear}_${input.kind}`;
  return {
    filePath: resolve(cacheDir, `${stem}.csv`),
    metadataPath: resolve(cacheDir, `${stem}.metadata.json`),
  };
}

async function readMetadata(path: string): Promise<NewYorkCityCfbArtifactMetadata | null> {
  try {
    const parsed = JSON.parse(await readFile(path, "utf8")) as Partial<NewYorkCityCfbArtifactMetadata>;
    return parsed.version === 1 && typeof parsed.url === "string" && typeof parsed.filePath === "string"
      ? (parsed as NewYorkCityCfbArtifactMetadata)
      : null;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return null;
    throw error;
  }
}

async function fileExists(path: string): Promise<boolean> {
  try {
    await stat(path);
    return true;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return false;
    throw error;
  }
}

type TimedFetch = { response: Response; signal: AbortSignal; clear: () => void };

async function fetchWithTimeout(url: string, headers: Headers, fetchImpl: typeof fetch, timeoutMs: number): Promise<TimedFetch> {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetchImpl(url, { headers, signal: controller.signal });
    return { response, signal: controller.signal, clear: () => clearTimeout(timeout) };
  } catch (error) {
    clearTimeout(timeout);
    if (error instanceof Error && error.name === "AbortError") {
      throw new Error(`NYC CFB artifact request timed out after ${timeoutMs}ms for ${url}`);
    }
    throw error;
  }
}

async function assertCsvArtifact(path: string, expectedHeader: string): Promise<void> {
  const handle = createReadStream(path, { encoding: "utf8", start: 0, end: 4095 });
  let prefix = "";
  for await (const chunk of handle) prefix += chunk;
  const normalized = prefix.replace(/^\uFEFF/, "").trimStart();
  if (/^<!doctype html|^<html/i.test(normalized)) {
    throw new Error("NYC CFB artifact response was HTML, not CSV");
  }
  const firstLine = normalized.split(/\r?\n/, 1)[0] ?? "";
  if (!parseCsvRecord(firstLine).map((cell) => cell.trim()).includes(expectedHeader)) {
    throw new Error(`NYC CFB artifact missing expected header: ${expectedHeader}`);
  }
}

export async function refreshNewYorkCityCfbArtifact(input: {
  cacheDir?: string;
  electionYear: number;
  kind: NewYorkCityCfbArtifactKind;
  url?: string;
  fetchImpl?: typeof fetch;
  timeoutMs?: number;
}): Promise<NewYorkCityCfbArtifactRefreshResult> {
  const electionYear = normalizeElectionYear(input.electionYear);
  const cacheDir = resolve(input.cacheDir ?? process.env.NEW_YORK_CITY_CFB_CACHE_DIR ?? DEFAULT_NEW_YORK_CITY_CFB_CACHE_DIR);
  const paths = getNewYorkCityCfbArtifactCachePaths({ cacheDir, electionYear, kind: input.kind });
  const url = input.url ?? buildNewYorkCityCfbArtifactUrl({ electionYear, kind: input.kind });
  const previous = await readMetadata(paths.metadataPath);
  const headers = new Headers({ accept: "text/csv,*/*;q=0.1" });
  if (previous?.url === url && (await fileExists(paths.filePath))) {
    if (previous.etag) headers.set("if-none-match", previous.etag);
    if (previous.lastModified) headers.set("if-modified-since", previous.lastModified);
  }
  const timeoutMs = input.timeoutMs ?? NEW_YORK_CITY_CFB_FETCH_TIMEOUT_MS;
  const request = await fetchWithTimeout(
    url,
    headers,
    input.fetchImpl ?? fetch,
    timeoutMs
  );
  const { response } = request;
  if (response.status === 404) {
    const checkedAt = new Date();
    request.clear();
    if (electionYear <= checkedAt.getUTCFullYear()) {
      throw new Error(`NYC CFB ${input.kind} artifact missing for published election year ${electionYear}`);
    }
    return {
      status: "not_yet_published",
      electionYear,
      kind: input.kind,
      url,
      checkedAt: checkedAt.toISOString(),
      nextCheckAt: new Date(checkedAt.getTime() + 24 * 60 * 60 * 1000).toISOString(),
    };
  }
  if (response.status === 304 && previous && (await fileExists(paths.filePath))) {
    request.clear();
    return { status: "unchanged", current: previous };
  }
  if (!response.ok || !response.body) {
    request.clear();
    throw new Error(`Failed to download NYC CFB ${input.kind}: ${response.status} ${response.statusText}`);
  }

  await mkdir(cacheDir, { recursive: true, mode: 0o700 });
  await chmod(cacheDir, 0o700);
  const temporaryPath = `${paths.filePath}.${process.pid}.${Date.now()}.tmp`;
  const metadataTemporaryPath = `${paths.metadataPath}.${process.pid}.${Date.now()}.tmp`;
  let artifactCommitted = false;
  try {
    await pipeline(
      Readable.fromWeb(response.body as NodeReadableStream<Uint8Array>),
      createWriteStream(temporaryPath, { mode: 0o600 }),
      { signal: request.signal }
    );
    await assertCsvArtifact(temporaryPath, input.kind === "contributions" ? "RECIPID" : "cand_id");
    const artifactStat = await stat(temporaryPath);
    await chmod(temporaryPath, 0o600);
    await rename(temporaryPath, paths.filePath);
    artifactCommitted = true;
    const current: NewYorkCityCfbArtifactMetadata = {
      version: 1,
      electionYear,
      kind: input.kind,
      url,
      filePath: paths.filePath,
      downloadedAt: new Date().toISOString(),
      bytes: artifactStat.size,
      etag: response.headers.get("etag"),
      lastModified: response.headers.get("last-modified"),
    };
    await writeFile(metadataTemporaryPath, `${JSON.stringify(current, null, 2)}\n`, { encoding: "utf8", mode: 0o600 });
    await rename(metadataTemporaryPath, paths.metadataPath);
    return { status: "downloaded", current };
  } catch (error) {
    await unlink(temporaryPath).catch(() => undefined);
    await unlink(metadataTemporaryPath).catch(() => undefined);
    if (artifactCommitted) await unlink(paths.metadataPath).catch(() => undefined);
    if (error instanceof Error && error.name === "AbortError") {
      throw new Error(`NYC CFB artifact request timed out after ${timeoutMs}ms for ${url}`);
    }
    throw error;
  } finally {
    request.clear();
  }
}
