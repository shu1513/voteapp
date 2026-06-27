import { createWriteStream } from "node:fs";
import { rm, stat } from "node:fs/promises";
import { resolve } from "node:path";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";
import type { ReadableStream as NodeReadableStream } from "node:stream/web";

export const MAINE_CFIS_PUBLIC_SITE_URL = "https://mainecampaignfinance.com/";
export const MAINE_CFIS_CSV_DOWNLOAD_API_URL =
  "https://mainecampaignfinance.com/api/DataDownload/CSVDownloadReport";
export const MAINE_CFIS_DOWNLOAD_LIST_API_URL =
  "https://mainecampaignfinance.com/api/DataDownload/GetCheckDatadownload";
export const MAINE_CFIS_FETCH_TIMEOUT_MS = 900_000;

export type MaineCfisArtifactKind = "contributions" | "expenditures";
export type MaineCfisTransactionType = "CON" | "EXP";

export type MaineCfisArtifactIdentity = {
  filingYear: number;
  artifactKind: MaineCfisArtifactKind;
};

export type MaineCfisDownloadListItem = {
  TransactionKey: string;
  ElectionYear: number;
  NameOfFile: string;
  TransactionType: MaineCfisTransactionType;
};

export type MaineCfisCsvDownloadRequestBody = {
  year: number;
  transactionType: MaineCfisTransactionType;
};

export type MaineCfisRemoteArtifactMetadata = MaineCfisArtifactIdentity & {
  url: string;
  requestBody: MaineCfisCsvDownloadRequestBody;
  contentLength: number | null;
  contentType: string | null;
  contentDisposition: string | null;
  etag: string | null;
  lastModified: string | null;
};

export type MaineCfisArtifactDownloadResult = MaineCfisRemoteArtifactMetadata & {
  outputPath: string;
  bytesWritten: number;
};

type FetchOptions = {
  fetchImpl?: typeof fetch;
  timeoutMs?: number;
};

export function normalizeMaineCfisFilingYear(filingYear: number): number {
  if (!Number.isInteger(filingYear) || filingYear < 2000 || filingYear > 2100) {
    throw new Error(`Invalid Maine CFIS filing year: ${filingYear}`);
  }
  return filingYear;
}

export function normalizeMaineCfisArtifactKind(kind: string): MaineCfisArtifactKind {
  const normalized = kind.trim().toLowerCase();
  // CFIS bundles loan receipts in the same CSV as regular contributions.
  if (normalized === "contributions" || normalized === "contribution" || normalized === "con" || normalized === "loans") {
    return "contributions";
  }
  if (normalized === "expenditures" || normalized === "expenditure" || normalized === "exp") {
    return "expenditures";
  }
  throw new Error(`Invalid Maine CFIS artifact kind: ${kind}`);
}

export function normalizeMaineCfisArtifactIdentity(input: {
  filingYear: number;
  artifactKind: string;
}): MaineCfisArtifactIdentity {
  return {
    filingYear: normalizeMaineCfisFilingYear(input.filingYear),
    artifactKind: normalizeMaineCfisArtifactKind(input.artifactKind),
  };
}

export function maineCfisTransactionType(kind: MaineCfisArtifactKind): MaineCfisTransactionType {
  switch (kind) {
    case "contributions":
      return "CON";
    case "expenditures":
      return "EXP";
  }
}

export function buildMaineCfisCsvDownloadRequestBody(input: {
  filingYear: number;
  artifactKind: MaineCfisArtifactKind;
}): MaineCfisCsvDownloadRequestBody {
  const artifact = normalizeMaineCfisArtifactIdentity(input);
  return {
    year: artifact.filingYear,
    transactionType: maineCfisTransactionType(artifact.artifactKind),
  };
}

export function parseMaineCfisHttpsUrl(value: string, fieldName = "Maine CFIS URL"): string {
  let parsed: URL;
  try {
    parsed = new URL(value);
  } catch {
    throw new Error(`Invalid ${fieldName}: ${value}`);
  }
  if (parsed.protocol !== "https:") {
    throw new Error(`Invalid ${fieldName} protocol: ${parsed.protocol}. Only https is allowed.`);
  }
  if (parsed.hostname !== "mainecampaignfinance.com") {
    throw new Error(`Invalid ${fieldName} host: ${parsed.hostname}`);
  }
  return parsed.toString();
}

function isAbortError(error: unknown): boolean {
  return (
    (error instanceof DOMException && error.name === "AbortError") ||
    (error instanceof Error && error.name === "AbortError")
  );
}

async function fetchWithTimeout(url: string, init: RequestInit, options: FetchOptions): Promise<Response> {
  const timeoutMs = options.timeoutMs ?? MAINE_CFIS_FETCH_TIMEOUT_MS;
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  const headers = new Headers(init.headers);
  if (!headers.has("accept")) {
    headers.set("accept", "application/octet-stream,text/csv;q=0.9,*/*;q=0.1");
  }
  if (!headers.has("content-type") && init.body !== undefined) {
    headers.set("content-type", "application/json;charset=UTF-8");
  }
  if (!headers.has("origin")) {
    headers.set("origin", "https://mainecampaignfinance.com");
  }
  if (!headers.has("referer")) {
    headers.set("referer", MAINE_CFIS_PUBLIC_SITE_URL);
  }
  if (!headers.has("user-agent")) {
    headers.set("user-agent", "Mozilla/5.0");
  }

  try {
    return await (options.fetchImpl ?? fetch)(url, {
      ...init,
      headers,
      signal: controller.signal,
    });
  } catch (error) {
    if (isAbortError(error)) {
      throw new Error(`Maine CFIS artifact request timed out after ${timeoutMs}ms for ${url}`);
    }
    throw error;
  } finally {
    clearTimeout(timeout);
  }
}

function metadataFromResponse(
  artifact: MaineCfisArtifactIdentity,
  url: string,
  requestBody: MaineCfisCsvDownloadRequestBody,
  response: Response
): MaineCfisRemoteArtifactMetadata {
  const contentLength = response.headers.get("content-length");
  const parsedLength = contentLength ? Number(contentLength) : null;
  return {
    ...artifact,
    url,
    requestBody,
    contentLength: parsedLength !== null && Number.isFinite(parsedLength) ? parsedLength : null,
    contentType: response.headers.get("content-type"),
    contentDisposition: response.headers.get("content-disposition"),
    etag: response.headers.get("etag"),
    lastModified: response.headers.get("last-modified"),
  };
}

export async function fetchMaineCfisDownloadList(input: {
  url?: string;
  fetchImpl?: typeof fetch;
  timeoutMs?: number;
} = {}): Promise<MaineCfisDownloadListItem[]> {
  const normalizedUrl = parseMaineCfisHttpsUrl(input.url ?? MAINE_CFIS_DOWNLOAD_LIST_API_URL, "--url");
  const response = await fetchWithTimeout(normalizedUrl, { method: "GET" }, input);
  if (!response.ok) {
    throw new Error(`Failed to fetch Maine CFIS download list: ${response.status} ${response.statusText}`);
  }
  const parsed = (await response.json()) as unknown;
  if (!Array.isArray(parsed)) {
    throw new Error("Maine CFIS download list response was not an array");
  }
  return parsed.filter((item): item is MaineCfisDownloadListItem => {
    const candidate = item as Partial<MaineCfisDownloadListItem>;
    return (
      typeof candidate.TransactionKey === "string" &&
      typeof candidate.ElectionYear === "number" &&
      typeof candidate.NameOfFile === "string" &&
      (candidate.TransactionType === "CON" || candidate.TransactionType === "EXP")
    );
  });
}

export async function fetchMaineCfisArtifactMetadata(input: {
  filingYear: number;
  artifactKind: MaineCfisArtifactKind;
  url?: string;
  fetchImpl?: typeof fetch;
  timeoutMs?: number;
}): Promise<MaineCfisRemoteArtifactMetadata> {
  const artifact = normalizeMaineCfisArtifactIdentity(input);
  const normalizedUrl = parseMaineCfisHttpsUrl(input.url ?? MAINE_CFIS_CSV_DOWNLOAD_API_URL, "--url");
  const requestBody = buildMaineCfisCsvDownloadRequestBody(artifact);
  const response = await fetchWithTimeout(
    normalizedUrl,
    {
      method: "POST",
      body: JSON.stringify(requestBody),
    },
    input
  );
  try {
    if (!response.ok) {
      throw new Error(`Failed to fetch Maine CFIS artifact metadata: ${response.status} ${response.statusText}`);
    }
    return metadataFromResponse(artifact, normalizedUrl, requestBody, response);
  } finally {
    await response.body?.cancel().catch(() => {});
  }
}

export async function downloadMaineCfisArtifact(input: {
  filingYear: number;
  artifactKind: MaineCfisArtifactKind;
  outputPath: string;
  url?: string;
  fetchImpl?: typeof fetch;
  timeoutMs?: number;
}): Promise<MaineCfisArtifactDownloadResult> {
  const startedAt = Date.now();
  const artifact = normalizeMaineCfisArtifactIdentity(input);
  const normalizedUrl = parseMaineCfisHttpsUrl(input.url ?? MAINE_CFIS_CSV_DOWNLOAD_API_URL, "--url");
  const requestBody = buildMaineCfisCsvDownloadRequestBody(artifact);
  const outputPath = resolve(input.outputPath);
  const response = await fetchWithTimeout(
    normalizedUrl,
    {
      method: "POST",
      body: JSON.stringify(requestBody),
    },
    input
  );
  if (!response.ok) {
    throw new Error(`Failed to download Maine CFIS artifact: ${response.status} ${response.statusText}`);
  }
  if (!response.body) {
    throw new Error("Maine CFIS artifact response did not include a body");
  }

  const timeoutMs = input.timeoutMs ?? MAINE_CFIS_FETCH_TIMEOUT_MS;
  const remainingTimeoutMs = Math.max(1, timeoutMs - (Date.now() - startedAt));
  let timeout: NodeJS.Timeout | undefined;
  let outputStat;
  try {
    const source = Readable.fromWeb(response.body as NodeReadableStream<Uint8Array>);
    timeout = setTimeout(() => {
      source.destroy(new Error(`Maine CFIS artifact download timed out after ${timeoutMs}ms for ${normalizedUrl}`));
    }, remainingTimeoutMs);
    await pipeline(source, createWriteStream(outputPath));
    outputStat = await stat(outputPath);
  } catch (error) {
    await rm(outputPath, { force: true }).catch(() => {});
    throw error;
  } finally {
    if (timeout) {
      clearTimeout(timeout);
    }
  }

  const metadata = metadataFromResponse(artifact, normalizedUrl, requestBody, response);
  if (metadata.contentLength !== null && outputStat.size !== metadata.contentLength) {
    await rm(outputPath, { force: true }).catch(() => {});
    throw new Error(
      `Maine CFIS artifact download size mismatch: expected ${metadata.contentLength} bytes, received ${outputStat.size} bytes`
    );
  }

  return {
    ...metadata,
    outputPath,
    bytesWritten: outputStat.size,
  };
}
