import { createWriteStream } from "node:fs";
import { rm, stat } from "node:fs/promises";
import { resolve } from "node:path";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";
import type { ReadableStream as NodeReadableStream } from "node:stream/web";

export const MARYLAND_CFS_PUBLIC_DOWNLOADS_URL =
  "https://campaignfinance.maryland.gov/public/cf/downloads";
export const MARYLAND_CFS_PUBLIC_EXPORT_API_URL =
  "https://api-campaignfinance.maryland.gov/api/ExportPublicData/GetExportPublicDownloadData";
export const MARYLAND_CFS_FETCH_TIMEOUT_MS = 900_000;

export type MarylandCfsArtifactKind = "contributions" | "expenditures" | "committees";
export type MarylandCfsTransactionTypeCode = "TCON" | "TEXP" | "TCMD";

export type MarylandCfsArtifactIdentity = {
  filingYear: number;
  artifactKind: MarylandCfsArtifactKind;
};

export type MarylandCfsRemoteArtifactMetadata = MarylandCfsArtifactIdentity & {
  url: string;
  requestBody: MarylandCfsPublicExportRequestBody;
  contentLength: number | null;
  contentType: string | null;
  etag: string | null;
  lastModified: string | null;
};

export type MarylandCfsArtifactDownloadResult = MarylandCfsRemoteArtifactMetadata & {
  outputPath: string;
  bytesWritten: number;
};

export type MarylandCfsPublicExportRequestBody = {
  Type: "CSV";
  TransactionTypeCode: MarylandCfsTransactionTypeCode;
  FilingYear: number;
};

type FetchOptions = {
  fetchImpl?: typeof fetch;
  timeoutMs?: number;
};

export function normalizeMarylandCfsFilingYear(filingYear: number): number {
  if (!Number.isInteger(filingYear) || filingYear < 2000 || filingYear > 2100) {
    throw new Error(`Invalid Maryland CFS filing year: ${filingYear}`);
  }
  return filingYear;
}

export function normalizeMarylandCfsArtifactKind(kind: string): MarylandCfsArtifactKind {
  const normalized = kind.trim().toLowerCase();
  if (normalized === "contributions" || normalized === "tcon") {
    return "contributions";
  }
  if (normalized === "expenditures" || normalized === "texp") {
    return "expenditures";
  }
  if (normalized === "committees" || normalized === "tcmd") {
    return "committees";
  }
  throw new Error(`Invalid Maryland CFS artifact kind: ${kind}`);
}

export function normalizeMarylandCfsArtifactIdentity(input: {
  filingYear: number;
  artifactKind: string;
}): MarylandCfsArtifactIdentity {
  return {
    filingYear: normalizeMarylandCfsFilingYear(input.filingYear),
    artifactKind: normalizeMarylandCfsArtifactKind(input.artifactKind),
  };
}

export function parseMarylandCfsHttpsUrl(value: string, fieldName = "Maryland CFS URL"): string {
  let parsed: URL;
  try {
    parsed = new URL(value);
  } catch {
    throw new Error(`Invalid ${fieldName}: ${value}`);
  }
  if (parsed.protocol !== "https:") {
    throw new Error(`Invalid ${fieldName} protocol: ${parsed.protocol}. Only https is allowed.`);
  }
  return parsed.toString();
}

export function marylandCfsTransactionTypeCode(kind: MarylandCfsArtifactKind): MarylandCfsTransactionTypeCode {
  switch (kind) {
    case "contributions":
      return "TCON";
    case "expenditures":
      return "TEXP";
    case "committees":
      return "TCMD";
  }
}

export function buildMarylandCfsPublicExportRequestBody(input: {
  filingYear: number;
  artifactKind: MarylandCfsArtifactKind;
}): MarylandCfsPublicExportRequestBody {
  const artifact = normalizeMarylandCfsArtifactIdentity(input);
  return {
    Type: "CSV",
    TransactionTypeCode: marylandCfsTransactionTypeCode(artifact.artifactKind),
    FilingYear: artifact.filingYear,
  };
}

function isAbortError(error: unknown): boolean {
  return (
    (error instanceof DOMException && error.name === "AbortError") ||
    (error instanceof Error && error.name === "AbortError")
  );
}

async function fetchWithTimeout(url: string, init: RequestInit, options: FetchOptions): Promise<Response> {
  const timeoutMs = options.timeoutMs ?? MARYLAND_CFS_FETCH_TIMEOUT_MS;
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  const headers = new Headers(init.headers);
  if (!headers.has("accept")) {
    headers.set("accept", "text/csv,text/plain;q=0.9,*/*;q=0.1");
  }
  if (!headers.has("content-type")) {
    headers.set("content-type", "application/json");
  }
  if (!headers.has("origin")) {
    headers.set("origin", "https://campaignfinance.maryland.gov");
  }
  if (!headers.has("referer")) {
    headers.set("referer", MARYLAND_CFS_PUBLIC_DOWNLOADS_URL);
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
      throw new Error(`Maryland CFS artifact request timed out after ${timeoutMs}ms for ${url}`);
    }
    throw error;
  } finally {
    clearTimeout(timeout);
  }
}

function metadataFromResponse(
  artifact: MarylandCfsArtifactIdentity,
  url: string,
  requestBody: MarylandCfsPublicExportRequestBody,
  response: Response
): MarylandCfsRemoteArtifactMetadata {
  const contentLength = response.headers.get("content-length");
  const parsedLength = contentLength ? Number(contentLength) : null;
  return {
    ...artifact,
    url,
    requestBody,
    contentLength: parsedLength !== null && Number.isFinite(parsedLength) ? parsedLength : null,
    contentType: response.headers.get("content-type"),
    etag: response.headers.get("etag"),
    lastModified: response.headers.get("last-modified"),
  };
}

export async function fetchMarylandCfsArtifactMetadata(input: {
  filingYear: number;
  artifactKind: MarylandCfsArtifactKind;
  url?: string;
  fetchImpl?: typeof fetch;
  timeoutMs?: number;
}): Promise<MarylandCfsRemoteArtifactMetadata> {
  const artifact = normalizeMarylandCfsArtifactIdentity(input);
  const normalizedUrl = parseMarylandCfsHttpsUrl(input.url ?? MARYLAND_CFS_PUBLIC_EXPORT_API_URL, "--url");
  const requestBody = buildMarylandCfsPublicExportRequestBody(artifact);
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
      throw new Error(`Failed to fetch Maryland CFS artifact metadata: ${response.status} ${response.statusText}`);
    }
    return metadataFromResponse(artifact, normalizedUrl, requestBody, response);
  } finally {
    await response.body?.cancel().catch(() => {});
  }
}

export async function downloadMarylandCfsArtifact(input: {
  filingYear: number;
  artifactKind: MarylandCfsArtifactKind;
  outputPath: string;
  url?: string;
  fetchImpl?: typeof fetch;
  timeoutMs?: number;
}): Promise<MarylandCfsArtifactDownloadResult> {
  const artifact = normalizeMarylandCfsArtifactIdentity(input);
  const normalizedUrl = parseMarylandCfsHttpsUrl(input.url ?? MARYLAND_CFS_PUBLIC_EXPORT_API_URL, "--url");
  const requestBody = buildMarylandCfsPublicExportRequestBody(artifact);
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
    throw new Error(`Failed to download Maryland CFS artifact: ${response.status} ${response.statusText}`);
  }
  if (!response.body) {
    throw new Error("Maryland CFS artifact response did not include a body");
  }

  const timeoutMs = input.timeoutMs ?? MARYLAND_CFS_FETCH_TIMEOUT_MS;
  let timeout: NodeJS.Timeout | undefined;
  let outputStat;
  try {
    const source = Readable.fromWeb(response.body as NodeReadableStream<Uint8Array>);
    timeout = setTimeout(() => {
      source.destroy(new Error(`Maryland CFS artifact download timed out after ${timeoutMs}ms for ${normalizedUrl}`));
    }, timeoutMs);
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
      `Maryland CFS artifact download size mismatch: expected ${metadata.contentLength} bytes, received ${outputStat.size} bytes`
    );
  }

  return {
    ...metadata,
    outputPath,
    bytesWritten: outputStat.size,
  };
}
