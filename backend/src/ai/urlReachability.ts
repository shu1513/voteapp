import { isIP } from "node:net";
import { lookup as dnsLookup } from "node:dns/promises";
import { normalizeHttpUrl } from "../utils/normalizeHttpUrl.js";

type UrlReachabilityOptions = {
  timeoutMs?: number;
  allowStatusCodes?: readonly number[];
};

export type UrlReachabilitySuccess = {
  ok: true;
  normalizedUrl: string;
  finalUrl: string;
  status: number;
};

export type UrlReachabilityFailure = {
  ok: false;
  reason: string;
};

export type UrlReachabilityResult = UrlReachabilitySuccess | UrlReachabilityFailure;

const TLS_CERTIFICATE_FAILURE_PATTERNS = [
  "unable_to_verify_leaf_signature",
  "unable to verify the first certificate",
  "unable to get local issuer certificate",
  "self-signed certificate",
  "self signed certificate",
  "depth_zero_self_signed_cert",
  "self_signed_cert_in_chain",
  "certificate has expired",
  "cert_has_expired",
  "unable_to_get_issuer_cert",
];

export function isTlsCertificateReachabilityFailure(reason: string): boolean {
  const normalized = reason.trim().toLowerCase();
  return TLS_CERTIFICATE_FAILURE_PATTERNS.some((pattern) => normalized.includes(pattern));
}

function isPrivateIpLiteral(hostnameOrIp: string): boolean {
  const host = hostnameOrIp.toLowerCase().replace(/^\[|\]$/g, "");
  const ipVersion = isIP(host);
  if (ipVersion === 4) {
    const octets = host.split(".").map((part) => Number.parseInt(part, 10));
    if (octets.length !== 4 || octets.some((n) => !Number.isInteger(n) || n < 0 || n > 255)) {
      return true;
    }

    const [a, b] = octets;
    return (
      a === 10 ||
      a === 127 ||
      (a === 169 && b === 254) ||
      (a === 172 && b >= 16 && b <= 31) ||
      (a === 192 && b === 168) ||
      a === 0 ||
      (a === 100 && b >= 64 && b <= 127) ||
      (a === 198 && (b === 18 || b === 19)) ||
      a >= 224
    );
  }

  if (ipVersion === 6) {
    return (
      host === "::1" ||
      host === "::" ||
      host.startsWith("fc") ||
      host.startsWith("fd") ||
      host.startsWith("fe8") ||
      host.startsWith("fe9") ||
      host.startsWith("fea") ||
      host.startsWith("feb")
    );
  }

  return false;
}

function isBlockedHostname(hostname: string): boolean {
  const host = hostname.toLowerCase().replace(/^\[|\]$/g, "");
  if (
    host === "localhost" ||
    host.endsWith(".localhost") ||
    host.endsWith(".local") ||
    host.endsWith(".internal") ||
    host === "metadata.google.internal" ||
    host === "metadata"
  ) {
    return true;
  }

  return isPrivateIpLiteral(host);
}

async function resolvesToBlockedPrivateIp(hostname: string): Promise<boolean> {
  const host = hostname.toLowerCase().replace(/^\[|\]$/g, "");
  if (!host || isIP(host) > 0) {
    return false;
  }

  try {
    const records = await dnsLookup(host, { all: true, verbatim: true });
    return records.some((record) => isPrivateIpLiteral(record.address));
  } catch {
    // Best-effort DNS safety check.
    return false;
  }
}

async function validateParsedUrlSafety(url: string): Promise<UrlReachabilityFailure | null> {
  let parsed: URL;
  try {
    parsed = new URL(url);
  } catch {
    return { ok: false, reason: "citation URL is not parseable" };
  }

  if (isBlockedHostname(parsed.hostname)) {
    return { ok: false, reason: "citation URL points to a blocked/private host" };
  }
  if (await resolvesToBlockedPrivateIp(parsed.hostname)) {
    return { ok: false, reason: "citation URL hostname resolves to a blocked/private IP" };
  }

  return null;
}

const MAX_REDIRECT_HOPS = 5;

const FOLLOWABLE_REDIRECT_STATUSES = new Set([301, 302, 303, 307, 308]);

async function cancelResponseBody(response: Response): Promise<void> {
  try {
    await response.body?.cancel();
  } catch {
    // Best effort: do not mask the primary result.
  }
}

/**
 * SSRF-safe fetch: redirects are followed manually so every Location target
 * is safety-checked BEFORE it is contacted. `redirect: "follow"` would let a
 * public URL bounce the backend into a private/internal address and only
 * reject it after the request already happened. Each hop gets its own
 * timeout window; abort errors propagate to the caller's catch.
 */
async function fetchWithValidatedRedirects(
  startUrl: string,
  method: "HEAD" | "GET",
  timeoutMs: number
): Promise<{ response: Response; finalUrl: string } | { failure: UrlReachabilityFailure }> {
  let currentUrl = startUrl;
  for (let hop = 0; hop <= MAX_REDIRECT_HOPS; hop++) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), timeoutMs);
    let response: Response;
    try {
      response = await fetch(currentUrl, {
        method,
        redirect: "manual",
        signal: controller.signal,
      });
    } finally {
      clearTimeout(timeout);
    }

    if (!FOLLOWABLE_REDIRECT_STATUSES.has(response.status)) {
      return { response, finalUrl: currentUrl };
    }

    const location = response.headers.get("location");
    await cancelResponseBody(response);
    if (!location) {
      return { failure: { ok: false, reason: "citation URL redirect is missing a Location header" } };
    }
    let resolvedLocation: string;
    try {
      resolvedLocation = new URL(location, currentUrl).toString();
    } catch {
      return { failure: { ok: false, reason: "citation final URL is invalid after redirects" } };
    }
    const nextUrl = normalizeHttpUrl(resolvedLocation);
    if (!nextUrl) {
      return { failure: { ok: false, reason: "citation final URL is invalid after redirects" } };
    }
    const nextSafety = await validateParsedUrlSafety(nextUrl);
    if (nextSafety) {
      return { failure: nextSafety };
    }
    currentUrl = nextUrl;
  }
  return { failure: { ok: false, reason: "citation URL exceeded the redirect limit" } };
}

export async function verifyHttpUrlReachability(
  rawUrl: string,
  options: UrlReachabilityOptions = {}
): Promise<UrlReachabilityResult> {
  const timeoutMs = options.timeoutMs ?? 8_000;
  const allowStatusCodes = new Set(options.allowStatusCodes ?? [403]);
  const normalizedInputUrl = normalizeHttpUrl(rawUrl);
  if (!normalizedInputUrl) {
    return { ok: false, reason: "citation URL is not a valid http(s) URL" };
  }

  const inputSafety = await validateParsedUrlSafety(normalizedInputUrl);
  if (inputSafety) {
    return inputSafety;
  }

  let response: Response | null = null;

  try {
    const headResult = await fetchWithValidatedRedirects(normalizedInputUrl, "HEAD", timeoutMs);
    if ("failure" in headResult) {
      return headResult.failure;
    }
    response = headResult.response;
    let finalUrl = headResult.finalUrl;

    // HEAD is an optimization, not the authoritative answer: beyond hosts
    // that reject the method outright (405/501), some servers answer HEAD
    // with a different status than GET for the same resource (CivicPlus
    // DocumentCenter PDFs return HEAD 404 / GET 200, verified live), so any
    // failing HEAD status is confirmed with a GET before the URL is failed.
    // The GET re-walks the redirect chain from the original URL with the
    // same per-hop validation — HEAD and GET can redirect differently, so
    // HEAD's chain proves nothing about GET's.
    if (!response.ok && !allowStatusCodes.has(response.status)) {
      await cancelResponseBody(response);
      response = null;
      const getResult = await fetchWithValidatedRedirects(normalizedInputUrl, "GET", timeoutMs);
      if ("failure" in getResult) {
        return getResult.failure;
      }
      response = getResult.response;
      finalUrl = getResult.finalUrl;
    }

    if (!response.ok && !allowStatusCodes.has(response.status)) {
      return { ok: false, reason: `citation fetch returned status ${response.status}` };
    }

    // Every hop, including this final URL, was validated before it was
    // requested; re-check once more as defense in depth.
    const finalSafety = await validateParsedUrlSafety(finalUrl);
    if (finalSafety) {
      return finalSafety;
    }

    return {
      ok: true,
      normalizedUrl: normalizedInputUrl,
      finalUrl,
      status: response.status,
    };
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    if (message.toLowerCase().includes("aborted")) {
      return { ok: false, reason: "citation URL fetch timed out" };
    }
    return { ok: false, reason: `citation URL fetch failed: ${message}` };
  } finally {
    if (response) {
      await cancelResponseBody(response);
    }
  }
}
