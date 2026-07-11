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

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  let response: Response | null = null;

  try {
    response = await fetch(normalizedInputUrl, {
      method: "HEAD",
      redirect: "follow",
      signal: controller.signal,
    });

    // HEAD is an optimization, not the authoritative answer: beyond hosts
    // that reject the method outright (405/501), some servers answer HEAD
    // with a different status than GET for the same resource (CivicPlus
    // DocumentCenter PDFs return HEAD 404 / GET 200, verified live), so any
    // failing HEAD status is confirmed with a GET before the URL is failed.
    if (!response.ok && !allowStatusCodes.has(response.status)) {
      try {
        await response.body?.cancel();
      } catch {
        // Best effort.
      }
      // SSRF guard: HEAD already followed redirects, so if its chain ended
      // on a blocked/private destination, fail here instead of repeating the
      // request as a GET against that destination.
      const headFinalUrl = normalizeHttpUrl(response.url || normalizedInputUrl);
      if (!headFinalUrl) {
        return { ok: false, reason: "citation final URL is invalid after redirects" };
      }
      const headFinalSafety = await validateParsedUrlSafety(headFinalUrl);
      if (headFinalSafety) {
        return headFinalSafety;
      }
      // The GET gets its own timeout window: a slow-failing HEAD would
      // otherwise leave the shared timer with almost no budget and surface a
      // misleading timeout instead of a status failure.
      const getController = new AbortController();
      const getTimeout = setTimeout(() => getController.abort(), timeoutMs);
      try {
        response = await fetch(normalizedInputUrl, {
          method: "GET",
          redirect: "follow",
          signal: getController.signal,
        });
      } finally {
        clearTimeout(getTimeout);
      }
    }

    if (!response.ok && !allowStatusCodes.has(response.status)) {
      return { ok: false, reason: `citation fetch returned status ${response.status}` };
    }

    const finalUrl = normalizeHttpUrl(response.url || normalizedInputUrl);
    if (!finalUrl) {
      return { ok: false, reason: "citation final URL is invalid after redirects" };
    }

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
      try {
        await response.body?.cancel();
      } catch {
        // Best effort: do not mask primary result.
      }
    }
    clearTimeout(timeout);
  }
}
