import type { LookupAddress } from "node:dns";
import { lookup as dnsLookup } from "node:dns/promises";
import { isIP, type LookupFunction } from "node:net";
import ipaddr from "ipaddr.js";
import { Agent } from "undici";
import { normalizeHttpUrl } from "../utils/normalizeHttpUrl.js";

type UrlReachabilityOptions = {
  timeoutMs?: number;
  allowStatusCodes?: readonly number[];
  method?: "HEAD" | "GET";
};

export type UrlReachabilitySuccess = {
  ok: true;
  normalizedUrl: string;
  finalUrl: string;
  status: number;
  contentType?: string;
  contentLength?: number;
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

function stripIpv6Brackets(hostnameOrIp: string): string {
  return hostnameOrIp.toLowerCase().replace(/^\[|\]$/g, "");
}

/** Only ordinary globally routable unicast addresses are safe citation targets. */
function isBlockedIpLiteral(hostnameOrIp: string): boolean {
  const host = stripIpv6Brackets(hostnameOrIp);
  if (!ipaddr.isValid(host)) {
    return true;
  }

  let address = ipaddr.parse(host);
  if (address instanceof ipaddr.IPv6 && address.isIPv4MappedAddress()) {
    address = address.toIPv4Address();
  }

  return address.range() !== "unicast";
}

function isBlockedHostname(hostname: string): boolean {
  const host = stripIpv6Brackets(hostname);
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

  return isIP(host) > 0 && isBlockedIpLiteral(host);
}

type ResolvedUrlTarget = {
  hostname: string;
  addresses: LookupAddress[];
};

type UrlTargetResolution =
  | { target: ResolvedUrlTarget }
  | { failure: UrlReachabilityFailure };

async function resolveSafeUrlTarget(url: string): Promise<UrlTargetResolution> {
  let parsed: URL;
  try {
    parsed = new URL(url);
  } catch {
    return { failure: { ok: false, reason: "citation URL is not parseable" } };
  }

  if (isBlockedHostname(parsed.hostname)) {
    return {
      failure: { ok: false, reason: "citation URL points to a blocked/private host" },
    };
  }

  const hostname = stripIpv6Brackets(parsed.hostname);
  const literalFamily = isIP(hostname);
  if (literalFamily > 0) {
    return {
      target: {
        hostname,
        addresses: [{ address: hostname, family: literalFamily }],
      },
    };
  }

  let records: LookupAddress[];
  try {
    records = await dnsLookup(hostname, { all: true, verbatim: true });
  } catch {
    return {
      failure: { ok: false, reason: "citation URL hostname could not be resolved" },
    };
  }

  if (records.length === 0) {
    return {
      failure: { ok: false, reason: "citation URL hostname could not be resolved" },
    };
  }

  if (
    records.some(
      (record) =>
        (record.family !== 4 && record.family !== 6) ||
        isIP(record.address) !== record.family ||
        isBlockedIpLiteral(record.address)
    )
  ) {
    return {
      failure: {
        ok: false,
        reason: "citation URL hostname resolves to a blocked/private IP",
      },
    };
  }

  const uniqueRecords = records.filter(
    (record, index) =>
      records.findIndex(
        (candidate) =>
          candidate.family === record.family && candidate.address === record.address
      ) === index
  );

  return { target: { hostname, addresses: uniqueRecords } };
}

function createPinnedLookup(target: ResolvedUrlTarget): LookupFunction {
  // Return only the addresses already classified above. The socket never gets
  // a second DNS answer that could differ from the one we validated.
  return (hostname, options, callback) => {
    if (stripIpv6Brackets(hostname) !== target.hostname) {
      const error = Object.assign(new Error("Pinned DNS lookup received an unexpected hostname"), {
        code: "ENOTFOUND",
      });
      callback(error, "", 0);
      return;
    }

    const family = options.family === 4 || options.family === 6 ? options.family : 0;
    const addresses =
      family === 0
        ? target.addresses
        : target.addresses.filter((address) => address.family === family);
    if (addresses.length === 0) {
      const error = Object.assign(
        new Error("Pinned DNS lookup has no address for the requested family"),
        { code: "EAI_ADDRFAMILY" }
      );
      callback(error, "", 0);
      return;
    }

    if (options.all) {
      callback(null, addresses);
      return;
    }
    const [address] = addresses;
    callback(null, address.address, address.family);
  };
}

const MAX_REDIRECT_HOPS = 5;

const FOLLOWABLE_REDIRECT_STATUSES = new Set([301, 302, 303, 307, 308]);

// Keeps a failure reason readable when a hop URL carries a huge query string.
const REDIRECT_CHAIN_HOP_MAX_LENGTH = 200;

/**
 * Intermediate redirect targets can carry live credentials the original
 * citation never contained (presigned S3 signatures, auth codes, session
 * tokens in query strings, userinfo). The reason string is persisted to
 * staging failure debug and fed back to AI retries, so every hop is
 * sanitized to origin + path before it is reported.
 */
function sanitizeHopUrlForReason(hopUrl: string): string {
  let parsed: URL;
  try {
    parsed = new URL(hopUrl);
  } catch {
    return "[unparseable-url]";
  }
  parsed.username = "";
  parsed.password = "";
  parsed.hash = "";
  const hadQuery = parsed.search.length > 0;
  parsed.search = "";
  return hadQuery ? `${parsed.toString()}?[redacted]` : parsed.toString();
}

function formatRedirectChain(hops: readonly string[]): string {
  return hops
    .map((hop) => sanitizeHopUrlForReason(hop))
    .map((hop) =>
      hop.length > REDIRECT_CHAIN_HOP_MAX_LENGTH
        ? `${hop.slice(0, REDIRECT_CHAIN_HOP_MAX_LENGTH)}…`
        : hop
    )
    .join(" -> ");
}

async function cancelResponseBody(response: Response): Promise<void> {
  try {
    await response.body?.cancel();
  } catch {
    // Best effort: do not mask the primary result.
  }
}

type HopResponse = {
  ok: boolean;
  status: number;
  location: string | null;
  contentType: string | null;
  contentLength: number | null;
};

async function fetchPinnedUrl(
  url: string,
  target: ResolvedUrlTarget,
  method: "HEAD" | "GET",
  timeoutMs: number
): Promise<HopResponse> {
  const dispatcher = new Agent({
    connect: { lookup: createPinnedLookup(target) },
    autoSelectFamily:
      target.addresses.some((address) => address.family === 4) &&
      target.addresses.some((address) => address.family === 6),
  });
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  let response: Response | null = null;
  try {
    response = await fetch(url, {
      method,
      redirect: "manual",
      signal: controller.signal,
      dispatcher,
    } as RequestInit & { dispatcher: Agent });
    return {
      ok: response.ok,
      status: response.status,
      location: response.headers.get("location"),
      contentType: response.headers.get("content-type"),
      contentLength: (() => {
        const raw = response.headers.get("content-length");
        if (!raw) {
          return null;
        }
        const parsed = Number.parseInt(raw, 10);
        return Number.isFinite(parsed) && parsed >= 0 ? parsed : null;
      })(),
    };
  } finally {
    clearTimeout(timeout);
    if (response) {
      await cancelResponseBody(response);
    }
    await dispatcher.close();
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
): Promise<{ response: HopResponse; finalUrl: string } | { failure: UrlReachabilityFailure }> {
  let currentUrl = startUrl;
  const visitedUrls: string[] = [startUrl];
  for (let hop = 0; hop <= MAX_REDIRECT_HOPS; hop++) {
    const resolution = await resolveSafeUrlTarget(currentUrl);
    if ("failure" in resolution) {
      return resolution;
    }
    const response = await fetchPinnedUrl(currentUrl, resolution.target, method, timeoutMs);

    if (!FOLLOWABLE_REDIRECT_STATUSES.has(response.status)) {
      return { response, finalUrl: currentUrl };
    }

    const location = response.location;
    if (!location) {
      return { failure: { ok: false, reason: "citation URL redirect is missing a Location header" } };
    }
    let resolvedLocation: string;
    try {
      resolvedLocation = new URL(location, currentUrl).toString();
    } catch {
      return { failure: { ok: false, reason: "citation final URL is invalid after redirects" } };
    }
    // Hop targets keep their trailing slash: many hosts redirect the slashless
    // path TO the trailing-slash form, so the comparison-oriented default
    // (stripTrailingSlash) would recreate the slashless URL and walk a
    // synthetic self-loop until the hop limit (hit live on ordinary readable
    // pages across four run reports). Normalization here only validates the
    // scheme and drops the fragment.
    const nextUrl = normalizeHttpUrl(resolvedLocation, { stripTrailingSlash: false });
    if (!nextUrl) {
      return { failure: { ok: false, reason: "citation final URL is invalid after redirects" } };
    }
    // A repeated URL is a genuine redirect cycle (host anti-bot behavior or
    // scheme oscillation) — fail it immediately with its own reason instead of
    // burning the remaining hops as "exceeded the redirect limit". Membership
    // is exact-form on purpose: visitedUrls[0] is the slash-stripped input, so
    // a slashless->slashed redirect is correctly treated as a NEW URL (the
    // very case the hop normalization above preserves); comparing
    // slash-insensitively would re-flag it as a false loop. The only cost is
    // that a root URL redirecting to itself is caught one hop later, when its
    // slashed form repeats.
    if (visitedUrls.includes(nextUrl)) {
      return {
        failure: {
          ok: false,
          reason: `citation URL redirect loop detected (chain: ${formatRedirectChain([...visitedUrls, nextUrl])})`,
        },
      };
    }
    currentUrl = nextUrl;
    visitedUrls.push(nextUrl);
  }
  // Expose the walked chain so the terminal hop is diagnosable from the
  // failure reason alone (live run reports could not tell where a loop landed).
  return {
    failure: {
      ok: false,
      reason: `citation URL exceeded the redirect limit (chain: ${formatRedirectChain(visitedUrls)})`,
    },
  };
}

export async function verifyHttpUrlReachability(
  rawUrl: string,
  options: UrlReachabilityOptions = {}
): Promise<UrlReachabilityResult> {
  const timeoutMs = options.timeoutMs ?? 8_000;
  const allowStatusCodes = new Set(options.allowStatusCodes ?? [403]);
  const initialMethod = options.method ?? "HEAD";
  const normalizedInputUrl = normalizeHttpUrl(rawUrl);
  if (!normalizedInputUrl) {
    return { ok: false, reason: "citation URL is not a valid http(s) URL" };
  }

  try {
    const initialResult = await fetchWithValidatedRedirects(
      normalizedInputUrl,
      initialMethod,
      timeoutMs
    );
    if ("failure" in initialResult) {
      return initialResult.failure;
    }
    let response = initialResult.response;
    let finalUrl = initialResult.finalUrl;

    // HEAD is an optimization, not the authoritative answer: beyond hosts
    // that reject the method outright (405/501), some servers answer HEAD
    // with a different status than GET for the same resource (CivicPlus
    // DocumentCenter PDFs return HEAD 404 / GET 200, verified live), so any
    // failing HEAD status is confirmed with a GET before the URL is failed.
    // The GET re-walks the redirect chain from the original URL with the
    // same per-hop validation — HEAD and GET can redirect differently, so
    // HEAD's chain proves nothing about GET's.
    if (
      initialMethod === "HEAD" &&
      !response.ok &&
      !allowStatusCodes.has(response.status)
    ) {
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

    return {
      ok: true,
      normalizedUrl: normalizedInputUrl,
      finalUrl,
      status: response.status,
      ...(response.contentType ? { contentType: response.contentType } : {}),
      ...(response.contentLength !== null ? { contentLength: response.contentLength } : {}),
    };
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    if (message.toLowerCase().includes("aborted")) {
      return { ok: false, reason: "citation URL fetch timed out" };
    }
    return { ok: false, reason: `citation URL fetch failed: ${message}` };
  }
}
