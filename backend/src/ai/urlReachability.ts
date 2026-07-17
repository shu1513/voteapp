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
  // Present when the server answered 429 with a parseable Retry-After header:
  // callers with retry loops can wait at least this long instead of their
  // default backoff.
  retryAfterSeconds?: number;
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
  // Hostname/SAN mismatch (live: wrong.host.badssl.com) — as permanent as an
  // expired certificate, and previously misclassified transient because only
  // the "fetch failed" wrapper text reached the classifier.
  "err_tls_cert_altname_invalid",
  "does not match certificate's altnames",
];

export function isTlsCertificateReachabilityFailure(reason: string): boolean {
  const normalized = reason.trim().toLowerCase();
  return TLS_CERTIFICATE_FAILURE_PATTERNS.some((pattern) => normalized.includes(pattern));
}

/**
 * Single owner of the reason-string retry contract (this module produces
 * every reason these substrings match). TLS certificate failures classify
 * permanent FIRST: they arrive wrapped in undici's "fetch failed" (which is
 * otherwise transient), and retrying an expired or self-signed certificate
 * can never succeed.
 */
export function classifyCitationVerificationFailure(reason: string): "transient" | "permanent" {
  if (isTlsCertificateReachabilityFailure(reason)) {
    return "permanent";
  }
  const normalized = reason.toLowerCase();
  if (
    normalized.includes("timed out") ||
    normalized.includes("dns lookup failed transiently") ||
    normalized.includes("fetch failed") ||
    normalized.includes("status 429") ||
    normalized.includes("status 500") ||
    normalized.includes("status 502") ||
    normalized.includes("status 503") ||
    normalized.includes("status 504")
  ) {
    return "transient";
  }
  return "permanent";
}

function stripIpv6Brackets(hostnameOrIp: string): string {
  return hostnameOrIp.toLowerCase().replace(/^\[|\]$/g, "");
}

// RFC 8215 local-use NAT64 prefix. ipaddr.js only special-cases the
// well-known RFC 6052 prefix (64:ff9b::/96, range "rfc6052"), so this one
// comes back plain "unicast" — but on a network with a NAT64 translator it
// maps onto arbitrary IPv4, including loopback and RFC 1918.
const NAT64_LOCAL_USE_RANGE = ipaddr.IPv6.parseCIDR("64:ff9b:1::/48");

// RFC 2544 benchmarking range. ipaddr.js only labels it "benchmarking" from
// v2.x; the installed 1.x returns plain "unicast", so check it explicitly.
const BENCHMARKING_RANGE = ipaddr.IPv4.parseCIDR("198.18.0.0/15");

/** Only ordinary globally routable unicast addresses are safe citation targets. */
function isBlockedIpLiteral(hostnameOrIp: string): boolean {
  const host = stripIpv6Brackets(hostnameOrIp);
  if (!ipaddr.isValid(host)) {
    return true;
  }

  let address = ipaddr.parse(host);
  if (address instanceof ipaddr.IPv6) {
    if (address.isIPv4MappedAddress()) {
      address = address.toIPv4Address();
    } else if (address.match(NAT64_LOCAL_USE_RANGE)) {
      return true;
    }
  }

  if (address instanceof ipaddr.IPv4 && address.match(BENCHMARKING_RANGE)) {
    return true;
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

// dns.lookup error codes that indicate the resolver (not the name) failed.
// These must stay retryable downstream; ENOTFOUND/ENODATA stay permanent.
const TRANSIENT_DNS_LOOKUP_ERROR_CODES = new Set([
  "EAI_AGAIN",
  "ETIMEOUT",
  "ETIMEDOUT",
  "ESERVFAIL",
  "EREFUSED",
]);

const DNS_LOOKUP_TIMEOUT_CODE = "DNS_LOOKUP_TIMEOUT";

async function resolveSafeUrlTarget(url: string, timeoutMs: number): Promise<UrlTargetResolution> {
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

  // dns.lookup has no abort support, so race it against a timer: the fetch
  // timeout only starts after resolution, and a stalled resolver must not
  // hold the verifier far past its advertised budget. On timeout the
  // in-flight getaddrinfo call is abandoned (it settles into a no-op).
  //
  // Reason strings are a contract with classifyCitationVerificationFailure
  // (exported below): "timed out" and "dns lookup failed transiently"
  // classify as transient/retryable, the bare could-not-be-resolved reason
  // as permanent.
  let records: LookupAddress[];
  let dnsTimeoutTimer: NodeJS.Timeout | undefined;
  try {
    records = await Promise.race([
      dnsLookup(hostname, { all: true, verbatim: true }),
      new Promise<never>((_, reject) => {
        dnsTimeoutTimer = setTimeout(() => {
          reject(
            Object.assign(new Error("DNS lookup timed out"), { code: DNS_LOOKUP_TIMEOUT_CODE })
          );
        }, timeoutMs);
      }),
    ]);
  } catch (error) {
    const code =
      error instanceof Error && "code" in error ? String((error as { code?: unknown }).code) : "";
    if (code === DNS_LOOKUP_TIMEOUT_CODE) {
      return { failure: { ok: false, reason: "citation URL DNS lookup timed out" } };
    }
    if (TRANSIENT_DNS_LOOKUP_ERROR_CODES.has(code)) {
      return {
        failure: { ok: false, reason: `citation URL DNS lookup failed transiently: ${code}` },
      };
    }
    return {
      failure: { ok: false, reason: "citation URL hostname could not be resolved" },
    };
  } finally {
    clearTimeout(dnsTimeoutTimer);
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
  retryAfterSeconds: number | null;
};

// Retry-After sanity ceiling: some hosts send hour-scale values; anything a
// verifier or its callers would actually wait is far below this, and the
// cap keeps a hostile header from parking a worker.
const RETRY_AFTER_MAX_SECONDS = 900;

/** Parses Retry-After in both delta-seconds and HTTP-date forms. */
function parseRetryAfterSeconds(raw: string | null): number | null {
  if (!raw) {
    return null;
  }
  const trimmed = raw.trim();
  if (/^\d+$/.test(trimmed)) {
    const seconds = Number.parseInt(trimmed, 10);
    return Number.isFinite(seconds) ? Math.min(seconds, RETRY_AFTER_MAX_SECONDS) : null;
  }
  const dateMs = Date.parse(trimmed);
  if (Number.isNaN(dateMs)) {
    return null;
  }
  const seconds = Math.ceil((dateMs - Date.now()) / 1000);
  return seconds > 0 ? Math.min(seconds, RETRY_AFTER_MAX_SECONDS) : 0;
}

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
      retryAfterSeconds:
        response.status === 429 ? parseRetryAfterSeconds(response.headers.get("retry-after")) : null,
    };
  } finally {
    clearTimeout(timeout);
    if (response) {
      await cancelResponseBody(response);
    }
    try {
      await dispatcher.close();
    } catch {
      // Best effort: do not mask the primary result.
    }
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
    const resolution = await resolveSafeUrlTarget(currentUrl, timeoutMs);
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

/**
 * undici's fetch reports every network-level failure as a bare
 * `TypeError: fetch failed` and hides the real error (TLS certificate codes,
 * ECONNRESET, ECONNREFUSED, ...) in the `cause` chain. Reasons built from the
 * top-level message alone made every such failure look identical AND
 * misclassified permanent TLS failures as transient (live: ERR-321/329).
 * Walk the chain and surface each distinct message/code.
 */
function describeFetchError(error: unknown): string {
  const parts: string[] = [];
  let current: unknown = error;
  for (let depth = 0; depth < 4 && current instanceof Error; depth += 1) {
    const code =
      "code" in current && typeof (current as { code?: unknown }).code === "string"
        ? ((current as { code: string }).code)
        : undefined;
    const message = current.message?.trim();
    // "||" not "??": an empty trimmed message must fall through to the code.
    const label = message && code && message !== code ? `${message} (${code})` : (message || code);
    if (label && !parts.includes(label)) {
      parts.push(label);
    }
    current = current.cause;
  }
  return parts.length > 0 ? parts.join(": ") : String(error);
}

// Adaptive per-host cooldown: set only when a host answers 429, so the
// normal path pays nothing. Later verifications against the same host wait
// out the cooldown (Retry-After when given, a short default otherwise)
// instead of re-bursting a host that just told us to slow down (ERR-022).
const HOST_COOLDOWN_DEFAULT_MS = 1_000;
const HOST_COOLDOWN_MAX_WAIT_MS = 15_000;
const HOST_COOLDOWN_MAP_MAX_ENTRIES = 200;

const hostCooldownUntilMs = new Map<string, number>();

// Drops expired entries once the map grows past the threshold. Entries still
// inside an active cooldown are deliberately kept even beyond the threshold:
// every entry expires within RETRY_AFTER_MAX_SECONDS, so the map is bounded
// in time, and a few hundred hostname strings are cheaper than evicting a
// cooldown a rate-limited host asked for.
function pruneExpiredHostCooldowns(nowMs: number): void {
  if (hostCooldownUntilMs.size <= HOST_COOLDOWN_MAP_MAX_ENTRIES) {
    return;
  }
  for (const [host, untilMs] of hostCooldownUntilMs) {
    if (untilMs <= nowMs) {
      hostCooldownUntilMs.delete(host);
    }
  }
}

// Waits out the host's active cooldown, bounded overall by
// HOST_COOLDOWN_MAX_WAIT_MS. Re-reads the map after every sleep: another
// in-flight verification can 429 and EXTEND the entry while this one sleeps,
// and deleting unconditionally on wake would erase that newer cooldown.
// Entries are only deleted when the observed value is still current.
async function awaitHostCooldown(hostname: string): Promise<void> {
  const waitDeadlineMs = Date.now() + HOST_COOLDOWN_MAX_WAIT_MS;
  while (true) {
    const untilMs = hostCooldownUntilMs.get(hostname);
    if (untilMs === undefined) {
      return;
    }
    const nowMs = Date.now();
    if (untilMs <= nowMs) {
      if (hostCooldownUntilMs.get(hostname) === untilMs) {
        hostCooldownUntilMs.delete(hostname);
      }
      return;
    }
    const waitMs = Math.min(untilMs, waitDeadlineMs) - nowMs;
    if (waitMs <= 0) {
      // Overall bound reached; proceed rather than park the verifier. The
      // entry stays for the next caller.
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, waitMs));
  }
}

function recordHostCooldown(hostname: string, retryAfterSeconds: number | null): void {
  const nowMs = Date.now();
  pruneExpiredHostCooldowns(nowMs);
  const cooldownMs =
    retryAfterSeconds !== null && retryAfterSeconds > 0
      ? retryAfterSeconds * 1_000
      : HOST_COOLDOWN_DEFAULT_MS;
  hostCooldownUntilMs.set(hostname, nowMs + cooldownMs);
}

/** Test-only: cooldown state is module-global and must not leak across tests. */
export function resetCitationHostCooldownsForTests(): void {
  hostCooldownUntilMs.clear();
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
  const inputHostname = (() => {
    try {
      return new URL(normalizedInputUrl).hostname.toLowerCase();
    } catch {
      return null;
    }
  })();
  if (inputHostname) {
    await awaitHostCooldown(inputHostname);
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
    //
    // EXCEPT 429: rate limiting is method-agnostic, so an immediate GET
    // would double the burst at a host that just asked us to back off, and
    // a differing GET status would silently discard the 429 and its
    // Retry-After. The HEAD 429 is authoritative; the transient retry path
    // re-verifies after the cooldown.
    if (
      initialMethod === "HEAD" &&
      !response.ok &&
      response.status !== 429 &&
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
      if (response.status === 429) {
        // The 429 comes from the FINAL hop (a redirect can land on a CDN or
        // document host), so cool that host down; the input host is cooled
        // too because retries re-enter through the input URL. Cooldowns are
        // still only awaited at entry, not per redirect hop — a different
        // citation that merely redirects onto the limited host won't wait.
        // Accepted: this pacing is a best-effort burst damper, not a rate
        // limiter.
        const finalHostname = (() => {
          try {
            return new URL(finalUrl).hostname.toLowerCase();
          } catch {
            return null;
          }
        })();
        for (const hostname of new Set(
          [inputHostname, finalHostname].filter((host): host is string => host !== null)
        )) {
          recordHostCooldown(hostname, response.retryAfterSeconds);
        }
        return {
          ok: false,
          reason:
            response.retryAfterSeconds !== null
              ? `citation fetch returned status 429 (retry-after: ${response.retryAfterSeconds}s)`
              : "citation fetch returned status 429",
          ...(response.retryAfterSeconds !== null
            ? { retryAfterSeconds: response.retryAfterSeconds }
            : {}),
        };
      }
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
    return { ok: false, reason: `citation URL fetch failed: ${describeFetchError(error)}` };
  }
}
