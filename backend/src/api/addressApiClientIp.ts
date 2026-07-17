import { createHash, timingSafeEqual } from "node:crypto";

export type HeaderRecord = Record<string, string | string[] | undefined>;

export type AddressApiClientIpInput = {
  headers: HeaderRecord | undefined;
  remoteAddress: string | undefined;
};

// Shared-secret header the edge proxy (Cloudflare Worker) and the SSR
// loaders attach so the API can tell edge-forwarded traffic from a direct
// hit on the public *.onrender.com host. Direct hits can spoof the trusted
// client-IP header freely, so without this gate per-IP rate limiting is
// bypassable (docs/deploy-render.md "Direct onrender.com access").
export const EDGE_SHARED_SECRET_HEADER_NAME = "x-edge-secret";

function readHeader(headers: HeaderRecord | undefined, name: string): string | undefined {
  const lowerName = name.toLowerCase();
  const value = headers?.[lowerName] ?? headers?.[name];
  if (Array.isArray(value)) {
    return value[0];
  }
  return value;
}

export function parseTrustedClientIpHeader(value: string | undefined): string | null {
  const firstValue = value
    ?.split(",")
    .map((part) => part.trim())
    .find((part) => part.length > 0);
  return firstValue ?? null;
}

// Hash both sides so the comparison is constant-time regardless of length;
// timingSafeEqual throws on unequal buffer lengths otherwise.
function secretMatches(presented: string | undefined, expected: string): boolean {
  if (!presented) {
    return false;
  }
  const presentedDigest = createHash("sha256").update(presented).digest();
  const expectedDigest = createHash("sha256").update(expected).digest();
  return timingSafeEqual(presentedDigest, expectedDigest);
}

export function createTrustedClientIpResolver(
  trustedClientIpHeader: string | null | undefined,
  edgeSharedSecret?: string | null
): (input: AddressApiClientIpInput) => string {
  return (input) => {
    if (trustedClientIpHeader) {
      // With a secret configured, the client-IP header is only trusted when
      // the request proves it came through the edge. On mismatch fall back
      // to the socket IP rather than rejecting: SSR/direct traffic keeps
      // working, it just shares the direct-traffic rate-limit bucket.
      const edgeVerified =
        !edgeSharedSecret || secretMatches(readHeader(input.headers, EDGE_SHARED_SECRET_HEADER_NAME), edgeSharedSecret);
      if (edgeVerified) {
        const trustedHeaderValue = parseTrustedClientIpHeader(readHeader(input.headers, trustedClientIpHeader));
        if (trustedHeaderValue) {
          return trustedHeaderValue;
        }
      }
    }
    return input.remoteAddress?.trim() || "unknown";
  };
}
