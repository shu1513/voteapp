import { timingSafeStringEqual } from "./loadFromApi";

/**
 * Server-only: where to redirect a request that reached the SSR server
 * without going through the edge Worker, or null to serve it normally.
 *
 * Render gives the SSR service a public *.onrender.com hostname that bypasses
 * Cloudflare, so a page fetched there carries none of the Worker's security
 * headers (CSP, X-Frame-Options, HSTS). The Host header cannot tell the two
 * paths apart — the Worker rewrites it to the upstream hostname — but the
 * Worker stamps X-Edge-Secret on every request it proxies, SSR-bound ones
 * included (infra/cloudflare/router-worker.js), and loadFromApi.ts already
 * verifies that proof. A request without a valid proof did not come through
 * the edge.
 *
 * Both env vars gate the redirect, so it is inert by default:
 * - CANONICAL_SITE_ORIGIN (e.g. "https://electionssimplified.com"): unset in
 *   dev and until the operator opts in; also the safety valve — deleting it
 *   turns the redirect off without a deploy.
 * - EDGE_SHARED_SECRET: without a proof to check, nothing can be concluded
 *   about the path a request took, so no redirect.
 */
export function canonicalHostRedirectUrl(request: Request, env: NodeJS.ProcessEnv = process.env): string | null {
  const canonicalOrigin = env.CANONICAL_SITE_ORIGIN?.trim().replace(/\/+$/, "");
  const edgeSharedSecret = env.EDGE_SHARED_SECRET?.trim();
  if (!canonicalOrigin || !edgeSharedSecret) {
    return null;
  }
  const proof = request.headers.get("x-edge-secret");
  if (proof !== null && timingSafeStringEqual(proof, edgeSharedSecret)) {
    return null;
  }
  const url = new URL(request.url);
  return `${canonicalOrigin}${url.pathname}${url.search}`;
}
