/**
 * Cloudflare Worker: path router for impactperdollar.com.
 *
 * Implements the reverse-proxy split from docs/deploy-checklist.md on
 * Cloudflare instead of a self-managed proxy: /api/* and /sitemap.xml go to
 * the API service, everything else to the SSR server. Both origins are the
 * services' public *.onrender.com hosts (Render routes by Host header, so
 * the upstream URL's hostname is all that changes).
 *
 * Client IP: Cloudflare sets CF-Connecting-IP on every proxied request and
 * always overwrites a client-supplied copy, so both servers can trust it
 * (ADDRESS_API_TRUSTED_CLIENT_IP_HEADER=CF-Connecting-IP). This worker adds
 * no IP handling of its own.
 *
 * Worker vars (set in the dashboard or wrangler.toml):
 *   API_ORIGIN — e.g. "voteapp-api.onrender.com"
 *   SSR_ORIGIN — e.g. "voteapp-ssr.onrender.com"
 *
 * Both must be BARE hostnames. They are validated, not trusted: assigning an
 * invalid value to url.hostname is a silent no-op per the URL spec, so an
 * unvalidated "https://host" or "  " would leave the public hostname in
 * place and make the Worker fetch its own URL instead of failing.
 *
 * Route: impactperdollar.com/* and www.impactperdollar.com/* (www 301s to
 * the apex so the canonical origin matches SITE_ORIGIN and robots.txt).
 */

export function isApiPath(pathname) {
  return pathname === "/sitemap.xml" || pathname === "/api" || pathname.startsWith("/api/");
}

// RFC 1123 label: 1-63 chars, alphanumeric at both ends, alphanumeric or
// hyphen inside. Deliberately stricter than the URL parser, which (with the
// non-strict IDNA browsers use) happily accepts ".", "foo..bar",
// "-bad.example", or "_bad.example" — values that would only fail later as
// an uncaught fetch() error instead of a controlled 503.
const DNS_LABEL = /^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$/;

function isValidDnsHostname(hostname) {
  if (hostname.length === 0 || hostname.length > 253) {
    return false;
  }
  return hostname.split(".").every((label) => DNS_LABEL.test(label));
}

/** Returns the validated, canonicalized bare hostname, or null. */
export function resolveUpstreamHost(raw) {
  const value = (raw ?? "").trim();
  if (!value) {
    return null;
  }
  let parsed;
  try {
    parsed = new URL(`https://${value}`);
  } catch {
    return null;
  }
  // A bare hostname round-trips exactly; a scheme, port, path, credentials,
  // or query string all leave residue that breaks the equality.
  if (parsed.hostname !== value.toLowerCase()) {
    return null;
  }
  // Canonicalize the FQDN trailing dot ("host.example.") so the self-proxy
  // guard's equality check can't be dodged by a dot that DNS ignores.
  const hostname = parsed.hostname.replace(/\.$/, "");
  return isValidDnsHostname(hostname) ? hostname : null;
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    // The redirect reads no origin config — keep www working (301 to apex)
    // even while the vars are broken.
    if (url.hostname.startsWith("www.")) {
      url.hostname = url.hostname.slice("www.".length);
      return Response.redirect(url.toString(), 301);
    }

    const apiHost = resolveUpstreamHost(env.API_ORIGIN);
    const ssrHost = resolveUpstreamHost(env.SSR_ORIGIN);
    if (!apiHost || !ssrHost) {
      return new Response(
        "Worker misconfigured: API_ORIGIN and SSR_ORIGIN must both be set to bare hostnames",
        { status: 503 }
      );
    }

    const upstreamHost = isApiPath(url.pathname) ? apiHost : ssrHost;
    // The Worker owns both the apex and its www variant; an origin equal to
    // either would send traffic back into hostnames this Worker serves (or
    // their placeholder DNS records) instead of a real upstream.
    if (upstreamHost === url.hostname || upstreamHost === `www.${url.hostname}`) {
      return new Response(
        "Worker misconfigured: upstream origin equals the public hostname",
        { status: 503 }
      );
    }

    url.hostname = upstreamHost;
    url.protocol = "https:";
    url.port = "";

    // Re-wrap so method, headers, and body stream pass through; fetch()
    // rewrites the Host header to the new hostname automatically.
    return fetch(new Request(url.toString(), request));
  },
};
