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

function isApiPath(pathname) {
  return pathname === "/sitemap.xml" || pathname === "/api" || pathname.startsWith("/api/");
}

/** Returns the validated bare hostname, or null for anything else. */
function resolveUpstreamHost(raw) {
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
  return parsed.hostname;
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
