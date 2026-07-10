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
 * Route: impactperdollar.com/* and www.impactperdollar.com/* (www 301s to
 * the apex so the canonical origin matches SITE_ORIGIN and robots.txt).
 */

function isApiPath(pathname) {
  return pathname === "/sitemap.xml" || pathname === "/api" || pathname.startsWith("/api/");
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    if (url.hostname.startsWith("www.")) {
      url.hostname = url.hostname.slice("www.".length);
      return Response.redirect(url.toString(), 301);
    }

    url.hostname = isApiPath(url.pathname) ? env.API_ORIGIN : env.SSR_ORIGIN;
    url.protocol = "https:";
    url.port = "";

    // Re-wrap so method, headers, and body stream pass through; fetch()
    // rewrites the Host header to the new hostname automatically.
    return fetch(new Request(url.toString(), request));
  },
};
