import type { ReactNode } from "react";
import { Links, Meta, Outlet, redirect, Scripts } from "react-router";
import type { LoaderFunctionArgs, MetaFunction } from "react-router";
import { isServer, MutationCache, QueryCache, QueryClient, QueryClientProvider } from "@tanstack/react-query";
import "./index.css";
import { APP_NAME, ApiError } from "@voteapp/api-client";
import { RouteError } from "./components/RouteError";
import { canonicalHostRedirectUrl } from "./lib/canonicalHostRedirect";
import { captureMonitoredError } from "./lib/errorMonitoring";
import { pageMeta } from "./lib/pageMeta";

// Runs on the server for every document request. A request that reached the
// SSR service on its raw *.onrender.com hostname (bypassing the edge Worker
// and its security headers) is sent to the canonical site; see
// lib/canonicalHostRedirect.ts for the gate. Returns nothing otherwise.
export function loader({ request }: LoaderFunctionArgs): null {
  const target = canonicalHostRedirectUrl(request);
  if (target !== null) {
    throw redirect(target, 301);
  }
  return null;
}

// The default for every page that does not export its own meta — which is most
// of them, since the rest set their title client-side with useDocumentTitle.
// Link scrapers do not run JavaScript, so this is what they see.
export const meta: MetaFunction = () => pageMeta({ title: APP_NAME });

// 5xx only: 4xx are expected product states (bad address, unverified email,
// rate limits), and non-ApiError failures are usually the user's network.
function reportServerError(error: unknown): void {
  if (error instanceof ApiError && error.status >= 500) {
    captureMonitoredError(error, {
      source: "api",
      status: String(error.status),
      code: error.code,
      // Correlates this event with the backend's log line and Sentry event
      // for the same failure (unexpected-500 envelopes carry the id).
      ...(error.requestId ? { request_id: error.requestId } : {}),
    });
  }
}

function makeQueryClient(): QueryClient {
  return new QueryClient({
    queryCache: new QueryCache({ onError: reportServerError }),
    mutationCache: new MutationCache({ onError: reportServerError }),
    defaultOptions: {
      queries: {
        retry: 1,
        staleTime: 60_000,
      },
    },
  });
}

let browserQueryClient: QueryClient | undefined;

// The SSR server keeps this module loaded across requests, and useQuery
// inserts a cache entry for every key it renders with — with gcTime
// defaulting to Infinity on the server, a shared client would retain every
// attacker-controlled URL value (verification tokens, district/candidate
// ids) forever: an unbounded memory leak. So the server gets a fresh client
// per render (dropped with the render), while the browser keeps a singleton
// so the cache survives navigation. Per TanStack Query's SSR guidance, this
// is deliberately not React state: React can discard and re-run renders
// (StrictMode, suspense), and client identity must not change when it does.
//
// Invariants this relies on: (1) a server re-render getting a fresh client is
// harmless because SSR never populates the cache — nothing prefetches or
// dehydrates, so every request renders pending state regardless; (2) the
// browser singleton resets when Vite HMR re-evaluates this module in dev,
// same as the previous module-global client — accepted, dev-only.
export function getQueryClient(): QueryClient {
  if (isServer) {
    return makeQueryClient();
  }
  browserQueryClient ??= makeQueryClient();
  return browserQueryClient;
}

function Providers({ children }: { children: ReactNode }) {
  return <QueryClientProvider client={getQueryClient()}>{children}</QueryClientProvider>;
}

export function Layout({ children }: { children: ReactNode }) {
  return (
    <html lang="en">
      <head>
        <meta charSet="UTF-8" />
        <link rel="icon" type="image/svg+xml" href="/favicon.svg" />
        <meta name="viewport" content="width=device-width, initial-scale=1.0" />
        <Meta />
        <Links />
      </head>
      <body>
        <Providers>{children}</Providers>
        <Scripts />
      </body>
    </html>
  );
}

export default function Root() {
  return <Outlet />;
}

export function ErrorBoundary() {
  return <RouteError />;
}
