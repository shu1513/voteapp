import type { ReactNode } from "react";
import { Links, Meta, Outlet, Scripts } from "react-router";
import type { MetaFunction } from "react-router";
import { MutationCache, QueryCache, QueryClient, QueryClientProvider } from "@tanstack/react-query";
import "./index.css";
import { ApiError } from "@voteapp/api-client";
import { RouteError } from "./components/RouteError";
import { captureMonitoredError } from "./lib/errorMonitoring";

export const meta: MetaFunction = () => [
  { title: "VoteApp" },
  {
    name: "description",
    content:
      "Enter your address to see the elections on your ballot, who is running, and independent AI-assisted research on every candidate.",
  },
];

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

const queryClient = new QueryClient({
  queryCache: new QueryCache({ onError: reportServerError }),
  mutationCache: new MutationCache({ onError: reportServerError }),
  defaultOptions: {
    queries: {
      retry: 1,
      staleTime: 60_000,
    },
  },
});

function Providers({ children }: { children: ReactNode }) {
  return <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>;
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
