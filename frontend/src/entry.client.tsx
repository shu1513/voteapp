import { StrictMode, startTransition } from "react";
import { hydrateRoot } from "react-dom/client";
import { HydratedRouter } from "react-router/dom";
import { configureApi } from "@voteapp/api-client";
import { initErrorMonitoring } from "./lib/errorMonitoring";

initErrorMonitoring();

// Optional build-time override for the browser API timeout, mirroring the
// SSR loader's API_LOADER_TIMEOUT_MS. Unset in production since 2026-08-21
// (the API left the cold-starting free tier), so the 15s client default
// applies; the hook stays for deployments that need longer. NaN (unset var)
// fails the guard, keeping the default.
const apiTimeoutOverrideMs = Number(import.meta.env.VITE_API_TIMEOUT_MS);
if (apiTimeoutOverrideMs > 0) {
  configureApi({ requestTimeoutMs: apiTimeoutOverrideMs });
}

startTransition(() => {
  hydrateRoot(
    document,
    <StrictMode>
      <HydratedRouter />
    </StrictMode>
  );
});
