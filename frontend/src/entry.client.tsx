import { StrictMode, startTransition } from "react";
import { hydrateRoot } from "react-dom/client";
import { HydratedRouter } from "react-router/dom";
import { configureApi } from "@voteapp/api-client";
import { initErrorMonitoring } from "./lib/errorMonitoring";

initErrorMonitoring();

// Build-time override for the browser API timeout, mirroring the SSR
// loader's API_LOADER_TIMEOUT_MS: while the API runs on a cold-starting
// free instance (~1 minute to wake), address search and auth calls need to
// ride out the wake instead of failing at the 15s default. NaN (unset var)
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
