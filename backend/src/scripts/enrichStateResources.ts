import { runStateResourcesEnricher } from "../pipeline/enrichers/stateResourcesEnricher.js";

const once = process.argv.includes("--once");

runStateResourcesEnricher({ once }).catch((error) => {
  console.error("state_resources enricher failed:", error);
  process.exitCode = 1;
});
