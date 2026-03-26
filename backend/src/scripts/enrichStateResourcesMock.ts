import { runStateResourcesMockEnricher } from "../pipeline/enrichers/stateResourcesMockEnricher.js";

const once = process.argv.includes("--once");

console.warn(
  "[MOCK MODE] state-resources:enrich-mock writes placeholder summaries. Use state-resources:enrich for state-specific researched content."
);

runStateResourcesMockEnricher({ once }).catch((error) => {
  console.error("state_resources mock enricher failed:", error);
  process.exit(1);
});
