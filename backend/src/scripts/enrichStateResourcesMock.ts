import { runStateResourcesMockEnricher } from "../pipeline/enrichers/stateResourcesMockEnricher.js";

const once = process.argv.includes("--once");

runStateResourcesMockEnricher({ once }).catch((error) => {
  console.error("state_resources mock enricher failed:", error);
  process.exit(1);
});
