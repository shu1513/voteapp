import { runStateResourcesProducer } from "../pipeline/producers/stateResourcesProducer.js";

const dryRun = process.argv.includes("--dry-run");

runStateResourcesProducer({ dryRun }).catch((error) => {
  console.error("state_resources producer failed:", error);
  process.exit(1);
});
