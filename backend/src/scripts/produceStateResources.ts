import { runStateResourcesProducer } from "../pipeline/producers/stateResourcesProducer.js";

const dryRun = process.argv.includes("--dry-run");
const force = process.argv.includes("--force");

runStateResourcesProducer({ dryRun, force }).catch((error) => {
  console.error("state_resources producer failed:", error);
  process.exit(1);
});
