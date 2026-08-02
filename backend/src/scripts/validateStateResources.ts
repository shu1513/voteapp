import { runStateResourcesValidator } from "../pipeline/validators/stateResourcesValidator.js";
import { readBatchSizeFlag } from "./streamWorkerFlags.js";

const once = process.argv.includes("--once");
const batchSize = readBatchSizeFlag(process.argv);

runStateResourcesValidator({ once, ...(batchSize !== undefined ? { batchSize } : {}) }).catch((error) => {
  console.error("state_resources validator failed:", error);
  process.exit(1);
});
