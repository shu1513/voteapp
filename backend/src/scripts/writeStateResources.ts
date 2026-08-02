import { runStateResourcesWriter } from "../pipeline/writers/stateResourcesWriter.js";
import { readBatchSizeFlag } from "./streamWorkerFlags.js";

const once = process.argv.includes("--once");
const batchSize = readBatchSizeFlag(process.argv);

runStateResourcesWriter({ once, ...(batchSize !== undefined ? { batchSize } : {}) }).catch((error) => {
  console.error("state_resources writer failed:", error);
  process.exit(1);
});
