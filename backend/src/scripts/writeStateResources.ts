import { runStateResourcesWriter } from "../pipeline/writers/stateResourcesWriter.js";

const once = process.argv.includes("--once");

runStateResourcesWriter({ once }).catch((error) => {
  console.error("state_resources writer failed:", error);
  process.exit(1);
});
