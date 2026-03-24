import { runStateResourcesValidator } from "../pipeline/validators/stateResourcesValidator.js";

const once = process.argv.includes("--once");

runStateResourcesValidator({ once }).catch((error) => {
  console.error("state_resources validator failed:", error);
  process.exit(1);
});
