import { loadProjectEnv } from "../config/env.js";
import { createNewYorkCityFinanceSyncWorker } from "../scheduler/newYorkCityCandidateFinanceSyncScheduler.js";

loadProjectEnv();
const worker = createNewYorkCityFinanceSyncWorker();
console.log("NYC candidate finance scheduler worker started");

async function shutdown(): Promise<void> {
  await worker.close();
  process.exit(0);
}
process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
