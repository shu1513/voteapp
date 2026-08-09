import { createSanFranciscoFinanceWorker } from "../scheduler/sanFranciscoCandidateFinanceSyncScheduler.js";
const worker = createSanFranciscoFinanceWorker();
const close = async () => {
  await worker.close();
  process.exit(0);
};
process.on("SIGINT", close);
process.on("SIGTERM", close);
console.log("San Francisco candidate-finance worker started");
