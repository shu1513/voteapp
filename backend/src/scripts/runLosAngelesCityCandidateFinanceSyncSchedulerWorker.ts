import { createLosAngelesCityFinanceWorker } from "../scheduler/losAngelesCityCandidateFinanceSyncScheduler.js";
const worker = createLosAngelesCityFinanceWorker();
const close = async () => {
  await worker.close();
  process.exit(0);
};
process.on("SIGINT", close);
process.on("SIGTERM", close);
console.log("Los Angeles City candidate-finance worker started");
