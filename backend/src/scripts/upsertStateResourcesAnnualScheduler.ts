import { upsertRecurringStateResourcesRefreshJobs } from "../scheduler/stateResourcesScheduler.js";

upsertRecurringStateResourcesRefreshJobs()
  .then(() => {
    console.log("state_resources recurring scheduler upserted (monthly)");
  })
  .catch((error) => {
    console.error("state_resources recurring scheduler upsert failed:", error);
    process.exit(1);
  });
