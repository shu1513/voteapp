import { upsertRecurringElectionsSearchJobs } from "../scheduler/electionsSearchScheduler.js";

upsertRecurringElectionsSearchJobs()
  .then(() => {
    console.log("elections_search recurring scheduler upserted (daily)");
  })
  .catch((error) => {
    console.error("elections_search recurring scheduler upsert failed:", error);
    process.exit(1);
  });

