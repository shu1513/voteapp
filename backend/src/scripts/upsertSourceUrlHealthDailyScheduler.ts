import { upsertRecurringSourceUrlHealthJobs } from "../scheduler/sourceUrlHealthScheduler.js";

upsertRecurringSourceUrlHealthJobs()
  .then(() => {
    console.log("source_url_health recurring scheduler upserted (daily)");
  })
  .catch((error) => {
    console.error("source_url_health recurring scheduler upsert failed:", error);
    process.exitCode = 1;
  });

