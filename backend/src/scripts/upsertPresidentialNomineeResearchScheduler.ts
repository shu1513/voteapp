import { upsertRecurringPresidentialNomineeResearchJobs } from "../scheduler/presidentialNomineeResearchScheduler.js";

upsertRecurringPresidentialNomineeResearchJobs({
  dryRun: process.argv.includes("--dry-run"),
  force: process.argv.includes("--force"),
})
  .then(() => {
    console.log("presidential nominee research recurring scheduler upserted (daily rollover)");
  })
  .catch((error) => {
    console.error("presidential nominee research recurring scheduler upsert failed:", error);
    process.exit(1);
  });
