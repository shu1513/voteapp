import { upsertRecurringPresidentialRosterResearchJobs } from "../scheduler/presidentialRosterResearchScheduler.js";

upsertRecurringPresidentialRosterResearchJobs({
  dryRun: process.argv.includes("--dry-run"),
  force: process.argv.includes("--force"),
})
  .then(() => {
    console.log("presidential roster research recurring scheduler upserted (daily rollover)");
  })
  .catch((error) => {
    console.error("presidential roster research recurring scheduler upsert failed:", error);
    process.exit(1);
  });
