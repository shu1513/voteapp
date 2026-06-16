import { isPresidentialElectionsEnabled } from "../config/featureFlags.js";
import { upsertRecurringPresidentialRosterResearchJobs } from "../scheduler/presidentialRosterResearchScheduler.js";

if (!isPresidentialElectionsEnabled()) {
  console.log("presidential roster research recurring scheduler disabled; no scheduler upserted");
  process.exit(0);
}

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
