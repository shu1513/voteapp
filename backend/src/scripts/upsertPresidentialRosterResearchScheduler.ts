import { isPresidentialElectionsEnabled } from "../config/featureFlags.js";
import { upsertRecurringPresidentialRosterResearchJobs } from "../scheduler/presidentialRosterResearchScheduler.js";

async function main(): Promise<void> {
  const enabled = isPresidentialElectionsEnabled();
  await upsertRecurringPresidentialRosterResearchJobs({
    dryRun: process.argv.includes("--dry-run"),
    force: process.argv.includes("--force"),
  });
  console.log(
    enabled
      ? "presidential roster research recurring scheduler upserted (daily rollover)"
      : "presidential roster research recurring scheduler disabled; scheduler cleanup completed"
  );
}

main().catch((error) => {
  console.error("presidential roster research recurring scheduler upsert failed:", error);
  process.exit(1);
});
