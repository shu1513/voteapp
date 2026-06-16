import { isPresidentialElectionsEnabled } from "../config/featureFlags.js";
import { upsertRecurringPresidentialNomineeResearchJobs } from "../scheduler/presidentialNomineeResearchScheduler.js";

async function main(): Promise<void> {
  const enabled = isPresidentialElectionsEnabled();
  await upsertRecurringPresidentialNomineeResearchJobs({
    dryRun: process.argv.includes("--dry-run"),
    force: process.argv.includes("--force"),
  });
  console.log(
    enabled
      ? "presidential nominee research recurring scheduler upserted (daily rollover)"
      : "presidential nominee research recurring scheduler disabled; scheduler cleanup completed"
  );
}

main().catch((error) => {
  console.error("presidential nominee research recurring scheduler upsert failed:", error);
  process.exit(1);
});
