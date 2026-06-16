import { isPresidentialElectionsEnabled } from "../config/featureFlags.js";
import { upsertRecurringPresidentialPrimaryDateResearchJobs } from "../scheduler/presidentialPrimaryDateResearchScheduler.js";

async function main(): Promise<void> {
  if (!isPresidentialElectionsEnabled()) {
    console.log("presidential primary date research scheduler disabled; no scheduler upserted");
    return;
  }

  const result = await upsertRecurringPresidentialPrimaryDateResearchJobs();
  console.log(
    [
      "presidential primary date research scheduler synced",
      `mode=${result.state.mode}`,
      `daily=${result.dailyScheduler}`,
      `activation=${result.activationJob}`,
      `activation_scheduled_for=${result.activationScheduledFor ?? "none"}`,
      `missing_rows=${result.state.missingStatePartyRowCount}`,
    ].join(" ")
  );
}

main().catch((error) => {
  console.error("presidential primary date research scheduler upsert failed:", error);
  process.exit(1);
});
