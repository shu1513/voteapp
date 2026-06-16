import { isPresidentialElectionsEnabled } from "../config/featureFlags.js";
import { upsertRecurringPresidentialPrimaryDateResearchJobs } from "../scheduler/presidentialPrimaryDateResearchScheduler.js";

async function main(): Promise<void> {
  const enabled = isPresidentialElectionsEnabled();
  const result = await upsertRecurringPresidentialPrimaryDateResearchJobs();
  if (!enabled) {
    console.log("presidential primary date research scheduler disabled; scheduler cleanup completed");
    return;
  }

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
