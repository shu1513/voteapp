import { upsertRecurringPresidentialPrimaryDateResearchJobs } from "../scheduler/presidentialPrimaryDateResearchScheduler.js";

async function main(): Promise<void> {
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
