import { upsertRecurringPresidentialPrimaryDateResearchJobs } from "../scheduler/presidentialPrimaryDateResearchScheduler.js";

async function main(): Promise<void> {
  await upsertRecurringPresidentialPrimaryDateResearchJobs();
  console.log("presidential primary date research daily scheduler upserted");
}

main().catch((error) => {
  console.error("presidential primary date research scheduler upsert failed:", error);
  process.exit(1);
});
