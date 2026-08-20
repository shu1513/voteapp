import { pathToFileURL } from "node:url";

import { loadProjectEnv } from "../config/env.js";
import { isMissouriCampaignFinanceEnabled } from "../config/featureFlags.js";
import { upsertRecurringMissouriCandidateFinanceSyncJobs } from "../scheduler/missouriCandidateFinanceSyncScheduler.js";
import { parseMissouriCandidateFinanceCliArgs } from "./missouriCandidateFinanceCli.js";

async function main(): Promise<void> {
  loadProjectEnv();
  const data = parseMissouriCandidateFinanceCliArgs(process.argv.slice(2));
  await upsertRecurringMissouriCandidateFinanceSyncJobs(data);
  console.log(isMissouriCampaignFinanceEnabled() ? "Missouri campaign finance recurring scheduler upserted" : "Missouri campaign finance recurring scheduler removed");
}

if ((process.argv[1] ? pathToFileURL(process.argv[1]).href : null) === import.meta.url) {
  main().catch((error) => { console.error("Missouri finance scheduler upsert failed:", error); process.exit(1); });
}
