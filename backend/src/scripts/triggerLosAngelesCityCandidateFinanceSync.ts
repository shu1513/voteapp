import { enqueueManualLosAngelesCityFinanceSyncJob } from "../scheduler/losAngelesCityCandidateFinanceSyncScheduler.js";
const args = new Set(process.argv.slice(2));
const jobId = await enqueueManualLosAngelesCityFinanceSyncJob({
  force: args.has("--force"),
  dryRun: args.has("--dry-run"),
  aiClassifyIndustries: args.has("--ai-classify-industries"),
});
console.log(JSON.stringify({ job_id: jobId }));
