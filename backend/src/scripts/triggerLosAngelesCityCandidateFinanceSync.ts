import { enqueueManualLosAngelesCityFinanceSyncJob } from "../scheduler/losAngelesCityCandidateFinanceSyncScheduler.js";
const KNOWN_FLAGS = new Set(["--dry-run", "--force"]);
const args = process.argv.slice(2);
// Strict like the Ohio/Georgia trigger CLIs: a typo (--dryrun) or bare
// positional ("dry-run" after npm's own "--" separator) must fail loudly
// instead of silently enqueueing a REAL sync.
for (const arg of args) {
  if (!KNOWN_FLAGS.has(arg)) {
    throw new Error(
      `Unknown Los Angeles City candidate finance sync flag: ${arg}`,
    );
  }
}
const jobId = await enqueueManualLosAngelesCityFinanceSyncJob({
  force: args.includes("--force"),
  dryRun: args.includes("--dry-run"),
});
console.log(JSON.stringify({ job_id: jobId }));
