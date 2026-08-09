import { loadProjectEnv } from "../config/env.js";
import { enqueueManualSanFranciscoFinanceSyncJob } from "../scheduler/sanFranciscoCandidateFinanceSyncScheduler.js";
const KNOWN_FLAGS = new Set(["--dry-run", "--force"]);
const args = process.argv.slice(2);
// Strict like the Ohio/Georgia trigger CLIs: a typo (--dryrun) or bare
// positional ("dry-run" after npm's own "--" separator) must fail loudly
// instead of silently enqueueing a REAL sync.
for (const arg of args) {
  if (!KNOWN_FLAGS.has(arg)) {
    throw new Error(
      `Unknown San Francisco candidate finance sync flag: ${arg}`,
    );
  }
}
// Load .env before the enqueue's flag check; without this a local run reads
// an unloaded environment and silently reports "disabled".
loadProjectEnv();
const jobId = await enqueueManualSanFranciscoFinanceSyncJob({
  force: args.includes("--force"),
  dryRun: args.includes("--dry-run"),
});
console.log(JSON.stringify({ job_id: jobId }));
