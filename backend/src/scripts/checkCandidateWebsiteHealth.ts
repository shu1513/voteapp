import { getPipelineEnv } from "../config/env.js";
import { runCandidateWebsiteHealthProducer } from "../pipeline/candidates/candidateWebsiteHealthProducer.js";
import { assertKnownCliFlags } from "./manualCliFlags.js";
import { requireLocalDatabaseTarget } from "./localDatabaseGuard.js";

// Direct-run sweep over candidates.official_website_url (no scheduler): checks
// reachability, records outcomes in source_url_health, and — only when
// CANDIDATE_WEBSITE_HEALTH_RETIRE_ENABLED is on and enough confirmed hard
// failures have accumulated — archives dead URLs into former_website_urls.
// dry-run still performs HTTP checks; it only skips persistence/retirement.

function readLimitArg(argv: readonly string[]): number | undefined {
  const flag = argv.find((arg) => arg.startsWith("--limit="));
  if (!flag) {
    return undefined;
  }
  const raw = flag.slice("--limit=".length);
  if (!/^[1-9]\d*$/.test(raw)) {
    throw new Error(`Invalid --limit value: ${raw}`);
  }
  return Number.parseInt(raw, 10);
}

const argv = process.argv.slice(2);
assertKnownCliFlags("candidates:website-health:check", argv, [
  { name: "--dry-run", value: "none" },
  { name: "--force", value: "none" },
  { name: "--limit", value: "equals" },
]);
const dryRun = argv.includes("--dry-run");
const force = argv.includes("--force");
const maxUrlsOverride = readLimitArg(argv);

requireLocalDatabaseTarget(getPipelineEnv().DATABASE_URL);

runCandidateWebsiteHealthProducer({
  dryRun,
  force,
  ...(maxUrlsOverride !== undefined ? { maxUrlsOverride } : {}),
})
  .then((result) => {
    const { off_domain_redirects, hard_fail_urls, deep_link_root_alive, retired, ...summary } =
      result;
    console.log(JSON.stringify(summary, null, 2));
    if (hard_fail_urls.length > 0) {
      console.log(`hard-fail URLs this run (${hard_fail_urls.length}):`);
      for (const url of hard_fail_urls) {
        console.log(`  ${url}`);
      }
    }
    if (deep_link_root_alive.length > 0) {
      console.log(
        `dead subpages whose site root is alive — suggested trims (${deep_link_root_alive.length}):`
      );
      for (const entry of deep_link_root_alive) {
        console.log(`  ${entry.url} -> ${entry.rootUrl}`);
      }
    }
    if (off_domain_redirects.length > 0) {
      console.log(`off-domain redirects (report-only, ${off_domain_redirects.length}):`);
      for (const redirect of off_domain_redirects) {
        console.log(`  ${redirect.url} -> ${redirect.finalUrl}`);
      }
    }
    if (retired.length > 0) {
      console.log(`retired websites (${retired.length}):`);
      for (const entry of retired) {
        console.log(`  ${entry.displayName ?? entry.candidateId}: ${entry.url}`);
      }
    }
  })
  .catch((error) => {
    console.error("candidate_website_health check failed:", error);
    process.exitCode = 1;
  });
