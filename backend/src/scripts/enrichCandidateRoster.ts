import {
  runCandidateRosterEnricher,
  runCandidateRosterEnricherForElection,
} from "../pipeline/enrichers/candidateRosterEnricher.js";
import { loadProjectEnv } from "../config/env.js";
import { assertKnownCliFlags } from "./manualCliFlags.js";
import { requireLocalDatabaseTarget } from "./localDatabaseGuard.js";

const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;

function parseNumberFlag(prefix: string, fallback: number): number {
  const arg = process.argv.find((token) => token.startsWith(`${prefix}=`));
  if (!arg) {
    return fallback;
  }
  const value = Number.parseInt(arg.slice(prefix.length + 1), 10);
  return Number.isFinite(value) && value > 0 ? value : fallback;
}

function parseStringFlag(prefix: string): string | null {
  const arg = process.argv.find((token) => token.startsWith(`${prefix}=`));
  return arg ? arg.slice(prefix.length + 1) : null;
}

async function main(): Promise<void> {
  assertKnownCliFlags("candidates:roster:enrich", process.argv.slice(2), [
    { name: "--once", value: "none" },
    { name: "--batch-size", value: "equals" },
    { name: "--block-ms", value: "equals" },
    { name: "--election-id", value: "equals" },
  ]);

  const electionIdRaw = parseStringFlag("--election-id");
  if (electionIdRaw !== null) {
    // Targeted mode processes exactly one election and never reads the shared
    // draft stream, so the worker pacing flags have no effect here; rejecting
    // them keeps a mixed invocation from silently ignoring what it was asked.
    const conflicting = ["--once", "--batch-size", "--block-ms"].filter((name) =>
      process.argv.some((token) => token === name || token.startsWith(`${name}=`))
    );
    if (conflicting.length > 0) {
      throw new Error(
        `--election-id runs a single targeted enrichment (no stream consumption); it cannot be combined with ${conflicting.join(", ")}`
      );
    }

    const electionId = electionIdRaw.trim().toLowerCase();
    if (!UUID_PATTERN.test(electionId)) {
      throw new Error(`--election-id must be a UUID, got "${electionIdRaw}"`);
    }

    // Targeted runs are a manual-research op; hold them to the same
    // local-database bar as the manual:* wrappers. The queue worker path
    // below is unchanged and keeps running against whatever the deployment
    // configures.
    loadProjectEnv();
    requireLocalDatabaseTarget(process.env.DATABASE_URL ?? "postgresql://localhost:5432/voteapp");

    const result = await runCandidateRosterEnricherForElection(electionId);
    console.log(JSON.stringify({ electionId, ...result }, null, 2));
    return;
  }

  const once = process.argv.includes("--once");
  const batchSize = parseNumberFlag("--batch-size", 25);
  const blockMs = parseNumberFlag("--block-ms", 5000);
  await runCandidateRosterEnricher({ once, batchSize, blockMs });
}

main().catch((error) => {
  const message = error instanceof Error ? error.message : String(error);
  console.error("candidate-roster enricher failed:", message);
  process.exitCode = 1;
});
