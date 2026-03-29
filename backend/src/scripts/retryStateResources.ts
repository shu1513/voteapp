import { runStateResourcesRetrySweeper } from "../pipeline/retries/stateResourcesRetry.js";

function readPositiveIntegerArg(prefix: string): number | undefined {
  const arg = process.argv.find((value) => value.startsWith(prefix));
  if (!arg) {
    return undefined;
  }

  const raw = arg.slice(prefix.length).trim();
  if (!/^\d+$/.test(raw)) {
    throw new Error(`Invalid ${prefix} value: ${raw}. Expected a positive integer.`);
  }
  const parsed = Number.parseInt(raw, 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new Error(`Invalid ${prefix} value: ${raw}. Expected a positive integer.`);
  }
  return parsed;
}

async function main(): Promise<void> {
  const maxItems = readPositiveIntegerArg("--max-items=");

  const result = await runStateResourcesRetrySweeper({ maxItems });

  console.log(
    `state_resources retry sweep completed. scanned=${result.scanned} requeued_to_draft=${result.requeuedToDraft} requeued_to_pending=${result.requeuedToPending} skipped=${result.skipped} failed=${result.failed}`
  );
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exit(1);
});
