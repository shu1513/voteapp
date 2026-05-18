import { runElectionsEnricher } from "../pipeline/enrichers/electionsEnricher.js";

function parseNumberFlag(prefix: string, fallback: number): number {
  const arg = process.argv.find((token) => token.startsWith(`${prefix}=`));
  if (!arg) {
    return fallback;
  }
  const value = Number.parseInt(arg.slice(prefix.length + 1), 10);
  return Number.isFinite(value) && value > 0 ? value : fallback;
}

async function main(): Promise<void> {
  const once = process.argv.includes("--once");
  const batchSize = parseNumberFlag("--batch-size", 25);
  const blockMs = parseNumberFlag("--block-ms", 5000);
  await runElectionsEnricher({ once, batchSize, blockMs });
}

main().catch((error) => {
  const message = error instanceof Error ? error.message : String(error);
  console.error("elections enricher failed:", message);
  process.exitCode = 1;
});
