import { runElectionsValidator } from "../pipeline/validators/electionsValidator.js";

function parseNumberFlag(prefix: string, fallback: number): number {
  const arg = process.argv.find((token) => token.startsWith(`${prefix}=`));
  if (!arg) {
    return fallback;
  }
  const value = Number.parseInt(arg.slice(prefix.length + 1), 10);
  return Number.isFinite(value) && value > 0 ? value : fallback;
}

function parseStringFlag(prefix: string): string | undefined {
  const eqArg = process.argv.find((token) => token.startsWith(`${prefix}=`));
  if (eqArg) {
    const value = eqArg.slice(prefix.length + 1).trim();
    return value.length > 0 ? value : undefined;
  }
  const index = process.argv.indexOf(prefix);
  if (index >= 0) {
    const value = process.argv[index + 1]?.trim();
    return value && !value.startsWith("--") ? value : undefined;
  }
  return undefined;
}

async function main(): Promise<void> {
  const once = process.argv.includes("--once");
  const batchSize = parseNumberFlag("--batch-size", 25);
  const blockMs = parseNumberFlag("--block-ms", 5000);
  // Targeted mode: validate one staging row by ingest_key without touching the
  // pending stream (recovery path when stale backlog starves --once batches).
  const ingestKey = parseStringFlag("--ingest-key");
  await runElectionsValidator({ once, batchSize, blockMs, ingestKey });
}

main().catch((error) => {
  const message = error instanceof Error ? error.message : String(error);
  console.error("elections validator failed:", message);
  process.exitCode = 1;
});
