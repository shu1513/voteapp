import { runElectionsProducer } from "../pipeline/producers/electionsProducer.js";

function parseFlag(name: string): boolean {
  return process.argv.includes(name);
}

async function main(): Promise<void> {
  const dryRun = parseFlag("--dry-run");
  const force = parseFlag("--force");
  const result = await runElectionsProducer({ dryRun, force });
  console.log(result);
}

main().catch((error) => {
  const message = error instanceof Error ? error.message : String(error);
  console.error("elections producer failed:", message);
  process.exitCode = 1;
});
