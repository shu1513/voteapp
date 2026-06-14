import { runPresidentialNomineeResearchProducer } from "../pipeline/producers/presidentialNomineeResearchProducer.js";

function parseNumberFlag(prefix: string): number | undefined {
  const arg = process.argv.find((token) => token.startsWith(`${prefix}=`));
  if (!arg) {
    return undefined;
  }
  const value = Number.parseInt(arg.slice(prefix.length + 1), 10);
  if (!Number.isInteger(value) || value <= 0) {
    throw new Error(`Invalid ${prefix} value: ${arg}`);
  }
  return value;
}

async function main(): Promise<void> {
  const result = await runPresidentialNomineeResearchProducer({
    dryRun: process.argv.includes("--dry-run"),
    force: process.argv.includes("--force"),
    maxCyclesPerRun: parseNumberFlag("--max-cycles-per-run"),
  });

  console.log(JSON.stringify({ type: "presidential_nominee_research_producer", ...result }, null, 2));
}

main().catch((error) => {
  const message = error instanceof Error ? error.message : String(error);
  console.error("presidential nominee research producer failed:", message);
  process.exitCode = 1;
});
