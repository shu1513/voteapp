import { runCandidateRosterRolloverProducer } from "../pipeline/producers/candidateRosterRolloverProducer.js";

function parseFlag(name: string): boolean {
  return process.argv.includes(name);
}

async function main(): Promise<void> {
  const force = parseFlag("--force");
  const result = await runCandidateRosterRolloverProducer({ force });
  console.log(result);
}

main().catch((error) => {
  const message = error instanceof Error ? error.message : String(error);
  console.error("candidate-roster rollover producer failed:", message);
  process.exitCode = 1;
});

