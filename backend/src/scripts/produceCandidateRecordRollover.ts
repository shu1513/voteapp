import { runCandidateRecordRolloverProducer } from "../pipeline/producers/candidateRecordRolloverProducer.js";

async function main(): Promise<void> {
  const force = process.argv.includes("--force");
  const result = await runCandidateRecordRolloverProducer({ force });
  console.log(JSON.stringify({ type: "candidate_record_rollover", ...result }, null, 2));
}

main().catch((error) => {
  console.error("candidate_record rollover producer failed:", error);
  process.exit(1);
});
