import { runElectionsValidator } from "../pipeline/validators/electionsValidator.js";
import {
  parseOptionalStringFlag,
  parsePositiveIntegerFlag,
} from "./electionsWorkerCliArgs.js";

import { assertKnownCliFlags } from "./manualCliFlags.js";
async function main(): Promise<void> {
  assertKnownCliFlags("elections:validate", process.argv.slice(2), [{ name: "--once", value: "none" }, { name: "--batch-size", value: "equals" }, { name: "--block-ms", value: "equals" }, { name: "--ingest-key", value: "both" }]);
  const once = process.argv.includes("--once");
  const batchSize = parsePositiveIntegerFlag(process.argv, "--batch-size", 25);
  const blockMs = parsePositiveIntegerFlag(process.argv, "--block-ms", 5000);
  // Targeted mode: validate one staging row by ingest_key without touching the
  // pending stream (recovery path when stale backlog starves --once batches).
  const ingestKey = parseOptionalStringFlag(process.argv, "--ingest-key");
  await runElectionsValidator({ once, batchSize, blockMs, ingestKey });
}

main().catch((error) => {
  const message = error instanceof Error ? error.message : String(error);
  console.error("elections validator failed:", message);
  process.exitCode = 1;
});
