import {
  createElectionResultSearchWorker,
  runElectionResultsEnricher,
} from "../pipeline/enrichers/electionResultsEnricher.js";

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
  const concurrency = parseNumberFlag("--concurrency", 1);
  if (once) {
    await runElectionResultsEnricher({
      once: true,
      blockMs: parseNumberFlag("--block-ms", 5000),
      concurrency,
    });
    return;
  }

  const worker = createElectionResultSearchWorker(concurrency);
  worker.on("ready", () => {
    console.log("election_result search worker ready");
  });
  worker.on("active", (job) => {
    console.log(`election_result search worker active jobId=${job.id} name=${job.name}`);
  });
  worker.on("completed", (job, result) => {
    console.log(`election_result search worker completed jobId=${job.id} result=${JSON.stringify(result)}`);
  });
  worker.on("failed", (job, error) => {
    console.error(`election_result search worker failed jobId=${job?.id ?? "unknown"}:`, error);
  });
  worker.on("error", (error) => {
    console.error("election_result search worker error:", error);
  });

  const shutdown = async (): Promise<void> => {
    try {
      await worker.close();
      process.exit(0);
    } catch (error) {
      console.error("election_result search worker shutdown failed:", error);
      process.exit(1);
    }
  };
  process.on("SIGINT", () => {
    void shutdown();
  });
  process.on("SIGTERM", () => {
    void shutdown();
  });
}

main().catch((error) => {
  const message = error instanceof Error ? error.message : String(error);
  console.error("election_result enricher failed:", message);
  process.exitCode = 1;
});
