import {
  createPresidentialNomineeResearchWorker,
  runPresidentialNomineeResearchEnricher,
} from "../pipeline/enrichers/presidentialNomineeResearchEnricher.js";

function parseNumberFlag(prefix: string, fallback: number): number {
  const arg = process.argv.find((token) => token.startsWith(`${prefix}=`));
  if (!arg) {
    return fallback;
  }
  const value = Number.parseInt(arg.slice(prefix.length + 1), 10);
  return Number.isInteger(value) && value > 0 ? value : fallback;
}

async function main(): Promise<void> {
  const once = process.argv.includes("--once");
  const concurrency = parseNumberFlag("--concurrency", 1);

  if (once) {
    await runPresidentialNomineeResearchEnricher({
      once: true,
      blockMs: parseNumberFlag("--block-ms", 5000),
      concurrency,
    });
    return;
  }

  const worker = createPresidentialNomineeResearchWorker(concurrency);
  let shutdownPromise: Promise<void> | null = null;

  worker.on("ready", () => {
    console.log("presidential nominee research worker ready");
  });
  worker.on("active", (job) => {
    console.log(`presidential nominee research worker active jobId=${job.id} name=${job.name}`);
  });
  worker.on("completed", (job, result) => {
    console.log(`presidential nominee research worker completed jobId=${job.id} result=${JSON.stringify(result)}`);
  });
  worker.on("failed", (job, error) => {
    console.error(`presidential nominee research worker failed jobId=${job?.id ?? "unknown"}:`, error);
  });
  worker.on("error", (error) => {
    console.error("presidential nominee research worker error:", error);
  });

  const shutdown = (): Promise<void> => {
    if (shutdownPromise) {
      return shutdownPromise;
    }
    shutdownPromise = worker.close().then(
      () => {
        process.exit(0);
      },
      (error) => {
        console.error("presidential nominee research worker shutdown failed:", error);
        process.exit(1);
      }
    );
    return shutdownPromise;
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
  console.error("presidential nominee research enricher failed:", message);
  process.exitCode = 1;
});
