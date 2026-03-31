import { runDistrictsLoader, type DistrictLoadType } from "../pipeline/loaders/districtsLoader.js";

const DISTRICT_LOAD_ORDER: readonly DistrictLoadType[] = [
  "state",
  "us_house",
  "state_upper",
  "state_lower",
  "county",
  "place",
  "school_unified",
  "school_secondary",
  "school_elementary",
];

const dryRun = process.argv.includes("--dry-run");
const continueOnError = process.argv.includes("--continue-on-error");

type LoadOutcome = {
  type: DistrictLoadType;
  ok: boolean;
  totalCandidates: number;
  inserted: number;
  updated: number;
  skipped: number;
  durationMs: number;
  errorMessage?: string;
};

async function main(): Promise<void> {
  const startedAt = Date.now();
  const outcomes: LoadOutcome[] = [];

  for (const type of DISTRICT_LOAD_ORDER) {
    const typeStartedAt = Date.now();
    console.log(`districts load starting type=${type} dryRun=${dryRun}`);

    try {
      const summary = await runDistrictsLoader({ type, dryRun });
      const durationMs = Date.now() - typeStartedAt;
      outcomes.push({
        type,
        ok: true,
        totalCandidates: summary.totalCandidates,
        inserted: summary.inserted,
        updated: summary.updated,
        skipped: summary.skipped,
        durationMs,
      });
      console.log(
        `districts load completed type=${type} dryRun=${dryRun} total=${summary.totalCandidates} inserted=${summary.inserted} updated=${summary.updated} skipped=${summary.skipped} duration_ms=${durationMs}`
      );
    } catch (error) {
      const durationMs = Date.now() - typeStartedAt;
      const errorMessage = error instanceof Error ? error.message : String(error);
      outcomes.push({
        type,
        ok: false,
        totalCandidates: 0,
        inserted: 0,
        updated: 0,
        skipped: 0,
        durationMs,
        errorMessage,
      });
      console.error(`districts load failed type=${type} dryRun=${dryRun} duration_ms=${durationMs} error=${errorMessage}`);

      if (!continueOnError) {
        break;
      }
    }
  }

  const successful = outcomes.filter((outcome) => outcome.ok);
  const failed = outcomes.filter((outcome) => !outcome.ok);

  const totalCandidates = successful.reduce((sum, outcome) => sum + outcome.totalCandidates, 0);
  const inserted = successful.reduce((sum, outcome) => sum + outcome.inserted, 0);
  const updated = successful.reduce((sum, outcome) => sum + outcome.updated, 0);
  const skipped = successful.reduce((sum, outcome) => sum + outcome.skipped, 0);
  const durationMs = Date.now() - startedAt;

  console.log(
    `districts load all summary dryRun=${dryRun} success=${successful.length} failed=${failed.length} total=${totalCandidates} inserted=${inserted} updated=${updated} skipped=${skipped} duration_ms=${durationMs}`
  );

  for (const outcome of outcomes) {
    if (outcome.ok) {
      console.log(
        `districts load all item type=${outcome.type} ok=true total=${outcome.totalCandidates} inserted=${outcome.inserted} updated=${outcome.updated} skipped=${outcome.skipped} duration_ms=${outcome.durationMs}`
      );
      continue;
    }

    console.error(
      `districts load all item type=${outcome.type} ok=false duration_ms=${outcome.durationMs} error=${outcome.errorMessage ?? "unknown"}`
    );
  }

  if (failed.length > 0) {
    process.exit(1);
  }
}

main().catch((error) => {
  console.error("districts load all failed:", error);
  process.exit(1);
});
