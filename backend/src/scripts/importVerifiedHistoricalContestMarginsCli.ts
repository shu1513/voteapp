import {
  listVerifiedHistoricalContestSourcePresets,
  VERIFIED_HISTORICAL_CONTEST_SOURCE_BY_PRESET,
  type VerifiedHistoricalContestSourcePreset,
} from "../pipeline/competitiveness/historicalContestSources.js";

export type VerifiedHistoricalContestMarginImportArgs = {
  dryRun: boolean;
  preset: VerifiedHistoricalContestSourcePreset | null;
};

export function parseVerifiedHistoricalContestMarginImportArgs(
  args: readonly string[]
): VerifiedHistoricalContestMarginImportArgs {
  let preset: VerifiedHistoricalContestSourcePreset | null = null;

  for (const arg of args) {
    if (arg === "--dry-run") {
      continue;
    }

    if (arg.startsWith("--preset=")) {
      const value = arg.slice("--preset=".length).trim();
      if (preset !== null) {
        throw new Error("Provide at most one verified historical contest import preset");
      }
      if (!Object.prototype.hasOwnProperty.call(VERIFIED_HISTORICAL_CONTEST_SOURCE_BY_PRESET, value)) {
        throw new Error(
          `Unknown verified historical contest import preset: ${value}. ` +
            `Known presets: ${listVerifiedHistoricalContestSourcePresets().join(", ")}`
        );
      }
      preset = value as VerifiedHistoricalContestSourcePreset;
      continue;
    }

    throw new Error(`Unknown verified historical contest import argument: ${arg}`);
  }

  return {
    dryRun: args.includes("--dry-run"),
    preset,
  };
}
