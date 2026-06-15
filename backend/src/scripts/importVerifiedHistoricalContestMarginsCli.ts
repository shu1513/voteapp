export type VerifiedHistoricalContestMarginImportArgs = {
  dryRun: boolean;
};

export function parseVerifiedHistoricalContestMarginImportArgs(
  args: readonly string[]
): VerifiedHistoricalContestMarginImportArgs {
  for (const arg of args) {
    if (arg !== "--dry-run") {
      throw new Error(`Unknown verified historical contest import argument: ${arg}`);
    }
  }

  return {
    dryRun: args.includes("--dry-run"),
  };
}
