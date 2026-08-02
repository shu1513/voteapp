/**
 * Parses an optional --batch-size flag for stream-worker wrapper scripts.
 * A single `--once` pass processes at most one batch (default 20), so bulk
 * staging runs pass an explicit batch size to drain everything in one pass.
 * Accepts "--batch-size 50" and "--batch-size=50".
 */
export function readBatchSizeFlag(argv: readonly string[]): number | undefined {
  const equalsForm = argv.find((token) => token.startsWith("--batch-size="));
  const spaceIndex = argv.indexOf("--batch-size");
  const raw =
    equalsForm !== undefined
      ? equalsForm.slice("--batch-size=".length)
      : spaceIndex >= 0
        ? argv[spaceIndex + 1]
        : undefined;

  if (raw === undefined) {
    return undefined;
  }

  const parsed = Number.parseInt(raw, 10);
  if (!Number.isFinite(parsed) || parsed <= 0 || String(parsed) !== raw.trim()) {
    throw new Error(`--batch-size must be a positive integer (got "${raw}")`);
  }

  return parsed;
}
