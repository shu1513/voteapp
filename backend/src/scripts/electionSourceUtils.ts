export type ElectionSourceMergeResult = {
  sources: string[];
  appended: boolean;
};

// elections.sources is a jsonb array of URL strings everywhere the pipeline
// writes it. Normalize defensively so repair wrappers converge provenance
// without preserving malformed entries or whitespace-only duplicates.
export function mergeElectionSource(
  sources: unknown,
  sourceUrl: string
): ElectionSourceMergeResult {
  const existing = Array.isArray(sources)
    ? sources
        .filter((value): value is string => typeof value === "string" && value.trim().length > 0)
        .map((value) => value.trim())
    : [];
  const normalizedSourceUrl = sourceUrl.trim();
  return {
    sources: [...new Set([...existing, normalizedSourceUrl])],
    appended: !existing.includes(normalizedSourceUrl),
  };
}

// Keep the original array-returning API for existing callers and the
// compatibility re-export from correctManualElectionDate.ts.
export function appendElectionSource(sources: unknown, sourceUrl: string): string[] {
  return mergeElectionSource(sources, sourceUrl).sources;
}
