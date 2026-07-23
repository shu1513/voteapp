// elections.sources is a jsonb array of URL strings everywhere the pipeline
// writes it. Normalize defensively so repair wrappers converge provenance
// without preserving malformed entries or whitespace-only duplicates.
export function appendElectionSource(sources: unknown, sourceUrl: string): string[] {
  const existing = Array.isArray(sources)
    ? sources
        .filter((value): value is string => typeof value === "string" && value.trim().length > 0)
        .map((value) => value.trim())
    : [];
  return [...new Set([...existing, sourceUrl.trim()])];
}
