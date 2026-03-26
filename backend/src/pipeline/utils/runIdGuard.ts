/**
 * Normalizes run identifiers from stream messages and DB rows.
 */
export function normalizeRunId(value: string | null | undefined): string | null {
  if (typeof value !== "string") {
    return null;
  }

  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : null;
}

/**
 * Returns true when message run_id is present and does not match the current row run_id.
 * Missing message run_id is treated as legacy/unversioned and does not block processing.
 */
export function hasRunIdMismatch(messageRunIdRaw: string | undefined, rowRunIdRaw: string | null): boolean {
  const messageRunId = normalizeRunId(messageRunIdRaw);
  if (!messageRunId) {
    return false;
  }

  const rowRunId = normalizeRunId(rowRunIdRaw);
  return rowRunId !== messageRunId;
}
