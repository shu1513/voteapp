/**
 * Snapshot writers open their own transaction on a Pool. Given a PoolClient
 * instead, the transaction helpers would either issue BEGIN/COMMIT on the
 * caller's client — committing whatever transaction the caller had open —
 * or call connect() on an already-connected client. Neither is a supported
 * composition: a caller that needs a snapshot inside its own transaction
 * has to use the link-upsert / query-only helpers, which do accept a
 * transaction client. Fail closed before any statement is issued.
 */
export function assertSnapshotDbIsNotPoolClient(label: string, db: unknown): void {
  if (typeof (db as { release?: unknown } | null)?.release === "function") {
    throw new Error(`${label} finance snapshot writes must receive a Pool, not a PoolClient`);
  }
}
