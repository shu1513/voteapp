/**
 * Same-identity manual-link protection (`M` in
 * docs/finance-module-capability-matrix.md), shared by every finance link
 * upsert.
 *
 * A `link_source = 'manual'` row is an operator's decision. Automation (bulk
 * syncs and auto-linkers, whose incoming link_source is anything else) that
 * hits the same (candidate, election, identity) row must not reclassify it
 * or flip its status: auto-link selects on "no ACTIVE link", so an unguarded
 * upsert would resurrect an operator-disabled row as active/<bulk> and erase
 * the provenance. A deliberate manual write (incoming link_source =
 * 'manual') applies in full, including status changes in both directions.
 * Metadata columns (names, office, district, source_url, last_verified_at)
 * may be refreshed by automation either way.
 */

const IDENTIFIER_PATTERN = /^[a-z][a-z0-9_]*$/;

/**
 * The `link_status = …, link_source = …` assignments for an
 * `ON CONFLICT … DO UPDATE SET` list. `table` is the links table name as it
 * appears in the statement (unqualified — Postgres resolves the target
 * table's columns by that name).
 */
export function manualProtectedLinkAssignments(table: string): string {
  if (!IDENTIFIER_PATTERN.test(table)) {
    throw new Error(`Invalid finance links table identifier: ${table}`);
  }
  const automaticOverManual = `${table}.link_source = 'manual' AND EXCLUDED.link_source <> 'manual'`;
  return `link_status = CASE
          WHEN ${automaticOverManual} THEN ${table}.link_status
          ELSE EXCLUDED.link_status
        END,
        link_source = CASE
          WHEN ${automaticOverManual} THEN ${table}.link_source
          ELSE EXCLUDED.link_source
        END`;
}

/**
 * WHERE fragment for the "retire other active identities" step some writers
 * run before inserting an active link: automation never retires an
 * operator's manual choice; a manual write may. `sourceParam` is the bound
 * parameter carrying the incoming link_source (e.g. `$4`).
 */
export function manualProtectedRetireCondition(sourceParam: string): string {
  if (!/^\$\d+$/.test(sourceParam)) {
    throw new Error(`Invalid retire-condition parameter: ${sourceParam}`);
  }
  return `(link_source IS DISTINCT FROM 'manual' OR ${sourceParam} = 'manual')`;
}

/** Columns the link upsert must RETURN for assertLinkWriteNotBlocked. */
export const MANUAL_PROTECTED_LINK_RETURNING = "id, link_status, link_source";

export type ManualProtectedLinkRow = {
  id: string;
  link_status?: string | null;
  link_source?: string | null;
};

/**
 * After an automatic upsert, fail closed when the guarded row turned out to
 * be an operator-DISABLED manual link: the identity automation keeps
 * proposing is one an operator rejected, so the sync must not proceed to
 * write a snapshot against it or count it as linked. Inside a snapshot
 * transaction the throw rolls the whole snapshot back. An active manual row
 * is fine — automation reuses it (status and source preserved by the CASE).
 */
export function assertLinkWriteNotBlocked(
  label: string,
  row: ManualProtectedLinkRow | undefined,
  incomingLinkSource: string
): void {
  if (!row || incomingLinkSource === "manual") {
    return;
  }
  if (row.link_source === "manual" && row.link_status !== undefined && row.link_status !== null && row.link_status !== "active") {
    throw new Error(`${label} automatic finance link matches an operator-disabled manual link`);
  }
}
