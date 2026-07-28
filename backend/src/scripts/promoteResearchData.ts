import { readdir, writeFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

import { parse as parsePostgresConnectionString } from "pg-connection-string";
import { Pool, type PoolClient } from "pg";

import { loadProjectEnv } from "../config/env.js";
import { assertKnownCliFlags } from "./manualCliFlags.js";

// Promotes manually researched rows from the local database to another
// database (production), inserting what is missing and updating what changed.
// Never deletes, never touches user/account tables, and is safe to re-run: a
// second consecutive run must be a pure no-op.
//
// This is deliberately NOT built on the manual research writers. Those are
// correct for research ingestion and wrong for promotion:
//   - candidateRecordStore fuzzy-matches records by description similarity
//     (0.86); promotion identity must be the database's declared natural key.
//   - writeManualCandidateRecords creates follow-notification events; promoted
//     rows are a backfill and must never notify anyone.
//   - the same writer deletes stale tags; promotion never deletes.

/** Connection details safe to print — a DSN carries a password, so never log one. */
export type EndpointFingerprint = {
  host: string;
  port: number;
  database: string;
  user: string;
};

export function describeEndpoint(fingerprint: EndpointFingerprint): string {
  return `${fingerprint.user}@${fingerprint.host}:${fingerprint.port}/${fingerprint.database}`;
}

/**
 * Parses a postgres URL into its effective connection target. Uses
 * pg-connection-string rather than URL parsing alone because libpq accepts a
 * `?host=` parameter that overrides the URL authority — the same reason
 * localDatabaseGuard parses it this way.
 */
export function parseEndpoint(label: string, databaseUrl: string): EndpointFingerprint {
  const trimmed = databaseUrl.trim();
  if (trimmed.length === 0) {
    throw new Error(`${label} is required`);
  }

  let protocol: string;
  try {
    protocol = new URL(trimmed).protocol;
  } catch {
    throw new Error(`${label} must be a postgres:// or postgresql:// URL`);
  }
  if (protocol !== "postgres:" && protocol !== "postgresql:") {
    throw new Error(`${label} has unsupported protocol ${protocol}`);
  }

  const parsed = parsePostgresConnectionString(trimmed);
  const host = String(parsed.host ?? "").trim().toLowerCase();
  const database = String(parsed.database ?? "").trim();
  if (host.length === 0) {
    throw new Error(`${label} does not name a host`);
  }
  if (database.length === 0) {
    throw new Error(`${label} does not name a database`);
  }

  return {
    host,
    // libpq's default; pg uses the same when the URL omits a port.
    port: Number(parsed.port ?? 5432) || 5432,
    database,
    user: String(parsed.user ?? "").trim(),
  };
}

const LOCAL_HOSTS = new Set(["localhost", "127.0.0.1", "::1", "[::1]"]);

export function isLocalHost(host: string): boolean {
  const normalized = host.trim().toLowerCase();
  return normalized.startsWith("/") || LOCAL_HOSTS.has(normalized);
}

export type PromotionEndpoints = {
  source: EndpointFingerprint;
  target: EndpointFingerprint;
};

/**
 * Validates the endpoint pair before any connection is opened.
 *
 * Deliberately does NOT require the target to be remote: rehearsing a
 * promotion against a local scratch database is good practice and is how the
 * integration test runs. The human gate is --confirm-target, not host shape.
 */
export function assertPromotionEndpoints(input: {
  sourceUrl: string;
  targetUrl: string;
  env?: NodeJS.ProcessEnv;
}): PromotionEndpoints {
  const env = input.env ?? process.env;

  // Fail closed. A shell configured to bypass the manual-writer localhost
  // guard must not silently also relax this command, which writes to
  // production by design.
  if (env.ALLOW_REMOTE_DB_WRITES !== undefined && env.ALLOW_REMOTE_DB_WRITES.trim().length > 0) {
    throw new Error(
      "Refusing to run with ALLOW_REMOTE_DB_WRITES set. That variable exists to relax the " +
        "manual-writer localhost guard; this command has its own endpoint checks and must not " +
        "inherit a loosened shell. Unset it and re-run."
    );
  }

  const source = parseEndpoint("source database URL", input.sourceUrl);
  const target = parseEndpoint("target database URL", input.targetUrl);

  if (!isLocalHost(source.host)) {
    throw new Error(
      `Refusing to promote FROM a non-local source (${source.host}). The source is the local ` +
        "research database; promoting out of a remote database is not what this tool does."
    );
  }

  if (
    source.host === target.host &&
    source.port === target.port &&
    source.database === target.database
  ) {
    throw new Error(
      `Refusing to promote a database into itself (${describeEndpoint(source)}).`
    );
  }

  return { source, target };
}

// ---------------------------------------------------------------------------
// Diff
// ---------------------------------------------------------------------------

/**
 * A row's classification against the target. `unchanged` rows must issue no
 * UPDATE at all: both record tables carry an updated_at trigger, so an
 * unconditional ON CONFLICT DO UPDATE would rewrite every row on every run and
 * destroy the "second run is a no-op" property this tool is built around.
 */
export type RowPlan<T> = {
  inserts: T[];
  updates: T[];
  unchangedCount: number;
  /** Rows present on the target and absent from source. Reported, never deleted. */
  targetOnlyCount: number;
};

export function planRows<T>(input: {
  sourceRows: readonly T[];
  targetRows: readonly T[];
  keyOf: (row: T) => string;
  isEqual: (source: T, target: T) => boolean;
}): RowPlan<T> {
  const targetByKey = new Map<string, T>();
  for (const row of input.targetRows) {
    targetByKey.set(input.keyOf(row), row);
  }

  const inserts: T[] = [];
  const updates: T[] = [];
  let unchangedCount = 0;
  const seenSourceKeys = new Set<string>();

  for (const sourceRow of input.sourceRows) {
    const key = input.keyOf(sourceRow);
    seenSourceKeys.add(key);
    const targetRow = targetByKey.get(key);
    if (targetRow === undefined) {
      inserts.push(sourceRow);
    } else if (input.isEqual(sourceRow, targetRow)) {
      unchangedCount += 1;
    } else {
      updates.push(sourceRow);
    }
  }

  let targetOnlyCount = 0;
  for (const key of targetByKey.keys()) {
    if (!seenSourceKeys.has(key)) {
      targetOnlyCount += 1;
    }
  }

  return { inserts, updates, unchangedCount, targetOnlyCount };
}

/** Null-safe scalar comparison, mirroring SQL's IS DISTINCT FROM. */
export function sameScalar(a: string | null | undefined, b: string | null | undefined): boolean {
  return (a ?? null) === (b ?? null);
}

/** Order-sensitive array comparison — source_urls is an ordered evidence list. */
export function sameStringArray(a: readonly string[] | null, b: readonly string[] | null): boolean {
  const left = a ?? [];
  const right = b ?? [];
  return left.length === right.length && left.every((value, index) => value === right[index]);
}

// ---------------------------------------------------------------------------
// Projections
//
// The same query runs against both databases, so source and target rows are
// directly comparable. Two normalisations matter:
//   - event_date is cast to text, so the driver's Date handling cannot make an
//     identical date look changed.
//   - researched_at is rendered AT TIME ZONE 'UTC', so two servers in
//     different session timezones do not report a false diff. (Verified: the
//     same row renders identically under PGTZ=Asia/Tokyo.)
// ---------------------------------------------------------------------------

export type PromotionClient = {
  query: (text: string, values?: readonly unknown[]) => Promise<{ rows: unknown[]; rowCount?: number | null }>;
};

export type RecordRow = {
  candidate_id: string;
  record_identity_key: string;
  description: string;
  source_url: string;
  event_date: string;
  origin: string | null;
  origin_run_id: string | null;
};

export type TagRow = {
  candidate_id: string;
  record_identity_key: string;
  research_area_slug: string;
  stance: string | null;
};

export type LabelRow = {
  source: string;
  committee_id: string;
  cycle: number;
  committee_name: string;
  label: string;
  source_urls: string[];
  researched_at_utc: string;
};

export const RECORD_PROJECTION_SQL = `
  SELECT
    candidate_id::text AS candidate_id,
    record_identity_key,
    description,
    source_url,
    event_date::text AS event_date,
    origin,
    origin_run_id
  FROM public.candidate_records
`;

// Tags are carried by logical reference — candidate + record identity key +
// area slug — never by candidate_record_id. A record that exists on both sides
// can hold a different id on the target, so a transported id would attach the
// tag to the wrong record or to nothing.
export const TAG_PROJECTION_SQL = `
  SELECT
    r.candidate_id::text AS candidate_id,
    r.record_identity_key,
    a.slug AS research_area_slug,
    t.stance
  FROM public.candidate_record_area_tags AS t
  JOIN public.candidate_records AS r ON r.id = t.candidate_record_id
  JOIN public.research_areas AS a ON a.id = t.research_area_id
`;

export const LABEL_PROJECTION_SQL = `
  SELECT
    source,
    committee_id,
    cycle,
    committee_name,
    label,
    source_urls,
    (researched_at AT TIME ZONE 'UTC')::text AS researched_at_utc
  FROM public.finance_committee_labels
`;

const KEY_SEPARATOR = " ";

export function recordKey(row: Pick<RecordRow, "candidate_id" | "record_identity_key">): string {
  return [row.candidate_id, row.record_identity_key].join(KEY_SEPARATOR);
}

export function tagKey(row: Pick<TagRow, "candidate_id" | "record_identity_key" | "research_area_slug">): string {
  return [row.candidate_id, row.record_identity_key, row.research_area_slug].join(KEY_SEPARATOR);
}

export function labelKey(row: Pick<LabelRow, "source" | "committee_id" | "cycle">): string {
  return [row.source, row.committee_id, String(row.cycle)].join(KEY_SEPARATOR);
}

/**
 * Content equality for a record. Provenance (origin, origin_run_id) is
 * deliberately excluded: it rides along on an update, but a difference in it
 * alone must not trigger one, or promoting rows whose local pipeline metadata
 * differs would rewrite target history for no reader-visible gain.
 *
 * Note this update path is narrow by construction — record_identity_key is
 * derived from the normalised description, URL and date, so an update only
 * fires when the raw text differs while normalising to the same key.
 */
export function sameRecord(a: RecordRow, b: RecordRow): boolean {
  return (
    sameScalar(a.description, b.description) &&
    sameScalar(a.source_url, b.source_url) &&
    sameScalar(a.event_date, b.event_date)
  );
}

export function sameTag(a: TagRow, b: TagRow): boolean {
  return sameScalar(a.stance, b.stance);
}

export function sameLabel(a: LabelRow, b: LabelRow): boolean {
  return (
    sameScalar(a.committee_name, b.committee_name) &&
    sameScalar(a.label, b.label) &&
    sameStringArray(a.source_urls, b.source_urls) &&
    sameScalar(a.researched_at_utc, b.researched_at_utc)
  );
}

export async function loadProjection<T>(client: PromotionClient, sql: string): Promise<T[]> {
  const result = await client.query(sql);
  return result.rows as T[];
}

// ---------------------------------------------------------------------------
// Upserts
//
// Every statement below is an INSERT with a conditional ON CONFLICT DO UPDATE.
// There is deliberately no DELETE, TRUNCATE or DDL anywhere in this file — a
// test asserts that, because "never deletes" is the property that makes this
// tool safe to point at production.
//
// The DO UPDATE ... WHERE clause is load-bearing, not defensive noise: both
// record tables have an updated_at trigger, so an unconditional update would
// rewrite every row on every run and the second-run-is-a-no-op guarantee would
// be false.
// ---------------------------------------------------------------------------

export const UPSERT_RECORDS_SQL = `
  INSERT INTO public.candidate_records
    (candidate_id, record_identity_key, description, source_url, event_date, origin, origin_run_id)
  SELECT
    s.candidate_id, s.record_identity_key, s.description, s.source_url,
    s.event_date::date, s.origin, s.origin_run_id
  FROM jsonb_to_recordset($1::jsonb) AS s(
    candidate_id uuid, record_identity_key text, description text,
    source_url text, event_date text, origin text, origin_run_id text)
  ON CONFLICT (candidate_id, record_identity_key) DO UPDATE SET
    description = EXCLUDED.description,
    source_url = EXCLUDED.source_url,
    event_date = EXCLUDED.event_date,
    origin = EXCLUDED.origin,
    origin_run_id = EXCLUDED.origin_run_id
  WHERE public.candidate_records.description IS DISTINCT FROM EXCLUDED.description
     OR public.candidate_records.source_url IS DISTINCT FROM EXCLUDED.source_url
     OR public.candidate_records.event_date IS DISTINCT FROM EXCLUDED.event_date
`;

// The two joins are the fix for the central hazard: they resolve the TARGET's
// record id and research-area id at write time, so a tag never carries a
// source id across the wire. A row whose parents do not resolve is dropped by
// the join, which is why preflight must prove resolvability first.
export const UPSERT_TAGS_SQL = `
  INSERT INTO public.candidate_record_area_tags (candidate_record_id, research_area_id, stance)
  SELECT r.id, a.id, s.stance
  FROM jsonb_to_recordset($1::jsonb) AS s(
    candidate_id uuid, record_identity_key text, research_area_slug text, stance text)
  JOIN public.candidate_records AS r
    ON r.candidate_id = s.candidate_id
   AND r.record_identity_key = s.record_identity_key
  JOIN public.research_areas AS a ON a.slug = s.research_area_slug
  ON CONFLICT (candidate_record_id, research_area_id) DO UPDATE SET
    stance = EXCLUDED.stance
  WHERE public.candidate_record_area_tags.stance IS DISTINCT FROM EXCLUDED.stance
`;

// researched_at is copied verbatim, never set to now(): it is research
// provenance the reader sees, and rewriting it would both falsify history and
// make every run look changed.
export const UPSERT_LABELS_SQL = `
  INSERT INTO public.finance_committee_labels
    (source, committee_id, cycle, committee_name, label, source_urls, researched_at)
  SELECT
    s.source, s.committee_id, s.cycle, s.committee_name, s.label,
    ARRAY(SELECT jsonb_array_elements_text(s.source_urls)),
    (s.researched_at_utc)::timestamp AT TIME ZONE 'UTC'
  FROM jsonb_to_recordset($1::jsonb) AS s(
    source text, committee_id text, cycle int, committee_name text,
    label text, source_urls jsonb, researched_at_utc text)
  ON CONFLICT (source, committee_id, cycle) DO UPDATE SET
    committee_name = EXCLUDED.committee_name,
    label = EXCLUDED.label,
    source_urls = EXCLUDED.source_urls,
    researched_at = EXCLUDED.researched_at
  WHERE public.finance_committee_labels.committee_name IS DISTINCT FROM EXCLUDED.committee_name
     OR public.finance_committee_labels.label IS DISTINCT FROM EXCLUDED.label
     OR public.finance_committee_labels.source_urls IS DISTINCT FROM EXCLUDED.source_urls
     OR public.finance_committee_labels.researched_at IS DISTINCT FROM EXCLUDED.researched_at
`;

export const UPSERT_BATCH_SIZE = 500;

export function chunk<T>(rows: readonly T[], size = UPSERT_BATCH_SIZE): T[][] {
  if (size <= 0) {
    throw new Error("chunk size must be positive");
  }
  const chunks: T[][] = [];
  for (let index = 0; index < rows.length; index += size) {
    chunks.push(rows.slice(index, index + size));
  }
  return chunks;
}

/** Runs one upsert statement over the rows in batches. Returns rows written. */
export async function upsertBatched(
  client: PromotionClient,
  sql: string,
  rows: readonly unknown[],
  batchSize = UPSERT_BATCH_SIZE
): Promise<number> {
  let written = 0;
  for (const batch of chunk(rows, batchSize)) {
    const result = await client.query(sql, [JSON.stringify(batch)]);
    written += result.rowCount ?? 0;
  }
  return written;
}

// ---------------------------------------------------------------------------
// Preflight resolution
// ---------------------------------------------------------------------------

/**
 * Candidate UUIDs that the target cannot accept as a record parent. A UUID
 * that is absent, soft-deleted or merged away is reported rather than skipped:
 * silently dropping a record is how a promotion reports success while leaving
 * a candidate half-populated.
 */
export async function findUnresolvableCandidates(
  target: PromotionClient,
  candidateIds: readonly string[]
): Promise<string[]> {
  if (candidateIds.length === 0) {
    return [];
  }
  const result = await target.query(
    `
      SELECT s.candidate_id::text AS candidate_id
      FROM unnest($1::uuid[]) AS s(candidate_id)
      LEFT JOIN public.candidates AS c ON c.id = s.candidate_id
      WHERE c.id IS NULL
         OR c.deleted_at IS NOT NULL
         OR c.merged_into_candidate_id IS NOT NULL
    `,
    [candidateIds]
  );
  return (result.rows as { candidate_id: string }[]).map((row) => row.candidate_id);
}

export async function findUnresolvableAreaSlugs(
  target: PromotionClient,
  slugs: readonly string[]
): Promise<string[]> {
  if (slugs.length === 0) {
    return [];
  }
  const result = await target.query(
    `
      SELECT s.slug
      FROM unnest($1::text[]) AS s(slug)
      LEFT JOIN public.research_areas AS a ON a.slug = s.slug
      WHERE a.slug IS NULL
    `,
    [slugs]
  );
  return (result.rows as { slug: string }[]).map((row) => row.slug);
}

/**
 * Schema parity. Equal migration sets do not prove equal seed data, but an
 * unequal set means the two databases disagree about table shape and no
 * promotion should be attempted. This tool never runs migrations itself.
 */
export async function readMigrationSet(client: PromotionClient): Promise<Map<string, string>> {
  const result = await client.query(
    "SELECT filename, checksum FROM public.schema_migrations ORDER BY filename"
  );
  return new Map((result.rows as { filename: string; checksum: string }[]).map((row) => [row.filename, row.checksum]));
}

/**
 * Compares migration state, scoped to the migration FILES that currently exist
 * on disk.
 *
 * Scoping matters. A long-lived database keeps schema_migrations rows for
 * migrations that were later renamed, and those files no longer exist — the
 * local database carries 9 such rows (e.g. 196_add_county_council_chairman_alias.sql,
 * renamed to ..._chair_alias.sql) that a freshly migrated database will never
 * have. A naive set-equality check therefore reports a mismatch forever and
 * blocks every promotion. Only files on disk are authoritative; rows without a
 * file are historical residue on either side and are ignored.
 */
export function diffMigrationSets(input: {
  source: ReadonlyMap<string, string>;
  target: ReadonlyMap<string, string>;
  knownFilenames: readonly string[];
}): { missingOnSource: string[]; missingOnTarget: string[]; checksumMismatch: string[] } {
  const missingOnSource: string[] = [];
  const missingOnTarget: string[] = [];
  const checksumMismatch: string[] = [];

  for (const filename of input.knownFilenames) {
    const sourceChecksum = input.source.get(filename);
    const targetChecksum = input.target.get(filename);
    if (sourceChecksum === undefined) {
      missingOnSource.push(filename);
    }
    if (targetChecksum === undefined) {
      missingOnTarget.push(filename);
    }
    if (
      sourceChecksum !== undefined &&
      targetChecksum !== undefined &&
      sourceChecksum !== targetChecksum
    ) {
      checksumMismatch.push(filename);
    }
  }

  return { missingOnSource, missingOnTarget, checksumMismatch };
}

/**
 * Apply mode requires the operator to retype the target host, so a promotion
 * can never commit to a database nobody looked at. Compared against the parsed
 * effective host, not the raw URL text.
 */
export function assertConfirmedTarget(target: EndpointFingerprint, confirmTarget: string): void {
  const supplied = confirmTarget.trim().toLowerCase();
  if (supplied.length === 0) {
    throw new Error(
      `--apply requires --confirm-target <host>. Target is ${describeEndpoint(target)}; ` +
        `re-run with --confirm-target ${target.host}`
    );
  }
  if (supplied !== target.host) {
    throw new Error(
      `--confirm-target "${supplied}" does not match the target host "${target.host}". ` +
        "Refusing to write to a database the operator did not name."
    );
  }
}

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

const SCRIPT_LABEL = "promote research data";

function usage(): string {
  return [
    "Usage:",
    "  npm run research:promote                       # dry run, writes nothing",
    "  npm run research:promote:apply -- --confirm-target <host>",
    "",
    "Endpoints:",
    "  source  DATABASE_URL                     (must be local; read-only)",
    "  target  PROMOTION_TARGET_DATABASE_URL    (env only — never a flag, so a",
    "                                            password cannot land in shell",
    "                                            history or the process list)",
  ].join("\n");
}

function readFlagValue(argv: readonly string[], name: string): string | null {
  const index = argv.indexOf(name);
  if (index < 0) {
    return null;
  }
  const value = argv[index + 1];
  return value !== undefined && !value.startsWith("--") ? value : null;
}

async function listMigrationFilenames(): Promise<string[]> {
  const dir = resolve(dirname(fileURLToPath(import.meta.url)), "../../../db/migrations");
  const names = await readdir(dir);
  return names.filter((name) => name.endsWith(".sql")).sort();
}

export type PromotionReport = {
  mode: "dry_run" | "apply";
  source: string;
  target: string;
  tables: Record<string, { inserts: number; updates: number; unchanged: number; targetOnly: number; written?: number }>;
};

async function main(): Promise<void> {
  const argv = process.argv.slice(2);
  assertKnownCliFlags(SCRIPT_LABEL, argv, [
    { name: "--apply", value: "none" },
    { name: "--confirm-target", value: "space" },
    { name: "--report-file", value: "space" },
  ]);

  loadProjectEnv();
  const apply = argv.includes("--apply");
  const endpoints = assertPromotionEndpoints({
    sourceUrl: process.env.DATABASE_URL ?? "",
    targetUrl: process.env.PROMOTION_TARGET_DATABASE_URL ?? "",
  });
  if (apply) {
    assertConfirmedTarget(endpoints.target, readFlagValue(argv, "--confirm-target") ?? "");
  }

  console.log(`source: ${describeEndpoint(endpoints.source)}`);
  console.log(`target: ${describeEndpoint(endpoints.target)}`);
  console.log(`mode:   ${apply ? "APPLY (writes)" : "dry run (writes nothing)"}`);

  const sourcePool = new Pool({ connectionString: process.env.DATABASE_URL });
  const targetPool = new Pool({ connectionString: process.env.PROMOTION_TARGET_DATABASE_URL });
  const source: PromotionClient = { query: (text, values) => sourcePool.query(text, values as unknown[]) };
  const target: PromotionClient = { query: (text, values) => targetPool.query(text, values as unknown[]) };

  try {
    const migrationDiff = diffMigrationSets({
      source: await readMigrationSet(source),
      target: await readMigrationSet(target),
      knownFilenames: await listMigrationFilenames(),
    });
    if (
      migrationDiff.missingOnSource.length > 0 ||
      migrationDiff.missingOnTarget.length > 0 ||
      migrationDiff.checksumMismatch.length > 0
    ) {
      throw new Error(
        `Refusing to promote across differing schemas: ${JSON.stringify(migrationDiff)}. ` +
          "Run db:migrate on the lagging database first; this tool never migrates."
      );
    }

    const [sourceRecords, targetRecords] = await Promise.all([
      loadProjection<RecordRow>(source, RECORD_PROJECTION_SQL),
      loadProjection<RecordRow>(target, RECORD_PROJECTION_SQL),
    ]);
    const [sourceTags, targetTags] = await Promise.all([
      loadProjection<TagRow>(source, TAG_PROJECTION_SQL),
      loadProjection<TagRow>(target, TAG_PROJECTION_SQL),
    ]);
    const [sourceLabels, targetLabels] = await Promise.all([
      loadProjection<LabelRow>(source, LABEL_PROJECTION_SQL),
      loadProjection<LabelRow>(target, LABEL_PROJECTION_SQL),
    ]);

    const recordPlan = planRows({ sourceRows: sourceRecords, targetRows: targetRecords, keyOf: recordKey, isEqual: sameRecord });
    const tagPlan = planRows({ sourceRows: sourceTags, targetRows: targetTags, keyOf: tagKey, isEqual: sameTag });
    const labelPlan = planRows({ sourceRows: sourceLabels, targetRows: targetLabels, keyOf: labelKey, isEqual: sameLabel });

    // Only rows we are about to write need resolvable parents; untouched rows
    // are the target's business.
    const pendingRecords = [...recordPlan.inserts, ...recordPlan.updates];
    const pendingTags = [...tagPlan.inserts, ...tagPlan.updates];
    const missingCandidates = await findUnresolvableCandidates(
      target,
      [...new Set(pendingRecords.map((row) => row.candidate_id))]
    );
    const missingSlugs = await findUnresolvableAreaSlugs(
      target,
      [...new Set(pendingTags.map((row) => row.research_area_slug))]
    );
    if (missingCandidates.length > 0 || missingSlugs.length > 0) {
      throw new Error(
        "Refusing to promote: the target is missing parents these rows depend on. " +
          `Unresolvable candidates: ${missingCandidates.slice(0, 10).join(", ") || "none"}. ` +
          `Unresolvable research areas: ${missingSlugs.slice(0, 10).join(", ") || "none"}. ` +
          "Promote or repair those first; this tool never invents a parent."
      );
    }

    const report: PromotionReport = {
      mode: apply ? "apply" : "dry_run",
      source: describeEndpoint(endpoints.source),
      target: describeEndpoint(endpoints.target),
      tables: {
        candidate_records: {
          inserts: recordPlan.inserts.length,
          updates: recordPlan.updates.length,
          unchanged: recordPlan.unchangedCount,
          targetOnly: recordPlan.targetOnlyCount,
        },
        candidate_record_area_tags: {
          inserts: tagPlan.inserts.length,
          updates: tagPlan.updates.length,
          unchanged: tagPlan.unchangedCount,
          targetOnly: tagPlan.targetOnlyCount,
        },
        finance_committee_labels: {
          inserts: labelPlan.inserts.length,
          updates: labelPlan.updates.length,
          unchanged: labelPlan.unchangedCount,
          targetOnly: labelPlan.targetOnlyCount,
        },
      },
    };

    if (apply) {
      const client: PoolClient = await targetPool.connect();
      try {
        await client.query("BEGIN");
        const wrapped: PromotionClient = { query: (text, values) => client.query(text, values as unknown[]) };
        // Records before tags: a tag resolves its parent by natural key, so
        // the record must already exist on the target.
        report.tables.candidate_records!.written = await upsertBatched(wrapped, UPSERT_RECORDS_SQL, pendingRecords);
        report.tables.candidate_record_area_tags!.written = await upsertBatched(wrapped, UPSERT_TAGS_SQL, pendingTags);
        report.tables.finance_committee_labels!.written = await upsertBatched(
          wrapped,
          UPSERT_LABELS_SQL,
          [...labelPlan.inserts, ...labelPlan.updates]
        );
        await client.query("COMMIT");
      } catch (error) {
        await client.query("ROLLBACK");
        throw error;
      } finally {
        client.release();
      }
    }

    const reportFile = readFlagValue(argv, "--report-file");
    if (reportFile) {
      await writeFile(reportFile, `${JSON.stringify(report, null, 2)}\n`, "utf8");
    }
    console.log(JSON.stringify(report, null, 2));
    if (!apply) {
      console.log("\nDry run only — nothing was written. Re-run with --apply --confirm-target <host> to commit.");
    }
  } finally {
    await sourcePool.end();
    await targetPool.end();
  }
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint && import.meta.url === entrypoint) {
  main().catch((error: unknown) => {
    console.error(`${SCRIPT_LABEL} failed: ${error instanceof Error ? error.message : String(error)}`);
    console.error(usage());
    process.exit(1);
  });
}
