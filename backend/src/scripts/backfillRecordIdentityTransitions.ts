import { pathToFileURL } from "node:url";

import { Pool } from "pg";

import { loadProjectEnv } from "../config/env.js";
import { buildCandidateRecordIdentityKey } from "../pipeline/candidates/candidateRecordStore.js";
import { assertKnownCliFlags } from "./manualCliFlags.js";
import { requireLocalDatabaseTarget } from "./localDatabaseGuard.js";

// One-time backfill of candidate_record_identity_transitions for re-keys that
// happened BEFORE the ledger existed (migration 209). The only reconstructable
// cohort is the plain-language rewrite: plain_language_rewrites stores the
// exact original_text per row, so the pre-rewrite identity key can be
// recomputed with the real key function — provided the row's source_url and
// event_date still match what the key was computed from, which is guaranteed
// when the row's current description still equals the audit row's
// rewritten_text (the URL/date repairs would have re-keyed and re-audited).
// Rows edited again since the rewrite are reported and skipped: their old key
// would be a guess.
//
// Idempotent — the ledger's unique constraint absorbs re-runs. Local only:
// transitions describe local history and travel to other databases through
// research:promote's plan, never by direct remote writes.

const SCRIPT_LABEL = "backfill record identity transitions";

async function main(): Promise<void> {
  const argv = process.argv.slice(2);
  assertKnownCliFlags(SCRIPT_LABEL, argv, [{ name: "--apply", value: "none" }]);
  const apply = argv.includes("--apply");

  loadProjectEnv();
  const databaseUrl = process.env.DATABASE_URL?.trim() ?? "";
  requireLocalDatabaseTarget(databaseUrl);
  const pool = new Pool({ connectionString: databaseUrl });

  try {
    const rows = await pool.query<{
      target_id: string;
      original_text: string;
      rewritten_text: string;
      candidate_id: string;
      description: string;
      source_url: string;
      event_date: string;
      record_identity_key: string;
    }>(
      `
        SELECT r.target_id, r.original_text, r.rewritten_text,
               cr.candidate_id::text AS candidate_id, cr.description, cr.source_url,
               to_char(cr.event_date, 'YYYY-MM-DD') AS event_date, cr.record_identity_key
        FROM public.plain_language_rewrites AS r
        JOIN public.candidate_records AS cr ON cr.id = r.target_id
        WHERE r.target_table = 'candidate_records'
          AND r.target_column = 'description'
          AND r.status = 'applied'
      `
    );

    let planned = 0;
    let inserted = 0;
    let driftSkipped = 0;
    const driftSamples: string[] = [];
    for (const row of rows.rows) {
      if (row.description !== row.rewritten_text) {
        // Edited again after the rewrite. If that edit went through a
        // ledgered writer it recorded its own transition; reconstructing the
        // rewrite-era key from current URL/date would be a guess either way.
        driftSkipped += 1;
        if (driftSamples.length < 5) {
          driftSamples.push(row.target_id);
        }
        continue;
      }
      const oldKey = buildCandidateRecordIdentityKey({
        description: row.original_text,
        sourceUrl: row.source_url,
        eventDate: row.event_date,
      });
      if (oldKey === row.record_identity_key) {
        continue; // rewrite normalized to the same identity; nothing moved
      }
      planned += 1;
      if (apply) {
        const result = await pool.query(
          `
            INSERT INTO public.candidate_record_identity_transitions
              (candidate_id, old_record_identity_key, new_record_identity_key, reason)
            VALUES ($1, $2, $3, 'backfill')
            ON CONFLICT (candidate_id, old_record_identity_key, new_record_identity_key) DO NOTHING
          `,
          [row.candidate_id, oldKey, row.record_identity_key]
        );
        inserted += result.rowCount ?? 0;
      }
    }

    console.log(
      JSON.stringify(
        {
          mode: apply ? "apply" : "dry_run",
          auditRows: rows.rows.length,
          planned,
          ...(apply ? { inserted, alreadyPresent: planned - inserted } : {}),
          driftSkipped,
          driftSamples,
        },
        null,
        2
      )
    );
    if (!apply) {
      console.log("\nDry run only — re-run with --apply to write the transitions.");
    }
  } finally {
    await pool.end();
  }
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint && import.meta.url === entrypoint) {
  main().catch((error: unknown) => {
    console.error(`${SCRIPT_LABEL} failed: ${error instanceof Error ? error.message : String(error)}`);
    process.exit(1);
  });
}
