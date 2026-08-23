// Guarded correction of a stored candidate name.
//
// The profile writer fills blanks only, and --replace-profile-fields
// deliberately excludes identity fields, so a wrong stored ballot name has no
// supported correction path (live case: candidate 2f4de7c9 stored as "Marvin
// Richardson" while the Idaho SOS lists him on the November 2026 governor
// ballot as "Pro-Life (A person formerly known as Marvin Richardson)" — he
// legally changed his name in 2008). Voters see the wrong ballot name, and
// roster matching by display_name can miss the row entirely. This wrapper
// renames EXACTLY ONE explicitly identified candidate; nothing is heuristic.
//
// Guard rails, all of which must pass before a single row changes:
// - the candidate exists, is row-locked, and is neither merged (rename the
//   survivor instead) nor soft-deleted;
// - no other live candidate in any election this candidate is linked to —
//   through candidate_id OR running_mate_candidate_id, on either side —
//   already carries the new display name: profile identity matching walks
//   both link columns and THROWS on a same-election duplicate name (see
//   findCandidateLinkedToElectionByDisplayName), so a rename creating that
//   collision would block every future profile write for the election;
// - competing renames serialize on the same per-name advisory lock the
//   profile identity resolver takes, so two concurrent renames to the same
//   name cannot both pass the collision check before either commits;
// - an official HTTPS source documenting the ballot name is appended to the
//   candidate's profile_sources;
// - an audit row (old/new names, source, reason) is written to
//   candidate_rename_audit in the same transaction;
// - records and candidate_elections links are left untouched;
// - local-database guard, row lock, single transaction, --dry-run (executes
//   everything and rolls back, so the reported values are real).
//
// Re-running the exact command is idempotent: an already-renamed row reports
// alreadyRenamed and only converges provenance (appends a missing source),
// without writing a second audit row.
import { pathToFileURL } from "node:url";

import { Pool } from "pg";

import { loadProjectEnv } from "../config/env.js";
import { normalizeCandidateName } from "../utils/candidateIdentity.js";
import { mergeElectionSource } from "./electionSourceUtils.js";
import { assertKnownCliFlags } from "./manualCliFlags.js";
import { requireLocalDatabaseTarget } from "./localDatabaseGuard.js";

type QueryResultLike<T> = { rows: T[] };

export type RenameCandidateClient = {
  query<T = unknown>(text: string, values?: unknown[]): Promise<QueryResultLike<T>>;
};

export type RenameCandidateOptions = {
  candidateId: string;
  displayName: string;
  firstName: string | null;
  lastName: string | null;
  sourceUrl: string;
  reason: string;
  dryRun: boolean;
};

export type RenameCandidateResult =
  | {
      alreadyRenamed: true;
      dryRun: boolean;
      candidateId: string;
      displayName: string;
      // The already-renamed path still converges provenance: when the stored
      // row is missing the official source, a re-run appends it (same
      // convention as manual:election-date:correct).
      sourceAppended: boolean;
    }
  | {
      alreadyRenamed: false;
      dryRun: boolean;
      candidateId: string;
      displayName: { old: string | null; new: string };
      firstName: { old: string; new: string } | null;
      lastName: { old: string; new: string } | null;
      sourceAppended: boolean;
      auditRowId: string | null;
    };

type CandidateRow = {
  id: string;
  display_name: string | null;
  first_name: string;
  last_name: string;
  deleted_at: string | null;
  merged_into_candidate_id: string | null;
  profile_sources: unknown;
};

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

function usage(): string {
  return [
    "Correct one candidate's stored name to the official ballot name.",
    "",
    "Usage:",
    '  npm run manual:candidates:rename -- --candidate-id uuid --display-name "New Name" --source-url https://... --reason text [--first-name text] [--last-name text] [--dry-run]',
    "",
    "For two rows that are the same person, use manual:candidates:merge",
    "instead — a rename cannot collapse duplicates.",
  ].join("\n");
}

function readFlag(name: string): string | null {
  const index = process.argv.indexOf(name);
  if (index < 0) return null;
  const value = process.argv[index + 1];
  if (!value || value.startsWith("--")) {
    throw new Error(`Missing value for ${name}.\n${usage()}`);
  }
  return value.trim();
}

function requireFlag(name: string): string {
  const value = readFlag(name);
  if (!value) throw new Error(`Missing ${name}.\n${usage()}`);
  return value;
}

function requireEnv(name: string): string {
  const value = process.env[name]?.trim();
  if (!value) throw new Error(`${name} is required for manual candidate rename`);
  return value;
}

// Same normalization migration 044 used to backfill display_name, so
// change detection and the collision guard never differ on whitespace alone.
export function normalizeName(value: string): string {
  return value.trim().replace(/\s+/g, " ");
}

export async function runRenameCandidate(
  client: RenameCandidateClient,
  options: RenameCandidateOptions
): Promise<RenameCandidateResult> {
  // PostgreSQL returns uuid columns lowercased; a valid uppercase input
  // would otherwise fail the row-matching below with a false "not found".
  const candidateId = options.candidateId.toLowerCase();
  const { sourceUrl, reason, dryRun } = options;

  // Enforced here, not only in main(): a direct caller (test, future script)
  // must not be able to store non-HTTPS provenance.
  if (new URL(sourceUrl).protocol !== "https:") {
    throw new Error("--source-url must use HTTPS");
  }
  const newDisplayName = normalizeName(options.displayName);
  if (newDisplayName.length === 0) throw new Error("--display-name must not be blank");
  const newFirstName = options.firstName === null ? null : normalizeName(options.firstName);
  const newLastName = options.lastName === null ? null : normalizeName(options.lastName);
  if (newFirstName !== null && newFirstName.length === 0) {
    throw new Error("--first-name must not be blank when provided");
  }
  if (newLastName !== null && newLastName.length === 0) {
    throw new Error("--last-name must not be blank when provided");
  }

  await client.query("BEGIN");
  try {
    // The collision check below is an unlocked read: two concurrent renames
    // of different candidates in the same election to the same name could
    // each pass it before the other commits. Serialize on the same per-name
    // advisory lock findOrCreateCandidateFromProfile takes for its own
    // read-then-insert race (transaction-scoped; releases on commit or
    // rollback). Taken before the row lock, mirroring the resolver's
    // ordering. When the display name is the person's "First Last" this also
    // serializes against concurrent pipeline identity resolution.
    await client.query(
      `SELECT pg_advisory_xact_lock(hashtextextended('candidate_identity:' || $1, 0))`,
      [normalizeCandidateName(newDisplayName)]
    );

    const candidateResult = await client.query<CandidateRow>(
      `
        SELECT id, display_name, first_name, last_name, deleted_at::text,
               merged_into_candidate_id, profile_sources
        FROM public.candidates
        WHERE id = $1::uuid
        FOR UPDATE
      `,
      [candidateId]
    );
    const candidate = candidateResult.rows[0];
    if (!candidate) throw new Error(`Candidate not found: ${candidateId}`);
    if (candidate.merged_into_candidate_id) {
      throw new Error(
        `Candidate ${candidateId} is merged into ${candidate.merged_into_candidate_id}; ` +
          "rename the surviving row instead"
      );
    }
    if (candidate.deleted_at) {
      throw new Error(`Candidate ${candidateId} is soft-deleted; a rename target must be live`);
    }

    const storedDisplayName =
      candidate.display_name === null ? null : normalizeName(candidate.display_name);
    const displayNameChanged = storedDisplayName !== newDisplayName;
    const firstNameChanged =
      newFirstName !== null && normalizeName(candidate.first_name) !== newFirstName;
    const lastNameChanged =
      newLastName !== null && normalizeName(candidate.last_name) !== newLastName;

    const { sources: mergedSources, appended: sourceAppended } = mergeElectionSource(
      candidate.profile_sources,
      sourceUrl
    );

    if (!displayNameChanged && !firstNameChanged && !lastNameChanged) {
      // Idempotent path: nothing to rename, no second audit row. Provenance
      // still converges — a row renamed out-of-band without the official
      // source gets the source appended. The UPDATE runs on dry runs too
      // (then rolls back), keeping the execute-then-rollback contract the
      // main path already honors.
      if (sourceAppended) {
        await client.query(
          `
            UPDATE public.candidates
            SET profile_sources = $2::jsonb,
                updated_at = now()
            WHERE id = $1::uuid
          `,
          [candidateId, JSON.stringify(mergedSources)]
        );
      }
      if (sourceAppended && !dryRun) {
        await client.query("COMMIT");
      } else {
        await client.query("ROLLBACK");
      }
      return {
        alreadyRenamed: true,
        dryRun,
        candidateId,
        displayName: newDisplayName,
        sourceAppended,
      };
    }

    // Same-election collision guard: profile identity matching treats a
    // same-election display_name match as the same person — walking BOTH
    // candidate_elections columns (candidate_id and
    // running_mate_candidate_id) — and throws on a duplicate name (see
    // findCandidateLinkedToElectionByDisplayName), which would block every
    // future profile write for the election. So the guard covers both
    // columns on both sides: elections this candidate touches as candidate
    // or as running mate, and peers linked to those elections either way.
    // Which row should carry the name (or whether this is really a merge) is
    // a research question — refuse. Peers without a display_name are
    // compared on "first last", the same fallback readers render.
    if (displayNameChanged) {
      const collision = await client.query<{ id: string; election_id: string }>(
        `
          SELECT peer.id, peer_link.election_id
          FROM public.candidate_elections own_link
          JOIN public.candidate_elections peer_link
            ON peer_link.election_id = own_link.election_id
          JOIN public.candidates peer
            ON peer.id IN (peer_link.candidate_id, peer_link.running_mate_candidate_id)
          WHERE (own_link.candidate_id = $1::uuid OR own_link.running_mate_candidate_id = $1::uuid)
            AND peer.id <> $1::uuid
            AND peer.merged_into_candidate_id IS NULL
            AND peer.deleted_at IS NULL
            AND lower(trim(coalesce(peer.display_name, peer.first_name || ' ' || peer.last_name))) = lower($2)
          LIMIT 1
        `,
        [candidateId, newDisplayName]
      );
      if (collision.rows[0]) {
        throw new Error(
          `Candidate ${collision.rows[0].id} in election ${collision.rows[0].election_id} already ` +
            `carries the name "${newDisplayName}"; a rename would make roster matching ambiguous. ` +
            "If the two rows are the same person, use manual:candidates:merge; otherwise resolve " +
            "the naming conflict first (user decision), then re-run."
        );
      }
    }

    // Column list is fixed; only values are parameterized.
    const assignments: string[] = [];
    const values: unknown[] = [candidateId];
    const addAssignment = (column: string, cast: string, value: unknown) => {
      values.push(value);
      assignments.push(`${column} = $${values.length}${cast}`);
    };
    if (displayNameChanged) addAssignment("display_name", "", newDisplayName);
    if (firstNameChanged) addAssignment("first_name", "", newFirstName);
    if (lastNameChanged) addAssignment("last_name", "", newLastName);
    if (sourceAppended) {
      addAssignment("profile_sources", "::jsonb", JSON.stringify(mergedSources));
    }
    await client.query(
      `UPDATE public.candidates SET ${assignments.join(", ")}, updated_at = now() WHERE id = $1::uuid`,
      values
    );

    const auditResult = await client.query<{ id: string }>(
      `
        INSERT INTO public.candidate_rename_audit
          (candidate_id, old_display_name, new_display_name,
           old_first_name, new_first_name, old_last_name, new_last_name,
           source_url, reason)
        VALUES ($1::uuid, $2, $3, $4, $5, $6, $7, $8, $9)
        RETURNING id
      `,
      [
        candidateId,
        candidate.display_name,
        newDisplayName,
        candidate.first_name,
        firstNameChanged ? newFirstName : null,
        candidate.last_name,
        lastNameChanged ? newLastName : null,
        sourceUrl.trim(),
        reason,
      ]
    );

    if (dryRun) {
      await client.query("ROLLBACK");
    } else {
      await client.query("COMMIT");
    }

    return {
      alreadyRenamed: false,
      dryRun,
      candidateId,
      displayName: { old: candidate.display_name, new: newDisplayName },
      firstName: firstNameChanged
        ? { old: candidate.first_name, new: newFirstName as string }
        : null,
      lastName: lastNameChanged ? { old: candidate.last_name, new: newLastName as string } : null,
      sourceAppended,
      auditRowId: auditResult.rows[0]?.id ?? null,
    };
  } catch (error) {
    await client.query("ROLLBACK").catch(() => undefined);
    throw error;
  }
}

async function main(): Promise<void> {
  assertKnownCliFlags("manual:candidates:rename", process.argv.slice(2), [
    { name: "--candidate-id", value: "space" },
    { name: "--display-name", value: "space" },
    { name: "--first-name", value: "space" },
    { name: "--last-name", value: "space" },
    { name: "--source-url", value: "space" },
    { name: "--reason", value: "space" },
    { name: "--dry-run", value: "none" },
  ]);
  loadProjectEnv();

  const candidateId = requireFlag("--candidate-id");
  const displayName = requireFlag("--display-name");
  const firstName = readFlag("--first-name");
  const lastName = readFlag("--last-name");
  const sourceUrl = requireFlag("--source-url");
  const reason = requireFlag("--reason");
  const dryRun = process.argv.includes("--dry-run");

  if (!UUID_RE.test(candidateId)) throw new Error(`Invalid --candidate-id: ${candidateId}`);
  if (reason.length < 20) {
    throw new Error("--reason must explain the rename in at least 20 characters");
  }

  const databaseUrl = requireEnv("DATABASE_URL");
  requireLocalDatabaseTarget(databaseUrl);
  const pool = new Pool({ connectionString: databaseUrl });
  const client = await pool.connect();
  try {
    const result = await runRenameCandidate(client, {
      candidateId,
      displayName,
      firstName,
      lastName,
      sourceUrl,
      reason,
      dryRun,
    });
    console.log(JSON.stringify({ ...result, reason }, null, 2));
  } finally {
    client.release();
    await pool.end();
  }
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  void main().catch((error) => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  });
}
