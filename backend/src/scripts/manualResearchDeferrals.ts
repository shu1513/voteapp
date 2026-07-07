import { Pool } from "pg";

import { loadProjectEnv } from "../config/env.js";

const DEFERRAL_STAGES = [
  "elections",
  "candidate_roster",
  "candidate_profile",
  "candidate_records",
  "ballot_measure",
] as const;

type DeferralStage = (typeof DEFERRAL_STAGES)[number];

function usage(): string {
  return [
    "Manual research deferral ledger CLI (agent-facing).",
    "",
    "Usage: npm run manual:deferral:<command> -- [flags]",
    "",
    "Commands:",
    "  record   Record (or re-record, bumping date/reason) a deferral.",
    "           --district-id <id> --stage <stage> --reason <text> --blocked-until <YYYY-MM-DD>",
    "           [--election-id <id>] [--source-url <url>]",
    `           Stages: ${DEFERRAL_STAGES.join(", ")}`,
    "  due      List deferred rows whose blocked_until has passed. [--limit <n>] [--all]",
    "           (--all lists every open deferral regardless of date)",
    "  resolve  Close a deferral after the deferred unit was completed. --deferral-id <id> [--note <text>]",
    "  cancel   Close a deferral recorded in error or made moot. --deferral-id <id> --note <text>",
    "  status   Print counts by status, due-now count, and the next due date.",
  ].join("\n");
}

function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value || value.trim().length === 0) {
    throw new Error(`Missing required env var: ${name}`);
  }
  return value;
}

function parseFlags(argv: readonly string[]): Map<string, string> {
  const flags = new Map<string, string>();
  for (let i = 0; i < argv.length; i += 1) {
    const token = argv[i];
    if (!token.startsWith("--")) {
      continue;
    }
    const key = token.slice(2);
    const next = argv[i + 1];
    if (next === undefined || next.startsWith("--")) {
      flags.set(key, "true");
    } else {
      flags.set(key, next);
      i += 1;
    }
  }
  return flags;
}

function requireFlag(flags: Map<string, string>, name: string): string {
  const value = flags.get(name);
  if (value === undefined || value.trim().length === 0 || value === "true") {
    throw new Error(`Missing required flag: --${name}`);
  }
  return value;
}

function print(payload: unknown): void {
  console.log(JSON.stringify(payload, null, 2));
}

function parseBlockedUntil(raw: string): string {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(raw) || Number.isNaN(Date.parse(raw))) {
    throw new Error(`Invalid --blocked-until: ${raw}. Expected YYYY-MM-DD.`);
  }
  return raw;
}

async function runCommand(pool: Pool, command: string, flags: Map<string, string>): Promise<void> {
  switch (command) {
    case "record": {
      const districtId = requireFlag(flags, "district-id");
      const stage = requireFlag(flags, "stage");
      if (!DEFERRAL_STAGES.includes(stage as DeferralStage)) {
        throw new Error(`Invalid --stage: ${stage}. Expected one of ${DEFERRAL_STAGES.join(", ")}.`);
      }
      const reason = requireFlag(flags, "reason");
      const blockedUntil = parseBlockedUntil(requireFlag(flags, "blocked-until"));
      const electionId = flags.get("election-id") ?? null;
      const sourceUrl = flags.get("source-url") ?? null;

      const districtRow = await pool.query(`SELECT name FROM public.districts WHERE id = $1`, [
        districtId,
      ]);
      if (districtRow.rows.length === 0) {
        throw new Error(`District ${districtId} not found.`);
      }
      const districtName = (districtRow.rows[0] as { name: string }).name;

      if (electionId) {
        const electionRow = await pool.query(
          `SELECT id FROM public.elections WHERE id = $1 AND district_id = $2`,
          [electionId, districtId]
        );
        if (electionRow.rows.length === 0) {
          throw new Error(`Election ${electionId} not found in district ${districtId}.`);
        }
      }

      // Manual upsert: two partial unique indexes (election-scoped and
      // district-wide) cannot share one ON CONFLICT target.
      const updated = await pool.query(
        `
          UPDATE public.manual_research_deferrals
          SET reason = $1, blocked_until = $2, source_url = $3, updated_at = now()
          WHERE status = 'deferred'
            AND district_id = $4
            AND stage = $5
            AND election_id IS NOT DISTINCT FROM $6
          RETURNING id
        `,
        [reason, blockedUntil, sourceUrl, districtId, stage, electionId]
      );
      if (updated.rows.length > 0) {
        print({ deferral_id: updated.rows[0].id, district_name: districtName, updated: true });
        return;
      }
      const inserted = await pool.query(
        `
          INSERT INTO public.manual_research_deferrals
            (district_id, election_id, stage, reason, blocked_until, source_url, district_name_snapshot)
          VALUES ($1, $2, $3, $4, $5, $6, $7)
          RETURNING id
        `,
        [districtId, electionId, stage, reason, blockedUntil, sourceUrl, districtName]
      );
      print({ deferral_id: inserted.rows[0].id, district_name: districtName, recorded: true });
      return;
    }

    case "due": {
      const limitRaw = flags.get("limit");
      const limit = limitRaw ? Number.parseInt(limitRaw, 10) : 20;
      if (!Number.isFinite(limit) || limit <= 0) {
        throw new Error(`Invalid --limit: ${limitRaw}. Expected a positive integer.`);
      }
      const all = flags.get("all") === "true";
      const rows = await pool.query(
        `
          SELECT id, district_id, election_id, stage, reason, blocked_until::text, source_url,
                 district_name_snapshot, created_at
          FROM public.manual_research_deferrals
          WHERE status = 'deferred'
            ${all ? "" : "AND blocked_until <= CURRENT_DATE"}
          ORDER BY blocked_until ASC, created_at ASC
          LIMIT $1
        `,
        [limit]
      );
      print({ due_count: rows.rows.length, deferrals: rows.rows });
      return;
    }

    case "resolve":
    case "cancel": {
      const deferralId = requireFlag(flags, "deferral-id");
      const note = command === "cancel" ? requireFlag(flags, "note") : (flags.get("note") ?? null);
      const status = command === "resolve" ? "resolved" : "cancelled";
      const result = await pool.query(
        `
          UPDATE public.manual_research_deferrals
          SET status = $1, resolved_at = now(), resolution_note = $2, updated_at = now()
          WHERE id = $3 AND status = 'deferred'
          RETURNING id
        `,
        [status, note, deferralId]
      );
      if (result.rows.length === 0) {
        throw new Error(`Cannot ${command} deferral ${deferralId}: not found or not in 'deferred' status.`);
      }
      print({ deferral_id: deferralId, status });
      return;
    }

    case "status": {
      const stats = await pool.query(
        `
          SELECT status, count(*)::int AS count
          FROM public.manual_research_deferrals
          GROUP BY status
        `
      );
      const due = await pool.query(
        `
          SELECT count(*)::int AS due_now, min(blocked_until)::text AS next_due
          FROM public.manual_research_deferrals
          WHERE status = 'deferred' AND blocked_until <= CURRENT_DATE
        `
      );
      const upcoming = await pool.query(
        `
          SELECT min(blocked_until)::text AS next_upcoming
          FROM public.manual_research_deferrals
          WHERE status = 'deferred' AND blocked_until > CURRENT_DATE
        `
      );
      print({
        by_status: stats.rows,
        due_now: due.rows[0]?.due_now ?? 0,
        next_upcoming: upcoming.rows[0]?.next_upcoming ?? null,
      });
      return;
    }

    default:
      throw new Error(`Unknown command: ${command}\n\n${usage()}`);
  }
}

async function main(): Promise<void> {
  loadProjectEnv();

  const [command, ...rest] = process.argv.slice(2);
  if (!command || command === "help" || command === "--help") {
    console.log(usage());
    return;
  }

  const pool = new Pool({ connectionString: requireEnv("DATABASE_URL") });
  try {
    await runCommand(pool, command, parseFlags(rest));
  } finally {
    await pool.end();
  }
}

main().catch((error: unknown) => {
  console.error(error instanceof Error ? error.message : error);
  process.exitCode = 1;
});
