import { Pool } from "pg";

import { loadProjectEnv } from "../config/env.js";

import { assertKnownCliFlags, type CliFlagSpec } from "./manualCliFlags.js";
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
    "  due      List deferred rows whose blocked_until has passed. [--limit <n>] [--all] [--district-id <id>]",
    "           (--all lists every open deferral regardless of date, uncapped unless --limit is given)",
    "  resolve  Close a deferral after the deferred unit was completed. --deferral-id <id> [--note <text>]",
    "  cancel   Close a deferral recorded in error or made moot. --deferral-id <id> --note <text>",
    "  status   Print counts by status, due-now count, and the next due date. [--district-id <id>]",
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

// parseFlags stores a bare `--foo` as "true". For flags that carry a value,
// that must be an error rather than the literal string "true" reaching SQL.
function optionalValueFlag(flags: Map<string, string>, name: string): string | null {
  const value = flags.get(name);
  if (value === undefined) {
    return null;
  }
  if (value.trim().length === 0 || value === "true") {
    throw new Error(`Missing value for flag: --${name}`);
  }
  return value;
}

function print(payload: unknown): void {
  console.log(JSON.stringify(payload, null, 2));
}

// Date.parse accepts overflow dates and silently normalizes them
// ("2026-02-30" -> Mar 2). Round-trip the parsed parts so only real calendar
// days are accepted.
function parseBlockedUntil(raw: string): string {
  const match = /^(\d{4})-(\d{2})-(\d{2})$/.exec(raw);
  if (!match) {
    throw new Error(`Invalid --blocked-until: ${raw}. Expected YYYY-MM-DD.`);
  }
  const [, year, month, day] = match.map(Number) as [never, number, number, number];
  const parsed = new Date(Date.UTC(year, month - 1, day));
  if (
    parsed.getUTCFullYear() !== year ||
    parsed.getUTCMonth() !== month - 1 ||
    parsed.getUTCDate() !== day
  ) {
    throw new Error(`Invalid --blocked-until: ${raw}. Not a real calendar date.`);
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
      const electionId = optionalValueFlag(flags, "election-id");
      const sourceUrl = optionalValueFlag(flags, "source-url");

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
      const limitRaw = optionalValueFlag(flags, "limit");
      // Number.parseInt would accept "20abc"; require a clean positive integer.
      if (limitRaw !== null && !/^\d+$/.test(limitRaw)) {
        throw new Error(`Invalid --limit: ${limitRaw}. Expected a positive integer.`);
      }
      const limit = limitRaw !== null ? Number(limitRaw) : 20;
      if (!Number.isSafeInteger(limit) || limit <= 0) {
        throw new Error(`Invalid --limit: ${limitRaw}. Expected a positive integer.`);
      }
      const all = flags.get("all") === "true";
      // Resuming one district should not require reading every district's
      // ledger (a statewide audit had to eyeball 66 global rows).
      const districtId = optionalValueFlag(flags, "district-id");
      // `--all` promises every open deferral, so it is unbounded unless the
      // caller explicitly asked for a cap.
      const applyLimit = !all || limitRaw !== null;
      const params: unknown[] = [];
      if (districtId !== null) {
        params.push(districtId);
      }
      const districtClause = districtId !== null ? `AND district_id = $${params.length}` : "";
      if (applyLimit) {
        params.push(limit);
      }
      const rows = await pool.query(
        `
          SELECT id, district_id, election_id, stage, reason, blocked_until::text, source_url,
                 district_name_snapshot, created_at
          FROM public.manual_research_deferrals
          WHERE status = 'deferred'
            ${all ? "" : "AND blocked_until <= CURRENT_DATE"}
            ${districtClause}
          ORDER BY blocked_until ASC, created_at ASC
          ${applyLimit ? `LIMIT $${params.length}` : ""}
        `,
        params
      );
      print({ due_count: rows.rows.length, deferrals: rows.rows });
      return;
    }

    case "resolve":
    case "cancel": {
      const deferralId = requireFlag(flags, "deferral-id");
      const note = command === "cancel" ? requireFlag(flags, "note") : optionalValueFlag(flags, "note");
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
      const districtId = optionalValueFlag(flags, "district-id");
      const districtClause = districtId !== null ? "district_id = $1" : "TRUE";
      const params = districtId !== null ? [districtId] : [];
      const stats = await pool.query(
        `
          SELECT status, count(*)::int AS count
          FROM public.manual_research_deferrals
          WHERE ${districtClause}
          GROUP BY status
        `,
        params
      );
      const due = await pool.query(
        `
          SELECT count(*)::int AS due_now, min(blocked_until)::text AS next_due
          FROM public.manual_research_deferrals
          WHERE status = 'deferred' AND blocked_until <= CURRENT_DATE AND ${districtClause}
        `,
        params
      );
      const upcoming = await pool.query(
        `
          SELECT min(blocked_until)::text AS next_upcoming
          FROM public.manual_research_deferrals
          WHERE status = 'deferred' AND blocked_until > CURRENT_DATE AND ${districtClause}
        `,
        params
      );
      print({
        ...(districtId !== null ? { district_id: districtId } : {}),
        by_status: stats.rows,
        due_now: due.rows[0]?.due_now ?? 0,
        // Oldest already-due date: what an operator acts on first.
        next_due: due.rows[0]?.next_due ?? null,
        next_upcoming: upcoming.rows[0]?.next_upcoming ?? null,
      });
      return;
    }

    default:
      throw new Error(`Unknown command: ${command}\n\n${usage()}`);
  }
}

// Per-command flag sets: a union across commands would let a flag that is
// valid only for ANOTHER command pass validation and be silently ignored
// ("status --note ..." doing nothing) — the exact failure class this guard
// exists to remove. All values are space-form: parseFlags reads the next
// token and does not understand "--name=value".
const COMMAND_FLAG_SPECS: Record<string, readonly CliFlagSpec[]> = {
  record: [
    { name: "--district-id", value: "space" },
    { name: "--election-id", value: "space" },
    { name: "--stage", value: "space" },
    { name: "--reason", value: "space" },
    { name: "--blocked-until", value: "space" },
    { name: "--source-url", value: "space" },
  ],
  due: [
    { name: "--limit", value: "space" },
    { name: "--all", value: "none" },
    { name: "--district-id", value: "space" },
  ],
  resolve: [
    { name: "--deferral-id", value: "space" },
    { name: "--note", value: "space" },
  ],
  cancel: [
    { name: "--deferral-id", value: "space" },
    { name: "--note", value: "space" },
  ],
  status: [{ name: "--district-id", value: "space" }],
};

async function main(): Promise<void> {
  const [command, ...rest] = process.argv.slice(2);
  if (!command || command === "help" || command === "--help") {
    console.log(usage());
    return;
  }
  const specs = COMMAND_FLAG_SPECS[command];
  if (specs) {
    // Before loadProjectEnv so `--help` (handled inside the assert) stays
    // environment-independent, matching every other manual wrapper.
    assertKnownCliFlags(`manual:deferral ${command}`, rest, specs);
  }
  loadProjectEnv();

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
