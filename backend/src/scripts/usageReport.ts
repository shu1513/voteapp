// npm run usage:report [-- --days 7] — the first-party usage analytics
// report (docs/plans/usage-analytics.md "Reports"). Read-only, owner role.
//
// Every table prints its denominator and, where a value can be unknown,
// says so — cohorts are separated (landed on the home page vs a deep link
// vs signed in) rather than forced through one funnel. Sessions are
// navigation sessions (a tab, rotated after 30 idle minutes), not people.

import { Pool } from "pg";

import { loadProjectEnv } from "../config/env.js";

function readDaysArg(argv: readonly string[]): number {
  const index = argv.indexOf("--days");
  const raw = index >= 0 ? Number(argv[index + 1]) : NaN;
  return Number.isInteger(raw) && raw > 0 && raw <= 90 ? raw : 7;
}

type Row = Record<string, string | number | null>;

function printTable(title: string, rows: Row[]): void {
  console.log(`\n== ${title}`);
  if (rows.length === 0) {
    console.log("(no rows)");
    return;
  }
  console.table(rows);
}

async function main(): Promise<void> {
  loadProjectEnv();
  const days = readDaysArg(process.argv.slice(2));
  const pool = new Pool({
    connectionString: process.env.DATABASE_URL?.trim() || "postgresql://localhost:5432/voteapp",
  });
  try {
    const since = `now() - interval '${days} days'`;
    console.log(`Usage report — last ${days} day(s), by server receipt time`);

    // 1. Sessions: how many, from where, on what.
    const sessions = await pool.query<Row>(`
      WITH starts AS (
        SELECT session_id, props FROM usage.events WHERE name = 'session_start' AND received_at >= ${since}
      ),
      auth AS (
        SELECT DISTINCT ON (session_id) session_id, props->>'auth' AS auth
        FROM usage.events WHERE name = 'auth_resolved' AND received_at >= ${since}
        ORDER BY session_id, client_offset_ms
      )
      SELECT
        count(*)::int AS sessions,
        count(*) FILTER (WHERE s.props->>'device' = 'phone')::int AS phone,
        count(*) FILTER (WHERE s.props->>'device' = 'desktop')::int AS desktop,
        count(*) FILTER (WHERE s.props->>'referrer_bucket' = 'search')::int AS from_search,
        count(*) FILTER (WHERE s.props->>'referrer_bucket' = 'social')::int AS from_social,
        count(*) FILTER (WHERE s.props->>'referrer_bucket' = 'direct')::int AS direct,
        count(*) FILTER (WHERE (s.props->>'had_saved_draft')::boolean)::int AS had_saved_draft,
        count(*) FILTER (WHERE a.auth = 'signed_in')::int AS signed_in,
        count(*) FILTER (WHERE a.auth = 'guest')::int AS guest,
        count(*) FILTER (WHERE a.auth IS NULL)::int AS auth_unknown
      FROM starts s LEFT JOIN auth a USING (session_id)
    `);
    printTable("Sessions", sessions.rows);

    const landing = await pool.query<Row>(`
      SELECT props->>'landing_route' AS landing_route, count(*)::int AS sessions
      FROM usage.events WHERE name = 'session_start' AND received_at >= ${since}
      GROUP BY 1 ORDER BY 2 DESC
    `);
    printTable("Landing route", landing.rows);

    // 2. Search completion, home-page entrants only (a session that ever
    //    viewed the home page).
    const search = await pool.query<Row>(`
      WITH home AS (
        SELECT DISTINCT session_id FROM usage.events
        WHERE name = 'page_view' AND route = 'home' AND received_at >= ${since}
      ),
      per AS (
        SELECT h.session_id,
          bool_or(e.name = 'address_input') AS typed,
          bool_or(e.name = 'address_suggestion') AS picked_suggestion,
          bool_or(e.name = 'why_address_open') AS opened_why,
          bool_or(e.name = 'terms_shown') AS terms_shown,
          bool_or(e.name = 'terms_decision' AND e.props->>'decision' = 'agree') AS terms_agreed,
          bool_or(e.name = 'terms_decision' AND e.props->>'decision' = 'cancel') AS terms_cancelled,
          bool_or(e.name = 'address_submit') AS submitted,
          bool_or(e.name = 'address_result' AND e.props->>'outcome' IN ('exact','zip','region')) AS resolved,
          bool_or(e.name = 'address_result' AND e.props->>'outcome' = 'error') AS resolve_error,
          bool_or(e.name = 'ballot_result' AND e.props->>'outcome' = 'ready') AS ballot_ready
        FROM home h JOIN usage.events e ON e.session_id = h.session_id AND e.received_at >= ${since}
        GROUP BY h.session_id
      )
      SELECT
        count(*)::int AS home_sessions,
        count(*) FILTER (WHERE typed)::int AS typed,
        count(*) FILTER (WHERE picked_suggestion)::int AS picked_suggestion,
        count(*) FILTER (WHERE opened_why)::int AS opened_why_address,
        count(*) FILTER (WHERE submitted)::int AS submitted,
        count(*) FILTER (WHERE terms_shown)::int AS terms_shown,
        count(*) FILTER (WHERE terms_agreed)::int AS terms_agreed,
        count(*) FILTER (WHERE terms_cancelled)::int AS terms_cancelled,
        count(*) FILTER (WHERE resolved)::int AS resolved,
        count(*) FILTER (WHERE resolve_error)::int AS resolve_error,
        count(*) FILTER (WHERE ballot_ready)::int AS ballot_ready
      FROM per
    `);
    printTable("Search completion (home-page entrants)", search.rows);

    const resolveOutcomes = await pool.query<Row>(`
      SELECT props->>'outcome' AS outcome, props->>'error_category' AS error_category,
        count(*)::int AS results,
        round((percentile_cont(0.5) WITHIN GROUP (ORDER BY (props->>'latency_ms')::int))::numeric)::int AS p50_latency_ms,
        round((percentile_cont(0.95) WITHIN GROUP (ORDER BY (props->>'latency_ms')::int))::numeric)::int AS p95_latency_ms
      FROM usage.events WHERE name = 'address_result' AND received_at >= ${since}
      GROUP BY 1, 2 ORDER BY 3 DESC
    `);
    printTable("Address results", resolveOutcomes.rows);

    // 3. Ballot usefulness and exploration.
    const ballots = await pool.query<Row>(`
      SELECT route, props->>'outcome' AS outcome, props->>'scope' AS scope,
        props->>'election_count_bucket' AS elections, count(*)::int AS loads,
        count(*) FILTER (WHERE (props->>'partial_banner')::boolean)::int AS partial_banner,
        count(*) FILTER (WHERE (props->>'ambiguous_banner')::boolean)::int AS ambiguous_banner
      FROM usage.events WHERE name = 'ballot_result' AND received_at >= ${since}
      GROUP BY 1, 2, 3, 4 ORDER BY 5 DESC
    `);
    printTable("Ballot loads", ballots.rows);

    const exploration = await pool.query<Row>(`
      WITH ready AS (
        SELECT DISTINCT session_id FROM usage.events
        WHERE name = 'ballot_result' AND props->>'outcome' = 'ready' AND received_at >= ${since}
      ),
      opens AS (
        SELECT session_id, count(*)::int AS opens FROM usage.events
        WHERE name = 'election_open' AND received_at >= ${since} GROUP BY session_id
      ),
      controls AS (
        SELECT DISTINCT session_id FROM usage.events
        WHERE name = 'list_control' AND received_at >= ${since}
      )
      SELECT
        count(*)::int AS sessions_with_ready_ballot,
        count(o.session_id)::int AS opened_an_election,
        count(*) FILTER (WHERE o.opens >= 2)::int AS opened_two_or_more,
        count(c.session_id)::int AS used_a_list_control
      FROM ready r LEFT JOIN opens o USING (session_id) LEFT JOIN controls c USING (session_id)
    `);
    printTable("Exploration (sessions with a ready ballot)", exploration.rows);

    const opens = await pool.query<Row>(`
      SELECT props->>'race_type' AS race_type, props->>'position_bucket' AS position, props->>'vote_power' AS vote_power,
        count(*)::int AS opens
      FROM usage.events WHERE name = 'election_open' AND received_at >= ${since}
      GROUP BY 1, 2, 3 ORDER BY 4 DESC LIMIT 20
    `);
    printTable("Election opens by race type / position / vote power", opens.rows);

    const listControls = await pool.query<Row>(`
      SELECT props->>'control' AS control, props->>'value' AS value, count(*)::int AS uses
      FROM usage.events WHERE name = 'list_control' AND received_at >= ${since}
      GROUP BY 1, 2 ORDER BY 3 DESC
    `);
    printTable("List controls", listControls.rows);

    // 4. Visible time per page view (max checkpoint), by route.
    const pageTime = await pool.query<Row>(`
      WITH per_view AS (
        SELECT page_view_id, route, max((props->>'visible_ms')::int) AS visible_ms
        FROM usage.events WHERE name = 'page_time' AND page_view_id IS NOT NULL AND received_at >= ${since}
        GROUP BY page_view_id, route
      ),
      views AS (
        SELECT route, count(*)::int AS page_views FROM usage.events
        WHERE name = 'page_view' AND received_at >= ${since} GROUP BY route
      )
      SELECT v.route, v.page_views,
        count(p.page_view_id)::int AS with_time,
        (v.page_views - count(p.page_view_id))::int AS time_unknown,
        round((percentile_cont(0.5) WITHIN GROUP (ORDER BY p.visible_ms) / 1000.0)::numeric, 1) AS p50_visible_s,
        round((percentile_cont(0.9) WITHIN GROUP (ORDER BY p.visible_ms) / 1000.0)::numeric, 1) AS p90_visible_s
      FROM views v LEFT JOIN per_view p USING (route)
      GROUP BY v.route, v.page_views ORDER BY v.page_views DESC
    `);
    printTable("Visible time by route", pageTime.rows);

    // 4b. Research → action (PR 2): what a detail-page view leads to, split
    //     office vs measure. Denominators are page views, not sessions.
    const research = await pool.query<Row>(`
      WITH views AS (
        SELECT page_view_id, route, COALESCE(props->>'race_type', 'n/a') AS race_type
        FROM usage.events
        WHERE name = 'page_view' AND route IN ('election', 'candidate') AND page_view_id IS NOT NULL
          AND received_at >= ${since}
      ),
      per AS (
        SELECT v.page_view_id, v.route, v.race_type,
          bool_or(e.name = 'section_exposed' AND e.props->>'section' IN ('candidates', 'summary', 'measure_summary')) AS saw_main,
          bool_or(e.name = 'section_exposed' AND e.props->>'section' = 'measure_yes_no') AS saw_yes_no,
          bool_or(e.name = 'candidate_open') AS opened_candidate,
          bool_or(e.name = 'official_source_click') AS clicked_source,
          bool_or(e.name = 'pick_attempt') AS pick_attempt,
          bool_or(e.name = 'pick_result' AND e.props->>'outcome' IN ('saved', 'draft_memory')) AS pick_recorded,
          bool_or(e.name = 'pick_result' AND e.props->>'outcome' = 'error') AS pick_error,
          bool_or(e.name = 'address_nudge_click') AS nudge_click
        FROM views v LEFT JOIN usage.events e ON e.page_view_id = v.page_view_id AND e.received_at >= ${since}
        GROUP BY v.page_view_id, v.route, v.race_type
      )
      SELECT route, race_type, count(*)::int AS page_views,
        count(*) FILTER (WHERE saw_main)::int AS saw_main_section,
        count(*) FILTER (WHERE saw_yes_no)::int AS saw_yes_no,
        count(*) FILTER (WHERE opened_candidate)::int AS opened_candidate,
        count(*) FILTER (WHERE clicked_source)::int AS clicked_source,
        count(*) FILTER (WHERE pick_attempt)::int AS pick_attempted,
        count(*) FILTER (WHERE pick_recorded)::int AS pick_recorded,
        count(*) FILTER (WHERE pick_error)::int AS pick_error,
        count(*) FILTER (WHERE nudge_click)::int AS address_nudge_click
      FROM per GROUP BY 1, 2 ORDER BY 3 DESC
    `);
    printTable("Research → action (per detail page view)", research.rows);

    const picks = await pool.query<Row>(`
      SELECT r.props->>'kind' AS kind, r.props->>'surface' AS surface, r.props->>'store' AS store,
        r.props->>'change' AS change, r.props->>'outcome' AS outcome, count(*)::int AS results,
        round((percentile_cont(0.5) WITHIN GROUP (ORDER BY (a.props->>'ms_since_view')::int) / 1000.0)::numeric, 1) AS p50_s_since_view
      FROM usage.events r
      LEFT JOIN LATERAL (
        SELECT a.props FROM usage.events a
        WHERE a.name = 'pick_attempt' AND a.page_view_id = r.page_view_id AND a.client_offset_ms <= r.client_offset_ms
        ORDER BY a.client_offset_ms DESC LIMIT 1
      ) a ON true
      WHERE r.name = 'pick_result' AND r.received_at >= ${since}
      GROUP BY 1, 2, 3, 4, 5 ORDER BY 6 DESC
    `);
    printTable("Pick results by surface", picks.rows);

    const arrivals = await pool.query<Row>(`
      SELECT route, props->>'arrival' AS arrival, count(*)::int AS page_views
      FROM usage.events WHERE name = 'page_view' AND route IN ('election', 'candidate') AND received_at >= ${since}
      GROUP BY 1, 2 ORDER BY 1, 3 DESC
    `);
    printTable("Detail-page arrivals", arrivals.rows);

    // 5b. Guest retention (PR 2): draft review, sign-up prompts, auth,
    //     handoff.
    const retention = await pool.query<Row>(`
      SELECT name, props->>'source' AS source, props->>'action' AS action, props->>'outcome' AS outcome,
        props->>'store' AS store, count(*)::int AS events
      FROM usage.events
      WHERE name IN ('draft_review', 'signup_prompt', 'auth_result', 'handoff_result', 'welcome_result', 'draft_complete_notice', 'follow_result')
        AND received_at >= ${since}
      GROUP BY 1, 2, 3, 4, 5 ORDER BY 1, 6 DESC
    `);
    printTable("Guest retention, sign-up, follow", retention.rows);

    // 5. What breaks.
    const errors = await pool.query<Row>(`
      SELECT route, props->>'category' AS category, count(*)::int AS shown
      FROM usage.events WHERE name = 'error_shown' AND received_at >= ${since}
      GROUP BY 1, 2 ORDER BY 3 DESC
    `);
    printTable("Errors shown", errors.rows);
  } finally {
    await pool.end();
  }
}

main().catch((error: unknown) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
