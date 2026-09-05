// First-party usage analytics — docs/plans/usage-analytics.md.
//
// The event catalog and its validation live here, on the write path: every
// name, route and prop the table can hold is enumerated below, so the
// database can only ever contain what this file allows. Privacy is enforced
// by omission — there is no place in the schema for an address, a district
// id, a candidate id, an election id, a user id or free text.
//
// Validation is per event, not per batch: one bad event is dropped and
// counted, its siblings are stored. A single client bug must never erase a
// whole session's trail.

import type { Pool } from "pg";

export const USAGE_PAYLOAD_VERSION = 1;
export const MAX_USAGE_EVENTS_PER_REQUEST = 40;
// Sessions rotate after 30 idle minutes, but a tab left open can keep one
// alive for days; a week bounds the offset without truncating real use.
const MAX_CLIENT_OFFSET_MS = 7 * 24 * 60 * 60 * 1000;
const MAX_PROPS_BYTES = 300;

// Route ids the frontend maps React Router matches onto (frontend/src/lib/
// usage.ts ROUTE_NAMES). Kept in sync by hand — the two lists are short and
// a drift shows up as dropped events in the report, never as bad data.
export const USAGE_ROUTES = [
  "home",
  "ballot",
  "draft",
  "election",
  "candidate",
  "mission",
  "support",
  "support_member",
  "support_once",
  "disclaimer",
  "terms",
  "privacy",
  "register",
  "login",
  "forgot_password",
  "reset_password",
  "verify_email",
  "verify_email_change",
  "welcome",
  "saved_ballot",
  "picks",
  "follows",
  "settings",
  "pick_card",
  "not_found",
  "other",
] as const;

type PropRule =
  | { kind: "bool" }
  | { kind: "int"; min: number; max: number }
  | { kind: "enum"; values: readonly string[] }
  | { kind: "state_codes" };

const bool: PropRule = { kind: "bool" };
const int = (min: number, max: number): PropRule => ({ kind: "int", min, max });
const oneOf = (...values: string[]): PropRule => ({ kind: "enum", values });

const COUNT_BUCKETS = ["0", "1-3", "4-10", "11-25", "26+"] as const;
const AUTH_STATES = ["unknown", "guest", "signed_in"] as const;

// name → { prop: rule, ... }; `optional` props may be absent, everything else
// must be present. Unknown keys are dropped silently (a newer client than the
// server is not an error), invalid values drop the event.
const CATALOG: Record<string, { required: Record<string, PropRule>; optional?: Record<string, PropRule> }> = {
  session_start: {
    required: {
      referrer_bucket: oneOf("search", "social", "direct", "internal", "other"),
      device: oneOf("phone", "tablet", "desktop"),
      landing_route: oneOf(...USAGE_ROUTES),
      had_saved_draft: bool,
      auth: oneOf(...AUTH_STATES),
    },
  },
  auth_resolved: { required: { auth: oneOf("guest", "signed_in") } },
  page_view: { required: {} },
  page_time: { required: { visible_ms: int(0, 86_400_000) } },
  error_shown: {
    required: { category: oneOf("not_found", "rate_limited", "server", "network", "address", "render", "other") },
  },
  address_input: { required: {} },
  address_suggestion: { required: { granularity: oneOf("address", "zip", "region", "unsupported") } },
  why_address_open: { required: { after_input: bool } },
  address_submit: { required: { via_suggestion: bool } },
  terms_shown: { required: {} },
  terms_decision: { required: { decision: oneOf("agree", "cancel"), open_ms: int(0, 3_600_000) } },
  terms_doc_open: { required: { doc: oneOf("terms", "privacy", "disclaimer") } },
  address_result: {
    required: { outcome: oneOf("exact", "zip", "region", "error"), latency_ms: int(0, 120_000) },
    optional: { error_category: oneOf("address", "not_found", "rate_limited", "server", "network", "other") },
  },
  ballot_result: {
    required: {
      outcome: oneOf("ready", "empty", "error"),
      scope: oneOf("exact", "zip", "region", "unknown"),
      states: { kind: "state_codes" },
      election_count_bucket: oneOf(...COUNT_BUCKETS),
      district_count_bucket: oneOf(...COUNT_BUCKETS),
      partial_banner: bool,
      ambiguous_banner: bool,
    },
  },
  list_control: {
    required: {
      control: oneOf(
        "sort",
        "filter_issues",
        "filter_impact",
        "show_all",
        "race_tab",
        "how_to_vote",
        "how_to_vote_link",
        "followed_first",
        "view_toggle"
      ),
      value: oneOf(
        // sort values (BALLOT_SORTS) and rail-equivalent words
        "vote_power",
        "soonest",
        "district_size",
        "district_size_smallest",
        "my_areas",
        "state_baseline",
        // booleans and thresholds
        "on",
        "off",
        "high",
        "medium",
        "any",
        // race tabs / views / links
        "all",
        "office",
        "ballot_measure",
        "list",
        "ballot",
        "open",
        "close",
        "mail",
        "polling",
        "other"
      ),
    },
  },
  election_open: {
    required: {
      race_type: oneOf("office", "ballot_measure"),
      vote_power: oneOf("very_low", "low", "medium", "high", "very_high", "unknown"),
      position_bucket: oneOf("1-3", "4-10", "11+"),
      awaiting: bool,
    },
  },
  partial_upgrade_click: { required: { banner: oneOf("partial", "ambiguous") } },
};

export const USAGE_EVENT_NAMES: readonly string[] = Object.keys(CATALOG);

export type UsageEventRow = {
  eventId: string;
  sessionId: string;
  pageViewId: string | null;
  name: string;
  route: string;
  clientOffsetMs: number;
  props: Record<string, unknown>;
};

export type ParsedUsageEvents = {
  accepted: UsageEventRow[];
  dropped: number;
};

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const STATE_CODE_RE = /^[A-Z]{2}$/;

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isUuid(value: unknown): value is string {
  return typeof value === "string" && UUID_RE.test(value);
}

function checkProp(rule: PropRule, value: unknown): boolean {
  switch (rule.kind) {
    case "bool":
      return typeof value === "boolean";
    case "int":
      return typeof value === "number" && Number.isInteger(value) && value >= rule.min && value <= rule.max;
    case "enum":
      return typeof value === "string" && rule.values.includes(value);
    case "state_codes":
      return (
        Array.isArray(value) &&
        value.length <= 3 &&
        value.every((entry) => typeof entry === "string" && STATE_CODE_RE.test(entry))
      );
  }
}

/** One event → a row, or null when anything about it is outside the catalog. */
export function parseUsageEvent(value: unknown): UsageEventRow | null {
  if (!isRecord(value)) {
    return null;
  }
  const { event_id, session_id, page_view_id, name, route, client_offset_ms, props } = value;
  if (!isUuid(event_id) || !isUuid(session_id)) {
    return null;
  }
  if (page_view_id !== null && page_view_id !== undefined && !isUuid(page_view_id)) {
    return null;
  }
  if (typeof name !== "string" || !Object.hasOwn(CATALOG, name)) {
    return null;
  }
  if (typeof route !== "string" || !(USAGE_ROUTES as readonly string[]).includes(route)) {
    return null;
  }
  if (
    typeof client_offset_ms !== "number" ||
    !Number.isInteger(client_offset_ms) ||
    client_offset_ms < 0 ||
    client_offset_ms > MAX_CLIENT_OFFSET_MS
  ) {
    return null;
  }
  const rawProps = props === undefined || props === null ? {} : props;
  if (!isRecord(rawProps)) {
    return null;
  }
  const schema = CATALOG[name]!;
  const kept: Record<string, unknown> = {};
  for (const [key, rule] of Object.entries(schema.required)) {
    if (!checkProp(rule, rawProps[key])) {
      return null;
    }
    kept[key] = rawProps[key];
  }
  for (const [key, rule] of Object.entries(schema.optional ?? {})) {
    const entry = rawProps[key];
    if (entry === undefined || entry === null) {
      continue;
    }
    if (!checkProp(rule, entry)) {
      return null;
    }
    kept[key] = entry;
  }
  if (JSON.stringify(kept).length > MAX_PROPS_BYTES) {
    return null;
  }
  return {
    eventId: event_id.toLowerCase(),
    sessionId: session_id.toLowerCase(),
    pageViewId: typeof page_view_id === "string" ? page_view_id.toLowerCase() : null,
    name,
    route,
    clientOffsetMs: client_offset_ms,
    props: kept,
  };
}

/**
 * Request body → accepted rows + dropped count. Throws (→ 400 via the
 * TypeError mapping) only when the envelope itself is unusable; individual
 * bad events never fail the request.
 */
export function parseUsageEventsBodyValue(parsed: unknown): ParsedUsageEvents {
  if (!isRecord(parsed)) {
    throw new TypeError("Request body must be a JSON object");
  }
  if (parsed.v !== USAGE_PAYLOAD_VERSION) {
    throw new TypeError(`v must be ${USAGE_PAYLOAD_VERSION}`);
  }
  const events = parsed.events;
  if (!Array.isArray(events) || events.length === 0) {
    throw new TypeError("events must be a non-empty array");
  }
  if (events.length > MAX_USAGE_EVENTS_PER_REQUEST) {
    throw new TypeError(`events must contain at most ${MAX_USAGE_EVENTS_PER_REQUEST} items`);
  }
  const accepted: UsageEventRow[] = [];
  let dropped = 0;
  for (const entry of events) {
    const row = parseUsageEvent(entry);
    if (row) {
      accepted.push(row);
    } else {
      dropped += 1;
    }
  }
  return { accepted, dropped };
}

/** One bounded multi-row insert; duplicates (client retries) are no-ops.
 * Untargeted ON CONFLICT on purpose: naming the arbiter column (`ON CONFLICT
 * (event_id)`) makes Postgres require SELECT on it, which the INSERT-only API
 * role does not have (42501 verified). The primary key is the table's only
 * unique constraint, so the two forms mean the same thing. */
export async function insertUsageEvents(db: Pool, rows: readonly UsageEventRow[]): Promise<void> {
  if (rows.length === 0) {
    return;
  }
  await db.query(
    `
      INSERT INTO usage.events (event_id, session_id, page_view_id, name, route, client_offset_ms, v, props)
      SELECT * FROM unnest(
        $1::uuid[], $2::uuid[], $3::uuid[], $4::text[], $5::text[], $6::integer[], $7::smallint[], $8::jsonb[]
      )
      ON CONFLICT DO NOTHING
    `,
    [
      rows.map((row) => row.eventId),
      rows.map((row) => row.sessionId),
      rows.map((row) => row.pageViewId),
      rows.map((row) => row.name),
      rows.map((row) => row.route),
      rows.map((row) => row.clientOffsetMs),
      rows.map(() => USAGE_PAYLOAD_VERSION),
      rows.map((row) => JSON.stringify(row.props)),
    ]
  );
}
