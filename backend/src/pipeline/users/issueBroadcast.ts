import type { Pool, PoolClient } from "pg";

import type { IssueBroadcastMailer } from "./issueBroadcastMailer.js";

type Queryable = Pick<Pool | PoolClient, "query">;

// Operator-sent issue broadcast: one email per user whose saved research
// areas intersect the broadcast's target areas. Deduplicated per
// (broadcast_id, user_id) in issue_broadcast_sends, so re-running the same
// broadcast id resumes instead of double-sending; the dedupe row is inserted
// only after a successful send (at-least-once: a crash between send and
// insert may duplicate an email, never lose one).
//
// This is the pipeline function a future admin page calls through an API
// route; the CLI (scripts/sendIssueBroadcast.ts) is a thin wrapper today.

export const DEFAULT_BROADCAST_MAX_USERS = 500;

export class IssueBroadcastError extends Error {
  constructor(
    readonly code: "invalid_broadcast" | "unknown_research_area_slugs",
    message: string
  ) {
    super(message);
    this.name = "IssueBroadcastError";
  }
}

export type SendIssueBroadcastOptions = {
  live: boolean;
  /** Operator-chosen stable id; re-runs with the same id resume, not resend. */
  broadcastId: string;
  /** Target research areas by slug; recipients saved at least one of them. */
  areaSlugs: readonly string[];
  subject: string;
  body: string;
  /**
   * Selection batch size, not a total cap: the run loops until every
   * eligible recipient is processed.
   */
  batchSize?: number;
  /** Per-user signed unsubscribe link builder; omit to send without one. */
  buildUnsubscribeUrl?: (userId: string) => string;
};

export type SendIssueBroadcastResult = {
  dryRun: boolean;
  broadcastId: string;
  targetAreas: Array<{ id: string; slug: string; name: string }>;
  /** Recipients due this broadcast that this run examined (all of them). */
  eligibleUserCount: number;
  /** Broadcast emails actually sent (includes sends whose mark step then failed). */
  usersEmailedCount: number;
  /** Users both emailed and stamped in the dedupe log. */
  usersMarkedCount: number;
  /**
   * stage "send": the email did not go out; a re-run retries it.
   * stage "mark_after_send": the email DID go out but the dedupe insert
   * failed, so a re-run would re-send (at-least-once).
   */
  failures: Array<{ userId: string; stage: "send" | "mark_after_send"; reason: string }>;
};

type ResearchAreaRow = {
  id: string;
  slug: string;
  name: string;
};

async function resolveTargetAreas(db: Queryable, areaSlugs: readonly string[]): Promise<ResearchAreaRow[]> {
  const normalized = [...new Set(areaSlugs.map((slug) => slug.trim()).filter((slug) => slug.length > 0))];
  if (normalized.length === 0) {
    throw new IssueBroadcastError("invalid_broadcast", "At least one research area slug is required");
  }
  const result = await db.query<ResearchAreaRow>(
    `
      SELECT id, slug, name
      FROM public.research_areas
      WHERE slug = ANY($1::text[])
      ORDER BY slug
    `,
    [normalized]
  );
  const found = new Set(result.rows.map((row) => row.slug));
  const unknown = normalized.filter((slug) => !found.has(slug));
  if (unknown.length > 0) {
    throw new IssueBroadcastError(
      "unknown_research_area_slugs",
      `Unknown research area slugs: ${unknown.join(", ")}`
    );
  }
  return result.rows;
}

type RecipientRow = {
  id: string;
  email: string;
  first_name: string;
  matched_area_names: string[];
};

async function selectRecipients(
  db: Queryable,
  broadcastId: string,
  areaIds: readonly string[],
  batchSize: number,
  excludedUserIds: readonly string[]
): Promise<RecipientRow[]> {
  // excludedUserIds carries the users this run already attempted: a failed
  // send never gets a dedupe row, so without the exclusion it would be
  // re-selected forever.
  const result = await db.query<RecipientRow>(
    `
      SELECT
        u.id,
        u.email,
        u.first_name,
        ARRAY(
          SELECT area.name
          FROM public.user_research_area_preferences AS preference
          JOIN public.research_areas AS area
            ON area.id = preference.research_area_id
          WHERE preference.user_id = u.id
            AND preference.research_area_id = ANY($2::uuid[])
          ORDER BY area.name
        ) AS matched_area_names
      FROM public.users AS u
      WHERE u.deleted_at IS NULL
        AND u.email_verified = true
        AND u.email_issue_updates = true
        AND u.id <> ALL($4::uuid[])
        AND EXISTS (
          SELECT 1
          FROM public.user_research_area_preferences AS preference
          WHERE preference.user_id = u.id
            AND preference.research_area_id = ANY($2::uuid[])
        )
        AND NOT EXISTS (
          SELECT 1
          FROM public.issue_broadcast_sends AS send
          WHERE send.user_id = u.id
            AND send.broadcast_id = $1
        )
      ORDER BY u.id
      LIMIT $3::int
    `,
    [broadcastId, areaIds, batchSize, excludedUserIds]
  );
  return result.rows;
}

async function markBroadcastSent(db: Queryable, broadcastId: string, userId: string): Promise<void> {
  await db.query(
    `
      INSERT INTO public.issue_broadcast_sends (broadcast_id, user_id)
      VALUES ($1, $2::uuid)
      ON CONFLICT DO NOTHING
    `,
    [broadcastId, userId]
  );
}

export async function sendIssueBroadcast(
  db: Queryable,
  mailer: IssueBroadcastMailer,
  options: SendIssueBroadcastOptions
): Promise<SendIssueBroadcastResult> {
  const broadcastId = options.broadcastId.trim();
  if (broadcastId.length === 0) {
    throw new IssueBroadcastError("invalid_broadcast", "broadcastId is required");
  }
  if (options.subject.trim().length === 0) {
    throw new IssueBroadcastError("invalid_broadcast", "subject is required");
  }
  if (options.body.trim().length === 0) {
    throw new IssueBroadcastError("invalid_broadcast", "body is required");
  }

  const targetAreas = await resolveTargetAreas(db, options.areaSlugs);
  const result: SendIssueBroadcastResult = {
    dryRun: !options.live,
    broadcastId,
    targetAreas,
    eligibleUserCount: 0,
    usersEmailedCount: 0,
    usersMarkedCount: 0,
    failures: [],
  };
  const areaIds = targetAreas.map((area) => area.id);
  const batchSize = options.batchSize ?? DEFAULT_BROADCAST_MAX_USERS;
  if (!Number.isInteger(batchSize) || batchSize <= 0) {
    throw new IssueBroadcastError("invalid_broadcast", "batchSize must be a positive integer");
  }

  // Loops through batches until every eligible recipient is processed —
  // a broadcast is one audience, one run. The exclusion list carries only
  // users the run attempted but could NOT stamp with a dedupe row (dry-run
  // recipients, failed sends, failed marks); successfully marked users are
  // already excluded by the NOT EXISTS in the selection, so the parameter
  // array stays bounded by failures, not audience size. Every selected user
  // either gains a dedupe row or joins the list, so each non-empty batch
  // strictly shrinks the remaining set.
  const unmarkedUserIds: string[] = [];
  for (;;) {
    const recipients = await selectRecipients(db, broadcastId, areaIds, batchSize, unmarkedUserIds);
    if (recipients.length === 0) {
      break;
    }
    for (const recipient of recipients) {
      result.eligibleUserCount += 1;
      if (!options.live) {
        unmarkedUserIds.push(recipient.id);
        continue;
      }

      try {
        const unsubscribeUrl = options.buildUnsubscribeUrl?.(recipient.id);
        await mailer.sendBroadcastEmail({
          email: recipient.email,
          firstName: recipient.first_name,
          subject: options.subject,
          body: options.body,
          matchedAreaNames: recipient.matched_area_names,
          ...(unsubscribeUrl ? { unsubscribeUrl } : {}),
        });
      } catch (error) {
        const reason = error instanceof Error ? error.message : String(error);
        result.failures.push({ userId: recipient.id, stage: "send", reason });
        unmarkedUserIds.push(recipient.id);
        continue;
      }
      result.usersEmailedCount += 1;

      try {
        await markBroadcastSent(db, broadcastId, recipient.id);
        result.usersMarkedCount += 1;
      } catch (error) {
        const reason = error instanceof Error ? error.message : String(error);
        result.failures.push({ userId: recipient.id, stage: "mark_after_send", reason });
        unmarkedUserIds.push(recipient.id);
      }
    }
  }

  return result;
}

/**
 * App-unique advisory lock key for live broadcast runs (digest 74_310_146,
 * alerts 74_310_147, reminders 74_310_148).
 */
export const ISSUE_BROADCAST_RUN_LOCK_KEY = 74_310_149;

/**
 * Serializes live broadcast runs across processes with a Postgres session
 * advisory lock. The lock must live on one dedicated connection for the whole
 * run — pool.query() hops connections — so this checks out a client and holds
 * it until fn settles. Returns null without calling fn when another run
 * already holds the lock.
 */
export async function withIssueBroadcastRunLock<T>(
  pool: Pick<Pool, "connect">,
  fn: () => Promise<T>
): Promise<T | null> {
  const client = await pool.connect();
  let locked = false;
  try {
    const acquired = await client.query<{ locked: boolean }>(
      "SELECT pg_try_advisory_lock($1) AS locked",
      [ISSUE_BROADCAST_RUN_LOCK_KEY]
    );
    locked = acquired.rows[0]?.locked === true;
    if (!locked) {
      return null;
    }
    return await fn();
  } finally {
    // release() must survive an unlock failure (e.g. the connection died
    // mid-run): leaking the client would pin a pool slot for the process
    // lifetime.
    try {
      if (locked) {
        await client.query("SELECT pg_advisory_unlock($1)", [ISSUE_BROADCAST_RUN_LOCK_KEY]);
      }
    } finally {
      client.release();
    }
  }
}
