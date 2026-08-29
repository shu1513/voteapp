import type { Pool, PoolClient } from "pg";

import type { MemberNewsletterMailer } from "./memberNewsletterMailer.js";

type Queryable = Pick<Pool | PoolClient, "query">;

// Operator-sent member newsletter: one email per user with an ACTIVE monthly
// membership who has not opted out (Terms 14.5 member communications).
// Deduplicated per (newsletter_id, user_id) in member_newsletter_sends, so
// re-running the same newsletter id resumes instead of double-sending; the
// dedupe row is inserted only after a successful send (at-least-once: a
// crash between send and insert may duplicate an email, never lose one).
// Mirrors pipeline/users/issueBroadcast.ts.
//
// This is the pipeline function a future admin page calls through an API
// route; the CLI (scripts/sendMemberNewsletter.ts) is a thin wrapper today.

export const DEFAULT_NEWSLETTER_MAX_USERS = 500;

export class MemberNewsletterError extends Error {
  constructor(
    readonly code: "invalid_newsletter",
    message: string
  ) {
    super(message);
    this.name = "MemberNewsletterError";
  }
}

export type SendMemberNewsletterOptions = {
  live: boolean;
  /** Operator-chosen stable id; re-runs with the same id resume, not resend. */
  newsletterId: string;
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

export type SendMemberNewsletterResult = {
  dryRun: boolean;
  newsletterId: string;
  /** Recipients due this newsletter that this run examined (all of them). */
  eligibleUserCount: number;
  /** Newsletter emails actually sent (includes sends whose mark step then failed). */
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

type RecipientRow = {
  id: string;
  email: string;
  first_name: string;
};

async function selectRecipients(
  db: Queryable,
  newsletterId: string,
  batchSize: number,
  excludedUserIds: readonly string[]
): Promise<RecipientRow[]> {
  // excludedUserIds carries the users this run already attempted: a failed
  // send never gets a dedupe row, so without the exclusion it would be
  // re-selected forever. Membership = an active subscription row; the
  // status-sync webhooks keep stripe_status current, and `active` matches
  // the paid-up definition the UI uses (MembershipThanks, Settings).
  const result = await db.query<RecipientRow>(
    `
      SELECT u.id, u.email, u.first_name
      FROM public.users AS u
      WHERE u.deleted_at IS NULL
        AND u.email_verified = true
        AND u.email_member_newsletter = true
        AND u.id <> ALL($3::uuid[])
        AND EXISTS (
          SELECT 1
          FROM public.billing_customers AS customer
          JOIN public.billing_subscriptions AS subscription
            ON subscription.billing_customer_id = customer.id
          WHERE customer.user_id = u.id
            AND subscription.stripe_status = 'active'
        )
        AND NOT EXISTS (
          SELECT 1
          FROM public.member_newsletter_sends AS send
          WHERE send.user_id = u.id
            AND send.newsletter_id = $1
        )
      ORDER BY u.id
      LIMIT $2::int
    `,
    [newsletterId, batchSize, excludedUserIds]
  );
  return result.rows;
}

async function markNewsletterSent(db: Queryable, newsletterId: string, userId: string): Promise<void> {
  await db.query(
    `
      INSERT INTO public.member_newsletter_sends (newsletter_id, user_id)
      VALUES ($1, $2::uuid)
      ON CONFLICT DO NOTHING
    `,
    [newsletterId, userId]
  );
}

export async function sendMemberNewsletter(
  db: Queryable,
  mailer: MemberNewsletterMailer,
  options: SendMemberNewsletterOptions
): Promise<SendMemberNewsletterResult> {
  const newsletterId = options.newsletterId.trim();
  if (newsletterId.length === 0) {
    throw new MemberNewsletterError("invalid_newsletter", "newsletterId is required");
  }
  if (options.subject.trim().length === 0) {
    throw new MemberNewsletterError("invalid_newsletter", "subject is required");
  }
  if (options.body.trim().length === 0) {
    throw new MemberNewsletterError("invalid_newsletter", "body is required");
  }

  const result: SendMemberNewsletterResult = {
    dryRun: !options.live,
    newsletterId,
    eligibleUserCount: 0,
    usersEmailedCount: 0,
    usersMarkedCount: 0,
    failures: [],
  };
  const batchSize = options.batchSize ?? DEFAULT_NEWSLETTER_MAX_USERS;
  if (!Number.isInteger(batchSize) || batchSize <= 0) {
    throw new MemberNewsletterError("invalid_newsletter", "batchSize must be a positive integer");
  }

  // Loops through batches until every eligible recipient is processed — a
  // newsletter is one audience, one run. The exclusion list carries only
  // users the run attempted but could NOT stamp with a dedupe row (dry-run
  // recipients, failed sends, failed marks); successfully marked users are
  // already excluded by the NOT EXISTS in the selection, so the parameter
  // array stays bounded by failures, not audience size. Every selected user
  // either gains a dedupe row or joins the list, so each non-empty batch
  // strictly shrinks the remaining set.
  const unmarkedUserIds: string[] = [];
  for (;;) {
    const recipients = await selectRecipients(db, newsletterId, batchSize, unmarkedUserIds);
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
        await mailer.sendNewsletterEmail({
          email: recipient.email,
          firstName: recipient.first_name,
          subject: options.subject,
          body: options.body,
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
        await markNewsletterSent(db, newsletterId, recipient.id);
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
 * App-unique advisory lock key for live newsletter runs (digest 74_310_146,
 * alerts 74_310_147, reminders 74_310_148, issue broadcast 74_310_149).
 */
export const MEMBER_NEWSLETTER_RUN_LOCK_KEY = 74_310_150;

/**
 * Serializes live newsletter runs across processes with a Postgres session
 * advisory lock. The lock must live on one dedicated connection for the whole
 * run — pool.query() hops connections — so this checks out a client and holds
 * it until fn settles. Returns null without calling fn when another run
 * already holds the lock.
 */
export async function withMemberNewsletterRunLock<T>(
  pool: Pick<Pool, "connect">,
  fn: () => Promise<T>
): Promise<T | null> {
  const client = await pool.connect();
  let locked = false;
  try {
    const acquired = await client.query<{ locked: boolean }>(
      "SELECT pg_try_advisory_lock($1) AS locked",
      [MEMBER_NEWSLETTER_RUN_LOCK_KEY]
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
        await client.query("SELECT pg_advisory_unlock($1)", [MEMBER_NEWSLETTER_RUN_LOCK_KEY]);
      }
    } catch {
      // The session lock dies with the connection; nothing to clean up.
    } finally {
      client.release();
    }
  }
}
