import { describe, expect, it, vi } from "vitest";

import {
  MEMBER_NEWSLETTER_RUN_LOCK_KEY,
  MemberNewsletterError,
  sendMemberNewsletter,
  withMemberNewsletterRunLock,
} from "../../../src/pipeline/users/memberNewsletter.js";
import { ISSUE_BROADCAST_RUN_LOCK_KEY } from "../../../src/pipeline/users/issueBroadcast.js";
import type { MemberNewsletterEmailInput } from "../../../src/pipeline/users/memberNewsletterMailer.js";

const USER_ALPHA = "11111111-1111-4111-8111-111111111111";
const USER_BETA = "22222222-2222-4222-8222-222222222222";

const baseOptions = {
  live: true,
  newsletterId: "2026-09-analysis",
  subject: "Member analysis: September",
  body: "Here is what we found.",
};

// Routes the sender's statements by their distinguishing SQL fragments,
// mirroring the issueBroadcast test harness.
function createDbMock(fixtures: {
  users: Array<{ id: string; email: string; first_name: string }>;
  failMark?: boolean;
}) {
  const markedSends: Array<{ newsletterId: string; userId: string }> = [];
  const query = vi.fn(async (sql: string, params?: unknown[]) => {
    if (sql.includes("INSERT INTO public.member_newsletter_sends")) {
      if (fixtures.failMark) {
        throw new Error("mark insert failed");
      }
      markedSends.push({ newsletterId: params?.[0] as string, userId: params?.[1] as string });
      return { rows: [], rowCount: 1 };
    }
    if (sql.includes("SELECT") && sql.includes("u.email")) {
      // Mirror the real query's contract: honor the batch size, the
      // unmarked-user exclusion list, and the dedupe NOT EXISTS (marked
      // users disappear from the audience).
      const batchSize = params?.[1] as number;
      const excluded = new Set(params?.[2] as string[]);
      const marked = new Set(markedSends.map((send) => send.userId));
      const rows = fixtures.users
        .filter((user) => !excluded.has(user.id) && !marked.has(user.id))
        .slice(0, batchSize);
      return { rows, rowCount: rows.length };
    }
    throw new Error(`Unexpected SQL in test: ${sql.slice(0, 80)}`);
  });
  return { query, markedSends };
}

function createMailerMock(failFor?: string) {
  const sent: MemberNewsletterEmailInput[] = [];
  return {
    sent,
    sendNewsletterEmail: vi.fn(async (input: MemberNewsletterEmailInput) => {
      if (failFor && input.email === failFor) {
        throw new Error("SES exploded");
      }
      sent.push(input);
    }),
  };
}

describe("sendMemberNewsletter", () => {
  it("rejects blank newsletter ids, subjects, and bodies", async () => {
    const db = createDbMock({ users: [] });
    const mailer = createMailerMock();

    await expect(
      sendMemberNewsletter(db as never, mailer, { ...baseOptions, newsletterId: " " })
    ).rejects.toBeInstanceOf(MemberNewsletterError);
    await expect(
      sendMemberNewsletter(db as never, mailer, { ...baseOptions, subject: "" })
    ).rejects.toBeInstanceOf(MemberNewsletterError);
    await expect(
      sendMemberNewsletter(db as never, mailer, { ...baseOptions, body: "  " })
    ).rejects.toBeInstanceOf(MemberNewsletterError);
    expect(db.query).not.toHaveBeenCalled();
  });

  it("selects only active members who have not opted out (SQL contract)", async () => {
    const db = createDbMock({ users: [] });
    const mailer = createMailerMock();

    await sendMemberNewsletter(db as never, mailer, baseOptions);

    const sql = String(db.query.mock.calls[0]?.[0]);
    // The eligibility gates the audience promise rests on.
    expect(sql).toContain("u.deleted_at IS NULL");
    expect(sql).toContain("u.email_verified = true");
    expect(sql).toContain("u.email_member_newsletter = true");
    expect(sql).toContain("stripe_status = 'active'");
    expect(sql).toContain("member_newsletter_sends");
  });

  it("dry run counts recipients without sending or marking", async () => {
    const db = createDbMock({
      users: [{ id: USER_ALPHA, email: "a@example.com", first_name: "A" }],
    });
    const mailer = createMailerMock();

    const result = await sendMemberNewsletter(db as never, mailer, { ...baseOptions, live: false });

    expect(result).toMatchObject({
      dryRun: true,
      newsletterId: "2026-09-analysis",
      eligibleUserCount: 1,
      usersEmailedCount: 0,
      usersMarkedCount: 0,
      failures: [],
    });
    expect(mailer.sendNewsletterEmail).not.toHaveBeenCalled();
    expect(db.markedSends).toEqual([]);
  });

  it("live run emails each member, marks the dedupe row, and passes the unsubscribe url", async () => {
    const db = createDbMock({
      users: [
        { id: USER_ALPHA, email: "a@example.com", first_name: "A" },
        { id: USER_BETA, email: "b@example.com", first_name: "B" },
      ],
    });
    const mailer = createMailerMock();

    const result = await sendMemberNewsletter(db as never, mailer, {
      ...baseOptions,
      buildUnsubscribeUrl: (userId) => `https://api.example.com/api/email/unsubscribe?u=${userId}`,
    });

    expect(result).toMatchObject({
      eligibleUserCount: 2,
      usersEmailedCount: 2,
      usersMarkedCount: 2,
      failures: [],
    });
    expect(mailer.sent.map((input) => input.email)).toEqual(["a@example.com", "b@example.com"]);
    expect(mailer.sent[0]?.unsubscribeUrl).toBe(
      `https://api.example.com/api/email/unsubscribe?u=${USER_ALPHA}`
    );
    expect(db.markedSends).toEqual([
      { newsletterId: "2026-09-analysis", userId: USER_ALPHA },
      { newsletterId: "2026-09-analysis", userId: USER_BETA },
    ]);
  });

  it("a failed send is reported, not marked, and does not stop the rest", async () => {
    const db = createDbMock({
      users: [
        { id: USER_ALPHA, email: "a@example.com", first_name: "A" },
        { id: USER_BETA, email: "b@example.com", first_name: "B" },
      ],
    });
    const mailer = createMailerMock("a@example.com");

    const result = await sendMemberNewsletter(db as never, mailer, baseOptions);

    expect(result.usersEmailedCount).toBe(1);
    expect(result.usersMarkedCount).toBe(1);
    expect(result.failures).toEqual([{ userId: USER_ALPHA, stage: "send", reason: "SES exploded" }]);
    expect(db.markedSends).toEqual([{ newsletterId: "2026-09-analysis", userId: USER_BETA }]);
  });

  it("a failed mark after a real send is reported as mark_after_send", async () => {
    const db = createDbMock({
      users: [{ id: USER_ALPHA, email: "a@example.com", first_name: "A" }],
      failMark: true,
    });
    const mailer = createMailerMock();

    const result = await sendMemberNewsletter(db as never, mailer, baseOptions);

    expect(result.usersEmailedCount).toBe(1);
    expect(result.usersMarkedCount).toBe(0);
    expect(result.failures).toEqual([
      { userId: USER_ALPHA, stage: "mark_after_send", reason: "mark insert failed" },
    ]);
  });

  it("loops batches until the audience is exhausted", async () => {
    const db = createDbMock({
      users: [
        { id: USER_ALPHA, email: "a@example.com", first_name: "A" },
        { id: USER_BETA, email: "b@example.com", first_name: "B" },
      ],
    });
    const mailer = createMailerMock();

    const result = await sendMemberNewsletter(db as never, mailer, { ...baseOptions, batchSize: 1 });

    expect(result.usersEmailedCount).toBe(2);
    expect(result.usersMarkedCount).toBe(2);
  });

  it("uses an app-unique advisory lock key", () => {
    expect(MEMBER_NEWSLETTER_RUN_LOCK_KEY).not.toBe(ISSUE_BROADCAST_RUN_LOCK_KEY);
  });

  it("withMemberNewsletterRunLock skips when another run holds the lock and always releases the client", async () => {
    const release = vi.fn();
    const query = vi.fn(async (sql: string) => {
      if (sql.includes("pg_try_advisory_lock")) {
        return { rows: [{ locked: false }], rowCount: 1 };
      }
      throw new Error(`Unexpected SQL: ${sql}`);
    });
    const pool = { connect: vi.fn(async () => ({ query, release })) };

    const fn = vi.fn();
    await expect(withMemberNewsletterRunLock(pool as never, fn)).resolves.toBeNull();
    expect(fn).not.toHaveBeenCalled();
    expect(release).toHaveBeenCalledTimes(1);
  });
});
