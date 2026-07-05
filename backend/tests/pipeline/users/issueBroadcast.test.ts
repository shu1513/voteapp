import { describe, expect, it, vi } from "vitest";

import {
  ISSUE_BROADCAST_RUN_LOCK_KEY,
  IssueBroadcastError,
  sendIssueBroadcast,
  withIssueBroadcastRunLock,
} from "../../../src/pipeline/users/issueBroadcast.js";
import { DIGEST_RUN_LOCK_KEY } from "../../../src/scripts/sendCandidateFollowDigests.js";
import { NEW_ELECTION_ALERT_RUN_LOCK_KEY } from "../../../src/scripts/sendNewElectionAlerts.js";
import { ELECTION_REMINDER_RUN_LOCK_KEY } from "../../../src/scripts/sendElectionReminders.js";
import type { IssueBroadcastEmailInput } from "../../../src/pipeline/users/issueBroadcastMailer.js";

const USER_ALPHA = "11111111-1111-4111-8111-111111111111";
const USER_BETA = "22222222-2222-4222-8222-222222222222";
const AREA_ENV = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";

const baseOptions = {
  live: true,
  broadcastId: "env-nonprofit-2026-07",
  areaSlugs: ["environment_and_public_health"],
  subject: "A nonprofit worth knowing",
  body: "Green Futures does great work.",
};

// Routes the sender's statements by their distinguishing SQL fragments.
function createDbMock(fixtures: {
  areas?: Array<{ id: string; slug: string; name: string }>;
  users: Array<{ id: string; email: string; first_name: string; matched_area_names: string[] }>;
  failMark?: boolean;
}) {
  const areas = fixtures.areas ?? [
    { id: AREA_ENV, slug: "environment_and_public_health", name: "Environment and Public Health" },
  ];
  const markedSends: Array<{ broadcastId: string; userId: string }> = [];
  const query = vi.fn(async (sql: string, params?: unknown[]) => {
    if (sql.includes("FROM public.research_areas")) {
      return { rows: areas, rowCount: areas.length };
    }
    if (sql.includes("INSERT INTO public.issue_broadcast_sends")) {
      if (fixtures.failMark) {
        throw new Error("mark insert failed");
      }
      markedSends.push({ broadcastId: params?.[0] as string, userId: params?.[1] as string });
      return { rows: [], rowCount: 1 };
    }
    if (sql.includes("SELECT") && sql.includes("u.email")) {
      // Mirror the real query's contract: honor the batch size, the
      // unmarked-user exclusion list, and the dedupe NOT EXISTS (marked
      // users disappear from the audience).
      const batchSize = params?.[2] as number;
      const excluded = new Set(params?.[3] as string[]);
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
  const sent: IssueBroadcastEmailInput[] = [];
  return {
    sent,
    sendBroadcastEmail: vi.fn(async (input: IssueBroadcastEmailInput) => {
      if (failFor && input.email === failFor) {
        throw new Error("SES exploded");
      }
      sent.push(input);
    }),
  };
}

describe("sendIssueBroadcast", () => {
  it("rejects blank broadcast ids, subjects, bodies, and empty area lists", async () => {
    const db = createDbMock({ users: [] });
    const mailer = createMailerMock();

    await expect(
      sendIssueBroadcast(db as never, mailer, { ...baseOptions, broadcastId: " " })
    ).rejects.toBeInstanceOf(IssueBroadcastError);
    await expect(
      sendIssueBroadcast(db as never, mailer, { ...baseOptions, subject: "" })
    ).rejects.toBeInstanceOf(IssueBroadcastError);
    await expect(
      sendIssueBroadcast(db as never, mailer, { ...baseOptions, body: "  " })
    ).rejects.toBeInstanceOf(IssueBroadcastError);
    await expect(
      sendIssueBroadcast(db as never, mailer, { ...baseOptions, areaSlugs: [" "] })
    ).rejects.toBeInstanceOf(IssueBroadcastError);
    expect(db.query).not.toHaveBeenCalled();
  });

  it("rejects unknown research area slugs before selecting anyone", async () => {
    const db = createDbMock({ users: [] });
    const mailer = createMailerMock();

    await expect(
      sendIssueBroadcast(db as never, mailer, { ...baseOptions, areaSlugs: ["environment_and_public_health", "nope"] })
    ).rejects.toMatchObject({ code: "unknown_research_area_slugs" });
    expect(db.query).toHaveBeenCalledTimes(1);
  });

  it("dry run counts recipients without sending or marking, with the full eligibility contract", async () => {
    const db = createDbMock({
      users: [
        { id: USER_ALPHA, email: "a@example.com", first_name: "A", matched_area_names: ["Environment and Public Health"] },
      ],
    });
    const mailer = createMailerMock();

    const result = await sendIssueBroadcast(db as never, mailer, { ...baseOptions, live: false });

    expect(result).toMatchObject({
      dryRun: true,
      broadcastId: "env-nonprofit-2026-07",
      eligibleUserCount: 1,
      usersEmailedCount: 0,
      usersMarkedCount: 0,
      failures: [],
    });
    expect(result.targetAreas).toEqual([
      { id: AREA_ENV, slug: "environment_and_public_health", name: "Environment and Public Health" },
    ]);
    expect(mailer.sendBroadcastEmail).not.toHaveBeenCalled();
    expect(db.markedSends).toEqual([]);
    const recipientSql = db.query.mock.calls.map((call) => String(call[0])).find((sql) => sql.includes("u.email"));
    expect(recipientSql).toContain("email_verified = true");
    expect(recipientSql).toContain("email_issue_updates = true");
    expect(recipientSql).toContain("deleted_at IS NULL");
    expect(recipientSql).toContain("NOT EXISTS");
    expect(recipientSql).toContain("issue_broadcast_sends");
    expect(recipientSql).toContain("<> ALL");
  });

  it("live run sends one email per recipient with matched areas and marks the dedupe log", async () => {
    const db = createDbMock({
      users: [
        { id: USER_ALPHA, email: "a@example.com", first_name: "A", matched_area_names: ["Environment and Public Health"] },
        { id: USER_BETA, email: "b@example.com", first_name: "B", matched_area_names: ["Environment and Public Health"] },
      ],
    });
    const mailer = createMailerMock();

    const result = await sendIssueBroadcast(db as never, mailer, baseOptions);

    expect(result).toMatchObject({
      eligibleUserCount: 2,
      usersEmailedCount: 2,
      usersMarkedCount: 2,
      failures: [],
    });
    expect(mailer.sent[0]).toMatchObject({
      email: "a@example.com",
      firstName: "A",
      subject: "A nonprofit worth knowing",
      body: "Green Futures does great work.",
      matchedAreaNames: ["Environment and Public Health"],
    });
    expect(db.markedSends).toEqual([
      { broadcastId: "env-nonprofit-2026-07", userId: USER_ALPHA },
      { broadcastId: "env-nonprofit-2026-07", userId: USER_BETA },
    ]);
  });

  it("rejects a non-positive batch size before selecting anyone", async () => {
    const db = createDbMock({ users: [] });
    const mailer = createMailerMock();

    await expect(
      sendIssueBroadcast(db as never, mailer, { ...baseOptions, batchSize: 0 })
    ).rejects.toMatchObject({ code: "invalid_broadcast" });
    // Area resolution runs first; no recipient selection happened.
    expect(db.query.mock.calls.filter((call) => String(call[0]).includes("u.email"))).toHaveLength(0);
  });

  it("keeps the exclusion list empty while sends succeed (marked users leave via the dedupe)", async () => {
    const db = createDbMock({
      users: [
        { id: USER_ALPHA, email: "a@example.com", first_name: "A", matched_area_names: [] },
        { id: USER_BETA, email: "b@example.com", first_name: "B", matched_area_names: [] },
      ],
    });
    const mailer = createMailerMock();

    await sendIssueBroadcast(db as never, mailer, { ...baseOptions, batchSize: 1 });

    const selectionCalls = db.query.mock.calls.filter((call) => String(call[0]).includes("u.email"));
    for (const call of selectionCalls) {
      expect((call[1] as unknown[])[3]).toEqual([]);
    }
  });

  it("loops through batches until every recipient is processed in one run", async () => {
    const db = createDbMock({
      users: [
        { id: USER_ALPHA, email: "a@example.com", first_name: "A", matched_area_names: [] },
        { id: USER_BETA, email: "b@example.com", first_name: "B", matched_area_names: [] },
      ],
    });
    const mailer = createMailerMock();

    const result = await sendIssueBroadcast(db as never, mailer, { ...baseOptions, batchSize: 1 });

    expect(result.usersEmailedCount).toBe(2);
    const selectionCalls = db.query.mock.calls.filter((call) => String(call[0]).includes("u.email"));
    // Batch of 1, batch of 1, empty terminator.
    expect(selectionCalls).toHaveLength(3);
  });

  it("isolates a failed send and terminates instead of retrying that user forever", async () => {
    const db = createDbMock({
      users: [
        { id: USER_ALPHA, email: "fail@example.com", first_name: "A", matched_area_names: [] },
        { id: USER_BETA, email: "b@example.com", first_name: "B", matched_area_names: [] },
      ],
    });
    const mailer = createMailerMock("fail@example.com");

    const result = await sendIssueBroadcast(db as never, mailer, { ...baseOptions, batchSize: 1 });

    expect(result.failures).toEqual([{ userId: USER_ALPHA, stage: "send", reason: "SES exploded" }]);
    expect(result.usersEmailedCount).toBe(1);
    expect(db.markedSends).toEqual([{ broadcastId: "env-nonprofit-2026-07", userId: USER_BETA }]);
  });

  it("records a mark_after_send failure when the email went out but the log insert failed", async () => {
    const db = createDbMock({
      users: [{ id: USER_ALPHA, email: "a@example.com", first_name: "A", matched_area_names: [] }],
      failMark: true,
    });
    const mailer = createMailerMock();

    const result = await sendIssueBroadcast(db as never, mailer, baseOptions);

    expect(result.usersEmailedCount).toBe(1);
    expect(result.usersMarkedCount).toBe(0);
    expect(result.failures).toEqual([
      { userId: USER_ALPHA, stage: "mark_after_send", reason: "mark insert failed" },
    ]);
  });

  it("passes the built per-user unsubscribe URL to the mailer", async () => {
    const db = createDbMock({
      users: [{ id: USER_ALPHA, email: "a@example.com", first_name: "A", matched_area_names: [] }],
    });
    const mailer = createMailerMock();

    await sendIssueBroadcast(db as never, mailer, {
      ...baseOptions,
      buildUnsubscribeUrl: (userId) => `https://api.example.com/api/email/unsubscribe?u=${userId}&pref=issue_updates`,
    });

    expect(mailer.sent[0].unsubscribeUrl).toBe(
      `https://api.example.com/api/email/unsubscribe?u=${USER_ALPHA}&pref=issue_updates`
    );
  });
});

describe("withIssueBroadcastRunLock", () => {
  it("uses its own lock key, distinct from the other senders", () => {
    expect(ISSUE_BROADCAST_RUN_LOCK_KEY).not.toBe(DIGEST_RUN_LOCK_KEY);
    expect(ISSUE_BROADCAST_RUN_LOCK_KEY).not.toBe(NEW_ELECTION_ALERT_RUN_LOCK_KEY);
    expect(ISSUE_BROADCAST_RUN_LOCK_KEY).not.toBe(ELECTION_REMINDER_RUN_LOCK_KEY);
  });

  it("runs fn under the lock, unlocks, and releases the client", async () => {
    const query = vi.fn(async (sql: string) => {
      if (sql.includes("pg_try_advisory_lock")) {
        return { rows: [{ locked: true }], rowCount: 1 };
      }
      return { rows: [], rowCount: 1 };
    });
    const release = vi.fn();
    const pool = { connect: vi.fn(async () => ({ query, release })) };
    const fn = vi.fn(async () => "ran");

    await expect(withIssueBroadcastRunLock(pool as never, fn)).resolves.toBe("ran");

    expect(query.mock.calls[0][1]).toEqual([ISSUE_BROADCAST_RUN_LOCK_KEY]);
    expect(String(query.mock.calls[1][0])).toContain("pg_advisory_unlock");
    expect(release).toHaveBeenCalledTimes(1);
  });

  it("returns null without running fn when the lock is held elsewhere", async () => {
    const query = vi.fn(async (sql: string) => {
      if (sql.includes("pg_try_advisory_lock")) {
        return { rows: [{ locked: false }], rowCount: 1 };
      }
      return { rows: [], rowCount: 1 };
    });
    const release = vi.fn();
    const pool = { connect: vi.fn(async () => ({ query, release })) };
    const fn = vi.fn();

    await expect(withIssueBroadcastRunLock(pool as never, fn as never)).resolves.toBeNull();
    expect(fn).not.toHaveBeenCalled();
    expect(release).toHaveBeenCalledTimes(1);
  });
});
