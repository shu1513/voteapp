import { describe, expect, it, vi } from "vitest";

import {
  DEFAULT_DIGEST_MAX_ITEMS_PER_EMAIL,
  DEFAULT_DIGEST_MAX_USERS,
  DIGEST_RUN_LOCK_KEY,
  parseSendCandidateFollowDigestsArgs,
  sendCandidateFollowDigests,
  withDigestRunLock,
} from "../../src/scripts/sendCandidateFollowDigests.js";
import { buildUnsubscribeUrlBuilderFromEnv } from "../../src/scripts/sendCandidateFollowDigests.js";
import { verifyEmailUnsubscribeToken } from "../../src/pipeline/users/emailUnsubscribeToken.js";
import type { CandidateFollowDigestEmailInput } from "../../src/pipeline/users/candidateFollowDigestMailer.js";

const USER_ALPHA = "11111111-1111-4111-8111-111111111111";
const USER_BETA = "22222222-2222-4222-8222-222222222222";

type PendingRow = {
  id: string;
  event_type: string;
  candidate_display_name: string;
  record_description: string | null;
  election_title: string | null;
  election_date: string | null;
};

function pendingRow(id: string, candidate: string, overrides: Partial<PendingRow> = {}): PendingRow {
  return {
    id,
    event_type: "candidate_record_update",
    candidate_display_name: candidate,
    record_description: "Did a thing.",
    election_title: null,
    election_date: null,
    ...overrides,
  };
}

// Routes the sender's statements by their distinguishing SQL fragments.
function createDbMock(fixtures: {
  orphanCount?: number;
  users: Array<{ id: string; email: string; first_name: string }>;
  pendingByUser: Record<string, PendingRow[]>;
  failMark?: boolean;
  pushTokensByUser?: Record<string, string[]>;
  pendingPushReceipts?: Array<{ receipt_id: string; expo_push_token: string }>;
}) {
  const markedEventIds: string[][] = [];
  const revokedPushTokens: string[] = [];
  const deletedReceiptIds: string[][] = [];
  const query = vi.fn(async (sql: string, params?: unknown[]) => {
    if (sql.includes("count(*)")) {
      return { rows: [{ matched: String(fixtures.orphanCount ?? 0) }], rowCount: 1 };
    }
    if (sql.includes("SELECT expo_push_token")) {
      const userId = params?.[0] as string;
      const rows = (fixtures.pushTokensByUser?.[userId] ?? []).map((token) => ({ expo_push_token: token }));
      return { rows, rowCount: rows.length };
    }
    if (sql.includes("SET revoked_at = now()")) {
      revokedPushTokens.push(params?.[0] as string);
      return { rows: [], rowCount: 1 };
    }
    if (sql.includes("INSERT INTO public.user_push_notification_receipts")) {
      return { rows: [], rowCount: (params?.[0] as string[]).length };
    }
    if (sql.includes("DELETE FROM public.user_push_notification_receipts")) {
      deletedReceiptIds.push([...(params?.[0] as string[])]);
      return { rows: [], rowCount: (params?.[0] as string[]).length };
    }
    if (sql.includes("FROM public.user_push_notification_receipts")) {
      const rows = fixtures.pendingPushReceipts ?? [];
      return { rows, rowCount: rows.length };
    }
    if (sql.includes("ANY($1::uuid[])")) {
      if (fixtures.failMark) {
        throw new Error("mark update failed");
      }
      markedEventIds.push([...(params?.[0] as string[])]);
      return { rows: [], rowCount: (params?.[0] as string[]).length };
    }
    if (sql.includes("SET notified_at = now()")) {
      return { rows: [], rowCount: fixtures.orphanCount ?? 0 };
    }
    if (sql.includes("FROM public.users AS u")) {
      return { rows: fixtures.users, rowCount: fixtures.users.length };
    }
    if (sql.includes("e.event_type")) {
      const userId = params?.[0] as string;
      const rows = fixtures.pendingByUser[userId] ?? [];
      return { rows, rowCount: rows.length };
    }
    throw new Error(`Unexpected SQL in test: ${sql.slice(0, 80)}`);
  });
  return { query, markedEventIds, revokedPushTokens, deletedReceiptIds };
}

function createPushClientMock(overrides: Record<string, unknown> = {}) {
  return {
    chunkPushNotifications: vi.fn((messages: unknown[]) => [messages]),
    sendPushNotificationsAsync: vi.fn(async (chunk: Array<{ to: string }>) =>
      chunk.map((_, index) => ({ status: "ok", id: `receipt-${index}` }))
    ),
    chunkPushNotificationReceiptIds: vi.fn((ids: string[]) => [ids]),
    getPushNotificationReceiptsAsync: vi.fn(async () => ({})),
    ...overrides,
  };
}

function createMailerMock(failFor?: string) {
  const sent: CandidateFollowDigestEmailInput[] = [];
  return {
    sent,
    sendDigestEmail: vi.fn(async (input: CandidateFollowDigestEmailInput) => {
      if (failFor && input.email === failFor) {
        throw new Error("SES exploded");
      }
      sent.push(input);
    }),
  };
}

describe("parseSendCandidateFollowDigestsArgs", () => {
  it("defaults to a dry run with default caps", () => {
    expect(parseSendCandidateFollowDigestsArgs([])).toEqual({
      live: false,
      maxUsers: DEFAULT_DIGEST_MAX_USERS,
      maxItemsPerEmail: DEFAULT_DIGEST_MAX_ITEMS_PER_EMAIL,
    });
  });

  it("parses --live and both flag forms", () => {
    expect(
      parseSendCandidateFollowDigestsArgs(["--live", "--max-users", "10", "--max-items-per-email=5"])
    ).toEqual({ live: true, maxUsers: 10, maxItemsPerEmail: 5 });
  });

  it("rejects malformed values", () => {
    expect(() => parseSendCandidateFollowDigestsArgs(["--max-users", "0"])).toThrow(
      "--max-users must be a positive integer"
    );
    expect(() => parseSendCandidateFollowDigestsArgs(["--max-users"])).toThrow("--max-users requires a value");
  });
});

describe("sendCandidateFollowDigests", () => {
  const options = { live: true, maxUsers: 500, maxItemsPerEmail: 20 };

  it("dry run counts orphans and pending events without sending or marking", async () => {
    const db = createDbMock({
      orphanCount: 2,
      users: [{ id: USER_ALPHA, email: "a@example.com", first_name: "A" }],
      pendingByUser: { [USER_ALPHA]: [pendingRow("e1", "Jane Doe"), pendingRow("e2", "Jane Doe")] },
    });
    const mailer = createMailerMock();

    const result = await sendCandidateFollowDigests(db as never, mailer, { ...options, live: false });

    expect(result).toMatchObject({
      dryRun: true,
      resolvedWithoutEmailCount: 2,
      eligibleUserCount: 1,
      eventsPendingCount: 2,
      usersEmailedCount: 0,
      eventsDeliveredCount: 0,
      failures: [],
    });
    expect(mailer.sendDigestEmail).not.toHaveBeenCalled();
    expect(db.markedEventIds).toEqual([]);
    // Dry-run orphan handling must count, not update.
    const sqls = db.query.mock.calls.map((call) => String(call[0]));
    expect(sqls.some((sql) => sql.includes("count(*)"))).toBe(true);
    // Eligibility must mirror deliverability so orphan-only users cannot
    // consume --max-users slots in dry runs.
    const usersSql = sqls.find((sql) => sql.includes("FROM public.users AS u"));
    expect(usersSql).toContain("user_candidate_follows");
    expect(usersSql).toContain("merged_into_candidate_id IS NULL");
  });

  it("live run sends one digest per user and marks exactly the sent events", async () => {
    const db = createDbMock({
      users: [
        { id: USER_ALPHA, email: "a@example.com", first_name: "A" },
        { id: USER_BETA, email: "b@example.com", first_name: "B" },
      ],
      pendingByUser: {
        [USER_ALPHA]: [
          pendingRow("e1", "Jane Doe"),
          pendingRow("e2", "Jane Doe", {
            event_type: "candidate_future_election",
            record_description: null,
            election_title: "Governor",
            election_date: "2026-11-03",
          }),
        ],
        [USER_BETA]: [pendingRow("e3", "John Smith")],
      },
    });
    const mailer = createMailerMock();

    const result = await sendCandidateFollowDigests(db as never, mailer, options);

    expect(result).toMatchObject({
      dryRun: false,
      eligibleUserCount: 2,
      eventsPendingCount: 3,
      usersEmailedCount: 2,
      eventsDeliveredCount: 3,
      failures: [],
    });
    expect(mailer.sent).toHaveLength(2);
    expect(mailer.sent[0]).toMatchObject({
      email: "a@example.com",
      firstName: "A",
      totalEventCount: 2,
    });
    expect(mailer.sent[0].items[1]).toMatchObject({
      eventType: "candidate_future_election",
      electionTitle: "Governor",
      electionDate: "2026-11-03",
    });
    expect(db.markedEventIds).toEqual([["e1", "e2"], ["e3"]]);
  });

  it("isolates a failed send: records the failure, does not mark, continues to the next user", async () => {
    const db = createDbMock({
      users: [
        { id: USER_ALPHA, email: "fail@example.com", first_name: "A" },
        { id: USER_BETA, email: "b@example.com", first_name: "B" },
      ],
      pendingByUser: {
        [USER_ALPHA]: [pendingRow("e1", "Jane Doe")],
        [USER_BETA]: [pendingRow("e2", "John Smith")],
      },
    });
    const mailer = createMailerMock("fail@example.com");

    const result = await sendCandidateFollowDigests(db as never, mailer, options);

    expect(result.failures).toEqual([{ userId: USER_ALPHA, stage: "send", reason: "SES exploded" }]);
    expect(result.usersEmailedCount).toBe(1);
    expect(result.eventsDeliveredCount).toBe(1);
    // Only the successful user's events were marked.
    expect(db.markedEventIds).toEqual([["e2"]]);
  });

  it("records a mark_after_send failure when the email went out but stamping failed", async () => {
    const db = createDbMock({
      users: [{ id: USER_ALPHA, email: "a@example.com", first_name: "A" }],
      pendingByUser: { [USER_ALPHA]: [pendingRow("e1", "Jane Doe")] },
      failMark: true,
    });
    const mailer = createMailerMock();

    const result = await sendCandidateFollowDigests(db as never, mailer, options);

    // The email was sent, so it counts as emailed but not delivered.
    expect(result.usersEmailedCount).toBe(1);
    expect(result.eventsDeliveredCount).toBe(0);
    expect(result.failures).toEqual([
      { userId: USER_ALPHA, stage: "mark_after_send", reason: "mark update failed" },
    ]);
  });

  it("sends a summary push after the email and still marks the events", async () => {
    const db = createDbMock({
      users: [{ id: USER_ALPHA, email: "a@example.com", first_name: "A" }],
      pendingByUser: { [USER_ALPHA]: [pendingRow("e1", "Jane Doe"), pendingRow("e2", "Jane Doe")] },
      pushTokensByUser: { [USER_ALPHA]: ["ExponentPushToken[aaa]"] },
    });
    const mailer = createMailerMock();
    const pushClient = createPushClientMock();

    const result = await sendCandidateFollowDigests(db as never, mailer, {
      ...options,
      pushClient: pushClient as never,
    });

    expect(result).toMatchObject({ usersEmailedCount: 1, usersPushedCount: 1, failures: [] });
    expect(db.markedEventIds).toEqual([["e1", "e2"]]);
    const sentChunk = pushClient.sendPushNotificationsAsync.mock.calls[0][0];
    expect(sentChunk).toEqual([
      {
        to: "ExponentPushToken[aaa]",
        title: "VoteApp",
        body: "2 updates on candidates you follow",
        data: { url: "/follows" },
        sound: "default",
      },
    ]);
  });

  it("records a push failure without blocking the mark (push is best-effort)", async () => {
    const db = createDbMock({
      users: [{ id: USER_ALPHA, email: "a@example.com", first_name: "A" }],
      pendingByUser: { [USER_ALPHA]: [pendingRow("e1", "Jane Doe")] },
      pushTokensByUser: { [USER_ALPHA]: ["ExponentPushToken[aaa]"] },
    });
    const mailer = createMailerMock();
    const pushClient = createPushClientMock({
      sendPushNotificationsAsync: vi.fn(async () => {
        throw new Error("expo api unreachable");
      }),
    });

    const result = await sendCandidateFollowDigests(db as never, mailer, {
      ...options,
      pushClient: pushClient as never,
    });

    expect(result.usersEmailedCount).toBe(1);
    expect(result.usersPushedCount).toBe(0);
    expect(result.eventsDeliveredCount).toBe(1);
    expect(result.failures).toEqual([
      { userId: USER_ALPHA, stage: "push_send", reason: "expo api unreachable" },
    ]);
    expect(db.markedEventIds).toEqual([["e1"]]);
  });

  it("skips the push channel entirely for users without tokens", async () => {
    const db = createDbMock({
      users: [{ id: USER_ALPHA, email: "a@example.com", first_name: "A" }],
      pendingByUser: { [USER_ALPHA]: [pendingRow("e1", "Jane Doe")] },
    });
    const mailer = createMailerMock();
    const pushClient = createPushClientMock();

    const result = await sendCandidateFollowDigests(db as never, mailer, {
      ...options,
      pushClient: pushClient as never,
    });

    expect(result).toMatchObject({ usersEmailedCount: 1, usersPushedCount: 0, failures: [] });
    expect(pushClient.sendPushNotificationsAsync).not.toHaveBeenCalled();
  });

  it("checks mature push receipts at run start and revokes dead tokens", async () => {
    const db = createDbMock({
      users: [],
      pendingByUser: {},
      pendingPushReceipts: [
        { receipt_id: "r1", expo_push_token: "ExponentPushToken[dead]" },
        { receipt_id: "r2", expo_push_token: "ExponentPushToken[fine]" },
      ],
    });
    const mailer = createMailerMock();
    const pushClient = createPushClientMock({
      getPushNotificationReceiptsAsync: vi.fn(async () => ({
        r1: { status: "error", message: "gone", details: { error: "DeviceNotRegistered" } },
        r2: { status: "ok" },
      })),
    });

    const result = await sendCandidateFollowDigests(db as never, mailer, {
      ...options,
      pushClient: pushClient as never,
    });

    expect(result.pushReceiptsCheckedCount).toBe(2);
    expect(result.pushTokensRevokedCount).toBe(1);
    expect(db.revokedPushTokens).toEqual(["ExponentPushToken[dead]"]);
    expect(db.deletedReceiptIds).toEqual([["r1", "r2"]]);
  });

  it("continues the email run when receipt processing fails (best-effort)", async () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    try {
      const db = createDbMock({
        users: [{ id: USER_ALPHA, email: "a@example.com", first_name: "A" }],
        pendingByUser: { [USER_ALPHA]: [pendingRow("e1", "Jane Doe")] },
        pendingPushReceipts: [{ receipt_id: "r1", expo_push_token: "ExponentPushToken[aaa]" }],
      });
      const mailer = createMailerMock();
      const pushClient = createPushClientMock({
        getPushNotificationReceiptsAsync: vi.fn(async () => {
          throw new Error("expo receipts api down");
        }),
      });

      const result = await sendCandidateFollowDigests(db as never, mailer, {
        ...options,
        pushClient: pushClient as never,
      });

      // The receipt failure is logged, not fatal: the email still goes out.
      expect(result.usersEmailedCount).toBe(1);
      expect(result.eventsDeliveredCount).toBe(1);
      expect(result.pushReceiptsCheckedCount).toBe(0);
      expect(warn).toHaveBeenCalledWith(expect.stringContaining("push receipt processing failed"));
    } finally {
      warn.mockRestore();
    }
  });

  it("does not touch the push channel in dry runs", async () => {
    const db = createDbMock({
      users: [{ id: USER_ALPHA, email: "a@example.com", first_name: "A" }],
      pendingByUser: { [USER_ALPHA]: [pendingRow("e1", "Jane Doe")] },
      pushTokensByUser: { [USER_ALPHA]: ["ExponentPushToken[aaa]"] },
      pendingPushReceipts: [{ receipt_id: "r1", expo_push_token: "ExponentPushToken[aaa]" }],
    });
    const mailer = createMailerMock();
    const pushClient = createPushClientMock();

    await sendCandidateFollowDigests(db as never, mailer, {
      ...options,
      live: false,
      pushClient: pushClient as never,
    });

    expect(pushClient.sendPushNotificationsAsync).not.toHaveBeenCalled();
    expect(pushClient.getPushNotificationReceiptsAsync).not.toHaveBeenCalled();
  });

  it("caps rendered items per email while marking and counting every event", async () => {
    const rows = Array.from({ length: 5 }, (_, i) => pendingRow(`e${i}`, "Jane Doe"));
    const db = createDbMock({
      users: [{ id: USER_ALPHA, email: "a@example.com", first_name: "A" }],
      pendingByUser: { [USER_ALPHA]: rows },
    });
    const mailer = createMailerMock();

    const result = await sendCandidateFollowDigests(db as never, mailer, {
      ...options,
      maxItemsPerEmail: 2,
    });

    expect(mailer.sent[0].items).toHaveLength(2);
    expect(mailer.sent[0].totalEventCount).toBe(5);
    expect(result.eventsDeliveredCount).toBe(5);
    expect(db.markedEventIds).toEqual([["e0", "e1", "e2", "e3", "e4"]]);
  });

  it("skips users whose pending events all resolved away without emailing them", async () => {
    const db = createDbMock({
      users: [{ id: USER_ALPHA, email: "a@example.com", first_name: "A" }],
      pendingByUser: { [USER_ALPHA]: [] },
    });
    const mailer = createMailerMock();

    const result = await sendCandidateFollowDigests(db as never, mailer, options);

    expect(result.eligibleUserCount).toBe(0);
    expect(result.usersEmailedCount).toBe(0);
    expect(mailer.sendDigestEmail).not.toHaveBeenCalled();
    expect(db.markedEventIds).toEqual([]);
  });
});

describe("withDigestRunLock", () => {
  function createLockClientMock(locked: boolean) {
    const query = vi.fn(async (sql: string) => {
      if (sql.includes("pg_try_advisory_lock")) {
        return { rows: [{ locked }], rowCount: 1 };
      }
      return { rows: [], rowCount: 1 };
    });
    const release = vi.fn();
    return { client: { query, release }, query, release };
  }

  it("runs fn under the lock, unlocks, and releases the client", async () => {
    const { client, query, release } = createLockClientMock(true);
    const pool = { connect: vi.fn(async () => client) };
    const fn = vi.fn(async () => "ran");

    await expect(withDigestRunLock(pool as never, fn)).resolves.toBe("ran");

    expect(fn).toHaveBeenCalledTimes(1);
    expect(query.mock.calls[0][0]).toContain("pg_try_advisory_lock");
    expect(query.mock.calls[0][1]).toEqual([DIGEST_RUN_LOCK_KEY]);
    expect(String(query.mock.calls[1][0])).toContain("pg_advisory_unlock");
    expect(release).toHaveBeenCalledTimes(1);
  });

  it("returns null without running fn when the lock is held elsewhere", async () => {
    const { client, query, release } = createLockClientMock(false);
    const pool = { connect: vi.fn(async () => client) };
    const fn = vi.fn();

    await expect(withDigestRunLock(pool as never, fn as never)).resolves.toBeNull();

    expect(fn).not.toHaveBeenCalled();
    // No unlock call for a lock we never held.
    expect(query.mock.calls.map((c) => String(c[0])).join(" ")).not.toContain("pg_advisory_unlock");
    expect(release).toHaveBeenCalledTimes(1);
  });

  it("still unlocks and releases when fn throws", async () => {
    const { client, query, release } = createLockClientMock(true);
    const pool = { connect: vi.fn(async () => client) };

    await expect(
      withDigestRunLock(pool as never, async () => {
        throw new Error("boom");
      })
    ).rejects.toThrow("boom");

    expect(String(query.mock.calls[1][0])).toContain("pg_advisory_unlock");
    expect(release).toHaveBeenCalledTimes(1);
  });

  it("releases the client even when the unlock itself fails", async () => {
    const query = vi.fn(async (sql: string) => {
      if (sql.includes("pg_try_advisory_lock")) {
        return { rows: [{ locked: true }], rowCount: 1 };
      }
      throw new Error("connection terminated");
    });
    const release = vi.fn();
    const pool = { connect: vi.fn(async () => ({ query, release })) };

    await expect(withDigestRunLock(pool as never, async () => "ran")).rejects.toThrow("connection terminated");
    expect(release).toHaveBeenCalledTimes(1);
  });
});

describe("unsubscribe URL wiring", () => {
  const SECRET = "test-secret-with-at-least-32-characters!";

  it("passes the built per-user unsubscribe URL to the mailer", async () => {
    const db = createDbMock({
      users: [{ id: USER_ALPHA, email: "a@example.com", first_name: "A" }],
      pendingByUser: { [USER_ALPHA]: [pendingRow("e1", "Jane Doe")] },
    });
    const mailer = createMailerMock();

    await sendCandidateFollowDigests(db as never, mailer, {
      live: true,
      maxUsers: 500,
      maxItemsPerEmail: 20,
      buildUnsubscribeUrl: (userId) => `https://api.example.com/api/email/unsubscribe?u=${userId}`,
    });

    expect(mailer.sent[0].unsubscribeUrl).toBe(
      `https://api.example.com/api/email/unsubscribe?u=${USER_ALPHA}`
    );
  });

  it("buildUnsubscribeUrlBuilderFromEnv returns null unless both envs are set, else signs verifiable tokens", () => {
    const saved = { ...process.env };
    try {
      delete process.env.NOTIFICATIONS_UNSUBSCRIBE_URL;
      delete process.env.NOTIFICATIONS_UNSUBSCRIBE_SECRET;
      expect(buildUnsubscribeUrlBuilderFromEnv()).toBeNull();

      process.env.NOTIFICATIONS_UNSUBSCRIBE_URL = "https://api.example.com/api/email/unsubscribe";
      expect(buildUnsubscribeUrlBuilderFromEnv()).toBeNull();

      process.env.NOTIFICATIONS_UNSUBSCRIBE_SECRET = "short";
      expect(() => buildUnsubscribeUrlBuilderFromEnv()).toThrow("at least 32 characters");

      process.env.NOTIFICATIONS_UNSUBSCRIBE_SECRET = SECRET;
      const build = buildUnsubscribeUrlBuilderFromEnv();
      expect(build).not.toBeNull();
      const url = new URL(build!(USER_ALPHA));
      expect(url.origin + url.pathname).toBe("https://api.example.com/api/email/unsubscribe");
      expect(verifyEmailUnsubscribeToken(url.searchParams.get("token") ?? "", SECRET)).toBe(USER_ALPHA);
      // Digest links keep their pre-pref shape.
      expect(url.searchParams.get("pref")).toBeNull();

      const buildAlerts = buildUnsubscribeUrlBuilderFromEnv("new_election_alerts");
      const alertUrl = new URL(buildAlerts!(USER_ALPHA));
      expect(alertUrl.searchParams.get("pref")).toBe("new_election_alerts");
      expect(verifyEmailUnsubscribeToken(alertUrl.searchParams.get("token") ?? "", SECRET)).toBe(USER_ALPHA);
    } finally {
      process.env = saved;
    }
  });
});
