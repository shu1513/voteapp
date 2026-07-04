import { describe, expect, it, vi } from "vitest";

import {
  DEFAULT_ALERT_MAX_ITEMS_PER_EMAIL,
  DEFAULT_ALERT_MAX_USERS,
  NEW_ELECTION_ALERT_RUN_LOCK_KEY,
  parseSendNewElectionAlertsArgs,
  sendNewElectionAlerts,
  withNewElectionAlertRunLock,
} from "../../src/scripts/sendNewElectionAlerts.js";
import { DIGEST_RUN_LOCK_KEY } from "../../src/scripts/sendCandidateFollowDigests.js";
import type { NewElectionAlertEmailInput } from "../../src/pipeline/users/newElectionAlertMailer.js";

const USER_ALPHA = "11111111-1111-4111-8111-111111111111";
const USER_BETA = "22222222-2222-4222-8222-222222222222";

type PendingRow = {
  id: string;
  election_title: string;
  election_date: string;
  district_name: string;
};

function pendingRow(id: string, district: string, overrides: Partial<PendingRow> = {}): PendingRow {
  return {
    id,
    election_title: "County Assessor",
    election_date: "2026-11-03",
    district_name: district,
    ...overrides,
  };
}

// Routes the sender's statements by their distinguishing SQL fragments.
function createDbMock(fixtures: {
  orphanCount?: number;
  users: Array<{ id: string; email: string; first_name: string }>;
  pendingByUser: Record<string, PendingRow[]>;
  failMark?: boolean;
}) {
  const markedEventIds: string[][] = [];
  const query = vi.fn(async (sql: string, params?: unknown[]) => {
    if (sql.includes("count(*)")) {
      return { rows: [{ matched: String(fixtures.orphanCount ?? 0) }], rowCount: 1 };
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
    if (sql.includes("district_name")) {
      const userId = params?.[0] as string;
      const rows = fixtures.pendingByUser[userId] ?? [];
      return { rows, rowCount: rows.length };
    }
    throw new Error(`Unexpected SQL in test: ${sql.slice(0, 80)}`);
  });
  return { query, markedEventIds };
}

function createMailerMock(failFor?: string) {
  const sent: NewElectionAlertEmailInput[] = [];
  return {
    sent,
    sendAlertEmail: vi.fn(async (input: NewElectionAlertEmailInput) => {
      if (failFor && input.email === failFor) {
        throw new Error("SES exploded");
      }
      sent.push(input);
    }),
  };
}

describe("parseSendNewElectionAlertsArgs", () => {
  it("defaults to a dry run with default caps", () => {
    expect(parseSendNewElectionAlertsArgs([])).toEqual({
      live: false,
      maxUsers: DEFAULT_ALERT_MAX_USERS,
      maxItemsPerEmail: DEFAULT_ALERT_MAX_ITEMS_PER_EMAIL,
    });
  });

  it("parses --live and both flag forms", () => {
    expect(parseSendNewElectionAlertsArgs(["--live", "--max-users", "10", "--max-items-per-email=5"])).toEqual({
      live: true,
      maxUsers: 10,
      maxItemsPerEmail: 5,
    });
  });
});

describe("sendNewElectionAlerts", () => {
  const options = { live: true, maxUsers: 500, maxItemsPerEmail: 20 };

  it("dry run counts orphans and pending events without sending or marking", async () => {
    const db = createDbMock({
      orphanCount: 2,
      users: [{ id: USER_ALPHA, email: "a@example.com", first_name: "A" }],
      pendingByUser: {
        [USER_ALPHA]: [pendingRow("e1", "Los Angeles County"), pendingRow("e2", "Los Angeles County")],
      },
    });
    const mailer = createMailerMock();

    const result = await sendNewElectionAlerts(db as never, mailer, { ...options, live: false });

    expect(result).toMatchObject({
      dryRun: true,
      resolvedWithoutEmailCount: 2,
      eligibleUserCount: 1,
      eventsPendingCount: 2,
      usersEmailedCount: 0,
      eventsDeliveredCount: 0,
      failures: [],
    });
    expect(mailer.sendAlertEmail).not.toHaveBeenCalled();
    expect(db.markedEventIds).toEqual([]);
    // Eligibility must mirror deliverability: alerts flag, verified email,
    // still-in-district join, future date.
    const sqls = db.query.mock.calls.map((call) => String(call[0]));
    const usersSql = sqls.find((sql) => sql.includes("SELECT u.id, u.email"));
    expect(usersSql).toContain("email_new_election_alerts = true");
    expect(usersSql).toContain("email_verified = true");
    expect(usersSql).toContain("user_districts");
  });

  it("live run sends one alert per user and marks exactly the sent events", async () => {
    const db = createDbMock({
      users: [
        { id: USER_ALPHA, email: "a@example.com", first_name: "A" },
        { id: USER_BETA, email: "b@example.com", first_name: "B" },
      ],
      pendingByUser: {
        [USER_ALPHA]: [
          pendingRow("e1", "Los Angeles County"),
          pendingRow("e2", "Los Angeles County", { election_title: "District Attorney" }),
        ],
        [USER_BETA]: [pendingRow("e3", "Texas Senate District 19")],
      },
    });
    const mailer = createMailerMock();

    const result = await sendNewElectionAlerts(db as never, mailer, options);

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
      electionTitle: "District Attorney",
      districtName: "Los Angeles County",
    });
    expect(db.markedEventIds).toEqual([["e1", "e2"], ["e3"]]);
  });

  it("isolates a failed send: records the failure, does not mark, continues", async () => {
    const db = createDbMock({
      users: [
        { id: USER_ALPHA, email: "fail@example.com", first_name: "A" },
        { id: USER_BETA, email: "b@example.com", first_name: "B" },
      ],
      pendingByUser: {
        [USER_ALPHA]: [pendingRow("e1", "Los Angeles County")],
        [USER_BETA]: [pendingRow("e2", "Texas Senate District 19")],
      },
    });
    const mailer = createMailerMock("fail@example.com");

    const result = await sendNewElectionAlerts(db as never, mailer, options);

    expect(result.failures).toEqual([{ userId: USER_ALPHA, stage: "send", reason: "SES exploded" }]);
    expect(result.usersEmailedCount).toBe(1);
    expect(db.markedEventIds).toEqual([["e2"]]);
  });

  it("records a mark_after_send failure when the email went out but stamping failed", async () => {
    const db = createDbMock({
      users: [{ id: USER_ALPHA, email: "a@example.com", first_name: "A" }],
      pendingByUser: { [USER_ALPHA]: [pendingRow("e1", "Los Angeles County")] },
      failMark: true,
    });
    const mailer = createMailerMock();

    const result = await sendNewElectionAlerts(db as never, mailer, options);

    expect(result.usersEmailedCount).toBe(1);
    expect(result.eventsDeliveredCount).toBe(0);
    expect(result.failures).toEqual([
      { userId: USER_ALPHA, stage: "mark_after_send", reason: "mark update failed" },
    ]);
  });

  it("caps rendered items per email while marking and counting every event", async () => {
    const rows = Array.from({ length: 5 }, (_, i) => pendingRow(`e${i}`, "Los Angeles County"));
    const db = createDbMock({
      users: [{ id: USER_ALPHA, email: "a@example.com", first_name: "A" }],
      pendingByUser: { [USER_ALPHA]: rows },
    });
    const mailer = createMailerMock();

    const result = await sendNewElectionAlerts(db as never, mailer, { ...options, maxItemsPerEmail: 2 });

    expect(mailer.sent[0].items).toHaveLength(2);
    expect(mailer.sent[0].totalEventCount).toBe(5);
    expect(result.eventsDeliveredCount).toBe(5);
  });

  it("passes the built per-user unsubscribe URL to the mailer", async () => {
    const db = createDbMock({
      users: [{ id: USER_ALPHA, email: "a@example.com", first_name: "A" }],
      pendingByUser: { [USER_ALPHA]: [pendingRow("e1", "Los Angeles County")] },
    });
    const mailer = createMailerMock();

    await sendNewElectionAlerts(db as never, mailer, {
      ...options,
      buildUnsubscribeUrl: (userId) => `https://api.example.com/api/email/unsubscribe?u=${userId}&pref=new_election_alerts`,
    });

    expect(mailer.sent[0].unsubscribeUrl).toBe(
      `https://api.example.com/api/email/unsubscribe?u=${USER_ALPHA}&pref=new_election_alerts`
    );
  });
});

describe("withNewElectionAlertRunLock", () => {
  it("uses its own lock key, distinct from the digest lock", () => {
    expect(NEW_ELECTION_ALERT_RUN_LOCK_KEY).not.toBe(DIGEST_RUN_LOCK_KEY);
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

    await expect(withNewElectionAlertRunLock(pool as never, fn)).resolves.toBe("ran");

    expect(query.mock.calls[0][1]).toEqual([NEW_ELECTION_ALERT_RUN_LOCK_KEY]);
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

    await expect(withNewElectionAlertRunLock(pool as never, fn as never)).resolves.toBeNull();
    expect(fn).not.toHaveBeenCalled();
    expect(release).toHaveBeenCalledTimes(1);
  });
});
