import { describe, expect, it, vi } from "vitest";

import {
  DEFAULT_REMINDER_MAX_ITEMS_PER_EMAIL,
  DEFAULT_REMINDER_MAX_USERS,
  ELECTION_REMINDER_RUN_LOCK_KEY,
  parseSendElectionRemindersArgs,
  sendElectionReminders,
  withElectionReminderRunLock,
} from "../../src/scripts/sendElectionReminders.js";
import { DIGEST_RUN_LOCK_KEY } from "../../src/scripts/sendCandidateFollowDigests.js";
import { NEW_ELECTION_ALERT_RUN_LOCK_KEY } from "../../src/scripts/sendNewElectionAlerts.js";
import type { ElectionReminderEmailInput } from "../../src/pipeline/users/electionReminderMailer.js";

const USER_ALPHA = "11111111-1111-4111-8111-111111111111";
const USER_BETA = "22222222-2222-4222-8222-222222222222";
const TARGET_DATE = "2026-11-03";

type ElectionRow = {
  election_title: string;
  district_name: string;
};

function electionRow(district: string, title = "County Assessor"): ElectionRow {
  return { election_title: title, district_name: district };
}

// Routes the sender's statements by their distinguishing SQL fragments.
function createDbMock(fixtures: {
  users: Array<{ id: string; email: string; first_name: string }>;
  electionsByUser: Record<string, ElectionRow[]>;
  failMark?: boolean;
}) {
  const markedSends: Array<{ userId: string; electionDate: string }> = [];
  const query = vi.fn(async (sql: string, params?: unknown[]) => {
    if (sql.includes("target_date")) {
      return { rows: [{ target_date: TARGET_DATE }], rowCount: 1 };
    }
    if (sql.includes("INSERT INTO public.user_election_reminder_sends")) {
      if (fixtures.failMark) {
        throw new Error("mark insert failed");
      }
      markedSends.push({ userId: params?.[0] as string, electionDate: params?.[1] as string });
      return { rows: [], rowCount: 1 };
    }
    if (sql.includes("SELECT u.id, u.email")) {
      return { rows: fixtures.users, rowCount: fixtures.users.length };
    }
    if (sql.includes("district_name")) {
      const userId = params?.[0] as string;
      const rows = fixtures.electionsByUser[userId] ?? [];
      return { rows, rowCount: rows.length };
    }
    throw new Error(`Unexpected SQL in test: ${sql.slice(0, 80)}`);
  });
  return { query, markedSends };
}

function createMailerMock(failFor?: string) {
  const sent: ElectionReminderEmailInput[] = [];
  return {
    sent,
    sendReminderEmail: vi.fn(async (input: ElectionReminderEmailInput) => {
      if (failFor && input.email === failFor) {
        throw new Error("SES exploded");
      }
      sent.push(input);
    }),
  };
}

describe("parseSendElectionRemindersArgs", () => {
  it("defaults to a dry run with default caps", () => {
    expect(parseSendElectionRemindersArgs([])).toEqual({
      live: false,
      maxUsers: DEFAULT_REMINDER_MAX_USERS,
      maxItemsPerEmail: DEFAULT_REMINDER_MAX_ITEMS_PER_EMAIL,
    });
  });

  it("parses --live and both flag forms", () => {
    expect(parseSendElectionRemindersArgs(["--live", "--max-users", "10", "--max-items-per-email=5"])).toEqual({
      live: true,
      maxUsers: 10,
      maxItemsPerEmail: 5,
    });
  });
});

describe("sendElectionReminders", () => {
  const options = { live: true, maxUsers: 500, maxItemsPerEmail: 20 };

  it("dry run counts eligible users and elections without sending or marking", async () => {
    const db = createDbMock({
      users: [{ id: USER_ALPHA, email: "a@example.com", first_name: "A" }],
      electionsByUser: {
        [USER_ALPHA]: [electionRow("Los Angeles County"), electionRow("Los Angeles County", "District Attorney")],
      },
    });
    const mailer = createMailerMock();

    const result = await sendElectionReminders(db as never, mailer, { ...options, live: false });

    expect(result).toMatchObject({
      dryRun: true,
      targetElectionDate: TARGET_DATE,
      eligibleUserCount: 1,
      electionsPendingCount: 2,
      usersEmailedCount: 0,
      usersMarkedCount: 0,
      failures: [],
    });
    expect(mailer.sendReminderEmail).not.toHaveBeenCalled();
    expect(db.markedSends).toEqual([]);
    // Eligibility must encode the full contract: reminders flag, verified
    // email, a district election on the target date, and no dedupe row yet.
    const sqls = db.query.mock.calls.map((call) => String(call[0]));
    const usersSql = sqls.find((sql) => sql.includes("SELECT u.id, u.email"));
    expect(usersSql).toContain("email_election_reminders = true");
    expect(usersSql).toContain("email_verified = true");
    expect(usersSql).toContain("user_districts");
    expect(usersSql).toContain("NOT EXISTS");
    expect(usersSql).toContain("user_election_reminder_sends");
  });

  it("passes the same target date to every query in the run", async () => {
    const db = createDbMock({
      users: [{ id: USER_ALPHA, email: "a@example.com", first_name: "A" }],
      electionsByUser: { [USER_ALPHA]: [electionRow("Los Angeles County")] },
    });
    const mailer = createMailerMock();

    await sendElectionReminders(db as never, mailer, options);

    const paramDates = db.query.mock.calls
      .map((call) => call[1] as unknown[] | undefined)
      .filter((params): params is unknown[] => Array.isArray(params))
      .map((params) => params.find((value) => value === TARGET_DATE));
    // eligible users, per-user elections, mark insert
    expect(paramDates).toEqual([TARGET_DATE, TARGET_DATE, TARGET_DATE]);
  });

  it("live run sends one reminder per user and marks the dedupe log", async () => {
    const db = createDbMock({
      users: [
        { id: USER_ALPHA, email: "a@example.com", first_name: "A" },
        { id: USER_BETA, email: "b@example.com", first_name: "B" },
      ],
      electionsByUser: {
        [USER_ALPHA]: [
          electionRow("Los Angeles County"),
          electionRow("Los Angeles County", "District Attorney"),
        ],
        [USER_BETA]: [electionRow("Texas Senate District 19", "State Senator, District 19")],
      },
    });
    const mailer = createMailerMock();

    const result = await sendElectionReminders(db as never, mailer, options);

    expect(result).toMatchObject({
      dryRun: false,
      eligibleUserCount: 2,
      electionsPendingCount: 3,
      usersEmailedCount: 2,
      usersMarkedCount: 2,
      failures: [],
    });
    expect(mailer.sent).toHaveLength(2);
    expect(mailer.sent[0]).toMatchObject({
      email: "a@example.com",
      firstName: "A",
      electionDateLabel: "Tuesday, November 3, 2026",
      totalElectionCount: 2,
    });
    expect(mailer.sent[0].items[1]).toMatchObject({
      electionTitle: "District Attorney",
      districtName: "Los Angeles County",
    });
    expect(db.markedSends).toEqual([
      { userId: USER_ALPHA, electionDate: TARGET_DATE },
      { userId: USER_BETA, electionDate: TARGET_DATE },
    ]);
  });

  it("isolates a failed send: records the failure, does not mark, continues", async () => {
    const db = createDbMock({
      users: [
        { id: USER_ALPHA, email: "fail@example.com", first_name: "A" },
        { id: USER_BETA, email: "b@example.com", first_name: "B" },
      ],
      electionsByUser: {
        [USER_ALPHA]: [electionRow("Los Angeles County")],
        [USER_BETA]: [electionRow("Texas Senate District 19")],
      },
    });
    const mailer = createMailerMock("fail@example.com");

    const result = await sendElectionReminders(db as never, mailer, options);

    expect(result.failures).toEqual([{ userId: USER_ALPHA, stage: "send", reason: "SES exploded" }]);
    expect(result.usersEmailedCount).toBe(1);
    expect(db.markedSends).toEqual([{ userId: USER_BETA, electionDate: TARGET_DATE }]);
  });

  it("records a mark_after_send failure when the email went out but the log insert failed", async () => {
    const db = createDbMock({
      users: [{ id: USER_ALPHA, email: "a@example.com", first_name: "A" }],
      electionsByUser: { [USER_ALPHA]: [electionRow("Los Angeles County")] },
      failMark: true,
    });
    const mailer = createMailerMock();

    const result = await sendElectionReminders(db as never, mailer, options);

    expect(result.usersEmailedCount).toBe(1);
    expect(result.usersMarkedCount).toBe(0);
    expect(result.failures).toEqual([
      { userId: USER_ALPHA, stage: "mark_after_send", reason: "mark insert failed" },
    ]);
  });

  it("caps rendered items per email while counting every election", async () => {
    const rows = Array.from({ length: 5 }, (_, i) => electionRow("Los Angeles County", `Race ${i}`));
    const db = createDbMock({
      users: [{ id: USER_ALPHA, email: "a@example.com", first_name: "A" }],
      electionsByUser: { [USER_ALPHA]: rows },
    });
    const mailer = createMailerMock();

    const result = await sendElectionReminders(db as never, mailer, { ...options, maxItemsPerEmail: 2 });

    expect(mailer.sent[0].items).toHaveLength(2);
    expect(mailer.sent[0].totalElectionCount).toBe(5);
    expect(result.electionsPendingCount).toBe(5);
    expect(result.usersMarkedCount).toBe(1);
  });

  it("skips a user whose elections vanished between the two queries", async () => {
    const db = createDbMock({
      users: [{ id: USER_ALPHA, email: "a@example.com", first_name: "A" }],
      electionsByUser: {},
    });
    const mailer = createMailerMock();

    const result = await sendElectionReminders(db as never, mailer, options);

    expect(result.eligibleUserCount).toBe(0);
    expect(mailer.sendReminderEmail).not.toHaveBeenCalled();
    expect(db.markedSends).toEqual([]);
  });

  it("passes the built per-user unsubscribe URL to the mailer", async () => {
    const db = createDbMock({
      users: [{ id: USER_ALPHA, email: "a@example.com", first_name: "A" }],
      electionsByUser: { [USER_ALPHA]: [electionRow("Los Angeles County")] },
    });
    const mailer = createMailerMock();

    await sendElectionReminders(db as never, mailer, {
      ...options,
      buildUnsubscribeUrl: (userId) =>
        `https://api.example.com/api/email/unsubscribe?u=${userId}&pref=election_reminders`,
    });

    expect(mailer.sent[0].unsubscribeUrl).toBe(
      `https://api.example.com/api/email/unsubscribe?u=${USER_ALPHA}&pref=election_reminders`
    );
  });
});

describe("withElectionReminderRunLock", () => {
  it("uses its own lock key, distinct from the digest and alert locks", () => {
    expect(ELECTION_REMINDER_RUN_LOCK_KEY).not.toBe(DIGEST_RUN_LOCK_KEY);
    expect(ELECTION_REMINDER_RUN_LOCK_KEY).not.toBe(NEW_ELECTION_ALERT_RUN_LOCK_KEY);
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

    await expect(withElectionReminderRunLock(pool as never, fn)).resolves.toBe("ran");

    expect(query.mock.calls[0][1]).toEqual([ELECTION_REMINDER_RUN_LOCK_KEY]);
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

    await expect(withElectionReminderRunLock(pool as never, fn as never)).resolves.toBeNull();
    expect(fn).not.toHaveBeenCalled();
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

    await expect(withElectionReminderRunLock(pool as never, async () => "ran")).rejects.toThrow(
      "connection terminated"
    );
    expect(release).toHaveBeenCalledTimes(1);
  });
});
