import { describe, expect, it, vi } from "vitest";

import {
  DEFAULT_DIGEST_MAX_ITEMS_PER_EMAIL,
  DEFAULT_DIGEST_MAX_USERS,
  parseSendCandidateFollowDigestsArgs,
  sendCandidateFollowDigests,
} from "../../src/scripts/sendCandidateFollowDigests.js";
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

// Routes the sender's four statements by their distinguishing SQL fragments.
function createDbMock(fixtures: {
  orphanCount?: number;
  users: Array<{ id: string; email: string; first_name: string }>;
  pendingByUser: Record<string, PendingRow[]>;
}) {
  const markedEventIds: string[][] = [];
  const query = vi.fn(async (sql: string, params?: unknown[]) => {
    if (sql.includes("count(*)")) {
      return { rows: [{ matched: String(fixtures.orphanCount ?? 0) }], rowCount: 1 };
    }
    if (sql.includes("ANY($1::uuid[])")) {
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
  return { query, markedEventIds };
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

    expect(result.failures).toEqual([{ userId: USER_ALPHA, reason: "SES exploded" }]);
    expect(result.usersEmailedCount).toBe(1);
    expect(result.eventsDeliveredCount).toBe(1);
    // Only the successful user's events were marked.
    expect(db.markedEventIds).toEqual([["e2"]]);
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
