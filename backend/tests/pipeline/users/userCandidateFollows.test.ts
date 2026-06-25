import { describe, expect, it, vi } from "vitest";

import {
  listUserCandidateFollows,
  setUserCandidateFollow,
  UserCandidateFollowsError,
} from "../../../src/pipeline/users/userCandidateFollows.js";

const userId = "11111111-1111-4111-8111-111111111111";
const candidateIdA = "22222222-2222-4222-8222-222222222222";
const candidateIdB = "33333333-3333-4333-8333-333333333333";

function createMockQueryable() {
  return {
    query: vi.fn(),
  };
}

function createMockTransactionalDb() {
  const client = {
    query: vi.fn(),
    release: vi.fn(),
  };
  const db = {
    connect: vi.fn().mockResolvedValue(client),
  };
  return { db, client };
}

function expectCandidateFollowsError(error: unknown, code: string): void {
  expect(error).toBeInstanceOf(UserCandidateFollowsError);
  expect((error as UserCandidateFollowsError).code).toBe(code);
}

describe("listUserCandidateFollows", () => {
  it("rejects invalid user IDs before querying", async () => {
    const db = createMockQueryable();

    await expect(listUserCandidateFollows(db, "not-a-uuid")).rejects.toSatisfy((error) => {
      expectCandidateFollowsError(error, "invalid_user_id");
      return true;
    });
    expect(db.query).not.toHaveBeenCalled();
  });

  it("rejects missing or deleted users", async () => {
    const db = createMockQueryable();
    db.query.mockResolvedValueOnce({ rows: [] });

    await expect(listUserCandidateFollows(db, userId)).rejects.toSatisfy((error) => {
      expectCandidateFollowsError(error, "user_not_found");
      return true;
    });

    expect(db.query).toHaveBeenCalledTimes(1);
    expect(String(db.query.mock.calls[0]?.[0])).toContain("user_row.deleted_at IS NULL");
    expect(db.query.mock.calls[0]?.[1]).toEqual([userId]);
  });

  it("returns an empty list when the user follows no candidates", async () => {
    const db = createMockQueryable();
    db.query.mockResolvedValueOnce({
      rows: [
        {
          candidate_id: null,
          display_name: null,
          party: null,
          state: null,
          current_office: null,
          notify_elections: null,
          notify_updates: null,
          created_at: null,
        },
      ],
    });

    await expect(listUserCandidateFollows(db, userId)).resolves.toEqual({ follows: [] });
    expect(String(db.query.mock.calls[0]?.[0])).toContain("LEFT JOIN public.user_candidate_follows AS follow");
  });

  it("returns active followed candidates in stable saved order", async () => {
    const db = createMockQueryable();
    db.query.mockResolvedValueOnce({
      rows: [
        {
          candidate_id: candidateIdA,
          display_name: "Jane Smith",
          party: "Democratic",
          state: "CA",
          current_office: "Mayor",
          notify_elections: true,
          notify_updates: false,
          created_at: "2026-01-02T03:04:05.000Z",
        },
        {
          candidate_id: candidateIdB,
          display_name: "John Jones",
          party: "Republican",
          state: "TX",
          current_office: null,
          notify_elections: false,
          notify_updates: true,
          created_at: new Date("2026-02-03T04:05:06.000Z"),
        },
      ],
    });

    await expect(listUserCandidateFollows(db, userId)).resolves.toEqual({
      follows: [
        {
          candidate_id: candidateIdA,
          display_name: "Jane Smith",
          party: "Democratic",
          state: "CA",
          current_office: "Mayor",
          notify_elections: true,
          notify_updates: false,
          created_at: "2026-01-02T03:04:05.000Z",
        },
        {
          candidate_id: candidateIdB,
          display_name: "John Jones",
          party: "Republican",
          state: "TX",
          current_office: null,
          notify_elections: false,
          notify_updates: true,
          created_at: "2026-02-03T04:05:06.000Z",
        },
      ],
    });
    expect(String(db.query.mock.calls[0]?.[0])).toContain("ORDER BY follow.created_at ASC NULLS LAST");
  });

  it("skips stale follows whose candidate is deleted or merged", async () => {
    const db = createMockQueryable();
    db.query.mockResolvedValueOnce({
      rows: [
        {
          candidate_id: candidateIdA,
          display_name: null,
          party: null,
          state: null,
          current_office: null,
          notify_elections: true,
          notify_updates: true,
          created_at: "2026-01-02T03:04:05.000Z",
        },
      ],
    });

    await expect(listUserCandidateFollows(db, userId)).resolves.toEqual({ follows: [] });
    expect(String(db.query.mock.calls[0]?.[0])).toContain("candidate.merged_into_candidate_id IS NULL");
  });
});

describe("setUserCandidateFollow", () => {
  it("rejects invalid user IDs before opening a database connection", async () => {
    const { db } = createMockTransactionalDb();

    await expect(
      setUserCandidateFollow(db, "not-a-uuid", { candidateId: candidateIdA, following: true })
    ).rejects.toSatisfy((error) => {
      expectCandidateFollowsError(error, "invalid_user_id");
      return true;
    });
    expect(db.connect).not.toHaveBeenCalled();
  });

  it("rejects invalid candidate IDs before opening a database connection", async () => {
    const { db } = createMockTransactionalDb();

    await expect(setUserCandidateFollow(db, userId, { candidateId: "not-a-uuid", following: true })).rejects.toSatisfy(
      (error) => {
        expectCandidateFollowsError(error, "invalid_candidate_id");
        return true;
      }
    );
    expect(db.connect).not.toHaveBeenCalled();
  });

  it("rejects non-boolean follow inputs before opening a database connection", async () => {
    const { db } = createMockTransactionalDb();

    await expect(
      setUserCandidateFollow(db, userId, { candidateId: candidateIdA, following: "yes" as unknown as boolean })
    ).rejects.toSatisfy((error) => {
      expectCandidateFollowsError(error, "invalid_follow_input");
      return true;
    });
    expect(db.connect).not.toHaveBeenCalled();
  });

  it.each([
    ["notifyElections", { notifyElections: "yes" as unknown as boolean }],
    ["notifyUpdates", { notifyUpdates: "yes" as unknown as boolean }],
  ])("rejects non-boolean %s inputs before opening a database connection", async (_field, notificationInput) => {
    const { db } = createMockTransactionalDb();

    await expect(
      setUserCandidateFollow(db, userId, {
        candidateId: candidateIdA,
        following: true,
        ...notificationInput,
      })
    ).rejects.toSatisfy((error) => {
      expectCandidateFollowsError(error, "invalid_follow_input");
      return true;
    });
    expect(db.connect).not.toHaveBeenCalled();
  });

  it("rolls back when the user is missing or deleted", async () => {
    const { db, client } = createMockTransactionalDb();
    client.query.mockResolvedValueOnce({ rows: [] }).mockResolvedValueOnce({ rows: [] }).mockResolvedValueOnce({ rows: [] });

    await expect(setUserCandidateFollow(db, userId, { candidateId: candidateIdA, following: true })).rejects.toSatisfy(
      (error) => {
        expectCandidateFollowsError(error, "user_not_found");
        return true;
      }
    );

    expect(client.query.mock.calls[0]?.[0]).toBe("BEGIN");
    expect(String(client.query.mock.calls[1]?.[0])).toContain("FOR UPDATE");
    expect(client.query.mock.calls[2]?.[0]).toBe("ROLLBACK");
    expect(client.release).toHaveBeenCalledOnce();
  });

  it("rolls back when the candidate is missing, deleted, or merged", async () => {
    const { db, client } = createMockTransactionalDb();
    client.query
      .mockResolvedValueOnce({ rows: [] })
      .mockResolvedValueOnce({ rows: [{ id: userId }] })
      .mockResolvedValueOnce({ rows: [] })
      .mockResolvedValueOnce({ rows: [] });

    await expect(setUserCandidateFollow(db, userId, { candidateId: candidateIdA, following: true })).rejects.toSatisfy(
      (error) => {
        expectCandidateFollowsError(error, "candidate_not_found");
        return true;
      }
    );

    expect(String(client.query.mock.calls[2]?.[0])).toContain("merged_into_candidate_id IS NULL");
    expect(client.query.mock.calls[3]?.[0]).toBe("ROLLBACK");
    expect(client.release).toHaveBeenCalledOnce();
  });

  it("inserts a candidate follow with default notification flags", async () => {
    const { db, client } = createMockTransactionalDb();
    client.query
      .mockResolvedValueOnce({ rows: [] })
      .mockResolvedValueOnce({ rows: [{ id: userId }] })
      .mockResolvedValueOnce({ rows: [{ id: candidateIdA }] })
      .mockResolvedValueOnce({
        rows: [
          {
            candidate_id: candidateIdA,
            notify_elections: true,
            notify_updates: true,
            created_at: "2026-01-02T03:04:05.000Z",
          },
        ],
      })
      .mockResolvedValueOnce({ rows: [] });

    const result = await setUserCandidateFollow(db, userId, { candidateId: candidateIdA, following: true });

    expect(result).toEqual({
      follow: {
        candidate_id: candidateIdA,
        following: true,
        notify_elections: true,
        notify_updates: true,
        created_at: "2026-01-02T03:04:05.000Z",
      },
    });
    expect(String(client.query.mock.calls[3]?.[0])).toContain("INSERT INTO public.user_candidate_follows");
    expect(String(client.query.mock.calls[3]?.[0])).toContain("ON CONFLICT (user_id, candidate_id)");
    expect(client.query.mock.calls[3]?.[1]).toEqual([userId, candidateIdA, true, true]);
    expect(client.query.mock.calls[4]?.[0]).toBe("COMMIT");
  });

  it("updates notification flags when following an already-followed candidate", async () => {
    const { db, client } = createMockTransactionalDb();
    client.query
      .mockResolvedValueOnce({ rows: [] })
      .mockResolvedValueOnce({ rows: [{ id: userId }] })
      .mockResolvedValueOnce({ rows: [{ id: candidateIdA }] })
      .mockResolvedValueOnce({
        rows: [
          {
            candidate_id: candidateIdA,
            notify_elections: false,
            notify_updates: true,
            created_at: new Date("2026-01-02T03:04:05.000Z"),
          },
        ],
      })
      .mockResolvedValueOnce({ rows: [] });

    await expect(
      setUserCandidateFollow(db, userId, {
        candidateId: candidateIdA,
        following: true,
        notifyElections: false,
        notifyUpdates: true,
      })
    ).resolves.toEqual({
      follow: {
        candidate_id: candidateIdA,
        following: true,
        notify_elections: false,
        notify_updates: true,
        created_at: "2026-01-02T03:04:05.000Z",
      },
    });
    expect(client.query.mock.calls[3]?.[1]).toEqual([userId, candidateIdA, false, true]);
  });

  it("deletes a candidate follow idempotently when unfollowing", async () => {
    const { db, client } = createMockTransactionalDb();
    client.query
      .mockResolvedValueOnce({ rows: [] })
      .mockResolvedValueOnce({ rows: [{ id: userId }] })
      .mockResolvedValueOnce({ rows: [] })
      .mockResolvedValueOnce({ rows: [] });

    const result = await setUserCandidateFollow(db, userId, { candidateId: candidateIdA, following: false });

    expect(result).toEqual({
      follow: {
        candidate_id: candidateIdA,
        following: false,
        notify_elections: false,
        notify_updates: false,
        created_at: null,
      },
    });
    expect(String(client.query.mock.calls[2]?.[0])).toContain("DELETE FROM public.user_candidate_follows");
    expect(client.query.mock.calls[2]?.[1]).toEqual([userId, candidateIdA]);
    expect(client.query.mock.calls[3]?.[0]).toBe("COMMIT");
  });

  it("allows unfollowing a stale merged or missing candidate follow", async () => {
    const { db, client } = createMockTransactionalDb();
    client.query
      .mockResolvedValueOnce({ rows: [] })
      .mockResolvedValueOnce({ rows: [{ id: userId }] })
      .mockResolvedValueOnce({ rows: [] })
      .mockResolvedValueOnce({ rows: [] });

    await expect(setUserCandidateFollow(db, userId, { candidateId: candidateIdA, following: false })).resolves.toEqual({
      follow: {
        candidate_id: candidateIdA,
        following: false,
        notify_elections: false,
        notify_updates: false,
        created_at: null,
      },
    });

    expect(client.query).toHaveBeenCalledTimes(4);
    expect(String(client.query.mock.calls[1]?.[0])).toContain("FROM public.users");
    expect(String(client.query.mock.calls[2]?.[0])).toContain("DELETE FROM public.user_candidate_follows");
    expect(String(client.query.mock.calls[2]?.[0])).not.toContain("FROM public.candidates");
    expect(client.query.mock.calls[3]?.[0]).toBe("COMMIT");
  });
});
