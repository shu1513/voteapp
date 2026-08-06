import { describe, expect, it, vi } from "vitest";

import {
  listUserCandidateFollows,
  setUserCandidateFollow,
  USER_CANDIDATE_FOLLOW_LIMIT,
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
          latest_record_description: null,
          latest_record_event_date: null,
          active_election_id: null,
          active_election_title: null,
          active_election_date: null,
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
          latest_record_description: null,
          latest_record_event_date: null,
          active_election_id: null,
          active_election_title: null,
          active_election_date: null,
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
          latest_record: null,
          active_election: null,
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
          latest_record: null,
          active_election: null,
          notify_elections: false,
          notify_updates: true,
          created_at: "2026-02-03T04:05:06.000Z",
        },
      ],
    });
    expect(String(db.query.mock.calls[0]?.[0])).toContain("ORDER BY follow.created_at ASC NULLS LAST");
  });

  it("includes lightweight latest-record and active-election previews", async () => {
    const db = createMockQueryable();
    const electionId = "44444444-4444-4444-8444-444444444444";
    db.query.mockResolvedValueOnce({
      rows: [
        {
          candidate_id: candidateIdA,
          display_name: "Jane Smith",
          party: "Democratic",
          state: "CA",
          current_office: "Mayor",
          latest_record_description: "Sponsored a housing affordability bill.",
          latest_record_event_date: "2026-01-15",
          active_election_id: electionId,
          active_election_title: "Mayor",
          active_election_date: "2026-11-03",
          notify_elections: true,
          notify_updates: true,
          created_at: "2026-01-02T03:04:05.000Z",
        },
      ],
    });

    const result = await listUserCandidateFollows(db, userId);

    expect(result).toEqual({
      follows: [
        {
          candidate_id: candidateIdA,
          display_name: "Jane Smith",
          party: "Democratic",
          state: "CA",
          current_office: "Mayor",
          latest_record: {
            description: "Sponsored a housing affordability bill.",
            event_date: "2026-01-15",
          },
          active_election: {
            election_id: electionId,
            official_ballot_title: "Mayor",
            election_date: "2026-11-03",
          },
          notify_elections: true,
          notify_updates: true,
          created_at: "2026-01-02T03:04:05.000Z",
        },
      ],
    });
    expect(result.follows[0]).not.toHaveProperty("records");
    const sql = String(db.query.mock.calls[0]?.[0]);
    expect(sql).toContain("LEFT JOIN LATERAL");
    expect(sql).toContain("FROM public.candidate_records AS record");
    expect(sql).toContain("candidate_election.status NOT IN ('withdrawn', 'lost')");
    expect(sql).toContain("election.election_date >= (now() AT TIME ZONE 'Pacific/Honolulu')::date");
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
      .mockResolvedValueOnce({ rows: [{ count: "0" }] })
      .mockResolvedValueOnce({ rows: [] });

    await expect(setUserCandidateFollow(db, userId, { candidateId: candidateIdA, following: true })).rejects.toSatisfy(
      (error) => {
        expectCandidateFollowsError(error, "candidate_not_found");
        return true;
      }
    );

    expect(String(client.query.mock.calls[3]?.[0])).toContain("WITH followable_candidate AS");
    expect(String(client.query.mock.calls[3]?.[0])).toContain("merged_into_candidate_id IS NULL");
    expect(client.query.mock.calls[4]?.[0]).toBe("ROLLBACK");
    expect(client.release).toHaveBeenCalledOnce();
  });

  it("rejects new follows past the limit and rolls back", async () => {
    const { db, client } = createMockTransactionalDb();
    client.query
      .mockResolvedValueOnce({ rows: [] })
      .mockResolvedValueOnce({ rows: [{ id: userId }] })
      .mockResolvedValueOnce({ rows: [{ count: String(USER_CANDIDATE_FOLLOW_LIMIT) }] });

    await expect(setUserCandidateFollow(db, userId, { candidateId: candidateIdA, following: true })).rejects.toSatisfy(
      (error) => {
        expectCandidateFollowsError(error, "follow_limit_reached");
        return true;
      }
    );

    // The count excludes the candidate being followed, so flag updates on an
    // existing follow still work at the limit.
    expect(String(client.query.mock.calls[2]?.[0])).toContain("candidate_id <> $2::uuid");
    expect(client.query.mock.calls[2]?.[1]).toEqual([userId, candidateIdA]);
    expect(client.query.mock.calls[3]?.[0]).toBe("ROLLBACK");
    expect(client.release).toHaveBeenCalledOnce();
  });

  it("allows updating an existing follow when other follows sit at the limit boundary", async () => {
    const { db, client } = createMockTransactionalDb();
    client.query
      .mockResolvedValueOnce({ rows: [] })
      .mockResolvedValueOnce({ rows: [{ id: userId }] })
      .mockResolvedValueOnce({ rows: [{ count: String(USER_CANDIDATE_FOLLOW_LIMIT - 1) }] })
      .mockResolvedValueOnce({
        rows: [
          {
            candidate_id: candidateIdA,
            notify_elections: false,
            notify_updates: true,
            created_at: "2026-01-02T03:04:05.000Z",
          },
        ],
      })
      .mockResolvedValueOnce({ rows: [] });

    await expect(
      setUserCandidateFollow(db, userId, { candidateId: candidateIdA, following: true, notifyElections: false })
    ).resolves.toMatchObject({ follow: { candidate_id: candidateIdA, following: true } });
    expect(client.query.mock.calls[4]?.[0]).toBe("COMMIT");
  });

  it("inserts a candidate follow with default notification flags", async () => {
    const { db, client } = createMockTransactionalDb();
    client.query
      .mockResolvedValueOnce({ rows: [] })
      .mockResolvedValueOnce({ rows: [{ id: userId }] })
      .mockResolvedValueOnce({ rows: [{ count: "0" }] })
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
    expect(String(client.query.mock.calls[3]?.[0])).toContain("WITH followable_candidate AS");
    expect(String(client.query.mock.calls[3]?.[0])).not.toContain("FOR SHARE");
    expect(String(client.query.mock.calls[3]?.[0])).toContain("COALESCE($3::boolean, true)");
    expect(client.query.mock.calls[3]?.[1]).toEqual([userId, candidateIdA, null, null]);
    expect(client.query.mock.calls[4]?.[0]).toBe("COMMIT");
  });

  it("updates notification flags when following an already-followed candidate", async () => {
    const { db, client } = createMockTransactionalDb();
    client.query
      .mockResolvedValueOnce({ rows: [] })
      .mockResolvedValueOnce({ rows: [{ id: userId }] })
      .mockResolvedValueOnce({ rows: [{ count: "1" }] })
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

  it("preserves existing notification flags when following without optional flags", async () => {
    const { db, client } = createMockTransactionalDb();
    client.query
      .mockResolvedValueOnce({ rows: [] })
      .mockResolvedValueOnce({ rows: [{ id: userId }] })
      .mockResolvedValueOnce({ rows: [{ count: "1" }] })
      .mockResolvedValueOnce({
        rows: [
          {
            candidate_id: candidateIdA,
            notify_elections: false,
            notify_updates: true,
            created_at: "2026-01-02T03:04:05.000Z",
          },
        ],
      })
      .mockResolvedValueOnce({ rows: [] });

    await expect(setUserCandidateFollow(db, userId, { candidateId: candidateIdA, following: true })).resolves.toEqual({
      follow: {
        candidate_id: candidateIdA,
        following: true,
        notify_elections: false,
        notify_updates: true,
        created_at: "2026-01-02T03:04:05.000Z",
      },
    });
    expect(String(client.query.mock.calls[3]?.[0])).toContain(
      "notify_elections = COALESCE($3::boolean, user_candidate_follows.notify_elections)"
    );
    expect(String(client.query.mock.calls[3]?.[0])).toContain(
      "notify_updates = COALESCE($4::boolean, user_candidate_follows.notify_updates)"
    );
    expect(client.query.mock.calls[3]?.[1]).toEqual([userId, candidateIdA, null, null]);
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
