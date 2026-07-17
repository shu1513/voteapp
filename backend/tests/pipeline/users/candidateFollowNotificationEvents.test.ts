import { describe, expect, it, vi } from "vitest";

import {
  CANDIDATE_ELECTION_WITHDRAWAL_EVENT_TYPE,
  CANDIDATE_FUTURE_ELECTION_EVENT_TYPE,
  CANDIDATE_RECORD_UPDATE_EVENT_TYPE,
  CandidateFollowNotificationEventsError,
  createCandidateElectionWithdrawalNotificationEvents,
  createCandidateFutureElectionNotificationEvents,
  createCandidateRecordUpdateNotificationEvents,
} from "../../../src/pipeline/users/candidateFollowNotificationEvents.js";

const candidateRecordId = "11111111-1111-4111-8111-111111111111";
const candidateId = "22222222-2222-4222-8222-222222222222";
const electionId = "33333333-3333-4333-8333-333333333333";

function createMockQueryable() {
  return {
    query: vi.fn(),
  };
}

function expectNotificationEventsError(error: unknown, code: string): void {
  expect(error).toBeInstanceOf(CandidateFollowNotificationEventsError);
  expect((error as CandidateFollowNotificationEventsError).code).toBe(code);
}

describe("createCandidateRecordUpdateNotificationEvents", () => {
  it("rejects invalid candidate record IDs before querying", async () => {
    const db = createMockQueryable();

    await expect(createCandidateRecordUpdateNotificationEvents(db, "not-a-uuid")).rejects.toSatisfy((error) => {
      expectNotificationEventsError(error, "invalid_candidate_record_id");
      return true;
    });

    expect(db.query).not.toHaveBeenCalled();
  });

  it("creates record-update events for users who follow the candidate with updates enabled", async () => {
    const db = createMockQueryable();
    db.query.mockResolvedValueOnce({ rowCount: 2, rows: [{ id: "a" }, { id: "b" }] });

    await expect(createCandidateRecordUpdateNotificationEvents(db, candidateRecordId)).resolves.toEqual({
      createdCount: 2,
    });

    expect(db.query).toHaveBeenCalledTimes(1);
    const sql = String(db.query.mock.calls[0]?.[0]);
    expect(sql).toContain("FROM public.candidate_records AS record");
    expect(sql).toContain("JOIN public.candidates AS candidate");
    expect(sql).toContain("candidate.deleted_at IS NULL");
    expect(sql).toContain("candidate.merged_into_candidate_id IS NULL");
    expect(sql).toContain("JOIN public.user_candidate_follows AS follow");
    expect(sql).toContain("follow.notify_updates = true");
    expect(sql).toContain("JOIN public.users AS user_row");
    expect(sql).toContain("user_row.deleted_at IS NULL");
    expect(sql).toContain("INSERT INTO public.user_candidate_follow_notification_events");
    expect(sql).toContain("$2");
    expect(sql).toContain("ON CONFLICT DO NOTHING");
    expect(db.query.mock.calls[0]?.[1]).toEqual([candidateRecordId, CANDIDATE_RECORD_UPDATE_EVENT_TYPE]);
  });

  it("falls back to returned row length when rowCount is unavailable", async () => {
    const db = createMockQueryable();
    db.query.mockResolvedValueOnce({ rows: [{ id: "a" }] });

    await expect(createCandidateRecordUpdateNotificationEvents(db, candidateRecordId)).resolves.toEqual({
      createdCount: 1,
    });
  });

  it("returns zero when duplicate or ineligible record-update events are not inserted", async () => {
    const db = createMockQueryable();
    db.query.mockResolvedValueOnce({ rowCount: 0, rows: [] });

    await expect(createCandidateRecordUpdateNotificationEvents(db, candidateRecordId)).resolves.toEqual({
      createdCount: 0,
    });

    expect(String(db.query.mock.calls[0]?.[0])).toContain("ON CONFLICT DO NOTHING");
  });
});

describe("createCandidateFutureElectionNotificationEvents", () => {
  it("rejects invalid candidate IDs before querying", async () => {
    const db = createMockQueryable();

    await expect(
      createCandidateFutureElectionNotificationEvents(db, {
        candidateId: "not-a-uuid",
        electionId,
      })
    ).rejects.toSatisfy((error) => {
      expectNotificationEventsError(error, "invalid_candidate_id");
      return true;
    });

    expect(db.query).not.toHaveBeenCalled();
  });

  it("rejects invalid election IDs before querying", async () => {
    const db = createMockQueryable();

    await expect(
      createCandidateFutureElectionNotificationEvents(db, {
        candidateId,
        electionId: "not-a-uuid",
      })
    ).rejects.toSatisfy((error) => {
      expectNotificationEventsError(error, "invalid_election_id");
      return true;
    });

    expect(db.query).not.toHaveBeenCalled();
  });

  it("creates future-election events for users who follow the candidate with election alerts enabled", async () => {
    const db = createMockQueryable();
    db.query.mockResolvedValueOnce({ rowCount: 3, rows: [{ id: "a" }, { id: "b" }, { id: "c" }] });

    await expect(
      createCandidateFutureElectionNotificationEvents(db, {
        candidateId,
        electionId,
      })
    ).resolves.toEqual({ createdCount: 3 });

    expect(db.query).toHaveBeenCalledTimes(1);
    const sql = String(db.query.mock.calls[0]?.[0]);
    expect(sql).toContain("FROM public.candidates AS candidate");
    expect(sql).toContain("JOIN public.candidate_elections AS candidate_election");
    expect(sql).toContain("JOIN public.elections AS election");
    expect(sql).toContain("candidate.deleted_at IS NULL");
    expect(sql).toContain("candidate.merged_into_candidate_id IS NULL");
    // Profile re-runs re-fire this creator for existing links; a withdrawn
    // link must never (re-)announce "on the ballot".
    expect(sql).toContain("candidate_election.status <> 'withdrawn'");
    expect(sql).toContain("election.election_date >= (now() AT TIME ZONE 'Pacific/Honolulu')::date");
    expect(sql).toContain("JOIN public.user_candidate_follows AS follow");
    expect(sql).toContain("follow.notify_elections = true");
    expect(sql).toContain("JOIN public.users AS user_row");
    expect(sql).toContain("user_row.deleted_at IS NULL");
    expect(sql).toContain("INSERT INTO public.user_candidate_follow_notification_events");
    expect(sql).toContain("$3");
    expect(sql).toContain("ON CONFLICT DO NOTHING");
    expect(db.query.mock.calls[0]?.[1]).toEqual([
      candidateId,
      electionId,
      CANDIDATE_FUTURE_ELECTION_EVENT_TYPE,
    ]);
  });

  it("returns zero when duplicate or ineligible future-election events are not inserted", async () => {
    const db = createMockQueryable();
    db.query.mockResolvedValueOnce({ rowCount: 0, rows: [] });

    await expect(
      createCandidateFutureElectionNotificationEvents(db, {
        candidateId,
        electionId,
      })
    ).resolves.toEqual({ createdCount: 0 });

    expect(String(db.query.mock.calls[0]?.[0])).toContain("ON CONFLICT DO NOTHING");
  });
});

describe("createCandidateElectionWithdrawalNotificationEvents", () => {
  it("rejects invalid candidate IDs before querying", async () => {
    const db = createMockQueryable();

    await expect(
      createCandidateElectionWithdrawalNotificationEvents(db, {
        candidateId: "not-a-uuid",
        electionId,
      })
    ).rejects.toSatisfy((error) => {
      expectNotificationEventsError(error, "invalid_candidate_id");
      return true;
    });

    expect(db.query).not.toHaveBeenCalled();
  });

  it("rejects invalid election IDs before querying", async () => {
    const db = createMockQueryable();

    await expect(
      createCandidateElectionWithdrawalNotificationEvents(db, {
        candidateId,
        electionId: "not-a-uuid",
      })
    ).rejects.toSatisfy((error) => {
      expectNotificationEventsError(error, "invalid_election_id");
      return true;
    });

    expect(db.query).not.toHaveBeenCalled();
  });

  it("creates withdrawal events only for withdrawn links, upcoming elections, and election-alert follows", async () => {
    const db = createMockQueryable();
    db.query.mockResolvedValueOnce({ rowCount: 2, rows: [{ id: "a" }, { id: "b" }] });

    await expect(
      createCandidateElectionWithdrawalNotificationEvents(db, {
        candidateId,
        electionId,
      })
    ).resolves.toEqual({ createdCount: 2 });

    expect(db.query).toHaveBeenCalledTimes(1);
    const sql = String(db.query.mock.calls[0]?.[0]);
    expect(sql).toContain("JOIN public.candidate_elections AS candidate_election");
    expect(sql).toContain("candidate.deleted_at IS NULL");
    expect(sql).toContain("candidate.merged_into_candidate_id IS NULL");
    // The creator runs after the withdraw wrapper's status update; requiring
    // the withdrawn status keeps a mis-targeted call from notifying followers
    // about a link that was never withdrawn.
    expect(sql).toContain("candidate_election.status = 'withdrawn'");
    // A withdrawal recorded after election day is history, not news.
    expect(sql).toContain("election.election_date >= (now() AT TIME ZONE 'Pacific/Honolulu')::date");
    // Withdrawal is election news: same toggle as candidate_future_election.
    expect(sql).toContain("follow.notify_elections = true");
    expect(sql).toContain("user_row.deleted_at IS NULL");
    expect(sql).toContain("INSERT INTO public.user_candidate_follow_notification_events");
    expect(sql).toContain("ON CONFLICT DO NOTHING");
    expect(db.query.mock.calls[0]?.[1]).toEqual([
      candidateId,
      electionId,
      CANDIDATE_ELECTION_WITHDRAWAL_EVENT_TYPE,
    ]);
  });

  it("returns zero when duplicate or ineligible withdrawal events are not inserted", async () => {
    const db = createMockQueryable();
    db.query.mockResolvedValueOnce({ rowCount: 0, rows: [] });

    await expect(
      createCandidateElectionWithdrawalNotificationEvents(db, {
        candidateId,
        electionId,
      })
    ).resolves.toEqual({ createdCount: 0 });

    expect(String(db.query.mock.calls[0]?.[0])).toContain("ON CONFLICT DO NOTHING");
  });
});
