import { beforeEach, describe, expect, it, vi } from "vitest";

const poolQueryMock = vi.hoisted(() => vi.fn());
const poolConnectMock = vi.hoisted(() => vi.fn());
const poolEndMock = vi.hoisted(() => vi.fn());
const redisConnectMock = vi.hoisted(() => vi.fn());
const redisQuitMock = vi.hoisted(() => vi.fn());
const redisXGroupCreateMock = vi.hoisted(() => vi.fn());
const redisXAutoClaimMock = vi.hoisted(() => vi.fn());
const redisSendCommandMock = vi.hoisted(() => vi.fn());
const redisXReadGroupMock = vi.hoisted(() => vi.fn());
const redisXAckMock = vi.hoisted(() => vi.fn());
const redisXAddMock = vi.hoisted(() => vi.fn());
const enrichCandidateProfileMock = vi.hoisted(() => vi.fn());
const enqueueCandidateRecordDraftsMock = vi.hoisted(() => vi.fn());
const enqueueCandidateLinkCandidateFinanceSyncJobMock = vi.hoisted(() => vi.fn());

vi.mock("pg", () => ({
  Pool: vi.fn(() => ({
    query: poolQueryMock,
    connect: poolConnectMock,
    end: poolEndMock,
  })),
}));

vi.mock("redis", () => ({
  createClient: vi.fn(() => ({
    connect: redisConnectMock,
    quit: redisQuitMock,
    xGroupCreate: redisXGroupCreateMock,
    xAutoClaim: redisXAutoClaimMock,
    sendCommand: redisSendCommandMock,
    xReadGroup: redisXReadGroupMock,
    xAck: redisXAckMock,
    xAdd: redisXAddMock,
  })),
}));

vi.mock("../../src/config/env.js", () => ({
  getPipelineEnv: () => ({
    DATABASE_URL: "postgres://test/test",
    REDIS_URL: "redis://localhost:6379/0",
    AI_TIMEOUT_MS: 1000,
    ANTHROPIC_WEB_SEARCH_MAX_USES: 1,
  }),
}));

vi.mock("../../src/ai/enrichCandidateProfile.js", () => ({
  buildCandidateProfileConfigFromEnv: vi.fn(() => ({ timeoutMs: 1000 })),
  enrichCandidateProfile: enrichCandidateProfileMock,
}));

vi.mock("../../src/pipeline/candidates/candidateRecordDraftEmitter.js", () => ({
  enqueueCandidateRecordDrafts: enqueueCandidateRecordDraftsMock,
}));

vi.mock("../../src/scheduler/candidateFinanceSyncScheduler.js", () => ({
  enqueueCandidateLinkCandidateFinanceSyncJob: enqueueCandidateLinkCandidateFinanceSyncJobMock,
}));

import { runCandidateProfileEnricher } from "../../src/pipeline/enrichers/candidateProfileEnricher.js";
import {
  STAGING_CANDIDATE_PROFILE_DRAFT_STREAM,
  STAGING_ITEM_TYPE_CANDIDATE_PROFILE,
} from "../../src/config/electionsPipeline.js";

// A redelivered draft whose candidate already committed used to be acked at
// the per-election name-dedupe gate without the post-commit fanout (finance
// sync + record drafts) that may have failed on the earlier delivery. The
// gate must replay the fanout from persisted state before acking.
describe("runCandidateProfileEnricher fanout replay on redelivered drafts", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    redisConnectMock.mockResolvedValue(undefined);
    redisQuitMock.mockResolvedValue(undefined);
    redisXGroupCreateMock.mockResolvedValue(undefined);
    redisXAutoClaimMock.mockResolvedValue({ nextId: "0-0", messages: [] });
    // XPENDING delivery-count probe: absent info keeps the message eligible.
    redisSendCommandMock.mockResolvedValue([]);
    redisXAckMock.mockResolvedValue(1);
    redisXAddMock.mockResolvedValue("2-0");
    enqueueCandidateRecordDraftsMock.mockResolvedValue({ emittedCount: 1, skippedCount: 0 });
    enqueueCandidateLinkCandidateFinanceSyncJobMock.mockResolvedValue("finance-job-1");
    poolEndMock.mockResolvedValue(undefined);

    redisXReadGroupMock.mockResolvedValueOnce([
      {
        name: STAGING_CANDIDATE_PROFILE_DRAFT_STREAM,
        messages: [
          {
            id: "1-0",
            message: {
              election_id: "e-1",
              item_type: STAGING_ITEM_TYPE_CANDIDATE_PROFILE,
              run_id: "run-1",
              candidate_display_name: "Jane Doe",
            },
          },
        ],
      },
    ]);
  });

  it("replays finance sync and record drafts for an already-linked candidate before acking", async () => {
    poolQueryMock
      // findElectionLinkedCandidateByName: candidate already committed.
      .mockResolvedValueOnce({
        rows: [
          {
            id: "cand-1",
            first_name: "Jane",
            last_name: "Doe",
            fec_ids: ["H0WY00091"],
          },
        ],
      })
      // resolveElectionDraftContext -> getElectionRow
      .mockResolvedValueOnce({
        rows: [
          {
            id: "e-1",
            state: "WY",
            district_name: "Congressional District (at Large), Wyoming",
            district_type: "us_house",
            election_date: "2026-11-03",
            official_ballot_title: "United States Representative",
            election_stage: "general",
            senate_class: null,
            term_end_year: null,
            is_partisan: true,
            sources: ["https://example.org"],
            office_scope: "us_house",
            office_canonical_name: "United States Representative",
          },
        ],
      });

    await runCandidateProfileEnricher({ once: true, batchSize: 5, blockMs: 10 });

    expect(enqueueCandidateLinkCandidateFinanceSyncJobMock).toHaveBeenCalledWith(
      expect.objectContaining({
        candidateId: "cand-1",
        fecCandidateId: "H0WY00091",
        electionYear: 2026,
      })
    );
    expect(enqueueCandidateRecordDraftsMock).toHaveBeenCalledTimes(1);
    const [, drafts] = enqueueCandidateRecordDraftsMock.mock.calls[0]!;
    expect(drafts).toEqual([{ candidateId: "cand-1", electionId: "e-1", runId: "run-1" }]);
    expect(redisXAckMock).toHaveBeenCalledTimes(1);
    // The replay must short-circuit: no AI call, no candidate write transaction.
    expect(enrichCandidateProfileMock).not.toHaveBeenCalled();
    expect(poolConnectMock).not.toHaveBeenCalled();
  });

  it("parks the draft instead of guessing when two same-name duplicates are linked", async () => {
    poolQueryMock.mockResolvedValueOnce({
      rows: [
        { id: "cand-1", first_name: "Jane", last_name: "Doe", fec_ids: [] },
        { id: "cand-2", first_name: "Jane", last_name: "Doe", fec_ids: [] },
      ],
    });

    await runCandidateProfileEnricher({ once: true, batchSize: 5, blockMs: 10 });

    // Parked: rejected-stream message + ack, and no fanout against an
    // arbitrarily chosen duplicate.
    expect(redisXAddMock).toHaveBeenCalledWith(
      "staging:candidates:profile:rejected",
      "*",
      expect.objectContaining({
        reason: expect.stringContaining("merge the duplicate rows"),
      })
    );
    expect(redisXAckMock).toHaveBeenCalledTimes(1);
    expect(enqueueCandidateLinkCandidateFinanceSyncJobMock).not.toHaveBeenCalled();
    expect(enqueueCandidateRecordDraftsMock).not.toHaveBeenCalled();
  });

  it("leaves the message unacked when the record-draft replay fails", async () => {
    poolQueryMock
      .mockResolvedValueOnce({
        rows: [{ id: "cand-1", first_name: "Jane", last_name: "Doe", fec_ids: [] }],
      })
      .mockResolvedValueOnce({
        rows: [
          {
            id: "e-1",
            state: "WY",
            district_name: "Congressional District (at Large), Wyoming",
            district_type: "us_house",
            election_date: "2026-11-03",
            official_ballot_title: "United States Representative",
            election_stage: "general",
            senate_class: null,
            term_end_year: null,
            is_partisan: true,
            sources: [],
            office_scope: "us_house",
            office_canonical_name: "United States Representative",
          },
        ],
      });
    enqueueCandidateRecordDraftsMock.mockRejectedValueOnce(new Error("redis down"));

    await runCandidateProfileEnricher({ once: true, batchSize: 5, blockMs: 10 });

    expect(redisXAckMock).not.toHaveBeenCalled();
  });
});
