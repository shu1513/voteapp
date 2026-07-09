import { beforeEach, describe, expect, it, vi } from "vitest";

const { stageManualElectionPayloadMock } = vi.hoisted(() => ({
  stageManualElectionPayloadMock: vi.fn(),
}));

vi.mock("../../src/scripts/injectManualElections.js", () => ({
  stageManualElectionPayload: stageManualElectionPayloadMock,
}));

import { bulkInjectManualNoResultElections } from "../../src/scripts/bulkInjectManualNoResultElections.js";

const protectedId = "11111111-1111-4111-8111-111111111111";
const failedId = "22222222-2222-4222-8222-222222222222";
const stagedId = "33333333-3333-4333-8333-333333333333";

describe("bulkInjectManualNoResultElections", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("preserves active staging and continues after a district staging failure", async () => {
    const query = vi.fn().mockResolvedValue({
      rowCount: 3,
      rows: [
        {
          id: protectedId,
          name: "Protected District",
          district_type: "place",
          state: "WA",
          has_future_election: false,
          existing_staging_status: "validated",
        },
        {
          id: failedId,
          name: "Failed District",
          district_type: "place",
          state: "WA",
          has_future_election: false,
          existing_staging_status: null,
        },
        {
          id: stagedId,
          name: "Staged District",
          district_type: "place",
          state: "WA",
          has_future_election: false,
          existing_staging_status: null,
        },
      ],
    });
    stageManualElectionPayloadMock
      .mockRejectedValueOnce(new Error("Redis unavailable"))
      .mockResolvedValueOnce({ staged: true, redisMessageId: "123-0" });

    const summary = await bulkInjectManualNoResultElections(
      { query } as unknown as Parameters<typeof bulkInjectManualNoResultElections>[0],
      {} as Parameters<typeof bulkInjectManualNoResultElections>[1],
      {
        districtIds: [protectedId, failedId, stagedId],
        dryRun: false,
        runId: "bulk-test-run",
        runYear: 2026,
      }
    );

    expect(summary).toEqual({
      staged: 1,
      missingDistricts: [],
      skippedWithFutureElections: [],
      skippedWithExistingStaging: [{ districtId: protectedId, status: "validated" }],
      failed: [{ districtId: failedId, reason: "Redis unavailable" }],
    });
    expect(stageManualElectionPayloadMock).toHaveBeenCalledTimes(2);
    expect(stageManualElectionPayloadMock.mock.calls[0]?.[2]).toMatchObject({
      ingestKey: `manual:elections:${failedId}:2026`,
      overwriteExisting: false,
    });
    expect(stageManualElectionPayloadMock.mock.calls[1]?.[2]).toMatchObject({
      ingestKey: `manual:elections:${stagedId}:2026`,
      overwriteExisting: false,
    });
  });
});
