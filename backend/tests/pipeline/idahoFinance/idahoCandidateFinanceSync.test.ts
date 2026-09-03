import { describe, expect, it, vi } from "vitest";

import {
  IDAHO_CFS_CONTRIBUTION_PAGE_SIZE,
  syncIdahoCandidateFinance,
  type IdahoCandidateFinanceSyncInput,
  type IdahoCfsDataClient,
} from "../../../src/pipeline/idahoFinance/idahoCandidateFinanceSync.js";
import { contribution, GUID_A, GUID_B, GUID_C, independentExpenditure, registration } from "./idahoTestFixtures.js";

const CANDIDATE_ID = "11111111-1111-4111-8111-111111111111";
const ELECTION_ID = "22222222-2222-4222-8222-222222222222";
const LINK_ID = "33333333-3333-4333-8333-333333333333";
const NOW = new Date("2026-09-03T12:00:00.000Z");
const PROFILE_URL = `https://sunshine.voteidaho.gov/public/cf/candidateprofile?guid=${GUID_A}&tabName=CAN&isLegacy=false`;

// The linked 2026 registration, the same entity's 2024 registration for the
// same office (IE filers target it — finding 3), and another entity.
const LINKED = registration({ registrationGuid: GUID_A, totalRaised: 1500, totalSpent: 50, balanceOfFunds: -25 });
const PRIOR = registration({ registrationGuid: GUID_B, filerRegistrationId: 1200, electionYear: 2024, status: "Terminated" });
const OTHER = registration({
  registrationGuid: GUID_C,
  entityGuid: "22222222-2222-4222-8222-222222222202",
  filerEntityId: 999,
  filerName: "Other, Person",
  firstName: "Person",
  middleName: null,
  lastName: "Other",
});
const GRID = [LINKED, PRIOR, OTHER];

const ROWS = [
  contribution({ transactionId: 1, guid: "33333333-3333-4333-8333-333333333301", transactionAmount: 1000 }),
  contribution({ transactionId: 2, guid: "33333333-3333-4333-8333-333333333302", transactionAmount: 500, sourceTypeCode: "TPAC" }),
  // Another registration of the same filer name: ignored by the aggregator.
  contribution({ transactionId: 3, guid: "33333333-3333-4333-8333-333333333303", filerRegistrationGuid: GUID_B, electionYear: 2024 }),
];

const IE_ROWS = [
  independentExpenditure({ guid: "44444444-4444-4444-8444-444444444401", amountApplied: 250 }),
  // Targets the prior registration: counted for the race, not stored in the linked artifact.
  independentExpenditure({
    guid: "44444444-4444-4444-8444-444444444402",
    candidateMeasureFilerRegistrationGuid: GUID_B,
    amountApplied: 100,
    stance: "Oppose",
    filerName: "Other PAC",
    filerRegistrationGuid: "55555555-5555-4555-8555-555555555502",
  }),
];

function createClient(overrides: Partial<IdahoCfsDataClient> = {}): IdahoCfsDataClient {
  return {
    getRegistrations: vi.fn().mockResolvedValue(GRID),
    getContributionPage: vi.fn().mockResolvedValue({ items: ROWS, totalItems: ROWS.length }),
    getIndependentExpenditures: vi.fn().mockResolvedValue(IE_ROWS),
    ...overrides,
  };
}

function baseInput(overrides: Partial<IdahoCandidateFinanceSyncInput> = {}): IdahoCandidateFinanceSyncInput {
  return {
    db: { query: vi.fn(), connect: vi.fn() } as never,
    candidateId: CANDIDATE_ID,
    electionId: ELECTION_ID,
    candidateName: "Todd Achilles",
    electionYear: 2026,
    officeScope: "state_upper",
    officeName: "State Senator",
    district: "16",
    link: { registrationGuid: GUID_A, filerName: "Achilles, Todd Baker", linkSource: "sunshine_grid", sourceUrl: null },
    registrations: GRID,
    expenditureRows: IE_ROWS,
    cfsClient: createClient(),
    now: NOW,
    storeArtifactFn: vi.fn().mockResolvedValue({ sha256: "abc" }),
    writeSnapshotFn: vi
      .fn()
      .mockResolvedValue({ linkId: LINK_ID, summaryWritten: true, directBreakdownsWritten: 4, outsideGroupsWritten: 2 }),
    ...overrides,
  };
}

describe("syncIdahoCandidateFinance", () => {
  it("writes grid totals, row breakdowns, and race-wide outside groups, and stores the registration artifact", async () => {
    const input = baseInput();
    const result = await syncIdahoCandidateFinance(input);

    expect(result).toMatchObject({
      dryRun: false,
      registrationGuid: GUID_A,
      summaryWritten: true,
      directBreakdownsWritten: 4,
      outsideGroupsWritten: 2,
      totalReceipts: 1500,
      totalDisbursements: 50,
      cashOnHand: -25,
      outsideSupportTotal: 250,
      outsideOpposeTotal: 100,
      rowCoverage: "exact",
      directCoverageNote: null,
      outsideSkippedReason: null,
      artifact: { sha256: "abc" },
    });
    expect(result.outside?.priorRegistrationRowCount).toBe(1);

    const client = input.cfsClient as ReturnType<typeof createClient>;
    expect(client.getContributionPage).toHaveBeenCalledWith(
      { filerName: "Todd Baker Achilles", pageSize: IDAHO_CFS_CONTRIBUTION_PAGE_SIZE },
      undefined
    );
    // Grid and IE list were supplied by the batch: never re-fetched.
    expect(client.getRegistrations).not.toHaveBeenCalled();
    expect(client.getIndependentExpenditures).not.toHaveBeenCalled();

    const write = vi.mocked(input.writeSnapshotFn!).mock.calls[0]![0];
    expect(write.link).toEqual({
      candidateId: CANDIDATE_ID,
      electionId: ELECTION_ID,
      electionYear: 2026,
      candidateNameNormalized: "TODD ACHILLES",
      officeName: "State Senator",
      district: "16",
      registrationGuid: GUID_A,
      filerName: "Achilles, Todd Baker",
      linkStatus: "active",
      linkSource: "sunshine_grid",
      sourceUrl: PROFILE_URL,
      lastVerifiedAt: NOW,
    });
    expect(write.syncedAt).toBe(NOW);
    expect(write.summary).toEqual({
      totalReceipts: 1500,
      directContributionTotal: 1500,
      totalDisbursements: 50,
      cashOnHand: -25,
      outsideSupportTotal: 250,
      outsideOpposeTotal: 100,
      sourceUrl: PROFILE_URL,
    });
    expect(write.directBreakdowns?.map((row) => [row.categoryType, row.categoryName, row.amount])).toEqual([
      ["contribution_size", "$1,000-$4,999", 1000],
      ["contribution_size", "$500-$999", 500],
      ["contributor_source_type", "individuals", 1000],
      ["contributor_source_type", "pac_independent", 500],
    ]);
    expect(write.outsideGroups?.map((group) => [group.supportOppose, group.filerKey, group.amount])).toEqual([
      ["support", "55555555-5555-4555-8555-555555555501", 250],
      ["oppose", "55555555-5555-4555-8555-555555555502", 100],
    ]);

    // Evidence: only rows keyed to the linked guid (cache contract).
    const stored = vi.mocked(input.storeArtifactFn!).mock.calls[0]![0];
    expect(stored.registrationGuid).toBe(GUID_A);
    expect(stored.sourceUrl).toBe(PROFILE_URL);
    expect(stored.retrievedAt).toBe(NOW);
    expect(stored.artifact.registration).toBe(LINKED);
    expect(stored.artifact.contributions.map((row) => row.transactionId)).toEqual([1, 2]);
    expect(stored.artifact.independentExpenditures.map((row) => row.guid)).toEqual([
      "44444444-4444-4444-8444-444444444401",
    ]);
    // Artifact is stored before the snapshot write.
    expect(vi.mocked(input.storeArtifactFn!).mock.invocationCallOrder[0]!).toBeLessThan(
      vi.mocked(input.writeSnapshotFn!).mock.invocationCallOrder[0]!
    );
  });

  it("fails closed on a partial contribution page and writes nothing", async () => {
    const input = baseInput({
      cfsClient: createClient({
        getContributionPage: vi.fn().mockResolvedValue({ items: ROWS, totalItems: ROWS.length + 1 }),
      }),
    });
    await expect(syncIdahoCandidateFinance(input)).rejects.toThrow("served 3 of 4 rows");
    expect(input.writeSnapshotFn).not.toHaveBeenCalled();
    expect(input.storeArtifactFn).not.toHaveBeenCalled();
  });

  it("refuses a registration missing from the grid or on another cycle", async () => {
    await expect(
      syncIdahoCandidateFinance(baseInput({ registrations: [PRIOR, OTHER] }))
    ).rejects.toThrow(`Idaho registration ${GUID_A} is not in the candidate grid`);
    await expect(
      syncIdahoCandidateFinance(baseInput({ link: { registrationGuid: GUID_B, filerName: "Achilles, Todd Baker", linkSource: "manual" } }))
    ).rejects.toThrow("is for election year 2024, link is 2026");
    await expect(
      syncIdahoCandidateFinance(baseInput({ officeScope: "county", officeName: "Prosecuting Attorney" }))
    ).rejects.toThrow("is not Idaho-finance eligible");
  });

  it("skips the outside leg with null totals when the IE list is unavailable, preserving prior outside data", async () => {
    const input = baseInput({ expenditureRows: null });
    const result = await syncIdahoCandidateFinance(input);
    expect(result.outside).toBeNull();
    expect(result.outsideSupportTotal).toBeNull();
    expect(result.outsideSkippedReason).toBe("independent expenditure list unavailable this run");
    const write = vi.mocked(input.writeSnapshotFn!).mock.calls[0]![0];
    expect(write.summary).toMatchObject({ totalReceipts: 1500, outsideSupportTotal: null, outsideOpposeTotal: null });
    expect(write.outsideGroups).toBeUndefined();
    expect(vi.mocked(input.storeArtifactFn!).mock.calls[0]![0].artifact.independentExpenditures).toEqual([]);
  });

  it("fetches the grid and the IE list itself when the batch did not supply them, and tolerates an IE failure", async () => {
    const client = createClient({ getIndependentExpenditures: vi.fn().mockRejectedValue(new Error("ie down")) });
    const input = baseInput({ registrations: undefined, expenditureRows: undefined, cfsClient: client });
    const result = await syncIdahoCandidateFinance(input);
    expect(client.getRegistrations).toHaveBeenCalledTimes(1);
    expect(result.outside).toBeNull();
    expect(result.outsideSkippedReason).toBe("ie down");
    expect(result.summaryWritten).toBe(true);
  });

  it("reports a coverage note when the search rows do not reconcile to the grid, and still writes breakdowns", async () => {
    const input = baseInput({ registrations: [registration({ registrationGuid: GUID_A, totalRaised: 2000 }), PRIOR, OTHER] });
    const result = await syncIdahoCandidateFinance(input);
    expect(result.rowCoverage).toBe("rows_below_grid");
    expect(result.directCoverageNote).toBe(
      "contribution search rows total $1500.00 against the $2000.00 state total (rows_below_grid); size and source-type breakdowns sum to $1500.00"
    );
    const write = vi.mocked(input.writeSnapshotFn!).mock.calls[0]![0];
    expect(write.summary?.totalReceipts).toBe(2000);
    expect(write.directBreakdowns).toHaveLength(4);
  });

  it("computes without writing or storing in dry-run mode", async () => {
    const input = baseInput({ dryRun: true });
    const result = await syncIdahoCandidateFinance(input);
    expect(result).toMatchObject({ dryRun: true, summaryWritten: false, totalReceipts: 1500, outsideSupportTotal: 250, artifact: null });
    expect(input.writeSnapshotFn).not.toHaveBeenCalled();
    expect(input.storeArtifactFn).not.toHaveBeenCalled();
  });
});
