import { describe, expect, it, vi } from "vitest";

import {
  aggregateNewYorkOutsideSpending,
  collectNewYorkOutsideSpending,
} from "../../../src/pipeline/newYorkFinance/newYorkOutsideSpendingAggregator.js";
import type {
  NewYorkFilerRecord,
  NewYorkParentExpenditureRow,
  NewYorkScheduleRAllocationRow,
} from "../../../src/pipeline/newYorkFinance/newYorkSodaClient.js";

function allocation(overrides: Partial<NewYorkScheduleRAllocationRow>): NewYorkScheduleRAllocationRow {
  return {
    filerId: "590891",
    committeeName: "Citizens for Affordable Rates PAC",
    candidateFirstName: "Kathy",
    candidateMiddleName: "",
    candidateLastName: "Hochul",
    officeDesc: "Governor",
    district: null,
    electionYear: "2026",
    supportOppose: "S",
    amount: 100,
    transNumber: "T-1",
    transMapping: "M-1",
    filingTransId: "F-1",
    ...overrides,
  };
}

function ieFiler(filerId: string, filerName: string): NewYorkFilerRecord {
  return {
    filerId,
    filerName,
    complianceType: "COMMITTEE",
    committeeType: "Independent Expenditure Committee",
    filerStatus: "ACTIVE",
    filerType: "State",
    officeDesc: null,
    district: null,
    countyDesc: null,
  };
}

const CFAR = ieFiler("590891", "Citizens for Affordable Rates PAC");
const PARTY_COMMITTEE: NewYorkFilerRecord = {
  ...ieFiler("11236", "New York State Democratic Committee"),
  committeeType: "Party State Committee",
};

function parents(entries: Record<string, NewYorkParentExpenditureRow[]>): ReadonlyMap<string, NewYorkParentExpenditureRow[]> {
  return new Map(Object.entries(entries));
}

const F_PARENT: NewYorkParentExpenditureRow = { transNumber: "M-1", scheduleAbbrev: "F", amount: 200 };

describe("aggregateNewYorkOutsideSpending", () => {
  it("accepts only IE-committee allocations with a clean Schedule F parent chain", () => {
    const result = aggregateNewYorkOutsideSpending({
      candidateName: "Kathy Hochul",
      allocations: [
        allocation({}),
        allocation({ filingTransId: "F-2", transNumber: "T-2", amount: 50 }),
        // Party committee allocation: excluded by the registry gate.
        allocation({ filerId: "11236", committeeName: "New York State Democratic Committee", filingTransId: "F-3" }),
        // Different candidate: never counted.
        allocation({ candidateFirstName: "Bruce", candidateLastName: "Blakeman", filingTransId: "F-4" }),
      ],
      filerRecords: new Map([
        ["590891", CFAR],
        ["11236", PARTY_COMMITTEE],
      ]),
      parentExpendituresByFiler: new Map([["590891", parents({ "M-1": [F_PARENT] })]]),
    });

    expect(result.groups).toEqual([
      {
        filerId: "590891",
        filerName: "Citizens for Affordable Rates PAC",
        supportOppose: "support",
        amount: 150,
        allocationCount: 2,
        sourceUrl: "https://data.ny.gov/d/e9ss-239a",
      },
    ]);
    expect(result.counters).toMatchObject({
      allocationRowCount: 4,
      nameMatchedRowCount: 3,
      nonIeCommitteeRowCount: 1,
      acceptedRowCount: 2,
    });
  });

  it("skips duplicate transactions, missing mappings, unresolved parents, and over-parent allocations", () => {
    const result = aggregateNewYorkOutsideSpending({
      candidateName: "Kathy Hochul",
      allocations: [
        allocation({}),
        // Duplicate filing_trans_id.
        allocation({ amount: 999 }),
        // No trans_mapping.
        allocation({ filingTransId: "F-5", transMapping: null }),
        // Mapping resolves to zero rows.
        allocation({ filingTransId: "F-6", transMapping: "M-GONE" }),
        // Mapping resolves to a non-F row.
        allocation({ filingTransId: "F-7", transMapping: "M-R" }),
        // Allocation exceeds the parent expenditure.
        allocation({ filingTransId: "F-8", transMapping: "M-SMALL", amount: 500 }),
      ],
      filerRecords: new Map([["590891", CFAR]]),
      parentExpendituresByFiler: new Map([
        [
          "590891",
          parents({
            "M-1": [F_PARENT],
            "M-R": [{ transNumber: "M-R", scheduleAbbrev: "R", amount: 500 }],
            "M-SMALL": [{ transNumber: "M-SMALL", scheduleAbbrev: "F", amount: 400 }],
          }),
        ],
      ]),
    });

    expect(result.groups).toHaveLength(1);
    expect(result.groups[0]).toMatchObject({ amount: 100, allocationCount: 1 });
    expect(result.counters).toMatchObject({
      duplicateTransactionRowCount: 1,
      unresolvedMappingRowCount: 4,
      acceptedRowCount: 1,
    });
  });

  it("keeps explicit oppose allocations separate from support", () => {
    const result = aggregateNewYorkOutsideSpending({
      candidateName: "Kathy Hochul",
      allocations: [
        allocation({}),
        allocation({ filingTransId: "F-9", transNumber: "T-9", supportOppose: "O", amount: 25 }),
      ],
      filerRecords: new Map([["590891", CFAR]]),
      parentExpendituresByFiler: new Map([["590891", parents({ "M-1": [F_PARENT] })]]),
    });

    expect(result.groups.map((group) => [group.supportOppose, group.amount])).toEqual([
      ["support", 100],
      ["oppose", 25],
    ]);
  });
});

describe("collectNewYorkOutsideSpending", () => {
  it("fetches allocations, gates filers, and validates parents only for IE committees", async () => {
    const getScheduleRAllocations = vi.fn(async () => [
      allocation({}),
      allocation({ filerId: "11236", committeeName: "New York State Democratic Committee", filingTransId: "F-3" }),
    ]);
    const getFilerRecords = vi.fn(async () => new Map([
      ["590891", CFAR],
      ["11236", PARTY_COMMITTEE],
    ]));
    const getParentExpenditures = vi.fn(async () => parents({ "M-1": [F_PARENT] }));

    const result = await collectNewYorkOutsideSpending(
      {
        candidateName: "Kathy Hochul",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2026,
      },
      {},
      { getScheduleRAllocations, getFilerRecords, getParentExpenditures }
    );

    expect(getScheduleRAllocations).toHaveBeenCalledWith(
      { electionYear: 2026, boeOfficeLabels: ["Governor"], district: null },
      {}
    );
    expect(getFilerRecords).toHaveBeenCalledWith({ filerIds: ["590891", "11236"] }, {});
    // Party committee never triggers a parent lookup.
    expect(getParentExpenditures).toHaveBeenCalledTimes(1);
    expect(getParentExpenditures).toHaveBeenCalledWith({ filerId: "590891", transNumbers: ["M-1"] }, {});
    expect(result.groups).toHaveLength(1);
    expect(result.counters.nonIeCommitteeRowCount).toBe(1);
  });

  it("returns empty for unsupported offices without fetching", async () => {
    const getScheduleRAllocations = vi.fn();
    const result = await collectNewYorkOutsideSpending(
      {
        candidateName: "Jane Doe",
        officeScope: "county",
        officeName: "County Executive",
        electionYear: 2026,
      },
      {},
      { getScheduleRAllocations }
    );
    expect(result.groups).toEqual([]);
    expect(getScheduleRAllocations).not.toHaveBeenCalled();
  });
});
