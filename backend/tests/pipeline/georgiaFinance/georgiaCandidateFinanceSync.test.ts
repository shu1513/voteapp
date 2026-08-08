import { describe, expect, it, vi } from "vitest";

import {
  buildGeorgiaSelectedReportGuids,
  discoverGeorgiaArchiveRegistrations,
  GeorgiaFinanceReconciliationError,
  syncGeorgiaCandidateFinance,
  type GeorgiaCandidateFinanceSyncInput,
} from "../../../src/pipeline/georgiaFinance/georgiaCandidateFinanceSync.js";
import {
  buildGeorgiaReportInventory,
  GeorgiaEthicsClientError,
  GEORGIA_ZERO_GUID,
  type GeorgiaCandidateIndexRow,
  type GeorgiaEthicsHost,
  type GeorgiaEthicsTransport,
  type GeorgiaFiledReportRow,
  type GeorgiaIndependentExpenditureRow,
  type GeorgiaTransactionRow,
  type GeorgiaWindowedTransactionFetchResult,
} from "../../../src/pipeline/georgiaFinance/georgiaEthicsClient.js";

const CANDIDATE_ID = "11111111-1111-4111-8111-111111111111";
const ELECTION_ID = "22222222-2222-4222-8222-222222222222";
const LINK_ID = "33333333-3333-4333-8333-333333333333";
const PF_REGISTRATION_GUID = "d973ab3b-54c2-416e-81ce-f5b1ee9a6f57";
const AR_REGISTRATION_GUID = "b31a4752-7fc6-45fb-b9b6-ffb2293d7f9e";
const AR_LEGACY_REGISTRATION_GUID = "aaaaaaaa-1111-4111-8111-aaaaaaaaaaaa";

const dummyTransport: GeorgiaEthicsTransport = {
  postJson: async () => {
    throw new Error("test must not touch the network");
  },
};

function createMockDb(mapQueryRows: unknown[] = []) {
  const query = vi.fn(async (sql: string) => {
    if (typeof sql === "string" && sql.includes("ga_finance_filer_identity_map")) {
      return { rows: mapQueryRows, rowCount: mapQueryRows.length };
    }
    return { rows: [{ id: LINK_ID }], rowCount: 1 };
  });
  const client = { query, release: vi.fn() };
  return {
    query,
    connect: vi.fn().mockResolvedValue(client),
    client,
  };
}

function indexRow(overrides: Partial<GeorgiaCandidateIndexRow> = {}): GeorgiaCandidateIndexRow {
  return {
    filerEntityId: 100035,
    filerRegistrationId: 12,
    guid: PF_REGISTRATION_GUID,
    filerName: "Carr, Christopher M.",
    committeeName: "Carr for Georgia, Inc.",
    candidateFirstName: "Christopher",
    candidateMiddleName: "Michael",
    candidateLastName: "Carr",
    ballotFullName: null,
    office: "Governor",
    districtName: null,
    filerStatusCode: "FACT",
    filingCycleName: "2026 Candidate/Committee Filing Cycle",
    electionCycleName: "2026 Georgia State Election",
    totalContributions: 3500,
    totalExpenditures: 1200,
    cashOnHand: 2300,
    ...overrides,
  };
}

function archiveIndexRow(overrides: Partial<GeorgiaCandidateIndexRow> = {}): GeorgiaCandidateIndexRow {
  return indexRow({
    filerEntityId: 757274,
    guid: AR_REGISTRATION_GUID,
    filerName: "Christopher Michael Carr",
    committeeName: "Carr for Georgia, Inc.",
    filerStatusCode: "A",
    filingCycleName: "2026 State/Statewide Election Cycle for Candidates (January and June)",
    electionCycleName: "2026 State/Statewide Election Cycle for Candidates (January and June)",
    totalContributions: 2000,
    ...overrides,
  });
}

function report(overrides: Partial<GeorgiaFiledReportRow> = {}): GeorgiaFiledReportRow {
  return {
    filerReportId: 1,
    filerReportGuid: "pf-r1",
    filerRegistrationGuid: PF_REGISTRATION_GUID,
    filerEntityId: 100035,
    reportTypeCode: "FPCFDR",
    reportName: "Campaign Contribution Disclosure Report",
    reportStatus: "Filed",
    reportVersionId: 1,
    startDate: "2025-02-01T00:00:00",
    endDate: "2025-06-30T00:00:00",
    filedDate: "2025-07-01T00:00:00",
    hasChild: false,
    childVersions: [],
    ...overrides,
  };
}

function transactionRow(overrides: Partial<GeorgiaTransactionRow> = {}): GeorgiaTransactionRow {
  return {
    guid: `t-${overrides.transactionId ?? 1}`,
    transactionId: 1,
    transactionAmount: 100,
    filerEntityId: 100035,
    filerRegistrationGuid: PF_REGISTRATION_GUID,
    filerReportGuid: "pf-r1",
    timedFiledReportGuid: null,
    filerReportId: 1,
    filerReportVersionId: 1,
    transactionDate: "11/22/2025",
    sourceName: "Jane Example",
    payeeOccupation: "Attorney",
    payeeEmployer: "Example LLP",
    transactionTypeCode: "TCON",
    transactionSubTypeCode: "ITMY",
    transactionSubTypeDesc: "Itemized Contribution",
    transactionSourceTypeCode: "TIND",
    transactionStatusCode: "TFIL",
    reportName: "Campaign Contribution Disclosure Report",
    electionYear: 2026,
    ...overrides,
  };
}

function windowedResult(rows: GeorgiaTransactionRow[]): GeorgiaWindowedTransactionFetchResult {
  return {
    rows,
    windows: [],
    windowFilterIneffectiveCount: 0,
    sweepPassCount: 2,
    sweepOnlyRowCount: 0,
    sweepMissedRowCount: 0,
  };
}

function ieRow(overrides: Partial<GeorgiaIndependentExpenditureRow> = {}): GeorgiaIndependentExpenditureRow {
  return {
    guid: `ie-${overrides.transactionId ?? 1}`,
    transactionId: 1,
    amountApplied: 1500,
    filerRegistrationGuid: "spender-a-guid",
    filerName: "Example PAC",
    filerReportGuid: "ie-r1",
    timedFiledReportGuid: null,
    filerReportVersionId: 1,
    transactionDate: "2026-02-01T00:00:00",
    transactionStatusCode: "TFIL",
    transactionTypeCode: "TIE",
    electionYear: 2026,
    candidateMeasures: [
      {
        candidateMeasureTitle: "Carr for Georgia, Inc.",
        stance: "Support",
        reasonTypeCode: "CAN",
        filerRegistrationGuid: PF_REGISTRATION_GUID,
      },
    ],
    ...overrides,
  };
}

// The Carr-shaped scenario: one migrated report both hosts hold (PeachFile
// wins), one archive-only report, one timed PeachFile report, plus a legacy
// terminated archive registration that must stay out.
function carrFetchers() {
  const peachfileReports = [
    report({
      filerReportGuid: "pf-r1",
      hasChild: true,
      reportVersionId: 2,
      childVersions: [
        {
          filerReportGuid: "pf-r1-v2",
          filerReportVersionId: 2,
          reportStatus: "Version 2",
          filedDate: "2025-08-01T00:00:00",
          filePath: null,
        },
      ],
    }),
    report({
      filerReportId: 2,
      filerReportGuid: "pf-timed-1",
      reportTypeCode: "FPTBDR",
      reportName: "Two Business Day Report",
      startDate: "2026-01-10T00:00:00",
      endDate: "2026-01-10T00:00:00",
    }),
    // Another filer's report caught by the name substring — must be scoped
    // out by registration guid.
    report({
      filerReportId: 3,
      filerReportGuid: "pf-foreign",
      filerRegistrationGuid: "99999999-9999-4999-8999-999999999999",
      filerEntityId: 999,
    }),
  ];
  const archiveReports = [
    // Archive copy of pf-r1 (same family + period) — superseded by PeachFile.
    report({
      filerReportId: 37,
      filerReportGuid: "ar-r1",
      filerRegistrationGuid: AR_REGISTRATION_GUID,
      filerEntityId: 757274,
      reportTypeCode: "103",
    }),
    // Archive-only report.
    report({
      filerReportId: 38,
      filerReportGuid: "ar-r2",
      filerRegistrationGuid: AR_REGISTRATION_GUID,
      filerEntityId: 757274,
      reportTypeCode: "103",
      startDate: "2024-07-01T00:00:00",
      endDate: "2025-01-31T00:00:00",
    }),
    // Legacy registration's report — scoped out.
    report({
      filerReportId: 39,
      filerReportGuid: "ar-legacy-r1",
      filerRegistrationGuid: AR_LEGACY_REGISTRATION_GUID,
      filerEntityId: 2750,
      reportTypeCode: "103",
      startDate: "2025-07-01T00:00:00",
      endDate: "2025-12-31T00:00:00",
    }),
  ];

  const peachfileTransactions = [
    // Lands on the child-version guid of the winning report.
    transactionRow({ transactionId: 1, transactionAmount: 1000, filerReportGuid: "pf-r1-v2" }),
    // Timed-pending: zero GUID + timed report guid.
    transactionRow({
      transactionId: 2,
      transactionAmount: 500,
      filerReportGuid: GEORGIA_ZERO_GUID,
      timedFiledReportGuid: "pf-timed-1",
      transactionStatusCode: "TPEN",
      sourceName: "Late Donor",
      payeeOccupation: "Physician",
    }),
    // Unassigned report group — excluded, counted.
    transactionRow({ transactionId: 3, transactionAmount: 250, filerReportGuid: "pf-unknown" }),
  ];
  const archiveTransactions = [
    // On the archive-only report — included.
    transactionRow({
      transactionId: 11,
      transactionAmount: 2000,
      filerEntityId: 757274,
      filerRegistrationGuid: AR_REGISTRATION_GUID,
      filerReportGuid: "ar-r2",
      transactionSubTypeCode: "MOI",
      transactionSourceTypeCode: "IND",
      transactionStatusCode: "F",
      sourceName: "Archive Donor",
      payeeOccupation: "Retired",
    }),
    // On the superseded archive copy — excluded as expected.
    transactionRow({
      transactionId: 12,
      transactionAmount: 300,
      filerEntityId: 757274,
      filerRegistrationGuid: AR_REGISTRATION_GUID,
      filerReportGuid: "ar-r1",
      transactionSubTypeCode: "MOI",
      transactionSourceTypeCode: "IND",
      transactionStatusCode: "F",
    }),
  ];

  return {
    fetchCandidateIndexRows: vi.fn(async (_transport: GeorgiaEthicsTransport, host: GeorgiaEthicsHost) =>
      host === "peachfile"
        ? [indexRow()]
        : [
            archiveIndexRow(),
            archiveIndexRow({
              filerEntityId: 2750,
              guid: AR_LEGACY_REGISTRATION_GUID,
              committeeName: "Friends of Chris Carr, Inc.",
              filerStatusCode: "T",
              totalContributions: 1202308.37,
            }),
            archiveIndexRow({
              filerEntityId: 9999,
              guid: "bbbbbbbb-2222-4222-8222-bbbbbbbbbbbb",
              filerName: "Carrie Smith",
              candidateFirstName: "Carrie",
              candidateMiddleName: null,
              candidateLastName: "Smith",
            }),
          ]
    ),
    fetchFiledReportRows: vi.fn(async (_transport: GeorgiaEthicsTransport, host: GeorgiaEthicsHost) =>
      host === "peachfile" ? peachfileReports : archiveReports
    ),
    fetchTransactionRowsWindowed: vi.fn(async (_transport: GeorgiaEthicsTransport, host: GeorgiaEthicsHost) =>
      windowedResult(host === "peachfile" ? peachfileTransactions : archiveTransactions)
    ),
    fetchIndependentExpenditureRows: vi.fn(async () => ({
      rows: [
        // Attributable: supporting IE from spender A.
        ieRow({ transactionId: 101, amountApplied: 1500 }),
        // Attributable: opposing IE from spender B.
        ieRow({
          transactionId: 102,
          amountApplied: 200,
          filerRegistrationGuid: "spender-b-guid",
          filerName: "Opposing PAC",
          candidateMeasures: [
            {
              candidateMeasureTitle: "Carr for Georgia, Inc.",
              stance: "Oppose",
              reasonTypeCode: "CAN",
              filerRegistrationGuid: PF_REGISTRATION_GUID,
            },
          ],
        }),
        // Multi-target row referencing Carr — quarantined dollars (D6).
        ieRow({
          transactionId: 103,
          amountApplied: 5000,
          candidateMeasures: [
            {
              candidateMeasureTitle: "Carr for Georgia, Inc.",
              stance: "Support",
              reasonTypeCode: "CAN",
              filerRegistrationGuid: PF_REGISTRATION_GUID,
            },
            {
              candidateMeasureTitle: "Someone Else for Georgia",
              stance: "Support",
              reasonTypeCode: "CAN",
              filerRegistrationGuid: "someone-else-guid",
            },
          ],
        }),
        // Another candidate's IE — invisible to Carr's aggregation.
        ieRow({
          transactionId: 104,
          amountApplied: 9999,
          candidateMeasures: [
            {
              candidateMeasureTitle: "Someone Else for Georgia",
              stance: "Support",
              reasonTypeCode: "CAN",
              filerRegistrationGuid: "someone-else-guid",
            },
          ],
        }),
      ],
      fetchedRowCount: 4,
      duplicateRowCount: 0,
      passCount: 2,
    })),
  };
}

function baseInput(
  db: ReturnType<typeof createMockDb>,
  fetchers: ReturnType<typeof carrFetchers>,
  overrides: Partial<GeorgiaCandidateFinanceSyncInput> = {}
): GeorgiaCandidateFinanceSyncInput {
  return {
    db,
    transport: dummyTransport,
    candidateId: CANDIDATE_ID,
    electionId: ELECTION_ID,
    candidateName: "Christopher Carr",
    electionYear: 2026,
    officeName: "Governor",
    district: null,
    committee: {
      committeeId: "100035",
      committeeName: "Carr for Georgia, Inc.",
      linkSource: "peachfile_api",
    },
    now: new Date("2026-08-07T12:00:00Z"),
    fetchers,
    ...overrides,
  };
}

describe("discoverGeorgiaArchiveRegistrations", () => {
  it("keeps same-person same-cycle registrations and drops terminated and foreign ones", () => {
    const discovered = discoverGeorgiaArchiveRegistrations({
      candidateName: "Christopher Carr",
      electionYear: 2026,
      archiveIndexRows: [
        archiveIndexRow(),
        archiveIndexRow({ filerEntityId: 2750, guid: AR_LEGACY_REGISTRATION_GUID, filerStatusCode: "T" }),
        archiveIndexRow({
          filerEntityId: 9999,
          guid: "bbbbbbbb-2222-4222-8222-bbbbbbbbbbbb",
          filerName: "Carrie Smith",
          candidateFirstName: "Carrie",
          candidateMiddleName: null,
          candidateLastName: "Smith",
        }),
        // Prior-cycle registration of the same person.
        archiveIndexRow({
          guid: "cccccccc-3333-4333-8333-cccccccccccc",
          filingCycleName: "2022 State/Statewide Election Cycle for Candidates (January and June)",
          electionCycleName: "2022 State/Statewide Election Cycle for Candidates (January and June)",
        }),
      ],
    });
    expect(discovered.map((row) => row.filerEntityId)).toEqual([757274]);
  });

  it("lets a middle-name conflict veto a same-cycle row", () => {
    const discovered = discoverGeorgiaArchiveRegistrations({
      candidateName: "Christopher Alan Carr",
      electionYear: 2026,
      archiveIndexRows: [archiveIndexRow()],
    });
    expect(discovered).toEqual([]);
  });
});

describe("buildGeorgiaSelectedReportGuids", () => {
  it("selects winner guids (including child versions) per host and tracks superseded archive copies", () => {
    const inventory = buildGeorgiaReportInventory({
      peachfileReports: [
        report({
          hasChild: true,
          childVersions: [
            {
              filerReportGuid: "pf-r1-v2",
              filerReportVersionId: 2,
              reportStatus: "Version 2",
              filedDate: null,
              filePath: null,
            },
          ],
        }),
      ],
      archiveReports: [report({ filerReportGuid: "ar-r1", reportTypeCode: "103" })],
    });
    const { selectedByHost, supersededArchiveGuids } = buildGeorgiaSelectedReportGuids(inventory);
    expect(selectedByHost.peachfile).toEqual(new Set(["pf-r1", "pf-r1-v2"]));
    expect(selectedByHost.efile_archive.size).toBe(0);
    expect(supersededArchiveGuids).toEqual(new Set(["ar-r1"]));
  });
});

describe("syncGeorgiaCandidateFinance", () => {
  it("syncs across both hosts with D8 source selection and writes the index-anchored snapshot", async () => {
    const db = createMockDb();
    const fetchers = carrFetchers();
    const result = await syncGeorgiaCandidateFinance(baseInput(db, fetchers));

    // Included: pf 1000 (child-version guid) + pf 500 (timed) + ar 2000
    // (archive-only report). Excluded: pf 250 (unassigned), ar 300
    // (superseded copy).
    expect(result.syncedRowSum).toBe(3500);
    expect(result.reconciliationDifference).toBe(0);
    expect(result.totalReceipts).toBe(3500);
    expect(result.totalDisbursements).toBe(1200);
    expect(result.cashOnHand).toBe(2300);
    expect(result.archiveRegistrationSource).toBe("discovered");
    expect(result.archiveRegistrationGuids).toEqual([AR_REGISTRATION_GUID]);
    expect(result.peachfile).toMatchObject({ includedRowCount: 2, unassignedRowCount: 1, supersededRowCount: 0 });
    expect(result.archive).toMatchObject({ includedRowCount: 1, supersededRowCount: 1, unassignedRowCount: 0 });
    expect(result.linkWritten).toBe(true);
    expect(result.directBreakdownsWritten).toBeGreaterThan(0);
    // Outside leg (PR 5): support 1500 + oppose 200 attributed; the $5,000
    // multi-target row referencing Carr is quarantined as excluded dollars.
    expect(result.outsideSupportTotal).toBe(1500);
    expect(result.outsideOpposeTotal).toBe(200);
    expect(result.outsideGroupsWritten).toBe(2);
    expect(result.outsideSpending).toMatchObject({
      storeRowCount: 4,
      candidateTargetRowCount: 3,
      attributedRowCount: 2,
      attributedAmount: 1700,
      multiTargetRowCount: 1,
      multiTargetAmount: 5000,
      malformedRowCount: 0,
      unrecognizedStatusRowCount: 0,
    });
    expect(fetchers.fetchIndependentExpenditureRows).toHaveBeenCalledWith(dummyTransport, "peachfile", {
      maxPasses: undefined,
    });
    expect(db.connect).toHaveBeenCalled();

    // The archive pull was keyed by the archive person display name.
    expect(fetchers.fetchTransactionRowsWindowed).toHaveBeenCalledWith(
      dummyTransport,
      "efile_archive",
      expect.objectContaining({ filerName: "Christopher Michael Carr", expectedFilerEntityIds: [757274] })
    );
    // Window range derives from the earliest inventory period start.
    expect(fetchers.fetchTransactionRowsWindowed).toHaveBeenCalledWith(
      dummyTransport,
      "peachfile",
      expect.objectContaining({ fromDate: "2024-07-01", toDate: "2026-08-07" })
    );
  });

  it("fails the sync and keeps the previous snapshot when reconciliation breaches tolerance", async () => {
    const db = createMockDb();
    const fetchers = carrFetchers();
    fetchers.fetchCandidateIndexRows.mockImplementation(async (_transport, host) =>
      host === "peachfile" ? [indexRow({ totalContributions: 999_999 })] : []
    );
    await expect(syncGeorgiaCandidateFinance(baseInput(db, fetchers))).rejects.toBeInstanceOf(
      GeorgiaFinanceReconciliationError
    );
    expect(db.connect).not.toHaveBeenCalled();
  });

  it("uses identity-map rows verbatim when present and skips archive discovery", async () => {
    const db = createMockDb([
      {
        canonical_committee_id: "100035",
        canonical_committee_name: "Carr for Georgia, Inc.",
        entity_role: "candidate_committee",
        source_system: "efile_archive",
        source_filer_entity_id: "757274",
        source_registration_guid: AR_REGISTRATION_GUID,
        source_filer_name: "Christopher Michael Carr",
        source_committee_name: "Carr for Georgia, Inc.",
        source_filing_cycle_name: null,
        include_in_candidate_totals: true,
        map_provenance: "manual",
        notes: null,
        last_verified_at: new Date("2026-08-01T00:00:00Z"),
      },
    ]);
    const fetchers = carrFetchers();
    const result = await syncGeorgiaCandidateFinance(baseInput(db, fetchers));
    expect(result.archiveRegistrationSource).toBe("identity_map");
    expect(result.syncedRowSum).toBe(3500);
    // Only the PeachFile index was fetched — no archive discovery call.
    expect(fetchers.fetchCandidateIndexRows).toHaveBeenCalledTimes(1);
    expect(fetchers.fetchCandidateIndexRows).toHaveBeenCalledWith(
      dummyTransport,
      "peachfile",
      expect.objectContaining({ filerName: "CARR" })
    );
  });

  it("treats a lone exclusion map row as authoritative — no discovery, no archive pull", async () => {
    const db = createMockDb([
      {
        canonical_committee_id: "100035",
        canonical_committee_name: "Carr for Georgia, Inc.",
        entity_role: "candidate_committee",
        source_system: "efile_archive",
        source_filer_entity_id: "2750",
        source_registration_guid: AR_LEGACY_REGISTRATION_GUID,
        source_filer_name: "Christopher Michael Carr",
        source_committee_name: "Friends of Chris Carr, Inc.",
        source_filing_cycle_name: null,
        include_in_candidate_totals: false,
        map_provenance: "manual",
        notes: "separate terminated ledger",
        last_verified_at: new Date("2026-08-01T00:00:00Z"),
      },
    ]);
    const fetchers = carrFetchers();
    // Without the archive leg only the PeachFile rows remain (1000 + 500).
    fetchers.fetchCandidateIndexRows.mockImplementation(async () => [indexRow({ totalContributions: 1500 })]);
    const result = await syncGeorgiaCandidateFinance(baseInput(db, fetchers));
    expect(result.archiveRegistrationSource).toBe("identity_map");
    expect(result.archiveRegistrationGuids).toEqual([]);
    expect(result.syncedRowSum).toBe(1500);
    const archiveTransactionCalls = fetchers.fetchTransactionRowsWindowed.mock.calls.filter(
      ([, host]) => host === "efile_archive"
    );
    expect(archiveTransactionCalls).toHaveLength(0);
  });

  it("treats a whole-pull filter_ineffective on a leg as zero rows and lets reconciliation arbitrate", async () => {
    const db = createMockDb();
    const fetchers = carrFetchers();
    fetchers.fetchTransactionRowsWindowed.mockImplementation(async (_transport, host) => {
      if (host === "efile_archive") {
        throw new GeorgiaEthicsClientError("filter_ineffective", "only foreign rows");
      }
      return windowedResult([transactionRow({ transactionId: 1, transactionAmount: 1000 })]);
    });
    fetchers.fetchCandidateIndexRows.mockImplementation(async (_transport, host) =>
      host === "peachfile" ? [indexRow({ totalContributions: 1000 })] : [archiveIndexRow()]
    );
    const result = await syncGeorgiaCandidateFinance(baseInput(db, fetchers));
    expect(result.archive.filterIneffective).toBe(true);
    expect(result.archive.includedRowCount).toBe(0);
    expect(result.syncedRowSum).toBe(1000);
  });

  it("selects the linked committee's registration by entity id AND cycle, failing on ambiguity", async () => {
    const db = createMockDb();
    const fetchers = carrFetchers();
    // A future-cycle re-registration of the same entity sorts FIRST in API
    // order — entity-id-only matching would pick it and reconcile the 2026
    // rows against the fresh cycle's zero totals.
    const futureCycleRow = indexRow({
      guid: "dddddddd-4444-4444-8444-dddddddddddd",
      filingCycleName: "2028 Candidate/Committee Filing Cycle",
      electionCycleName: "2028 Georgia State Election",
      totalContributions: 0,
      totalExpenditures: 0,
      cashOnHand: 0,
    });
    fetchers.fetchCandidateIndexRows.mockImplementation(async (_transport, host) =>
      host === "peachfile"
        ? [futureCycleRow, indexRow()]
        : [archiveIndexRow()]
    );
    const result = await syncGeorgiaCandidateFinance(baseInput(db, fetchers));
    expect(result.totalReceipts).toBe(3500);
    expect(result.syncedRowSum).toBe(3500);

    // Two registrations inside the SAME cycle is ambiguous — fail closed.
    fetchers.fetchCandidateIndexRows.mockImplementation(async (_transport, host) =>
      host === "peachfile"
        ? [indexRow(), indexRow({ guid: "eeeeeeee-5555-4555-8555-eeeeeeeeeeee" })]
        : [archiveIndexRow()]
    );
    await expect(syncGeorgiaCandidateFinance(baseInput(db, fetchers))).rejects.toThrow(/2 rows for filerEntityId/);
  });

  it("fails closed when the index row is missing official totals instead of treating them as zero", async () => {
    const db = createMockDb();
    const fetchers = carrFetchers();
    fetchers.fetchCandidateIndexRows.mockImplementation(async (_transport, host) =>
      host === "peachfile" ? [indexRow({ totalContributions: null })] : [archiveIndexRow()]
    );
    await expect(syncGeorgiaCandidateFinance(baseInput(db, fetchers))).rejects.toThrow(/missing official totals/);
    expect(db.connect).not.toHaveBeenCalled();
  });

  it("fails a nonzero index total with zero selected rows even inside the absolute tolerance floor", async () => {
    const db = createMockDb();
    const fetchers = carrFetchers();
    // $80 official total sits under the $100 floor; a dead pull (zero rows
    // everywhere) must still fail rather than deleting stored breakdowns.
    fetchers.fetchCandidateIndexRows.mockImplementation(async (_transport, host) =>
      host === "peachfile" ? [indexRow({ totalContributions: 80 })] : []
    );
    fetchers.fetchTransactionRowsWindowed.mockImplementation(async () => windowedResult([]));
    await expect(syncGeorgiaCandidateFinance(baseInput(db, fetchers))).rejects.toBeInstanceOf(
      GeorgiaFinanceReconciliationError
    );
    expect(db.connect).not.toHaveBeenCalled();

    // A true zero — official total 0, no rows — still syncs and writes the
    // (correctly empty) snapshot.
    fetchers.fetchCandidateIndexRows.mockImplementation(async (_transport, host) =>
      host === "peachfile" ? [indexRow({ totalContributions: 0, totalExpenditures: 0, cashOnHand: 0 })] : []
    );
    const result = await syncGeorgiaCandidateFinance(baseInput(db, fetchers));
    expect(result.syncedRowSum).toBe(0);
    expect(result.totalReceipts).toBe(0);
    expect(db.connect).toHaveBeenCalled();
  });

  it("fails when the PeachFile index has no row for the linked committee", async () => {
    const db = createMockDb();
    const fetchers = carrFetchers();
    fetchers.fetchCandidateIndexRows.mockImplementation(async () => []);
    await expect(syncGeorgiaCandidateFinance(baseInput(db, fetchers))).rejects.toThrow(
      /0 rows for filerEntityId 100035/
    );
    expect(db.connect).not.toHaveBeenCalled();
  });

  it("uses pre-fetched IE store rows without calling the IE fetcher", async () => {
    const db = createMockDb();
    const fetchers = carrFetchers();
    const result = await syncGeorgiaCandidateFinance(
      baseInput(db, fetchers, {
        independentExpenditureRows: [ieRow({ transactionId: 201, amountApplied: 750 })],
      })
    );
    expect(result.outsideSupportTotal).toBe(750);
    expect(result.outsideOpposeTotal).toBe(0);
    expect(result.outsideGroupsWritten).toBe(1);
    expect(fetchers.fetchIndependentExpenditureRows).not.toHaveBeenCalled();
  });

  it("writes a truthful zero outside leg when the (nonempty) store has no IE targeting the candidate", async () => {
    const db = createMockDb();
    const fetchers = carrFetchers();
    // The store is never legitimately empty (client guard), but a candidate
    // nobody targeted sees only foreign rows.
    fetchers.fetchIndependentExpenditureRows.mockImplementation(async () => ({
      rows: [
        ieRow({
          transactionId: 301,
          candidateMeasures: [
            {
              candidateMeasureTitle: "Someone Else for Georgia",
              stance: "Support",
              reasonTypeCode: "CAN",
              filerRegistrationGuid: "someone-else-guid",
            },
          ],
        }),
      ],
      fetchedRowCount: 1,
      duplicateRowCount: 0,
      passCount: 2,
    }));
    const result = await syncGeorgiaCandidateFinance(baseInput(db, fetchers));
    expect(result.outsideSupportTotal).toBe(0);
    expect(result.outsideOpposeTotal).toBe(0);
    expect(result.outsideGroupsWritten).toBe(0);
    expect(result.outsideSpending?.storeRowCount).toBe(1);
    expect(result.outsideSpendingSkippedReason).toBeNull();
    expect(db.connect).toHaveBeenCalled();
  });

  it("degrades to a direct-only sync when the IE fetch fails with a client error, preserving stored outside data", async () => {
    const db = createMockDb();
    const fetchers = carrFetchers();
    fetchers.fetchIndependentExpenditureRows.mockImplementation(async () => {
      throw new GeorgiaEthicsClientError("bad_response", "stable EMPTY store");
    });
    const result = await syncGeorgiaCandidateFinance(baseInput(db, fetchers));
    // Direct leg still written in full.
    expect(result.syncedRowSum).toBe(3500);
    expect(result.directBreakdownsWritten).toBeGreaterThan(0);
    expect(db.connect).toHaveBeenCalled();
    // Outside leg skipped: null totals (preserveWhenNull keeps stored
    // values), zero groups written, reason carried through.
    expect(result.outsideSupportTotal).toBeNull();
    expect(result.outsideOpposeTotal).toBeNull();
    expect(result.outsideGroupsWritten).toBe(0);
    expect(result.outsideSpending).toBeNull();
    expect(result.outsideSpendingSkippedReason).toContain("stable EMPTY store");

    // A non-client error is a bug and still fails the sync.
    fetchers.fetchIndependentExpenditureRows.mockImplementation(async () => {
      throw new TypeError("boom");
    });
    await expect(syncGeorgiaCandidateFinance(baseInput(db, fetchers))).rejects.toThrow("boom");
  });

  it("skips the outside leg without fetching when the batch passes the null store sentinel", async () => {
    const db = createMockDb();
    const fetchers = carrFetchers();
    const result = await syncGeorgiaCandidateFinance(baseInput(db, fetchers, { independentExpenditureRows: null }));
    expect(fetchers.fetchIndependentExpenditureRows).not.toHaveBeenCalled();
    expect(result.outsideSpending).toBeNull();
    expect(result.outsideSupportTotal).toBeNull();
    expect(result.outsideSpendingSkippedReason).toContain("IE store unavailable");
    expect(result.syncedRowSum).toBe(3500);
    expect(db.connect).toHaveBeenCalled();
  });

  it("dry run aggregates and reconciles without writing", async () => {
    const db = createMockDb();
    const fetchers = carrFetchers();
    const result = await syncGeorgiaCandidateFinance(baseInput(db, fetchers, { dryRun: true }));
    expect(result.dryRun).toBe(true);
    expect(result.linkWritten).toBe(false);
    expect(result.summaryWritten).toBe(false);
    expect(result.directBreakdownsWritten).toBe(0);
    expect(result.outsideGroupsWritten).toBe(0);
    expect(result.outsideSupportTotal).toBe(1500);
    expect(result.syncedRowSum).toBe(3500);
    expect(db.connect).not.toHaveBeenCalled();
  });
});
