import { describe, expect, it, vi } from "vitest";

import {
  type CandidateFinanceSyncFecClient,
  syncCandidateFinance,
} from "../../../src/pipeline/finance/candidateFinanceSync.js";

function createMockDb() {
  return {
    query: vi.fn().mockResolvedValue({ rows: [], rowCount: 1 }),
  };
}

function createFecClient(overrides: Partial<Record<keyof CandidateFinanceSyncFecClient, ReturnType<typeof vi.fn>>> = {}) {
  return {
    getCandidateTotals: vi.fn().mockResolvedValue({
      fecCandidateId: "P80001571",
      electionYear: 2024,
      totalReceipts: 1000,
      totalDisbursements: 700,
      cashOnHand: 300,
      debtsOwed: 10,
      individualItemizedTotal: 400,
      individualUnitemizedTotal: 200,
      otherCommitteeContributions: 50,
      transfersFromAffiliatedCommittees: 25,
      sourceUrl: "https://www.fec.gov/data/candidate/P80001571/?cycle=2024",
    }),
    listCandidateCommittees: vi.fn().mockResolvedValue([
      {
        committeeId: "C00000001",
        name: "Candidate Principal Committee",
        designation: "P",
        cycles: [2024],
        sourceUrl: "https://www.fec.gov/data/committee/C00000001/",
      },
      {
        committeeId: "C00000002",
        name: "Candidate Joint Fundraising Committee",
        designation: "J",
        cycles: [2024],
        sourceUrl: "https://www.fec.gov/data/committee/C00000002/",
      },
    ]),
    getCommitteeAggregatesByEmployer: vi.fn().mockResolvedValue([
      { type: "employer", label: "Google LLC", amount: 100, count: 3 },
      { type: "employer", label: "Energy Transfer LP", amount: 50, count: 1 },
    ]),
    getCommitteeAggregatesByOccupation: vi.fn().mockResolvedValue([
      { type: "occupation", label: "Attorney", amount: 80, count: 2 },
      { type: "occupation", label: "Retired", amount: 20, count: 1 },
    ]),
    getCommitteeAggregatesBySize: vi.fn().mockResolvedValue([
      { type: "contribution_size", label: "$200-$499", amount: 120, count: 4 },
    ]),
    getOutsideSpendingTotalsByCandidate: vi.fn().mockResolvedValue({
      fecCandidateId: "P80001571",
      electionYear: 2024,
      supportTotal: 500,
      opposeTotal: 200,
      sourceUrl: "https://www.fec.gov/data/independent-expenditures/?candidate_id=P80001571&cycle=2024",
    }),
    listOutsideSpendingGroupsByCandidate: vi.fn().mockResolvedValue([]),
    ...overrides,
  } as unknown as CandidateFinanceSyncFecClient;
}

describe("candidateFinanceSync", () => {
  it("syncs candidate totals and direct campaign breakdowns", async () => {
    const db = createMockDb();
    const fecClient = createFecClient();

    const result = await syncCandidateFinance({
      db,
      fecCandidateId: " p80001571 ",
      electionYear: 2024,
      openFecOptions: { apiKeys: ["k1"], timeoutMs: 1000 },
      fecClient,
      now: new Date("2026-01-02T03:04:05.000Z"),
      perPage: 10,
    });

    expect(result).toEqual({
      fecCandidateId: "P80001571",
      electionYear: 2024,
      dryRun: false,
      directCommitteeCount: 1,
      summaryWritten: true,
      directBreakdownsWritten: 7,
      industryBreakdownsWritten: 2,
      classificationsWritten: 4,
      outsideIncluded: false,
      outsideGroupsWritten: 0,
      outsideGroupBreakdownsWritten: 0,
      outsideSupportTotal: null,
      outsideOpposeTotal: null,
    });

    expect(fecClient.getOutsideSpendingTotalsByCandidate).not.toHaveBeenCalled();
    expect(fecClient.getCommitteeAggregatesByEmployer).toHaveBeenCalledTimes(1);
    expect(fecClient.getCommitteeAggregatesByEmployer).toHaveBeenCalledWith(
      { committeeId: "C00000001", electionYear: 2024, perPage: 10 },
      { apiKeys: ["k1"], timeoutMs: 1000 }
    );
    expect(fecClient.getCommitteeAggregatesByOccupation).toHaveBeenCalledTimes(1);
    expect(fecClient.getCommitteeAggregatesBySize).toHaveBeenCalledTimes(1);

    expect(db.query).toHaveBeenCalledTimes(13);
    expect(String(db.query.mock.calls[0]?.[0])).toContain("INSERT INTO public.candidate_finance_summaries");
    expect(String(db.query.mock.calls[0]?.[0])).toContain("total_receipts = EXCLUDED.total_receipts");
    expect(String(db.query.mock.calls[0]?.[0])).not.toContain("COALESCE(EXCLUDED.total_receipts");
    expect(db.query.mock.calls[0]?.[1]).toEqual([
      "P80001571",
      2024,
      1000,
      700,
      300,
      10,
      400,
      200,
      50,
      25,
      null,
      null,
      "https://www.fec.gov/data/candidate/P80001571/?cycle=2024",
      "2026-01-02T03:04:05.000Z",
    ]);

    const directBreakdownCalls = db.query.mock.calls.filter((call) =>
      String(call[0]).includes("INSERT INTO public.candidate_finance_direct_breakdowns")
    );
    expect(directBreakdownCalls).toHaveLength(7);
    expect(directBreakdownCalls.map((call) => call[1]?.slice(2, 6))).toEqual(
      expect.arrayContaining([
        ["employer", "Google LLC", 100, 3],
        ["employer", "Energy Transfer LP", 50, 1],
        ["occupation", "Attorney", 80, 2],
        ["occupation", "Retired", 20, 1],
        ["contribution_size", "$200-$499", 120, 4],
        ["industry", "technology", 100, 3],
        ["industry", "oil_gas_energy", 50, 1],
      ])
    );

    const classificationCalls = db.query.mock.calls.filter((call) =>
      String(call[0]).includes("INSERT INTO public.finance_label_classifications")
    );
    expect(classificationCalls).toHaveLength(4);
    expect(classificationCalls.map((call) => call[1])).toEqual(
      expect.arrayContaining([
        ["Google LLC", "employer", "GOOGLE", "technology", "high", "rule"],
        ["Energy Transfer LP", "employer", "ENERGY TRANSFER", "oil_gas_energy", "high", "rule"],
        ["Attorney", "occupation", "ATTORNEY", "legal", "high", "rule"],
        ["Retired", "occupation", "RETIRED", null, "high", "rule"],
      ])
    );
    const staleDirectDeleteCalls = db.query.mock.calls.filter((call) =>
      String(call[0]).includes("DELETE FROM public.candidate_finance_direct_breakdowns")
    );
    expect(staleDirectDeleteCalls).toHaveLength(1);
    expect(staleDirectDeleteCalls[0]?.[1]).toEqual(["P80001571", 2024, "2026-01-02T03:04:05.000Z"]);
  });

  it("wraps finance writes in a transaction when a pool client is available", async () => {
    const client = {
      query: vi.fn().mockResolvedValue({ rows: [], rowCount: 1 }),
      release: vi.fn(),
    };
    const db = {
      query: vi.fn(),
      connect: vi.fn().mockResolvedValue(client),
    };
    const fecClient = createFecClient();

    await syncCandidateFinance({
      db,
      fecCandidateId: "P80001571",
      electionYear: 2024,
      openFecOptions: { apiKeys: ["k1"] },
      fecClient,
      now: new Date("2026-01-02T03:04:05.000Z"),
    });

    expect(db.connect).toHaveBeenCalledTimes(1);
    expect(db.query).not.toHaveBeenCalled();
    expect(client.query.mock.calls[0]?.[0]).toBe("BEGIN");
    expect(client.query.mock.calls.at(-1)?.[0]).toBe("COMMIT");
    expect(
      client.query.mock.calls.some((call) => String(call[0]).includes("DELETE FROM public.candidate_finance_direct_breakdowns"))
    ).toBe(true);
    expect(client.release).toHaveBeenCalledTimes(1);
  });

  it("syncs outside spending groups and funder breakdowns when requested", async () => {
    const db = createMockDb();
    const fecClient = createFecClient({
      getCommitteeAggregatesByEmployer: vi
        .fn()
        .mockResolvedValueOnce([{ type: "employer", label: "Google LLC", amount: 100, count: 3 }])
        .mockResolvedValueOnce([{ type: "employer", label: "Energy Transfer LP", amount: 300, count: 2 }])
        .mockResolvedValueOnce([{ type: "employer", label: "Cantor Fitzgerald", amount: 125, count: 1 }]),
      getCommitteeAggregatesByOccupation: vi
        .fn()
        .mockResolvedValueOnce([{ type: "occupation", label: "Attorney", amount: 80, count: 2 }])
        .mockResolvedValueOnce([{ type: "occupation", label: "Oil and Gas", amount: 250, count: 1 }])
        .mockResolvedValueOnce([{ type: "occupation", label: "Investor", amount: 125, count: 1 }]),
      getCommitteeAggregatesBySize: vi.fn().mockResolvedValue([{ type: "contribution_size", label: "$200-$499", amount: 120, count: 4 }]),
      listOutsideSpendingGroupsByCandidate: vi
        .fn()
        .mockResolvedValueOnce([
          {
            committeeId: "C00825851",
            committeeName: "Make America Great Again Inc.",
            supportOppose: "support",
            amount: 500,
            count: 5,
            sourceUrl: "https://www.fec.gov/data/independent-expenditures/?committee_id=C00825851",
          },
        ])
        .mockResolvedValueOnce([
          {
            committeeId: "C00999999",
            committeeName: "Oppose Candidate PAC",
            supportOppose: "oppose",
            amount: 200,
            count: 2,
            sourceUrl: "https://www.fec.gov/data/independent-expenditures/?committee_id=C00999999",
          },
        ]),
    });

    const result = await syncCandidateFinance({
      db,
      fecCandidateId: "P80001571",
      electionYear: 2024,
      openFecOptions: { apiKeys: ["k1"], timeoutMs: 1000 },
      fecClient,
      includeOutside: true,
      outsideGroupLimit: 2,
      now: new Date("2026-01-02T03:04:05.000Z"),
    });

    expect(result).toMatchObject({
      outsideIncluded: true,
      outsideGroupsWritten: 2,
      outsideGroupBreakdownsWritten: 6,
      outsideSupportTotal: 500,
      outsideOpposeTotal: 200,
      directBreakdownsWritten: 4,
      industryBreakdownsWritten: 1,
      classificationsWritten: 6,
    });

    expect(fecClient.getOutsideSpendingTotalsByCandidate).toHaveBeenCalledWith(
      "P80001571",
      2024,
      { apiKeys: ["k1"], timeoutMs: 1000 }
    );
    expect(fecClient.listOutsideSpendingGroupsByCandidate).toHaveBeenCalledWith(
      { fecCandidateId: "P80001571", electionYear: 2024, supportOppose: "support", perPage: 2 },
      { apiKeys: ["k1"], timeoutMs: 1000 }
    );
    expect(fecClient.listOutsideSpendingGroupsByCandidate).toHaveBeenCalledWith(
      { fecCandidateId: "P80001571", electionYear: 2024, supportOppose: "oppose", perPage: 2 },
      { apiKeys: ["k1"], timeoutMs: 1000 }
    );

    const summaryCall = db.query.mock.calls.find((call) =>
      String(call[0]).includes("INSERT INTO public.candidate_finance_summaries")
    );
    expect(summaryCall?.[1]?.slice(10, 12)).toEqual([500, 200]);

    const outsideGroupCalls = db.query.mock.calls.filter((call) =>
      String(call[0]).includes("INSERT INTO public.candidate_finance_outside_groups (")
    );
    expect(outsideGroupCalls.map((call) => call[1]?.slice(2, 6))).toEqual(
      expect.arrayContaining([
        ["C00825851", "Make America Great Again Inc.", "support", 500],
        ["C00999999", "Oppose Candidate PAC", "oppose", 200],
      ])
    );

    const outsideBreakdownCalls = db.query.mock.calls.filter((call) =>
      String(call[0]).includes("INSERT INTO public.candidate_finance_outside_group_breakdowns (")
    );
    expect(outsideBreakdownCalls.map((call) => call[1]?.slice(2, 8))).toEqual(
      expect.arrayContaining([
        ["C00825851", "support", "employer", "Energy Transfer LP", 300, 2],
        ["C00825851", "support", "occupation", "Oil and Gas", 250, 1],
        ["C00825851", "support", "industry", "oil_gas_energy", 300, 2],
        ["C00999999", "oppose", "employer", "Cantor Fitzgerald", 125, 1],
        ["C00999999", "oppose", "occupation", "Investor", 125, 1],
        ["C00999999", "oppose", "industry", "finance_investment", 125, 1],
      ])
    );
    expect(
      db.query.mock.calls.filter((call) =>
        String(call[0]).includes("DELETE FROM public.candidate_finance_outside_group_breakdowns")
      )
    ).toHaveLength(1);
    expect(
      db.query.mock.calls.filter((call) =>
        String(call[0]).includes("DELETE FROM public.candidate_finance_outside_groups")
      )
    ).toHaveLength(1);
  });

  it("aggregates duplicate labels across multiple direct committees", async () => {
    const db = createMockDb();
    const fecClient = createFecClient({
      listCandidateCommittees: vi.fn().mockResolvedValue([
        { committeeId: "C00000001", name: "Principal", designation: "P", cycles: [2024], sourceUrl: "" },
        { committeeId: "C00000003", name: "Authorized", designation: "A", cycles: [2024], sourceUrl: "" },
      ]),
      getCommitteeAggregatesByEmployer: vi
        .fn()
        .mockResolvedValueOnce([{ type: "employer", label: "Google", amount: 100, count: 2 }])
        .mockResolvedValueOnce([{ type: "employer", label: "Google LLC", amount: 50, count: 1 }]),
      getCommitteeAggregatesByOccupation: vi.fn().mockResolvedValue([]),
      getCommitteeAggregatesBySize: vi.fn().mockResolvedValue([]),
    });

    const result = await syncCandidateFinance({
      db,
      fecCandidateId: "P80001571",
      electionYear: 2024,
      openFecOptions: { apiKeys: ["k1"] },
      fecClient,
      now: new Date("2026-01-02T03:04:05.000Z"),
    });

    expect(result.directCommitteeCount).toBe(2);
    expect(result.directBreakdownsWritten).toBe(2);
    expect(result.industryBreakdownsWritten).toBe(1);

    const directBreakdownCalls = db.query.mock.calls.filter((call) =>
      String(call[0]).includes("INSERT INTO public.candidate_finance_direct_breakdowns")
    );
    expect(directBreakdownCalls.map((call) => call[1]?.slice(2, 6))).toEqual(
      expect.arrayContaining([
        ["employer", "Google", 150, 3],
        ["industry", "technology", 150, 3],
      ])
    );
  });

  it("uses AI fallback for high-value unknown employer classifications", async () => {
    const db = createMockDb();
    const fecClient = createFecClient({
      getCommitteeAggregatesByEmployer: vi.fn().mockResolvedValue([
        { type: "employer", label: "Acme Quantum Labs LLC", amount: 150_000, count: 4 },
      ]),
      getCommitteeAggregatesByOccupation: vi.fn().mockResolvedValue([]),
      getCommitteeAggregatesBySize: vi.fn().mockResolvedValue([]),
    });
    const financeIndustryClassifier = vi.fn().mockResolvedValue([
      {
        rawLabel: "Acme Quantum Labs LLC",
        labelType: "employer",
        normalizedLabel: "ACME QUANTUM LABS",
        industrySlug: "technology",
        confidence: "medium",
        classificationSource: "ai",
        matchedRule: null,
      },
    ]);

    const result = await syncCandidateFinance({
      db,
      fecCandidateId: "P80001571",
      electionYear: 2024,
      openFecOptions: { apiKeys: ["k1"] },
      fecClient,
      financeIndustryClassifier,
    });

    expect(financeIndustryClassifier).toHaveBeenCalledWith({
      labels: [
        {
          rawLabel: "Acme Quantum Labs LLC",
          labelType: "employer",
          normalizedLabel: "ACME QUANTUM LABS",
          amount: 150_000,
        },
      ],
    });
    expect(result.directBreakdownsWritten).toBe(2);
    expect(result.industryBreakdownsWritten).toBe(1);
    expect(result.classificationsWritten).toBe(1);

    const directBreakdownCalls = db.query.mock.calls.filter((call) =>
      String(call[0]).includes("INSERT INTO public.candidate_finance_direct_breakdowns")
    );
    expect(directBreakdownCalls.map((call) => call[1]?.slice(2, 6))).toEqual(
      expect.arrayContaining([
        ["employer", "Acme Quantum Labs LLC", 150_000, 4],
        ["industry", "technology", 150_000, 4],
      ])
    );

    const classificationCall = db.query.mock.calls.find((call) =>
      String(call[0]).includes("INSERT INTO public.finance_label_classifications")
    );
    expect(classificationCall?.[1]).toEqual([
      "Acme Quantum Labs LLC",
      "employer",
      "ACME QUANTUM LABS",
      "technology",
      "medium",
      "ai",
    ]);
  });

  it("uses cached finance label classifications before calling AI", async () => {
    const db = {
      query: vi.fn().mockImplementation(async (sql: string) => {
        if (String(sql).includes("FROM public.finance_label_classifications AS classification")) {
          return {
            rows: [
              {
                raw_label: "Acme Quantum Labs LLC",
                label_type: "employer",
                normalized_label: "ACME QUANTUM LABS",
                industry_slug: "technology",
                confidence: "medium",
                classification_source: "ai",
              },
            ],
            rowCount: 1,
          };
        }
        return { rows: [], rowCount: 1 };
      }),
    };
    const fecClient = createFecClient({
      getCommitteeAggregatesByEmployer: vi.fn().mockResolvedValue([
        { type: "employer", label: "Acme Quantum Labs LLC", amount: 150_000, count: 4 },
      ]),
      getCommitteeAggregatesByOccupation: vi.fn().mockResolvedValue([]),
      getCommitteeAggregatesBySize: vi.fn().mockResolvedValue([]),
    });
    const financeIndustryClassifier = vi.fn();

    const result = await syncCandidateFinance({
      db,
      fecCandidateId: "P80001571",
      electionYear: 2024,
      openFecOptions: { apiKeys: ["k1"] },
      fecClient,
      financeIndustryClassifier,
    });

    expect(financeIndustryClassifier).not.toHaveBeenCalled();
    expect(result.industryBreakdownsWritten).toBe(1);
    expect(result.classificationsWritten).toBe(1);
  });

  it("does not call AI fallback below the default high-value threshold", async () => {
    const db = createMockDb();
    const fecClient = createFecClient({
      getCommitteeAggregatesByEmployer: vi.fn().mockResolvedValue([
        { type: "employer", label: "Acme Quantum Labs LLC", amount: 50_000, count: 4 },
      ]),
      getCommitteeAggregatesByOccupation: vi.fn().mockResolvedValue([]),
      getCommitteeAggregatesBySize: vi.fn().mockResolvedValue([]),
    });
    const financeIndustryClassifier = vi.fn();

    const result = await syncCandidateFinance({
      db,
      fecCandidateId: "P80001571",
      electionYear: 2024,
      openFecOptions: { apiKeys: ["k1"] },
      fecClient,
      financeIndustryClassifier,
    });

    expect(financeIndustryClassifier).not.toHaveBeenCalled();
    expect(result.directBreakdownsWritten).toBe(1);
    expect(result.industryBreakdownsWritten).toBe(0);
    expect(result.classificationsWritten).toBe(1);
  });

  it("does not write when dryRun is true", async () => {
    const db = createMockDb();
    const fecClient = createFecClient();

    const result = await syncCandidateFinance({
      db,
      fecCandidateId: "P80001571",
      electionYear: 2024,
      openFecOptions: { apiKeys: ["k1"] },
      fecClient,
      includeOutside: true,
      dryRun: true,
    });

    expect(result).toMatchObject({
      dryRun: true,
      directCommitteeCount: 1,
      summaryWritten: false,
      directBreakdownsWritten: 0,
      industryBreakdownsWritten: 0,
      classificationsWritten: 0,
      outsideIncluded: true,
      outsideGroupsWritten: 0,
      outsideGroupBreakdownsWritten: 0,
      outsideSupportTotal: 500,
      outsideOpposeTotal: 200,
    });
    expect(db.query).not.toHaveBeenCalled();
  });

  it("clears stale direct breakdowns when FEC returns no candidate totals and no direct committees", async () => {
    const db = createMockDb();
    const fecClient = createFecClient({
      getCandidateTotals: vi.fn().mockResolvedValue(null),
      listCandidateCommittees: vi.fn().mockResolvedValue([
        { committeeId: "C00000002", name: "Joint", designation: "J", cycles: [2024], sourceUrl: "" },
      ]),
    });

    const result = await syncCandidateFinance({
      db,
      fecCandidateId: "P80001571",
      electionYear: 2024,
      openFecOptions: { apiKeys: ["k1"] },
      fecClient,
    });

    expect(result).toEqual({
      fecCandidateId: "P80001571",
      electionYear: 2024,
      dryRun: false,
      directCommitteeCount: 0,
      summaryWritten: false,
      directBreakdownsWritten: 0,
      industryBreakdownsWritten: 0,
      classificationsWritten: 0,
      outsideIncluded: false,
      outsideGroupsWritten: 0,
      outsideGroupBreakdownsWritten: 0,
      outsideSupportTotal: null,
      outsideOpposeTotal: null,
    });
    expect(fecClient.getCommitteeAggregatesByEmployer).not.toHaveBeenCalled();
    expect(db.query).toHaveBeenCalledTimes(1);
    expect(String(db.query.mock.calls[0]?.[0])).toContain("DELETE FROM public.candidate_finance_direct_breakdowns");
  });

  it("rejects invalid input before calling FEC", async () => {
    const db = createMockDb();
    const fecClient = createFecClient();

    await expect(
      syncCandidateFinance({
        db,
        fecCandidateId: "X00000001",
        electionYear: 2024,
        openFecOptions: { apiKeys: ["k1"] },
        fecClient,
      })
    ).rejects.toThrow("Invalid FEC candidate ID");

    expect(fecClient.getCandidateTotals).not.toHaveBeenCalled();
    expect(db.query).not.toHaveBeenCalled();
  });
});
