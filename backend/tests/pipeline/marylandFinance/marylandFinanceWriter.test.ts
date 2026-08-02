import { describe, expect, it, vi } from "vitest";

import {
  replaceMarylandCandidateFinanceSnapshot,
  upsertMarylandFinanceLink,
} from "../../../src/pipeline/marylandFinance/marylandFinanceWriter.js";

const CANDIDATE_ID = "11111111-1111-4111-8111-111111111111";
const ELECTION_ID = "22222222-2222-4222-8222-222222222222";
const LINK_ID = "33333333-3333-4333-8333-333333333333";

function baseLink() {
  return {
    candidateId: CANDIDATE_ID,
    electionId: ELECTION_ID,
    electionYear: 2026,
    candidateNameNormalized: "JUSTIN GALLUCCI",
    officeName: "State Senator",
    district: "Maryland State",
    committeeId: "16018290",
    committeeName: "Gallucci, Justin Friends of",
    linkStatus: "active" as const,
    linkSource: "cfs_public_export" as const,
    sourceUrl: "https://campaignfinance.maryland.gov/public/cf/downloads",
    lastVerifiedAt: new Date("2026-06-25T12:00:00.000Z"),
  };
}

function createMockDb() {
  const poolQuery = vi.fn().mockResolvedValue({ rows: [{ id: LINK_ID }], rowCount: 1 });
  const clientQuery = vi.fn().mockResolvedValue({ rows: [{ id: LINK_ID }], rowCount: 1 });
  const client = {
    query: clientQuery,
    release: vi.fn(),
  };
  return {
    query: poolQuery,
    connect: vi.fn().mockResolvedValue(client),
    client,
  };
}

describe("marylandFinanceWriter", () => {
  it("upserts Maryland finance links with normalized nullable fields", async () => {
    const db = {
      query: vi.fn().mockResolvedValue({ rows: [{ id: LINK_ID }], rowCount: 1 }),
    };

    await expect(
      upsertMarylandFinanceLink({
        db,
        link: {
          ...baseLink(),
          district: " ",
          committeeId: " 16018290 ",
        },
      })
    ).resolves.toEqual({ linkId: LINK_ID });

    expect(String(db.query.mock.calls[0]?.[0])).toContain("INSERT INTO public.md_candidate_finance_links");
    expect(String(db.query.mock.calls[0]?.[0])).toContain("ON CONFLICT (candidate_id, election_id, committee_id)");
    expect(db.query.mock.calls[0]?.[1]).toEqual([
      CANDIDATE_ID,
      ELECTION_ID,
      2026,
      "JUSTIN GALLUCCI",
      "State Senator",
      null,
      "16018290",
      "Gallucci, Justin Friends of",
      "active",
      "cfs_public_export",
      "https://campaignfinance.maryland.gov/public/cf/downloads",
      "2026-06-25T12:00:00.000Z",
    ]);
  });

  it("defaults optional link metadata safely", async () => {
    const db = {
      query: vi.fn().mockResolvedValue({ rows: [{ id: LINK_ID }], rowCount: 1 }),
    };

    await expect(
      upsertMarylandFinanceLink({
        db,
        link: {
          ...baseLink(),
          linkSource: undefined,
          sourceUrl: " ",
          lastVerifiedAt: null,
        },
      })
    ).resolves.toEqual({ linkId: LINK_ID });

    expect(db.query.mock.calls[0]?.[1]?.slice(8)).toEqual(["active", "manual", null, null]);
  });

  it("replaces a Maryland finance snapshot inside one transaction", async () => {
    const db = createMockDb();

    const result = await replaceMarylandCandidateFinanceSnapshot({
      db,
      link: baseLink(),
      syncedAt: new Date("2026-07-08T09:10:11.000Z"),
      summary: {
        totalReceipts: 125000,
        directContributionTotal: 110000,
        totalDisbursements: 42000,
        cashOnHand: 83000,
        outsideSupportTotal: 75000,
        outsideOpposeTotal: 5000,
        sourceUrl: "https://campaignfinance.maryland.gov/public/cf/downloads",
      },
      directBreakdowns: [
        {
          categoryType: "contribution_size",
          categoryName: "$250-$999",
          amount: 60000,
          contributorCount: 90,
          sourceUrl: "https://campaignfinance.maryland.gov/public/cf/downloads",
        },
      ],
      outsideGroups: [
        {
          committeeId: "16020184",
          committeeName: "Momentum Maryland PAC",
          supportOppose: "support",
          amount: 75000,
          sourceUrl: "https://campaignfinance.maryland.gov/public/cf/downloads",
        },
      ],
      outsideGroupBreakdowns: [
        {
          committeeId: "16020184",
          supportOppose: "support",
          categoryType: "industry",
          categoryName: "real_estate",
          amount: 75000,
          contributorCount: 3,
          sourceUrl: "https://campaignfinance.maryland.gov/public/cf/downloads",
        },
      ],
      classifications: [
        {
          rawLabel: "Maryland REALTORS",
          labelType: "donor",
          normalizedLabel: "Maryland REALTORS",
          industrySlug: "real_estate",
          confidence: "high",
          classificationSource: "rule",
          matchedRule: "organization_pattern_real_estate",
        },
      ],
    });

    expect(result).toEqual({
      linkId: LINK_ID,
      summaryWritten: true,
      directBreakdownsWritten: 1,
      outsideGroupsWritten: 1,
      outsideGroupBreakdownsWritten: 1,
    });
    expect(db.connect).toHaveBeenCalledTimes(1);
    expect(db.query).not.toHaveBeenCalled();
    expect(db.client.query.mock.calls[0]?.[0]).toBe("BEGIN");
    expect(db.client.query.mock.calls.at(-1)?.[0]).toBe("COMMIT");
    expect(db.client.release).toHaveBeenCalledTimes(1);

    const sql = db.client.query.mock.calls.map((call) => String(call[0]));
    expect(sql.filter((statement) => statement.includes("INSERT INTO public.md_candidate_finance_summaries"))).toHaveLength(1);
    expect(sql.filter((statement) => statement.includes("INSERT INTO public.md_candidate_finance_direct_breakdowns"))).toHaveLength(1);
    expect(sql.filter((statement) => statement.includes("INSERT INTO public.md_candidate_finance_outside_groups"))).toHaveLength(1);
    expect(sql.filter((statement) => statement.includes("INSERT INTO public.md_candidate_finance_outside_group_breakdowns"))).toHaveLength(1);
    expect(sql.filter((statement) => statement.includes("INSERT INTO public.finance_label_classifications"))).toHaveLength(1);
    expect(sql.filter((statement) => statement.includes("UPDATE public.md_candidate_finance_links"))).toHaveLength(1);
    expect(sql.some((statement) => statement.includes("DELETE FROM public.md_candidate_finance_direct_breakdowns"))).toBe(true);
    expect(sql.some((statement) => statement.includes("DELETE FROM public.md_candidate_finance_outside_groups"))).toBe(true);
    expect(sql.some((statement) => statement.includes("DELETE FROM public.md_candidate_finance_outside_group_breakdowns"))).toBe(true);

    const deactivationCall = db.client.query.mock.calls.find((call) =>
      String(call[0]).includes("UPDATE public.md_candidate_finance_links")
    );
    expect(deactivationCall?.[1]).toEqual([CANDIDATE_ID, ELECTION_ID, LINK_ID]);

    const summaryCall = db.client.query.mock.calls.find((call) =>
      String(call[0]).includes("INSERT INTO public.md_candidate_finance_summaries")
    );
    expect(String(summaryCall?.[0])).toContain("total_receipts = EXCLUDED.total_receipts");
    expect(String(summaryCall?.[0])).toContain("outside_support_total = COALESCE");
    expect(String(summaryCall?.[0])).toContain("EXCLUDED.outside_support_total");
    expect(String(summaryCall?.[0])).toContain("md_candidate_finance_summaries.outside_support_total");
    expect(summaryCall?.[1]).toEqual([
      LINK_ID,
      2026,
      125000,
      110000,
      42000,
      83000,
      75000,
      5000,
      "https://campaignfinance.maryland.gov/public/cf/downloads",
      "2026-07-08T09:10:11.000Z",
    ]);
  });

  it("preserves prior outside totals when a partial refresh does not have expenditure data", async () => {
    const db = createMockDb();

    await replaceMarylandCandidateFinanceSnapshot({
      db,
      link: baseLink(),
      syncedAt: new Date("2026-07-08T09:10:11.000Z"),
      summary: {
        totalReceipts: 125000,
        directContributionTotal: 110000,
        outsideSupportTotal: null,
        outsideOpposeTotal: null,
      },
    });

    const summaryCall = db.client.query.mock.calls.find((call) =>
      String(call[0]).includes("INSERT INTO public.md_candidate_finance_summaries")
    );
    // Whitespace-insensitive: the factory renders these clauses on one line
    // where the bespoke writer used three. Same SQL semantics.
    expect(String(summaryCall?.[0])).toMatch(
      /outside_support_total = COALESCE\(\s*EXCLUDED\.outside_support_total,\s*md_candidate_finance_summaries\.outside_support_total\s*\)/
    );
    expect(String(summaryCall?.[0])).toMatch(
      /outside_oppose_total = COALESCE\(\s*EXCLUDED\.outside_oppose_total,\s*md_candidate_finance_summaries\.outside_oppose_total\s*\)/
    );
    expect(summaryCall?.[1]?.[6]).toBeNull();
    expect(summaryCall?.[1]?.[7]).toBeNull();
  });

  it("normalizes outside committee ids consistently for groups, breakdowns, and stale deletes", async () => {
    const db = createMockDb();

    await replaceMarylandCandidateFinanceSnapshot({
      db,
      link: baseLink(),
      syncedAt: new Date("2026-07-08T09:10:11.000Z"),
      outsideGroups: [
        {
          committeeId: " 16020184 ",
          committeeName: "Momentum Maryland PAC",
          supportOppose: "support",
          amount: 75000,
        },
      ],
      outsideGroupBreakdowns: [
        {
          committeeId: "16020184",
          supportOppose: "support",
          categoryType: "industry",
          categoryName: "real_estate",
          amount: 75000,
        },
      ],
    });

    const outsideGroupCall = db.client.query.mock.calls.find((call) =>
      String(call[0]).includes("INSERT INTO public.md_candidate_finance_outside_groups")
    );
    const outsideBreakdownCall = db.client.query.mock.calls.find((call) =>
      String(call[0]).includes("INSERT INTO public.md_candidate_finance_outside_group_breakdowns")
    );
    const deleteOutsideBreakdownsCall = db.client.query.mock.calls.find((call) =>
      String(call[0]).includes("DELETE FROM public.md_candidate_finance_outside_group_breakdowns")
    );
    const deleteOutsideGroupsCall = db.client.query.mock.calls.find((call) =>
      String(call[0]).includes("DELETE FROM public.md_candidate_finance_outside_groups")
    );

    expect(outsideGroupCall?.[1]?.[2]).toBe("16020184");
    expect(outsideBreakdownCall?.[1]?.[2]).toBe("16020184");
    expect(JSON.parse(String(deleteOutsideBreakdownsCall?.[1]?.[2]))).toEqual([
      {
        committee_id: "16020184",
        support_oppose: "support",
        category_type: "industry",
        category_name: "real_estate",
      },
    ]);
    expect(JSON.parse(String(deleteOutsideGroupsCall?.[1]?.[2]))).toEqual([
      {
        committee_id: "16020184",
        support_oppose: "support",
      },
    ]);
  });

  it("does not delete omitted breakdown sections", async () => {
    const db = createMockDb();

    const result = await replaceMarylandCandidateFinanceSnapshot({
      db,
      link: baseLink(),
      syncedAt: new Date("2026-07-08T09:10:11.000Z"),
      summary: {
        outsideSupportTotal: 0,
        outsideOpposeTotal: 0,
      },
    });

    expect(result).toMatchObject({
      summaryWritten: true,
      directBreakdownsWritten: 0,
      outsideGroupsWritten: 0,
      outsideGroupBreakdownsWritten: 0,
    });
    const sql = db.client.query.mock.calls.map((call) => String(call[0]));
    expect(sql.some((statement) => statement.includes("DELETE FROM public.md_candidate_finance_direct_breakdowns"))).toBe(false);
    expect(sql.some((statement) => statement.includes("DELETE FROM public.md_candidate_finance_outside_groups"))).toBe(false);
    expect(sql.some((statement) => statement.includes("DELETE FROM public.md_candidate_finance_outside_group_breakdowns"))).toBe(false);
  });

  it("rejects outside group breakdowns without matching outside groups", async () => {
    const db = createMockDb();

    await expect(
      replaceMarylandCandidateFinanceSnapshot({
        db,
        link: baseLink(),
        outsideGroups: [],
        outsideGroupBreakdowns: [
          {
            committeeId: "16020184",
            supportOppose: "support",
            categoryType: "donor",
            categoryName: "Maryland REALTORS",
            amount: 75000,
          },
        ],
      })
    ).rejects.toThrow("Maryland outside group breakdowns require matching outside groups in the same snapshot");
    expect(db.connect).not.toHaveBeenCalled();
  });

  it("rejects query-only and PoolClient-like snapshot writers so writes stay atomic", async () => {
    const queryOnlyDb = { query: vi.fn().mockResolvedValue({ rows: [{ id: LINK_ID }], rowCount: 1 }) };
    await expect(
      replaceMarylandCandidateFinanceSnapshot({
        db: queryOnlyDb as never,
        link: baseLink(),
        summary: { totalReceipts: 1000 },
      })
    ).rejects.toThrow("Maryland finance snapshot writes must receive a Pool");
    expect(queryOnlyDb.query).not.toHaveBeenCalled();

    const clientLikeDb = {
      query: vi.fn().mockResolvedValue({ rows: [{ id: LINK_ID }], rowCount: 1 }),
      release: vi.fn(),
    };
    await expect(
      replaceMarylandCandidateFinanceSnapshot({
        db: clientLikeDb as never,
        link: baseLink(),
        summary: { totalReceipts: 1000 },
      })
    ).rejects.toThrow("Maryland finance snapshot writes must receive a Pool, not a PoolClient");
    expect(clientLikeDb.query).not.toHaveBeenCalled();
  });

  it("rejects invalid link inputs", async () => {
    const db = {
      query: vi.fn(),
    };

    await expect(
      upsertMarylandFinanceLink({
        db,
        link: {
          ...baseLink(),
          committeeId: " ",
        },
      })
    ).rejects.toThrow("Maryland committee id is required");

    expect(db.query).not.toHaveBeenCalled();
  });

  it("rolls back and releases the client when a transactional write fails", async () => {
    const client = {
      query: vi
        .fn()
        .mockResolvedValueOnce({ rows: [], rowCount: 0 })
        .mockRejectedValueOnce(new Error("write failed"))
        .mockResolvedValueOnce({ rows: [], rowCount: 0 }),
      release: vi.fn(),
    };
    const db = {
      query: vi.fn(),
      connect: vi.fn().mockResolvedValue(client),
    };

    await expect(
      replaceMarylandCandidateFinanceSnapshot({
        db,
        link: baseLink(),
        summary: { totalReceipts: 1000 },
      })
    ).rejects.toThrow("write failed");

    expect(client.query.mock.calls[0]?.[0]).toBe("BEGIN");
    expect(client.query.mock.calls.at(-1)?.[0]).toBe("ROLLBACK");
    expect(client.release).toHaveBeenCalledTimes(1);
  });
});
