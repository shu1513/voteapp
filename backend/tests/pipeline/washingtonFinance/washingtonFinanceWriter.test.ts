import { describe, expect, it, vi } from "vitest";

import {
  replaceWashingtonCandidateFinanceSnapshot,
  upsertWashingtonFinanceLink,
} from "../../../src/pipeline/washingtonFinance/washingtonFinanceWriter.js";

const CANDIDATE_ID = "11111111-1111-1111-1111-111111111111";
const ELECTION_ID = "22222222-2222-2222-2222-222222222222";
const LINK_ID = "33333333-3333-3333-3333-333333333333";

function createMockDb() {
  const query = vi.fn().mockResolvedValue({ rows: [{ id: LINK_ID }], rowCount: 1 });
  const client = {
    query,
    release: vi.fn(),
  };
  return {
    query,
    connect: vi.fn().mockResolvedValue(client),
    client,
  };
}

function baseLink() {
  return {
    candidateId: CANDIDATE_ID,
    electionId: ELECTION_ID,
    electionYear: 2024,
    candidateNameNormalized: "BOB FERGUSON",
    officeName: "Governor",
    filerId: "FERGR *115",
    committeeId: "32311",
    committeeName: "Robert W. Ferguson (Bob Ferguson)",
    candidacyId: "689556",
    linkSource: "pdc_api" as const,
    sourceUrl: "https://data.wa.gov/resource/3h9x-7bvm.json",
    lastVerifiedAt: new Date("2024-01-01T00:00:00.000Z"),
  };
}

describe("washingtonFinanceWriter", () => {
  it("upserts Washington finance links and returns the link id", async () => {
    const db = createMockDb();

    await expect(upsertWashingtonFinanceLink({ db, link: baseLink() })).resolves.toEqual({ linkId: LINK_ID });

    expect(db.query).toHaveBeenCalledTimes(1);
    expect(String(db.query.mock.calls[0]?.[0])).toContain("INSERT INTO public.wa_candidate_finance_links");
    expect(String(db.query.mock.calls[0]?.[0])).toContain("ON CONFLICT (candidate_id, election_id, filer_id)");
    expect(String(db.query.mock.calls[0]?.[0])).toContain("RETURNING id");
    expect(db.query.mock.calls[0]?.[1]).toEqual([
      CANDIDATE_ID,
      ELECTION_ID,
      2024,
      "BOB FERGUSON",
      "Governor",
      null,
      "FERGR *115",
      "32311",
      "Robert W. Ferguson (Bob Ferguson)",
      "689556",
      "active",
      "pdc_api",
      "https://data.wa.gov/resource/3h9x-7bvm.json",
      "2024-01-01T00:00:00.000Z",
    ]);
  });

  it("replaces a Washington finance snapshot inside a transaction", async () => {
    const client = {
      query: vi.fn().mockResolvedValue({ rows: [{ id: LINK_ID }], rowCount: 1 }),
      release: vi.fn(),
    };
    const db = {
      query: vi.fn(),
      connect: vi.fn().mockResolvedValue(client),
    };

    const result = await replaceWashingtonCandidateFinanceSnapshot({
      db,
      link: baseLink(),
      syncedAt: new Date("2024-02-03T04:05:06.000Z"),
      summary: {
        totalReceipts: 11962407.92,
        directContributionTotal: 11962407.92,
        totalDisbursements: 1000,
        cashOnHand: 500,
        outsideSupportTotal: 2457.26,
        outsideOpposeTotal: 10000,
        sourceUrl: "https://data.wa.gov/resource/kv7h-kjye.json",
      },
      directBreakdowns: [
        {
          categoryType: "occupation",
          categoryName: "ATTORNEY - LAWYER",
          amount: 719187.76,
          contributorCount: 1200,
          sourceUrl: "https://data.wa.gov/resource/kv7h-kjye.json",
        },
        {
          categoryType: "contribution_size",
          categoryName: "$1,000+",
          amount: 100000,
          contributorCount: 20,
          sourceUrl: "https://data.wa.gov/resource/kv7h-kjye.json",
        },
      ],
      outsideGroups: [
        {
          sponsorId: "FUSEV  147",
          sponsorName: "FUSE VOTES",
          supportOppose: "support",
          amount: 2457.26,
          sourceUrl: "https://data.wa.gov/resource/67cp-h962.json",
        },
        {
          sponsorId: "WASH24 397",
          sponsorName: "WASHINGTON 24",
          supportOppose: "oppose",
          amount: 10000,
          sourceUrl: "https://data.wa.gov/resource/67cp-h962.json",
        },
      ],
      outsideGroupBreakdowns: [
        {
          sponsorId: "FUSEV  147",
          supportOppose: "support",
          categoryType: "donor",
          categoryName: "Washington Conservation Action Votes",
          amount: 20000,
          contributorCount: 1,
          sourceUrl: "https://data.wa.gov/resource/kv7h-kjye.json",
        },
        {
          sponsorId: "FUSEV  147",
          supportOppose: "support",
          categoryType: "industry",
          categoryName: "environmental_group",
          amount: 20000,
          contributorCount: 1,
          sourceUrl: "https://data.wa.gov/resource/kv7h-kjye.json",
        },
      ],
    });

    expect(result).toEqual({
      linkId: LINK_ID,
      summaryWritten: true,
      directBreakdownsWritten: 2,
      outsideGroupsWritten: 2,
      outsideGroupBreakdownsWritten: 2,
    });
    expect(db.connect).toHaveBeenCalledTimes(1);
    expect(db.query).not.toHaveBeenCalled();
    expect(client.query.mock.calls[0]?.[0]).toBe("BEGIN");
    expect(client.query.mock.calls.at(-1)?.[0]).toBe("COMMIT");
    expect(client.release).toHaveBeenCalledTimes(1);

    const sql = client.query.mock.calls.map((call) => String(call[0]));
    expect(sql.some((statement) => statement.includes("INSERT INTO public.wa_candidate_finance_summaries"))).toBe(true);
    const summaryCall = client.query.mock.calls.find((call) =>
      String(call[0]).includes("INSERT INTO public.wa_candidate_finance_summaries")
    );
    expect(String(summaryCall?.[0])).toContain("outside_support_total");
    expect(String(summaryCall?.[0])).toContain("total_receipts = EXCLUDED.total_receipts");
    expect(String(summaryCall?.[0])).not.toContain("COALESCE(");
    expect(summaryCall?.[1]).toEqual([
      LINK_ID,
      2024,
      11962407.92,
      11962407.92,
      1000,
      500,
      2457.26,
      10000,
      "https://data.wa.gov/resource/kv7h-kjye.json",
      "2024-02-03T04:05:06.000Z",
    ]);
    expect(sql.filter((statement) => statement.includes("INSERT INTO public.wa_candidate_finance_direct_breakdowns"))).toHaveLength(2);
    expect(sql.filter((statement) => statement.includes("INSERT INTO public.wa_candidate_finance_outside_groups"))).toHaveLength(2);
    expect(sql.filter((statement) => statement.includes("INSERT INTO public.wa_candidate_finance_outside_group_breakdowns"))).toHaveLength(2);
    expect(sql.some((statement) => statement.includes("DELETE FROM public.wa_candidate_finance_direct_breakdowns"))).toBe(true);
    expect(sql.some((statement) => statement.includes("DELETE FROM public.wa_candidate_finance_outside_groups"))).toBe(true);
    expect(sql.some((statement) => statement.includes("DELETE FROM public.wa_candidate_finance_outside_group_breakdowns"))).toBe(true);
  });

  it("rejects a supplied query-only wrapper so snapshot writes stay atomic", async () => {
    const db = {
      query: vi.fn().mockResolvedValue({ rows: [{ id: LINK_ID }], rowCount: 1 }),
    };

    await expect(
      replaceWashingtonCandidateFinanceSnapshot({
        db: db as never,
        link: baseLink(),
        syncedAt: new Date("2024-02-03T04:05:06.000Z"),
        summary: {
          totalReceipts: 1000,
        },
      })
    ).rejects.toThrow("Washington finance snapshot writes must receive a Pool");
    expect(db.query).not.toHaveBeenCalled();
  });

  it("uses a supplied Pool to open a transaction", async () => {
    const db = createMockDb();

    const result = await replaceWashingtonCandidateFinanceSnapshot({
      db,
      link: baseLink(),
      syncedAt: new Date("2024-02-03T04:05:06.000Z"),
      summary: {
        totalReceipts: 1000,
      },
    });

    expect(result.summaryWritten).toBe(true);
    expect(db.connect).toHaveBeenCalledTimes(1);
    expect(db.query.mock.calls[0]?.[0]).toBe("BEGIN");
    expect(db.query.mock.calls.at(-1)?.[0]).toBe("COMMIT");
    expect(db.client.release).toHaveBeenCalledTimes(1);
  });

  it("rejects a supplied PoolClient so it cannot commit an outer transaction", async () => {
    const client = {
      query: vi.fn().mockResolvedValue({ rows: [{ id: LINK_ID }], rowCount: 1 }),
      release: vi.fn(),
    };

    await expect(
      replaceWashingtonCandidateFinanceSnapshot({
        db: client as never,
        link: baseLink(),
        syncedAt: new Date("2024-02-03T04:05:06.000Z"),
        summary: {
          totalReceipts: 1000,
        },
      })
    ).rejects.toThrow("Washington finance snapshot writes must receive a Pool, not a PoolClient");
    expect(client.query).not.toHaveBeenCalled();
    expect(client.release).not.toHaveBeenCalled();
  });

  it("does not delete omitted breakdown groups", async () => {
    const db = createMockDb();

    const result = await replaceWashingtonCandidateFinanceSnapshot({
      db,
      link: baseLink(),
      syncedAt: new Date("2024-02-03T04:05:06.000Z"),
      summary: {
        outsideSupportTotal: 0,
      },
    });

    expect(result).toEqual({
      linkId: LINK_ID,
      summaryWritten: true,
      directBreakdownsWritten: 0,
      outsideGroupsWritten: 0,
      outsideGroupBreakdownsWritten: 0,
    });
    const sql = db.query.mock.calls.map((call) => String(call[0]));
    const summarySql = sql.find((statement) => statement.includes("INSERT INTO public.wa_candidate_finance_summaries"));
    expect(summarySql).toContain("outside_support_total = EXCLUDED.outside_support_total");
    expect(summarySql).not.toContain("COALESCE(");
    expect(sql.some((statement) => statement.includes("DELETE FROM public.wa_candidate_finance_direct_breakdowns"))).toBe(false);
    expect(sql.some((statement) => statement.includes("DELETE FROM public.wa_candidate_finance_outside_groups"))).toBe(false);
    expect(sql.some((statement) => statement.includes("DELETE FROM public.wa_candidate_finance_outside_group_breakdowns"))).toBe(false);
  });

  it("requires outside groups when writing outside group breakdowns", async () => {
    const db = createMockDb();

    await expect(
      replaceWashingtonCandidateFinanceSnapshot({
        db,
        link: baseLink(),
        outsideGroupBreakdowns: [
          {
            sponsorId: "FUSEV  147",
            supportOppose: "support",
            categoryType: "donor",
            categoryName: "Washington Conservation Action Votes",
            amount: 1000,
          },
        ],
      })
    ).rejects.toThrow("Washington outside group breakdowns require outside groups in the same snapshot");
    expect(db.connect).not.toHaveBeenCalled();
  });

  it("uses current snapshot keys when cleaning repeated writes with the same timestamp", async () => {
    const db = createMockDb();
    const syncedAt = new Date("2024-02-03T04:05:06.000Z");

    await replaceWashingtonCandidateFinanceSnapshot({
      db,
      link: baseLink(),
      syncedAt,
      directBreakdowns: [
        {
          categoryType: "occupation",
          categoryName: "ATTORNEY - LAWYER",
          amount: 700,
        },
      ],
      outsideGroups: [
        {
          sponsorId: "FUSEV  147",
          sponsorName: "FUSE VOTES",
          supportOppose: "support",
          amount: 1000,
        },
      ],
      outsideGroupBreakdowns: [
        {
          sponsorId: "FUSEV  147",
          supportOppose: "support",
          categoryType: "donor",
          categoryName: "Washington Conservation Action Votes",
          amount: 1000,
        },
      ],
    });
    await replaceWashingtonCandidateFinanceSnapshot({
      db,
      link: baseLink(),
      syncedAt,
      directBreakdowns: [
        {
          categoryType: "occupation",
          categoryName: "CONSULTANT",
          amount: 500,
        },
      ],
      outsideGroups: [
        {
          sponsorId: "WASH24 397",
          sponsorName: "WASHINGTON 24",
          supportOppose: "oppose",
          amount: 2000,
        },
      ],
      outsideGroupBreakdowns: [
        {
          sponsorId: "WASH24 397",
          supportOppose: "oppose",
          categoryType: "industry",
          categoryName: "labor_unions",
          amount: 2000,
        },
      ],
    });

    const directDeleteCalls = db.query.mock.calls.filter((call) =>
      String(call[0]).includes("DELETE FROM public.wa_candidate_finance_direct_breakdowns")
    );
    expect(directDeleteCalls.at(-1)?.[1]).toEqual([
      LINK_ID,
      2024,
      JSON.stringify([{ category_type: "occupation", category_name: "CONSULTANT" }]),
    ]);

    const groupDeleteCalls = db.query.mock.calls.filter((call) =>
      String(call[0]).includes("DELETE FROM public.wa_candidate_finance_outside_groups")
    );
    expect(groupDeleteCalls.at(-1)?.[1]).toEqual([
      LINK_ID,
      2024,
      JSON.stringify([{ sponsor_id: "WASH24 397", support_oppose: "oppose" }]),
    ]);

    const outsideBreakdownDeleteCalls = db.query.mock.calls.filter((call) =>
      String(call[0]).includes("DELETE FROM public.wa_candidate_finance_outside_group_breakdowns")
    );
    expect(outsideBreakdownDeleteCalls.at(-1)?.[1]).toEqual([
      LINK_ID,
      2024,
      JSON.stringify([
        {
          sponsor_id: "WASH24 397",
          support_oppose: "oppose",
          category_type: "industry",
          category_name: "labor_unions",
        },
      ]),
    ]);
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
      replaceWashingtonCandidateFinanceSnapshot({
        db,
        link: baseLink(),
        summary: { totalReceipts: 1000 },
      })
    ).rejects.toThrow("write failed");

    expect(client.query.mock.calls[0]?.[0]).toBe("BEGIN");
    expect(client.query.mock.calls.at(-1)?.[0]).toBe("ROLLBACK");
    expect(client.release).toHaveBeenCalledTimes(1);
  });

  it("validates core inputs before writing", async () => {
    const db = createMockDb();

    await expect(
      replaceWashingtonCandidateFinanceSnapshot({
        db,
        link: {
          ...baseLink(),
          filerId: " ",
        },
      })
    ).rejects.toThrow("Washington filer id is required");

    expect(db.query).not.toHaveBeenCalled();
  });

  it("rejects invalid election years", async () => {
    const db = createMockDb();

    await expect(
      upsertWashingtonFinanceLink({
        db,
        link: {
          ...baseLink(),
          electionYear: 1999,
        },
      })
    ).rejects.toThrow("Invalid Washington finance election year");

    expect(db.query).not.toHaveBeenCalled();
  });
});
