import { describe, expect, it, vi } from "vitest";

import {
  replaceHoustonCandidateFinanceSnapshot,
  upsertHoustonFinanceLink,
} from "../../../src/pipeline/houstonFinance/houstonFinanceWriter.js";

const CANDIDATE_ID = "11111111-1111-1111-1111-111111111111";
const ELECTION_ID = "22222222-2222-2222-2222-222222222222";
const LINK_ID = "33333333-3333-3333-3333-333333333333";

function createMockDb() {
  return {
    query: vi.fn().mockResolvedValue({ rows: [{ id: LINK_ID }], rowCount: 1 }),
  };
}

function baseLink() {
  return {
    candidateId: CANDIDATE_ID,
    electionId: ELECTION_ID,
    electionYear: 2026,
    candidateNameNormalized: "GREG ABBOTT",
    officeName: "Governor",
    committeeId: "00012345",
    committeeName: "ABBOTT, GREG",
    linkSource: "tec_bulk" as const,
    sourceUrl: "https://prd.tecprd.ethicsefile.com/public/cf/public/TEC_CF_CSV.zip",
    lastVerifiedAt: new Date("2026-01-01T00:00:00.000Z"),
  };
}

describe("houstonFinanceWriter", () => {
  it("upserts Houston finance links and returns the link id", async () => {
    const db = createMockDb();

    await expect(upsertHoustonFinanceLink({ db, link: baseLink() })).resolves.toEqual({ linkId: LINK_ID });

    expect(db.query).toHaveBeenCalledTimes(1);
    expect(String(db.query.mock.calls[0]?.[0])).toContain("INSERT INTO public.hou_candidate_finance_links");
    expect(String(db.query.mock.calls[0]?.[0])).toContain("ON CONFLICT (candidate_id, election_id, committee_id)");
    expect(String(db.query.mock.calls[0]?.[0])).toContain("RETURNING id");
    expect(db.query.mock.calls[0]?.[1]).toEqual([
      CANDIDATE_ID,
      ELECTION_ID,
      2026,
      "GREG ABBOTT",
      "Governor",
      null,
      "00012345",
      "ABBOTT, GREG",
      "active",
      "tec_bulk",
      "https://prd.tecprd.ethicsefile.com/public/cf/public/TEC_CF_CSV.zip",
      "2026-01-01T00:00:00.000Z",
    ]);
  });

  it("replaces a Houston finance snapshot inside a transaction", async () => {
    const client = {
      query: vi.fn().mockResolvedValue({ rows: [{ id: LINK_ID }], rowCount: 1 }),
      release: vi.fn(),
    };
    const db = {
      query: vi.fn(),
      connect: vi.fn().mockResolvedValue(client),
    };

    const result = await replaceHoustonCandidateFinanceSnapshot({
      db,
      link: baseLink(),
      syncedAt: new Date("2026-02-03T04:05:06.000Z"),
      summary: {
        totalReceipts: 5350,
        directContributionTotal: 5350,
        outsideSupportTotal: 100000.25,
        outsideOpposeTotal: 5000,
        sourceUrl: "https://prd.tecprd.ethicsefile.com/public/cf/public/TEC_CF_CSV.zip",
      },
      directBreakdowns: [
        {
          categoryType: "occupation",
          categoryName: "Attorney",
          amount: 350,
          contributorCount: 2,
          sourceUrl: "https://prd.tecprd.ethicsefile.com/public/cf/public/TEC_CF_CSV.zip",
        },
        {
          categoryType: "contribution_size",
          categoryName: "$250-$499",
          amount: 250,
          contributorCount: 1,
          sourceUrl: "https://prd.tecprd.ethicsefile.com/public/cf/public/TEC_CF_CSV.zip",
        },
      ],
      outsideGroups: [
        {
          committeeId: "7001",
          committeeName: "Texans for Example",
          supportOppose: "support",
          amount: 100000.25,
          sourceUrl: "https://prd.tecprd.ethicsefile.com/public/cf/public/TEC_CF_CSV.zip",
        },
        {
          committeeId: "7002",
          committeeName: "Houston Accountability PAC",
          supportOppose: "oppose",
          amount: 5000,
          sourceUrl: "https://prd.tecprd.ethicsefile.com/public/cf/public/TEC_CF_CSV.zip",
        },
      ],
      outsideGroupBreakdowns: [
        {
          committeeId: "7001",
          supportOppose: "support",
          categoryType: "donor",
          categoryName: "Energy Transfer LLC",
          amount: 35000,
          contributorCount: 1,
          sourceUrl: "https://prd.tecprd.ethicsefile.com/public/cf/public/TEC_CF_CSV.zip",
        },
        {
          committeeId: "7001",
          supportOppose: "support",
          categoryType: "industry",
          categoryName: "oil_gas_energy",
          amount: 35000,
          contributorCount: 1,
          sourceUrl: "https://prd.tecprd.ethicsefile.com/public/cf/public/TEC_CF_CSV.zip",
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
    expect(sql.some((statement) => statement.includes("INSERT INTO public.hou_candidate_finance_summaries"))).toBe(true);
    const summaryCall = client.query.mock.calls.find((call) =>
      String(call[0]).includes("INSERT INTO public.hou_candidate_finance_summaries")
    );
    expect(String(summaryCall?.[0])).toContain("outside_support_total");
    expect(String(summaryCall?.[0])).toContain("COALESCE(EXCLUDED.total_receipts");
    expect(summaryCall?.[1]).toEqual([
      LINK_ID,
      2026,
      5350,
      5350,
      null,
      null,
      100000.25,
      5000,
      "https://prd.tecprd.ethicsefile.com/public/cf/public/TEC_CF_CSV.zip",
      "2026-02-03T04:05:06.000Z",
    ]);
    expect(sql.filter((statement) => statement.includes("INSERT INTO public.hou_candidate_finance_direct_breakdowns"))).toHaveLength(2);
    expect(sql.filter((statement) => statement.includes("INSERT INTO public.hou_candidate_finance_outside_groups"))).toHaveLength(2);
    expect(sql.filter((statement) => statement.includes("INSERT INTO public.hou_candidate_finance_outside_group_breakdowns"))).toHaveLength(2);
    expect(sql.some((statement) => statement.includes("DELETE FROM public.hou_candidate_finance_direct_breakdowns"))).toBe(true);
    expect(sql.some((statement) => statement.includes("DELETE FROM public.hou_candidate_finance_outside_groups"))).toBe(true);
    expect(sql.some((statement) => statement.includes("DELETE FROM public.hou_candidate_finance_outside_group_breakdowns"))).toBe(true);
  });

  it("wraps a supplied queryable in a transaction", async () => {
    const db = createMockDb();

    const result = await replaceHoustonCandidateFinanceSnapshot({
      db,
      link: baseLink(),
      syncedAt: new Date("2026-02-03T04:05:06.000Z"),
      summary: {
        totalReceipts: 1000,
      },
    });

    expect(result.summaryWritten).toBe(true);
    expect(db.query.mock.calls[0]?.[0]).toBe("BEGIN");
    expect(db.query.mock.calls.at(-1)?.[0]).toBe("COMMIT");
  });

  it("rejects a supplied PoolClient so it cannot commit an outer transaction", async () => {
    const client = {
      query: vi.fn().mockResolvedValue({ rows: [{ id: LINK_ID }], rowCount: 1 }),
      release: vi.fn(),
    };

    await expect(
      replaceHoustonCandidateFinanceSnapshot({
        db: client,
        link: baseLink(),
        syncedAt: new Date("2026-02-03T04:05:06.000Z"),
        summary: {
          totalReceipts: 1000,
        },
      })
    ).rejects.toThrow("Houston finance snapshot writes must receive a Pool, not a PoolClient");
    expect(client.query).not.toHaveBeenCalled();
    expect(client.release).not.toHaveBeenCalled();
  });

  it("does not delete omitted breakdown groups", async () => {
    const db = createMockDb();

    const result = await replaceHoustonCandidateFinanceSnapshot({
      db,
      link: baseLink(),
      syncedAt: new Date("2026-02-03T04:05:06.000Z"),
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
    const summarySql = sql.find((statement) => statement.includes("INSERT INTO public.hou_candidate_finance_summaries"));
    expect(summarySql).toContain("outside_support_total = COALESCE(EXCLUDED.outside_support_total");
    expect(sql.some((statement) => statement.includes("DELETE FROM public.hou_candidate_finance_direct_breakdowns"))).toBe(false);
    expect(sql.some((statement) => statement.includes("DELETE FROM public.hou_candidate_finance_outside_groups"))).toBe(false);
    expect(sql.some((statement) => statement.includes("DELETE FROM public.hou_candidate_finance_outside_group_breakdowns"))).toBe(false);
  });

  it("uses current snapshot keys when cleaning repeated writes with the same timestamp", async () => {
    const db = createMockDb();
    const syncedAt = new Date("2026-02-03T04:05:06.000Z");

    await replaceHoustonCandidateFinanceSnapshot({
      db,
      link: baseLink(),
      syncedAt,
      directBreakdowns: [
        {
          categoryType: "occupation",
          categoryName: "Attorney",
          amount: 700,
        },
      ],
      outsideGroups: [
        {
          committeeId: "7001",
          committeeName: "Texans for Example",
          supportOppose: "support",
          amount: 1000,
        },
      ],
      outsideGroupBreakdowns: [
        {
          committeeId: "7001",
          supportOppose: "support",
          categoryType: "donor",
          categoryName: "Energy Transfer LLC",
          amount: 1000,
        },
      ],
    });
    await replaceHoustonCandidateFinanceSnapshot({
      db,
      link: baseLink(),
      syncedAt,
      directBreakdowns: [
        {
          categoryType: "occupation",
          categoryName: "Teacher",
          amount: 500,
        },
      ],
      outsideGroups: [
        {
          committeeId: "7002",
          committeeName: "Houston Accountability PAC",
          supportOppose: "oppose",
          amount: 2000,
        },
      ],
      outsideGroupBreakdowns: [
        {
          committeeId: "7002",
          supportOppose: "oppose",
          categoryType: "industry",
          categoryName: "oil_gas_energy",
          amount: 2000,
        },
      ],
    });

    const directDeleteCalls = db.query.mock.calls.filter((call) =>
      String(call[0]).includes("DELETE FROM public.hou_candidate_finance_direct_breakdowns")
    );
    expect(directDeleteCalls.at(-1)?.[1]).toEqual([
      LINK_ID,
      2026,
      JSON.stringify([{ category_type: "occupation", category_name: "Teacher" }]),
    ]);

    const groupDeleteCalls = db.query.mock.calls.filter((call) =>
      String(call[0]).includes("DELETE FROM public.hou_candidate_finance_outside_groups")
    );
    expect(groupDeleteCalls.at(-1)?.[1]).toEqual([
      LINK_ID,
      2026,
      JSON.stringify([{ committee_id: "7002", support_oppose: "oppose" }]),
    ]);

    const outsideBreakdownDeleteCalls = db.query.mock.calls.filter((call) =>
      String(call[0]).includes("DELETE FROM public.hou_candidate_finance_outside_group_breakdowns")
    );
    expect(outsideBreakdownDeleteCalls.at(-1)?.[1]).toEqual([
      LINK_ID,
      2026,
      JSON.stringify([
        {
          committee_id: "7002",
          support_oppose: "oppose",
          category_type: "industry",
          category_name: "oil_gas_energy",
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
      replaceHoustonCandidateFinanceSnapshot({
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
      replaceHoustonCandidateFinanceSnapshot({
        db,
        link: {
          ...baseLink(),
          committeeId: " ",
        },
      })
    ).rejects.toThrow("Houston committee id is required");

    expect(db.query).not.toHaveBeenCalled();
  });

  it("rejects invalid election years", async () => {
    const db = createMockDb();

    await expect(
      upsertHoustonFinanceLink({
        db,
        link: {
          ...baseLink(),
          electionYear: 2013,
        },
      })
    ).rejects.toThrow("Invalid Houston finance election year");

    expect(db.query).not.toHaveBeenCalled();
  });
});
