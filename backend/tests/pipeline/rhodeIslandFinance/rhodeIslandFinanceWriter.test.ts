import { describe, expect, it, vi } from "vitest";

import {
  replaceRhodeIslandCandidateFinanceSnapshot,
  upsertRhodeIslandFinanceLink,
} from "../../../src/pipeline/rhodeIslandFinance/rhodeIslandFinanceWriter.js";

const CANDIDATE_ID = "11111111-1111-1111-1111-111111111111";
const ELECTION_ID = "22222222-2222-2222-2222-222222222222";
const LINK_ID = "33333333-3333-3333-3333-333333333333";
const SOURCE_URL = "https://www.ricampaignfinance.com/RIPublic/Homepage.aspx";

function createMockDb() {
  return {
    query: vi.fn().mockResolvedValue({ rows: [{ id: LINK_ID }], rowCount: 1 }),
  };
}

function createMockPool() {
  const client = {
    query: vi.fn().mockResolvedValue({ rows: [{ id: LINK_ID }], rowCount: 1 }),
    release: vi.fn(),
  };
  return {
    client,
    db: {
      query: vi.fn(),
      connect: vi.fn().mockResolvedValue(client),
    },
  };
}

function baseLink() {
  return {
    candidateId: CANDIDATE_ID,
    electionId: ELECTION_ID,
    electionYear: 2026,
    candidateNameNormalized: "JANE EXAMPLE",
    officeName: "Governor",
    committeeId: "2235",
    committeeName: "Friends of Jane Example",
    linkSource: "erts_portal" as const,
    sourceUrl: SOURCE_URL,
    lastVerifiedAt: new Date("2026-01-01T00:00:00.000Z"),
  };
}

describe("rhodeIslandFinanceWriter", () => {
  it("upserts Rhode Island finance links and returns the link id", async () => {
    const db = createMockDb();

    await expect(upsertRhodeIslandFinanceLink({ db, link: baseLink() })).resolves.toEqual({ linkId: LINK_ID });

    expect(db.query).toHaveBeenCalledTimes(1);
    expect(String(db.query.mock.calls[0]?.[0])).toContain("INSERT INTO public.ri_candidate_finance_links");
    expect(String(db.query.mock.calls[0]?.[0])).toContain("ON CONFLICT (candidate_id, election_id, committee_id)");
    expect(String(db.query.mock.calls[0]?.[0])).toContain("RETURNING id");
    expect(db.query.mock.calls[0]?.[1]).toEqual([
      CANDIDATE_ID,
      ELECTION_ID,
      2026,
      "JANE EXAMPLE",
      "Governor",
      null,
      "2235",
      "Friends of Jane Example",
      "active",
      "erts_portal",
      SOURCE_URL,
      "2026-01-01T00:00:00.000Z",
    ]);
  });

  it("trims ERTS organization keys before writing", async () => {
    const db = createMockDb();

    await upsertRhodeIslandFinanceLink({
      db,
      link: { ...baseLink(), committeeId: " 2235 " },
    });

    expect(db.query.mock.calls[0]?.[1]).toContain("2235");
  });

  it("rejects non-numeric ERTS organization keys before writing", async () => {
    const db = createMockDb();

    await expect(
      upsertRhodeIslandFinanceLink({
        db,
        link: { ...baseLink(), committeeId: "MCKEE-2235" },
      })
    ).rejects.toThrow("Invalid Rhode Island ERTS organization key: MCKEE-2235");

    expect(db.query).not.toHaveBeenCalled();
  });

  it("rejects empty committee ids before writing", async () => {
    const db = createMockDb();

    await expect(
      upsertRhodeIslandFinanceLink({
        db,
        link: { ...baseLink(), committeeId: "   " },
      })
    ).rejects.toThrow(/organization key/i);

    expect(db.query).not.toHaveBeenCalled();
  });

  it("replaces a Rhode Island finance snapshot inside a transaction", async () => {
    const { db, client } = createMockPool();

    const result = await replaceRhodeIslandCandidateFinanceSnapshot({
      db,
      link: baseLink(),
      syncedAt: new Date("2026-02-03T04:05:06.000Z"),
      summary: {
        totalReceipts: 1614060.79,
        directContributionTotal: 253828.24,
        totalDisbursements: 945434.57,
        cashOnHand: 668626.22,
        outsideSupportTotal: 12000,
        outsideOpposeTotal: 5000,
        sourceUrl: SOURCE_URL,
      },
      directBreakdowns: [
        {
          categoryType: "contribution_size",
          categoryName: "$1,000-$4,999",
          amount: 42000,
          contributorCount: 30,
          sourceUrl: SOURCE_URL,
        },
        {
          categoryType: "contribution_size",
          categoryName: "$100-$249",
          amount: 750,
          contributorCount: 5,
          sourceUrl: SOURCE_URL,
        },
      ],
      outsideGroups: [
        {
          committeeId: "9001",
          committeeName: "Ocean State Example IEPAC",
          supportOppose: "support",
          amount: 12000,
          sourceUrl: SOURCE_URL,
        },
        {
          committeeId: "9002",
          committeeName: "Narragansett Accountability Example Fund",
          supportOppose: "oppose",
          amount: 5000,
          sourceUrl: SOURCE_URL,
        },
      ],
      outsideGroupBreakdowns: [
        {
          committeeId: "9001",
          supportOppose: "support",
          categoryType: "donor",
          categoryName: "Harbor Light Fund",
          amount: 4000,
          contributorCount: 1,
          sourceUrl: SOURCE_URL,
        },
      ],
    });

    expect(result).toEqual({
      linkId: LINK_ID,
      summaryWritten: true,
      directBreakdownsWritten: 2,
      outsideGroupsWritten: 2,
      outsideGroupBreakdownsWritten: 1,
    });
    expect(db.connect).toHaveBeenCalledTimes(1);
    expect(db.query).not.toHaveBeenCalled();
    expect(client.query.mock.calls[0]?.[0]).toBe("BEGIN");
    expect(client.query.mock.calls.at(-1)?.[0]).toBe("COMMIT");
    expect(client.release).toHaveBeenCalledTimes(1);

    const sql = client.query.mock.calls.map((call) => String(call[0]));
    expect(sql.filter((statement) => statement.includes("INSERT INTO public.ri_candidate_finance_summaries"))).toHaveLength(1);
    expect(sql.filter((statement) => statement.includes("INSERT INTO public.ri_candidate_finance_direct_breakdowns"))).toHaveLength(2);
    expect(sql.filter((statement) => statement.includes("INSERT INTO public.ri_candidate_finance_outside_groups"))).toHaveLength(2);
    expect(
      sql.filter((statement) => statement.includes("INSERT INTO public.ri_candidate_finance_outside_group_breakdowns"))
    ).toHaveLength(1);
    expect(sql.some((statement) => statement.includes("DELETE FROM public.ri_candidate_finance_direct_breakdowns"))).toBe(true);
    expect(sql.some((statement) => statement.includes("DELETE FROM public.ri_candidate_finance_outside_groups"))).toBe(true);
    expect(sql.some((statement) => statement.includes("DELETE FROM public.ri_candidate_finance_outside_group_breakdowns"))).toBe(
      true
    );

    const summaryCall = client.query.mock.calls.find((call) =>
      String(call[0]).includes("INSERT INTO public.ri_candidate_finance_summaries")
    );
    expect(String(summaryCall?.[0])).toContain("total_receipts = EXCLUDED.total_receipts");
    expect(String(summaryCall?.[0])).toContain("direct_contribution_total = EXCLUDED.direct_contribution_total");
    expect(String(summaryCall?.[0])).toContain("cash_on_hand = EXCLUDED.cash_on_hand");
    expect(String(summaryCall?.[0])).toContain("outside_support_total = COALESCE(EXCLUDED.outside_support_total");
    expect(String(summaryCall?.[0])).toContain("outside_oppose_total = COALESCE(EXCLUDED.outside_oppose_total");
    expect(summaryCall?.[1]).toEqual([
      LINK_ID,
      2026,
      1614060.79,
      253828.24,
      945434.57,
      668626.22,
      12000,
      5000,
      SOURCE_URL,
      "2026-02-03T04:05:06.000Z",
    ]);
  });

  it("accepts a negative cash on hand as a signed balance", async () => {
    const { db, client } = createMockPool();

    // RI CF-2s carry liabilities; an indebted campaign's official ending
    // balance is negative and must be written as-is, never as NULL
    // (rhode_island_plan.md, migration 236 relaxed amounts check).
    await replaceRhodeIslandCandidateFinanceSnapshot({
      db,
      link: baseLink(),
      syncedAt: new Date("2026-02-03T04:05:06.000Z"),
      summary: { totalReceipts: 1000, cashOnHand: -21922.88 },
    });

    const summaryCall = client.query.mock.calls.find((call) =>
      String(call[0]).includes("INSERT INTO public.ri_candidate_finance_summaries")
    );
    expect(summaryCall?.[1]).toContain(-21922.88);
  });

  it("preserves prior outside totals when a direct-only refresh has no CF-8 data", async () => {
    const { db, client } = createMockPool();

    await replaceRhodeIslandCandidateFinanceSnapshot({
      db,
      link: baseLink(),
      syncedAt: new Date("2026-02-03T04:05:06.000Z"),
      summary: {
        totalReceipts: 1614060.79,
        outsideSupportTotal: null,
        outsideOpposeTotal: null,
      },
    });

    const summaryCall = client.query.mock.calls.find((call) =>
      String(call[0]).includes("INSERT INTO public.ri_candidate_finance_summaries")
    );
    expect(String(summaryCall?.[0])).toContain(
      "outside_support_total = COALESCE(EXCLUDED.outside_support_total, ri_candidate_finance_summaries.outside_support_total)"
    );
    expect(String(summaryCall?.[0])).toContain("total_disbursements = EXCLUDED.total_disbursements");
  });

  it("deactivates other active erts_portal links for the same candidate election", async () => {
    const { db, client } = createMockPool();

    await replaceRhodeIslandCandidateFinanceSnapshot({
      db,
      link: baseLink(),
      syncedAt: new Date("2026-02-03T04:05:06.000Z"),
      summary: { totalReceipts: 1000 },
    });

    const deactivationCall = client.query.mock.calls.find((call) =>
      String(call[0]).includes("UPDATE public.ri_candidate_finance_links")
    );
    expect(String(deactivationCall?.[0])).toContain("link_source = 'erts_portal'");
    expect(deactivationCall?.[1]).toEqual([CANDIDATE_ID, ELECTION_ID, LINK_ID]);
  });

  it("does not deactivate erts_portal links when the incoming link is manual", async () => {
    const { db, client } = createMockPool();

    await replaceRhodeIslandCandidateFinanceSnapshot({
      db,
      link: { ...baseLink(), linkSource: "manual" as const },
      syncedAt: new Date("2026-02-03T04:05:06.000Z"),
      summary: { totalReceipts: 1000 },
    });

    const sql = client.query.mock.calls.map((call) => String(call[0]));
    expect(sql.some((statement) => statement.includes("UPDATE public.ri_candidate_finance_links"))).toBe(false);
  });

  it("requires each outside group breakdown to pair with an outside group in the snapshot", async () => {
    const { db, client } = createMockPool();

    await expect(
      replaceRhodeIslandCandidateFinanceSnapshot({
        db,
        link: baseLink(),
        outsideGroups: [
          {
            committeeId: "9001",
            committeeName: "Ocean State Example IEPAC",
            supportOppose: "support",
            amount: 1000,
          },
        ],
        outsideGroupBreakdowns: [
          {
            committeeId: "9001",
            supportOppose: "oppose",
            categoryType: "donor",
            categoryName: "Harbor Light Fund",
            amount: 1000,
          },
        ],
      })
    ).rejects.toThrow(/outside group/i);
    expect(client.query).not.toHaveBeenCalled();
  });

  it("rejects a supplied PoolClient so it cannot commit an outer transaction", async () => {
    const client = {
      query: vi.fn().mockResolvedValue({ rows: [{ id: LINK_ID }], rowCount: 1 }),
      release: vi.fn(),
    };

    await expect(
      replaceRhodeIslandCandidateFinanceSnapshot({
        db: client as never,
        link: baseLink(),
        summary: { totalReceipts: 1000 },
      })
    ).rejects.toThrow("Rhode Island finance snapshot writes must receive a Pool, not a PoolClient");
    expect(client.query).not.toHaveBeenCalled();
    expect(client.release).not.toHaveBeenCalled();
  });

  it("rolls back and releases the client when a transactional write fails", async () => {
    const client = {
      query: vi
        .fn()
        .mockResolvedValueOnce({ rows: [], rowCount: 0 })
        .mockRejectedValueOnce(new Error("write failed")),
      release: vi.fn(),
    };
    const db = {
      query: vi.fn(),
      connect: vi.fn().mockResolvedValue(client),
    };

    await expect(
      replaceRhodeIslandCandidateFinanceSnapshot({
        db,
        link: baseLink(),
        summary: { totalReceipts: 1000 },
      })
    ).rejects.toThrow("write failed");

    expect(client.query.mock.calls[0]?.[0]).toBe("BEGIN");
    expect(client.query.mock.calls.at(-1)?.[0]).toBe("ROLLBACK");
    expect(client.release).toHaveBeenCalledTimes(1);
  });

  it("rejects election years below the Rhode Island floor", async () => {
    const db = createMockDb();

    // v1 is the current (2026) cycle only: no statewide export exists, so
    // historical cycles are a separate, separately tested expansion
    // (rhode_island_plan.md).
    await expect(
      upsertRhodeIslandFinanceLink({
        db,
        link: { ...baseLink(), electionYear: 2025 },
      })
    ).rejects.toThrow("Invalid Rhode Island finance election year");

    expect(db.query).not.toHaveBeenCalled();
  });
});
