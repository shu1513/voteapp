import { describe, expect, it, vi } from "vitest";

import {
  replaceMaineCandidateFinanceSnapshot,
  upsertMaineFinanceLink,
} from "../../../src/pipeline/maineFinance/maineFinanceWriter.js";

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
    electionYear: 2024,
    candidateNameNormalized: "REAGAN LEEANN PAUL",
    officeName: "Representative",
    district: "37",
    committeeId: "1001",
    committeeName: "Paul for Maine",
    linkSource: "cfis_bulk" as const,
    sourceUrl: "https://mainecampaignfinance.com/",
    lastVerifiedAt: new Date("2026-06-25T00:00:00.000Z"),
  };
}

describe("maineFinanceWriter", () => {
  it("upserts Maine finance links and returns the link id", async () => {
    const db = createMockDb();

    await expect(upsertMaineFinanceLink({ db, link: baseLink() })).resolves.toEqual({ linkId: LINK_ID });

    expect(db.query).toHaveBeenCalledTimes(1);
    expect(String(db.query.mock.calls[0]?.[0])).toContain("INSERT INTO public.me_candidate_finance_links");
    expect(String(db.query.mock.calls[0]?.[0])).toContain("ON CONFLICT (candidate_id, election_id, committee_id)");
    expect(String(db.query.mock.calls[0]?.[0])).toContain("RETURNING id");
    expect(db.query.mock.calls[0]?.[1]).toEqual([
      CANDIDATE_ID,
      ELECTION_ID,
      2024,
      "REAGAN LEEANN PAUL",
      "Representative",
      "37",
      "1001",
      "Paul for Maine",
      "active",
      "cfis_bulk",
      "https://mainecampaignfinance.com/",
      "2026-06-25T00:00:00.000Z",
    ]);
  });

  it("normalizes committee ids at the writer boundary", async () => {
    const db = createMockDb();

    await upsertMaineFinanceLink({ db, link: { ...baseLink(), committeeId: " org abc 123 " } });

    expect(db.query.mock.calls[0]?.[1]?.[6]).toBe("ORG ABC 123");
  });

  it("replaces a full Maine finance snapshot inside a transaction", async () => {
    const client = {
      query: vi.fn().mockResolvedValue({ rows: [{ id: LINK_ID }], rowCount: 1 }),
      release: vi.fn(),
    };
    const db = {
      query: vi.fn(),
      connect: vi.fn().mockResolvedValue(client),
    };

    const result = await replaceMaineCandidateFinanceSnapshot({
      db,
      link: baseLink(),
      syncedAt: new Date("2026-06-25T12:00:00.000Z"),
      summary: {
        totalReceipts: 5500,
        directContributionTotal: 500,
        outsideSupportTotal: 1600,
        outsideOpposeTotal: 200,
        sourceUrl: "https://mainecampaignfinance.com/",
      },
      directBreakdowns: [
        {
          categoryType: "occupation",
          categoryName: "Attorney/Legal",
          amount: 250,
          contributorCount: 1,
          sourceUrl: "https://mainecampaignfinance.com/",
        },
        {
          categoryType: "contribution_size",
          categoryName: "$100-$249",
          amount: 250,
          contributorCount: 1,
        },
      ],
      outsideGroups: [
        {
          committeeId: "242",
          committeeName: "ASSOCIATED BUILDERS AND CONTRACTORS OF MAINE PAC",
          supportOppose: "support",
          amount: 1600,
          sourceUrl: "https://mainecampaignfinance.com/",
        },
      ],
      outsideGroupBreakdowns: [
        {
          committeeId: "242",
          supportOppose: "support",
          categoryType: "industry",
          categoryName: "construction",
          amount: 35000,
          contributorCount: 1,
          sourceUrl: "https://mainecampaignfinance.com/",
        },
      ],
    });

    expect(result).toEqual({
      linkId: LINK_ID,
      summaryWritten: true,
      directBreakdownsWritten: 2,
      outsideGroupsWritten: 1,
      outsideGroupBreakdownsWritten: 1,
    });
    expect(db.connect).toHaveBeenCalledTimes(1);
    expect(db.query).not.toHaveBeenCalled();
    expect(client.query.mock.calls[0]?.[0]).toBe("BEGIN");
    expect(client.query.mock.calls.at(-1)?.[0]).toBe("COMMIT");
    expect(client.release).toHaveBeenCalledTimes(1);

    const sql = client.query.mock.calls.map((call) => String(call[0]));
    expect(sql.some((statement) => statement.includes("INSERT INTO public.me_candidate_finance_summaries"))).toBe(true);
    const summaryCall = client.query.mock.calls.find((call) =>
      String(call[0]).includes("INSERT INTO public.me_candidate_finance_summaries")
    );
    expect(String(summaryCall?.[0])).toContain("cash_on_hand");
    expect(summaryCall?.[1]).toEqual([
      LINK_ID,
      2024,
      5500,
      500,
      null,
      null,
      1600,
      200,
      "https://mainecampaignfinance.com/",
      "2026-06-25T12:00:00.000Z",
    ]);
    expect(sql.some((statement) => statement.includes("UPDATE public.me_candidate_finance_links"))).toBe(true);
    expect(sql.filter((statement) => statement.includes("INSERT INTO public.me_candidate_finance_direct_breakdowns"))).toHaveLength(2);
    expect(sql.filter((statement) => statement.includes("INSERT INTO public.me_candidate_finance_outside_groups"))).toHaveLength(1);
    expect(sql.filter((statement) => statement.includes("INSERT INTO public.me_candidate_finance_outside_group_breakdowns"))).toHaveLength(1);
    expect(sql.some((statement) => statement.includes("DELETE FROM public.me_candidate_finance_direct_breakdowns"))).toBe(true);
    expect(sql.some((statement) => statement.includes("DELETE FROM public.me_candidate_finance_outside_groups"))).toBe(true);
    expect(sql.some((statement) => statement.includes("DELETE FROM public.me_candidate_finance_outside_group_breakdowns"))).toBe(true);
  });

  it("does not delete omitted sections", async () => {
    const client = {
      query: vi.fn().mockResolvedValue({ rows: [{ id: LINK_ID }], rowCount: 1 }),
      release: vi.fn(),
    };
    const db = {
      query: vi.fn(),
      connect: vi.fn().mockResolvedValue(client),
    };

    const result = await replaceMaineCandidateFinanceSnapshot({
      db,
      link: { ...baseLink(), linkSource: "manual" },
      syncedAt: new Date("2026-06-25T12:00:00.000Z"),
      summary: {
        outsideSupportTotal: 1000,
      },
    });

    expect(result).toEqual({
      linkId: LINK_ID,
      summaryWritten: true,
      directBreakdownsWritten: 0,
      outsideGroupsWritten: 0,
      outsideGroupBreakdownsWritten: 0,
    });
    const sql = client.query.mock.calls.map((call) => String(call[0]));
    const summarySql = sql.find((statement) => statement.includes("INSERT INTO public.me_candidate_finance_summaries"));
    expect(summarySql).toContain(
      "outside_support_total = COALESCE(\n          EXCLUDED.outside_support_total"
    );
    expect(sql.some((statement) => statement.includes("DELETE FROM public.me_candidate_finance_direct_breakdowns"))).toBe(false);
    expect(sql.some((statement) => statement.includes("DELETE FROM public.me_candidate_finance_outside_groups"))).toBe(false);
    expect(sql.some((statement) => statement.includes("DELETE FROM public.me_candidate_finance_outside_group_breakdowns"))).toBe(
      false
    );
  });

  it("validates outside group breakdowns before writing", async () => {
    const db = {
      query: vi.fn(),
      connect: vi.fn(),
    };

    await expect(
      replaceMaineCandidateFinanceSnapshot({
        db,
        link: baseLink(),
        outsideGroupBreakdowns: [
          {
            committeeId: "242",
            supportOppose: "support",
            categoryType: "industry",
            categoryName: "construction",
            amount: 100,
          },
        ],
      })
    ).rejects.toThrow("Maine outside group breakdowns require outside groups in the same snapshot");

    await expect(
      replaceMaineCandidateFinanceSnapshot({
        db,
        link: baseLink(),
        outsideGroups: [
          {
            committeeId: "999",
            committeeName: "Other PAC",
            supportOppose: "support",
            amount: 50,
          },
        ],
        outsideGroupBreakdowns: [
          {
            committeeId: "242",
            supportOppose: "support",
            categoryType: "industry",
            categoryName: "construction",
            amount: 100,
          },
        ],
      })
    ).rejects.toThrow("Maine outside group breakdowns require matching outside groups in the same snapshot");

    expect(db.connect).not.toHaveBeenCalled();
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
      replaceMaineCandidateFinanceSnapshot({
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
      upsertMaineFinanceLink({
        db,
        link: {
          ...baseLink(),
          electionYear: 1999,
        },
      })
    ).rejects.toThrow("Invalid Maine finance election year");

    await expect(
      upsertMaineFinanceLink({
        db,
        link: {
          ...baseLink(),
          committeeId: " ",
        },
      })
    ).rejects.toThrow("Maine committee id is required");

    expect(db.query).not.toHaveBeenCalled();
  });
});
