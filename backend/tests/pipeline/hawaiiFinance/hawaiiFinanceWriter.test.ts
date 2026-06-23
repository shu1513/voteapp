import { describe, expect, it, vi } from "vitest";

import { upsertHawaiiFinanceLink } from "../../../src/pipeline/hawaiiFinance/hawaiiFinanceWriter.js";

const CANDIDATE_ID = "11111111-1111-4111-8111-111111111111";
const ELECTION_ID = "22222222-2222-4222-8222-222222222222";
const LINK_ID = "33333333-3333-4333-8333-333333333333";

function createMockDb() {
  return {
    query: vi.fn().mockResolvedValue({ rows: [{ id: LINK_ID }], rowCount: 1 }),
  };
}

function baseLink() {
  return {
    candidateId: CANDIDATE_ID,
    electionId: ELECTION_ID,
    electionYear: 2022,
    candidateNameNormalized: "JOSH GREEN",
    officeName: "Governor",
    district: null,
    committeeId: "CC10174",
    committeeName: "Green, Josh",
    electionPeriod: "2018-2022",
    linkSource: "csc_api" as const,
    sourceUrl: "https://hicscdata.hawaii.gov/dataset/Campaign-Contributions-Received-By-Hawaii-State-an/jexd-xbcg",
    lastVerifiedAt: new Date("2026-06-01T00:00:00.000Z"),
  };
}

describe("hawaiiFinanceWriter", () => {
  it("upserts Hawaii finance links and returns the link id", async () => {
    const db = createMockDb();

    await expect(upsertHawaiiFinanceLink({ db, link: baseLink() })).resolves.toEqual({ linkId: LINK_ID });

    expect(db.query).toHaveBeenCalledTimes(1);
    expect(String(db.query.mock.calls[0]?.[0])).toContain("INSERT INTO public.hi_candidate_finance_links");
    expect(String(db.query.mock.calls[0]?.[0])).toContain("ON CONFLICT (candidate_id, election_id, committee_id)");
    expect(String(db.query.mock.calls[0]?.[0])).toContain("RETURNING id");
    expect(db.query.mock.calls[0]?.[1]).toEqual([
      CANDIDATE_ID,
      ELECTION_ID,
      2022,
      "JOSH GREEN",
      "Governor",
      null,
      "CC10174",
      "Green, Josh",
      "2018-2022",
      "active",
      "csc_api",
      "https://hicscdata.hawaii.gov/dataset/Campaign-Contributions-Received-By-Hawaii-State-an/jexd-xbcg",
      "2026-06-01T00:00:00.000Z",
    ]);
  });

  it("defaults optional link metadata safely", async () => {
    const db = createMockDb();

    await expect(
      upsertHawaiiFinanceLink({
        db,
        link: {
          ...baseLink(),
          district: " 12 ",
          linkSource: undefined,
          sourceUrl: " ",
          lastVerifiedAt: null,
        },
      })
    ).resolves.toEqual({ linkId: LINK_ID });

    expect(db.query.mock.calls[0]?.[1]).toEqual([
      CANDIDATE_ID,
      ELECTION_ID,
      2022,
      "JOSH GREEN",
      "Governor",
      "12",
      "CC10174",
      "Green, Josh",
      "2018-2022",
      "active",
      "manual",
      null,
      null,
    ]);
  });

  it("validates required Hawaii finance link fields before writing", async () => {
    const db = createMockDb();

    await expect(
      upsertHawaiiFinanceLink({
        db,
        link: {
          ...baseLink(),
          electionPeriod: " ",
        },
      })
    ).rejects.toThrow("Hawaii election period is required");

    expect(db.query).not.toHaveBeenCalled();
  });

  it("rejects invalid election years and timestamps", async () => {
    const db = createMockDb();

    await expect(
      upsertHawaiiFinanceLink({
        db,
        link: {
          ...baseLink(),
          electionYear: 1999,
        },
      })
    ).rejects.toThrow("Invalid Hawaii finance election year: 1999");

    await expect(
      upsertHawaiiFinanceLink({
        db,
        link: {
          ...baseLink(),
          lastVerifiedAt: new Date("not-a-date"),
        },
      })
    ).rejects.toThrow("Invalid Hawaii finance timestamp");

    expect(db.query).not.toHaveBeenCalled();
  });
});

describe("hawaiiFinanceSnapshotWriter", () => {
  function createTransactionalMockDb() {
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

  it("replaces a Hawaii direct finance snapshot inside a transaction", async () => {
    const { replaceHawaiiCandidateFinanceSnapshot } = await import(
      "../../../src/pipeline/hawaiiFinance/hawaiiFinanceWriter.js"
    );
    const db = createTransactionalMockDb();

    const result = await replaceHawaiiCandidateFinanceSnapshot({
      db,
      link: baseLink(),
      syncedAt: new Date("2026-06-02T03:04:05.000Z"),
      summary: {
        totalReceipts: 4070153.38,
        directContributionTotal: 4070153.38,
        outsideSupportTotal: 500557,
        outsideOpposeTotal: 10000,
        sourceUrl: "https://hicscdata.hawaii.gov/resource/jexd-xbcg.json",
      },
      directBreakdowns: [
        {
          categoryType: "occupation",
          categoryName: "Attorney",
          amount: 332962.31,
          contributorCount: 1200,
          sourceUrl: "https://hicscdata.hawaii.gov/resource/jexd-xbcg.json",
        },
        {
          categoryType: "contribution_size",
          categoryName: "1000_4999",
          amount: 150000,
          contributorCount: 30,
          sourceUrl: "https://hicscdata.hawaii.gov/resource/jexd-xbcg.json",
        },
      ],
      outsideGroups: [
        {
          committeeId: "NC101",
          committeeName: "Be Change Now",
          supportOppose: "support",
          amount: 500557,
          sourceUrl: "https://hicscdata.hawaii.gov/resource/riiu-7d4b.json",
        },
        {
          committeeId: "NC202",
          committeeName: "Hawaii Future PAC",
          supportOppose: "oppose",
          amount: 10000,
          sourceUrl: "https://hicscdata.hawaii.gov/resource/riiu-7d4b.json",
        },
      ],
      outsideGroupBreakdowns: [
        {
          committeeId: "NC101",
          supportOppose: "support",
          categoryType: "donor",
          categoryName: "Hawaii Carpenters Market Recovery Program Fund",
          amount: 2086436.92,
          contributorCount: 1,
          sourceUrl: "https://hicscdata.hawaii.gov/resource/rajm-32md.json",
        },
        {
          committeeId: "NC101",
          supportOppose: "support",
          categoryType: "industry",
          categoryName: "construction",
          amount: 2086436.92,
          contributorCount: 1,
          sourceUrl: "https://hicscdata.hawaii.gov/resource/rajm-32md.json",
        },
      ],
      classifications: [
        {
          rawLabel: "Hawaii Carpenters Market Recovery Program Fund",
          labelType: "donor",
          normalizedLabel: "HAWAII CARPENTERS MARKET RECOVERY PROGRAM FUND",
          industrySlug: "construction",
          confidence: "high",
          classificationSource: "ai",
          matchedRule: null,
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
    expect(db.query.mock.calls[0]?.[0]).toBe("BEGIN");
    expect(db.query.mock.calls.at(-1)?.[0]).toBe("COMMIT");
    expect(db.client.release).toHaveBeenCalledTimes(1);

    const sql = db.query.mock.calls.map((call) => String(call[0]));
    expect(sql.some((statement) => statement.includes("INSERT INTO public.hi_candidate_finance_summaries"))).toBe(true);
    expect(sql.filter((statement) => statement.includes("INSERT INTO public.hi_candidate_finance_direct_breakdowns"))).toHaveLength(2);
    expect(sql.filter((statement) => statement.includes("INSERT INTO public.hi_candidate_finance_outside_groups"))).toHaveLength(2);
    expect(sql.filter((statement) => statement.includes("INSERT INTO public.hi_candidate_finance_outside_group_breakdowns"))).toHaveLength(2);
    expect(sql.filter((statement) => statement.includes("INSERT INTO public.finance_label_classifications"))).toHaveLength(1);
    expect(sql.some((statement) => statement.includes("DELETE FROM public.hi_candidate_finance_direct_breakdowns"))).toBe(true);
    expect(sql.some((statement) => statement.includes("DELETE FROM public.hi_candidate_finance_outside_groups"))).toBe(true);
    expect(sql.some((statement) => statement.includes("DELETE FROM public.hi_candidate_finance_outside_group_breakdowns"))).toBe(true);

    const summaryCall = db.query.mock.calls.find((call) =>
      String(call[0]).includes("INSERT INTO public.hi_candidate_finance_summaries")
    );
    expect(String(summaryCall?.[0])).toContain(
      "total_receipts = COALESCE(EXCLUDED.total_receipts, hi_candidate_finance_summaries.total_receipts)"
    );
    expect(String(summaryCall?.[0])).toContain(
      "direct_contribution_total = COALESCE(EXCLUDED.direct_contribution_total, hi_candidate_finance_summaries.direct_contribution_total)"
    );
    expect(String(summaryCall?.[0])).toContain("outside_support_total = EXCLUDED.outside_support_total");
    expect(summaryCall?.[1]).toEqual([
      LINK_ID,
      2022,
      4070153.38,
      4070153.38,
      null,
      null,
      500557,
      10000,
      "https://hicscdata.hawaii.gov/resource/jexd-xbcg.json",
      "2026-06-02T03:04:05.000Z",
    ]);

    const outsideGroupCall = db.query.mock.calls.find((call) =>
      String(call[0]).includes("INSERT INTO public.hi_candidate_finance_outside_groups")
    );
    expect(outsideGroupCall?.[1]).toEqual([
      LINK_ID,
      2022,
      "NC101",
      "Be Change Now",
      "support",
      500557,
      "https://hicscdata.hawaii.gov/resource/riiu-7d4b.json",
      "2026-06-02T03:04:05.000Z",
    ]);

    const outsideBreakdownCall = db.query.mock.calls.find((call) =>
      String(call[0]).includes("INSERT INTO public.hi_candidate_finance_outside_group_breakdowns")
    );
    expect(outsideBreakdownCall?.[1]).toEqual([
      LINK_ID,
      2022,
      "NC101",
      "support",
      "donor",
      "Hawaii Carpenters Market Recovery Program Fund",
      2086436.92,
      1,
      "https://hicscdata.hawaii.gov/resource/rajm-32md.json",
      "2026-06-02T03:04:05.000Z",
    ]);
    const classificationCall = db.query.mock.calls.find((call) =>
      String(call[0]).includes("INSERT INTO public.finance_label_classifications")
    );
    expect(classificationCall?.[1]).toEqual([
      "Hawaii Carpenters Market Recovery Program Fund",
      "donor",
      "HAWAII CARPENTERS MARKET RECOVERY PROGRAM FUND",
      "construction",
      "high",
      "ai",
    ]);
  });

  it("rejects query-only and PoolClient-like snapshot writers so writes stay atomic", async () => {
    const { replaceHawaiiCandidateFinanceSnapshot } = await import(
      "../../../src/pipeline/hawaiiFinance/hawaiiFinanceWriter.js"
    );
    const queryOnlyDb = {
      query: vi.fn().mockResolvedValue({ rows: [{ id: LINK_ID }], rowCount: 1 }),
    };
    await expect(
      replaceHawaiiCandidateFinanceSnapshot({
        db: queryOnlyDb as never,
        link: baseLink(),
        summary: { totalReceipts: 1000 },
      })
    ).rejects.toThrow("Hawaii finance snapshot writes must receive a Pool");
    expect(queryOnlyDb.query).not.toHaveBeenCalled();

    const clientLikeDb = {
      query: vi.fn().mockResolvedValue({ rows: [{ id: LINK_ID }], rowCount: 1 }),
      release: vi.fn(),
    };
    await expect(
      replaceHawaiiCandidateFinanceSnapshot({
        db: clientLikeDb as never,
        link: baseLink(),
        summary: { totalReceipts: 1000 },
      })
    ).rejects.toThrow("Hawaii finance snapshot writes must receive a Pool, not a PoolClient");
    expect(clientLikeDb.query).not.toHaveBeenCalled();
  });

  it("rolls back and releases the client when a transactional write fails", async () => {
    const { replaceHawaiiCandidateFinanceSnapshot } = await import(
      "../../../src/pipeline/hawaiiFinance/hawaiiFinanceWriter.js"
    );
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
      replaceHawaiiCandidateFinanceSnapshot({
        db,
        link: baseLink(),
        summary: { totalReceipts: 1000 },
      })
    ).rejects.toThrow("write failed");

    expect(client.query.mock.calls[0]?.[0]).toBe("BEGIN");
    expect(client.query.mock.calls.at(-1)?.[0]).toBe("ROLLBACK");
    expect(client.release).toHaveBeenCalledTimes(1);
  });

  it("rejects outside group breakdowns without same-snapshot outside groups", async () => {
    const { replaceHawaiiCandidateFinanceSnapshot } = await import(
      "../../../src/pipeline/hawaiiFinance/hawaiiFinanceWriter.js"
    );
    const db = createTransactionalMockDb();

    await expect(
      replaceHawaiiCandidateFinanceSnapshot({
        db,
        link: baseLink(),
        outsideGroupBreakdowns: [
          {
            committeeId: "NC101",
            supportOppose: "support",
            categoryType: "donor",
            categoryName: "Hawaii Carpenters Market Recovery Program Fund",
            amount: 2086436.92,
          },
        ],
      })
    ).rejects.toThrow("Hawaii outside group breakdowns require outside groups in the same snapshot");
    expect(db.connect).not.toHaveBeenCalled();
  });

  it("allows empty outside group breakdowns without outside groups", async () => {
    const { replaceHawaiiCandidateFinanceSnapshot } = await import(
      "../../../src/pipeline/hawaiiFinance/hawaiiFinanceWriter.js"
    );
    const db = createTransactionalMockDb();

    await expect(
      replaceHawaiiCandidateFinanceSnapshot({
        db,
        link: baseLink(),
        outsideGroupBreakdowns: [],
      })
    ).resolves.toMatchObject({
      linkId: LINK_ID,
      outsideGroupBreakdownsWritten: 0,
    });
    expect(db.connect).toHaveBeenCalledTimes(1);
  });

  it("rejects outside group breakdowns with an empty same-snapshot outside group list", async () => {
    const { replaceHawaiiCandidateFinanceSnapshot } = await import(
      "../../../src/pipeline/hawaiiFinance/hawaiiFinanceWriter.js"
    );
    const db = createTransactionalMockDb();

    await expect(
      replaceHawaiiCandidateFinanceSnapshot({
        db,
        link: baseLink(),
        outsideGroups: [],
        outsideGroupBreakdowns: [
          {
            committeeId: "NC101",
            supportOppose: "support",
            categoryType: "donor",
            categoryName: "Hawaii Carpenters Market Recovery Program Fund",
            amount: 2086436.92,
          },
        ],
      })
    ).rejects.toThrow("Hawaii outside group breakdowns require outside groups in the same snapshot");
    expect(db.connect).not.toHaveBeenCalled();
  });
});
