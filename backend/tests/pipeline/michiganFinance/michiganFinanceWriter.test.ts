import { describe, expect, it, vi } from "vitest";

import {
  deactivateMichiganFinanceLinksForCandidateElection,
  replaceMichiganCandidateFinanceSnapshot,
  upsertMichiganFinanceLink,
} from "../../../src/pipeline/michiganFinance/michiganFinanceWriter.js";

const CANDIDATE_ID = "11111111-1111-1111-1111-111111111111";
const ELECTION_ID = "22222222-2222-2222-2222-222222222222";
const LINK_ID = "33333333-3333-3333-3333-333333333333";
const SOURCE_URL = "https://www.michigan.gov/sos/example/2022_mi_cfr.7z";

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
    candidateNameNormalized: "GRETCHEN WHITMER",
    officeName: "Governor",
    committeeId: "514456",
    committeeName: "WHITMER FOR GOVERNOR",
    linkSource: "mitn_legacy" as const,
    sourceUrl: SOURCE_URL,
    lastVerifiedAt: new Date("2022-01-01T00:00:00.000Z"),
  };
}

describe("michiganFinanceWriter", () => {
  it("upserts Michigan finance links and returns the link id", async () => {
    const db = createMockDb();

    await expect(upsertMichiganFinanceLink({ db, link: baseLink() })).resolves.toEqual({ linkId: LINK_ID });

    expect(db.query).toHaveBeenCalledTimes(1);
    expect(String(db.query.mock.calls[0]?.[0])).toContain("INSERT INTO public.mi_candidate_finance_links");
    expect(String(db.query.mock.calls[0]?.[0])).toContain("ON CONFLICT (candidate_id, election_id, committee_id)");
    expect(String(db.query.mock.calls[0]?.[0])).toContain("WHEN mi_candidate_finance_links.link_source = 'manual'");
    expect(db.query.mock.calls[0]?.[1]).toEqual([
      CANDIDATE_ID,
      ELECTION_ID,
      2022,
      "GRETCHEN WHITMER",
      "Governor",
      null,
      "514456",
      "WHITMER FOR GOVERNOR",
      "active",
      "mitn_legacy",
      SOURCE_URL,
      "2022-01-01T00:00:00.000Z",
    ]);
  });

  it("does not deactivate manually curated Michigan finance links", async () => {
    const db = createMockDb();

    await expect(
      deactivateMichiganFinanceLinksForCandidateElection({
        db,
        candidateId: CANDIDATE_ID,
        electionId: ELECTION_ID,
        electionYear: 2022,
        verifiedAt: new Date("2022-01-01T00:00:00.000Z"),
      })
    ).resolves.toBe(1);

    expect(db.query).toHaveBeenCalledTimes(1);
    expect(String(db.query.mock.calls[0]?.[0])).toContain("link_source IS DISTINCT FROM 'manual'");
    expect(db.query.mock.calls[0]?.[1]).toEqual([
      CANDIDATE_ID,
      ELECTION_ID,
      2022,
      "2022-01-01T00:00:00.000Z",
    ]);
  });

  it("replaces a Michigan finance snapshot inside a transaction", async () => {
    const client = {
      query: vi.fn().mockResolvedValue({ rows: [{ id: LINK_ID }], rowCount: 1 }),
      release: vi.fn(),
    };
    const db = {
      query: vi.fn(),
      connect: vi.fn().mockResolvedValue(client),
    };

    const result = await replaceMichiganCandidateFinanceSnapshot({
      db,
      link: baseLink(),
      syncedAt: new Date("2022-02-03T04:05:06.000Z"),
      summary: {
        totalReceipts: 5350,
        directContributionTotal: 5350,
        outsideSupportTotal: 0,
        outsideOpposeTotal: 863076.75,
        sourceUrl: SOURCE_URL,
      },
      directBreakdowns: [
        {
          categoryType: "occupation",
          categoryName: "Attorney",
          amount: 350,
          contributorCount: 2,
          sourceUrl: SOURCE_URL,
        },
        {
          categoryType: "contribution_size",
          categoryName: "$250-$499",
          amount: 250,
          contributorCount: 1,
          sourceUrl: SOURCE_URL,
        },
      ],
      outsideGroups: [
        {
          committeeId: "520012",
          committeeName: "GET MICHIGAN WORKING AGAIN (SUPERPAC)",
          supportOppose: "oppose",
          amount: 863076.75,
          sourceUrl: SOURCE_URL,
        },
      ],
      outsideGroupBreakdowns: [
        {
          committeeId: "520012",
          supportOppose: "oppose",
          categoryType: "donor",
          categoryName: "Energy Transfer LLC",
          amount: 35000,
          contributorCount: 1,
          sourceUrl: SOURCE_URL,
        },
        {
          committeeId: "520012",
          supportOppose: "oppose",
          categoryType: "industry",
          categoryName: "oil_gas_energy",
          amount: 35000,
          contributorCount: 1,
          sourceUrl: SOURCE_URL,
        },
      ],
    });

    expect(result).toEqual({
      linkId: LINK_ID,
      summaryWritten: true,
      directBreakdownsWritten: 2,
      outsideGroupsWritten: 1,
      outsideGroupBreakdownsWritten: 2,
    });
    expect(db.connect).toHaveBeenCalledTimes(1);
    expect(db.query).not.toHaveBeenCalled();
    expect(client.query.mock.calls[0]?.[0]).toBe("BEGIN");
    expect(client.query.mock.calls.at(-1)?.[0]).toBe("COMMIT");
    expect(client.release).toHaveBeenCalledTimes(1);

    const sql = client.query.mock.calls.map((call) => String(call[0]));
    expect(sql.some((statement) => statement.includes("INSERT INTO public.mi_candidate_finance_summaries"))).toBe(true);
    expect(sql.some((statement) => statement.includes("total_receipts = EXCLUDED.total_receipts"))).toBe(true);
    expect(sql.some((statement) => statement.includes("COALESCE(EXCLUDED.total_receipts"))).toBe(false);
    expect(sql.filter((statement) => statement.includes("INSERT INTO public.mi_candidate_finance_direct_breakdowns"))).toHaveLength(2);
    expect(sql.filter((statement) => statement.includes("INSERT INTO public.mi_candidate_finance_outside_groups"))).toHaveLength(1);
    expect(sql.filter((statement) => statement.includes("INSERT INTO public.mi_candidate_finance_outside_group_breakdowns"))).toHaveLength(2);
    expect(sql.some((statement) => statement.includes("DELETE FROM public.mi_candidate_finance_direct_breakdowns"))).toBe(true);
    expect(sql.some((statement) => statement.includes("DELETE FROM public.mi_candidate_finance_outside_groups"))).toBe(true);
    expect(sql.some((statement) => statement.includes("DELETE FROM public.mi_candidate_finance_outside_group_breakdowns"))).toBe(true);
  });

  it("preserves prior outside totals when a partial refresh does not have expenditure data", async () => {
    const client = {
      query: vi.fn().mockResolvedValue({ rows: [{ id: LINK_ID }], rowCount: 1 }),
      release: vi.fn(),
    };
    const db = {
      query: vi.fn(),
      connect: vi.fn().mockResolvedValue(client),
    };

    await replaceMichiganCandidateFinanceSnapshot({
      db,
      link: baseLink(),
      syncedAt: new Date("2022-02-03T04:05:06.000Z"),
      summary: {
        totalReceipts: 5350,
        directContributionTotal: 5350,
        outsideSupportTotal: null,
        outsideOpposeTotal: null,
      },
    });

    const summaryCall = client.query.mock.calls.find((call) =>
      String(call[0]).includes("INSERT INTO public.mi_candidate_finance_summaries")
    );
    expect(String(summaryCall?.[0])).toContain(
      "outside_support_total = COALESCE(\n          EXCLUDED.outside_support_total,\n          mi_candidate_finance_summaries.outside_support_total\n        )"
    );
    expect(String(summaryCall?.[0])).toContain(
      "outside_oppose_total = COALESCE(\n          EXCLUDED.outside_oppose_total,\n          mi_candidate_finance_summaries.outside_oppose_total\n        )"
    );
    expect(summaryCall?.[1]?.[6]).toBeNull();
    expect(summaryCall?.[1]?.[7]).toBeNull();
  });

  it("wraps a supplied queryable in a transaction", async () => {
    const db = createMockDb();

    const result = await replaceMichiganCandidateFinanceSnapshot({
      db,
      link: baseLink(),
      syncedAt: new Date("2022-02-03T04:05:06.000Z"),
      summary: { totalReceipts: 1000 },
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
      replaceMichiganCandidateFinanceSnapshot({
        db: client,
        link: baseLink(),
        syncedAt: new Date("2022-02-03T04:05:06.000Z"),
        summary: { totalReceipts: 1000 },
      })
    ).rejects.toThrow("Michigan finance snapshot writes must receive a Pool, not a PoolClient");
    expect(client.query).not.toHaveBeenCalled();
  });

  it("rejects outside breakdown snapshots without outside groups", async () => {
    const db = createMockDb();

    await expect(
      replaceMichiganCandidateFinanceSnapshot({
        db,
        link: baseLink(),
        outsideGroupBreakdowns: [
          {
            committeeId: "520012",
            supportOppose: "oppose",
            categoryType: "industry",
            categoryName: "oil_gas_energy",
            amount: 35000,
          },
        ],
      })
    ).rejects.toThrow("outside group breakdowns require outside groups");
    expect(db.query).not.toHaveBeenCalled();
  });

  it("rejects outside breakdown snapshots with mismatched outside groups", async () => {
    const db = createMockDb();

    await expect(
      replaceMichiganCandidateFinanceSnapshot({
        db,
        link: baseLink(),
        outsideGroups: [
          {
            committeeId: "520012",
            committeeName: "GET MICHIGAN WORKING AGAIN",
            supportOppose: "support",
            amount: 100,
          },
        ],
        outsideGroupBreakdowns: [
          {
            committeeId: "520012",
            supportOppose: "oppose",
            categoryType: "industry",
            categoryName: "oil_gas_energy",
            amount: 35000,
          },
        ],
      })
    ).rejects.toThrow("outside group breakdowns must reference outside groups");
    expect(db.query).not.toHaveBeenCalled();
  });
});
