import { describe, expect, it, vi } from "vitest";

import {
  replaceAlaskaCandidateFinanceSnapshot,
  upsertAlaskaFinanceLink,
} from "../../../src/pipeline/alaskaFinance/alaskaFinanceWriter.js";

const CANDIDATE_ID = "11111111-1111-4111-8111-111111111111";
const ELECTION_ID = "22222222-2222-4222-8222-222222222222";
const LINK_ID = "33333333-3333-4333-8333-333333333333";

function createMockDb() {
  return {
    query: vi.fn().mockResolvedValue({ rows: [{ id: LINK_ID }], rowCount: 1 }),
  };
}

function createTransactionalMockDb() {
  const query = vi.fn().mockResolvedValue({ rows: [{ id: LINK_ID }], rowCount: 1 });
  const client = { query, release: vi.fn() };
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
    electionYear: 2026,
    candidateNameNormalized: "JANE DOE",
    officeName: "Governor",
    district: null,
    candidateFilerId: "1001",
    candidateFilerName: "Jane Doe",
    linkSource: "apoc_csv" as const,
    sourceUrl: "https://aws.state.ak.us/ApocReports/CampaignDisclosure/CDIncome.aspx",
    lastVerifiedAt: new Date("2026-06-25T12:00:00.000Z"),
  };
}

describe("alaskaFinanceWriter", () => {
  it("upserts Alaska finance links and returns the link id", async () => {
    const db = createMockDb();

    await expect(upsertAlaskaFinanceLink({ db, link: baseLink() })).resolves.toEqual({ linkId: LINK_ID });

    expect(db.query).toHaveBeenCalledTimes(1);
    expect(String(db.query.mock.calls[0]?.[0])).toContain("INSERT INTO public.ak_candidate_finance_links");
    expect(String(db.query.mock.calls[0]?.[0])).toContain("ON CONFLICT (candidate_id, election_id, candidate_filer_id)");
    expect(db.query.mock.calls[0]?.[1]).toEqual([
      CANDIDATE_ID,
      ELECTION_ID,
      2026,
      "JANE DOE",
      "Governor",
      null,
      "1001",
      "Jane Doe",
      "active",
      "apoc_csv",
      "https://aws.state.ak.us/ApocReports/CampaignDisclosure/CDIncome.aspx",
      "2026-06-25T12:00:00.000Z",
    ]);
  });

  it("defaults optional link metadata safely", async () => {
    const db = createMockDb();

    await expect(
      upsertAlaskaFinanceLink({
        db,
        link: {
          ...baseLink(),
          district: " 4 ",
          linkSource: undefined,
          sourceUrl: " ",
          lastVerifiedAt: null,
        },
      })
    ).resolves.toEqual({ linkId: LINK_ID });

    expect(db.query.mock.calls[0]?.[1]).toEqual([
      CANDIDATE_ID,
      ELECTION_ID,
      2026,
      "JANE DOE",
      "Governor",
      "4",
      "1001",
      "Jane Doe",
      "active",
      "manual",
      null,
      null,
    ]);
  });

  it("replaces an Alaska finance snapshot inside one transaction", async () => {
    const db = createTransactionalMockDb();

    const result = await replaceAlaskaCandidateFinanceSnapshot({
      db,
      link: baseLink(),
      syncedAt: new Date("2026-06-25T12:30:00.000Z"),
      summary: {
        totalReceipts: 5_350,
        directContributionTotal: 5_350,
        outsideSupportTotal: 35_000,
        outsideOpposeTotal: 5_000,
        sourceUrl: "https://aws.state.ak.us/ApocReports/CampaignDisclosure/CDIncome.aspx",
      },
      directBreakdowns: [
        {
          categoryType: "occupation",
          categoryName: "Attorney",
          amount: 350,
          contributorCount: 2,
          sourceUrl: "https://aws.state.ak.us/ApocReports/CampaignDisclosure/CDIncome.aspx",
        },
        {
          categoryType: "contribution_size",
          categoryName: "$5,000+",
          amount: 5_000,
          contributorCount: 1,
          sourceUrl: "https://aws.state.ak.us/ApocReports/CampaignDisclosure/CDIncome.aspx",
        },
      ],
      outsideGroups: [
        {
          outsideGroupId: "8001",
          outsideGroupName: "Alaska Future PAC",
          supportOppose: "support",
          amount: 35_000,
          sourceUrl: "https://aws.state.ak.us/ApocReports/IndependentExpenditures/IEExpenditures.aspx",
        },
      ],
      outsideGroupBreakdowns: [
        {
          outsideGroupId: "8001",
          supportOppose: "support",
          categoryType: "donor",
          categoryName: "Energy Transfer LLC",
          amount: 35_000,
          contributorCount: 1,
          sourceUrl: "https://aws.state.ak.us/ApocReports/IndependentExpenditures/IEContributions.aspx",
        },
        {
          outsideGroupId: "8001",
          supportOppose: "support",
          categoryType: "industry",
          categoryName: "oil_gas_energy",
          amount: 35_000,
          contributorCount: 1,
          sourceUrl: "https://aws.state.ak.us/ApocReports/IndependentExpenditures/IEContributions.aspx",
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
    expect(db.query.mock.calls[0]?.[0]).toBe("BEGIN");
    expect(db.query.mock.calls.at(-1)?.[0]).toBe("COMMIT");
    expect(db.client.release).toHaveBeenCalledTimes(1);

    const sql = db.query.mock.calls.map((call) => String(call[0]));
    expect(sql.some((statement) => statement.includes("INSERT INTO public.ak_candidate_finance_summaries"))).toBe(true);
    expect(sql.filter((statement) => statement.includes("INSERT INTO public.ak_candidate_finance_direct_breakdowns"))).toHaveLength(2);
    expect(sql.filter((statement) => statement.includes("INSERT INTO public.ak_candidate_finance_outside_groups"))).toHaveLength(1);
    expect(sql.filter((statement) => statement.includes("INSERT INTO public.ak_candidate_finance_outside_group_breakdowns"))).toHaveLength(2);
    expect(sql.some((statement) => statement.includes("DELETE FROM public.ak_candidate_finance_direct_breakdowns"))).toBe(true);
    expect(sql.some((statement) => statement.includes("DELETE FROM public.ak_candidate_finance_outside_groups"))).toBe(true);
    expect(sql.some((statement) => statement.includes("DELETE FROM public.ak_candidate_finance_outside_group_breakdowns"))).toBe(true);

    const summaryCall = db.query.mock.calls.find((call) =>
      String(call[0]).includes("INSERT INTO public.ak_candidate_finance_summaries")
    );
    expect(summaryCall?.[1]).toEqual([
      LINK_ID,
      2026,
      5_350,
      5_350,
      null,
      null,
      35_000,
      5_000,
      "https://aws.state.ak.us/ApocReports/CampaignDisclosure/CDIncome.aspx",
      "2026-06-25T12:30:00.000Z",
    ]);
  });

  it("rejects query-only and PoolClient-like snapshot writers so writes stay atomic", async () => {
    const queryOnlyDb = { query: vi.fn().mockResolvedValue({ rows: [{ id: LINK_ID }], rowCount: 1 }) };
    await expect(
      replaceAlaskaCandidateFinanceSnapshot({
        db: queryOnlyDb as never,
        link: baseLink(),
        summary: { totalReceipts: 1000 },
      })
    ).rejects.toThrow("Alaska finance snapshot writes must receive a Pool");
    expect(queryOnlyDb.query).not.toHaveBeenCalled();

    const clientLikeDb = {
      query: vi.fn().mockResolvedValue({ rows: [{ id: LINK_ID }], rowCount: 1 }),
      release: vi.fn(),
    };
    await expect(
      replaceAlaskaCandidateFinanceSnapshot({
        db: clientLikeDb as never,
        link: baseLink(),
        summary: { totalReceipts: 1000 },
      })
    ).rejects.toThrow("Alaska finance snapshot writes must receive a Pool, not a PoolClient");
    expect(clientLikeDb.query).not.toHaveBeenCalled();
  });

  it("rejects outside group breakdowns that do not reference same-snapshot outside groups", async () => {
    const db = createTransactionalMockDb();

    await expect(
      replaceAlaskaCandidateFinanceSnapshot({
        db,
        link: baseLink(),
        outsideGroups: [
          {
            outsideGroupId: "8001",
            outsideGroupName: "Alaska Future PAC",
            supportOppose: "support",
            amount: 35_000,
          },
        ],
        outsideGroupBreakdowns: [
          {
            outsideGroupId: "9999",
            supportOppose: "support",
            categoryType: "donor",
            categoryName: "Energy Transfer LLC",
            amount: 35_000,
          },
        ],
      })
    ).rejects.toThrow("Alaska outside group breakdowns must reference outside groups in the same snapshot");
    expect(db.connect).not.toHaveBeenCalled();
  });
});
