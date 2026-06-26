import { describe, expect, it, vi } from "vitest";

import {
  replaceNewJerseyCandidateFinanceSnapshot,
  upsertNewJerseyFinanceLink,
} from "../../../src/pipeline/newJerseyFinance/newJerseyFinanceWriter.js";

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
    electionYear: 2025,
    candidateNameNormalized: "MIKIE SHERRILL",
    officeName: "Governor",
    district: null,
    candidateEntityS: 473742,
    entityName: "SHERRILL, MIKIE",
    electionTypeCode: "G",
    linkSource: "elec_api" as const,
    sourceUrl: "https://www.njelecefilesearch.com/api/VWEntity/GetEntityList?LastName=Sherrill",
    lastVerifiedAt: new Date("2026-06-25T12:00:00.000Z"),
  };
}

describe("newJerseyFinanceWriter", () => {
  it("upserts New Jersey finance links and returns the link id", async () => {
    const db = createMockDb();

    await expect(upsertNewJerseyFinanceLink({ db, link: baseLink() })).resolves.toEqual({ linkId: LINK_ID });

    expect(db.query).toHaveBeenCalledTimes(1);
    expect(String(db.query.mock.calls[0]?.[0])).toContain("INSERT INTO public.nj_candidate_finance_links");
    expect(String(db.query.mock.calls[0]?.[0])).toContain("ON CONFLICT (candidate_id, election_id, candidate_entity_s)");
    expect(db.query.mock.calls[0]?.[1]).toEqual([
      CANDIDATE_ID,
      ELECTION_ID,
      2025,
      "MIKIE SHERRILL",
      "Governor",
      null,
      473742,
      "SHERRILL, MIKIE",
      "G",
      "active",
      "elec_api",
      "https://www.njelecefilesearch.com/api/VWEntity/GetEntityList?LastName=Sherrill",
      "2026-06-25T12:00:00.000Z",
    ]);
  });

  it("defaults optional link metadata safely", async () => {
    const db = createMockDb();

    await expect(
      upsertNewJerseyFinanceLink({
        db,
        link: {
          ...baseLink(),
          district: " 11 ",
          electionTypeCode: " ",
          linkSource: undefined,
          sourceUrl: " ",
          lastVerifiedAt: null,
        },
      })
    ).resolves.toEqual({ linkId: LINK_ID });

    expect(db.query.mock.calls[0]?.[1]).toEqual([
      CANDIDATE_ID,
      ELECTION_ID,
      2025,
      "MIKIE SHERRILL",
      "Governor",
      "11",
      473742,
      "SHERRILL, MIKIE",
      null,
      "active",
      "manual",
      null,
      null,
    ]);
  });

  it("replaces a New Jersey finance snapshot inside one transaction", async () => {
    const db = createTransactionalMockDb();

    const result = await replaceNewJerseyCandidateFinanceSnapshot({
      db,
      link: baseLink(),
      syncedAt: new Date("2026-06-25T13:00:00.000Z"),
      summary: {
        totalReceipts: 350,
        directContributionTotal: 350,
        outsideSupportTotal: 100_082.02,
        outsideOpposeTotal: 0,
        sourceUrl: "https://www.njelecefilesearch.com/SearchContributionToEntity?eid=473742",
      },
      directBreakdowns: [
        {
          categoryType: "occupation",
          categoryName: "Attorney",
          amount: 350,
          contributorCount: 2,
          sourceUrl: "https://www.njelecefilesearch.com/SearchContributionToEntity?eid=473742",
        },
        {
          categoryType: "employer",
          categoryName: "Acme Law",
          amount: 350,
          contributorCount: 2,
          sourceUrl: "https://www.njelecefilesearch.com/SearchContributionToEntity?eid=473742",
        },
      ],
      outsideGroups: [
        {
          outsideEntityS: 477267,
          outsideEntityName: "ONE GIANT LEAP PAC - OGL PAC",
          supportOppose: "support",
          amount: 100_082.02,
          sourceUrl: "https://www.njelecefilesearch.com/SearchIndExpReports/?handler=DownloadReport&DocId=3909738",
        },
      ],
      outsideGroupBreakdowns: [
        {
          outsideEntityS: 477267,
          supportOppose: "support",
          categoryType: "donor",
          categoryName: "Jane Street Capital LLC",
          amount: 100_000,
          contributorCount: 1,
          sourceUrl: "https://www.njelecefilesearch.com/SearchContributionToEntity?eid=477267",
        },
        {
          outsideEntityS: 477267,
          supportOppose: "support",
          categoryType: "industry",
          categoryName: "finance_investment",
          amount: 100_000,
          contributorCount: 1,
          sourceUrl: "https://www.njelecefilesearch.com/SearchContributionToEntity?eid=477267",
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
    expect(sql.some((statement) => statement.includes("INSERT INTO public.nj_candidate_finance_summaries"))).toBe(true);
    expect(sql.filter((statement) => statement.includes("INSERT INTO public.nj_candidate_finance_direct_breakdowns"))).toHaveLength(2);
    expect(sql.filter((statement) => statement.includes("INSERT INTO public.nj_candidate_finance_outside_groups"))).toHaveLength(1);
    expect(sql.filter((statement) => statement.includes("INSERT INTO public.nj_candidate_finance_outside_group_breakdowns"))).toHaveLength(2);
    expect(sql.some((statement) => statement.includes("DELETE FROM public.nj_candidate_finance_direct_breakdowns"))).toBe(true);
    expect(sql.some((statement) => statement.includes("DELETE FROM public.nj_candidate_finance_outside_groups"))).toBe(true);
    expect(sql.some((statement) => statement.includes("DELETE FROM public.nj_candidate_finance_outside_group_breakdowns"))).toBe(true);

    const summaryCall = db.query.mock.calls.find((call) =>
      String(call[0]).includes("INSERT INTO public.nj_candidate_finance_summaries")
    );
    expect(summaryCall?.[1]).toEqual([
      LINK_ID,
      2025,
      350,
      350,
      null,
      null,
      100_082.02,
      0,
      "https://www.njelecefilesearch.com/SearchContributionToEntity?eid=473742",
      "2026-06-25T13:00:00.000Z",
    ]);
  });

  it("rejects query-only and PoolClient-like snapshot writers so writes stay atomic", async () => {
    const queryOnlyDb = { query: vi.fn().mockResolvedValue({ rows: [{ id: LINK_ID }], rowCount: 1 }) };
    await expect(
      replaceNewJerseyCandidateFinanceSnapshot({
        db: queryOnlyDb as never,
        link: baseLink(),
        summary: { totalReceipts: 1000 },
      })
    ).rejects.toThrow("New Jersey finance snapshot writes must receive a Pool");
    expect(queryOnlyDb.query).not.toHaveBeenCalled();

    const clientLikeDb = {
      query: vi.fn().mockResolvedValue({ rows: [{ id: LINK_ID }], rowCount: 1 }),
      release: vi.fn(),
    };
    await expect(
      replaceNewJerseyCandidateFinanceSnapshot({
        db: clientLikeDb as never,
        link: baseLink(),
        summary: { totalReceipts: 1000 },
      })
    ).rejects.toThrow("New Jersey finance snapshot writes must receive a Pool, not a PoolClient");
    expect(clientLikeDb.query).not.toHaveBeenCalled();
  });

  it("rejects outside group breakdowns without same-snapshot outside groups", async () => {
    const db = createTransactionalMockDb();

    await expect(
      replaceNewJerseyCandidateFinanceSnapshot({
        db,
        link: baseLink(),
        outsideGroupBreakdowns: [
          {
            outsideEntityS: 477267,
            supportOppose: "support",
            categoryType: "donor",
            categoryName: "Jane Street Capital LLC",
            amount: 100_000,
          },
        ],
      })
    ).rejects.toThrow("New Jersey outside group breakdowns require outside groups in the same snapshot");
    expect(db.connect).not.toHaveBeenCalled();
  });

  it("rejects outside group breakdowns that do not match a same-snapshot group side", async () => {
    const db = createTransactionalMockDb();

    await expect(
      replaceNewJerseyCandidateFinanceSnapshot({
        db,
        link: baseLink(),
        outsideGroups: [
          {
            outsideEntityS: 477267,
            outsideEntityName: "ONE GIANT LEAP PAC - OGL PAC",
            supportOppose: "support",
            amount: 100_000,
          },
        ],
        outsideGroupBreakdowns: [
          {
            outsideEntityS: 477267,
            supportOppose: "oppose",
            categoryType: "industry",
            categoryName: "finance_investment",
            amount: 100_000,
          },
        ],
      })
    ).rejects.toThrow("New Jersey outside group breakdowns require matching outside groups in the same snapshot");
    expect(db.connect).not.toHaveBeenCalled();
  });
});
