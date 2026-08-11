import { describe, expect, it, vi } from "vitest";

import {
  replaceMassachusettsCandidateFinanceSnapshot,
  upsertMassachusettsFinanceLink,
} from "../../../src/pipeline/massachusettsFinance/massachusettsFinanceWriter.js";

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
    electionYear: 2022,
    candidateNameNormalized: "MAURA HEALEY",
    officeName: "Governor",
    district: null,
    candidateCpfId: "15710",
    filerName: "Healey, Maura T.",
    committeeName: "Healey Committee",
    linkSource: "ocpf_api" as const,
    sourceUrl: "https://api.ocpf.us/filers/listings/A?searchPhrase=Maura%20Healey",
    lastVerifiedAt: new Date("2026-06-01T00:00:00.000Z"),
  };
}

describe("massachusettsFinanceWriter", () => {
  it("accepts signed cash on hand but rejects negative flow totals", async () => {
    const db = createTransactionalMockDb();

    await replaceMassachusettsCandidateFinanceSnapshot({
      db,
      link: baseLink(),
      syncedAt: new Date("2026-06-01T00:00:00.000Z"),
      summary: { totalReceipts: 100, totalDisbursements: 50, cashOnHand: -786.78 },
    });
    const summaryCall = db.query.mock.calls.find((call) =>
      String(call[0]).includes("INSERT INTO public.ma_candidate_finance_summaries")
    );
    expect(summaryCall?.[1]?.[5]).toBe(-786.78);

    await expect(
      replaceMassachusettsCandidateFinanceSnapshot({
        db: createTransactionalMockDb(),
        link: baseLink(),
        syncedAt: new Date("2026-06-01T00:00:00.000Z"),
        summary: { totalReceipts: -1 },
      })
    ).rejects.toThrow("total receipts must be a nonnegative number");
  });

  it("upserts Massachusetts finance links and returns the link id", async () => {
    const db = createMockDb();

    await expect(upsertMassachusettsFinanceLink({ db, link: baseLink() })).resolves.toEqual({ linkId: LINK_ID });

    expect(db.query).toHaveBeenCalledTimes(1);
    expect(String(db.query.mock.calls[0]?.[0])).toContain("INSERT INTO public.ma_candidate_finance_links");
    expect(String(db.query.mock.calls[0]?.[0])).toContain("ON CONFLICT (candidate_id, election_id, candidate_cpf_id)");
    expect(db.query.mock.calls[0]?.[1]).toEqual([
      CANDIDATE_ID,
      ELECTION_ID,
      2022,
      "MAURA HEALEY",
      "Governor",
      null,
      "15710",
      "Healey, Maura T.",
      "Healey Committee",
      "active",
      "ocpf_api",
      "https://api.ocpf.us/filers/listings/A?searchPhrase=Maura%20Healey",
      "2026-06-01T00:00:00.000Z",
    ]);
  });

  it("defaults optional link metadata safely", async () => {
    const db = createMockDb();

    await expect(
      upsertMassachusettsFinanceLink({
        db,
        link: {
          ...baseLink(),
          district: " 2nd Bristol & Plymouth ",
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
      "MAURA HEALEY",
      "Governor",
      "2nd Bristol & Plymouth",
      "15710",
      "Healey, Maura T.",
      "Healey Committee",
      "active",
      "manual",
      null,
      null,
    ]);
  });

  it("replaces a Massachusetts finance snapshot inside one transaction", async () => {
    const db = createTransactionalMockDb();

    const result = await replaceMassachusettsCandidateFinanceSnapshot({
      db,
      link: baseLink(),
      syncedAt: new Date("2026-06-02T03:04:05.000Z"),
      summary: {
        totalReceipts: 5_610_826.29,
        directContributionTotal: 5_080_033.4,
        outsideSupportTotal: 32_420,
        outsideOpposeTotal: 70_000,
        sourceUrl: "https://api.ocpf.us/search/items?cpfId=15710",
      },
      directBreakdowns: [
        {
          categoryType: "occupation",
          categoryName: "Attorney",
          amount: 498_559.34,
          contributorCount: 1200,
          sourceUrl: "https://api.ocpf.us/search/items?cpfId=15710",
        },
        {
          categoryType: "contribution_size",
          categoryName: "$1,000-$4,999",
          amount: 2_669_000,
          contributorCount: 900,
          sourceUrl: "https://api.ocpf.us/search/items?cpfId=15710",
        },
      ],
      outsideGroups: [
        {
          iepacCpfId: "81068",
          iepacName: "Local 103 IBEW IE PAC",
          supportOppose: "support",
          amount: 32_420,
          sourceUrl: "https://www.ocpf.us/Reports/DisplayReport?menuHidden=true&id=858575",
        },
      ],
      outsideGroupBreakdowns: [
        {
          iepacCpfId: "81068",
          supportOppose: "support",
          categoryType: "donor",
          categoryName: "IBEW 103",
          amount: 32_420,
          contributorCount: 1,
          sourceUrl: "https://www.ocpf.us/Reports/DisplayReport?menuHidden=true&id=858575",
        },
        {
          iepacCpfId: "81068",
          supportOppose: "support",
          categoryType: "industry",
          categoryName: "labor_unions",
          amount: 32_420,
          contributorCount: 1,
          sourceUrl: "https://www.ocpf.us/Reports/DisplayReport?menuHidden=true&id=858575",
        },
      ],
      classifications: [
        {
          rawLabel: "IBEW 103",
          labelType: "donor",
          normalizedLabel: "IBEW 103",
          industrySlug: "labor_unions",
          confidence: "medium",
          classificationSource: "rule",
          matchedRule: "organization_pattern_labor_unions",
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
    expect(sql.some((statement) => statement.includes("INSERT INTO public.ma_candidate_finance_summaries"))).toBe(true);
    expect(sql.filter((statement) => statement.includes("INSERT INTO public.ma_candidate_finance_direct_breakdowns"))).toHaveLength(2);
    expect(sql.filter((statement) => statement.includes("INSERT INTO public.ma_candidate_finance_outside_groups"))).toHaveLength(1);
    expect(sql.filter((statement) => statement.includes("INSERT INTO public.ma_candidate_finance_outside_group_breakdowns"))).toHaveLength(2);
    expect(sql.filter((statement) => statement.includes("INSERT INTO public.finance_label_classifications"))).toHaveLength(1);
    expect(sql.some((statement) => statement.includes("DELETE FROM public.ma_candidate_finance_direct_breakdowns"))).toBe(true);
    expect(sql.some((statement) => statement.includes("DELETE FROM public.ma_candidate_finance_outside_groups"))).toBe(true);
    expect(sql.some((statement) => statement.includes("DELETE FROM public.ma_candidate_finance_outside_group_breakdowns"))).toBe(true);

    const summaryCall = db.query.mock.calls.find((call) =>
      String(call[0]).includes("INSERT INTO public.ma_candidate_finance_summaries")
    );
    expect(String(summaryCall?.[0])).toContain("total_receipts = EXCLUDED.total_receipts");
    expect(String(summaryCall?.[0])).toContain("direct_contribution_total = EXCLUDED.direct_contribution_total");
    expect(String(summaryCall?.[0])).toContain("source_url = EXCLUDED.source_url");
    expect(String(summaryCall?.[0])).toContain("outside_support_total = EXCLUDED.outside_support_total");
    expect(summaryCall?.[1]).toEqual([
      LINK_ID,
      2022,
      5_610_826.29,
      5_080_033.4,
      null,
      null,
      32_420,
      70_000,
      "https://api.ocpf.us/search/items?cpfId=15710",
      "2026-06-02T03:04:05.000Z",
    ]);
  });

  it("rejects query-only and PoolClient-like snapshot writers so writes stay atomic", async () => {
    const queryOnlyDb = { query: vi.fn().mockResolvedValue({ rows: [{ id: LINK_ID }], rowCount: 1 }) };
    await expect(
      replaceMassachusettsCandidateFinanceSnapshot({
        db: queryOnlyDb as never,
        link: baseLink(),
        summary: { totalReceipts: 1000 },
      })
    ).rejects.toThrow("Massachusetts finance snapshot writes must receive a Pool");
    expect(queryOnlyDb.query).not.toHaveBeenCalled();

    const clientLikeDb = {
      query: vi.fn().mockResolvedValue({ rows: [{ id: LINK_ID }], rowCount: 1 }),
      release: vi.fn(),
    };
    await expect(
      replaceMassachusettsCandidateFinanceSnapshot({
        db: clientLikeDb as never,
        link: baseLink(),
        summary: { totalReceipts: 1000 },
      })
    ).rejects.toThrow("Massachusetts finance snapshot writes must receive a Pool, not a PoolClient");
    expect(clientLikeDb.query).not.toHaveBeenCalled();
  });

  it("rejects outside group breakdowns without same-snapshot outside groups", async () => {
    const db = createTransactionalMockDb();

    await expect(
      replaceMassachusettsCandidateFinanceSnapshot({
        db,
        link: baseLink(),
        outsideGroupBreakdowns: [
          {
            iepacCpfId: "81068",
            supportOppose: "support",
            categoryType: "donor",
            categoryName: "IBEW 103",
            amount: 32_420,
          },
        ],
      })
    ).rejects.toThrow("Massachusetts outside group breakdowns require outside groups in the same snapshot");
    expect(db.connect).not.toHaveBeenCalled();
  });
});
