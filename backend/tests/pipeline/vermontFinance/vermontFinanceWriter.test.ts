import { describe, expect, it, vi } from "vitest";

import {
  replaceVermontCandidateFinanceSnapshot,
  upsertVermontFinanceLink,
  type VermontFinanceDirectBreakdownInput,
} from "../../../src/pipeline/vermontFinance/vermontFinanceWriter.js";

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
    electionYear: 2024,
    candidateNameNormalized: "PHIL SCOTT",
    officeName: "Governor",
    district: null,
    filerRegistrationGuid: "f174929e-e5ba-4e8a-ab5b-54661c3c5c88",
    entityId: 33545,
    filerName: "SCOTT, PHIL",
    linkSource: "vermont_public_transactions" as const,
    sourceUrl: "https://campaignfinance.vermont.gov/",
    lastVerifiedAt: new Date("2026-06-01T00:00:00.000Z"),
  };
}

describe("vermontFinanceWriter", () => {
  it("upserts Vermont finance links with filer guid and entity id", async () => {
    const db = createMockDb();

    await expect(upsertVermontFinanceLink({ db, link: baseLink() })).resolves.toEqual({ linkId: LINK_ID });

    expect(db.query).toHaveBeenCalledTimes(1);
    expect(String(db.query.mock.calls[0]?.[0])).toContain("INSERT INTO public.vt_candidate_finance_links");
    expect(String(db.query.mock.calls[0]?.[0])).toContain(
      "ON CONFLICT (candidate_id, election_id, filer_registration_guid)"
    );
    expect(db.query.mock.calls[0]?.[1]).toEqual([
      CANDIDATE_ID,
      ELECTION_ID,
      2024,
      "PHIL SCOTT",
      "Governor",
      null,
      "f174929e-e5ba-4e8a-ab5b-54661c3c5c88",
      33545,
      "SCOTT, PHIL",
      "active",
      "vermont_public_transactions",
      "https://campaignfinance.vermont.gov/",
      "2026-06-01T00:00:00.000Z",
    ]);
  });

  it("defaults optional link metadata safely", async () => {
    const db = createMockDb();

    await expect(
      upsertVermontFinanceLink({
        db,
        link: {
          ...baseLink(),
          district: " District 1 ",
          entityId: null,
          linkSource: undefined,
          sourceUrl: " ",
          lastVerifiedAt: null,
        },
      })
    ).resolves.toEqual({ linkId: LINK_ID });

    expect(db.query.mock.calls[0]?.[1]).toEqual([
      CANDIDATE_ID,
      ELECTION_ID,
      2024,
      "PHIL SCOTT",
      "Governor",
      "District 1",
      "f174929e-e5ba-4e8a-ab5b-54661c3c5c88",
      null,
      "SCOTT, PHIL",
      "active",
      "manual",
      null,
      null,
    ]);
  });

  it("replaces a Vermont finance snapshot inside one transaction", async () => {
    const db = createTransactionalMockDb();

    const result = await replaceVermontCandidateFinanceSnapshot({
      db,
      link: baseLink(),
      syncedAt: new Date("2026-06-02T03:04:05.000Z"),
      summary: {
        totalReceipts: 5000,
        directContributionTotal: 4500,
        outsideSupportTotal: 1000,
        outsideOpposeTotal: 0,
        sourceUrl: "https://campaignfinance.vermont.gov/",
      },
      directBreakdowns: [
        {
          categoryType: "contribution_size",
          categoryName: "$1,000-$4,999",
          amount: 3000,
          contributorCount: 2,
          sourceUrl: "https://campaignfinance.vermont.gov/",
        },
        {
          categoryType: "contributor_source_type",
          categoryName: "Individual",
          amount: 1500,
          contributorCount: 3,
          sourceUrl: "https://campaignfinance.vermont.gov/",
        },
      ],
      outsideGroups: [
        {
          filerRegistrationGuid: "pac-guid",
          filerName: "VERMONT FUTURE PAC",
          supportOppose: "support",
          supportMechanism: "vt_pac_contribution_to_registrant",
          amount: 1000,
          sourceUrl: "https://campaignfinance.vermont.gov/",
        },
      ],
      outsideGroupBreakdowns: [
        {
          filerRegistrationGuid: "pac-guid",
          supportOppose: "support",
          categoryType: "donor",
          categoryName: "Sierra Club",
          amount: 1000,
          contributorCount: 1,
          sourceUrl: "https://campaignfinance.vermont.gov/",
        },
        {
          filerRegistrationGuid: "pac-guid",
          supportOppose: "support",
          categoryType: "industry",
          categoryName: "environmental_group",
          amount: 1000,
          contributorCount: 1,
          sourceUrl: "https://campaignfinance.vermont.gov/",
        },
      ],
      classifications: [
        {
          rawLabel: "Sierra Club",
          labelType: "donor",
          normalizedLabel: "SIERRA CLUB",
          industrySlug: "environmental_group",
          confidence: "high",
          classificationSource: "rule",
          matchedRule: "organization_exact_sierra_club",
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
    expect(sql.some((statement) => statement.includes("INSERT INTO public.vt_candidate_finance_summaries"))).toBe(true);
    expect(sql.filter((statement) => statement.includes("INSERT INTO public.vt_candidate_finance_direct_breakdowns"))).toHaveLength(2);
    expect(sql.filter((statement) => statement.includes("INSERT INTO public.vt_candidate_finance_outside_groups"))).toHaveLength(1);
    expect(sql.filter((statement) => statement.includes("INSERT INTO public.vt_candidate_finance_outside_group_breakdowns"))).toHaveLength(2);
    expect(sql.filter((statement) => statement.includes("INSERT INTO public.finance_label_classifications"))).toHaveLength(1);
    expect(sql.some((statement) => statement.includes("DELETE FROM public.vt_candidate_finance_direct_breakdowns"))).toBe(true);
    expect(sql.some((statement) => statement.includes("DELETE FROM public.vt_candidate_finance_outside_groups"))).toBe(true);
    expect(sql.some((statement) => statement.includes("DELETE FROM public.vt_candidate_finance_outside_group_breakdowns"))).toBe(true);

    const outsideGroupCall = db.query.mock.calls.find((call) =>
      String(call[0]).includes("INSERT INTO public.vt_candidate_finance_outside_groups")
    );
    expect(outsideGroupCall?.[1]).toEqual([
      LINK_ID,
      2024,
      "pac-guid",
      "VERMONT FUTURE PAC",
      "support",
      "vt_pac_contribution_to_registrant",
      1000,
      "https://campaignfinance.vermont.gov/",
      "2026-06-02T03:04:05.000Z",
    ]);
  });

  it("rejects non-Pool snapshot writers and outside breakdowns without same-snapshot outside groups", async () => {
    const queryOnlyDb = { query: vi.fn().mockResolvedValue({ rows: [{ id: LINK_ID }], rowCount: 1 }) };
    await expect(
      replaceVermontCandidateFinanceSnapshot({
        db: queryOnlyDb as never,
        link: baseLink(),
        summary: { totalReceipts: 1000 },
      })
    ).rejects.toThrow("Vermont finance snapshot writes must receive a Pool");
    expect(queryOnlyDb.query).not.toHaveBeenCalled();

    const clientLikeDb = {
      query: vi.fn().mockResolvedValue({ rows: [{ id: LINK_ID }], rowCount: 1 }),
      release: vi.fn(),
    };
    await expect(
      replaceVermontCandidateFinanceSnapshot({
        db: clientLikeDb as never,
        link: baseLink(),
        summary: { totalReceipts: 1000 },
      })
    ).rejects.toThrow("Vermont finance snapshot writes must receive a Pool, not a PoolClient");
    expect(clientLikeDb.query).not.toHaveBeenCalled();

    const db = createTransactionalMockDb();
    await expect(
      replaceVermontCandidateFinanceSnapshot({
        db,
        link: baseLink(),
        outsideGroupBreakdowns: [
          {
            filerRegistrationGuid: "pac-guid",
            supportOppose: "support",
            categoryType: "donor",
            categoryName: "Sierra Club",
            amount: 1000,
          },
        ],
      })
    ).rejects.toThrow("Vermont outside group breakdowns require outside groups in the same snapshot");
    expect(db.connect).not.toHaveBeenCalled();
  });

  it("rejects occupation as a Vermont direct breakdown category", async () => {
    const db = createTransactionalMockDb();
    const badBreakdown = {
      categoryType: "occupation",
      categoryName: "Attorney",
      amount: 100,
    } as unknown as VermontFinanceDirectBreakdownInput;

    await expect(
      replaceVermontCandidateFinanceSnapshot({
        db,
        link: baseLink(),
        directBreakdowns: [badBreakdown],
      })
    ).rejects.toThrow("Unsupported Vermont direct breakdown category type: occupation");
    expect(db.connect).not.toHaveBeenCalled();
  });
});
