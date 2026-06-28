import { describe, expect, it, vi } from "vitest";

import {
  replaceIllinoisCandidateFinanceSnapshot,
  upsertIllinoisFinanceLink,
} from "../../../src/pipeline/illinoisFinance/illinoisFinanceWriter.js";

const CANDIDATE_ID = "11111111-1111-4111-8111-111111111111";
const ELECTION_ID = "22222222-2222-4222-8222-222222222222";
const LINK_ID = "33333333-3333-4333-8333-333333333333";
const SOURCE_URL = "https://www.elections.il.gov/CampaignDisclosure/ContributionSearchByCandidates.aspx";

function baseLink() {
  return {
    candidateId: CANDIDATE_ID,
    electionId: ELECTION_ID,
    electionYear: 2022,
    candidateNameNormalized: "JANE DOE",
    officeName: "Governor",
    district: " ",
    committeeKey: " friends  of jane ",
    committeeName: "Friends of Jane",
    linkStatus: "active" as const,
    linkSource: "illinois_sbe" as const,
    sourceUrl: SOURCE_URL,
    lastVerifiedAt: new Date("2022-06-01T00:00:00.000Z"),
  };
}

function createMockDb() {
  const client = {
    query: vi.fn().mockResolvedValue({ rows: [{ id: LINK_ID }], rowCount: 1 }),
    release: vi.fn(),
  };
  return {
    query: vi.fn().mockResolvedValue({ rows: [{ id: LINK_ID }], rowCount: 1 }),
    connect: vi.fn().mockResolvedValue(client),
    client,
  };
}

describe("illinoisFinanceWriter", () => {
  it("upserts Illinois finance links with normalized nullable fields", async () => {
    const db = {
      query: vi.fn().mockResolvedValue({ rows: [{ id: LINK_ID }], rowCount: 1 }),
    };

    await expect(upsertIllinoisFinanceLink({ db, link: baseLink() })).resolves.toEqual({ linkId: LINK_ID });

    expect(String(db.query.mock.calls[0]?.[0])).toContain("INSERT INTO public.il_candidate_finance_links");
    expect(String(db.query.mock.calls[0]?.[0])).toContain("ON CONFLICT (candidate_id, election_id, committee_key)");
    expect(String(db.query.mock.calls[0]?.[0])).toContain("WHEN il_candidate_finance_links.link_source = 'manual'");
    expect(String(db.query.mock.calls[0]?.[0])).toContain("THEN il_candidate_finance_links.source_url");
    expect(String(db.query.mock.calls[0]?.[0])).toContain("THEN il_candidate_finance_links.last_verified_at");
    expect(db.query.mock.calls[0]?.[1]).toEqual([
      CANDIDATE_ID,
      ELECTION_ID,
      2022,
      "JANE DOE",
      "Governor",
      null,
      "FRIENDS OF JANE",
      "Friends of Jane",
      "active",
      "illinois_sbe",
      SOURCE_URL,
      "2022-06-01T00:00:00.000Z",
    ]);
  });

  it("replaces an Illinois finance snapshot inside a transaction", async () => {
    const db = createMockDb();

    const result = await replaceIllinoisCandidateFinanceSnapshot({
      db,
      link: baseLink(),
      syncedAt: new Date("2022-07-08T09:10:11.000Z"),
      summary: {
        totalReceipts: 120000,
        directContributionTotal: 120000,
        outsideSupportTotal: 35000,
        outsideOpposeTotal: 5000,
        sourceUrl: SOURCE_URL,
      },
      directBreakdowns: [
        {
          categoryType: "occupation",
          categoryName: "Attorney",
          amount: 90000,
          contributorCount: 12,
          sourceUrl: SOURCE_URL,
        },
      ],
      outsideGroups: [
        {
          committeeKey: " illinois conservation action ",
          committeeName: "Illinois Conservation Action",
          supportOppose: "support",
          amount: 35000,
          sourceUrl: SOURCE_URL,
        },
      ],
      outsideGroupBreakdowns: [
        {
          committeeKey: "ILLINOIS CONSERVATION ACTION",
          supportOppose: "support",
          categoryType: "industry",
          categoryName: "environmental_group",
          amount: 35000,
          contributorCount: 1,
          sourceUrl: SOURCE_URL,
        },
      ],
    });

    expect(result).toEqual({
      linkId: LINK_ID,
      summaryWritten: true,
      directBreakdownsWritten: 1,
      outsideGroupsWritten: 1,
      outsideGroupBreakdownsWritten: 1,
    });
    expect(db.connect).toHaveBeenCalledTimes(1);
    expect(db.client.query.mock.calls[0]?.[0]).toBe("BEGIN");
    expect(db.client.query.mock.calls.at(-1)?.[0]).toBe("COMMIT");
    expect(db.client.release).toHaveBeenCalledTimes(1);

    const sql = db.client.query.mock.calls.map((call) => String(call[0]));
    expect(sql.some((statement) => statement.includes("INSERT INTO public.il_candidate_finance_summaries"))).toBe(true);
    expect(sql.some((statement) => statement.includes("total_receipts = EXCLUDED.total_receipts"))).toBe(true);
    expect(
      sql.some(
        (statement) =>
          statement.includes("outside_support_total = COALESCE(") &&
          statement.includes("il_candidate_finance_summaries.outside_support_total") &&
          statement.includes("outside_oppose_total = COALESCE(") &&
          statement.includes("il_candidate_finance_summaries.outside_oppose_total")
      )
    ).toBe(true);
    expect(sql.filter((statement) => statement.includes("INSERT INTO public.il_candidate_finance_direct_breakdowns"))).toHaveLength(1);
    expect(sql.filter((statement) => statement.includes("INSERT INTO public.il_candidate_finance_outside_groups"))).toHaveLength(1);
    expect(sql.filter((statement) => statement.includes("INSERT INTO public.il_candidate_finance_outside_group_breakdowns"))).toHaveLength(1);
    expect(sql.some((statement) => statement.includes("DELETE FROM public.il_candidate_finance_direct_breakdowns"))).toBe(true);
    expect(sql.some((statement) => statement.includes("DELETE FROM public.il_candidate_finance_outside_groups"))).toBe(true);
    expect(sql.some((statement) => statement.includes("DELETE FROM public.il_candidate_finance_outside_group_breakdowns"))).toBe(true);

    const outsideGroupCall = db.client.query.mock.calls.find((call) =>
      String(call[0]).includes("INSERT INTO public.il_candidate_finance_outside_groups")
    );
    const outsideBreakdownCall = db.client.query.mock.calls.find((call) =>
      String(call[0]).includes("INSERT INTO public.il_candidate_finance_outside_group_breakdowns")
    );
    expect(outsideGroupCall?.[1]?.[2]).toBe("ILLINOIS CONSERVATION ACTION");
    expect(outsideBreakdownCall?.[1]?.[2]).toBe("ILLINOIS CONSERVATION ACTION");
  });

  it("rejects outside group breakdowns without matching outside groups", async () => {
    const db = createMockDb();

    await expect(
      replaceIllinoisCandidateFinanceSnapshot({
        db,
        link: baseLink(),
        outsideGroups: [],
        outsideGroupBreakdowns: [
          {
            committeeKey: "ILLINOIS CONSERVATION ACTION",
            supportOppose: "support",
            categoryType: "donor",
            categoryName: "Sierra Club",
            amount: 1000,
          },
        ],
      })
    ).rejects.toThrow("Illinois outside group breakdowns require outside groups");
    expect(db.connect).not.toHaveBeenCalled();
  });
});
