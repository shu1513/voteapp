import { describe, expect, it, vi } from "vitest";

import {
  replaceWisconsinCandidateFinanceSnapshot,
  upsertWisconsinFinanceLink,
} from "../../../src/pipeline/wisconsinFinance/wisconsinFinanceWriter.js";

const CANDIDATE_ID = "11111111-1111-4111-8111-111111111111";
const ELECTION_ID = "22222222-2222-4222-8222-222222222222";
const NOW = new Date("2026-06-01T00:00:00.000Z");

function linkInput() {
  return {
    candidateId: CANDIDATE_ID,
    electionId: ELECTION_ID,
    electionYear: 2026,
    candidateNameNormalized: "TOM TIFFANY",
    officeName: "Governor",
    district: null,
    entityId: "16621",
    committeeId: "407",
    committeeName: "Tiffany for Wisconsin",
    assignedCommitteeId: "0104212",
    linkStatus: "active" as const,
    linkSource: "sunshine_api" as const,
    sourceUrl: "https://campaignfinance.wi.gov/browse-data/registrants/16621",
    lastVerifiedAt: NOW,
  };
}

describe("wisconsinFinanceWriter", () => {
  it("upserts a Wisconsin finance link with Sunshine-native IDs", async () => {
    const db = { query: vi.fn().mockResolvedValue({ rows: [{ id: "link-1" }], rowCount: 1 }) };

    await expect(upsertWisconsinFinanceLink({ db, link: linkInput() })).resolves.toEqual({ linkId: "link-1" });

    expect(String(db.query.mock.calls[0]?.[0])).toContain("INSERT INTO public.wi_candidate_finance_links");
    expect(String(db.query.mock.calls[0]?.[0])).toContain("entity_id");
    expect(String(db.query.mock.calls[0]?.[0])).toContain("assigned_committee_id");
    expect(db.query.mock.calls[0]?.[1]).toEqual([
      CANDIDATE_ID,
      ELECTION_ID,
      2026,
      "TOM TIFFANY",
      "Governor",
      null,
      "16621",
      "407",
      "Tiffany for Wisconsin",
      "0104212",
      "active",
      "sunshine_api",
      "https://campaignfinance.wi.gov/browse-data/registrants/16621",
      "2026-06-01T00:00:00.000Z",
    ]);
  });

  it("transactionally replaces summary and direct breakdowns", async () => {
    const client = {
      query: vi
        .fn()
        .mockResolvedValueOnce({ rows: [], rowCount: 0 })
        .mockResolvedValueOnce({ rows: [{ id: "link-1" }], rowCount: 1 })
        .mockResolvedValue({ rows: [], rowCount: 0 }),
      release: vi.fn(),
    };
    const db = { connect: vi.fn().mockResolvedValue(client), query: vi.fn() };

    await expect(
      replaceWisconsinCandidateFinanceSnapshot({
        db,
        link: linkInput(),
        syncedAt: NOW,
        summary: {
          directContributionTotal: 7500,
          sourceUrl: "https://campaignfinance.wi.gov/browse-data/transactions",
        },
        directBreakdowns: [
          {
            categoryType: "occupation",
            categoryName: "ATTORNEY",
            amount: 7500,
            contributorCount: 3,
            sourceUrl: "https://campaignfinance.wi.gov/browse-data/transactions",
          },
          {
            categoryType: "contribution_size",
            categoryName: "1000_4999",
            amount: 7500,
            contributorCount: 3,
            sourceUrl: "https://campaignfinance.wi.gov/browse-data/transactions",
          },
        ],
      })
    ).resolves.toEqual({
      linkId: "link-1",
      summaryWritten: true,
      directBreakdownsWritten: 2,
      outsideGroupsWritten: 0,
      outsideGroupBreakdownsWritten: 0,
    });

    expect(client.query.mock.calls.map((call) => String(call[0]).trim().split(/\s+/).slice(0, 3).join(" "))).toEqual([
      "BEGIN",
      "INSERT INTO public.wi_candidate_finance_links",
      "INSERT INTO public.wi_candidate_finance_summaries",
      "INSERT INTO public.wi_candidate_finance_direct_breakdowns",
      "INSERT INTO public.wi_candidate_finance_direct_breakdowns",
      "DELETE FROM public.wi_candidate_finance_direct_breakdowns",
      "COMMIT",
    ]);
    expect(client.query.mock.calls.at(-2)?.[1]).toEqual([
      "link-1",
      2026,
      JSON.stringify([
        { category_type: "occupation", category_name: "ATTORNEY" },
        { category_type: "contribution_size", category_name: "1000_4999" },
      ]),
    ]);
    expect(client.release).toHaveBeenCalledTimes(1);
  });

  it("transactionally replaces outside groups when supplied", async () => {
    const client = {
      query: vi
        .fn()
        .mockResolvedValueOnce({ rows: [], rowCount: 0 })
        .mockResolvedValueOnce({ rows: [{ id: "link-1" }], rowCount: 1 })
        .mockResolvedValue({ rows: [], rowCount: 0 }),
      release: vi.fn(),
    };
    const db = { connect: vi.fn().mockResolvedValue(client), query: vi.fn() };

    await expect(
      replaceWisconsinCandidateFinanceSnapshot({
        db,
        link: linkInput(),
        syncedAt: NOW,
        summary: {
          outsideSupportTotal: 175000,
          outsideOpposeTotal: 10000,
          sourceUrl: "https://campaignfinance.wi.gov/browse-data/transactions",
        },
        outsideGroups: [
          {
            sponsorId: "12231502",
            sponsorName: "AMERICANS FOR PROSPERITY",
            supportOppose: "support",
            amount: 175000,
            sourceUrl: "https://campaignfinance.wi.gov/browse-data/transactions",
          },
          {
            sponsorId: "777",
            sponsorName: "OPPOSE PAC",
            supportOppose: "oppose",
            amount: 10000,
            sourceUrl: "https://campaignfinance.wi.gov/browse-data/transactions",
          },
        ],
      })
    ).resolves.toEqual({
      linkId: "link-1",
      summaryWritten: true,
      directBreakdownsWritten: 0,
      outsideGroupsWritten: 2,
      outsideGroupBreakdownsWritten: 0,
    });

    expect(client.query.mock.calls.map((call) => String(call[0]).trim().split(/\s+/).slice(0, 3).join(" "))).toEqual([
      "BEGIN",
      "INSERT INTO public.wi_candidate_finance_links",
      "INSERT INTO public.wi_candidate_finance_summaries",
      "INSERT INTO public.wi_candidate_finance_outside_groups",
      "INSERT INTO public.wi_candidate_finance_outside_groups",
      "DELETE FROM public.wi_candidate_finance_outside_groups",
      "COMMIT",
    ]);
    expect(client.query.mock.calls.at(-2)?.[1]).toEqual([
      "link-1",
      2026,
      JSON.stringify([
        { sponsor_id: "12231502", support_oppose: "support" },
        { sponsor_id: "777", support_oppose: "oppose" },
      ]),
    ]);
    expect(client.release).toHaveBeenCalledTimes(1);
  });

  it("transactionally replaces outside group breakdowns and stores classifications", async () => {
    const client = {
      query: vi
        .fn()
        .mockResolvedValueOnce({ rows: [], rowCount: 0 })
        .mockResolvedValueOnce({ rows: [{ id: "link-1" }], rowCount: 1 })
        .mockResolvedValue({ rows: [], rowCount: 0 }),
      release: vi.fn(),
    };
    const db = { connect: vi.fn().mockResolvedValue(client), query: vi.fn() };

    await expect(
      replaceWisconsinCandidateFinanceSnapshot({
        db,
        link: linkInput(),
        syncedAt: NOW,
        outsideGroups: [
          {
            sponsorId: "12231502",
            sponsorName: "AMERICANS FOR PROSPERITY",
            supportOppose: "support",
            amount: 175000,
            sourceUrl: "https://campaignfinance.wi.gov/browse-data/transactions",
          },
        ],
        outsideGroupBreakdowns: [
          {
            sponsorId: "12231502",
            supportOppose: "support",
            categoryType: "donor",
            categoryName: "Wisconsin Conservation Action",
            amount: 75000,
            contributorCount: 2,
            sourceUrl: "https://campaignfinance.wi.gov/browse-data/transactions",
          },
          {
            sponsorId: "12231502",
            supportOppose: "support",
            categoryType: "industry",
            categoryName: "environmental_group",
            amount: 75000,
            contributorCount: 2,
            sourceUrl: "https://campaignfinance.wi.gov/browse-data/transactions",
          },
        ],
        classifications: [
          {
            rawLabel: "Wisconsin Conservation Action",
            labelType: "donor",
            normalizedLabel: "WISCONSIN CONSERVATION ACTION",
            industrySlug: "environmental_group",
            confidence: "medium",
            classificationSource: "deterministic_rule",
            matchedRule: "organization_pattern_environmental_group",
          },
        ],
      })
    ).resolves.toEqual({
      linkId: "link-1",
      summaryWritten: false,
      directBreakdownsWritten: 0,
      outsideGroupsWritten: 1,
      outsideGroupBreakdownsWritten: 2,
    });

    expect(client.query.mock.calls.map((call) => String(call[0]).trim().split(/\s+/).slice(0, 3).join(" "))).toEqual([
      "BEGIN",
      "INSERT INTO public.wi_candidate_finance_links",
      "INSERT INTO public.wi_candidate_finance_outside_groups",
      "INSERT INTO public.wi_candidate_finance_outside_group_breakdowns",
      "INSERT INTO public.wi_candidate_finance_outside_group_breakdowns",
      "DELETE FROM public.wi_candidate_finance_outside_group_breakdowns",
      "DELETE FROM public.wi_candidate_finance_outside_groups",
      "INSERT INTO public.finance_label_classifications",
      "COMMIT",
    ]);
    expect(client.query.mock.calls.at(-4)?.[1]).toEqual([
      "link-1",
      2026,
      JSON.stringify([
        {
          sponsor_id: "12231502",
          support_oppose: "support",
          category_type: "donor",
          category_name: "Wisconsin Conservation Action",
        },
        {
          sponsor_id: "12231502",
          support_oppose: "support",
          category_type: "industry",
          category_name: "environmental_group",
        },
      ]),
    ]);
    expect(client.release).toHaveBeenCalledTimes(1);
  });

  it("rejects outside group breakdowns without outside groups", async () => {
    const db = { connect: vi.fn(), query: vi.fn() };

    await expect(
      replaceWisconsinCandidateFinanceSnapshot({
        db,
        link: linkInput(),
        syncedAt: NOW,
        outsideGroupBreakdowns: [
          {
            sponsorId: "12231502",
            supportOppose: "support",
            categoryType: "donor",
            categoryName: "Wisconsin Conservation Action",
            amount: 75000,
          },
        ],
      })
    ).rejects.toThrow("Wisconsin outside group breakdowns require outside groups");

    expect(db.connect).not.toHaveBeenCalled();
  });

  it("rolls back and releases the client on write failure", async () => {
    const client = {
      query: vi
        .fn()
        .mockResolvedValueOnce({ rows: [], rowCount: 0 })
        .mockRejectedValueOnce(new Error("link insert failed"))
        .mockResolvedValueOnce({ rows: [], rowCount: 0 }),
      release: vi.fn(),
    };
    const db = { connect: vi.fn().mockResolvedValue(client), query: vi.fn() };

    await expect(
      replaceWisconsinCandidateFinanceSnapshot({
        db,
        link: linkInput(),
        syncedAt: NOW,
      })
    ).rejects.toThrow("link insert failed");

    expect(client.query.mock.calls.map((call) => call[0])).toEqual([
      "BEGIN",
      expect.stringContaining("INSERT INTO public.wi_candidate_finance_links"),
      "ROLLBACK",
    ]);
    expect(client.release).toHaveBeenCalledTimes(1);
  });
});
