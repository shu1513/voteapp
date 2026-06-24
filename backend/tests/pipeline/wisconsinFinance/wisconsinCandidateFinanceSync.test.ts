import { describe, expect, it, vi } from "vitest";

import { syncWisconsinCandidateFinance } from "../../../src/pipeline/wisconsinFinance/wisconsinCandidateFinanceSync.js";

const CANDIDATE_ID = "11111111-1111-4111-8111-111111111111";
const ELECTION_ID = "22222222-2222-4222-8222-222222222222";
const NOW = new Date("2026-06-01T00:00:00.000Z");

function mockPool() {
  const client = {
    query: vi
      .fn()
      .mockResolvedValueOnce({ rows: [], rowCount: 0 })
      .mockResolvedValueOnce({ rows: [{ id: "link-1" }], rowCount: 1 })
      .mockResolvedValue({ rows: [], rowCount: 0 }),
    release: vi.fn(),
  };
  return {
    db: {
      query: vi.fn().mockResolvedValue({ rows: [], rowCount: 0 }),
      connect: vi.fn().mockResolvedValue(client),
    },
    client,
  };
}

describe("wisconsinCandidateFinanceSync", () => {
  it("returns an empty result and does not write when the resolver is unmatched", async () => {
    const { db } = mockPool();
    const searchAndResolveCandidateCommittee = vi.fn().mockResolvedValue({
      status: "unmatched",
      reason: "no_candidate_committee_match",
      candidateNameNormalized: "TOM TIFFANY",
      officeNameNormalized: "GOVERNOR",
    });

    await expect(
      syncWisconsinCandidateFinance({
        db,
        candidateId: CANDIDATE_ID,
        electionId: ELECTION_ID,
        candidateName: "Tom Tiffany",
        electionYear: 2026,
        officeScope: "statewide",
        officeName: "Governor",
        now: NOW,
        sunshineClient: { searchAndResolveCandidateCommittee },
      })
    ).resolves.toMatchObject({
      candidateId: CANDIDATE_ID,
      electionId: ELECTION_ID,
      electionYear: 2026,
      linkWritten: false,
      summaryWritten: false,
      directBreakdownsWritten: 0,
      outsideGroupsWritten: 0,
      outsideGroupBreakdownsWritten: 0,
      resolution: { status: "unmatched" },
    });

    expect(db.connect).not.toHaveBeenCalled();
  });

  it("uses a trusted committee and computes dry-run counts without writing", async () => {
    const { db } = mockPool();
    const getDirectOccupationAggregates = vi.fn().mockResolvedValue([
      { categoryName: "ATTORNEY", amount: 5000, count: 2 },
    ]);
    const getContributionSizeAggregates = vi.fn().mockResolvedValue([
      { categoryName: "1000_4999", amount: 5000, count: 2 },
    ]);
    const getIndependentExpenditureGroups = vi.fn().mockResolvedValue([
      {
        sponsorId: "12231502",
        sponsorName: "AMERICANS FOR PROSPERITY",
        supportOppose: "support",
        amount: 175000,
        expenditureCount: 2,
      },
    ]);
    const getOutsideSpenderOrganizationFunders = vi.fn().mockResolvedValue([
      { categoryName: "Wisconsin Conservation Action", amount: 50000, count: 1 },
    ]);

    await expect(
      syncWisconsinCandidateFinance({
        db,
        candidateId: CANDIDATE_ID,
        electionId: ELECTION_ID,
        candidateName: "Tom Tiffany",
        electionYear: 2026,
        officeScope: "statewide",
        officeName: "Governor",
        now: NOW,
        dryRun: true,
        trustedCommittee: {
          entityId: "16621",
          committeeId: "407",
          committeeName: "Tiffany for Wisconsin",
          assignedCommitteeId: "0104212",
        },
        sunshineClient: {
          getDirectOccupationAggregates,
          getContributionSizeAggregates,
          getIndependentExpenditureGroups,
          getOutsideSpenderOrganizationFunders,
        },
      })
    ).resolves.toMatchObject({
      dryRun: true,
      linkWritten: false,
      summaryWritten: false,
      directBreakdownsWritten: 0,
      outsideGroupsWritten: 0,
      outsideGroupBreakdownsWritten: 0,
      directContributionTotal: 5000,
      outsideSupportTotal: 175000,
      outsideOpposeTotal: 0,
      directOccupationRowCount: 1,
      directContributionSizeRowCount: 1,
      outsideGroupCount: 1,
      outsideFunderRowCount: 1,
    });

    expect(db.connect).not.toHaveBeenCalled();
    expect(getIndependentExpenditureGroups).toHaveBeenCalledWith(
      {
        candidateCommitteeName: "Tiffany for Wisconsin",
        electionYear: 2026,
        office: "Governor",
        district: undefined,
        limit: 20,
      },
      undefined
    );
  });

  it("writes a combined direct and outside finance snapshot", async () => {
    const { db, client } = mockPool();
    const getDirectOccupationAggregates = vi.fn().mockResolvedValue([
      { categoryName: "ATTORNEY", amount: 5000, count: 2 },
    ]);
    const getContributionSizeAggregates = vi.fn().mockResolvedValue([
      { categoryName: "1000_4999", amount: 5000, count: 2 },
    ]);
    const getIndependentExpenditureGroups = vi.fn().mockResolvedValue([
      {
        sponsorId: "12231502",
        sponsorName: "AMERICANS FOR PROSPERITY",
        supportOppose: "support",
        amount: 175000,
        expenditureCount: 2,
      },
    ]);
    const getOutsideSpenderOrganizationFunders = vi.fn().mockResolvedValue([
      { categoryName: "Wisconsin Conservation Action", amount: 50000, count: 1 },
    ]);

    await expect(
      syncWisconsinCandidateFinance({
        db,
        candidateId: CANDIDATE_ID,
        electionId: ELECTION_ID,
        candidateName: "Tom Tiffany",
        electionYear: 2026,
        officeScope: "statewide",
        officeName: "Governor",
        now: NOW,
        trustedCommittee: {
          entityId: "16621",
          committeeId: "407",
          committeeName: "Tiffany for Wisconsin",
          assignedCommitteeId: "0104212",
        },
        sunshineClient: {
          getDirectOccupationAggregates,
          getContributionSizeAggregates,
          getIndependentExpenditureGroups,
          getOutsideSpenderOrganizationFunders,
        },
      })
    ).resolves.toMatchObject({
      dryRun: false,
      linkWritten: true,
      summaryWritten: true,
      directBreakdownsWritten: 2,
      outsideGroupsWritten: 1,
      outsideGroupBreakdownsWritten: 2,
      directContributionTotal: 5000,
      outsideSupportTotal: 175000,
      outsideOpposeTotal: 0,
    });

    expect(db.query).not.toHaveBeenCalled();
    expect(client.query.mock.calls.map((call) => String(call[0]).trim().split(/\s+/).slice(0, 3).join(" "))).toEqual([
      "BEGIN",
      "INSERT INTO public.wi_candidate_finance_links",
      "INSERT INTO public.wi_candidate_finance_summaries",
      "INSERT INTO public.wi_candidate_finance_direct_breakdowns",
      "INSERT INTO public.wi_candidate_finance_direct_breakdowns",
      "DELETE FROM public.wi_candidate_finance_direct_breakdowns",
      "INSERT INTO public.wi_candidate_finance_outside_groups",
      "INSERT INTO public.wi_candidate_finance_outside_group_breakdowns",
      "INSERT INTO public.wi_candidate_finance_outside_group_breakdowns",
      "DELETE FROM public.wi_candidate_finance_outside_group_breakdowns",
      "DELETE FROM public.wi_candidate_finance_outside_groups",
      "INSERT INTO public.finance_label_classifications",
      "COMMIT",
    ]);
    expect(client.release).toHaveBeenCalledTimes(1);
  });
});
