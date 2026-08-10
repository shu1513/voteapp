import { describe, expect, it, vi } from "vitest";

import { syncWashingtonCandidateFinance } from "../../../src/pipeline/washingtonFinance/washingtonCandidateFinanceSync.js";
import type { WashingtonCandidateCommitteeResolution } from "../../../src/pipeline/washingtonFinance/washingtonCandidateCommitteeResolver.js";
import type {
  WashingtonPdcAggregate,
  WashingtonPdcCandidateSummary,
  WashingtonPdcIndependentSpendingGroup,
} from "../../../src/pipeline/washingtonFinance/washingtonPdcClient.js";

const CANDIDATE_ID = "11111111-1111-1111-1111-111111111111";
const ELECTION_ID = "22222222-2222-2222-2222-222222222222";
const LINK_ID = "33333333-3333-3333-3333-333333333333";
const PDC_SOURCE_URL = "https://data.wa.gov/resource/3h9x-7bvm.json";
const CONTRIBUTION_SOURCE_URL = "https://data.wa.gov/resource/kv7h-kjye.json";
const IE_SOURCE_URL = "https://data.wa.gov/resource/67cp-h962.json";

function createMockDb() {
  const query = vi.fn(async (sql: string) => {
    if (String(sql).includes("FROM public.finance_label_classifications AS classification")) {
      return { rows: [], rowCount: 0 };
    }
    return { rows: [{ id: LINK_ID }], rowCount: 1 };
  });
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

function matchedResolution(overrides: Partial<Extract<WashingtonCandidateCommitteeResolution, { status: "matched" }>> = {}) {
  return {
    status: "matched" as const,
    filerId: "FERGR *115",
    committeeId: "32311",
    committeeName: "Robert W. Ferguson (Bob Ferguson)",
    candidacyId: "689556",
    contributionsAmount: 11962407.92,
    expendituresAmount: 8000000,
    confidence: "exact" as const,
    source: "pdc_api" as const,
    sourceUrl: PDC_SOURCE_URL,
    matchedSummaryRowCount: 1,
    ...overrides,
  };
}

function sponsorSummary(overrides: Partial<WashingtonPdcCandidateSummary> = {}): WashingtonPdcCandidateSummary {
  return {
    filerId: "FUSEV  147",
    committeeId: "7777",
    filerName: "FUSE VOTES",
    activeCandidate: null,
    hasReports: true,
    electionYear: 2024,
    sourceUrl: PDC_SOURCE_URL,
    ...overrides,
  };
}

function createPdcClient(input: {
  resolution?: WashingtonCandidateCommitteeResolution;
  occupations?: WashingtonPdcAggregate[];
  contributionSizes?: WashingtonPdcAggregate[];
  outsideGroups?: WashingtonPdcIndependentSpendingGroup[];
  sponsorSummaries?: WashingtonPdcCandidateSummary[];
  sponsorFunders?: WashingtonPdcAggregate[];
} = {}) {
  return {
    searchAndResolveCandidateCommittee: vi.fn(async () => input.resolution ?? matchedResolution()),
    getDirectOccupationAggregates: vi.fn(async () =>
      input.occupations ?? [
        {
          categoryName: "ATTORNEY - LAWYER",
          amount: 719187.76,
          count: 1200,
          sourceUrl: CONTRIBUTION_SOURCE_URL,
        },
        {
          categoryName: "RETIRED",
          amount: 992900.26,
          count: 4500,
          sourceUrl: CONTRIBUTION_SOURCE_URL,
        },
      ]
    ),
    getContributionSizeAggregates: vi.fn(async () =>
      input.contributionSizes ?? [
        {
          categoryName: "1000_4999",
          amount: 100000,
          count: 20,
          sourceUrl: CONTRIBUTION_SOURCE_URL,
        },
      ]
    ),
    getIndependentExpenditureGroups: vi.fn(async () =>
      input.outsideGroups ?? [
        {
          sponsorId: "FUSEV  147",
          sponsorName: "FUSE VOTES",
          supportOppose: "support" as const,
          amount: 2457.26,
          expenditureCount: 1,
          sourceUrl: IE_SOURCE_URL,
        },
      ]
    ),
    getSponsorSummaryByName: vi.fn(async () => input.sponsorSummaries ?? [sponsorSummary()]),
    getSponsorOrganizationFunders: vi.fn(async () =>
      input.sponsorFunders ?? [
        {
          categoryName: "Washington Conservation Action Votes",
          amount: 30000,
          count: 1,
          sourceUrl: CONTRIBUTION_SOURCE_URL,
        },
      ]
    ),
  };
}

function baseInput() {
  return {
    candidateId: CANDIDATE_ID,
    electionId: ELECTION_ID,
    candidateName: "Bob Ferguson",
    electionYear: 2024,
    officeScope: "statewide",
    officeName: "Governor",
    sourceUrl: PDC_SOURCE_URL,
    now: new Date("2024-02-03T04:05:06.000Z"),
  };
}

describe("washingtonCandidateFinanceSync", () => {
  it("resolves a candidate committee, fetches PDC finance data, and writes a snapshot", async () => {
    const db = createMockDb();
    const pdcClient = createPdcClient();

    const result = await syncWashingtonCandidateFinance({
      db,
      ...baseInput(),
      pdcClient,
    });

    expect(result).toMatchObject({
      candidateId: CANDIDATE_ID,
      electionId: ELECTION_ID,
      electionYear: 2024,
      dryRun: false,
      linkWritten: true,
      summaryWritten: true,
      directBreakdownsWritten: 3,
      outsideGroupsWritten: 1,
      outsideGroupBreakdownsWritten: 2,
      totalReceipts: 11962407.92,
      directContributionTotal: 11962407.92,
      totalDisbursements: 8000000,
      // Resolution carries no summary IE fields here, so the headline falls
      // back to the fetched group sums.
      outsideSupportTotal: 2457.26,
      outsideOpposeTotal: 0,
      outsideSupportGroupTotal: 2457.26,
      outsideOpposeGroupTotal: 0,
      directOccupationRowCount: 2,
      directContributionSizeRowCount: 1,
      outsideGroupCount: 1,
      outsideFunderRowCount: 1,
      skippedOutsideGroupFunderLookupCount: 0,
      resolution: {
        status: "matched",
        filerId: "FERGR *115",
        committeeId: "32311",
      },
    });

    expect(pdcClient.getDirectOccupationAggregates).toHaveBeenCalledWith(
      expect.objectContaining({ filerId: "FERGR *115", committeeId: "32311", electionYear: 2024 }),
      undefined
    );
    expect(pdcClient.getIndependentExpenditureGroups).toHaveBeenCalledWith(
      expect.objectContaining({
        candidateName: "Bob Ferguson",
        office: "GOVERNOR",
        electionYear: 2024,
        candidateFilerId: "FERGR *115",
        candidateCommitteeId: "32311",
      }),
      undefined
    );

    expect(String(db.query.mock.calls[0]?.[0])).toContain("FROM public.finance_label_classifications");
    expect(db.query.mock.calls[1]?.[0]).toBe("BEGIN");
    expect(db.query.mock.calls.at(-1)?.[0]).toBe("COMMIT");

    const linkCall = db.query.mock.calls.find((call) =>
      String(call[0]).includes("INSERT INTO public.wa_candidate_finance_links")
    );
    expect(linkCall?.[1]).toEqual([
      CANDIDATE_ID,
      ELECTION_ID,
      2024,
      "BOB FERGUSON",
      "Governor",
      null,
      "FERGR *115",
      "32311",
      "Robert W. Ferguson (Bob Ferguson)",
      "689556",
      "active",
      "pdc_api",
      PDC_SOURCE_URL,
      "2024-02-03T04:05:06.000Z",
    ]);

    const summaryCall = db.query.mock.calls.find((call) =>
      String(call[0]).includes("INSERT INTO public.wa_candidate_finance_summaries")
    );
    expect(summaryCall?.[1]).toEqual([
      LINK_ID,
      2024,
      11962407.92,
      11962407.92,
      8000000,
      null,
      2457.26,
      0,
      PDC_SOURCE_URL,
      "2024-02-03T04:05:06.000Z",
    ]);

    expect(
      db.query.mock.calls.filter((call) =>
        String(call[0]).includes("INSERT INTO public.wa_candidate_finance_direct_breakdowns")
      )
    ).toHaveLength(3);
    expect(
      db.query.mock.calls.filter((call) =>
        String(call[0]).includes("INSERT INTO public.wa_candidate_finance_outside_groups")
      )
    ).toHaveLength(1);
    const outsideBreakdownCalls = db.query.mock.calls.filter((call) =>
      String(call[0]).includes("INSERT INTO public.wa_candidate_finance_outside_group_breakdowns")
    );
    expect(outsideBreakdownCalls).toHaveLength(2);
    expect(outsideBreakdownCalls.map((call) => call[1])).toEqual([
      [
        LINK_ID,
        2024,
        "FUSEV  147",
        "support",
        "donor",
        "Washington Conservation Action Votes",
        30000,
        1,
        CONTRIBUTION_SOURCE_URL,
        "2024-02-03T04:05:06.000Z",
      ],
      [
        LINK_ID,
        2024,
        "FUSEV  147",
        "support",
        "industry",
        "environmental_group",
        30000,
        1,
        CONTRIBUTION_SOURCE_URL,
        "2024-02-03T04:05:06.000Z",
      ],
    ]);

    const classificationCall = db.query.mock.calls.find((call) =>
      String(call[0]).includes("INSERT INTO public.finance_label_classifications")
    );
    expect(classificationCall?.[1]).toEqual([
      "Washington Conservation Action Votes",
      "donor",
      "WASHINGTON CONSERVATION ACTION VOTES",
      "environmental_group",
      "medium",
      "rule",
    ]);
  });

  it("takes headline outside totals from the summary IE fields, not the group sums", async () => {
    const db = createMockDb();
    const pdcClient = createPdcClient({
      resolution: matchedResolution({
        // PDC's own Wilson 2025 totals; the single fetched group below is a
        // deliberately incomplete explanation.
        independentExpendituresForAmount: 273026.25,
        independentExpendituresAgainstAmount: 1232834.74,
      }),
    });

    const result = await syncWashingtonCandidateFinance({
      db,
      ...baseInput(),
      pdcClient,
    });

    expect(result).toMatchObject({
      outsideSupportTotal: 273026.25,
      outsideOpposeTotal: 1232834.74,
      outsideSupportGroupTotal: 2457.26,
      outsideOpposeGroupTotal: 0,
    });

    const summaryCall = db.query.mock.calls.find((call) =>
      String(call[0]).includes("INSERT INTO public.wa_candidate_finance_summaries")
    );
    expect(summaryCall?.[1]).toEqual([
      LINK_ID,
      2024,
      11962407.92,
      11962407.92,
      8000000,
      null,
      273026.25,
      1232834.74,
      PDC_SOURCE_URL,
      "2024-02-03T04:05:06.000Z",
    ]);
  });

  it("aggregates but does not write in dry-run mode", async () => {
    const db = createMockDb();
    const pdcClient = createPdcClient();

    const result = await syncWashingtonCandidateFinance({
      db,
      ...baseInput(),
      pdcClient,
      dryRun: true,
    });

    expect(result).toMatchObject({
      dryRun: true,
      linkWritten: false,
      summaryWritten: false,
      directBreakdownsWritten: 0,
      outsideGroupsWritten: 0,
      outsideGroupBreakdownsWritten: 0,
      totalReceipts: 11962407.92,
      outsideSupportTotal: 2457.26,
    });
    expect(db.query).not.toHaveBeenCalled();
  });

  it("does not write when committee resolution is unmatched", async () => {
    const db = createMockDb();
    const pdcClient = createPdcClient({
      resolution: {
        status: "unmatched",
        reason: "no_candidate_committee_match",
        candidateNameNormalized: "BOB FERGUSON",
        officeNameNormalized: "GOVERNOR",
      },
    });

    const result = await syncWashingtonCandidateFinance({
      db,
      ...baseInput(),
      pdcClient,
    });

    expect(result).toMatchObject({
      linkWritten: false,
      summaryWritten: false,
      directBreakdownsWritten: 0,
      outsideGroupsWritten: 0,
      totalReceipts: null,
      resolution: { status: "unmatched", reason: "no_candidate_committee_match" },
    });
    expect(db.query).not.toHaveBeenCalled();
    expect(pdcClient.getDirectOccupationAggregates).not.toHaveBeenCalled();
  });

  it("uses the shared classifier for high-dollar unknown outside organization donors", async () => {
    const db = createMockDb();
    const pdcClient = createPdcClient({
      sponsorSummaries: [sponsorSummary({ filerId: " FUSEV  147 ", committeeId: " 7777 " })],
      sponsorFunders: [
        {
          categoryName: "Evergreen Strategic Holdings",
          amount: 30000,
          count: 1,
          sourceUrl: CONTRIBUTION_SOURCE_URL,
        },
      ],
    });
    const financeIndustryClassifier = vi.fn(async ({ labels }) =>
      labels.map((label) => ({
        rawLabel: label.rawLabel,
        labelType: label.labelType,
        normalizedLabel: label.normalizedLabel,
        industrySlug: "technology" as const,
        confidence: "medium" as const,
        classificationSource: "ai" as const,
        matchedRule: null,
      }))
    );

    const result = await syncWashingtonCandidateFinance({
      db,
      ...baseInput(),
      pdcClient,
      financeIndustryClassifier,
      aiClassificationMinAmount: 25_000,
    });

    expect(result).toMatchObject({
      outsideGroupsWritten: 1,
      outsideGroupBreakdownsWritten: 2,
      outsideFunderRowCount: 1,
    });
    expect(pdcClient.getSponsorOrganizationFunders).toHaveBeenCalledWith(
      expect.objectContaining({
        filerId: "FUSEV  147",
        committeeId: "7777",
      }),
      undefined
    );
    expect(financeIndustryClassifier).toHaveBeenCalledWith({
      labels: [
        expect.objectContaining({
          rawLabel: "Evergreen Strategic Holdings",
          labelType: "donor",
          amount: 30000,
        }),
      ],
    });

    const outsideBreakdownCalls = db.query.mock.calls.filter((call) =>
      String(call[0]).includes("INSERT INTO public.wa_candidate_finance_outside_group_breakdowns")
    );
    expect(outsideBreakdownCalls.map((call) => call[1])).toEqual(
      expect.arrayContaining([
        [
          LINK_ID,
          2024,
          "FUSEV  147",
          "support",
          "industry",
          "technology",
          30000,
          1,
          CONTRIBUTION_SOURCE_URL,
          "2024-02-03T04:05:06.000Z",
        ],
      ])
    );
  });

  it("classifies every funder but caps the persisted donor rows per group", async () => {
    const db = createMockDb();
    const pdcClient = createPdcClient({
      sponsorFunders: [
        { categoryName: "IBEW Local 46", amount: 50_000, count: 1, sourceUrl: CONTRIBUTION_SOURCE_URL },
        { categoryName: "IBEW Local 77", amount: 25_000, count: 1, sourceUrl: CONTRIBUTION_SOURCE_URL },
      ],
    });

    const result = await syncWashingtonCandidateFinance({
      db,
      ...baseInput(),
      pdcClient,
      // Cap of 1: the smaller IBEW funder must be dropped from the WRITTEN
      // donor rows, yet still feed the classifications and the rebuilt
      // labor_unions industry total.
      outsideMaxFundersPerGroup: 1,
    });

    // 1 capped donor row + 1 industry row built from BOTH funders.
    expect(result.outsideGroupBreakdownsWritten).toBe(2);
    expect(result.outsideFunderRowCount).toBe(2);
    // No limit: funders are fetched uncapped so every one feeds classification.
    expect(pdcClient.getSponsorOrganizationFunders).toHaveBeenCalledWith(
      { filerId: "FUSEV  147", committeeId: "7777", electionYear: 2024 },
      undefined
    );
    const breakdownInsertParams = db.query.mock.calls
      .filter((call) => String(call[0]).includes("wa_candidate_finance_outside_group_breakdowns"))
      .flatMap((call) => (Array.isArray(call[1]) ? call[1] : []));
    expect(breakdownInsertParams).toContain("IBEW Local 46");
    expect(breakdownInsertParams).not.toContain("IBEW Local 77");
    // The rebuilt industry total covers the dropped funder too.
    expect(breakdownInsertParams).toContain("labor_unions");
    expect(breakdownInsertParams).toContain(75_000);
    // Both funders persisted classification rows.
    const classificationParams = db.query.mock.calls
      .filter((call) => String(call[0]).includes("INSERT INTO public.finance_label_classifications"))
      .flatMap((call) => (Array.isArray(call[1]) ? call[1] : []));
    expect(classificationParams).toContain("IBEW Local 46");
    expect(classificationParams).toContain("IBEW Local 77");
  });

  it("skips outside funder backtrace when sponsor resolution is ambiguous", async () => {
    const db = createMockDb();
    const pdcClient = createPdcClient({
      sponsorSummaries: [
        sponsorSummary({ filerId: "FUSEV  147", committeeId: "7777" }),
        sponsorSummary({ filerId: "FUSEV  148", committeeId: "7778" }),
      ],
    });

    const result = await syncWashingtonCandidateFinance({
      db,
      ...baseInput(),
      pdcClient,
    });

    expect(result).toMatchObject({
      outsideGroupsWritten: 1,
      outsideGroupBreakdownsWritten: 0,
      outsideFunderRowCount: 0,
      skippedOutsideGroupFunderLookupCount: 1,
    });
    expect(pdcClient.getSponsorOrganizationFunders).not.toHaveBeenCalled();
  });
});
