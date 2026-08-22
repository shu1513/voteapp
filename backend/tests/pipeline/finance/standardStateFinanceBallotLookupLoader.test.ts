import { describe, expect, it, vi } from "vitest";

import { loadStandardStateFinanceSummariesByCandidateElection } from "../../../src/pipeline/finance/standardStateFinanceBallotLookupLoader.js";

const CANDIDATE_ID = "11111111-1111-4111-8111-111111111111";
const ELECTION_ID = "22222222-2222-4222-8222-222222222222";

const TABLES = {
  links: "wa_candidate_finance_links",
  summaries: "wa_candidate_finance_summaries",
  directBreakdowns: "wa_candidate_finance_direct_breakdowns",
  outsideGroups: "wa_candidate_finance_outside_groups",
  outsideGroupBreakdowns: "wa_candidate_finance_outside_group_breakdowns",
} as const;

type LoadOverrides = Partial<Parameters<typeof loadStandardStateFinanceSummariesByCandidateElection>[0]>;

async function captureQueries(overrides: LoadOverrides): Promise<string[]> {
  const queries: string[] = [];
  const query = vi.fn(async (sql: string) => {
    queries.push(sql);
    return { rows: queries.length === 1 ? [{ candidate_id: CANDIDATE_ID, election_id: ELECTION_ID }] : [] };
  });

  await loadStandardStateFinanceSummariesByCandidateElection({
    db: { query },
    candidateRows: [{ candidate_id: CANDIDATE_ID, election_id: ELECTION_ID }],
    electionRows: [{ election_id: ELECTION_ID, state: "WA" }],
    state: "WA",
    source: "WASHINGTON_PDC" as never,
    sourceUrl: "https://www.pdc.wa.gov/",
    enabled: () => true,
    tables: TABLES,
    ...overrides,
  });
  return queries;
}

describe("standardStateFinanceBallotLookupLoader identity descriptor", () => {
  it("swaps only the outside relations under outsideGroupIdentityColumns", async () => {
    const queries = await captureQueries({
      outsideGroupIdentityColumns: { id: "sponsor_id", name: "sponsor_name" },
    });
    expect(queries).toHaveLength(5);
    const [summary, direct, outsideGroup, outsideIndustry, donorEvidence] = queries as [
      string,
      string,
      string,
      string,
      string,
    ];

    // Link identity stays canonical in the summary query.
    expect(summary).toContain("count(DISTINCT link.committee_id)");
    expect(summary).toContain("min(link.committee_id)");
    expect(summary).not.toContain("sponsor_id");
    expect(direct).not.toContain("sponsor_id");

    // Outside-group query: sponsor columns behind the canonical output aliases.
    expect(outsideGroup).toContain("outside_group.sponsor_id AS committee_id");
    expect(outsideGroup).toContain("min(outside_group.sponsor_name) AS committee_name");
    expect(outsideGroup).toContain("outside_group.sponsor_id, outside_group.support_oppose");
    expect(outsideGroup).not.toContain("outside_group.committee_id");
    expect(outsideGroup).not.toContain("outside_group.committee_name");

    // Outside-industry query groups per sponsor before summing per industry.
    expect(outsideIndustry).toContain("breakdown.sponsor_id");
    expect(outsideIndustry).not.toContain("breakdown.committee_id");

    // Donor-evidence query: every breakdown/industry/outside_group identity
    // site swaps, including the name fallback and the pairing join.
    expect(donorEvidence).toContain("industry.sponsor_id AS committee_id");
    expect(donorEvidence).toContain("breakdown.sponsor_id AS committee_id");
    expect(donorEvidence).toContain("COALESCE(outside_group.sponsor_name, breakdown.sponsor_id) AS committee_name");
    expect(donorEvidence).toContain("outside_group.sponsor_id = breakdown.sponsor_id");
    expect(donorEvidence).toContain("breakdown.category_name ASC, breakdown.sponsor_id ASC");
    expect(donorEvidence).not.toContain("breakdown.committee_id");
    expect(donorEvidence).not.toContain("outside_group.committee_name");
  });

  it("swaps only the summary query under linkIdentityColumn", async () => {
    const queries = await captureQueries({ linkIdentityColumn: "candidate_filer_id" });
    const [summary, , outsideGroup, , donorEvidence] = queries as [string, string, string, string, string];

    expect(summary).toContain("count(DISTINCT link.candidate_filer_id)");
    expect(summary).toContain("min(link.candidate_filer_id)");
    expect(summary).not.toContain("link.committee_id");

    expect(outsideGroup).toContain("outside_group.committee_id AS committee_id");
    expect(donorEvidence).toContain("outside_group.committee_id = breakdown.committee_id");
    expect(outsideGroup).not.toContain("candidate_filer_id");
    expect(donorEvidence).not.toContain("candidate_filer_id");
  });

  it("applies linkIdentityColumn to the illinoisD2 aggregate guards", async () => {
    const queries = await captureQueries({
      linkIdentityColumn: "candidate_filer_id",
      summaryVariant: "illinoisD2",
    });
    const summary = queries[0]!;
    const guards = summary.match(/count\(DISTINCT link\.candidate_filer_id\)/g) ?? [];
    expect(guards.length).toBe(6);
    expect(summary).not.toContain("link.committee_id");
  });

  it("keeps committeeColumn as the default for both new options", async () => {
    const queries = await captureQueries({ committeeColumn: "committee_key" });
    const [summary, , outsideGroup] = queries as [string, string, string];
    expect(summary).toContain("count(DISTINCT link.committee_key)");
    expect(outsideGroup).toContain("outside_group.committee_key AS committee_id");
    // Name column has no committeeColumn tie — stays canonical.
    expect(outsideGroup).toContain("min(outside_group.committee_name) AS committee_name");
  });

  it.each([
    ["linkIdentityColumn", { linkIdentityColumn: "bad;drop" }, "link"],
    ["outsideGroupIdentityColumns.id", { outsideGroupIdentityColumns: { id: "Bad-Column" } }, "outside-group"],
    ["outsideGroupIdentityColumns.name", { outsideGroupIdentityColumns: { name: "name--" } }, "outside-group name"],
  ] as const)("rejects invalid identifiers in %s", async (_label, overrides, kind) => {
    await expect(captureQueries(overrides as LoadOverrides)).rejects.toThrow(
      `Invalid standard finance ${kind} identity column`
    );
  });

  it("selects the funding columns only for a source that opts in", async () => {
    const [optedOut] = (await captureQueries({})) as [string];
    expect(optedOut).not.toContain("loans_received");
    expect(optedOut).not.toContain("public_funds_received");

    const [optedIn] = (await captureQueries({
      fundingColumns: ["loans_received", "public_funds_received"],
    })) as [string];
    expect(optedIn).toContain("sum(summary.loans_received) END AS loans_received");
    expect(optedIn).toContain("sum(summary.public_funds_received) END AS public_funds_received");
  });

  it("rejects a funding column that is not part of the contract", async () => {
    await expect(
      captureQueries({ fundingColumns: ["debts_owed" as never] })
    ).rejects.toThrow(/Invalid standard finance funding columns: debts_owed/);
  });

  it("narrows the direct-breakdown filter under directBreakdownCategoryTypes", async () => {
    const queries = await captureQueries({ directBreakdownCategoryTypes: ["contribution_size"] });
    const [, direct, ...rest] = queries as [string, string, string, string, string];

    expect(direct).toContain("breakdown.category_type IN ('contribution_size')");
    expect(direct).not.toContain("'occupation'");
    // Only the direct-breakdown query changes; the evidence label filter is a
    // separate option and stays at its default.
    for (const sql of rest) {
      expect(sql).not.toContain("IN ('contribution_size')");
    }
  });

  it("keeps the default direct-breakdown filter byte-identical", async () => {
    const queries = await captureQueries({});
    expect(queries[1]).toContain("WHERE breakdown.category_type IN ('occupation', 'contribution_size')");
  });

  it.each([
    ["empty list", { directBreakdownCategoryTypes: [] }],
    ["unknown type", { directBreakdownCategoryTypes: ["employer"] }],
  ])("rejects %s direct category types", async (_label, overrides) => {
    await expect(captureQueries(overrides as unknown as LoadOverrides)).rejects.toThrow(
      "Invalid standard finance direct category types"
    );
  });

  it("routes direct industry rows to industries, never occupations", async () => {
    let calls = 0;
    const query = vi.fn(async () => {
      calls += 1;
      if (calls === 1) {
        return { rows: [{ candidate_id: CANDIDATE_ID, election_id: ELECTION_ID, election_year: 2026 }] };
      }
      if (calls === 2) {
        return {
          rows: [
            {
              candidate_id: CANDIDATE_ID,
              election_id: ELECTION_ID,
              category_type: "industry",
              category_name: "healthcare",
              amount: "1200",
              contributor_count: "3",
              source_url: null,
            },
          ],
        };
      }
      return { rows: [] };
    });

    const result = await loadStandardStateFinanceSummariesByCandidateElection({
      db: { query },
      candidateRows: [{ candidate_id: CANDIDATE_ID, election_id: ELECTION_ID }],
      electionRows: [{ election_id: ELECTION_ID, state: "NH" }],
      state: "NH",
      source: "NEW_HAMPSHIRE_CFS",
      sourceUrl: "https://cfs.sos.nh.gov/",
      enabled: () => true,
      directBreakdownCategoryTypes: ["industry", "contribution_size"],
      tables: TABLES,
    });

    const summary = result.get(`${CANDIDATE_ID}\u0000${ELECTION_ID}`);
    expect(summary?.direct_campaign.top_industries).toEqual([
      {
        category_name: "healthcare",
        amount: 1200,
        contributor_count: 3,
        source_url: "https://cfs.sos.nh.gov/",
      },
    ]);
    expect(summary?.direct_campaign.top_occupations).toEqual([]);
    expect(summary?.backing_summary.top_direct_donor_occupations).toEqual([]);
  });

  it("exempts contribution_size from the direct-breakdown top-5 cap, and only there", async () => {
    const queries = await captureQueries({});
    const [, direct, ...rest] = queries as [string, string, string, string, string];

    // All six size buckets must survive ranking; occupation keeps top-5.
    expect(direct).toContain("WHERE rn <= 5 OR category_type = 'contribution_size'");
    // The outside-spending queries are genuine top-N lists — no exemption.
    for (const sql of rest) {
      expect(sql).not.toContain("OR category_type = 'contribution_size'");
    }
  });

  it("returns all six contribution-size buckets when a candidate populates every one", async () => {
    // The Georgia-derived bucket scheme every shared-loader state clones —
    // six buckets, so a top-5 cap would silently drop the smallest one.
    // Rows arrive in the query's amount-DESC order.
    const bucketRows = [
      { category_name: "$5,000+", amount: "60000" },
      { category_name: "$1,000-$4,999", amount: "25000" },
      { category_name: "$500-$999", amount: "9000" },
      { category_name: "$250-$499", amount: "4000" },
      { category_name: "$100-$249", amount: "1500" },
      { category_name: "$1-$99", amount: "300" },
    ];
    let calls = 0;
    const query = vi.fn(async () => {
      calls += 1;
      if (calls === 1) {
        return { rows: [{ candidate_id: CANDIDATE_ID, election_id: ELECTION_ID, election_year: 2026 }] };
      }
      if (calls === 2) {
        return {
          rows: bucketRows.map((row) => ({
            candidate_id: CANDIDATE_ID,
            election_id: ELECTION_ID,
            category_type: "contribution_size",
            contributor_count: null,
            source_url: null,
            ...row,
          })),
        };
      }
      return { rows: [] };
    });

    const result = await loadStandardStateFinanceSummariesByCandidateElection({
      db: { query },
      candidateRows: [{ candidate_id: CANDIDATE_ID, election_id: ELECTION_ID }],
      electionRows: [{ election_id: ELECTION_ID, state: "WA" }],
      state: "WA",
      source: "WASHINGTON_PDC" as never,
      sourceUrl: "https://www.pdc.wa.gov/",
      enabled: () => true,
      tables: TABLES,
    });

    const summary = result.get(`${CANDIDATE_ID}\u0000${ELECTION_ID}`);
    expect(summary?.direct_campaign.contribution_size_buckets.map((bucket) => bucket.category_name)).toEqual(
      bucketRows.map((row) => row.category_name)
    );
  });

  it.each([
    [undefined, "independent spending supporting this candidate"],
    ["PAC contributions supporting this candidate", "PAC contributions supporting this candidate"],
  ])("outsideSupportActionLabel %s puts %s into the explanation", async (label, expectedAction) => {
    let calls = 0;
    const query = vi.fn(async () => {
      calls += 1;
      if (calls === 1) {
        return { rows: [{ candidate_id: CANDIDATE_ID, election_id: ELECTION_ID, election_year: 2026 }] };
      }
      if (calls === 4) {
        return {
          rows: [
            {
              candidate_id: CANDIDATE_ID,
              election_id: ELECTION_ID,
              support_oppose: "support",
              category_name: "energy",
              amount: "800",
              contributor_count: null,
              source_url: null,
            },
          ],
        };
      }
      return { rows: [] };
    });

    const result = await loadStandardStateFinanceSummariesByCandidateElection({
      db: { query },
      candidateRows: [{ candidate_id: CANDIDATE_ID, election_id: ELECTION_ID }],
      electionRows: [{ election_id: ELECTION_ID, state: "WA" }],
      state: "WA",
      source: "WASHINGTON_PDC" as never,
      sourceUrl: "https://www.pdc.wa.gov/",
      enabled: () => true,
      tables: TABLES,
      ...(label ? { outsideSupportActionLabel: label } : {}),
    });

    const summary = result.get(`${CANDIDATE_ID}\u0000${ELECTION_ID}`);
    const explanation = summary?.backing_summary.top_outside_supporting_industries[0]?.explanation;
    expect(explanation).toContain(expectedAction);
  });
});
