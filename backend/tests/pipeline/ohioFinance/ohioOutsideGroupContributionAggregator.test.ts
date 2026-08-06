import { describe, expect, it } from "vitest";

import { aggregateOhioOutsideGroupContributions } from "../../../src/pipeline/ohioFinance/ohioOutsideGroupContributionAggregator.js";
import type { OhioFinanceOutsideGroup } from "../../../src/pipeline/ohioFinance/ohioOutsideSpendingAggregator.js";
import type { OhioSosContributionRow } from "../../../src/pipeline/ohioFinance/ohioSosBulkFiles.js";

function group(overrides: Partial<OhioFinanceOutsideGroup> = {}): OhioFinanceOutsideGroup {
  return {
    committeeId: "16182",
    committeeName: "V-PAC VICTORS NOT VICTIMS (SUPER PAC)",
    supportOppose: "oppose",
    amount: 8_211_114.5,
    sourceUrl: "https://example.test/ohio",
    ...overrides,
  };
}

function contributionRow(overrides: Partial<OhioSosContributionRow> = {}): OhioSosContributionRow {
  return {
    committeeName: "V-PAC VICTORS NOT VICTIMS (SUPER PAC)",
    masterKey: "16182",
    reportYear: 2026,
    reportKey: "500000001",
    reportDescription: "PRE-PRIMARY",
    shortDescription: "31-A Stmt of Contribution",
    contributorFirstName: null,
    contributorMiddleName: null,
    contributorLastName: null,
    contributorSuffix: null,
    nonIndividual: "ACME INDUSTRIES LLC",
    pacRegNo: null,
    address: "1 MAIN ST",
    city: "COLUMBUS",
    state: "OH",
    zip: "43215",
    fileDateIso: "2026-03-01",
    amountCents: 100_000_00,
    eventDateIso: null,
    empOccupation: null,
    inkindDescription: null,
    otherIncomeType: null,
    rcvEvent: null,
    candidateFirstName: null,
    candidateLastName: null,
    office: null,
    district: null,
    party: null,
    ...overrides,
  };
}

describe("aggregateOhioOutsideGroupContributions", () => {
  it("aggregates organization donors per committee and direction", () => {
    const result = aggregateOhioOutsideGroupContributions({
      electionYear: 2026,
      outsideGroups: [group()],
      contributionRows: [
        contributionRow({ amountCents: 60_000_00 }),
        contributionRow({ amountCents: 40_000_00 }),
        contributionRow({ nonIndividual: "BUCKEYE UNION PAC", amountCents: 5_000_00 }),
      ],
      sourceUrl: "https://example.test/ohio",
    });

    expect(result.matchedContributionRowCount).toBe(3);
    expect(result.includedContributionRowCount).toBe(3);
    expect(result.skippedContributionRowCount).toBe(0);
    const donors = result.outsideGroupBreakdowns.filter((breakdown) => breakdown.categoryType === "donor");
    expect(donors).toEqual([
      {
        committeeId: "16182",
        supportOppose: "oppose",
        categoryType: "donor",
        categoryName: "ACME INDUSTRIES LLC",
        amount: 100_000,
        contributorCount: 1,
        sourceUrl: "https://example.test/ohio",
      },
      {
        committeeId: "16182",
        supportOppose: "oppose",
        categoryType: "donor",
        categoryName: "BUCKEYE UNION PAC",
        amount: 5_000,
        contributorCount: 1,
        sourceUrl: "https://example.test/ohio",
      },
    ]);
  });

  it("projects a committee's donors onto every direction the committee spent in", () => {
    const result = aggregateOhioOutsideGroupContributions({
      electionYear: 2026,
      outsideGroups: [
        group({ supportOppose: "support" }),
        group({ supportOppose: "oppose" }),
      ],
      contributionRows: [contributionRow()],
    });

    const donors = result.outsideGroupBreakdowns.filter((breakdown) => breakdown.categoryType === "donor");
    expect(donors.map((donor) => donor.supportOppose).sort()).toEqual(["oppose", "support"]);
    // One physical row, projected — not double-counted in the row counters.
    expect(result.includedContributionRowCount).toBe(1);
  });

  it("skips individual contributions without a NON_INDIVIDUAL value", () => {
    const result = aggregateOhioOutsideGroupContributions({
      electionYear: 2026,
      outsideGroups: [group()],
      contributionRows: [
        contributionRow({
          nonIndividual: null,
          contributorFirstName: "PAT",
          contributorLastName: "SMITH",
        }),
      ],
    });

    expect(result.matchedContributionRowCount).toBe(1);
    expect(result.includedContributionRowCount).toBe(0);
    expect(result.individualRowCount).toBe(1);
    expect(result.outsideGroupBreakdowns).toEqual([]);
  });

  it("rejects transaction-description labels but keeps entity-bearing dues labels", () => {
    const result = aggregateOhioOutsideGroupContributions({
      electionYear: 2026,
      outsideGroups: [group()],
      contributionRows: [
        // Pure descriptions — no entity to name (real 2026-cycle values).
        contributionRow({ nonIndividual: "CONTRIBUTION FROM DUES MONEY" }),
        contributionRow({ nonIndividual: "TRANSFER OF MEMBERSHIP DUES" }),
        contributionRow({ nonIndividual: "SOLELY FROM MEMBERSHIP DUES" }),
        contributionRow({ nonIndividual: "PROCEEDS FROM DUES FUNDS" }),
        // Entity-bearing values keep their verbatim label.
        contributionRow({ nonIndividual: "OHIO AFL-CIO/DUES MONEY", amountCents: 10_000_00 }),
        contributionRow({ nonIndividual: "UAW REGION 2B MEMBERSHIP DUES", amountCents: 5_000_00 }),
      ],
    });

    expect(result.nonEntityLabelRowCount).toBe(4);
    expect(result.includedContributionRowCount).toBe(2);
    const donors = result.outsideGroupBreakdowns.filter((breakdown) => breakdown.categoryType === "donor");
    expect(donors.map((donor) => donor.categoryName)).toEqual([
      "OHIO AFL-CIO/DUES MONEY",
      "UAW REGION 2B MEMBERSHIP DUES",
    ]);
  });

  it("fails closed on the contribution vocabulary", () => {
    const result = aggregateOhioOutsideGroupContributions({
      electionYear: 2026,
      outsideGroups: [group()],
      contributionRows: [
        contributionRow({ shortDescription: "31-A-2 Other Income" }),
        contributionRow({ shortDescription: "31-CC St. Political Party Rest. Fund" }),
        contributionRow({ shortDescription: "31-Z SOMETHING NEW" }),
      ],
    });

    expect(result.includedContributionRowCount).toBe(0);
    expect(result.excludedShortDescriptionRowCount).toBe(2);
    expect(result.unknownShortDescriptionRowCount).toBe(1);
    expect(result.outsideGroupBreakdowns).toEqual([]);
  });

  it("skips missing/non-positive amounts and out-of-cycle report years", () => {
    const result = aggregateOhioOutsideGroupContributions({
      electionYear: 2026,
      outsideGroups: [group()],
      contributionRows: [
        contributionRow({ amountCents: null }),
        contributionRow({ amountCents: -5_00 }),
        contributionRow({ reportYear: 2024 }),
        contributionRow({ reportYear: null }),
      ],
    });

    expect(result.matchedContributionRowCount).toBe(4);
    expect(result.unusableRowCount).toBe(4);
    expect(result.outsideGroupBreakdowns).toEqual([]);
  });

  it("ignores rows for committees outside the candidate's groups", () => {
    const result = aggregateOhioOutsideGroupContributions({
      electionYear: 2026,
      outsideGroups: [group()],
      contributionRows: [contributionRow({ masterKey: "99999" })],
    });

    expect(result.matchedContributionRowCount).toBe(0);
    expect(result.outsideGroupBreakdowns).toEqual([]);
  });

  it("rolls rule-classifiable donors into industry breakdowns", () => {
    const result = aggregateOhioOutsideGroupContributions({
      electionYear: 2026,
      outsideGroups: [group()],
      contributionRows: [
        // The rule classifier maps IBEW to labor_unions.
        contributionRow({ nonIndividual: "IBEW LOCAL 540", amountCents: 50_000_00 }),
        contributionRow({ nonIndividual: "IBEW LOCAL 8", amountCents: 25_000_00 }),
      ],
    });

    const industries = result.outsideGroupBreakdowns.filter(
      (breakdown) => breakdown.categoryType === "industry"
    );
    expect(industries).toEqual([
      {
        committeeId: "16182",
        supportOppose: "oppose",
        categoryType: "industry",
        categoryName: "labor_unions",
        amount: 75_000,
        contributorCount: 2,
        sourceUrl: null,
      },
    ]);
  });

  it("keeps donors below minIndustryAmount out of the industry rollup", () => {
    const result = aggregateOhioOutsideGroupContributions({
      electionYear: 2026,
      outsideGroups: [group()],
      contributionRows: [contributionRow({ nonIndividual: "IBEW LOCAL 540", amountCents: 100_00 })],
      minIndustryAmount: 1_000,
    });

    expect(
      result.outsideGroupBreakdowns.filter((breakdown) => breakdown.categoryType === "industry")
    ).toEqual([]);
    expect(
      result.outsideGroupBreakdowns.filter((breakdown) => breakdown.categoryType === "donor")
    ).toHaveLength(1);
  });

  it("caps donors per group at maxBreakdownsPerCategory by amount", () => {
    const rows = Array.from({ length: 4 }, (_, index) =>
      contributionRow({
        nonIndividual: `DONOR ${index} LLC`,
        amountCents: (index + 1) * 1_000_00,
      })
    );
    const result = aggregateOhioOutsideGroupContributions({
      electionYear: 2026,
      outsideGroups: [group()],
      contributionRows: rows,
      maxBreakdownsPerCategory: 2,
    });

    const donors = result.outsideGroupBreakdowns.filter((breakdown) => breakdown.categoryType === "donor");
    expect(donors.map((donor) => donor.categoryName)).toEqual(["DONOR 3 LLC", "DONOR 2 LLC"]);
  });

  it("returns empty result when the candidate has no valid outside groups", () => {
    const result = aggregateOhioOutsideGroupContributions({
      electionYear: 2026,
      outsideGroups: [group({ committeeId: "not-numeric" })],
      contributionRows: [contributionRow()],
    });

    expect(result.matchedContributionRowCount).toBe(0);
    expect(result.outsideGroupBreakdowns).toEqual([]);
  });
});
