import { describe, expect, it } from "vitest";
import { aggregateLosAngelesDirectContributions } from "../../../src/pipeline/losAngelesCityFinance/losAngelesDirectContributionAggregator.js";
import type { LosAngelesContributionRecord } from "../../../src/pipeline/losAngelesCityFinance/losAngelesOpenDataClient.js";

const row = (
  overrides: Partial<LosAngelesContributionRecord>,
): LosAngelesContributionRecord => ({
  contributionDate: "2026-01-01",
  contributorName: "Donor",
  occupation: "Teacher",
  employer: "LAUSD",
  committeeName: "Committee",
  committeeId: "1471359",
  candidateName: "Bass, Karen",
  seatDescription: "Mayor",
  contributionType: "Monetary Contributions (Itemized)",
  amount: 100,
  amountPaidOrForgiven: 0,
  schedule: "A",
  periodEndDate: "2026-05-27",
  electionDate: "2026-06-02",
  ...overrides,
});

describe("aggregateLosAngelesDirectContributions", () => {
  it("excludes Schedule I and nets repaid/forgiven loans", () => {
    const result = aggregateLosAngelesDirectContributions({
      records: [
        row({ amount: 500 }),
        row({ schedule: "B", amount: 1_000, amountPaidOrForgiven: 300 }),
        row({ amount: -100 }),
        row({ schedule: "I", amount: 9_999 }),
      ],
    });
    expect(result.reconciledContributionTotal).toBe(1_100);
    expect(result.includedRowCount).toBe(3);
    expect(result.skippedRowCount).toBe(1);
    expect(
      result.breakdowns.find(
        (item) =>
          item.categoryType === "contribution_size" &&
          item.categoryName === "$500-$999",
      )?.amount,
    ).toBe(1_200);
  });
});
