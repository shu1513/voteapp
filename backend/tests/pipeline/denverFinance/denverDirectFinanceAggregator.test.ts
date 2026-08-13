import { describe, expect, it } from "vitest";

import { aggregateDenverDirectContributions } from "../../../src/pipeline/denverFinance/denverDirectFinanceAggregator.js";
import type { DenverContributionTransaction } from "../../../src/pipeline/denverFinance/denverSearchlightClient.js";

let nextTransactionId = 1;
function row(
  overrides: Partial<DenverContributionTransaction> = {},
): DenverContributionTransaction {
  return {
    transactionId: nextTransactionId++,
    transactionSubType: "Monetary",
    recipientName: "Mike Johnston",
    recipientCommitteeName: "Johnston for Denver",
    recipientCommitteeId: 807,
    officeSought: "Mayor",
    district: null,
    contributorName: "A Donor",
    contributorId: 1,
    amountCents: 10_000,
    date: "2023-01-15T07:00:00",
    contributorEmployer: null,
    contributorOccupation: null,
    contributorCity: "Denver",
    contributorStateCode: "CO",
    contactTypeId: 1,
    txnPurpose: null,
    fefTransaction: false,
    ...overrides,
  };
}

const ENTITY_IDS = [641, 807];

describe("aggregateDenverDirectContributions", () => {
  it("applies the fixture-pinned inclusion matrix with signed netting", () => {
    const result = aggregateDenverDirectContributions({
      rows: [
        row({ amountCents: 10_000, contributorOccupation: "Teacher" }),
        row({
          transactionSubType: "In-Kind",
          amountCents: 6_000,
          contributorOccupation: "Lawyer",
        }),
        // Refund: nets the occupation, never lands in a size bucket.
        row({
          amountCents: -1_000,
          contributorOccupation: "Teacher",
          txnPurpose: "Overlimit",
        }),
        // FEF city money: counted separately, never bucketed.
        row({
          transactionSubType: "Fair Elections Payments",
          amountCents: 5_000,
          contributorName: "Denver Fair Elections Fund Disbursement",
        }),
        // Foreign entity id: someone else's committee — dropped.
        row({ recipientCommitteeId: 999, amountCents: 77_700 }),
      ],
      committeeEntityIds: ENTITY_IDS,
    });
    expect(result.directContributionCents).toBe(15_000);
    expect(result.fefFundingCents).toBe(5_000);
    expect(result.includedRowCount).toBe(4);
    expect(result.entityFilteredRowCount).toBe(1);
    expect(result.breakdowns).toEqual([
      {
        categoryType: "occupation",
        categoryName: "Teacher",
        amountCents: 9_000,
        contributorCount: 1,
      },
      {
        categoryType: "occupation",
        categoryName: "Lawyer",
        amountCents: 6_000,
        contributorCount: 1,
      },
      {
        categoryType: "contribution_size",
        categoryName: "$100-$249",
        amountCents: 10_000,
        contributorCount: 1,
      },
      {
        categoryType: "contribution_size",
        categoryName: "$1-$99",
        amountCents: 6_000,
        contributorCount: 1,
      },
    ]);
  });

  it("buckets at the LA boundaries in cents", () => {
    const result = aggregateDenverDirectContributions({
      rows: [
        row({ amountCents: 9_999 }),
        row({ amountCents: 10_000 }),
        row({ amountCents: 49_999 }),
        row({ amountCents: 50_000 }),
        row({ amountCents: 500_000 }),
      ],
      committeeEntityIds: ENTITY_IDS,
    });
    const sizes = result.breakdowns.filter(
      (entry) => entry.categoryType === "contribution_size",
    );
    expect(new Map(sizes.map((entry) => [entry.categoryName, entry.amountCents])))
      .toEqual(
        new Map([
          ["$1-$99", 9_999],
          ["$100-$249", 10_000],
          ["$250-$499", 49_999],
          ["$500-$999", 50_000],
          ["$5,000+", 500_000],
        ]),
      );
  });

  it("merges occupations case-insensitively, keeps first-seen spelling, and drops net-negative rows from output", () => {
    const result = aggregateDenverDirectContributions({
      rows: [
        row({ amountCents: 5_000, contributorOccupation: "Retired  Teacher" }),
        row({ amountCents: 2_000, contributorOccupation: "RETIRED TEACHER" }),
        row({ amountCents: 4_000, contributorOccupation: "Nurse" }),
        row({ amountCents: -6_000, contributorOccupation: "Nurse" }),
        row({ amountCents: 1_000, contributorOccupation: null }),
      ],
      committeeEntityIds: ENTITY_IDS,
    });
    const occupations = result.breakdowns.filter(
      (entry) => entry.categoryType === "occupation",
    );
    expect(occupations).toEqual([
      {
        categoryType: "occupation",
        categoryName: "Retired Teacher",
        amountCents: 7_000,
        contributorCount: 2,
      },
    ]);
  });

  it("caps occupations at the limit, largest first", () => {
    const rows = Array.from({ length: 4 }, (_, index) =>
      row({
        amountCents: (index + 1) * 1_000,
        contributorOccupation: `Occupation ${index}`,
      }),
    );
    const result = aggregateDenverDirectContributions({
      rows,
      committeeEntityIds: ENTITY_IDS,
      maxOccupationBreakdowns: 2,
    });
    const occupations = result.breakdowns.filter(
      (entry) => entry.categoryType === "occupation",
    );
    expect(occupations.map((entry) => entry.categoryName)).toEqual([
      "Occupation 3",
      "Occupation 2",
    ]);
  });

  it("tracks Loan rows separately and never buckets them", () => {
    const result = aggregateDenverDirectContributions({
      rows: [
        row({ amountCents: 10_000, contributorOccupation: "Teacher" }),
        // Walker-shaped: candidate self-loan, occupation present — must not
        // enter occupation or size buckets, only loanCents.
        row({
          transactionSubType: "Loan",
          amountCents: 2_500,
          contributorOccupation: "Candidate",
        }),
      ],
      committeeEntityIds: ENTITY_IDS,
    });
    expect(result.directContributionCents).toBe(10_000);
    expect(result.loanCents).toBe(2_500);
    expect(result.includedRowCount).toBe(2);
    expect(result.breakdowns).toEqual([
      {
        categoryType: "occupation",
        categoryName: "Teacher",
        amountCents: 10_000,
        contributorCount: 1,
      },
      {
        categoryType: "contribution_size",
        categoryName: "$100-$249",
        amountCents: 10_000,
        contributorCount: 1,
      },
    ]);
  });

  it("rejects a non-positive occupation breakdown limit", () => {
    expect(() =>
      aggregateDenverDirectContributions({
        rows: [],
        committeeEntityIds: ENTITY_IDS,
        maxOccupationBreakdowns: 0,
      }),
    ).toThrow(/Invalid Denver occupation breakdown limit: 0/);
  });

  it("fails closed on an unproven transaction subtype", () => {
    expect(() =>
      aggregateDenverDirectContributions({
        rows: [row({ transactionSubType: "Loan Received" })],
        committeeEntityIds: ENTITY_IDS,
      }),
    ).toThrow(/unproven transaction subtypes: Loan Received/);
  });

  it("requires committee entity ids", () => {
    expect(() =>
      aggregateDenverDirectContributions({ rows: [], committeeEntityIds: [] }),
    ).toThrow(/requires the filer's committee entity ids/);
  });
});
