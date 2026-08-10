import { describe, expect, it } from "vitest";

import { aggregateNorthCarolinaOutsideGroupContributions } from "../../../src/pipeline/northCarolinaFinance/northCarolinaOutsideGroupContributionAggregator.js";
import type { NorthCarolinaFinanceOutsideGroup } from "../../../src/pipeline/northCarolinaFinance/northCarolinaOutsideSpendingAggregator.js";
import type { NcsbeReceiptRow } from "../../../src/pipeline/northCarolinaFinance/northCarolinaNcsbeParsers.js";

const SPENDER_SBOE_ID = "STA-XY34ZQ-C-002";
const IE_FILER_KEY = `NC-IE-FILER:${"a".repeat(64)}`;

function group(overrides: Partial<NorthCarolinaFinanceOutsideGroup> = {}): NorthCarolinaFinanceOutsideGroup {
  return {
    committeeId: SPENDER_SBOE_ID,
    committeeName: "CAROLINA ACTION PAC",
    supportOppose: "support",
    amount: 1000,
    sourceUrl: null,
    ...overrides,
  };
}

function receipt(overrides: Partial<NcsbeReceiptRow> = {}): NcsbeReceiptRow {
  return {
    groupId: 1,
    occurDate: { raw: "01/15/2026", iso: "2026-01-15", implausible: false },
    orgName: "ROLLING SEA FUND",
    isOrg: true,
    amountCents: 2_450_600,
    sumToDateCents: null,
    profession: null,
    employersName: null,
    isAggregated: false,
    receiptTypeDesc: "Donation",
    receiptTypeCode: "DON ",
    accountAbbr: "1",
    formOfPaymentDesc: "Check",
    purpose: null,
    ...overrides,
  };
}

describe("aggregateNorthCarolinaOutsideGroupContributions", () => {
  it("builds donor and industry breakdowns from pinned entity receipt codes", () => {
    const result = aggregateNorthCarolinaOutsideGroupContributions({
      electionYear: 2026,
      outsideGroups: [group(), group({ supportOppose: "oppose", amount: 400 })],
      receiptRowsByCommitteeId: new Map([
        [
          SPENDER_SBOE_ID,
          [
            receipt(),
            receipt({
              orgName: "Carolina Realty PAC",
              receiptTypeDesc: "Other Political Committee Contribution",
              receiptTypeCode: "CPCM",
              amountCents: 50_000,
            }),
            receipt({
              orgName: "rolling  sea fund",
              amountCents: 10_000,
            }),
          ],
        ],
      ]),
      sourceUrl: "https://cf.ncsbe.gov/CFOrgLkup/",
    });

    expect(result.matchedReceiptRowCount).toBe(3);
    expect(result.includedReceiptRowCount).toBe(3);
    expect(result.skippedReceiptRowCount).toBe(0);
    const donors = result.outsideGroupBreakdowns.filter((row) => row.categoryType === "donor");
    // Both directions get the same funder picture (the money funds the
    // committee, not one direction), and same-normalized names merge.
    expect(donors).toHaveLength(4);
    const supportDonors = donors.filter((row) => row.supportOppose === "support");
    expect(supportDonors).toEqual([
      expect.objectContaining({
        committeeId: SPENDER_SBOE_ID,
        categoryName: "ROLLING SEA FUND",
        amount: 24_606,
        contributorCount: 1,
        sourceUrl: "https://cf.ncsbe.gov/CFOrgLkup/",
      }),
      expect.objectContaining({ categoryName: "Carolina Realty PAC", amount: 500 }),
    ]);
    // The realty donor classifies by static rule; industry rows carry the
    // donor count behind them.
    const industries = result.outsideGroupBreakdowns.filter((row) => row.categoryType === "industry");
    expect(industries.filter((row) => row.supportOppose === "support")).toEqual([
      expect.objectContaining({ categoryName: "real_estate", contributorCount: 1, amount: 500 }),
    ]);
  });

  it("skips individuals, roll-ups, blanks, non-positive amounts, and unknown codes into counters", () => {
    const result = aggregateNorthCarolinaOutsideGroupContributions({
      electionYear: 2026,
      outsideGroups: [group()],
      receiptRowsByCommitteeId: new Map([
        [
          SPENDER_SBOE_ID,
          [
            receipt({ receiptTypeDesc: "Individual Contribution", receiptTypeCode: "IND ", isOrg: false }),
            receipt({ orgName: "Aggregated Individual Contribution", isAggregated: true }),
            receipt({ orgName: "   " }),
            receipt({ amountCents: 0 }),
            receipt({ receiptTypeCode: "LOAN" }),
            receipt({ receiptTypeCode: null }),
          ],
        ],
      ]),
    });

    expect(result).toMatchObject({
      matchedReceiptRowCount: 6,
      includedReceiptRowCount: 0,
      skippedReceiptRowCount: 6,
      individualRowCount: 1,
      aggregatedRowCount: 1,
      blankDonorNameRowCount: 1,
      unusableRowCount: 1,
      unknownReceiptTypeCodeRowCount: 2,
      unknownReceiptTypeCodes: ["", "LOAN"],
    });
    expect(result.outsideGroupBreakdowns).toEqual([]);
  });

  it("counts the live-reviewed entity codes as funders and refunds as non-donor money", () => {
    // PR 9 vocabulary review: "OUTS" (Outside Source) and "NFPC" (Not for
    // Profit Contribution) carry the largest real funder money in NC, while
    // "RFND" is a vendor refund flowing back to the committee — known, so it
    // must skip quietly instead of withholding the candidate's whole slice.
    const result = aggregateNorthCarolinaOutsideGroupContributions({
      electionYear: 2026,
      outsideGroups: [group()],
      receiptRowsByCommitteeId: new Map([
        [
          SPENDER_SBOE_ID,
          [
            receipt({ orgName: "GOOD GOVERMENT COALITION INC", receiptTypeCode: "OUTS", amountCents: 1_250_000_00 }),
            receipt({ orgName: "HEALTHY DEVELOPMENT FUND", receiptTypeCode: "NFPC", amountCents: 15_000_00 }),
            receipt({ orgName: "WIX.COM", receiptTypeCode: "RFND", amountCents: 45_24 }),
          ],
        ],
      ]),
    });

    expect(result).toMatchObject({
      matchedReceiptRowCount: 3,
      includedReceiptRowCount: 2,
      skippedReceiptRowCount: 1,
      nonDonorRowCount: 1,
      unknownReceiptTypeCodeRowCount: 0,
      unknownReceiptTypeCodes: [],
    });
    const donors = result.outsideGroupBreakdowns.filter((row) => row.categoryType === "donor");
    expect(donors.map((row) => [row.categoryName, row.amount])).toEqual([
      ["GOOD GOVERMENT COALITION INC", 1_250_000],
      ["HEALTHY DEVELOPMENT FUND", 15_000],
    ]);
    expect(donors.map((row) => row.categoryName)).not.toContain("WIX.COM");
  });

  it("ignores receipt rows for committees outside the candidate's groups", () => {
    const result = aggregateNorthCarolinaOutsideGroupContributions({
      electionYear: 2026,
      outsideGroups: [group({ committeeId: IE_FILER_KEY, committeeName: "ADVANCE CAROLINA ACTION" })],
      receiptRowsByCommitteeId: new Map([
        [IE_FILER_KEY, [receipt()]],
        ["STA-OTHERR-C-009", [receipt({ orgName: "SOMEONE ELSE PAC", receiptTypeCode: "CPCM" })]],
      ]),
    });

    expect(result.matchedReceiptRowCount).toBe(1);
    // The writer upper-cases committee ids unconditionally, so the breakdown
    // rows carry the uppercased key and still join the stored group rows.
    expect(result.outsideGroupBreakdowns.filter((row) => row.categoryType === "donor")).toEqual([
      expect.objectContaining({
        committeeId: IE_FILER_KEY.toUpperCase(),
        categoryName: "ROLLING SEA FUND",
        amount: 24_506,
      }),
    ]);
  });

  it("rejects an invalid election year", () => {
    expect(() =>
      aggregateNorthCarolinaOutsideGroupContributions({
        electionYear: 1999,
        outsideGroups: [],
        receiptRowsByCommitteeId: new Map(),
      })
    ).toThrow(/election year/);
  });
});
