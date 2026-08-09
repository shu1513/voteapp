import { describe, expect, it } from "vitest";

import { aggregateGeorgiaOutsideGroupContributions } from "../../../src/pipeline/georgiaFinance/georgiaOutsideGroupContributionAggregator.js";
import type { GeorgiaOutsideSpendingGroup } from "../../../src/pipeline/georgiaFinance/georgiaOutsideSpendingAggregator.js";
import type { GeorgiaTransactionRow } from "../../../src/pipeline/georgiaFinance/georgiaEthicsClient.js";

const SPENDER_GUID = "aaaaaaaa-1111-2222-3333-444444444444";
const OTHER_SPENDER_GUID = "bbbbbbbb-1111-2222-3333-444444444444";

function group(overrides: Partial<GeorgiaOutsideSpendingGroup> = {}): GeorgiaOutsideSpendingGroup {
  return {
    committeeId: SPENDER_GUID,
    committeeName: "Example PAC",
    supportOppose: "support",
    amount: 10_000,
    sourceUrl: "https://ethics.ga.gov/records-search-all/",
    ...overrides,
  };
}

let nextTransactionId = 1;

function tconRow(overrides: Partial<GeorgiaTransactionRow> = {}): GeorgiaTransactionRow {
  nextTransactionId += 1;
  return {
    guid: `row-${nextTransactionId}`,
    transactionId: nextTransactionId,
    transactionAmount: 1_000,
    filerEntityId: 200_001,
    filerRegistrationGuid: SPENDER_GUID,
    filerReportGuid: "report-1",
    timedFiledReportGuid: null,
    filerReportId: 11,
    filerReportVersionId: 1,
    transactionDate: "2026-02-01T00:00:00",
    sourceName: "Acme Corp",
    payeeOccupation: null,
    payeeEmployer: null,
    transactionTypeCode: "TCON",
    transactionSubTypeCode: "ITMY",
    transactionSubTypeDesc: "Itemized Contribution",
    transactionSourceTypeCode: "TBSN",
    transactionStatusCode: "TFIL",
    reportName: "2026 Jan 31 CCDR",
    electionYear: 2026,
    ...overrides,
  };
}

describe("aggregateGeorgiaOutsideGroupContributions", () => {
  it("aggregates organization donors per group and direction, sorted by amount", () => {
    const groups = [group(), group({ supportOppose: "oppose", amount: 500 })];
    const result = aggregateGeorgiaOutsideGroupContributions({
      electionYear: 2026,
      outsideGroups: groups,
      contributionRowsBySpender: new Map([
        [
          SPENDER_GUID,
          [
            tconRow({ sourceName: "Acme Corp", transactionAmount: 1_000 }),
            tconRow({ sourceName: "Acme Corp", transactionAmount: 250.5 }),
            tconRow({ sourceName: "Builders Union Local 5", transactionAmount: 2_000 }),
          ],
        ],
      ]),
      sourceUrl: "https://example.test/source",
    });

    // Each donor row attaches to BOTH direction rows of the spender.
    expect(result.matchedContributionRowCount).toBe(3);
    expect(result.includedContributionRowCount).toBe(3);
    expect(result.skippedContributionRowCount).toBe(0);
    expect(result.outsideGroupBreakdowns).toEqual([
      {
        committeeId: SPENDER_GUID,
        supportOppose: "oppose",
        categoryType: "donor",
        categoryName: "Builders Union Local 5",
        amount: 2_000,
        contributorCount: 1,
        sourceUrl: "https://example.test/source",
      },
      {
        committeeId: SPENDER_GUID,
        supportOppose: "oppose",
        categoryType: "donor",
        categoryName: "Acme Corp",
        amount: 1_250.5,
        contributorCount: 1,
        sourceUrl: "https://example.test/source",
      },
      {
        committeeId: SPENDER_GUID,
        supportOppose: "support",
        categoryType: "donor",
        categoryName: "Builders Union Local 5",
        amount: 2_000,
        contributorCount: 1,
        sourceUrl: "https://example.test/source",
      },
      {
        committeeId: SPENDER_GUID,
        supportOppose: "support",
        categoryType: "donor",
        categoryName: "Acme Corp",
        amount: 1_250.5,
        contributorCount: 1,
        sourceUrl: "https://example.test/source",
      },
    ]);
  });

  it("excludes individuals, unlabeled subtypes, returns, off-cycle rows, and unrecognized statuses", () => {
    const result = aggregateGeorgiaOutsideGroupContributions({
      electionYear: 2026,
      outsideGroups: [group()],
      contributionRowsBySpender: new Map([
        [
          SPENDER_GUID,
          [
            tconRow({ sourceName: "Acme Corp", transactionAmount: 1_000 }),
            // Individual by structured source code — even with an org-looking name.
            tconRow({ sourceName: "Smith Holdings", transactionSourceTypeCode: "TIND" }),
            // Individual by name shape (comma name, no org word, null code).
            tconRow({ sourceName: "Smith, Jane", transactionSourceTypeCode: null }),
            // No org signal at all (null code, plain person name).
            tconRow({ sourceName: "Jane Smith", transactionSourceTypeCode: null }),
            // Unitemized carries no donor identity.
            tconRow({ transactionSubTypeCode: "NITMY" }),
            // Unpinned subtype fails closed.
            tconRow({ transactionSubTypeCode: "MYSTERY" }),
            // Return: always-negative row.
            tconRow({ transactionAmount: -300 }),
            // Off-cycle by date year.
            tconRow({ transactionDate: "2023-05-01T00:00:00", electionYear: 2026 }),
            // Unrecognized status fails closed (archive code on the PeachFile host).
            tconRow({ transactionStatusCode: "F" }),
          ],
        ],
      ]),
    });

    expect(result.matchedContributionRowCount).toBe(9);
    expect(result.includedContributionRowCount).toBe(1);
    expect(result.skippedContributionRowCount).toBe(8);
    expect(result.outsideGroupBreakdowns).toHaveLength(1);
    expect(result.outsideGroupBreakdowns[0]).toMatchObject({ categoryName: "Acme Corp", amount: 1_000 });
  });

  it("falls back to the row electionYear when the date is missing and counts in-kind donor money", () => {
    const result = aggregateGeorgiaOutsideGroupContributions({
      electionYear: 2026,
      outsideGroups: [group()],
      contributionRowsBySpender: new Map([
        [
          SPENDER_GUID,
          [
            tconRow({ transactionDate: null, electionYear: 2026, sourceName: "Acme Corp" }),
            tconRow({ transactionDate: "garbage", electionYear: 2025, sourceName: "Acme Corp" }),
            tconRow({ transactionDate: null, electionYear: null, sourceName: "Acme Corp" }),
            tconRow({
              transactionSubTypeCode: "INKIND",
              sourceName: "Catering Company LLC",
              transactionAmount: 500,
            }),
          ],
        ],
      ]),
    });

    expect(result.includedContributionRowCount).toBe(3);
    expect(result.skippedContributionRowCount).toBe(1);
    expect(result.outsideGroupBreakdowns.map((row) => [row.categoryName, row.amount])).toEqual([
      ["Acme Corp", 2_000],
      ["Catering Company LLC", 500],
    ]);
  });

  it("ignores spenders outside the written group list and matches guids case-insensitively", () => {
    const result = aggregateGeorgiaOutsideGroupContributions({
      electionYear: 2026,
      outsideGroups: [group()],
      contributionRowsBySpender: new Map([
        [SPENDER_GUID.toUpperCase(), [tconRow({ sourceName: "Acme Corp" })]],
        [OTHER_SPENDER_GUID, [tconRow({ sourceName: "Foreign PAC", filerRegistrationGuid: OTHER_SPENDER_GUID })]],
      ]),
    });

    // The foreign spender's rows never enter the counters — it has no group
    // row in this snapshot, so a breakdown for it would fail the writer's
    // pairing validation.
    expect(result.matchedContributionRowCount).toBe(1);
    expect(result.outsideGroupBreakdowns).toHaveLength(1);
    expect(result.outsideGroupBreakdowns[0]!.categoryName).toBe("Acme Corp");
  });

  it("rejects an invalid election year", () => {
    expect(() =>
      aggregateGeorgiaOutsideGroupContributions({
        electionYear: 1800,
        outsideGroups: [],
        contributionRowsBySpender: new Map(),
      })
    ).toThrow("Invalid Georgia outside group contribution election year");
  });
});
