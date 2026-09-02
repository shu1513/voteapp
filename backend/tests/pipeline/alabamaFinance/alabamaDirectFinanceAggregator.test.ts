import { describe, expect, it } from "vitest";

import type { AlabamaCashRow } from "../../../src/pipeline/alabamaFinance/alabamaFcpaCsv.js";
import { aggregateAlabamaDirectFinance } from "../../../src/pipeline/alabamaFinance/alabamaDirectFinanceAggregator.js";

function cashRow(overrides: Partial<AlabamaCashRow>): AlabamaCashRow {
  return {
    committeeId: "32837",
    amountCents: 10_000,
    contributionDate: "2026-03-01",
    lastName: "Smith",
    firstName: "Ann",
    contributionId: "1",
    filedDate: "2026-03-02",
    contributionType: "Cash (Itemized)",
    contributorType: "Individual",
    committeeType: "Principal Campaign Committee",
    committeeName: "Friends of Jones",
    candidateName: "Doug Jones",
    amended: "N",
    ...overrides,
  };
}

describe("aggregateAlabamaDirectFinance", () => {
  it("builds size buckets at the fleet's bucket edges", () => {
    const result = aggregateAlabamaDirectFinance({
      cashRows: [
        cashRow({ contributionId: "1", amountCents: 9_999 }),
        cashRow({ contributionId: "2", amountCents: 10_000, lastName: "Baker" }),
        cashRow({ contributionId: "3", amountCents: 24_999, lastName: "Cole" }),
        cashRow({ contributionId: "4", amountCents: 500_000, lastName: "Dean" }),
      ],
      fcpaCommitteeNumber: "32837",
      authoritativeCashContrib: 5449.98,
    });
    expect(result.bucketsUsable).toBe(true);
    expect(result.breakdowns.map((row) => [row.categoryName, row.amount, row.contributorCount])).toEqual([
      ["$5,000+", 5000, 1],
      ["$100-$249", 349.99, 2],
      ["$1-$99", 99.99, 1],
    ]);
    expect(result.coverageRatio).toBeCloseTo(1, 5);
  });

  it("ignores other committees and excludes in-kind rows from cash and buckets", () => {
    const result = aggregateAlabamaDirectFinance({
      cashRows: [
        cashRow({ contributionId: "1", amountCents: 10_000 }),
        cashRow({ contributionId: "2", committeeId: "99999", amountCents: 77_000 }),
        cashRow({ contributionId: "3", contributionType: "In-Kind (Itemized)", amountCents: 5_000 }),
        cashRow({
          contributionId: "4",
          contributionType: "In-Kind (Non-Itemized)",
          amountCents: 2_000,
        }),
      ],
      fcpaCommitteeNumber: "32837",
      authoritativeCashContrib: 100,
    });
    expect(result.committeeRowCount).toBe(3);
    expect(result.inKindRowCount).toBe(2);
    expect(result.coverageCashCents).toBe(10_000);
    expect(result.breakdowns).toHaveLength(1);
  });

  it("counts non-itemized cash toward coverage but never buckets", () => {
    const result = aggregateAlabamaDirectFinance({
      cashRows: [
        cashRow({ contributionId: "1", amountCents: 10_000 }),
        cashRow({ contributionId: "2", contributionType: "Cash (Non-Itemized)", amountCents: 4_000 }),
        cashRow({
          contributionId: "3",
          contributionType: "Non-Itemized Employee Payroll Contribution",
          amountCents: 1_000,
        }),
      ],
      fcpaCommitteeNumber: "32837",
      authoritativeCashContrib: 150,
    });
    expect(result.coverageCashCents).toBe(15_000);
    expect(result.nonItemizedCashCents).toBe(5_000);
    expect(result.breakdowns).toHaveLength(1);
    expect(result.breakdowns[0]).toMatchObject({ categoryName: "$100-$249", amount: 100 });
  });

  it("keeps returned and negative rows out of buckets while counting them signed in coverage", () => {
    const result = aggregateAlabamaDirectFinance({
      cashRows: [
        cashRow({ contributionId: "1", amountCents: 10_000 }),
        cashRow({ contributionId: "2", contributorType: "Returned (Cash Only)", amountCents: 5_000 }),
        cashRow({ contributionId: "3", amountCents: -2_500 }),
      ],
      fcpaCommitteeNumber: "32837",
      authoritativeCashContrib: 125,
    });
    expect(result.coverageCashCents).toBe(12_500);
    expect(result.returnedRowCount).toBe(1);
    expect(result.negativeOrZeroItemizedRowCount).toBe(1);
    expect(result.breakdowns).toHaveLength(1);
    expect(result.coverageRatio).toBeCloseTo(1, 5);
  });

  it("gates buckets off outside the coverage tolerance", () => {
    const low = aggregateAlabamaDirectFinance({
      cashRows: [cashRow({ amountCents: 90_000 })],
      fcpaCommitteeNumber: "32837",
      authoritativeCashContrib: 1_000,
    });
    expect(low.bucketsUsable).toBe(false);
    expect(low.breakdowns).toEqual([]);
    expect(low.bucketDiagnostics[0]).toContain("cash_coverage_out_of_tolerance");

    const high = aggregateAlabamaDirectFinance({
      cashRows: [cashRow({ amountCents: 200_000 })],
      fcpaCommitteeNumber: "32837",
      authoritativeCashContrib: 1_000,
    });
    expect(high.bucketsUsable).toBe(false);
  });

  it("flags a zero race total with nonzero extract cash as a bad join", () => {
    const result = aggregateAlabamaDirectFinance({
      cashRows: [cashRow({ amountCents: 10_000 })],
      fcpaCommitteeNumber: "32837",
      authoritativeCashContrib: 0,
    });
    expect(result.bucketsUsable).toBe(false);
    expect(result.bucketDiagnostics[0]).toContain("zero_authoritative_cash_nonzero_extract_cash");
  });

  it("treats a zero race total with zero extract cash as usable-and-empty", () => {
    const result = aggregateAlabamaDirectFinance({
      cashRows: [],
      fcpaCommitteeNumber: "32837",
      authoritativeCashContrib: 0,
    });
    expect(result.bucketsUsable).toBe(true);
    expect(result.breakdowns).toEqual([]);
    expect(result.coverageRatio).toBeNull();
  });
});
