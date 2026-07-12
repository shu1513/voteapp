import type { LosAngelesContributionRecord } from "./losAngelesOpenDataClient.js";
import { LOS_ANGELES_CONTRIBUTIONS_SOURCE_URL } from "./losAngelesOpenDataClient.js";

export type LosAngelesDirectBreakdown = {
  categoryType: "occupation" | "employer" | "industry" | "contribution_size";
  categoryName: string;
  amount: number;
  contributorCount: number;
  sourceUrl: string;
};

// Ethics UI calls the loan schedule B1; Open Data encodes it as B.
const RECEIPT_SCHEDULES = new Set(["A", "B", "B1", "C"]);

function round(value: number): number {
  return Math.round(value * 100) / 100;
}
function bucket(amount: number): string {
  if (amount < 100) return "$1-$99";
  if (amount < 250) return "$100-$249";
  if (amount < 500) return "$250-$499";
  if (amount < 1_000) return "$500-$999";
  if (amount < 5_000) return "$1,000-$4,999";
  return "$5,000+";
}

export function aggregateLosAngelesDirectContributions(input: {
  records: readonly LosAngelesContributionRecord[];
  maxBreakdownsPerCategory?: number;
}): {
  reconciledContributionTotal: number;
  breakdowns: LosAngelesDirectBreakdown[];
  includedRowCount: number;
  skippedRowCount: number;
} {
  const limit = input.maxBreakdownsPerCategory ?? 20;
  if (!Number.isInteger(limit) || limit <= 0)
    throw new Error(`Invalid Los Angeles direct breakdown limit: ${limit}`);
  const maps = new Map<
    Exclude<LosAngelesDirectBreakdown["categoryType"], "industry">,
    Map<string, { name: string; cents: number; count: number }>
  >();
  const add = (
    type: Exclude<LosAngelesDirectBreakdown["categoryType"], "industry">,
    rawName: string | null,
    cents: number,
    count: number,
  ): void => {
    const name = rawName?.trim().replace(/\s+/g, " ");
    if (!name) return;
    const values = maps.get(type) ?? new Map();
    const key = name.toUpperCase();
    const current = values.get(key) ?? { name, cents: 0, count: 0 };
    current.cents += cents;
    current.count += count;
    values.set(key, current);
    maps.set(type, values);
  };
  let totalCents = 0;
  let includedRowCount = 0;
  let skippedRowCount = 0;
  for (const record of input.records) {
    if (!RECEIPT_SCHEDULES.has(record.schedule)) {
      skippedRowCount += 1;
      continue;
    }
    const net = round(record.amount - record.amountPaidOrForgiven);
    if (net === 0) {
      skippedRowCount += 1;
      continue;
    }
    const cents = Math.round(net * 100);
    totalCents += cents;
    includedRowCount += 1;
    add("occupation", record.occupation, cents, net > 0 ? 1 : 0);
    add("employer", record.employer, cents, net > 0 ? 1 : 0);
    // Size buckets describe gross positive receipts. A negative amendment or
    // refund does not identify its original receipt, so bucketing its absolute
    // amount could move a partial refund into the wrong original-size bucket.
    if (net > 0) add("contribution_size", bucket(net), cents, 1);
  }
  const breakdowns: LosAngelesDirectBreakdown[] = [];
  for (const type of ["occupation", "employer", "contribution_size"] as const) {
    const categoryLimit =
      type === "contribution_size" ? Number.POSITIVE_INFINITY : limit;
    breakdowns.push(
      ...[...(maps.get(type)?.values() ?? [])]
        .filter((value) => value.cents > 0)
        .sort((a, b) => b.cents - a.cents || a.name.localeCompare(b.name))
        .slice(0, categoryLimit)
        .map((value) => ({
          categoryType: type,
          categoryName: value.name,
          amount: value.cents / 100,
          contributorCount: value.count,
          sourceUrl: LOS_ANGELES_CONTRIBUTIONS_SOURCE_URL,
        })),
    );
  }
  return {
    reconciledContributionTotal: totalCents / 100,
    breakdowns,
    includedRowCount,
    skippedRowCount,
  };
}
