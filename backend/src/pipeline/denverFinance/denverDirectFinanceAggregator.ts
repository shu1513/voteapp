// Direct-contribution aggregation for Denver (plan Phase 3). Input is a full
// candidate-name sweep of SearchContributionTransactions for one cycle; the
// aggregator applies the fixture-pinned inclusion matrix:
//   - hard entity filter: the candidate-name search is a string filter, so
//     every counted row's recipientCommitteeId must be in the linked filer's
//     committee entity id set (rows outside it belong to someone else);
//   - private direct money = subtypes Monetary + In-Kind, summed SIGNED
//     (refunds/"Overlimit" rows are real negatives the API totals already
//     net — fixture 6);
//   - Fair Elections Fund city money = subtype "Fair Elections Payments",
//     tracked separately for the receipts-composition check and NEVER
//     bucketed (it is public matching, not donor money);
//   - any other subtype fails the aggregation closed: the composition
//     contract (direct + FEF = contributions endpoint) has only been proven
//     over these three subtypes.
// Bucket semantics follow the LA precedent exactly
// (losAngelesDirectContributionAggregator): occupation buckets net signed
// amounts; size buckets describe GROSS POSITIVE receipts only (a refund has
// no pointer to its original receipt, so signed bucketing would invent
// negative-size buckets); contributor counts count positive rows. Missing
// occupation is expected below Denver's $50 aggregate-itemization threshold.

import type { DenverContributionTransaction } from "./denverSearchlightClient.js";
import type { DenverDirectBreakdownInput } from "./denverFinanceWriter.js";

export const DENVER_FEF_FUNDING_SUBTYPE = "Fair Elections Payments";

const DIRECT_SUBTYPES: ReadonlySet<string> = new Set(["Monetary", "In-Kind"]);

export const DENVER_DEFAULT_MAX_OCCUPATION_BREAKDOWNS = 20;

/** LA bucket boundaries, in cents (a $250.00 row lands in $250-$499). */
function contributionSizeBucket(cents: number): string {
  if (cents < 10_000) return "$1-$99";
  if (cents < 25_000) return "$100-$249";
  if (cents < 50_000) return "$250-$499";
  if (cents < 100_000) return "$500-$999";
  if (cents < 500_000) return "$1,000-$4,999";
  return "$5,000+";
}

export type DenverDirectAggregation = {
  /** Monetary + In-Kind, signed net — must equal the overview's private figure. */
  directContributionCents: number;
  /** Fair Elections Payments rows — must equal the overview's FEF figure. */
  fefFundingCents: number;
  breakdowns: DenverDirectBreakdownInput[];
  /** Rows counted into a total (direct + FEF). */
  includedRowCount: number;
  /** Rows dropped because their entity id is outside the filer's set. */
  entityFilteredRowCount: number;
};

export function aggregateDenverDirectContributions(input: {
  rows: readonly DenverContributionTransaction[];
  /** The linked filer's committee entity ids (api/Filer/filer/{id}, fresh). */
  committeeEntityIds: readonly number[];
  maxOccupationBreakdowns?: number;
}): DenverDirectAggregation {
  const limit =
    input.maxOccupationBreakdowns ?? DENVER_DEFAULT_MAX_OCCUPATION_BREAKDOWNS;
  if (!Number.isInteger(limit) || limit <= 0)
    throw new Error(`Invalid Denver occupation breakdown limit: ${limit}`);
  const entityIds = new Set(input.committeeEntityIds);
  if (entityIds.size === 0)
    throw new Error(
      "Denver direct aggregation requires the filer's committee entity ids",
    );
  const occupations = new Map<
    string,
    { name: string; cents: number; count: number }
  >();
  const sizes = new Map<string, { cents: number; count: number }>();
  const unknownSubtypes = new Set<string>();
  let directContributionCents = 0;
  let fefFundingCents = 0;
  let includedRowCount = 0;
  let entityFilteredRowCount = 0;
  for (const row of input.rows) {
    if (!entityIds.has(row.recipientCommitteeId)) {
      entityFilteredRowCount += 1;
      continue;
    }
    if (row.transactionSubType === DENVER_FEF_FUNDING_SUBTYPE) {
      fefFundingCents += row.amountCents;
      includedRowCount += 1;
      continue;
    }
    if (!DIRECT_SUBTYPES.has(row.transactionSubType)) {
      unknownSubtypes.add(row.transactionSubType);
      continue;
    }
    directContributionCents += row.amountCents;
    includedRowCount += 1;
    const occupationName = row.contributorOccupation
      ?.replace(/\s+/g, " ")
      .trim();
    if (occupationName) {
      const key = occupationName.toUpperCase();
      const current = occupations.get(key) ?? {
        name: occupationName,
        cents: 0,
        count: 0,
      };
      current.cents += row.amountCents;
      current.count += row.amountCents > 0 ? 1 : 0;
      occupations.set(key, current);
    }
    if (row.amountCents > 0) {
      const bucket = contributionSizeBucket(row.amountCents);
      const current = sizes.get(bucket) ?? { cents: 0, count: 0 };
      current.cents += row.amountCents;
      current.count += 1;
      sizes.set(bucket, current);
    }
  }
  if (unknownSubtypes.size > 0)
    throw new Error(
      `Denver contribution feed returned unproven transaction subtypes: ${[...unknownSubtypes].sort().join(", ")}`,
    );
  const breakdowns: DenverDirectBreakdownInput[] = [
    ...[...occupations.values()]
      .filter((value) => value.cents > 0)
      .sort((a, b) => b.cents - a.cents || a.name.localeCompare(b.name))
      .slice(0, limit)
      .map((value) => ({
        categoryType: "occupation" as const,
        categoryName: value.name,
        amountCents: value.cents,
        contributorCount: value.count,
      })),
    ...[...sizes]
      .filter(([, value]) => value.cents > 0)
      .sort((a, b) => b[1].cents - a[1].cents || a[0].localeCompare(b[0]))
      .map(([bucket, value]) => ({
        categoryType: "contribution_size" as const,
        categoryName: bucket,
        amountCents: value.cents,
        contributorCount: value.count,
      })),
  ];
  return {
    directContributionCents,
    fefFundingCents,
    breakdowns,
    includedRowCount,
    entityFilteredRowCount,
  };
}
