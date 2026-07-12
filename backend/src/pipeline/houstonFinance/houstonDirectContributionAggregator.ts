import type { HoustonFinanceParsedReport } from "./houstonFinanceTypes.js";

export type HoustonFinanceDirectBreakdown = {
  categoryType: "occupation" | "contribution_size";
  categoryName: string;
  amount: number;
  contributorCount: number;
  sourceUrl: string | null;
};

function cents(value: number): number {
  return Math.round(value * 100);
}

function bucket(amount: number): string {
  if (amount < 100) return "$1-$99";
  if (amount < 250) return "$100-$249";
  if (amount < 500) return "$250-$499";
  if (amount < 1_000) return "$500-$999";
  if (amount < 5_000) return "$1,000-$4,999";
  return "$5,000+";
}

function occupation(value: string): string {
  return value.trim().replace(/\s+/g, " ").toUpperCase();
}

export function aggregateHoustonDirectContributions(input: {
  reports: readonly HoustonFinanceParsedReport[];
  maxOccupations?: number;
}): {
  totalReceipts: number;
  directContributionTotal: number;
  directBreakdowns: HoustonFinanceDirectBreakdown[];
  contributionCount: number;
} {
  const occupations = new Map<string, { amountCents: number; contributors: Set<string>; sourceUrl: string | null }>();
  const sizes = new Map<string, { amountCents: number; contributors: Set<string>; sourceUrl: string | null }>();
  let contributionCount = 0;
  for (const report of input.reports) {
    for (const contribution of report.contributions) {
      const amountCents = cents(contribution.amount);
      if (!Number.isSafeInteger(amountCents) || amountCents <= 0) continue;
      contributionCount += 1;
      const contributorKey = contribution.contributorName.trim().toUpperCase();
      const sizeName = bucket(contribution.amount);
      const size = sizes.get(sizeName) ?? { amountCents: 0, contributors: new Set<string>(), sourceUrl: contribution.sourceUrl };
      size.amountCents += amountCents;
      size.contributors.add(contributorKey);
      sizes.set(sizeName, size);
      if (contribution.occupation) {
        const name = occupation(contribution.occupation);
        const row = occupations.get(name) ?? { amountCents: 0, contributors: new Set<string>(), sourceUrl: contribution.sourceUrl };
        row.amountCents += amountCents;
        row.contributors.add(contributorKey);
        occupations.set(name, row);
      }
    }
  }
  const mapRows = (categoryType: HoustonFinanceDirectBreakdown["categoryType"], rows: typeof occupations) =>
    [...rows].map(([categoryName, row]) => ({
      categoryType,
      categoryName,
      amount: row.amountCents / 100,
      contributorCount: row.contributors.size,
      sourceUrl: row.sourceUrl,
    }));
  const directBreakdowns = [
    ...mapRows("occupation", occupations)
      .sort((left, right) => right.amount - left.amount || left.categoryName.localeCompare(right.categoryName))
      .slice(0, input.maxOccupations ?? 50),
    ...mapRows("contribution_size", sizes).sort((left, right) => right.amount - left.amount),
  ];
  const totalCents = input.reports.reduce((sum, report) => {
    if (report.directContributionTotal !== null) return sum + cents(report.directContributionTotal);
    return sum + report.contributions.reduce((reportSum, contribution) => reportSum + cents(contribution.amount), 0);
  }, 0);
  return {
    totalReceipts: totalCents / 100,
    directContributionTotal: totalCents / 100,
    directBreakdowns,
    contributionCount,
  };
}
