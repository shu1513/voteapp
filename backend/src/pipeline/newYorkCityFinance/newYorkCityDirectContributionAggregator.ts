import { NEW_YORK_CITY_CFB_DATA_LIBRARY_URL, type NewYorkCityCfbContributionRow } from "./newYorkCityCfbCsv.js";

export type NewYorkCityFinanceDirectBreakdown = {
  categoryType: "occupation" | "employer" | "industry" | "contribution_size";
  categoryName: string;
  amount: number;
  contributorCount: number | null;
  sourceUrl: string | null;
};

const INCLUDED_SCHEDULES = new Set(["ABC", "D", "G", "K", "M"]);

function roundMoney(value: number): number {
  return Math.round(value * 100) / 100;
}

function normalizedLabel(value: string): string {
  return value.replace(/\s+/g, " ").trim().toUpperCase();
}

function sizeBucket(amount: number): string {
  if (amount < 100) return "$1-$99";
  if (amount < 250) return "$100-$249";
  if (amount < 500) return "$250-$499";
  if (amount < 1_000) return "$500-$999";
  if (amount < 5_000) return "$1,000-$4,999";
  return "$5,000+";
}

type Bucket = { displayName: string; amount: number; contributorCount: number };

function addBucket(map: Map<string, Bucket>, rawName: string, amount: number): void {
  const key = normalizedLabel(rawName);
  if (!key) return;
  const existing = map.get(key) ?? { displayName: rawName.replace(/\s+/g, " ").trim(), amount: 0, contributorCount: 0 };
  existing.amount = roundMoney(existing.amount + amount);
  if (amount > 0) existing.contributorCount += 1;
  map.set(key, existing);
}

function signedAmount(row: NewYorkCityCfbContributionRow): number {
  return row.schedule === "M" && row.amount > 0 ? -row.amount : row.amount;
}

function latestTransactionRows(rows: readonly NewYorkCityCfbContributionRow[]): NewYorkCityCfbContributionRow[] {
  const latest = new Map<string, NewYorkCityCfbContributionRow>();
  for (const row of rows) {
    const key = `${row.candidateId}\u0000${row.schedule}\u0000${row.referenceNumber}`;
    const existing = latest.get(key);
    if (!existing || row.filing > existing.filing) latest.set(key, row);
  }
  return [...latest.values()];
}

function toBreakdowns(
  categoryType: NewYorkCityFinanceDirectBreakdown["categoryType"],
  buckets: ReadonlyMap<string, Bucket>,
  limit: number
): NewYorkCityFinanceDirectBreakdown[] {
  return [...buckets.values()]
    .filter((bucket) => bucket.amount > 0)
    .map((bucket) => ({
      categoryType,
      categoryName: bucket.displayName,
      amount: bucket.amount,
      contributorCount: bucket.contributorCount,
      sourceUrl: NEW_YORK_CITY_CFB_DATA_LIBRARY_URL,
    }))
    .sort((left, right) => right.amount - left.amount || left.categoryName.localeCompare(right.categoryName))
    .slice(0, limit);
}

export function aggregateNewYorkCityDirectContributions(input: {
  rows: readonly NewYorkCityCfbContributionRow[];
  candidateId: string;
  electionYear: number;
  officeCode: "1" | "2" | "3" | "4";
  maxBreakdownsPerCategory?: number;
}): { breakdowns: NewYorkCityFinanceDirectBreakdown[]; acceptedRowCount: number; ignoredRowCount: number } {
  const limit = input.maxBreakdownsPerCategory ?? 20;
  if (!Number.isInteger(limit) || limit <= 0) throw new Error(`Invalid NYC CFB breakdown limit: ${limit}`);
  const occupations = new Map<string, Bucket>();
  const employers = new Map<string, Bucket>();
  const sizes = new Map<string, Bucket>();
  let acceptedRowCount = 0;
  let ignoredRowCount = 0;

  const candidateRows = input.rows.filter(
    (row) =>
      row.candidateId === input.candidateId &&
      row.electionYear === input.electionYear &&
      row.officeCode === input.officeCode
  );
  for (const row of latestTransactionRows(candidateRows)) {
    if (!INCLUDED_SCHEDULES.has(row.schedule) || row.amount === 0) {
      ignoredRowCount += 1;
      continue;
    }
    const amount = signedAmount(row);
    acceptedRowCount += 1;
    addBucket(sizes, sizeBucket(Math.abs(row.amount)), amount);
    if (row.contributorType === "IND") {
      if (row.occupation) addBucket(occupations, row.occupation, amount);
      if (row.employer) addBucket(employers, row.employer, amount);
    }
  }

  return {
    breakdowns: [
      ...toBreakdowns("occupation", occupations, limit),
      ...toBreakdowns("employer", employers, limit),
      ...toBreakdowns("contribution_size", sizes, limit),
    ],
    acceptedRowCount,
    ignoredRowCount,
  };
}
