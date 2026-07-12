import {
  buildNewYorkCityCfbIndependentSpenderFunderSourceUrl,
  type NewYorkCityCfbIndependentSpenderFunderRow,
} from "./newYorkCityCfbIndependentSpendingClient.js";
import type { NewYorkCityOutsideSpendingGroup } from "./newYorkCityOutsideSpendingAggregator.js";

const ORGANIZATION_TYPES = new Set(["LLC", "CORP", "OTHR", "EMPO", "PART"]);

export type NewYorkCityOutsideGroupBreakdown = {
  spenderId: string;
  supportOppose: "support" | "oppose";
  categoryType: "donor" | "industry";
  categoryName: string;
  amount: number;
  contributorCount: number | null;
  sourceUrl: string;
};

function normalizeLabel(value: string): string {
  return value.replace(/\s+/g, " ").trim().toUpperCase();
}

export function aggregateNewYorkCityOutsideGroupFunders(input: {
  rows: readonly NewYorkCityCfbIndependentSpenderFunderRow[];
  groups: readonly NewYorkCityOutsideSpendingGroup[];
  electionYear: number;
  electionCycle: string;
  maxFundersPerGroup?: number;
}): NewYorkCityOutsideGroupBreakdown[] {
  const limit = input.maxFundersPerGroup ?? 20;
  if (!Number.isInteger(limit) || limit <= 0) throw new Error(`Invalid NYC outside funder limit: ${limit}`);
  const rowsBySpender = new Map<string, NewYorkCityCfbIndependentSpenderFunderRow[]>();
  for (const row of input.rows) {
    if (
      row.electionYear !== input.electionYear ||
      row.electionCycle !== input.electionCycle ||
      !ORGANIZATION_TYPES.has(row.funderType)
    ) continue;
    const rows = rowsBySpender.get(row.spenderId) ?? [];
    rows.push(row);
    rowsBySpender.set(row.spenderId, rows);
  }
  const result: NewYorkCityOutsideGroupBreakdown[] = [];
  for (const group of input.groups) {
    const sourceUrl = buildNewYorkCityCfbIndependentSpenderFunderSourceUrl({
      electionYear: input.electionYear,
      electionCycle: input.electionCycle,
      spenderId: group.spenderId,
    });
    const donors = new Map<string, { name: string; cents: number; contributorCount: number }>();
    for (const row of rowsBySpender.get(group.spenderId) ?? []) {
      const key = normalizeLabel(row.funderName);
      const donor = donors.get(key) ?? { name: row.funderName.replace(/\s+/g, " ").trim(), cents: 0, contributorCount: 0 };
      donor.cents += Math.round(row.amount * 100);
      if (row.amount > 0) donor.contributorCount += 1;
      donors.set(key, donor);
    }
    result.push(
      ...[...donors.values()]
        .filter((donor) => donor.cents > 0)
        .sort((left, right) => right.cents - left.cents || left.name.localeCompare(right.name))
        .slice(0, limit)
        .map((donor) => ({
          spenderId: group.spenderId,
          supportOppose: group.supportOppose,
          categoryType: "donor" as const,
          categoryName: donor.name,
          amount: donor.cents / 100,
          contributorCount: donor.contributorCount,
          sourceUrl,
        }))
    );
  }
  return result;
}
