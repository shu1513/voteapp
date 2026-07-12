import type { LosAngelesEthicsIndependentSpendingRow } from "./losAngelesCityEthicsClient.js";

export function aggregateLosAngelesOutsideSpending(
  rows: readonly LosAngelesEthicsIndependentSpendingRow[],
): {
  supportTotal: number;
  opposeTotal: number;
  groups: Array<{
    spenderId: string;
    spenderName: string;
    supportOppose: "support" | "oppose";
    amount: number;
    expenditureCount: number;
    sourceUrl: string | null;
  }>;
} {
  const seen = new Set<string>();
  const groups = new Map<
    string,
    {
      spenderId: string;
      spenderName: string;
      supportOppose: "support" | "oppose";
      cents: number;
      expenditureCount: number;
      sourceUrl: string | null;
    }
  >();
  for (const row of rows) {
    if (seen.has(row.expenditureId)) continue;
    seen.add(row.expenditureId);
    const key = `${row.spenderId}\u0000${row.supportOppose}`;
    const current = groups.get(key) ?? {
      spenderId: row.spenderId,
      spenderName: row.spenderName,
      supportOppose: row.supportOppose,
      cents: 0,
      expenditureCount: 0,
      sourceUrl: row.reportUrl,
    };
    current.cents += Math.round(row.amount * 100);
    current.expenditureCount += 1;
    current.sourceUrl ??= row.reportUrl;
    groups.set(key, current);
  }
  const mapped = [...groups.values()]
    .map((group) => ({ ...group, amount: group.cents / 100 }))
    .map(({ cents: _cents, ...group }) => group)
    .sort(
      (a, b) =>
        b.amount - a.amount || a.spenderName.localeCompare(b.spenderName),
    );
  return {
    supportTotal: mapped
      .filter((g) => g.supportOppose === "support")
      .reduce((sum, g) => sum + g.amount, 0),
    opposeTotal: mapped
      .filter((g) => g.supportOppose === "oppose")
      .reduce((sum, g) => sum + g.amount, 0),
    groups: mapped,
  };
}
