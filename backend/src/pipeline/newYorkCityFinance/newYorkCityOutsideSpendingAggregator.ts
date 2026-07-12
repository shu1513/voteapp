import type { NewYorkCityCfbIndependentSpendingRow } from "./newYorkCityCfbIndependentSpendingClient.js";

export type NewYorkCityOutsideSpendingGroup = {
  spenderId: string;
  spenderName: string;
  supportOppose: "support" | "oppose";
  amount: number;
  expenditureCount: number;
  sourceUrl: string;
};

export function aggregateNewYorkCityOutsideSpending(input: {
  rows: readonly NewYorkCityCfbIndependentSpendingRow[];
  electionYear: number;
  electionCycle: string;
  candidateId: string;
  sourceUrl: string;
}): { supportTotal: number; opposeTotal: number; groups: NewYorkCityOutsideSpendingGroup[] } {
  const seen = new Map<string, NewYorkCityCfbIndependentSpendingRow>();
  const groups = new Map<string, Omit<NewYorkCityOutsideSpendingGroup, "amount"> & { tenThousandths: number }>();
  for (const row of input.rows) {
    if (
      row.electionYear !== input.electionYear ||
      row.electionCycle !== input.electionCycle ||
      row.candidateId !== input.candidateId
    ) continue;
    const allocationKey = `${row.spenderId}\u0000${row.communicationId}\u0000${row.candidateId}`;
    const previous = seen.get(allocationKey);
    if (previous) {
      if (
        previous.spenderName !== row.spenderName ||
        previous.supportOppose !== row.supportOppose ||
        Math.round(previous.allocation * 10_000) !== Math.round(row.allocation * 10_000)
      ) {
        throw new Error(`Conflicting NYC outside-spending allocation: ${allocationKey}`);
      }
      continue;
    }
    seen.set(allocationKey, row);
    const groupKey = `${row.spenderId}\u0000${row.supportOppose}`;
    const group = groups.get(groupKey) ?? {
      spenderId: row.spenderId,
      spenderName: row.spenderName,
      supportOppose: row.supportOppose,
      tenThousandths: 0,
      expenditureCount: 0,
      sourceUrl: input.sourceUrl,
    };
    if (group.spenderName !== row.spenderName) {
      throw new Error(`Conflicting NYC outside-spending spender name: ${row.spenderId}`);
    }
    group.tenThousandths += Math.round(row.allocation * 10_000);
    group.expenditureCount += 1;
    groups.set(groupKey, group);
  }
  const mapped = [...groups.values()]
    .map(({ tenThousandths, ...group }) => ({ ...group, amount: Math.round(tenThousandths / 100) / 100 }))
    .sort((left, right) => right.amount - left.amount || left.spenderName.localeCompare(right.spenderName));
  return {
    supportTotal:
      Math.round(
        [...groups.values()]
          .filter((row) => row.supportOppose === "support")
          .reduce((sum, row) => sum + row.tenThousandths, 0) / 100
      ) / 100,
    opposeTotal:
      Math.round(
        [...groups.values()]
          .filter((row) => row.supportOppose === "oppose")
          .reduce((sum, row) => sum + row.tenThousandths, 0) / 100
      ) / 100,
    groups: mapped,
  };
}
