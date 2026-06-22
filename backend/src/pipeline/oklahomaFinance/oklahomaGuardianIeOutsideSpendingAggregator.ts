import type {
  OklahomaFinanceOutsideGroupInput,
  OklahomaFinanceSummaryInput,
} from "./oklahomaFinanceWriter.js";
import type { OklahomaGuardianIeOutsideSpendingReport } from "./oklahomaGuardianIeOutsideSpendingDiscovery.js";

export type OklahomaGuardianIeOutsideFinanceSnapshot = {
  summary: Pick<OklahomaFinanceSummaryInput, "outsideSupportTotal" | "outsideOpposeTotal" | "sourceUrl">;
  outsideGroups: OklahomaFinanceOutsideGroupInput[];
};

type OutsideGroupAccumulator = {
  committeeId: string;
  committeeName: string;
  supportOppose: "support" | "oppose";
  amountCents: number;
  sourceUrl: string | null;
};

function centsToDollars(value: number): number {
  return Math.round(value) / 100;
}

function amountToCents(value: number): number {
  if (!Number.isFinite(value) || value < 0) {
    throw new Error(`Invalid Oklahoma Guardian IE outside spending amount: ${value}`);
  }
  return Math.round(value * 100);
}

function normalizeOutsideCommitteeId(value: string): string {
  const normalized = value.trim().replace(/\s+/g, " ").toUpperCase();
  if (!normalized) {
    throw new Error("Oklahoma Guardian IE outside spender name is required");
  }
  return normalized;
}

export function buildOklahomaGuardianIeOutsideFinanceSnapshot(
  reports: readonly OklahomaGuardianIeOutsideSpendingReport[]
): OklahomaGuardianIeOutsideFinanceSnapshot {
  const groups = new Map<string, OutsideGroupAccumulator>();
  let supportTotalCents = 0;
  let opposeTotalCents = 0;
  let sourceUrl: string | null = null;

  for (const report of reports) {
    const committeeId = normalizeOutsideCommitteeId(report.spenderName);
    const amountCents = amountToCents(report.amount);
    const key = `${committeeId}\u0000${report.supportOppose}`;
    const existing = groups.get(key);
    if (existing) {
      existing.amountCents += amountCents;
      existing.sourceUrl ??= report.sourceUrl;
    } else {
      groups.set(key, {
        committeeId,
        committeeName: report.spenderName.trim().replace(/\s+/g, " "),
        supportOppose: report.supportOppose,
        amountCents,
        sourceUrl: report.sourceUrl,
      });
    }

    if (report.supportOppose === "support") {
      supportTotalCents += amountCents;
    } else {
      opposeTotalCents += amountCents;
    }
    sourceUrl ??= report.sourceUrl;
  }

  return {
    summary: {
      outsideSupportTotal: centsToDollars(supportTotalCents),
      outsideOpposeTotal: centsToDollars(opposeTotalCents),
      sourceUrl,
    },
    outsideGroups: [...groups.values()]
      .map((group) => ({
        committeeId: group.committeeId,
        committeeName: group.committeeName,
        supportOppose: group.supportOppose,
        amount: centsToDollars(group.amountCents),
        sourceUrl: group.sourceUrl,
      }))
      .sort((left, right) => right.amount - left.amount || left.committeeName.localeCompare(right.committeeName)),
  };
}
