import {
  getNewYorkCommitteeItemizedReceipts,
  NEW_YORK_SODA_DISCLOSURES_PAGE_URL,
  type NewYorkCommitteeReceiptRow,
  type NewYorkSodaClientOptions,
} from "./newYorkSodaClient.js";

// Who funds an outside group. Organization donors only: individuals (and
// candidate/family money) are never presented as company backing, and NYSBOE
// has no occupation/employer fields to classify them with anyway
// (plan-new-york-finance.md). Receipts are already cycle-scoped by the client
// (election_year filter) so historical funding never counts toward a current
// race.

export type NewYorkOutsideGroupFunder = {
  categoryType: "donor";
  categoryName: string;
  amount: number;
  contributorCount: number;
  sourceUrl: string | null;
};

export type NewYorkOutsideGroupFunderResult = {
  funders: NewYorkOutsideGroupFunder[];
  receiptRowCount: number;
  organizationRowCount: number;
  skippedIndividualRowCount: number;
};

const DEFAULT_MAX_FUNDERS = 20;

const INDIVIDUAL_CONTRIBUTOR_TYPES = new Set([
  "INDIVIDUAL",
  "CANDIDATE/CANDIDATE SPOUSE",
  "CANDIDATE FAMILY MEMBER",
]);

function roundCurrency(value: number): number {
  return Math.round(value * 100) / 100;
}

export function normalizeNewYorkFunderKey(value: string): string {
  return value.toUpperCase().replace(/\s+/g, " ").trim();
}

// Shared with the direct-campaign aggregator: organization receipts are the
// only ones that may feed donor/industry breakdowns.
export function isNewYorkOrganizationReceipt(row: NewYorkCommitteeReceiptRow): boolean {
  if (!row.entityName || row.entityFirstName || row.entityLastName) {
    return false;
  }
  const contributorType = row.contributorType?.toUpperCase().replace(/\s+/g, " ").trim();
  // Schedule B (corporate) rows often omit the contributor type; a present
  // individual-ish type always disqualifies.
  return contributorType === undefined || contributorType === null || !INDIVIDUAL_CONTRIBUTOR_TYPES.has(contributorType);
}

export function aggregateNewYorkOutsideGroupFunders(input: {
  receipts: readonly NewYorkCommitteeReceiptRow[];
  maxFunders?: number;
}): NewYorkOutsideGroupFunderResult {
  const maxFunders = input.maxFunders ?? DEFAULT_MAX_FUNDERS;
  if (!Number.isInteger(maxFunders) || maxFunders <= 0) {
    throw new Error(`Invalid New York outside group funder limit: ${input.maxFunders}`);
  }

  const funders = new Map<string, NewYorkOutsideGroupFunder>();
  let organizationRowCount = 0;
  for (const receipt of input.receipts) {
    if (!isNewYorkOrganizationReceipt(receipt)) {
      continue;
    }
    organizationRowCount += 1;
    const key = normalizeNewYorkFunderKey(receipt.entityName);
    const existing = funders.get(key);
    if (existing) {
      existing.amount = roundCurrency(existing.amount + receipt.amount);
      existing.contributorCount += 1;
      continue;
    }
    funders.set(key, {
      categoryType: "donor",
      categoryName: receipt.entityName,
      amount: roundCurrency(receipt.amount),
      contributorCount: 1,
      sourceUrl: NEW_YORK_SODA_DISCLOSURES_PAGE_URL,
    });
  }

  return {
    funders: [...funders.values()]
      .sort((left, right) => right.amount - left.amount || left.categoryName.localeCompare(right.categoryName))
      .slice(0, maxFunders),
    receiptRowCount: input.receipts.length,
    organizationRowCount,
    skippedIndividualRowCount: input.receipts.length - organizationRowCount,
  };
}

export async function getNewYorkOutsideGroupFunderBreakdowns(
  input: { filerId: string; electionYear: number; maxFunders?: number },
  options: NewYorkSodaClientOptions = {},
  getReceipts: typeof getNewYorkCommitteeItemizedReceipts = getNewYorkCommitteeItemizedReceipts
): Promise<NewYorkOutsideGroupFunderResult> {
  const receipts = await getReceipts({ filerId: input.filerId, electionYear: input.electionYear }, options);
  return aggregateNewYorkOutsideGroupFunders({ receipts, maxFunders: input.maxFunders });
}
