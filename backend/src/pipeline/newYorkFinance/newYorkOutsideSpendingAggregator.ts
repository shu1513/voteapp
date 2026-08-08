import {
  newYorkCandidateNameMiddleConflict,
  normalizeNewYorkCandidateNameKeys,
} from "./newYorkCandidateCommitteeResolver.js";
import {
  getNewYorkFilerRecords,
  getNewYorkParentExpenditures,
  getNewYorkScheduleRAllocations,
  NEW_YORK_INDEPENDENT_EXPENDITURE_COMMITTEE_TYPE,
  NEW_YORK_SODA_DISCLOSURES_PAGE_URL,
  type NewYorkFilerRecord,
  type NewYorkParentExpenditureRow,
  type NewYorkScheduleRAllocationRow,
  type NewYorkSodaClientOptions,
} from "./newYorkSodaClient.js";
import { toNewYorkBoeOfficeSearchInput } from "./newYorkFinanceEligibleOffices.js";

// Strict Schedule R acceptance rules (plan-new-york-finance.md). Every rule
// must hold; anything ambiguous or structurally incomplete is skipped and
// counted, never guessed. Party committees also file Schedule R (verified:
// NYS Democratic Committee allocations to Hochul), so the registry gate to
// Independent Expenditure Committees is mandatory for the outside-spending
// framing to be honest.

export type NewYorkOutsideSpendingGroup = {
  filerId: string;
  filerName: string;
  supportOppose: "support" | "oppose";
  amount: number;
  allocationCount: number;
  sourceUrl: string | null;
};

export type NewYorkOutsideSpendingCounters = {
  allocationRowCount: number;
  nameMatchedRowCount: number;
  duplicateTransactionRowCount: number;
  nonIeCommitteeRowCount: number;
  unresolvedMappingRowCount: number;
  acceptedRowCount: number;
};

export type NewYorkOutsideSpendingResult = {
  groups: NewYorkOutsideSpendingGroup[];
  // Totals cover every accepted group, not just the (possibly capped) groups
  // array — a field named "total" must never silently undercount.
  supportTotal: number;
  opposeTotal: number;
  counters: NewYorkOutsideSpendingCounters;
};

export type NewYorkOutsideSpendingDataClient = {
  getScheduleRAllocations: typeof getNewYorkScheduleRAllocations;
  getFilerRecords: typeof getNewYorkFilerRecords;
  getParentExpenditures: typeof getNewYorkParentExpenditures;
};

const DEFAULT_OUTSIDE_SPENDING_CLIENT: NewYorkOutsideSpendingDataClient = {
  getScheduleRAllocations: getNewYorkScheduleRAllocations,
  getFilerRecords: getNewYorkFilerRecords,
  getParentExpenditures: getNewYorkParentExpenditures,
};

// Parent amounts arrive as decimal strings; allow a cent of float slack.
const PARENT_AMOUNT_TOLERANCE = 0.01;

function roundCurrency(value: number): number {
  return Math.round(value * 100) / 100;
}

function allocationMatchesCandidate(
  row: NewYorkScheduleRAllocationRow,
  candidateName: string,
  candidateNameKeys: ReadonlySet<string>
): boolean {
  const rowName = [row.candidateFirstName, row.candidateMiddleName, row.candidateLastName]
    .filter((part) => part.length > 0)
    .join(" ");
  if (!rowName) {
    return false;
  }
  for (const key of normalizeNewYorkCandidateNameKeys(rowName)) {
    if (candidateNameKeys.has(key)) {
      return !newYorkCandidateNameMiddleConflict(candidateName, rowName);
    }
  }
  return false;
}

function exactlyOneScheduleFParentAmount(
  parents: readonly NewYorkParentExpenditureRow[] | undefined
): number | null {
  if (!parents || parents.length !== 1) {
    return null;
  }
  const parent = parents[0];
  return parent.scheduleAbbrev === "F" && parent.amount !== null && parent.amount > 0 ? parent.amount : null;
}

export function aggregateNewYorkOutsideSpending(input: {
  candidateName: string;
  allocations: readonly NewYorkScheduleRAllocationRow[];
  filerRecords: ReadonlyMap<string, NewYorkFilerRecord>;
  parentExpendituresByFiler: ReadonlyMap<string, ReadonlyMap<string, readonly NewYorkParentExpenditureRow[]>>;
}): NewYorkOutsideSpendingResult {
  const candidateNameKeys = normalizeNewYorkCandidateNameKeys(input.candidateName);
  const counters: NewYorkOutsideSpendingCounters = {
    allocationRowCount: input.allocations.length,
    nameMatchedRowCount: 0,
    duplicateTransactionRowCount: 0,
    nonIeCommitteeRowCount: 0,
    unresolvedMappingRowCount: 0,
    acceptedRowCount: 0,
  };
  const groups = new Map<string, NewYorkOutsideSpendingGroup>();
  const seenFilingTransIds = new Set<string>();
  // Several allocations may share one parent expenditure; their accepted sum
  // must never exceed it, or outside totals overstate the real spending.
  const acceptedAmountByParent = new Map<string, number>();

  for (const allocation of input.allocations) {
    if (!allocationMatchesCandidate(allocation, input.candidateName, candidateNameKeys)) {
      continue;
    }
    counters.nameMatchedRowCount += 1;

    if (allocation.amount <= 0) {
      counters.unresolvedMappingRowCount += 1;
      continue;
    }
    if (seenFilingTransIds.has(allocation.filingTransId)) {
      counters.duplicateTransactionRowCount += 1;
      continue;
    }
    seenFilingTransIds.add(allocation.filingTransId);

    const filerRecord = input.filerRecords.get(allocation.filerId);
    if (!filerRecord || filerRecord.committeeType !== NEW_YORK_INDEPENDENT_EXPENDITURE_COMMITTEE_TYPE) {
      counters.nonIeCommitteeRowCount += 1;
      continue;
    }

    if (!allocation.transMapping) {
      counters.unresolvedMappingRowCount += 1;
      continue;
    }
    const parents = input.parentExpendituresByFiler.get(allocation.filerId)?.get(allocation.transMapping);
    const parentAmount = exactlyOneScheduleFParentAmount(parents);
    if (parentAmount === null) {
      counters.unresolvedMappingRowCount += 1;
      continue;
    }
    const parentKey = `${allocation.filerId} ${allocation.transMapping}`;
    const acceptedForParent = acceptedAmountByParent.get(parentKey) ?? 0;
    if (acceptedForParent + allocation.amount > parentAmount + PARENT_AMOUNT_TOLERANCE) {
      counters.unresolvedMappingRowCount += 1;
      continue;
    }
    acceptedAmountByParent.set(parentKey, acceptedForParent + allocation.amount);

    counters.acceptedRowCount += 1;
    const supportOppose = allocation.supportOppose === "S" ? "support" : "oppose";
    const key = `${allocation.filerId}\u0000${supportOppose}`;
    const existing = groups.get(key);
    if (existing) {
      existing.amount = roundCurrency(existing.amount + allocation.amount);
      existing.allocationCount += 1;
      continue;
    }
    groups.set(key, {
      filerId: allocation.filerId,
      filerName: filerRecord.filerName,
      supportOppose,
      amount: roundCurrency(allocation.amount),
      allocationCount: 1,
      sourceUrl: NEW_YORK_SODA_DISCLOSURES_PAGE_URL,
    });
  }

  const sortedGroups = [...groups.values()].sort(
    (left, right) => right.amount - left.amount || left.filerName.localeCompare(right.filerName)
  );
  return {
    groups: sortedGroups,
    supportTotal: sumGroupAmounts(sortedGroups, "support"),
    opposeTotal: sumGroupAmounts(sortedGroups, "oppose"),
    counters,
  };
}

function sumGroupAmounts(groups: readonly NewYorkOutsideSpendingGroup[], supportOppose: "support" | "oppose"): number {
  return roundCurrency(
    groups.filter((group) => group.supportOppose === supportOppose).reduce((sum, group) => sum + group.amount, 0)
  );
}

export async function collectNewYorkOutsideSpending(
  input: {
    candidateName: string;
    officeScope: string;
    officeName: string;
    electionYear: number;
    district?: string | null;
    maxGroups?: number;
  },
  options: NewYorkSodaClientOptions = {},
  client: Partial<NewYorkOutsideSpendingDataClient> = {}
): Promise<NewYorkOutsideSpendingResult> {
  const dataClient: NewYorkOutsideSpendingDataClient = { ...DEFAULT_OUTSIDE_SPENDING_CLIENT, ...client };
  const officeSearch = toNewYorkBoeOfficeSearchInput({
    officeScope: input.officeScope,
    officeCanonicalName: input.officeName,
    district: input.district,
  });
  if (!officeSearch) {
    return {
      groups: [],
      supportTotal: 0,
      opposeTotal: 0,
      counters: {
        allocationRowCount: 0,
        nameMatchedRowCount: 0,
        duplicateTransactionRowCount: 0,
        nonIeCommitteeRowCount: 0,
        unresolvedMappingRowCount: 0,
        acceptedRowCount: 0,
      },
    };
  }

  const allocations = await dataClient.getScheduleRAllocations(
    {
      electionYear: input.electionYear,
      boeOfficeLabels: officeSearch.boeOfficeLabels,
      district: officeSearch.district,
    },
    options
  );

  const candidateNameKeys = normalizeNewYorkCandidateNameKeys(input.candidateName);
  const candidateAllocations = allocations.filter((allocation) =>
    allocationMatchesCandidate(allocation, input.candidateName, candidateNameKeys)
  );

  const filerIds = [...new Set(candidateAllocations.map((allocation) => allocation.filerId))];
  const filerRecords =
    filerIds.length > 0
      ? await dataClient.getFilerRecords({ filerIds }, options)
      : new Map<string, NewYorkFilerRecord>();

  // Only IE committees ever need the parent-expenditure validation lookups.
  const parentExpendituresByFiler = new Map<string, ReadonlyMap<string, readonly NewYorkParentExpenditureRow[]>>();
  for (const filerId of filerIds) {
    if (filerRecords.get(filerId)?.committeeType !== NEW_YORK_INDEPENDENT_EXPENDITURE_COMMITTEE_TYPE) {
      continue;
    }
    const transNumbers = candidateAllocations
      .filter((allocation) => allocation.filerId === filerId && allocation.transMapping)
      .map((allocation) => allocation.transMapping as string);
    if (transNumbers.length === 0) {
      continue;
    }
    parentExpendituresByFiler.set(filerId, await dataClient.getParentExpenditures({ filerId, transNumbers }, options));
  }

  const result = aggregateNewYorkOutsideSpending({
    candidateName: input.candidateName,
    allocations,
    filerRecords,
    parentExpendituresByFiler,
  });

  const maxGroups = input.maxGroups ?? Number.POSITIVE_INFINITY;
  return {
    // Groups are capped for persistence, but the totals stay uncapped.
    groups: (["support", "oppose"] as const).flatMap((supportOppose) =>
      result.groups.filter((group) => group.supportOppose === supportOppose).slice(0, maxGroups)
    ),
    supportTotal: result.supportTotal,
    opposeTotal: result.opposeTotal,
    counters: result.counters,
  };
}
