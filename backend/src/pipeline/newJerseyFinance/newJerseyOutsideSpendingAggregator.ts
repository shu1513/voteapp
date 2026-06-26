import {
  getNewJerseyElecEntityFilings,
  getNewJerseyElecReportDownload,
  type NewJerseyElecClientOptions,
  type NewJerseyElecFiling,
  type NewJerseyElecReportDownload,
} from "./newJerseyElecClient.js";
import { normalizeNewJerseyCandidateNameKeys } from "./newJerseyCandidateCommitteeResolver.js";

export type NewJerseySupportOppose = "support" | "oppose";

export type NewJerseyElecIndependentExpenditureAllocation = {
  supportOppose: NewJerseySupportOppose;
  office: string | null;
  candidateOrCommitteeName: string;
  electionDate: string;
  location: string | null;
  amount: number;
  sourceUrl: string | null;
  docId: number | null;
};

export type NewJerseyOutsideSpendingGroup = {
  entityS: number;
  entityName: string;
  supportOppose: NewJerseySupportOppose;
  amount: number;
  sourceUrl: string | null;
  docIds: number[];
};

export type NewJerseyOutsideSpendingSummary = {
  supportTotal: number;
  opposeTotal: number;
  groups: NewJerseyOutsideSpendingGroup[];
  sourceUrl: string | null;
};

export type NewJerseyOutsideSpendingReportText = {
  text: string;
  sourceUrl?: string | null;
  docId?: number | null;
};

export type NewJerseyOutsideSpendingAggregationInput = {
  candidateName: string;
  electionYear: number;
  outsideGroupEntityS: number;
  outsideGroupName: string;
  reportTexts: readonly NewJerseyOutsideSpendingReportText[];
  sourceUrl?: string | null;
  maxGroups?: number;
};

export type NewJerseyOutsideSpendingAggregationResult = {
  summary: NewJerseyOutsideSpendingSummary | null;
  matchedAllocationRowCount: number;
  includedAllocationRowCount: number;
  skippedAllocationRowCount: number;
};

export type NewJerseyReportTextExtractor = (input: {
  docId: number;
  reportUrl: string;
  filing: NewJerseyElecFiling;
  download: NewJerseyElecReportDownload;
}) => Promise<string | null>;

export type NewJerseyOutsideSpendingFromFilingsInput = {
  candidateName: string;
  electionYear: number;
  outsideGroupEntityS: number;
  outsideGroupName: string;
  sourceUrl?: string | null;
  maxFilings?: number;
  maxGroups?: number;
  clientOptions?: NewJerseyElecClientOptions;
  textExtractor: NewJerseyReportTextExtractor;
  filings?: readonly NewJerseyElecFiling[];
  elecClient?: Partial<{
    getEntityFilings: typeof getNewJerseyElecEntityFilings;
    getReportDownload: typeof getNewJerseyElecReportDownload;
  }>;
};

export type NewJerseyOutsideSpendingFromFilingsResult = NewJerseyOutsideSpendingAggregationResult & {
  filingRowCount: number;
  downloadedReportCount: number;
  extractedReportTextCount: number;
  skippedFilingRowCount: number;
};

type GroupAccumulator = {
  entityS: number;
  entityName: string;
  supportOppose: NewJerseySupportOppose;
  amountCents: number;
  sourceUrl: string | null;
  docIds: Set<number>;
};

const DEFAULT_MAX_GROUPS = 50;

const OFFICE_PREFIXES = [
  "NJ Gubernatorial",
  "Governor",
  "Lieutenant Governor",
  "State Senate",
  "General Assembly",
  "County",
  "Municipal",
] as const;

function requireNonEmpty(value: string, fieldName: string): string {
  const trimmed = value.trim();
  if (!trimmed) {
    throw new Error(`${fieldName} is required`);
  }
  return trimmed;
}

function normalizeEntityS(value: number): number {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new Error(`Invalid New Jersey outside spending entityS: ${value}`);
  }
  return value;
}

function normalizeElectionYear(value: number): number {
  if (!Number.isInteger(value) || value < 1980 || value > 2100) {
    throw new Error(`Invalid New Jersey outside spending election year: ${value}`);
  }
  return value;
}

function normalizePositiveInteger(value: number | undefined, fallback: number, fieldName: string): number {
  const normalized = value ?? fallback;
  if (!Number.isInteger(normalized) || normalized <= 0) {
    throw new Error(`Invalid New Jersey outside spending ${fieldName}: ${value}`);
  }
  return normalized;
}

function normalizeTextKey(value: string | null | undefined): string {
  return (value ?? "")
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toUpperCase()
    .replace(/&/g, " AND ")
    .replace(/[^A-Z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function amountToCents(amount: number): number | null {
  if (!Number.isFinite(amount)) {
    return null;
  }
  const cents = Math.round(amount * 100);
  return Number.isSafeInteger(cents) ? cents : null;
}

function centsToDollars(cents: number): number {
  return cents / 100;
}

function parseCurrencyAmount(raw: string): number | null {
  const parsed = Number(raw.replace(/[$,]/g, ""));
  return Number.isFinite(parsed) ? parsed : null;
}

function parseDateYear(raw: string | null | undefined): number | null {
  const trimmed = raw?.trim();
  if (!trimmed) {
    return null;
  }
  const slashMatch = /^(\d{1,2})\/(\d{1,2})\/(\d{4})\b/.exec(trimmed);
  if (slashMatch?.[3]) {
    return Number(slashMatch[3]);
  }
  const isoMatch = /^(\d{4})-\d{2}-\d{2}/.exec(trimmed);
  if (isoMatch?.[1]) {
    return Number(isoMatch[1]);
  }
  return null;
}

function supportOpposeFromHeading(line: string): NewJerseySupportOppose | null {
  const normalized = normalizeTextKey(line);
  if (normalized.includes("EXPENDITURES BENEFITING CANDIDATE") || normalized.includes("BENEFITING CANDIDATE")) {
    return "support";
  }
  if (normalized.includes("EXPENDITURES OPPOSING CANDIDATE") || normalized.includes("OPPOSING CANDIDATE")) {
    return "oppose";
  }
  return null;
}

function splitOfficeAndCandidate(rawBeforeDate: string): { office: string | null; candidateOrCommitteeName: string } {
  const compact = rawBeforeDate.trim().replace(/\s+/g, " ");
  for (const office of OFFICE_PREFIXES) {
    const prefix = `${office} `;
    if (compact.toUpperCase().startsWith(prefix.toUpperCase())) {
      return {
        office,
        candidateOrCommitteeName: compact.slice(prefix.length).trim(),
      };
    }
  }
  return {
    office: null,
    candidateOrCommitteeName: compact,
  };
}

function parseAllocationLine(input: {
  line: string;
  supportOppose: NewJerseySupportOppose;
  sourceUrl: string | null;
  docId: number | null;
}): NewJerseyElecIndependentExpenditureAllocation | null {
  const match = /^(?<beforeDate>.+?)\s+(?<electionDate>\d{1,2}\/\d{1,2}\/\d{4})\s+(?<location>.+?)\s+\$(?<amount>[\d,]+(?:\.\d{2})?)$/.exec(
    input.line.trim().replace(/\s+/g, " ")
  );
  const groups = match?.groups;
  if (!groups?.beforeDate || !groups.electionDate || !groups.amount) {
    return null;
  }

  const amount = parseCurrencyAmount(groups.amount);
  if (amount === null) {
    return null;
  }
  const { office, candidateOrCommitteeName } = splitOfficeAndCandidate(groups.beforeDate);
  if (!candidateOrCommitteeName) {
    return null;
  }

  return {
    supportOppose: input.supportOppose,
    office,
    candidateOrCommitteeName,
    electionDate: groups.electionDate,
    location: groups.location?.trim().replace(/\s+/g, " ") || null,
    amount,
    sourceUrl: input.sourceUrl,
    docId: input.docId,
  };
}

export function parseNewJerseyElecIndependentExpenditureAllocations(input: {
  text: string;
  sourceUrl?: string | null;
  docId?: number | null;
}): NewJerseyElecIndependentExpenditureAllocation[] {
  const sourceUrl = input.sourceUrl ?? null;
  const docId = input.docId ?? null;
  const lines = input.text
    .split(/\r?\n/)
    .map((line) => line.trim().replace(/\s+/g, " "))
    .filter(Boolean);
  const allocations: NewJerseyElecIndependentExpenditureAllocation[] = [];
  let currentSupportOppose: NewJerseySupportOppose | null = null;

  for (const line of lines) {
    const headingSupportOppose = supportOpposeFromHeading(line);
    if (headingSupportOppose) {
      currentSupportOppose = headingSupportOppose;
      continue;
    }
    if (!currentSupportOppose) {
      continue;
    }

    const allocation = parseAllocationLine({
      line,
      supportOppose: currentSupportOppose,
      sourceUrl,
      docId,
    });
    if (allocation) {
      allocations.push(allocation);
    }
  }

  return allocations;
}

function candidateMatchesAllocation(input: {
  candidateName: string;
  candidateOrCommitteeName: string;
}): boolean {
  const allocationKey = normalizeTextKey(input.candidateOrCommitteeName);
  if (!allocationKey) {
    return false;
  }
  for (const key of normalizeNewJerseyCandidateNameKeys(input.candidateName)) {
    if (allocationKey.includes(key)) {
      return true;
    }
  }
  return false;
}

function groupKey(input: { entityS: number; supportOppose: NewJerseySupportOppose }): string {
  return `${input.entityS}\u0000${input.supportOppose}`;
}

function addGroup(
  groups: Map<string, GroupAccumulator>,
  input: {
    entityS: number;
    entityName: string;
    supportOppose: NewJerseySupportOppose;
    amountCents: number;
    sourceUrl: string | null;
    docId: number | null;
  }
): void {
  const key = groupKey(input);
  const existing = groups.get(key);
  if (!existing) {
    groups.set(key, {
      entityS: input.entityS,
      entityName: input.entityName,
      supportOppose: input.supportOppose,
      amountCents: input.amountCents,
      sourceUrl: input.sourceUrl,
      docIds: input.docId ? new Set([input.docId]) : new Set(),
    });
    return;
  }

  existing.amountCents += input.amountCents;
  if (!existing.sourceUrl && input.sourceUrl) {
    existing.sourceUrl = input.sourceUrl;
  }
  if (input.docId) {
    existing.docIds.add(input.docId);
  }
}

function toGroups(input: { groups: Iterable<GroupAccumulator>; maxGroups: number }): NewJerseyOutsideSpendingGroup[] {
  return [...input.groups]
    .sort(
      (left, right) =>
        right.amountCents - left.amountCents ||
        left.supportOppose.localeCompare(right.supportOppose) ||
        left.entityName.localeCompare(right.entityName)
    )
    .slice(0, input.maxGroups)
    .map((group) => ({
      entityS: group.entityS,
      entityName: group.entityName,
      supportOppose: group.supportOppose,
      amount: centsToDollars(group.amountCents),
      sourceUrl: group.sourceUrl,
      docIds: [...group.docIds].sort((left, right) => left - right),
    }));
}

export function aggregateNewJerseyOutsideSpending(
  input: NewJerseyOutsideSpendingAggregationInput
): NewJerseyOutsideSpendingAggregationResult {
  const candidateName = requireNonEmpty(input.candidateName, "New Jersey outside spending candidate name");
  const electionYear = normalizeElectionYear(input.electionYear);
  const entityS = normalizeEntityS(input.outsideGroupEntityS);
  const entityName = requireNonEmpty(input.outsideGroupName, "New Jersey outside group name").replace(/\s+/g, " ");
  const maxGroups = normalizePositiveInteger(input.maxGroups, DEFAULT_MAX_GROUPS, "maxGroups");
  const fallbackSourceUrl = input.sourceUrl ?? null;
  const groups = new Map<string, GroupAccumulator>();
  let matchedAllocationRowCount = 0;
  let includedAllocationRowCount = 0;
  let skippedAllocationRowCount = 0;
  let supportTotalCents = 0;
  let opposeTotalCents = 0;

  for (const reportText of input.reportTexts) {
    for (const allocation of parseNewJerseyElecIndependentExpenditureAllocations(reportText)) {
      if (!candidateMatchesAllocation({ candidateName, candidateOrCommitteeName: allocation.candidateOrCommitteeName })) {
        continue;
      }
      matchedAllocationRowCount += 1;
      const amountCents = amountToCents(allocation.amount);
      if (
        amountCents === null ||
        amountCents <= 0 ||
        parseDateYear(allocation.electionDate) !== electionYear
      ) {
        skippedAllocationRowCount += 1;
        continue;
      }

      includedAllocationRowCount += 1;
      if (allocation.supportOppose === "support") {
        supportTotalCents += amountCents;
      } else {
        opposeTotalCents += amountCents;
      }
      addGroup(groups, {
        entityS,
        entityName,
        supportOppose: allocation.supportOppose,
        amountCents,
        sourceUrl: allocation.sourceUrl ?? fallbackSourceUrl,
        docId: allocation.docId,
      });
    }
  }

  const groupedRows = toGroups({ groups: groups.values(), maxGroups });
  return {
    summary:
      groupedRows.length > 0
        ? {
            supportTotal: centsToDollars(supportTotalCents),
            opposeTotal: centsToDollars(opposeTotalCents),
            groups: groupedRows,
            sourceUrl: fallbackSourceUrl,
          }
        : null,
    matchedAllocationRowCount,
    includedAllocationRowCount,
    skippedAllocationRowCount,
  };
}

function reportTextMentionsCandidate(input: { text: string; candidateName: string }): boolean {
  const textKey = normalizeTextKey(input.text);
  if (!textKey) {
    return false;
  }
  for (const key of normalizeNewJerseyCandidateNameKeys(input.candidateName)) {
    if (textKey.includes(key)) {
      return true;
    }
  }
  return false;
}

function combineOutsideSpendingResults(input: {
  candidateName: string;
  sourceUrl: string | null;
  results: readonly NewJerseyOutsideSpendingAggregationResult[];
  extraSkippedAllocationRowCount: number;
  maxGroups: number;
}): NewJerseyOutsideSpendingAggregationResult {
  const groups = new Map<string, GroupAccumulator>();
  let supportTotalCents = 0;
  let opposeTotalCents = 0;
  let matchedAllocationRowCount = 0;
  let includedAllocationRowCount = 0;
  let skippedAllocationRowCount = input.extraSkippedAllocationRowCount;

  for (const result of input.results) {
    matchedAllocationRowCount += result.matchedAllocationRowCount;
    includedAllocationRowCount += result.includedAllocationRowCount;
    skippedAllocationRowCount += result.skippedAllocationRowCount;
    supportTotalCents += Math.round((result.summary?.supportTotal ?? 0) * 100);
    opposeTotalCents += Math.round((result.summary?.opposeTotal ?? 0) * 100);

    for (const group of result.summary?.groups ?? []) {
      addGroup(groups, {
        entityS: group.entityS,
        entityName: group.entityName,
        supportOppose: group.supportOppose,
        amountCents: Math.round(group.amount * 100),
        sourceUrl: group.sourceUrl,
        docId: null,
      });
      const key = groupKey(group);
      const existing = groups.get(key);
      for (const docId of group.docIds) {
        existing?.docIds.add(docId);
      }
    }
  }

  const groupedRows = toGroups({ groups: groups.values(), maxGroups: input.maxGroups });
  return {
    summary:
      groupedRows.length > 0
        ? {
            supportTotal: centsToDollars(supportTotalCents),
            opposeTotal: centsToDollars(opposeTotalCents),
            groups: groupedRows,
            sourceUrl: input.sourceUrl,
          }
        : null,
    matchedAllocationRowCount,
    includedAllocationRowCount,
    skippedAllocationRowCount,
  };
}

export async function aggregateNewJerseyOutsideSpendingFromElecFilings(
  input: NewJerseyOutsideSpendingFromFilingsInput
): Promise<NewJerseyOutsideSpendingFromFilingsResult> {
  const candidateName = requireNonEmpty(input.candidateName, "New Jersey outside spending candidate name");
  const electionYear = normalizeElectionYear(input.electionYear);
  const outsideGroupEntityS = normalizeEntityS(input.outsideGroupEntityS);
  const outsideGroupName = requireNonEmpty(input.outsideGroupName, "New Jersey outside group name");
  const maxGroups = normalizePositiveInteger(input.maxGroups, DEFAULT_MAX_GROUPS, "maxGroups");
  const maxFilings = input.maxFilings === undefined
    ? Number.POSITIVE_INFINITY
    : normalizePositiveInteger(input.maxFilings, DEFAULT_MAX_GROUPS, "maxFilings");
  const getEntityFilings = input.elecClient?.getEntityFilings ?? getNewJerseyElecEntityFilings;
  const getReportDownload = input.elecClient?.getReportDownload ?? getNewJerseyElecReportDownload;
  const filings = (input.filings ?? (await getEntityFilings({ entityS: outsideGroupEntityS }, input.clientOptions)))
    .filter((filing) => filing.publicAccess)
    .slice(0, maxFilings);

  const results: NewJerseyOutsideSpendingAggregationResult[] = [];
  let downloadedReportCount = 0;
  let extractedReportTextCount = 0;
  let skippedFilingRowCount = 0;
  let extraSkippedAllocationRowCount = 0;

  for (const filing of filings) {
    let download: NewJerseyElecReportDownload;
    try {
      download = await getReportDownload(filing.docId, input.clientOptions);
      downloadedReportCount += 1;
    } catch {
      skippedFilingRowCount += 1;
      extraSkippedAllocationRowCount += 1;
      continue;
    }

    let text: string | null;
    try {
      text = await input.textExtractor({
        docId: filing.docId,
        reportUrl: download.fileNameWithSas,
        filing,
        download,
      });
    } catch {
      text = null;
    }

    if (!text?.trim()) {
      skippedFilingRowCount += 1;
      extraSkippedAllocationRowCount += 1;
      continue;
    }
    extractedReportTextCount += 1;

    const result = aggregateNewJerseyOutsideSpending({
      candidateName,
      electionYear,
      outsideGroupEntityS,
      outsideGroupName,
      reportTexts: [
        {
          text,
          sourceUrl: download.sourceUrl || filing.reportDownloadUrl,
          docId: filing.docId,
        },
      ],
      sourceUrl: input.sourceUrl ?? download.sourceUrl ?? filing.reportDownloadUrl,
      maxGroups,
    });
    if (result.matchedAllocationRowCount === 0 && reportTextMentionsCandidate({ text, candidateName })) {
      extraSkippedAllocationRowCount += 1;
      skippedFilingRowCount += 1;
    }
    results.push(result);
  }

  return {
    ...combineOutsideSpendingResults({
      candidateName,
      sourceUrl: input.sourceUrl ?? null,
      results,
      extraSkippedAllocationRowCount,
      maxGroups,
    }),
    filingRowCount: filings.length,
    downloadedReportCount,
    extractedReportTextCount,
    skippedFilingRowCount,
  };
}
