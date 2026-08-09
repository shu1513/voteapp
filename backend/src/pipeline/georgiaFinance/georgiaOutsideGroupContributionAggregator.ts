import { normalizeFinanceLabel } from "../finance/financeLabelClassifier.js";
import {
  GEORGIA_INDIVIDUAL_SOURCE_TYPE_CODE_BY_HOST,
  isGeorgiaRecognizedTransactionStatus,
  type GeorgiaTransactionRow,
} from "./georgiaEthicsClient.js";
import { GEORGIA_TRANSACTION_SUB_TYPE_CLASS_BY_HOST_CODE } from "./georgiaDirectContributionAggregator.js";
import type { GeorgiaOutsideSpendingGroup, GeorgiaSupportOppose } from "./georgiaOutsideSpendingAggregator.js";
import type { GeorgiaFinanceOutsideGroupBreakdownInput } from "./georgiaFinanceWriter.js";

// Funders of Georgia outside spenders (georgia_plan.md PR 6, maryland/ohio
// donor+industry pattern): each IE spender is an ordinary PeachFile filer, so
// its itemized contributions come from the same TCON search the direct leg
// uses. The sync layer fetches the rows per spender (scoped by registration
// guid — the group identity from the IE leg); this module is a pure
// aggregation over those pre-scoped rows.
//
// The outside-breakdown schema allows only donor + industry categories, so
// ORGANIZATION contributors are the only donors surfaced — an individual's
// money has no outside category to land in. The org test uses the structured
// per-host source-type code first (TIND individual / TBSN business, A8) and
// falls back to a name-shape heuristic when the code is absent.
//
// Donor rows are UNCAPPED here (sorted by amount within each group). The
// sync layer classifies every donor and only caps the PERSISTED display rows
// — capping here would silently drop tail donors from the rebuilt industry
// totals of a >cap-donor group. Industry rows are NOT built here: the sync
// rebuilds them from the merged classification state (rules + cached manual
// verdicts), so a manual industry label always wins.

// Rows keyed by the spender's lowercase PeachFile registration guid — the
// outside-group committeeId from the IE leg.
export type GeorgiaSpenderContributionRowsByGuid = ReadonlyMap<string, readonly GeorgiaTransactionRow[]>;

export type GeorgiaOutsideGroupContributionAggregationInput = {
  electionYear: number;
  // The capped group list actually being written this sync — breakdowns must
  // pair with these rows (writer pairing validation).
  outsideGroups: readonly GeorgiaOutsideSpendingGroup[];
  contributionRowsBySpender: GeorgiaSpenderContributionRowsByGuid;
  sourceUrl?: string | null;
};

export type GeorgiaOutsideGroupContributionAggregationResult = {
  outsideGroupBreakdowns: GeorgiaFinanceOutsideGroupBreakdownInput[];
  matchedContributionRowCount: number;
  includedContributionRowCount: number;
  skippedContributionRowCount: number;
};

type DonorAggregate = {
  committeeId: string;
  supportOppose: GeorgiaSupportOppose;
  displayName: string;
  normalizedName: string;
  amountCents: number;
};

// Name-shape fallback for rows without a source-type code (the pinned
// PeachFile vocabulary is TIND / TBSN / null, A8). Tennessee's single-string
// convention: a comma name ("Last, First") without an organization word is an
// individual; otherwise an organization word decides.
const ORGANIZATION_NAME_PATTERN =
  /\b(INC|INCORPORATED|LLC|L L C|LP|L P|LLP|L L P|LTD|LIMITED|CO|COMPANY|CORP|CORPORATION|PLC|PAC|COMMITTEE|ASSOCIATION|UNION|FOUNDATION|FUND|TRUST|PARTNERS|PARTNERSHIP|BANK|GROUP|COALITION|CLUB|PARTY|LOCAL|ENTERPRISES|INDUSTRIES|HOLDINGS?)\b/;

function normalizeElectionYear(value: number): number {
  if (!Number.isInteger(value) || value < 2000 || value > 2100) {
    throw new Error(`Invalid Georgia outside group contribution election year: ${value}`);
  }
  return value;
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

function cleanDisplayLabel(value: string | null | undefined): string {
  return (value ?? "").trim().replace(/\s+/g, " ");
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

function parseDateYear(raw: string | null | undefined): number | null {
  const trimmed = raw?.trim();
  if (!trimmed) {
    return null;
  }
  const isoMatch = /^(\d{4})-\d{2}-\d{2}/.exec(trimmed);
  if (isoMatch?.[1]) {
    return Number(isoMatch[1]);
  }
  const slashMatch = /^(\d{1,2})\/(\d{1,2})\/(\d{4})\b/.exec(trimmed);
  if (slashMatch?.[3]) {
    return Number(slashMatch[3]);
  }
  return null;
}

// Cycle gate (tennessee/maryland convention): the transaction date's year
// leads, the row's electionYear field fills in when the date is missing or
// unparseable, and a row with neither is excluded. The store holds garbage
// dates on valid rows (A4), which for this enrichment-only leg costs a donor
// row at worst — never a reconciled total.
function isCycleRow(row: GeorgiaTransactionRow, electionYear: number): boolean {
  const year = parseDateYear(row.transactionDate) ?? row.electionYear;
  return year !== null && year >= electionYear - 1 && year <= electionYear;
}

function isOrganizationContributor(row: GeorgiaTransactionRow): boolean {
  const sourceTypeCode = row.transactionSourceTypeCode?.trim();
  if (sourceTypeCode === GEORGIA_INDIVIDUAL_SOURCE_TYPE_CODE_BY_HOST.peachfile) {
    return false;
  }
  if (sourceTypeCode === "TBSN") {
    return true;
  }
  const rawName = row.sourceName?.trim() ?? "";
  const nameKey = normalizeTextKey(row.sourceName);
  if (!nameKey) {
    return false;
  }
  if (rawName.includes(",") && !ORGANIZATION_NAME_PATTERN.test(nameKey)) {
    return false;
  }
  return ORGANIZATION_NAME_PATTERN.test(nameKey);
}

// Monetary-itemized and in-kind rows both carry a real contributor name and
// are both genuine funding (maryland counts in-kind donor money the same
// way); unitemized and anonymous rows carry no donor identity.
function isDonorSubType(row: GeorgiaTransactionRow): boolean {
  const code = row.transactionSubTypeCode?.trim();
  if (!code) {
    return false;
  }
  const subClass = GEORGIA_TRANSACTION_SUB_TYPE_CLASS_BY_HOST_CODE.peachfile[
    code as keyof typeof GEORGIA_TRANSACTION_SUB_TYPE_CLASS_BY_HOST_CODE.peachfile
  ];
  return subClass === "itemized" || subClass === "in_kind";
}

function donorKey(input: { committeeId: string; supportOppose: GeorgiaSupportOppose; normalizedName: string }): string {
  return `${input.committeeId}\u0000${input.supportOppose}\u0000${input.normalizedName}`;
}

export function aggregateGeorgiaOutsideGroupContributions(
  input: GeorgiaOutsideGroupContributionAggregationInput
): GeorgiaOutsideGroupContributionAggregationResult {
  const electionYear = normalizeElectionYear(input.electionYear);
  const sourceUrl = input.sourceUrl ?? null;

  // The same spender can appear as two group rows (support + oppose); its
  // donors attach to each direction row it funded.
  const groupsBySpenderGuid = new Map<string, GeorgiaOutsideSpendingGroup[]>();
  for (const group of input.outsideGroups) {
    const guid = group.committeeId.trim().toLowerCase();
    if (!guid) {
      continue;
    }
    const existing = groupsBySpenderGuid.get(guid) ?? [];
    existing.push(group);
    groupsBySpenderGuid.set(guid, existing);
  }

  const donors = new Map<string, DonorAggregate>();
  let matchedContributionRowCount = 0;
  let includedContributionRowCount = 0;
  let skippedContributionRowCount = 0;

  for (const [spenderGuid, rows] of input.contributionRowsBySpender) {
    const matchingGroups = groupsBySpenderGuid.get(spenderGuid.trim().toLowerCase()) ?? [];
    if (matchingGroups.length === 0) {
      continue;
    }
    for (const row of rows) {
      matchedContributionRowCount += 1;
      const amountCents = amountToCents(row.transactionAmount);
      const displayName = cleanDisplayLabel(row.sourceName);
      const normalizedName = displayName ? normalizeFinanceLabel(displayName, "donor") : "";
      if (
        amountCents === null ||
        amountCents <= 0 ||
        !isGeorgiaRecognizedTransactionStatus("peachfile", row.transactionStatusCode) ||
        !isDonorSubType(row) ||
        !isCycleRow(row, electionYear) ||
        !displayName ||
        !normalizedName ||
        !isOrganizationContributor(row)
      ) {
        skippedContributionRowCount += 1;
        continue;
      }

      includedContributionRowCount += 1;
      for (const group of matchingGroups) {
        const key = donorKey({
          committeeId: group.committeeId,
          supportOppose: group.supportOppose,
          normalizedName,
        });
        const existing = donors.get(key);
        if (existing) {
          existing.amountCents += amountCents;
          continue;
        }
        donors.set(key, {
          committeeId: group.committeeId,
          supportOppose: group.supportOppose,
          displayName,
          normalizedName,
          amountCents,
        });
      }
    }
  }

  const donorsByGroup = new Map<string, DonorAggregate[]>();
  for (const donor of donors.values()) {
    const key = `${donor.committeeId}\u0000${donor.supportOppose}`;
    const list = donorsByGroup.get(key) ?? [];
    list.push(donor);
    donorsByGroup.set(key, list);
  }

  const outsideGroupBreakdowns: GeorgiaFinanceOutsideGroupBreakdownInput[] = [];
  for (const groupKey of [...donorsByGroup.keys()].sort()) {
    for (const donor of donorsByGroup
      .get(groupKey)!
      .sort((left, right) => right.amountCents - left.amountCents || left.displayName.localeCompare(right.displayName))) {
      outsideGroupBreakdowns.push({
        committeeId: donor.committeeId,
        supportOppose: donor.supportOppose,
        categoryType: "donor",
        categoryName: donor.displayName,
        amount: centsToDollars(donor.amountCents),
        contributorCount: 1,
        sourceUrl,
      });
    }
  }

  return {
    outsideGroupBreakdowns,
    matchedContributionRowCount,
    includedContributionRowCount,
    skippedContributionRowCount,
  };
}
