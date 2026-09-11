import { classifyFinanceLabel, normalizeFinanceLabel } from "../finance/financeLabelClassifier.js";
import type { OhioFinanceOutsideGroup } from "./ohioOutsideSpendingAggregator.js";
import type { OhioFinanceOutsideGroupBreakdownInput } from "./ohioFinanceWriter.js";
import type { OhioSosContributionRow } from "./ohioSosBulkFiles.js";

// Outside-group funder aggregation for Ohio (docs/plans/ohio_plan.md PR 8, #3):
// who funds the committees behind a candidate's outside spending. The
// maryland pattern (marylandOutsideGroupContributionAggregator.ts), adapted
// to the Ohio SoS bulk rows: matching is by the spender committee's numeric
// MASTER_KEY (exact — never by name), and organization donors come from the
// explicit NON_INDIVIDUAL column instead of a name heuristic.
//
// The standard writer only accepts donor and industry outside breakdowns,
// so individual contributors never become labels: a row without a
// NON_INDIVIDUAL value is counted and skipped. Donor money is the pinned
// contribution vocabulary below — anything outside it fails closed into a
// counter instead of a breakdown.
//
// The caller (batch sync) owns streaming the PAC/party contribution files
// and passes only the rows whose MASTER_KEY belongs to a 31-U spender, so
// this aggregator holds a small filtered slice, never a whole file.

export type OhioOutsideGroupContributionAggregationInput = {
  electionYear: number;
  // One candidate's outside groups (the 31-U aggregation slice being
  // written); the same committee may appear once per direction.
  outsideGroups: readonly OhioFinanceOutsideGroup[];
  // Contribution rows for 31-U spender committees (pre-filtered by the
  // caller; rows for other committees are ignored here anyway).
  contributionRows: readonly OhioSosContributionRow[];
  sourceUrl?: string | null;
  minIndustryAmount?: number;
};

export type OhioOutsideGroupContributionAggregationResult = {
  // ALL donor rows, uncapped (sorted by amount within each group). The sync
  // layer classifies every donor and only caps the PERSISTED donor display
  // rows — capping here would silently drop tail donors from the rebuilt
  // industry totals of a >cap-donor group.
  outsideGroupBreakdowns: OhioFinanceOutsideGroupBreakdownInput[];
  // Rows whose MASTER_KEY matched one of the candidate's outside groups.
  matchedContributionRowCount: number;
  // Matched rows that became donor money.
  includedContributionRowCount: number;
  // Matched rows that did not: matched = included + skipped, and the four
  // reason counters below partition skipped.
  skippedContributionRowCount: number;
  // Individual contributions (no NON_INDIVIDUAL value) — real receipts,
  // just never donor labels.
  individualRowCount: number;
  // NON_INDIVIDUAL values that describe the transaction instead of naming
  // an entity ("CONTRIBUTION FROM DUES MONEY", "TRANSFER OF MEMBERSHIP
  // DUES") — real union-PAC receipts whose label is unusable as a donor.
  nonEntityLabelRowCount: number;
  // Missing/non-positive amount, or RPT_YEAR outside the cycle window.
  unusableRowCount: number;
  // Pinned non-donor vocabulary (other income, party restricted fund).
  excludedShortDescriptionRowCount: number;
  // SHORT_DESCRIPTION outside the pinned vocabulary entirely — a growing
  // count means the portal added a form type.
  unknownShortDescriptionRowCount: number;
};

type DonorAggregate = {
  committeeId: string;
  supportOppose: "support" | "oppose";
  displayName: string;
  normalizedName: string;
  amountCents: number;
};

type IndustryAggregate = {
  committeeId: string;
  supportOppose: "support" | "oppose";
  industrySlug: string;
  amountCents: number;
  donorKeys: Set<string>;
};

const DEFAULT_MIN_INDUSTRY_AMOUNT_CENTS = 0;

// The pinned donor vocabulary, from the real 2026-cycle PAC/party
// contribution files (same forms as the CAC_CON direct vocabulary).
const DONOR_SHORT_DESCRIPTIONS = new Set([
  "31-A STMT OF CONTRIBUTION",
  "31-E FR CONTRIBUTIONS",
  "31-J-1 IN-KIND CONT RCVD",
]);
// Known non-donor receipts: other income is not a donor, and the party
// restricted fund is a separate legal account (party files only).
const EXCLUDED_SHORT_DESCRIPTIONS = new Set([
  "31-A-2 OTHER INCOME",
  "31-CC ST. POLITICAL PARTY REST. FUND",
]);

// Union PACs routinely put a transaction description in NON_INDIVIDUAL
// ("CONTRIBUTION FROM DUES MONEY", "SOLELY FROM MEMBERSHIP DUES",
// "TRANSFER OF MEMBERSHIP DUES"), observed on the real 2026-cycle files. A
// value is rejected only when it BOTH starts like a generic description AND
// carries dues/transfer context, so entity-bearing values ("OHIO AFL-CIO/
// DUES MONEY", "UAW REGION 2B MEMBERSHIP DUES") keep their verbatim label.
const NON_ENTITY_LABEL_START =
  /^(AGGREGATE|CONTRIBUTIONS?|DUES|LABOR UNION|MEMBER|MEMBERSHIP|PROCEEDS|SOLELY|TRANSFERS?)\b/;
const NON_ENTITY_LABEL_CONTEXT = /\b(DUES|MONEY|MONIES|FUNDS?|DEDUCTIONS?|ASSESSMENTS?|MEMBERS?)\b/;

function isNonEntityDescriptionLabel(value: string): boolean {
  const upper = value.toUpperCase();
  return NON_ENTITY_LABEL_START.test(upper) && NON_ENTITY_LABEL_CONTEXT.test(upper);
}

function normalizeElectionYear(value: number): number {
  if (!Number.isInteger(value) || value < 2000 || value > 2100) {
    throw new Error(`Invalid Ohio outside group contribution election year: ${value}`);
  }
  return value;
}

function normalizeMinAmount(value: number | undefined): number {
  if (value === undefined) {
    return DEFAULT_MIN_INDUSTRY_AMOUNT_CENTS;
  }
  if (!Number.isFinite(value) || value < 0) {
    throw new Error(`Invalid Ohio outside group contribution minIndustryAmount: ${value}`);
  }
  return Math.round(value * 100);
}

function normalizeShortDescription(value: string | null): string {
  return (value ?? "").toUpperCase().replace(/\s+/g, " ").trim();
}

function cleanDisplayLabel(value: string): string {
  return value.trim().replace(/\s+/g, " ");
}

function centsToDollars(cents: number): number {
  return cents / 100;
}

function groupKey(input: { committeeId: string; supportOppose: "support" | "oppose" }): string {
  return `${input.committeeId}\u0000${input.supportOppose}`;
}

function donorAggregateKey(input: {
  committeeId: string;
  supportOppose: "support" | "oppose";
  normalizedName: string;
}): string {
  return `${input.committeeId}\u0000${input.supportOppose}\u0000${input.normalizedName}`;
}

function industryAggregateKey(input: {
  committeeId: string;
  supportOppose: "support" | "oppose";
  industrySlug: string;
}): string {
  return `${input.committeeId}\u0000${input.supportOppose}\u0000${input.industrySlug}`;
}

function toBreakdowns(input: {
  donors: Iterable<DonorAggregate>;
  industries: Iterable<IndustryAggregate>;
  sourceUrl: string | null;
}): OhioFinanceOutsideGroupBreakdownInput[] {
  const result: OhioFinanceOutsideGroupBreakdownInput[] = [];
  const donorsByGroup = new Map<string, DonorAggregate[]>();
  const industriesByGroup = new Map<string, IndustryAggregate[]>();

  for (const donor of input.donors) {
    const key = groupKey(donor);
    const list = donorsByGroup.get(key) ?? [];
    list.push(donor);
    donorsByGroup.set(key, list);
  }
  for (const industry of input.industries) {
    const key = groupKey(industry);
    const list = industriesByGroup.get(key) ?? [];
    list.push(industry);
    industriesByGroup.set(key, list);
  }

  for (const donors of donorsByGroup.values()) {
    for (const donor of donors
      .sort((left, right) => right.amountCents - left.amountCents || left.displayName.localeCompare(right.displayName))) {
      result.push({
        committeeId: donor.committeeId,
        supportOppose: donor.supportOppose,
        categoryType: "donor",
        categoryName: donor.displayName,
        amount: centsToDollars(donor.amountCents),
        contributorCount: 1,
        sourceUrl: input.sourceUrl,
      });
    }
  }

  for (const industries of industriesByGroup.values()) {
    for (const industry of industries
      .sort((left, right) => right.amountCents - left.amountCents || left.industrySlug.localeCompare(right.industrySlug))) {
      result.push({
        committeeId: industry.committeeId,
        supportOppose: industry.supportOppose,
        categoryType: "industry",
        categoryName: industry.industrySlug,
        amount: centsToDollars(industry.amountCents),
        contributorCount: industry.donorKeys.size,
        sourceUrl: input.sourceUrl,
      });
    }
  }

  return result;
}

export function aggregateOhioOutsideGroupContributions(
  input: OhioOutsideGroupContributionAggregationInput
): OhioOutsideGroupContributionAggregationResult {
  const electionYear = normalizeElectionYear(input.electionYear);
  const cycleStartYear = electionYear - 1;
  const minIndustryAmountCents = normalizeMinAmount(input.minIndustryAmount);

  // The same committee appears once per direction it spent in; a donor row
  // funds the committee, so it projects onto every direction's group.
  const groupsByCommitteeId = new Map<string, OhioFinanceOutsideGroup[]>();
  for (const group of input.outsideGroups) {
    const committeeId = group.committeeId.trim();
    if (!/^[0-9]+$/.test(committeeId)) {
      continue;
    }
    const existing = groupsByCommitteeId.get(committeeId) ?? [];
    existing.push(group);
    groupsByCommitteeId.set(committeeId, existing);
  }

  if (groupsByCommitteeId.size === 0) {
    return {
      outsideGroupBreakdowns: [],
      matchedContributionRowCount: 0,
      includedContributionRowCount: 0,
      skippedContributionRowCount: 0,
      individualRowCount: 0,
      nonEntityLabelRowCount: 0,
      unusableRowCount: 0,
      excludedShortDescriptionRowCount: 0,
      unknownShortDescriptionRowCount: 0,
    };
  }

  const donors = new Map<string, DonorAggregate>();
  let matchedContributionRowCount = 0;
  let includedContributionRowCount = 0;
  let skippedContributionRowCount = 0;
  let individualRowCount = 0;
  let nonEntityLabelRowCount = 0;
  let unusableRowCount = 0;
  let excludedShortDescriptionRowCount = 0;
  let unknownShortDescriptionRowCount = 0;

  for (const row of input.contributionRows) {
    const committeeId = row.masterKey.trim();
    const matchingGroups = groupsByCommitteeId.get(committeeId);
    if (!matchingGroups) {
      continue;
    }
    matchedContributionRowCount += 1;

    // Same cycle-window rule as the direct aggregator: the bulk files are
    // already RPT_YEAR-scoped, so an out-of-window row means the caller fed
    // the wrong file (or portal damage) and is skipped, never included.
    if (
      row.amountCents === null ||
      row.amountCents <= 0 ||
      row.reportYear === null ||
      row.reportYear < cycleStartYear ||
      row.reportYear > electionYear
    ) {
      unusableRowCount += 1;
      skippedContributionRowCount += 1;
      continue;
    }

    const shortDescription = normalizeShortDescription(row.shortDescription);
    if (!DONOR_SHORT_DESCRIPTIONS.has(shortDescription)) {
      if (EXCLUDED_SHORT_DESCRIPTIONS.has(shortDescription)) {
        excludedShortDescriptionRowCount += 1;
      } else {
        unknownShortDescriptionRowCount += 1;
      }
      skippedContributionRowCount += 1;
      continue;
    }

    // Organization donors only, from the explicit NON_INDIVIDUAL column —
    // individuals never become donor labels (fail closed, no name
    // heuristics).
    const displayName = cleanDisplayLabel(row.nonIndividual ?? "");
    const normalizedName = displayName ? normalizeFinanceLabel(displayName, "donor") : "";
    if (!displayName || !normalizedName) {
      individualRowCount += 1;
      skippedContributionRowCount += 1;
      continue;
    }
    if (isNonEntityDescriptionLabel(displayName)) {
      nonEntityLabelRowCount += 1;
      skippedContributionRowCount += 1;
      continue;
    }

    includedContributionRowCount += 1;
    for (const group of matchingGroups) {
      const key = donorAggregateKey({
        committeeId,
        supportOppose: group.supportOppose,
        normalizedName,
      });
      const existing = donors.get(key);
      if (existing) {
        existing.amountCents += row.amountCents;
      } else {
        donors.set(key, {
          committeeId,
          supportOppose: group.supportOppose,
          displayName,
          normalizedName,
          amountCents: row.amountCents,
        });
      }
    }
  }

  // Static rule classification only — the DB-backed/manual enrichment runs
  // at sync time on top of these rows.
  const industries = new Map<string, IndustryAggregate>();
  for (const donor of donors.values()) {
    if (donor.amountCents < minIndustryAmountCents) {
      continue;
    }
    const classification = classifyFinanceLabel({ rawLabel: donor.displayName, labelType: "donor" });
    if (!classification.industrySlug) {
      continue;
    }
    const key = industryAggregateKey({
      committeeId: donor.committeeId,
      supportOppose: donor.supportOppose,
      industrySlug: classification.industrySlug,
    });
    const existing = industries.get(key);
    if (existing) {
      existing.amountCents += donor.amountCents;
      existing.donorKeys.add(donor.normalizedName);
    } else {
      industries.set(key, {
        committeeId: donor.committeeId,
        supportOppose: donor.supportOppose,
        industrySlug: classification.industrySlug,
        amountCents: donor.amountCents,
        donorKeys: new Set([donor.normalizedName]),
      });
    }
  }

  return {
    outsideGroupBreakdowns: toBreakdowns({
      donors: donors.values(),
      industries: industries.values(),
      sourceUrl: input.sourceUrl ?? null,
    }),
    matchedContributionRowCount,
    includedContributionRowCount,
    skippedContributionRowCount,
    individualRowCount,
    nonEntityLabelRowCount,
    unusableRowCount,
    excludedShortDescriptionRowCount,
    unknownShortDescriptionRowCount,
  };
}
