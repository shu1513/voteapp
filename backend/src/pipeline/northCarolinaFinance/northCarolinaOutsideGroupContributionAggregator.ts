import { classifyFinanceLabel, normalizeFinanceLabel } from "../finance/financeLabelClassifier.js";
import type { NorthCarolinaFinanceOutsideGroup } from "./northCarolinaOutsideSpendingAggregator.js";
import type { NorthCarolinaFinanceOutsideGroupBreakdownInput } from "./northCarolinaFinanceWriter.js";
import type { NcsbeReceiptRow } from "./northCarolinaNcsbeParsers.js";

// Outside-group funder aggregation for North Carolina (north_carolina_plan.md
// PR 8, #3): who funds the committees behind a candidate's outside spending.
// The tennessee/ohio pattern over NCSBE receipt rows. The batch layer owns
// the artifact cache and passes each spender's receipt rows keyed by the
// group's committeeId — a registered spender's (SBoEID) rows come from its
// regular disclosure reports, an unregistered filer's (`NC-IE-FILER:` key)
// from its own IE reports' receipts, the decision-6 disclosed-funder rows
// (same-report `Donation` money, verified live on Rolling Sea Fund → Advance
// NC) — never presented as the group's full funding, never backfilled from
// older cycles.
//
// The standard writer only accepts donor and industry outside breakdowns, so
// individual contributors never become labels. Donor money is the pinned
// entity receipt-type vocabulary below (decision 12's seeded-vocabulary
// discipline) — `"IND "` individual rows and `IsAggregated` roll-ups are
// counted and skipped, and a code outside the pinned set fails closed into a
// counter plus the code list, never a breakdown.

export type NorthCarolinaOutsideGroupContributionAggregationInput = {
  electionYear: number;
  // One candidate's outside groups (the outside-spending aggregation slice
  // being written); the same committee may appear once per direction.
  outsideGroups: readonly NorthCarolinaFinanceOutsideGroup[];
  // Each spender's receipt rows keyed by the group committeeId (SBoEID or
  // `NC-IE-FILER:` hash). Ids without groups are ignored here.
  receiptRowsByCommitteeId: ReadonlyMap<string, readonly NcsbeReceiptRow[]>;
  sourceUrl?: string | null;
  minIndustryAmount?: number;
};

export type NorthCarolinaOutsideGroupContributionAggregationResult = {
  // ALL donor rows, uncapped (sorted by amount within each group). The sync
  // layer classifies every donor and only caps the PERSISTED donor display
  // rows — capping here would silently drop tail donors from the rebuilt
  // industry totals of a >cap-donor group.
  outsideGroupBreakdowns: NorthCarolinaFinanceOutsideGroupBreakdownInput[];
  // Rows whose committeeId matched one of the candidate's outside groups.
  matchedReceiptRowCount: number;
  // Matched rows that became donor money.
  includedReceiptRowCount: number;
  // Matched rows that did not: matched = included + skipped, and the six
  // reason counters below partition skipped.
  skippedReceiptRowCount: number;
  // `"IND "` individual contributions — real receipts, never donor labels.
  individualRowCount: number;
  // Pinned non-donor entity money (refunds/reimbursements back to the
  // committee) — known, so it never trips the unknown-code withholding.
  nonDonorRowCount: number;
  // IsAggregated roll-up rows (OrgName is the placeholder "Aggregated
  // Individual Contribution", not an entity).
  aggregatedRowCount: number;
  // Entity-coded rows with a blank or unusable OrgName.
  blankDonorNameRowCount: number;
  // Non-positive amount.
  unusableRowCount: number;
  // ReceiptTypeCode outside the pinned vocabulary entirely — a growing count
  // means the portal added a form type.
  unknownReceiptTypeCodeRowCount: number;
  unknownReceiptTypeCodes: string[];
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

// Pinned entity donor codes (decision 12 — seeded from spike/fixture bytes,
// trailing space on "DON " is real): other political committee, party, and
// the IE-form donation code that carries unregistered filers' disclosed
// funders (Rolling Sea Fund's $24,506 row). `"IND "` is the only individual
// code and is skipped by name below.
//
// The PR 9 live run added two after reviewing their bytes, and they carry
// most of the money the feature exists to show: "OUTS" (Outside Source, 87
// rows / $9.73M, including GOOD GOVERMENT COALITION INC's $1.25M and $1M
// gifts to IE committees) and "NFPC" (Not for Profit Contribution, 36 rows /
// $3.00M). Both are entity money into an outside group — exactly a funder.
const INDIVIDUAL_RECEIPT_TYPE_CODE = "IND ";
const DONOR_RECEIPT_TYPE_CODES = new Set(["CPCM", "PPTY", "DON ", "OUTS", "NFPC"]);

// Known codes that are NOT anyone funding the group, so they are skipped
// without tripping the unknown-code withholding: "RFND" is a
// refund/reimbursement flowing BACK to the committee (98 live rows / $160k,
// e.g. a $45.24 WIX.COM refund) — counting it would invent a donor out of a
// vendor.
const NON_DONOR_RECEIPT_TYPE_CODES = new Set(["RFND"]);

function normalizeElectionYear(value: number): number {
  if (!Number.isInteger(value) || value < 2000 || value > 2100) {
    throw new Error(`Invalid North Carolina outside group contribution election year: ${value}`);
  }
  return value;
}

function normalizeMinAmount(value: number | undefined): number {
  if (value === undefined) {
    return DEFAULT_MIN_INDUSTRY_AMOUNT_CENTS;
  }
  if (!Number.isFinite(value) || value < 0) {
    throw new Error(`Invalid North Carolina outside group contribution minIndustryAmount: ${value}`);
  }
  return Math.round(value * 100);
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
}): NorthCarolinaFinanceOutsideGroupBreakdownInput[] {
  const result: NorthCarolinaFinanceOutsideGroupBreakdownInput[] = [];
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
    for (const donor of donors.sort(
      (left, right) => right.amountCents - left.amountCents || left.displayName.localeCompare(right.displayName)
    )) {
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
    for (const industry of industries.sort(
      (left, right) => right.amountCents - left.amountCents || left.industrySlug.localeCompare(right.industrySlug)
    )) {
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

export function aggregateNorthCarolinaOutsideGroupContributions(
  input: NorthCarolinaOutsideGroupContributionAggregationInput
): NorthCarolinaOutsideGroupContributionAggregationResult {
  normalizeElectionYear(input.electionYear);
  const minIndustryAmountCents = normalizeMinAmount(input.minIndustryAmount);

  // The same committee appears once per direction it spent in; a receipt row
  // funds the committee, so it projects onto every direction's group.
  const groupsByCommitteeId = new Map<string, NorthCarolinaFinanceOutsideGroup[]>();
  for (const group of input.outsideGroups) {
    const committeeId = group.committeeId.trim().toUpperCase();
    if (committeeId.length === 0) {
      continue;
    }
    const existing = groupsByCommitteeId.get(committeeId) ?? [];
    existing.push(group);
    groupsByCommitteeId.set(committeeId, existing);
  }

  const donors = new Map<string, DonorAggregate>();
  let matchedReceiptRowCount = 0;
  let includedReceiptRowCount = 0;
  let skippedReceiptRowCount = 0;
  let individualRowCount = 0;
  let nonDonorRowCount = 0;
  let aggregatedRowCount = 0;
  let blankDonorNameRowCount = 0;
  let unusableRowCount = 0;
  let unknownReceiptTypeCodeRowCount = 0;
  const unknownReceiptTypeCodes = new Set<string>();

  for (const [rawCommitteeId, rows] of input.receiptRowsByCommitteeId) {
    const committeeId = rawCommitteeId.trim().toUpperCase();
    const matchingGroups = groupsByCommitteeId.get(committeeId);
    if (!matchingGroups) {
      continue;
    }
    for (const row of rows) {
      matchedReceiptRowCount += 1;

      if (row.amountCents <= 0) {
        unusableRowCount += 1;
        skippedReceiptRowCount += 1;
        continue;
      }
      // Roll-up rows first: their OrgName is the "Aggregated Individual
      // Contribution" placeholder, never an entity.
      if (row.isAggregated) {
        aggregatedRowCount += 1;
        skippedReceiptRowCount += 1;
        continue;
      }
      const code = row.receiptTypeCode ?? "";
      if (code === INDIVIDUAL_RECEIPT_TYPE_CODE) {
        individualRowCount += 1;
        skippedReceiptRowCount += 1;
        continue;
      }
      if (NON_DONOR_RECEIPT_TYPE_CODES.has(code)) {
        nonDonorRowCount += 1;
        skippedReceiptRowCount += 1;
        continue;
      }
      if (!DONOR_RECEIPT_TYPE_CODES.has(code)) {
        unknownReceiptTypeCodeRowCount += 1;
        unknownReceiptTypeCodes.add(code);
        skippedReceiptRowCount += 1;
        continue;
      }

      const displayName = cleanDisplayLabel(row.orgName ?? "");
      const normalizedName = displayName ? normalizeFinanceLabel(displayName, "donor") : "";
      if (!displayName || !normalizedName) {
        blankDonorNameRowCount += 1;
        skippedReceiptRowCount += 1;
        continue;
      }

      includedReceiptRowCount += 1;
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
    matchedReceiptRowCount,
    includedReceiptRowCount,
    skippedReceiptRowCount,
    individualRowCount,
    nonDonorRowCount,
    aggregatedRowCount,
    blankDonorNameRowCount,
    unusableRowCount,
    unknownReceiptTypeCodeRowCount,
    unknownReceiptTypeCodes: [...unknownReceiptTypeCodes].sort(),
  };
}
