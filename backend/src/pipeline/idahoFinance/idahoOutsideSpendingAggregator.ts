// Idaho outside money (docs/plans/idaho-finance.md, Phase 2b). Pure: takes
// the linked grid registration, the whole candidate grid, and the all-time
// IE list, and returns the support/oppose totals and per-filer groups the
// writer stores.
//
// Target selection (survey 2026-09-02, findings doc "IE targets"):
// - Filers pick the target from a registration list and often pick the
//   candidate's OLDER registration: 1,640 of 8,738 guid-resolved rows are
//   dated two years after their target registration's election year, and
//   for the 101 linked candidates 389 rows / $710k of 2026 money sit on
//   2022/2024 same-office registrations against 357 rows / $264k on the 2026
//   ones. So a row counts when its target guid is ANY registration of the
//   linked candidate's filer entity for the SAME grid office, and its
//   transaction year falls in the cycle window: after the entity's previous
//   same-office registration year (two years back when there is none — the
//   shortest Idaho cycle; the system's data starts in 2023) through the
//   linked election year. A row on the entity's other-office registration
//   (Hensley 2026: 29 rows on her 2025 city-council registration) is
//   declared for another race and only counted in the diagnostics.
// - Name-only targets (isCandidateNonRegisteredEntity; 1,159 rows all-time,
//   mostly local odd-year races) count when officeSought equals the linked
//   office, the "Last, First" text passes the shared middle-name gate against
//   the entity's registration names (nickname expansion on the IE side), the
//   year is in the window, and no other entity holds a same-office
//   registration matching the same name (ambiguous rows are excluded and
//   counted).
// - Measure rows (officeSought null) never count.
// Stance is filer-declared on every row (Support / Oppose; electioneering
// communications never appear in this search). Amounts are amountApplied,
// the per-target allocation. Identical allocation rows recur and the state
// counts them, so nothing is de-duplicated (the row guid is the parent
// transaction's, shared by its allocation rows).
//
// Groups are keyed by filer: the filer registration guid for Idaho-registered
// filers (TIECOM rows always carry one), otherwise the FEC id the filer wrote
// into its name ("Make Liberty Win (FEC ID: C00731133)" — three spellings
// live) or the normalized name (TEXP rows: federal PACs, businesses, and
// individuals not registered in Idaho).

import {
  IDAHO_CFS_PUBLIC_SITE_URL,
  normalizeIdahoRegistrationGuid,
  type IdahoCandidateRegistrationRow,
  type IdahoIndependentExpenditureRow,
} from "./idahoCfsClient.js";
import { idahoCandidateNameMatchConfidence, idahoRegistrationRowName } from "./idahoCandidateFilerResolver.js";
import type { IdahoFinanceSupportOppose } from "./idahoFinanceWriter.js";

/** Public IE search page (the SPA route behind the IE list endpoint). */
export const IDAHO_INDEPENDENT_EXPENDITURE_PAGE_URL = `${IDAHO_CFS_PUBLIC_SITE_URL}/public/cf/independent`;

// TIECOM = IE reported by a registered Idaho filer; TEXP = IE reported by an
// entity not registered in Idaho. Anything else fails closed.
const IDAHO_IE_TRANSACTION_TYPE_CODES: ReadonlySet<string> = new Set(["TIECOM", "TEXP"]);
const DEFAULT_MAX_GROUPS = 50;
// Shortest Idaho cycle (legislature); used when the entity has no previous
// same-office registration to bound the window.
const DEFAULT_CYCLE_YEARS = 2;

export type IdahoOutsideSpendingGroup = {
  /** Filer registration guid, "fec:<id>", or "name:<normalized name>". */
  filerKey: string;
  filerName: string;
  supportOppose: IdahoFinanceSupportOppose;
  amount: number;
  sourceUrl: string;
};

export type IdahoOutsideSpendingSummary = {
  supportTotal: number;
  opposeTotal: number;
  groups: IdahoOutsideSpendingGroup[];
  sourceUrl: string;
};

export type IdahoOutsideSpendingAggregationInput = {
  /** The linked registration (grid row). */
  registration: IdahoCandidateRegistrationRow;
  /** The candidate grid; must contain the linked registration. */
  registrations: readonly IdahoCandidateRegistrationRow[];
  /** The all-time IE list. */
  expenditureRows: readonly IdahoIndependentExpenditureRow[];
  /** Defaults to the public IE search page. */
  sourceUrl?: string | null;
  /** Groups kept per direction (largest first); totals are unaffected. */
  maxGroups?: number;
};

export type IdahoOutsideSpendingAggregationResult = {
  summary: IdahoOutsideSpendingSummary;
  /** Rows count when windowStartYear < transaction year <= windowEndYear. */
  windowStartYear: number;
  windowEndYear: number;
  /** Same-entity, same-office registration guids whose rows count (the linked one first). */
  raceRegistrationGuids: string[];
  sourceRowCount: number;
  includedRowCount: number;
  /** Included rows that named a race registration guid. */
  guidResolvedRowCount: number;
  /** Included rows that named a race registration other than the linked one. */
  priorRegistrationRowCount: number;
  /** Included rows matched on the target name alone. */
  nameResolvedRowCount: number;
  /** Rows of this race dated outside the window. */
  outOfWindowRowCount: number;
  /** Rows on the entity's other-office registrations, or name rows declaring another office. */
  otherOfficeRowCount: number;
  /** Name rows that also match another entity's same-office registration. */
  ambiguousNameRowCount: number;
  nonPositiveRowCount: number;
};

type GroupAccumulator = {
  filerKey: string;
  supportOppose: IdahoFinanceSupportOppose;
  amountCents: number;
};

type FilerNameEvidence = {
  filerName: string;
  transactionDate: string;
};

function normalizeTextKey(value: string): string {
  return value
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toUpperCase()
    .replace(/[^A-Z0-9]+/g, " ")
    .trim();
}

/**
 * Group identity for one IE row's filer: the Idaho registration guid when the
 * filer is registered, else the FEC committee id written into the name, else
 * the normalized name.
 */
export function idahoOutsideFilerKey(row: Pick<IdahoIndependentExpenditureRow, "filerName" | "filerRegistrationGuid">): string {
  if (row.filerRegistrationGuid !== null) {
    return normalizeIdahoRegistrationGuid(row.filerRegistrationGuid);
  }
  const fecId = /\bC\d{8}\b/.exec(row.filerName.toUpperCase())?.[0];
  if (fecId) return `fec:${fecId}`;
  const name = normalizeTextKey(row.filerName);
  if (!name) {
    throw new Error(`Idaho IE row has a blank filer name: ${JSON.stringify(row.filerName)}`);
  }
  return `name:${name}`;
}

/**
 * IE target text as the shared matcher reads it: a quoted call name becomes
 * the parenthetical alias form ("Corbus, Franklin 'Bud'" -> "Corbus, Franklin
 * (Bud)"), and a trailing comma ("Chapman, Ada,") is dropped.
 */
export function idahoIeTargetName(candidateMeasure: string): string {
  return candidateMeasure
    .replace(/["']([^"']{2,})["']/g, "($1)")
    .replace(/\s+/g, " ")
    .replace(/[\s,]+$/, "")
    .trim();
}

function amountToCents(amount: number, label: string): number {
  const cents = Math.round(amount * 100);
  if (!Number.isFinite(amount) || !Number.isSafeInteger(cents)) {
    throw new Error(`Invalid Idaho IE amount for ${label}: ${amount}`);
  }
  return cents;
}

function transactionYear(row: IdahoIndependentExpenditureRow): number {
  if (!/^\d{4}-\d{2}-\d{2}T/.test(row.transactionDate)) {
    throw new Error(`Idaho IE row ${row.guid} has an unexpected date ${JSON.stringify(row.transactionDate)}`);
  }
  return Number(row.transactionDate.slice(0, 4));
}

// Contract checks for a row that targets this race. A registered filer must
// carry its guid so groups never split between a guid and a name key.
function validateRow(row: IdahoIndependentExpenditureRow): void {
  if (!IDAHO_IE_TRANSACTION_TYPE_CODES.has(row.transactionTypeCode)) {
    throw new Error(`Idaho IE row ${row.guid} has unknown transaction type ${JSON.stringify(row.transactionTypeCode)}`);
  }
  if (row.stance !== "Support" && row.stance !== "Oppose") {
    throw new Error(`Idaho IE row ${row.guid} has unknown stance ${JSON.stringify(row.stance)}`);
  }
  if (row.transactionTypeCode === "TIECOM" && row.filerRegistrationGuid === null) {
    throw new Error(`Idaho IE row ${row.guid} is a registered-filer row without a filer registration guid`);
  }
  transactionYear(row);
  amountToCents(row.amountApplied, `row ${row.guid}`);
}

function rememberFilerName(names: Map<string, FilerNameEvidence>, filerKey: string, row: IdahoIndependentExpenditureRow): void {
  // Newest row wins when one filer key appears under several spellings
  // (FEC-id and name keys only; a registration guid has one name).
  const existing = names.get(filerKey);
  if (
    existing &&
    (row.transactionDate.localeCompare(existing.transactionDate) || row.filerName.localeCompare(existing.filerName)) <= 0
  ) {
    return;
  }
  names.set(filerKey, { filerName: row.filerName, transactionDate: row.transactionDate });
}

function toGroups(input: {
  groups: Iterable<GroupAccumulator>;
  filerNames: ReadonlyMap<string, FilerNameEvidence>;
  maxGroups: number;
  sourceUrl: string;
}): IdahoOutsideSpendingGroup[] {
  const counts: Record<IdahoFinanceSupportOppose, number> = { support: 0, oppose: 0 };
  return [...input.groups]
    .map((group) => {
      const filerName = input.filerNames.get(group.filerKey)?.filerName;
      if (!filerName) throw new Error(`Missing Idaho IE filer name for ${group.filerKey}`);
      return { ...group, filerName };
    })
    .sort(
      (left, right) =>
        right.amountCents - left.amountCents ||
        left.supportOppose.localeCompare(right.supportOppose) ||
        left.filerName.localeCompare(right.filerName) ||
        left.filerKey.localeCompare(right.filerKey)
    )
    .filter((group) => {
      if (counts[group.supportOppose] >= input.maxGroups) return false;
      counts[group.supportOppose] += 1;
      return true;
    })
    .map((group) => ({
      filerKey: group.filerKey,
      filerName: group.filerName,
      supportOppose: group.supportOppose,
      amount: group.amountCents / 100,
      sourceUrl: input.sourceUrl,
    }));
}

export function aggregateIdahoOutsideSpending(
  input: IdahoOutsideSpendingAggregationInput
): IdahoOutsideSpendingAggregationResult {
  const { registration } = input;
  const linkedGuid = normalizeIdahoRegistrationGuid(registration.registrationGuid);
  const office = registration.office?.trim();
  if (!office) {
    throw new Error(`Idaho registration ${linkedGuid} has no office`);
  }
  const maxGroups = input.maxGroups ?? DEFAULT_MAX_GROUPS;
  if (!Number.isInteger(maxGroups) || maxGroups <= 0) {
    throw new Error(`Invalid Idaho outside spending maxGroups: ${input.maxGroups}`);
  }
  const sourceUrl = input.sourceUrl?.trim() || IDAHO_INDEPENDENT_EXPENDITURE_PAGE_URL;

  const entityRegistrations = input.registrations.filter((row) => row.filerEntityId === registration.filerEntityId);
  if (!entityRegistrations.some((row) => normalizeIdahoRegistrationGuid(row.registrationGuid) === linkedGuid)) {
    throw new Error(`Idaho registration ${linkedGuid} is not in the candidate grid`);
  }
  // The race = every same-office registration of the entity up to the
  // linked cycle, the linked one first.
  const raceRegistrations = entityRegistrations
    .filter((row) => row.office === office && row.electionYear <= registration.electionYear)
    .sort((left, right) => {
      const leftLinked = normalizeIdahoRegistrationGuid(left.registrationGuid) === linkedGuid ? 0 : 1;
      const rightLinked = normalizeIdahoRegistrationGuid(right.registrationGuid) === linkedGuid ? 0 : 1;
      return leftLinked - rightLinked || right.electionYear - left.electionYear;
    });
  const raceGuids = raceRegistrations.map((row) => normalizeIdahoRegistrationGuid(row.registrationGuid));
  const otherEntityGuids = new Set(
    entityRegistrations
      .map((row) => normalizeIdahoRegistrationGuid(row.registrationGuid))
      .filter((guid) => !raceGuids.includes(guid))
  );
  const windowEndYear = registration.electionYear;
  const priorYears = raceRegistrations
    .map((row) => row.electionYear)
    .filter((year) => year < windowEndYear);
  const windowStartYear = priorYears.length > 0 ? Math.max(...priorYears) : windowEndYear - DEFAULT_CYCLE_YEARS;
  const entityRowNames = [...new Set(entityRegistrations.map(idahoRegistrationRowName))];
  const otherEntitySameOffice = input.registrations.filter(
    (row) => row.filerEntityId !== registration.filerEntityId && row.office === office
  );

  const groups = new Map<string, GroupAccumulator>();
  const filerNames = new Map<string, FilerNameEvidence>();
  let supportTotalCents = 0;
  let opposeTotalCents = 0;
  let includedRowCount = 0;
  let guidResolvedRowCount = 0;
  let priorRegistrationRowCount = 0;
  let nameResolvedRowCount = 0;
  let outOfWindowRowCount = 0;
  let otherOfficeRowCount = 0;
  let ambiguousNameRowCount = 0;
  let nonPositiveRowCount = 0;

  // Adds a row of this race to the totals and its filer group; false when
  // the amount is not positive.
  const include = (row: IdahoIndependentExpenditureRow): boolean => {
    const amountCents = amountToCents(row.amountApplied, `row ${row.guid}`);
    if (amountCents <= 0) {
      nonPositiveRowCount += 1;
      return false;
    }
    const supportOppose: IdahoFinanceSupportOppose = row.stance === "Support" ? "support" : "oppose";
    const filerKey = idahoOutsideFilerKey(row);
    rememberFilerName(filerNames, filerKey, row);
    includedRowCount += 1;
    if (supportOppose === "support") supportTotalCents += amountCents;
    else opposeTotalCents += amountCents;
    const key = `${supportOppose} ${filerKey}`;
    const existing = groups.get(key);
    if (existing) existing.amountCents += amountCents;
    else groups.set(key, { filerKey, supportOppose, amountCents });
    return true;
  };

  for (const row of input.expenditureRows) {
    const targetGuid =
      row.candidateMeasureFilerRegistrationGuid === null
        ? null
        : normalizeIdahoRegistrationGuid(row.candidateMeasureFilerRegistrationGuid);
    if (targetGuid !== null) {
      if (otherEntityGuids.has(targetGuid)) {
        validateRow(row);
        otherOfficeRowCount += 1;
        continue;
      }
      if (!raceGuids.includes(targetGuid)) continue;
      validateRow(row);
      const year = transactionYear(row);
      if (year <= windowStartYear || year > windowEndYear) {
        outOfWindowRowCount += 1;
        continue;
      }
      if (include(row)) {
        guidResolvedRowCount += 1;
        if (targetGuid !== linkedGuid) priorRegistrationRowCount += 1;
      }
      continue;
    }
    // Measure rows carry no office; everything else here is a name-only
    // candidate target.
    if (row.officeSought === null) continue;
    const targetName = idahoIeTargetName(row.candidateMeasure);
    if (idahoCandidateNameMatchConfidence([targetName], entityRowNames) === null) continue;
    validateRow(row);
    if (row.officeSought.trim() !== office) {
      otherOfficeRowCount += 1;
      continue;
    }
    const year = transactionYear(row);
    if (year <= windowStartYear || year > windowEndYear) {
      outOfWindowRowCount += 1;
      continue;
    }
    if (
      otherEntitySameOffice.some(
        (other) => idahoCandidateNameMatchConfidence([targetName], [idahoRegistrationRowName(other)]) !== null
      )
    ) {
      ambiguousNameRowCount += 1;
      continue;
    }
    if (include(row)) nameResolvedRowCount += 1;
  }

  return {
    summary: {
      supportTotal: supportTotalCents / 100,
      opposeTotal: opposeTotalCents / 100,
      groups: toGroups({ groups: groups.values(), filerNames, maxGroups, sourceUrl }),
      sourceUrl,
    },
    windowStartYear,
    windowEndYear,
    raceRegistrationGuids: raceGuids,
    sourceRowCount: input.expenditureRows.length,
    includedRowCount,
    guidResolvedRowCount,
    priorRegistrationRowCount,
    nameResolvedRowCount,
    outOfWindowRowCount,
    otherOfficeRowCount,
    ambiguousNameRowCount,
    nonPositiveRowCount,
  };
}
