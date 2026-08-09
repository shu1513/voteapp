import { createHash } from "node:crypto";

import { northCarolinaPersonNamesMatch } from "./northCarolinaCandidateCommitteeResolver.js";
import {
  selectNcsbeCurrentFilings,
  type NcsbeFiling,
  type NcsbeQuarantinedGroup,
} from "./northCarolinaReportSelector.js";
import type { NcsbeDocumentRow, NcsbeExpenditureRow } from "./northCarolinaNcsbeParsers.js";

// Outside-spending aggregation over the IE doc-type inventories
// (north_carolina_plan.md decisions 3–6). Single-source rule (decision 3):
// outside totals come ONLY from reports discovered via the IRIEX/IRCIX/RPIER
// inventories — the same IE row can be mirrored verbatim into a registered
// committee's regular quarterly, so regular-report rows are a cross-check
// diagnostic (in the direct aggregator), never money here.
//
// Amount semantics are pinned PER FORM (decision 4): on the unregistered
// "Independent Expenditure Report" form, `IEAmount` carries the per-target
// amount while `Amount` repeats the full vendor invoice on every target row
// (a $20K overstatement on the probed report); on the registered-committee
// form, `IEAmount` can be null with `Amount` holding the single-target
// value. Every report is gated by reconciling its chosen-amount sum against
// the official cover expenditure total before any row is attributed; a
// failing report quarantines whole.
//
// Target matching is decision 5: token-order-insensitive strict name match
// (the portal serves `PIERCE RODNEY`, `PIERCE RODNEY D`, and `RODNEY PIERCE`
// for one person across filers), unique across the candidate set, with the
// row's office/district as a CONFIRMING filter only. Federal rows are
// removed before matching (FEC owns that money); unmatched and ambiguous
// targets quarantine into diagnostics — never a guess.

export type NorthCarolinaOutsideReportInput = {
  reportId: string;
  // The report's official expenditure total from its cover (Total
  // Expenditures, Period column) — the decision-4 reconciliation gate. Null
  // when the cover is missing or failed to parse: the report quarantines.
  officialExpenditureTotalCents: number | null;
  expenditureRows: readonly NcsbeExpenditureRow[];
};

export type NorthCarolinaOutsideCandidateTarget = {
  // Opaque caller identity (e.g. candidate|election pair); results key on it.
  candidateKey: string;
  candidateName: string;
  // VoteApp office scope: "state_lower" | "state_upper" (anything else never
  // office-confirms and only matches rows without a state-chamber office).
  officeScope: string;
  district?: string | null;
};

export type NorthCarolinaFinanceOutsideGroup = {
  committeeId: string;
  committeeName: string;
  supportOppose: "support" | "oppose";
  amount: number;
  sourceUrl: string | null;
};

export type NorthCarolinaOutsideCandidateResult = {
  candidateKey: string;
  supportTotal: number;
  opposeTotal: number;
  groups: NorthCarolinaFinanceOutsideGroup[];
};

export type NorthCarolinaOutsideReportQuarantineReason =
  | "missing_artifacts"
  | "missing_official_total"
  | "null_ie_amount_on_unregistered_form"
  | "official_total_mismatch";

export type NorthCarolinaOutsideReportResult = {
  reportId: string;
  filerKey: string;
  committeeName: string;
  reportType: string | null;
  chosenAmountSumCents: number;
  officialExpenditureTotalCents: number | null;
  toleranceCents: number;
  quarantined: boolean;
  quarantineReason: NorthCarolinaOutsideReportQuarantineReason | null;
};

export type NorthCarolinaOutsideTargetDiagnostic = {
  value: string;
  rowCount: number;
  amountCents: number;
};

export type NorthCarolinaOutsideCoverageGap = {
  filerKey: string;
  committeeName: string;
  reportType: string | null;
  periodStartRaw: string;
  periodEndRaw: string;
  kind: "image_only_current" | "quarantined_lineage";
};

export type NorthCarolinaOutsideSpendingAggregationResult = {
  // Only candidates with at least one attributed directional row.
  candidates: NorthCarolinaOutsideCandidateResult[];
  reports: NorthCarolinaOutsideReportResult[];
  // Structured filings without supplied report data are quarantined
  // (missing_artifacts) AND listed here for the acquisition to chase.
  missingReportIds: string[];
  coverageGaps: NorthCarolinaOutsideCoverageGap[];
  quarantinedGroups: NcsbeQuarantinedGroup[];
  unmatchedTargets: NorthCarolinaOutsideTargetDiagnostic[];
  ambiguousTargets: NorthCarolinaOutsideTargetDiagnostic[];
  // Rows excluded by decision 3's fail-closed filters, each counted with its
  // money so a growing bucket is investigated, never silent.
  excludedDeclarationRowCount: number;
  excludedDeclarationCents: number;
  nonIndependentExpenditureRowCount: number;
  federalTargetRowCount: number;
  federalTargetCents: number;
  nonCandidateTargetRowCount: number;
  countyMunicipalTargetRowCount: number;
  blankTargetRowCount: number;
  nonPositiveAmountRowCount: number;
  // Inventory rows outside the two pinned IE report types — new portal
  // vocabulary is reviewed, never silently aggregated.
  unknownReportTypeRowCount: number;
  attributedRowCount: number;
  attributedCents: number;
  quarantinedReportCount: number;
  // Decision 8 / spike item 13: overlapping-period reports from one filer are
  // incremental, not cumulative, so nothing is deduped — but identical
  // directional rows across a filer's overlapping selected reports are
  // surfaced for the PR 9 audit.
  overlappingReportPairCount: number;
  duplicateLookingRowCount: number;
  duplicateLookingCents: number;
};

// The two IE report-type strings observed across both cycle-year inventories
// (95 + 51 filings) — pinned; anything else fails closed into a counted
// diagnostic.
export const NCSBE_IE_REPORT_TYPE_UNREGISTERED = "Independent Expenditure Report";
export const NCSBE_IE_REPORT_TYPE_REGISTERED = "Independent Expenditure for Registered Committees";

// Known non-candidate target sentinel (spike results item 4).
const NON_CANDIDATE_TARGET_SENTINEL = "SPECIFIC NON CANDIDATE";

const DEFAULT_MAX_GROUPS = 50;

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

function centsToDollars(cents: number): number {
  return cents / 100;
}

function normalizePositiveInteger(value: number | undefined, fallback: number, fieldName: string): number {
  const normalized = value ?? fallback;
  if (!Number.isInteger(normalized) || normalized <= 0) {
    throw new Error(`Invalid North Carolina outside spending aggregation ${fieldName}: ${normalized}`);
  }
  return normalized;
}

// Outside-group identity (decision 6): the SBoEID when the filer has one,
// else a synthetic source-scoped key from the normalized name — never the
// literal `No Id`, never the raw name. (The NC-OGID:<id> variant needs a
// portal entity search and joins with the funders work.) Uppercase by
// definition, so the writer's unconditional upper-casing never mangles it.
export function northCarolinaOutsideGroupCommitteeId(filing: {
  sboeId: string | null;
  committeeName: string;
}): string {
  if (filing.sboeId) {
    return filing.sboeId.toUpperCase();
  }
  const nameKey = normalizeTextKey(filing.committeeName);
  return `NC-IE-FILER:${createHash("sha256").update(nameKey).digest("hex")}`;
}

// Decision 5's order-insensitive widening: the portal serves targets in both
// LAST FIRST [MIDDLE] and FIRST [MIDDLE] LAST orders WITHOUT a comma, so a
// no-comma multi-token target is also read with its leading token moved to
// the end ("PIERCE RODNEY D" -> "RODNEY D PIERCE"). Both readings go through
// the strict resolver matcher (keys + middle/suffix conflict guard); a
// target matching two different candidates via different readings lands in
// the ambiguity quarantine downstream.
export function northCarolinaIeTargetMatchesCandidate(candidateName: string, target: string): boolean {
  if (northCarolinaPersonNamesMatch(candidateName, target)) {
    return true;
  }
  if (target.includes(",")) {
    return false;
  }
  const tokens = target.trim().split(/\s+/).filter(Boolean);
  if (tokens.length < 2) {
    return false;
  }
  const flipped = [...tokens.slice(1), tokens[0]!].join(" ");
  return northCarolinaPersonNamesMatch(candidateName, flipped);
}

type OfficeSoughtClass = "federal" | "state_house" | "state_senate" | "county_municipal" | "other";

// OfficeSought vocabulary from spike/fixture bytes: `House`, `NC HOUSE 27`
// (district-bearing), `US HOUSE OF REPRESENTATIVES` and the dotted
// `U.S. HOUSE OF REPRESENTATIVES` (both federal), `County/Municipal`.
function classifyOfficeSought(value: string | null): { officeClass: OfficeSoughtClass; district: string | null } {
  const key = normalizeTextKey(value).replace(/^U S /, "US ");
  if (!key) {
    return { officeClass: "other", district: null };
  }
  if (key.startsWith("US ") || key.includes("PRESIDENT") || key.includes("CONGRESS")) {
    return { officeClass: "federal", district: null };
  }
  const districtMatch = /\b(\d{1,3})\b/.exec(key);
  const district = districtMatch ? String(Number(districtMatch[1])) : null;
  if (key.includes("COUNTY") || key.includes("MUNICIPAL")) {
    return { officeClass: "county_municipal", district };
  }
  if (key.includes("HOUSE")) {
    return { officeClass: "state_house", district };
  }
  if (key.includes("SENATE")) {
    return { officeClass: "state_senate", district };
  }
  return { officeClass: "other", district };
}

function candidateChamber(officeScope: string): OfficeSoughtClass | null {
  if (officeScope === "state_lower") {
    return "state_house";
  }
  if (officeScope === "state_upper") {
    return "state_senate";
  }
  return null;
}

function normalizeDistrict(value: string | null | undefined): string | null {
  const key = normalizeTextKey(value);
  if (!key || !/^\d+$/.test(key)) {
    return null;
  }
  return String(Number(key));
}

type CandidateAccumulator = {
  target: NorthCarolinaOutsideCandidateTarget;
  chamber: OfficeSoughtClass | null;
  district: string | null;
  supportCents: number;
  opposeCents: number;
  groups: Map<string, { committeeId: string; committeeName: string; supportOppose: "support" | "oppose"; amountCents: number }>;
};

function addTargetDiagnostic(
  map: Map<string, NorthCarolinaOutsideTargetDiagnostic>,
  value: string,
  amountCents: number
): void {
  const existing = map.get(value);
  if (existing) {
    existing.rowCount += 1;
    existing.amountCents += amountCents;
    return;
  }
  map.set(value, { value, rowCount: 1, amountCents });
}

function periodsOverlap(left: NcsbeFiling, right: NcsbeFiling): boolean {
  if (
    left.periodStartIso === null ||
    left.periodEndIso === null ||
    right.periodStartIso === null ||
    right.periodEndIso === null
  ) {
    return false;
  }
  return left.periodStartIso <= right.periodEndIso && right.periodStartIso <= left.periodEndIso;
}

// Decision-4 amount pinning, per row of a given form. Returns null only on
// the unregistered form with a null IEAmount — a form violation that
// quarantines the report.
function chosenAmountCents(row: NcsbeExpenditureRow, reportType: string | null): number | null {
  if (reportType === NCSBE_IE_REPORT_TYPE_UNREGISTERED) {
    return row.ieAmountCents;
  }
  return row.ieAmountCents ?? row.amountCents;
}

function isDirectionalIeRow(row: NcsbeExpenditureRow): "support" | "oppose" | "not_ie" | "undeclared" {
  if (normalizeTextKey(row.expenditureTypeDesc) !== "INDEPENDENT EXPENDITURE") {
    return "not_ie";
  }
  const declaration = normalizeTextKey(row.declaration);
  if (declaration === "SUPPORT") {
    return "support";
  }
  if (declaration === "OPPOSE") {
    return "oppose";
  }
  return "undeclared";
}

export function aggregateNorthCarolinaOutsideSpending(input: {
  // Parsed IE doc-type inventory rows for both cycle years (the selector
  // dedups the filings both inventories list).
  ieInventoryRows: readonly NcsbeDocumentRow[];
  reports: readonly NorthCarolinaOutsideReportInput[];
  candidates: readonly NorthCarolinaOutsideCandidateTarget[];
  sourceUrl?: string | null;
  maxGroups?: number;
}): NorthCarolinaOutsideSpendingAggregationResult {
  const maxGroups = normalizePositiveInteger(input.maxGroups, DEFAULT_MAX_GROUPS, "maxGroups");
  const sourceUrl = input.sourceUrl ?? null;

  // Pinned IE report types only; anything else is new portal vocabulary.
  const ieRows: NcsbeDocumentRow[] = [];
  let unknownReportTypeRowCount = 0;
  for (const row of input.ieInventoryRows) {
    if (
      row.reportType === NCSBE_IE_REPORT_TYPE_UNREGISTERED ||
      row.reportType === NCSBE_IE_REPORT_TYPE_REGISTERED
    ) {
      ieRows.push(row);
    } else {
      unknownReportTypeRowCount += 1;
    }
  }

  const selection = selectNcsbeCurrentFilings({ rows: ieRows });
  const coverageGaps: NorthCarolinaOutsideCoverageGap[] = [
    ...selection.supersededUnavailable.map(
      (filing): NorthCarolinaOutsideCoverageGap => ({
        filerKey: filing.filerKey,
        committeeName: filing.committeeName,
        reportType: filing.reportType,
        periodStartRaw: filing.periodStartRaw,
        periodEndRaw: filing.periodEndRaw,
        kind: "image_only_current",
      })
    ),
    ...selection.quarantinedGroups.map(
      (group): NorthCarolinaOutsideCoverageGap => ({
        filerKey: group.filerKey,
        committeeName: group.committeeName,
        reportType: group.reportType,
        periodStartRaw: group.periodStartRaw,
        periodEndRaw: group.periodEndRaw,
        kind: "quarantined_lineage",
      })
    ),
  ];

  const candidateAccumulators = input.candidates.map((target): CandidateAccumulator => {
    if (!target.candidateKey.trim()) {
      throw new Error("North Carolina outside spending candidate target needs a candidateKey");
    }
    return {
      target,
      chamber: candidateChamber(target.officeScope),
      district: normalizeDistrict(target.district),
      supportCents: 0,
      opposeCents: 0,
      groups: new Map(),
    };
  });

  const reportsById = new Map(input.reports.map((report) => [report.reportId, report]));
  const reports: NorthCarolinaOutsideReportResult[] = [];
  const missingReportIds: string[] = [];
  const unmatchedTargets = new Map<string, NorthCarolinaOutsideTargetDiagnostic>();
  const ambiguousTargets = new Map<string, NorthCarolinaOutsideTargetDiagnostic>();
  let excludedDeclarationRowCount = 0;
  let excludedDeclarationCents = 0;
  let nonIndependentExpenditureRowCount = 0;
  let federalTargetRowCount = 0;
  let federalTargetCents = 0;
  let nonCandidateTargetRowCount = 0;
  let countyMunicipalTargetRowCount = 0;
  let blankTargetRowCount = 0;
  let nonPositiveAmountRowCount = 0;
  let attributedRowCount = 0;
  let attributedCents = 0;
  let quarantinedReportCount = 0;

  // Row fingerprints per aggregated filing, for the overlap diagnostic.
  const aggregatedFilings: Array<{ filing: NcsbeFiling; fingerprints: Map<string, { rowCount: number; amountCents: number }> }> = [];

  for (const filing of selection.selected) {
    const reportId = filing.reportId!;
    const report = reportsById.get(reportId) ?? null;

    let chosenAmountSumCents = 0;
    let toleranceCents = 0;
    let quarantineReason: NorthCarolinaOutsideReportQuarantineReason | null = null;
    if (report === null) {
      quarantineReason = "missing_artifacts";
      missingReportIds.push(reportId);
    } else {
      for (const row of report.expenditureRows) {
        const amount = chosenAmountCents(row, filing.reportType);
        if (amount === null) {
          quarantineReason = "null_ie_amount_on_unregistered_form";
          break;
        }
        chosenAmountSumCents += amount;
        // One cent of rounding slack per split vendor-invoice row
        // (1166.66 + 1166.67 + 1166.68 = 3500.01 against a 3500.00 invoice),
        // never a percentage (decision 4).
        if (row.ieAmountCents !== null && row.ieAmountCents !== row.amountCents) {
          toleranceCents += 1;
        }
      }
      if (quarantineReason === null) {
        if (report.officialExpenditureTotalCents === null) {
          quarantineReason = "missing_official_total";
        } else if (
          Math.abs(chosenAmountSumCents - report.officialExpenditureTotalCents) > toleranceCents
        ) {
          quarantineReason = "official_total_mismatch";
        }
      }
    }

    reports.push({
      reportId,
      filerKey: filing.filerKey,
      committeeName: filing.committeeName,
      reportType: filing.reportType,
      chosenAmountSumCents,
      officialExpenditureTotalCents: report?.officialExpenditureTotalCents ?? null,
      toleranceCents,
      quarantined: quarantineReason !== null,
      quarantineReason,
    });
    if (quarantineReason !== null) {
      quarantinedReportCount += 1;
      continue;
    }

    const committeeId = northCarolinaOutsideGroupCommitteeId(filing);
    const fingerprints = new Map<string, { rowCount: number; amountCents: number }>();
    for (const row of report!.expenditureRows) {
      const direction = isDirectionalIeRow(row);
      const amount = chosenAmountCents(row, filing.reportType)!;
      if (direction === "not_ie") {
        nonIndependentExpenditureRowCount += 1;
        continue;
      }
      if (direction === "undeclared") {
        // Blank or other declarations never enter totals; never infer
        // direction from a group's name or politics (decision 3).
        excludedDeclarationRowCount += 1;
        excludedDeclarationCents += amount;
        continue;
      }
      if (amount <= 0) {
        nonPositiveAmountRowCount += 1;
        continue;
      }

      const targetValue = row.candidate?.trim() ?? "";
      if (!targetValue) {
        blankTargetRowCount += 1;
        continue;
      }
      if (normalizeTextKey(targetValue) === NON_CANDIDATE_TARGET_SENTINEL) {
        nonCandidateTargetRowCount += 1;
        continue;
      }
      const office = classifyOfficeSought(row.officeSought);
      if (office.officeClass === "federal") {
        // FEC owns federal money; these rows survive only as artifacts.
        federalTargetRowCount += 1;
        federalTargetCents += amount;
        continue;
      }

      const fingerprintKey = JSON.stringify([
        normalizeTextKey(targetValue),
        direction,
        amount,
        row.occurDate.raw,
        normalizeTextKey(row.orgName),
      ]);
      const fingerprint = fingerprints.get(fingerprintKey);
      if (fingerprint) {
        fingerprint.rowCount += 1;
        fingerprint.amountCents += amount;
      } else {
        fingerprints.set(fingerprintKey, { rowCount: 1, amountCents: amount });
      }

      let matched = candidateAccumulators.filter((accumulator) =>
        northCarolinaIeTargetMatchesCandidate(accumulator.target.candidateName, targetValue)
      );
      if (office.officeClass === "county_municipal") {
        // Decision 2: county/municipal candidates are out of scope, so a
        // stated county office removes every state candidate — a same-named
        // state candidate must not absorb county money.
        matched = [];
        countyMunicipalTargetRowCount += 1;
      } else if (office.officeClass === "state_house" || office.officeClass === "state_senate") {
        // Office confirms but never requires (decision 5): a stated chamber
        // (or district) that contradicts the candidate's removes them; a
        // blank office filters nothing.
        matched = matched.filter(
          (accumulator) =>
            accumulator.chamber === office.officeClass &&
            (office.district === null ||
              accumulator.district === null ||
              accumulator.district === office.district)
        );
      }
      if (matched.length === 0) {
        if (office.officeClass !== "county_municipal") {
          addTargetDiagnostic(unmatchedTargets, targetValue, amount);
        }
        continue;
      }
      if (matched.length > 1) {
        addTargetDiagnostic(ambiguousTargets, targetValue, amount);
        continue;
      }

      const accumulator = matched[0]!;
      attributedRowCount += 1;
      attributedCents += amount;
      if (direction === "support") {
        accumulator.supportCents += amount;
      } else {
        accumulator.opposeCents += amount;
      }
      const groupKey = `${committeeId}\u0000${direction}`;
      const group = accumulator.groups.get(groupKey);
      if (group) {
        group.amountCents += amount;
      } else {
        accumulator.groups.set(groupKey, {
          committeeId,
          committeeName: filing.committeeName,
          supportOppose: direction,
          amountCents: amount,
        });
      }
    }
    aggregatedFilings.push({ filing, fingerprints });
  }

  // Overlap diagnostic (spike item 13): a filer's overlapping-period reports
  // are incremental, so identical directional rows across them look like the
  // repeats the spike ruled out — surface, never dedup.
  let overlappingReportPairCount = 0;
  let duplicateLookingRowCount = 0;
  let duplicateLookingCents = 0;
  for (let leftIndex = 0; leftIndex < aggregatedFilings.length; leftIndex += 1) {
    for (let rightIndex = leftIndex + 1; rightIndex < aggregatedFilings.length; rightIndex += 1) {
      const left = aggregatedFilings[leftIndex]!;
      const right = aggregatedFilings[rightIndex]!;
      if (left.filing.filerKey !== right.filing.filerKey || !periodsOverlap(left.filing, right.filing)) {
        continue;
      }
      overlappingReportPairCount += 1;
      for (const [key, fingerprint] of left.fingerprints) {
        const other = right.fingerprints.get(key);
        if (other) {
          const repeated = Math.min(fingerprint.rowCount, other.rowCount);
          duplicateLookingRowCount += repeated;
          duplicateLookingCents += Math.min(fingerprint.amountCents, other.amountCents);
        }
      }
    }
  }

  const candidates = candidateAccumulators
    .filter((accumulator) => accumulator.groups.size > 0)
    .map(
      (accumulator): NorthCarolinaOutsideCandidateResult => ({
        candidateKey: accumulator.target.candidateKey,
        supportTotal: centsToDollars(accumulator.supportCents),
        opposeTotal: centsToDollars(accumulator.opposeCents),
        groups: [...accumulator.groups.values()]
          .sort(
            (left, right) =>
              right.amountCents - left.amountCents ||
              left.supportOppose.localeCompare(right.supportOppose) ||
              left.committeeName.localeCompare(right.committeeName)
          )
          .slice(0, maxGroups)
          .map((group) => ({
            committeeId: group.committeeId,
            committeeName: group.committeeName,
            supportOppose: group.supportOppose,
            amount: centsToDollars(group.amountCents),
            sourceUrl,
          })),
      })
    )
    .sort((left, right) => left.candidateKey.localeCompare(right.candidateKey));

  return {
    candidates,
    reports: reports.sort((left, right) => left.reportId.localeCompare(right.reportId)),
    missingReportIds: missingReportIds.sort(),
    coverageGaps: coverageGaps.sort(
      (left, right) =>
        left.filerKey.localeCompare(right.filerKey) ||
        left.periodStartRaw.localeCompare(right.periodStartRaw)
    ),
    quarantinedGroups: selection.quarantinedGroups,
    unmatchedTargets: [...unmatchedTargets.values()].sort((left, right) => right.amountCents - left.amountCents),
    ambiguousTargets: [...ambiguousTargets.values()].sort((left, right) => right.amountCents - left.amountCents),
    excludedDeclarationRowCount,
    excludedDeclarationCents,
    nonIndependentExpenditureRowCount,
    federalTargetRowCount,
    federalTargetCents,
    nonCandidateTargetRowCount,
    countyMunicipalTargetRowCount,
    blankTargetRowCount,
    nonPositiveAmountRowCount,
    unknownReportTypeRowCount,
    attributedRowCount,
    attributedCents,
    quarantinedReportCount,
    overlappingReportPairCount,
    duplicateLookingRowCount,
    duplicateLookingCents,
  };
}
