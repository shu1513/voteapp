import {
  selectNcsbeCurrentFilings,
  type NcsbeFiling,
  type NcsbeQuarantinedGroup,
} from "./northCarolinaReportSelector.js";
import {
  isNcsbeReportYearInCycle,
  NCSBE_NO_TOTAL_REPORT_TYPES,
  NCSBE_OUTSIDE_LEG_REPORT_TYPES,
  type NcsbeDocumentRow,
  type NcsbeExpenditureRow,
  type NcsbeReceiptRow,
  type NcsbeReportDetail,
} from "./northCarolinaNcsbeParsers.js";

// Direct-money aggregation for one linked NC candidate committee over one
// Y−1..Y cycle window (north_carolina_plan.md decisions 7 + 11). The cover
// summary is authoritative for every summary number — `total_receipts` /
// `total_disbursements` are the selected reports' official Period values,
// `direct_contribution_total` is the cover's itemized + aggregated
// individual-contribution sections, and `cash_on_hand` is the latest
// report's end-of-period cash. Itemized receipt-row sums are reconciliation
// diagnostics, never totals (Ohio's $25.4M non-itemized lesson).
//
// Occupations DO ship for NC (decision 7): built only from itemized
// individual receipts (`ReceiptTypeCode` `"IND "` — trailing space is real —
// `IsAggregated` false, positive amount), with the placeholder vocabulary
// pinned from spike bytes excluded from `top_occupations` and counted in
// diagnostics. A receipt code outside the pinned set quarantines the derived
// breakdowns only (occupations + size buckets); cover-derived totals are
// unaffected.
//
// Pure aggregation: the caller (PR 7 sync) reads the artifact cache and
// supplies parsed inventory rows + per-report covers and transaction rows;
// nothing here fetches or writes.

export type NorthCarolinaDirectReportInput = {
  reportId: string;
  cover: NcsbeReportDetail;
  receiptRows: readonly NcsbeReceiptRow[];
  // Optional: regular-report expenditure rows feed the decision-3
  // single-source cross-check diagnostic (IE-typed rows in a regular report
  // whose money must only be counted from IE-inventory reports).
  expenditureRows?: readonly NcsbeExpenditureRow[];
};

export type NorthCarolinaDirectAggregationInput = {
  electionYear: number;
  // The committee's full document inventory; this module applies the
  // Disclosure Report + period-overlap filter and decision-8 selection.
  inventoryRows: readonly NcsbeDocumentRow[];
  reports: readonly NorthCarolinaDirectReportInput[];
  sourceUrl?: string | null;
  maxBreakdownsPerCategory?: number;
};

// What the sync should do with the snapshot (decision 8):
// - "ok": aggregate normally and write.
// - "honest_null": the portal proves a required period's current filing is
//   unavailable (image-only) or its lineage is ambiguous — WRITE the honest
//   snapshot (null summary fields, emptied direct breakdowns; the writer's
//   preserve-when-null policy keeps outside totals). Never leave stale money
//   visible.
// - "incomplete_artifacts": a selected report's cached artifacts were not
//   supplied, or a supplied cover declares a different rptID than the report
//   it was cached for (provably another report's bytes). Either way the
//   cache, not the portal, is suspect: do NOT write; keep the previous valid
//   snapshot and re-acquire.
export type NorthCarolinaDirectAggregationStatus = "ok" | "honest_null" | "incomplete_artifacts";

export type NorthCarolinaDirectFinanceSummary = {
  totalReceipts: number | null;
  directContributionTotal: number | null;
  totalDisbursements: number | null;
  cashOnHand: number | null;
  sourceUrl: string | null;
};

export type NorthCarolinaFinanceDirectBreakdown = {
  categoryType: "occupation" | "contribution_size";
  categoryName: string;
  amount: number;
  contributorCount: number;
  sourceUrl: string | null;
};

export type NorthCarolinaCycleChainMismatch = {
  previousReportId: string;
  reportId: string;
  section: "total_receipts" | "total_expenditures";
  expectedCycleCents: number;
  actualCycleCents: number;
};

export type NorthCarolinaDirectAggregationResult = {
  status: NorthCarolinaDirectAggregationStatus;
  summary: NorthCarolinaDirectFinanceSummary;
  directBreakdowns: NorthCarolinaFinanceDirectBreakdown[];
  selectedReportIds: string[];
  supersededUnavailablePeriods: Array<{ reportType: string | null; periodStartRaw: string; periodEndRaw: string }>;
  quarantinedGroups: NcsbeQuarantinedGroup[];
  missingReportIds: string[];
  // Inventory rows whose period bounds are missing/implausible — included in
  // selection (a typo must widen the window, never narrow it) and counted.
  unusablePeriodRowCount: number;
  // Pinned no-total forms (48-Hour Notice) dropped before selection —
  // counted so a dropped filing is visible, never silent.
  excludedNoTotalReportRowCount: number;
  // IE filings the outside leg owns (decision 3), dropped before selection
  // and counted so the single-source split stays visible.
  excludedOutsideLegReportRowCount: number;
  // Undated rows whose ReportYear puts them outside the cycle (a long-lived
  // committee's 1990s filings) — dropped before selection and counted.
  excludedUndatedOutOfCycleRowCount: number;
  // Advisory reconciliation (decision 11): itemized receipt-row sums vs the
  // authoritative cover values. In-kind timing and non-itemized income make
  // modest gaps normal; a large gap means artifact damage.
  itemizedReceiptsCents: number;
  coverTotalReceiptsCents: number | null;
  itemizedIndividualCents: number;
  coverIndividualContributionCents: number | null;
  // Cycle-column chain check (decision 11, advisory): consecutive selected
  // reports must satisfy Cycle_n = Cycle_{n−1} + Period_n; a mismatch means
  // a missing filing between them or portal damage.
  cycleChainMismatches: NorthCarolinaCycleChainMismatch[];
  // A cover whose own declared rptID is not the report it was cached for —
  // provably another report's bytes. Nonempty forces
  // status "incomplete_artifacts": suspect bytes never become writable money.
  coverIdentityMismatchReportIds: string[];
  // Advisory: the cover's begin/end dates disagree with the inventory period
  // of the same filing. Live PR 9 evidence says this is portal sloppiness,
  // not mispairing — 17 of 697 covers disagree, including begin-after-end
  // pairs (RID 233220: 07/01/2026 -> 06/30/2026) — so it is surfaced and
  // counted, never a reason to withhold a candidate's money.
  coverPeriodDisagreementReportIds: string[];
  // Derived-breakdown quarantine (decision 7): a ReceiptTypeCode outside the
  // pinned vocabulary empties occupations + size buckets for this candidate.
  derivedBreakdownsQuarantined: boolean;
  unknownReceiptTypeCodes: Array<{ code: string; rowCount: number; amountCents: number }>;
  includedIndividualRowCount: number;
  aggregatedIndividualRowCount: number;
  nonPositiveIndividualRowCount: number;
  placeholderOccupationRowCount: number;
  placeholderOccupationCents: number;
  occupationAttributedCents: number;
  // Cover section 109 sum — decision 11's premise (48-hour money reappears
  // on scheduled reports) is unverifiable offline, so surface the hook.
  fortyEightHourNoticeSumCents: number;
  negativeCashOnHand: boolean;
  // Decision-3 cross-check: IE-typed, explicitly-declared rows found in the
  // committee's REGULAR reports. Their money is only ever counted from
  // IE-inventory reports; a nonzero count with no ingested IE report for
  // this filer flags the inverse miss for audit (wired at PR 7).
  ieTypedRegularReportRowCount: number;
  ieTypedRegularReportCents: number;
};

const DEFAULT_MAX_BREAKDOWNS_PER_CATEGORY = 50;

// Cover summary sequences (pinned in NCSBE_COVER_SECTIONS).
const SECTION_AGGREGATED_INDIVIDUAL = 15;
const SECTION_ITEMIZED_INDIVIDUAL = 20;
const SECTION_TOTAL_RECEIPTS = 60;
const SECTION_TOTAL_EXPENDITURES = 90;
const SECTION_CASH_ON_HAND_END = 95;
const SECTION_48_HOUR_SUM = 109;

// ReceiptTypeCode vocabulary observed in regular-report spike/fixture bytes
// (decision 12 — seeded, grows at fixture time; the trailing spaces are
// real). "IND " is the only individual-contribution code; the others are
// known non-individual money. Anything else — including the IE-receipt
// "DON " code, never observed on a candidate committee — fails closed into
// the derived-breakdown quarantine.
//
// The PR 9 live run quarantined 52 of 167 candidates — 31% of the state lost
// occupations, NC's flagship finance feature — on six codes the two-committee
// spike had never produced. Each was read from its own rows before being
// admitted here, and every one is entity money, never a person:
//   "OUTS" Outside Source (26 rows / $79.6k over 19 committees)
//   "RFND" Refund/Reimbursement to the Committee (77 / $38.1k over 36)
//   "NFPC" Not for Profit Contribution (2 / $13.6k)
//   "GEN " General Contribution (5 / $9.9k, e.g. another candidate committee)
//   "CNRE" Contribution to be Reimbursed (1 / $529.46, the Postmaster)
//   "INT " Interest Earned (18 / $3.94, credit-union interest)
// Admitting them changes no published total — totals are cover-authoritative
// — it only stops those candidates' occupation and size buckets from being
// withheld over money that was never individual to begin with. A code still
// outside this vocabulary fails closed exactly as before.
const INDIVIDUAL_RECEIPT_TYPE_CODE = "IND ";
const KNOWN_NON_INDIVIDUAL_RECEIPT_TYPE_CODES = new Set([
  "CPCM",
  "PPTY",
  "OUTS",
  "RFND",
  "NFPC",
  "GEN ",
  "CNRE",
  "INT ",
]);

// Occupation placeholder vocabulary (spike results item 3), matched
// case-insensitively; `United States` is observed junk, not an occupation.
const OCCUPATION_PLACEHOLDERS = new Set([
  "NOT EMPLOYED",
  "RETIRED",
  "HOMEMAKER",
  "NO JOB TITLE",
  "UNITED STATES",
]);

function normalizeElectionYear(value: number): number {
  if (!Number.isInteger(value) || value < 2000 || value > 2100) {
    throw new Error(`Invalid North Carolina direct aggregation election year: ${value}`);
  }
  return value;
}

function normalizePositiveInteger(value: number | undefined, fallback: number, fieldName: string): number {
  const normalized = value ?? fallback;
  if (!Number.isInteger(normalized) || normalized <= 0) {
    throw new Error(`Invalid North Carolina direct aggregation ${fieldName}: ${normalized}`);
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

function centsToDollars(cents: number): number {
  return cents / 100;
}

// Shared bucket boundaries (maryland/ohio parity — the UI renders both).
function contributionSizeBucket(amountCents: number): string {
  if (amountCents < 100_00) {
    return "$1-$99";
  }
  if (amountCents < 250_00) {
    return "$100-$249";
  }
  if (amountCents < 500_00) {
    return "$250-$499";
  }
  if (amountCents < 1_000_00) {
    return "$500-$999";
  }
  if (amountCents < 5_000_00) {
    return "$1,000-$4,999";
  }
  return "$5,000+";
}

type SectionValues = { periodCents: number; cycleCents: number };

function coverSectionValues(cover: NcsbeReportDetail): Map<number, SectionValues> {
  const map = new Map<number, SectionValues>();
  for (const section of cover.summarySections) {
    map.set(section.sequence, { periodCents: section.periodCents, cycleCents: section.cycleCents });
  }
  return map;
}

function requireSection(sections: Map<number, SectionValues>, sequence: number, reportId: string): SectionValues {
  const section = sections.get(sequence);
  if (!section) {
    // The parser enforces the full 34-section set, so a miss here is caller
    // damage (a cover from a different parser version), not portal drift.
    throw new Error(`NCSBE report ${reportId} cover is missing summary sequence ${sequence}`);
  }
  return section;
}

// Direct-side filing filter: Disclosure Reports whose period touches the
// Y−1..Y window (spike extra finding: period overlap, not a report-type
// whitelist — Organizational and Interim reports carry money too). A row
// with unusable period bounds is INCLUDED and counted: a data-entry typo
// (live year-3026 date) must widen the window, never silently narrow it.
// 48-hour filings are Informational Reports, so they never enter (decision
// 11). Registered-committee IE mirrors are Informational too; a candidate
// committee's hypothetical IE Disclosure Report would enter the sums here —
// its cover money is that committee's own official spending, and the
// decision-3 single-source rule governs only OUTSIDE totals.
// Exported for the batch funder leg (PR 8): a registered spender's receipts
// go through this same cycle filter + the current-filing selector, so its
// donor money can never be summed off a superseded original.
export function selectNorthCarolinaDirectCycleReportRows(input: {
  rows: readonly NcsbeDocumentRow[];
  electionYear: number;
}): {
  rows: NcsbeDocumentRow[];
  unusablePeriodRowCount: number;
  excludedNoTotalReportRowCount: number;
  excludedOutsideLegReportRowCount: number;
  excludedUndatedOutOfCycleRowCount: number;
} {
  const cycleStartIso = `${input.electionYear - 1}-01-01`;
  const cycleEndIso = `${input.electionYear}-12-31`;
  const rows: NcsbeDocumentRow[] = [];
  let unusablePeriodRowCount = 0;
  let excludedNoTotalReportRowCount = 0;
  let excludedOutsideLegReportRowCount = 0;
  let excludedUndatedOutOfCycleRowCount = 0;
  for (const row of input.rows) {
    if (row.documentType !== "Disclosure Report") {
      continue;
    }
    // Pinned no-total forms (48-Hour Notice, verified live): the cover has
    // no totals and the receipts re-appear on the covering regular report,
    // so including one would fail the candidate on a missing artifact the
    // acquisition deliberately never fetches — and aggregating it would
    // double-count the money and its occupation row.
    if (row.reportType !== null && NCSBE_NO_TOTAL_REPORT_TYPES.has(row.reportType)) {
      excludedNoTotalReportRowCount += 1;
      continue;
    }
    // Decision 3: IE filings are the outside leg's, single-source. Reading
    // one here would double-count IE money, and an image-only one would fail
    // the reader closed over a report the direct side never wanted.
    if (row.reportType !== null && NCSBE_OUTSIDE_LEG_REPORT_TYPES.has(row.reportType)) {
      excludedOutsideLegReportRowCount += 1;
      continue;
    }
    const startIso = row.periodStartDate.iso;
    const endIso = row.periodEndDate.iso;
    if (startIso === null || endIso === null) {
      unusablePeriodRowCount += 1;
      // Undated rows fall back to ReportYear (same rule the acquisition
      // fetches by). A long-lived committee's undated 1990s filings must not
      // enter selection: the acquisition never fetched them, so they would
      // surface as permanently missing artifacts and fail the candidate — or,
      // on a spender, the whole year's funder leg.
      if (isNcsbeReportYearInCycle(row.reportYear, input.electionYear)) {
        rows.push(row);
      } else {
        excludedUndatedOutOfCycleRowCount += 1;
      }
      continue;
    }
    if (startIso <= cycleEndIso && endIso >= cycleStartIso) {
      rows.push(row);
    }
  }
  return {
    rows,
    unusablePeriodRowCount,
    excludedNoTotalReportRowCount,
    excludedOutsideLegReportRowCount,
    excludedUndatedOutOfCycleRowCount,
  };
}

type OccupationAggregate = {
  categoryName: string;
  amountCents: number;
  contributorKeys: Set<string>;
};

function contributorIdentityKey(row: NcsbeReceiptRow, reportId: string, rowIndex: number): string {
  if (row.groupId !== null) {
    return `G${row.groupId}`;
  }
  const nameKey = normalizeTextKey(row.orgName);
  return nameKey || `row-${reportId}-${rowIndex}`;
}

// Chronological order of selected filings: period end (the reporting-period
// boundary decision 11's cash rule keys on), then period start, then report
// id for determinism.
function comparePeriodOrder(left: NcsbeFiling, right: NcsbeFiling): number {
  return (
    (left.periodEndIso ?? "").localeCompare(right.periodEndIso ?? "") ||
    (left.periodStartIso ?? "").localeCompare(right.periodStartIso ?? "") ||
    (left.reportId ?? "").localeCompare(right.reportId ?? "")
  );
}

export function aggregateNorthCarolinaDirectFinance(
  input: NorthCarolinaDirectAggregationInput
): NorthCarolinaDirectAggregationResult {
  const electionYear = normalizeElectionYear(input.electionYear);
  const maxBreakdownsPerCategory = normalizePositiveInteger(
    input.maxBreakdownsPerCategory,
    DEFAULT_MAX_BREAKDOWNS_PER_CATEGORY,
    "maxBreakdownsPerCategory"
  );
  const sourceUrl = input.sourceUrl ?? null;

  const {
    rows: cycleRows,
    unusablePeriodRowCount,
    excludedNoTotalReportRowCount,
    excludedOutsideLegReportRowCount,
    excludedUndatedOutOfCycleRowCount,
  } = selectNorthCarolinaDirectCycleReportRows({
    rows: input.inventoryRows,
    electionYear,
  });
  const selection = selectNcsbeCurrentFilings({ rows: cycleRows });
  const selectedFilings = [...selection.selected].sort(comparePeriodOrder);
  const selectedReportIds = selectedFilings.map((filing) => filing.reportId!);
  const supersededUnavailablePeriods = selection.supersededUnavailable.map((filing) => ({
    reportType: filing.reportType,
    periodStartRaw: filing.periodStartRaw,
    periodEndRaw: filing.periodEndRaw,
  }));

  const reportsById = new Map(input.reports.map((report) => [report.reportId, report]));
  const missingReportIds = selectedReportIds.filter((reportId) => !reportsById.has(reportId));

  // Mispaired-artifact guard: the cover declares its own rptID, so bytes
  // cached for another report are provable — checked BEFORE any summing so
  // they can never become writable money. Period dates are NOT evidence of
  // mispairing: the live run found 17 of 697 covers disagreeing with their
  // inventory row on dates alone while the identity matched, so a strict
  // date check withheld eight real candidates' money. Those disagreements
  // stay as an advisory diagnostic.
  const coverIdentityMismatchReportIds: string[] = [];
  const coverPeriodDisagreementReportIds: string[] = [];
  for (const filing of selectedFilings) {
    const report = reportsById.get(filing.reportId!);
    if (!report) {
      continue;
    }
    if (report.cover.cover.reportId !== filing.reportId) {
      coverIdentityMismatchReportIds.push(filing.reportId!);
      continue;
    }
    const coverBegin = report.cover.cover.beginDate.iso;
    const coverEnd = report.cover.cover.endDate.iso;
    if (
      (coverBegin !== null && filing.periodStartIso !== null && coverBegin !== filing.periodStartIso) ||
      (coverEnd !== null && filing.periodEndIso !== null && coverEnd !== filing.periodEndIso)
    ) {
      coverPeriodDisagreementReportIds.push(filing.reportId!);
    }
  }

  const nullSummary: NorthCarolinaDirectFinanceSummary = {
    totalReceipts: null,
    directContributionTotal: null,
    totalDisbursements: null,
    cashOnHand: null,
    sourceUrl,
  };
  const emptyDiagnostics = {
    selectedReportIds,
    supersededUnavailablePeriods,
    quarantinedGroups: selection.quarantinedGroups,
    missingReportIds,
    unusablePeriodRowCount,
    excludedNoTotalReportRowCount,
    excludedOutsideLegReportRowCount,
    excludedUndatedOutOfCycleRowCount,
    itemizedReceiptsCents: 0,
    coverTotalReceiptsCents: null,
    itemizedIndividualCents: 0,
    coverIndividualContributionCents: null,
    cycleChainMismatches: [],
    coverIdentityMismatchReportIds,
    coverPeriodDisagreementReportIds,
    derivedBreakdownsQuarantined: false,
    unknownReceiptTypeCodes: [],
    includedIndividualRowCount: 0,
    aggregatedIndividualRowCount: 0,
    nonPositiveIndividualRowCount: 0,
    placeholderOccupationRowCount: 0,
    placeholderOccupationCents: 0,
    occupationAttributedCents: 0,
    fortyEightHourNoticeSumCents: 0,
    negativeCashOnHand: false,
    ieTypedRegularReportRowCount: 0,
    ieTypedRegularReportCents: 0,
  };

  // Decision 8: proof of supersession or ambiguous lineage anywhere in the
  // window taints the whole direct picture — totals summed around the hole
  // would understate silently. The honest snapshot is written.
  if (selection.supersededUnavailable.length > 0 || selection.quarantinedGroups.length > 0) {
    return {
      status: "honest_null",
      summary: nullSummary,
      directBreakdowns: [],
      ...emptyDiagnostics,
    };
  }
  // Missing cached artifacts and mispaired covers are cache/acquisition
  // problems, not portal evidence — the sync keeps the previous snapshot
  // and re-acquires.
  if (missingReportIds.length > 0 || coverIdentityMismatchReportIds.length > 0) {
    return {
      status: "incomplete_artifacts",
      summary: nullSummary,
      directBreakdowns: [],
      ...emptyDiagnostics,
    };
  }

  let totalReceiptsCents = 0;
  let totalDisbursementsCents = 0;
  let coverIndividualCents = 0;
  let fortyEightHourNoticeSumCents = 0;
  let itemizedReceiptsCents = 0;
  let itemizedIndividualCents = 0;
  let includedIndividualRowCount = 0;
  let aggregatedIndividualRowCount = 0;
  let nonPositiveIndividualRowCount = 0;
  let placeholderOccupationRowCount = 0;
  let placeholderOccupationCents = 0;
  let occupationAttributedCents = 0;
  let ieTypedRegularReportRowCount = 0;
  let ieTypedRegularReportCents = 0;
  const cycleChainMismatches: NorthCarolinaCycleChainMismatch[] = [];
  const unknownReceiptTypeCodes = new Map<string, { code: string; rowCount: number; amountCents: number }>();
  const occupationAggregates = new Map<string, OccupationAggregate>();
  const bucketAggregates = new Map<string, OccupationAggregate>();

  let previousSections: { reportId: string; sections: Map<number, SectionValues> } | null = null;
  let latestSections: Map<number, SectionValues> | null = null;

  for (const filing of selectedFilings) {
    const reportId = filing.reportId!;
    const report = reportsById.get(reportId)!;
    const sections = coverSectionValues(report.cover);

    totalReceiptsCents += requireSection(sections, SECTION_TOTAL_RECEIPTS, reportId).periodCents;
    totalDisbursementsCents += requireSection(sections, SECTION_TOTAL_EXPENDITURES, reportId).periodCents;
    coverIndividualCents +=
      requireSection(sections, SECTION_ITEMIZED_INDIVIDUAL, reportId).periodCents +
      requireSection(sections, SECTION_AGGREGATED_INDIVIDUAL, reportId).periodCents;
    fortyEightHourNoticeSumCents += requireSection(sections, SECTION_48_HOUR_SUM, reportId).periodCents;

    // Cycle chain (decision 11, spike-proven): valid between consecutive
    // filings; the window's first filing has no in-window predecessor.
    if (previousSections !== null) {
      for (const [sequence, section] of [
        [SECTION_TOTAL_RECEIPTS, "total_receipts"],
        [SECTION_TOTAL_EXPENDITURES, "total_expenditures"],
      ] as const) {
        const previous = requireSection(previousSections.sections, sequence, previousSections.reportId);
        const current = requireSection(sections, sequence, reportId);
        const expectedCycleCents = previous.cycleCents + current.periodCents;
        if (expectedCycleCents !== current.cycleCents) {
          cycleChainMismatches.push({
            previousReportId: previousSections.reportId,
            reportId,
            section,
            expectedCycleCents,
            actualCycleCents: current.cycleCents,
          });
        }
      }
    }
    previousSections = { reportId, sections };
    latestSections = sections;

    for (const [rowIndex, row] of report.receiptRows.entries()) {
      itemizedReceiptsCents += row.amountCents;
      const code = row.receiptTypeCode ?? "";
      if (code === INDIVIDUAL_RECEIPT_TYPE_CODE) {
        itemizedIndividualCents += row.amountCents;
        if (row.isAggregated) {
          aggregatedIndividualRowCount += 1;
          continue;
        }
        if (row.amountCents <= 0) {
          nonPositiveIndividualRowCount += 1;
          continue;
        }
        includedIndividualRowCount += 1;
        const contributorKey = contributorIdentityKey(row, reportId, rowIndex);

        const bucketName = contributionSizeBucket(row.amountCents);
        const bucket = bucketAggregates.get(bucketName);
        if (bucket) {
          bucket.amountCents += row.amountCents;
          bucket.contributorKeys.add(contributorKey);
        } else {
          bucketAggregates.set(bucketName, {
            categoryName: bucketName,
            amountCents: row.amountCents,
            contributorKeys: new Set([contributorKey]),
          });
        }

        const professionRaw = (row.profession ?? "").replace(/\s+/g, " ").trim();
        const professionKey = normalizeTextKey(professionRaw);
        if (!professionKey || OCCUPATION_PLACEHOLDERS.has(professionKey)) {
          placeholderOccupationRowCount += 1;
          placeholderOccupationCents += row.amountCents;
          continue;
        }
        occupationAttributedCents += row.amountCents;
        const occupation = occupationAggregates.get(professionKey);
        if (occupation) {
          occupation.amountCents += row.amountCents;
          occupation.contributorKeys.add(contributorKey);
        } else {
          occupationAggregates.set(professionKey, {
            // First-seen casing is displayed; the uppercase key merges the
            // portal's free-text case variants ("psychiatrist"/"Psychiatrist").
            categoryName: professionRaw,
            amountCents: row.amountCents,
            contributorKeys: new Set([contributorKey]),
          });
        }
        continue;
      }
      if (!KNOWN_NON_INDIVIDUAL_RECEIPT_TYPE_CODES.has(code)) {
        const existing = unknownReceiptTypeCodes.get(code);
        if (existing) {
          existing.rowCount += 1;
          existing.amountCents += row.amountCents;
        } else {
          unknownReceiptTypeCodes.set(code, { code, rowCount: 1, amountCents: row.amountCents });
        }
      }
    }

    for (const row of report.expenditureRows ?? []) {
      const declaration = normalizeTextKey(row.declaration);
      if (
        normalizeTextKey(row.expenditureTypeDesc) === "INDEPENDENT EXPENDITURE" &&
        (declaration === "SUPPORT" || declaration === "OPPOSE")
      ) {
        ieTypedRegularReportRowCount += 1;
        ieTypedRegularReportCents += row.ieAmountCents ?? row.amountCents;
      }
    }
  }

  // Decision 11: cash is the LATEST report's end-of-period value, never a
  // sum; a negative balance writes NULL + diagnostic (canonical schema
  // rejects negatives), never a clamp.
  let cashOnHandCents: number | null = null;
  let negativeCashOnHand = false;
  if (latestSections !== null) {
    const latestReportId = selectedReportIds[selectedReportIds.length - 1]!;
    const cash = requireSection(latestSections, SECTION_CASH_ON_HAND_END, latestReportId).periodCents;
    if (cash < 0) {
      negativeCashOnHand = true;
    } else {
      cashOnHandCents = cash;
    }
  }

  const derivedBreakdownsQuarantined = unknownReceiptTypeCodes.size > 0;
  const directBreakdowns: NorthCarolinaFinanceDirectBreakdown[] = [];
  if (!derivedBreakdownsQuarantined) {
    for (const [categoryType, aggregates, limit] of [
      ["occupation", occupationAggregates, maxBreakdownsPerCategory],
      ["contribution_size", bucketAggregates, Number.POSITIVE_INFINITY],
    ] as const) {
      for (const aggregate of [...aggregates.values()]
        .sort(
          (left, right) =>
            right.amountCents - left.amountCents || left.categoryName.localeCompare(right.categoryName)
        )
        .slice(0, limit === Number.POSITIVE_INFINITY ? undefined : limit)) {
        directBreakdowns.push({
          categoryType,
          categoryName: aggregate.categoryName,
          amount: centsToDollars(aggregate.amountCents),
          contributorCount: aggregate.contributorKeys.size,
          sourceUrl,
        });
      }
    }
  }

  return {
    status: "ok",
    summary: {
      totalReceipts: centsToDollars(totalReceiptsCents),
      directContributionTotal: centsToDollars(coverIndividualCents),
      totalDisbursements: centsToDollars(totalDisbursementsCents),
      cashOnHand: cashOnHandCents === null ? null : centsToDollars(cashOnHandCents),
      sourceUrl,
    },
    directBreakdowns,
    selectedReportIds,
    supersededUnavailablePeriods,
    quarantinedGroups: selection.quarantinedGroups,
    missingReportIds,
    unusablePeriodRowCount,
    excludedNoTotalReportRowCount,
    excludedOutsideLegReportRowCount,
    excludedUndatedOutOfCycleRowCount,
    itemizedReceiptsCents,
    coverTotalReceiptsCents: totalReceiptsCents,
    itemizedIndividualCents,
    coverIndividualContributionCents: coverIndividualCents,
    cycleChainMismatches,
    coverIdentityMismatchReportIds,
    coverPeriodDisagreementReportIds,
    derivedBreakdownsQuarantined,
    unknownReceiptTypeCodes: [...unknownReceiptTypeCodes.values()].sort((left, right) =>
      left.code.localeCompare(right.code)
    ),
    includedIndividualRowCount,
    aggregatedIndividualRowCount,
    nonPositiveIndividualRowCount,
    placeholderOccupationRowCount,
    placeholderOccupationCents,
    occupationAttributedCents,
    fortyEightHourNoticeSumCents,
    negativeCashOnHand,
    ieTypedRegularReportRowCount,
    ieTypedRegularReportCents,
  };
}
