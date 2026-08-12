// Outside (independent-expenditure) aggregation for one San Diego roster
// candidate (plan Phase 3). Structured like the San José aggregator but the
// dedup and gates are San Diego's own, specified by the Phase 0 probe's
// hand-derived tally (probeSanDiegoCandidateFinance, live 2026-08-10..12) —
// the SJ implementation is wrong for San Diego in three load-bearing ways:
//
// 1. DUAL-IDENTITY dedup. SJ dedups within (spender, Tran_ID, target), which
//    requires the spender identity to match. San Diego's live data breaks
//    both halves of that key:
//      - the same expenditure appears as a blank-Filer_ID 496 row AND an
//        id-carrying Schedule D row (Working Families Opposing Bailey EDT2 —
//        id-only matching double-counts $22,165), and
//      - committee 1490398 files its 496s under a 68-char short spelling and
//        its Schedule D rows under the 179-char sponsored spelling
//        (name-only matching double-counts $146k).
//    So rows bucket by (Tran_ID, canonical target) alone, and within a bucket
//    rows are the same expenditure iff they share a real FPPC id OR a
//    normalized spender name (connected components — neither alone suffices).
//    Within a component the latest Rpt_Date wins CROSS-source: an amended 496
//    outranks an older 460's Schedule D row (live: WFOB PDT1 is $50,000 on
//    the 05-20 460 but $45,000 on the 07-06 496 amendment). Ties break to the
//    higher e_filing_id (one global ascending sequence).
// 2. Council office codes are a SET: CAL writes CCM, but this vendor's SD
//    tenant emits COU on a minority of rows (10 of 158 S496 rows in the live
//    2026 file) — both are council. Blank still fails closed.
// 3. The jurisdiction veto looks for "SAN DIEGO" ("City of San Diego"
//    observed; SJ's hardcoded "SAN JOSE" excludes every SD row).
//
// Everything else keeps the SJ semantics: S496 ∪ Schedule-D `Expn_Code=IND`
// as ONE pool (D MON/IKD rows are contributions TO committees), memo rows
// dropped, token-based target matching (never substring), office/jurisdiction/
// district as fail-closed vetoes on name-matched rows, direction only from a
// literal SUPPORT/OPPOSE, S497 deliberately absent (24-hour recency signal),
// curated paper-496 supplements joining the pool through the same pipeline,
// spender labels applied at sync time.
import type {
  EfileCalS496Row,
  EfileCalScheduleDRow,
} from "../efileCalFinance/efileCalWorkbookParser.js";
import {
  normalizeSanDiegoCityTextKey,
  sanDiegoCityPersonNameMatchesCandidate,
} from "./sanDiegoCityCandidateCommitteeResolver.js";
import { SAN_DIEGO_PENDING_FILER_ID } from "./sanDiegoCityFinanceWriter.js";
import {
  validateSanDiegoCityPaper496Supplements,
  type SanDiegoCityPaper496Supplement,
} from "./sanDiegoCityPaperFilingSupplements.js";

export type SanDiegoCityOutsideTargetCandidate = {
  displayName: string;
  officeName: "Mayor" | "City Council Member";
  /** Council district seat (1–9); null for Mayor. */
  seatNumber: number | null;
};

export type SanDiegoCityOutsideSpendingGroup = {
  /** Raw Filer_ID — may be the literal "Pending". */
  spenderFilerId: string;
  spenderName: string;
  direction: "support" | "oppose";
  amountCents: number;
  expenditureCount: number;
};

export type SanDiegoCityOutsideSpendingAggregate = {
  supportTotalCents: number;
  opposeTotalCents: number;
  /** All groups, largest first; the writer slices its own top-N. */
  groups: SanDiegoCityOutsideSpendingGroup[];
  diagnostics: {
    s496Rows: number;
    scheduleDIndRows: number;
    /** Non-surviving rows of dedup components (cross-source twins included). */
    duplicateReportRowsExcluded: number;
    /** Rows kept in a (Tran_ID, target) bucket holding >1 distinct spender. */
    sharedTranIdRowsKept: number;
    memoRowsExcluded: number;
    /** No candidate named — ballot-measure spending, out of scope. */
    nonCandidateTargetRows: number;
    /** Candidate named, but not this one. */
    otherCandidateRows: number;
    /** Name matched but Office_Cd / jurisdiction / Dist_No vetoed the row. */
    officeGateExcludedRows: number;
    jurisdictionGateExcludedRows: number;
    districtGateExcludedRows: number;
    /** Supp_Opp_Cd was not SUPPORT/OPPOSE — direction never guessed. */
    unknownDirectionRows: number;
    unknownDirectionCents: number;
    /** Curated paper-496 entries fed in (pre-filter; most target OTHER candidates). */
    paperSupplementRows: number;
    /** Supplement rows that survived the target/veto gates INTO this
     * candidate's totals — the count the coverage note must speak about. */
    paperSupplementRowsIncluded: number;
    /** Supplements dropped because their filing entered the export (stale entry). */
    paperSupplementRowsSuppressed: number;
  };
};

/** The union row shape both sources map into. */
type OutsideRow = {
  filerId: string;
  filerName: string;
  tranId: string;
  eFilingId: string;
  rptDate: string | null;
  amountCents: number;
  candidateLastName: string | null;
  candidateFirstName: string | null;
  officeCd: string | null;
  jurisDscr: string | null;
  distNo: string | null;
  suppOppCd: string | null;
  memo: boolean;
};

// CAL office codes accepted per office. Council is a set (CCM + the vendor's
// COU variant, both observed live); MAY per the CAL spec — no San Diego mayor
// race in the export window to observe.
const OFFICE_CODES_BY_OFFICE_NAME: Record<
  SanDiegoCityOutsideTargetCandidate["officeName"],
  ReadonlySet<string>
> = {
  Mayor: new Set(["MAY"]),
  "City Council Member": new Set(["CCM", "COU"]),
};

function targetName(row: OutsideRow): string | null {
  if (row.candidateLastName === null && row.candidateFirstName === null) return null;
  if (row.candidateFirstName === null) return row.candidateLastName;
  if (row.candidateLastName === null) return row.candidateFirstName;
  return `${row.candidateFirstName} ${row.candidateLastName}`;
}

// "Pending" is not an identity (two ID-less spenders must never meet at one
// key), so pending spenders key by normalized committee name — the same rule
// the spender grouping below and the resolver's committee grouping use.
function spenderIdentity(row: OutsideRow): string {
  return row.filerId === SAN_DIEGO_PENDING_FILER_ID
    ? `${SAN_DIEGO_PENDING_FILER_ID}::${normalizeSanDiegoCityTextKey(row.filerName)}`
    : row.filerId;
}

// Canonical target: the combined person name, normalized — so "Rafael Perez"
// whole in Cand_NamL and "Perez"+"Rafael" split across NamL/NamF are ONE
// target, and a re-report that fixes the name layout, amount, or direction
// still collapses onto its original instead of double-counting.
function rowTargetIdentity(row: OutsideRow): string {
  return normalizeSanDiegoCityTextKey(targetName(row));
}

/** Latest report first: Rpt_Date desc, then e_filing_id desc (numeric text). */
function byLatestReport(a: OutsideRow, b: OutsideRow): number {
  return (
    (b.rptDate ?? "").localeCompare(a.rptDate ?? "") ||
    b.eFilingId.length - a.eFilingId.length ||
    b.eFilingId.localeCompare(a.eFilingId)
  );
}

/**
 * Collapse duplicate reports of one expenditure with the dual-identity rule:
 * bucket by (Tran_ID, canonical target); within a bucket, rows connect into
 * one component when they share a real FPPC id OR a normalized spender name;
 * within a component the latest report wins. Distinct components on a shared
 * key are genuinely different spenders' expenditures — all kept.
 */
function dedupeLatestReports(rows: readonly OutsideRow[]): {
  rows: { row: OutsideRow; componentFilerIds: string[]; componentNames: string[] }[];
  duplicateReportRowsExcluded: number;
  sharedTranIdRowsKept: number;
} {
  const byBucket = new Map<string, OutsideRow[]>();
  for (const row of rows) {
    const key = JSON.stringify([row.tranId, rowTargetIdentity(row)]);
    const bucket = byBucket.get(key) ?? [];
    bucket.push(row);
    byBucket.set(key, bucket);
  }
  let duplicateReportRowsExcluded = 0;
  let sharedTranIdRowsKept = 0;
  const deduped: {
    row: OutsideRow;
    componentFilerIds: string[];
    componentNames: string[];
  }[] = [];
  for (const bucket of byBucket.values()) {
    // Connected components over shared-real-id / shared-name edges (buckets
    // are a handful of rows, so quadratic scanning is fine).
    const components: OutsideRow[][] = [];
    for (const row of bucket) {
      // A name edge needs an actual name: the parser rejects blank
      // Filer_NamL but a punctuation-only value normalizes to "" — an empty
      // key proves nothing shared, and matching on it would merge two
      // distinct spenders and drop one expenditure.
      const rowName = normalizeSanDiegoCityTextKey(row.filerName);
      const linked = components.filter((component) =>
        component.some(
          (other) =>
            (row.filerId !== SAN_DIEGO_PENDING_FILER_ID && other.filerId === row.filerId) ||
            (rowName !== "" && normalizeSanDiegoCityTextKey(other.filerName) === rowName),
        ),
      );
      if (linked.length === 0) {
        components.push([row]);
        continue;
      }
      const merged = [...linked.flat(), row];
      for (const component of linked) components.splice(components.indexOf(component), 1);
      components.push(merged);
    }
    if (components.length > 1) sharedTranIdRowsKept += bucket.length;
    for (const component of components) {
      component.sort(byLatestReport);
      duplicateReportRowsExcluded += component.length - 1;
      deduped.push({
        row: component[0]!,
        // The component's collective identity, latest-report order: a blank-id
        // survivor still groups under the real FPPC id its Schedule D twin
        // disclosed (they are the same spender by construction).
        componentFilerIds: component
          .map((row) => row.filerId)
          .filter((filerId) => filerId !== SAN_DIEGO_PENDING_FILER_ID),
        componentNames: component.map((row) => row.filerName),
      });
    }
  }
  return { rows: deduped, duplicateReportRowsExcluded, sharedTranIdRowsKept };
}

export function aggregateSanDiegoCityOutsideSpending(input: {
  candidate: SanDiegoCityOutsideTargetCandidate;
  /** Concatenated rows from every calendar-year workbook the cycle spans. */
  s496: readonly EfileCalS496Row[];
  scheduleD: readonly EfileCalScheduleDRow[];
  /**
   * Curated paper-496 entries for THIS cycle (sync filters by election year).
   * They run through the same target-match and veto pipeline as export rows.
   */
  paperSupplements?: readonly SanDiegoCityPaper496Supplement[];
}): SanDiegoCityOutsideSpendingAggregate {
  const { candidate } = input;
  if (candidate.officeName === "City Council Member" && candidate.seatNumber === null) {
    throw new Error(
      `San Diego outside-spending aggregation needs a seat number for council candidate ${candidate.displayName}`,
    );
  }
  const expectedOfficeCds = OFFICE_CODES_BY_OFFICE_NAME[candidate.officeName];

  const mapRow = (row: EfileCalS496Row | EfileCalScheduleDRow): OutsideRow => ({
    filerId: row.filerId,
    filerName: row.filerName,
    tranId: row.tranId,
    eFilingId: row.eFilingId,
    rptDate: row.rptDate,
    amountCents: row.amountCents,
    candidateLastName: row.candidateLastName,
    candidateFirstName: row.candidateFirstName,
    officeCd: row.officeCd,
    jurisDscr: row.jurisDscr,
    distNo: row.distNo,
    suppOppCd: row.suppOppCd,
    memo: row.memo,
  });
  const s496Rows = input.s496.map(mapRow);
  const dIndRows = input.scheduleD.filter((row) => row.expnCode === "IND");

  // --- Curated paper filings: synthetic rows with a "paper-496-" Tran_ID
  // namespace, so they can never collide with (or be deduped against) export
  // rows; validation already rejected duplicates within the list itself.
  //
  // Double-count backstop: portal e_filing_ids are one global sequence, so an
  // export row carrying a supplement's id — directly or as its amendment
  // chain's origin — can only mean that filing entered the export. The export
  // row is then authoritative and the stale supplement is suppressed
  // (counted, never added). A FRESH e-filed re-report under a new id is
  // undetectable by any id check — that path stays on the module's manual
  // re-verification contract.
  const exportFilingIds = new Set<string>();
  for (const row of input.s496) {
    exportFilingIds.add(row.eFilingId);
    exportFilingIds.add(row.origEFilingId);
  }
  for (const row of input.scheduleD) {
    exportFilingIds.add(row.eFilingId);
    exportFilingIds.add(row.origEFilingId);
  }
  const paperSupplements = input.paperSupplements ?? [];
  validateSanDiegoCityPaper496Supplements(paperSupplements);
  const livePaperSupplements = paperSupplements.filter(
    (entry) => !exportFilingIds.has(entry.eFilingId),
  );
  const supplementRows: OutsideRow[] = livePaperSupplements.map((entry) => ({
    filerId: entry.spenderFilerId,
    filerName: entry.spenderName,
    tranId: `paper-496-${entry.eFilingId}`,
    eFilingId: entry.eFilingId,
    rptDate: entry.expenditureDate,
    amountCents: entry.amountCents,
    candidateLastName: entry.candidateLastName,
    candidateFirstName: entry.candidateFirstName,
    officeCd: entry.officeCd,
    jurisDscr: entry.jurisDscr,
    distNo: entry.distNo,
    suppOppCd: entry.direction,
    memo: false,
  }));

  // --- ONE pool across both sources (+ supplements), dual-identity dedup:
  // a 496 row and its Schedule D twin meet in the same (Tran_ID, target)
  // bucket and the latest report survives regardless of source sheet.
  const deduped = dedupeLatestReports([...s496Rows, ...dIndRows.map(mapRow), ...supplementRows]);

  // --- Per-candidate filter: name match first, then fail-closed vetoes. ---
  const diagnostics = {
    s496Rows: s496Rows.length,
    scheduleDIndRows: dIndRows.length,
    duplicateReportRowsExcluded: deduped.duplicateReportRowsExcluded,
    sharedTranIdRowsKept: deduped.sharedTranIdRowsKept,
    memoRowsExcluded: 0,
    nonCandidateTargetRows: 0,
    otherCandidateRows: 0,
    officeGateExcludedRows: 0,
    jurisdictionGateExcludedRows: 0,
    districtGateExcludedRows: 0,
    unknownDirectionRows: 0,
    unknownDirectionCents: 0,
    paperSupplementRows: supplementRows.length,
    paperSupplementRowsIncluded: 0,
    paperSupplementRowsSuppressed:
      paperSupplements.length - livePaperSupplements.length,
  };
  const included: {
    row: OutsideRow;
    componentFilerIds: string[];
    componentNames: string[];
    direction: "support" | "oppose";
  }[] = [];
  for (const entry of deduped.rows) {
    const { row } = entry;
    if (row.memo) {
      diagnostics.memoRowsExcluded += 1;
      continue;
    }
    const name = targetName(row);
    if (name === null) {
      diagnostics.nonCandidateTargetRows += 1;
      continue;
    }
    if (!sanDiegoCityPersonNameMatchesCandidate(name, candidate.displayName)) {
      diagnostics.otherCandidateRows += 1;
      continue;
    }
    // Office veto: a same-named person in a different race must never book
    // money here. Blank Office_Cd on a name-matched row fails closed too.
    if (row.officeCd === null || !expectedOfficeCds.has(row.officeCd)) {
      diagnostics.officeGateExcludedRows += 1;
      continue;
    }
    // Jurisdiction veto when disclosed ("City of San Diego" observed, mixed
    // case; accents normalized). Blank passes — the office and district gates
    // still hold.
    if (
      row.jurisDscr !== null &&
      !normalizeSanDiegoCityTextKey(row.jurisDscr).includes("SAN DIEGO")
    ) {
      diagnostics.jurisdictionGateExcludedRows += 1;
      continue;
    }
    // District veto when disclosed; blank passes (observed live on rows that
    // are unambiguously ours by name + office). Council seats must agree;
    // a mayor row disclosing any district is malformed and fails closed.
    // Fail-closed casualty known at build time: California Working Families
    // Party's 7 S496 rows supporting Nicole Crosby (D2) are tagged Dist_No=6
    // upstream — $8,194.67 excluded here until the Phase 4 PDF check settles
    // the correction (the veto is right; the data is wrong).
    if (row.distNo !== null) {
      const district = /^\d{1,2}$/.test(row.distNo) ? Number(row.distNo) : null;
      if (candidate.seatNumber === null || district !== candidate.seatNumber) {
        diagnostics.districtGateExcludedRows += 1;
        continue;
      }
    }
    const direction = row.suppOppCd?.trim().toUpperCase();
    if (direction !== "SUPPORT" && direction !== "OPPOSE") {
      diagnostics.unknownDirectionRows += 1;
      diagnostics.unknownDirectionCents += row.amountCents;
      continue;
    }
    // Supplement rows live in the reserved "paper-496-" Tran_ID namespace,
    // so a surviving one is recognizable here.
    if (row.tranId.startsWith("paper-496-"))
      diagnostics.paperSupplementRowsIncluded += 1;
    included.push({
      ...entry,
      direction: direction === "SUPPORT" ? "support" : "oppose",
    });
  }

  // --- Group by spender × direction. A component's identity is its best
  // disclosed one: the first real FPPC id any of its rows carried (latest-
  // report order), so a blank-id survivor still lands in its committee's
  // group instead of spawning a parallel "Pending" group.
  const groups = new Map<
    string,
    { spenderFilerId: string; names: Set<string>; direction: "support" | "oppose"; cents: number; count: number }
  >();
  for (const { row, componentFilerIds, componentNames, direction } of included) {
    const bestFilerId = componentFilerIds[0] ?? SAN_DIEGO_PENDING_FILER_ID;
    const identity =
      bestFilerId === SAN_DIEGO_PENDING_FILER_ID
        ? spenderIdentity(row)
        : bestFilerId;
    const key = JSON.stringify([identity, direction]);
    const group = groups.get(key) ?? {
      spenderFilerId: bestFilerId,
      names: new Set<string>(),
      direction,
      cents: 0,
      count: 0,
    };
    for (const name of componentNames) group.names.add(name);
    group.cents += row.amountCents;
    group.count += 1;
    groups.set(key, group);
  }
  const mapped: SanDiegoCityOutsideSpendingGroup[] = [...groups.values()]
    .map((group) => ({
      spenderFilerId: group.spenderFilerId,
      // Longest observed spelling — the fullest disclosed name.
      spenderName: [...group.names].sort((a, b) => b.length - a.length || a.localeCompare(b))[0]!,
      direction: group.direction,
      amountCents: group.cents,
      expenditureCount: group.count,
    }))
    .sort((a, b) => b.amountCents - a.amountCents || a.spenderName.localeCompare(b.spenderName));

  return {
    supportTotalCents: mapped
      .filter((group) => group.direction === "support")
      .reduce((sum, group) => sum + group.amountCents, 0),
    opposeTotalCents: mapped
      .filter((group) => group.direction === "oppose")
      .reduce((sum, group) => sum + group.amountCents, 0),
    groups: mapped,
    diagnostics,
  };
}
