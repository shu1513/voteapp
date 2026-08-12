// Outside (independent-expenditure) aggregation for one San José roster
// candidate (plan Phase 3, "Outside spending" semantics audited live
// 2026-08-10 against the 2025+2026 exports):
//
// - Sources: `S496` sheet ∪ `F460-D-ContribIndepExpn` rows with
//   `Expn_Code=IND`, deduped by (Filer_ID, Tran_ID) — verified live: all 41
//   Schedule D IND rows matched an S496 row by that key, so today the union
//   adds nothing; it exists for spenders who file 460s outside the 496
//   window. Schedule D `MON`/`IKD` rows are contributions TO committees and
//   never touch outside totals.
// - The same expenditure can be REPORTED twice (one live S496 case: spender
//   744711 Tran_ID PDT8743 under two e-filings; and this export demonstrably
//   re-reports 460s whole — Van Le and committee 1487288 both carry two
//   current filings for one period, which would duplicate Schedule D rows
//   too). Within (spender, Tran_ID, canonical target) the LATEST report wins
//   regardless of amount or direction, so a correcting re-report replaces
//   the original instead of adding to it. Only rows naming a DIFFERENT
//   canonical target on a shared key are all kept: multi-candidate mailers
//   were verified to carry distinct Tran_IDs per candidate, so a
//   differing-target collision is new information, not a duplicate. The
//   accepted trade-off: a spender reusing one Tran_ID across filings for two
//   distinct same-target expenditures would collapse — unobserved live
//   (173 of 174 keys unique) and indistinguishable from a correction anyway;
//   this fails toward never double-counting.
// - Target identity is structured but dirty: names change casing across rows
//   ("BIEN DOAN"/"Bien Doan"), sometimes sit whole in Cand_NamL and sometimes
//   split across NamL/NamF ("Ortiz"+"Peter"), Dist_No and Juris can be blank,
//   and rows with no candidate at all are ballot-measure spending. Name
//   matching reuses the resolver's token-based person gates (never
//   substring), and office/jurisdiction/district act as fail-closed vetoes on
//   name-matched rows.
// - Direction comes from Supp_Opp_Cd (this vendor writes the full words
//   SUPPORT/OPPOSE); anything else is never guessed — excluded and counted.
// - S497 is a 24-hour recency signal only and is deliberately absent here.
// - Spender labels (financeLabelClassifier) are applied at sync time, so this
//   aggregator emits raw spender names; `Filer_ID` may be the literal
//   "Pending", in which case spenders group by normalized name.
import type {
  EfileCalS496Row,
  EfileCalScheduleDRow,
} from "../efileCalFinance/efileCalWorkbookParser.js";
import {
  SAN_JOSE_PENDING_FILER_ID,
  normalizeSanJoseTextKey,
  sanJosePersonNameMatchesCandidate,
} from "./sanJoseCandidateCommitteeResolver.js";
import {
  validateSanJosePaper496Supplements,
  type SanJosePaper496Supplement,
} from "./sanJosePaperFilingSupplements.js";

export type SanJoseOutsideTargetCandidate = {
  displayName: string;
  officeName: "Mayor" | "City Council Member";
  /** Council district seat (1–10); null for Mayor. */
  seatNumber: number | null;
};

export type SanJoseOutsideSpendingGroup = {
  /** Raw Filer_ID — may be the literal "Pending". */
  spenderFilerId: string;
  spenderName: string;
  direction: "support" | "oppose";
  amountCents: number;
  expenditureCount: number;
};

export type SanJoseOutsideSpendingAggregate = {
  supportTotalCents: number;
  opposeTotalCents: number;
  /** All groups, largest first; the writer slices its own top-N. */
  groups: SanJoseOutsideSpendingGroup[];
  diagnostics: {
    s496Rows: number;
    /** Later reports of an already-reported expenditure (S496 and D alike). */
    duplicateReportRowsExcluded: number;
    /** (spender, Tran_ID) collisions with differing targets — kept, visible. */
    sharedTranIdRowsKept: number;
    scheduleDIndRows: number;
    /** D IND rows absent from S496 that entered the union (post-dedup). */
    scheduleDRowsAdded: number;
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

// CAL office codes: CCM = City Council Member (the only code observed live),
// MAY = Mayor (CAL spec; no San José mayor race until 2028 to observe).
const OFFICE_CODE_BY_OFFICE_NAME = {
  Mayor: "MAY",
  "City Council Member": "CCM",
} as const;

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
  return row.filerId === SAN_JOSE_PENDING_FILER_ID
    ? `${SAN_JOSE_PENDING_FILER_ID}::${normalizeSanJoseTextKey(row.filerName)}`
    : row.filerId;
}

// JSON.stringify keys everywhere: collision-proof without control-character
// delimiters (which also turn the source file binary for git).
function rowKey(row: OutsideRow): string {
  return JSON.stringify([spenderIdentity(row), row.tranId]);
}

// Canonical target: the combined person name, normalized — so "Peter Ortiz"
// whole in Cand_NamL and "Ortiz"+"Peter" split across NamL/NamF are ONE
// target, and a re-report that fixes the name layout, amount, or direction
// still collapses onto its original instead of double-counting.
function rowTargetIdentity(row: OutsideRow): string {
  return normalizeSanJoseTextKey(targetName(row));
}

/**
 * Collapse duplicate reports of one expenditure: within
 * (spender, Tran_ID, canonical target) the latest Rpt_Date wins, amount and
 * direction notwithstanding. Same-key rows with different canonical targets
 * are all kept and surfaced via sharedTranIdRowsKept.
 */
function dedupeLatestReports(rows: readonly OutsideRow[]): {
  rows: OutsideRow[];
  duplicateReportRowsExcluded: number;
  sharedTranIdRowsKept: number;
} {
  const byKey = new Map<string, OutsideRow[]>();
  for (const row of rows) {
    const keyRows = byKey.get(rowKey(row)) ?? [];
    keyRows.push(row);
    byKey.set(rowKey(row), keyRows);
  }
  let duplicateReportRowsExcluded = 0;
  let sharedTranIdRowsKept = 0;
  const deduped: OutsideRow[] = [];
  for (const keyRows of byKey.values()) {
    const byTarget = new Map<string, OutsideRow[]>();
    for (const row of keyRows) {
      const twins = byTarget.get(rowTargetIdentity(row)) ?? [];
      twins.push(row);
      byTarget.set(rowTargetIdentity(row), twins);
    }
    if (byTarget.size > 1) sharedTranIdRowsKept += keyRows.length;
    for (const twins of byTarget.values()) {
      twins.sort(
        (a, b) =>
          (b.rptDate ?? "").localeCompare(a.rptDate ?? "") ||
          b.eFilingId.length - a.eFilingId.length ||
          b.eFilingId.localeCompare(a.eFilingId),
      );
      deduped.push(twins[0]!);
      duplicateReportRowsExcluded += twins.length - 1;
    }
  }
  return { rows: deduped, duplicateReportRowsExcluded, sharedTranIdRowsKept };
}

export function aggregateSanJoseOutsideSpending(input: {
  candidate: SanJoseOutsideTargetCandidate;
  /** Concatenated rows from every calendar-year workbook the cycle spans. */
  s496: readonly EfileCalS496Row[];
  scheduleD: readonly EfileCalScheduleDRow[];
  /**
   * Curated paper-496 entries for THIS cycle (sync filters by election year).
   * They run through the same target-match and veto pipeline as export rows.
   */
  paperSupplements?: readonly SanJosePaper496Supplement[];
}): SanJoseOutsideSpendingAggregate {
  const { candidate } = input;
  if (candidate.officeName === "City Council Member" && candidate.seatNumber === null) {
    throw new Error(
      `San José outside-spending aggregation needs a seat number for council candidate ${candidate.displayName}`,
    );
  }
  const expectedOfficeCd = OFFICE_CODE_BY_OFFICE_NAME[candidate.officeName];

  const s496Rows: OutsideRow[] = input.s496.map((row) => ({
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
  }));
  const dedupedS496 = dedupeLatestReports(s496Rows);

  // --- Union: Schedule D IND rows not already reported on a 496, deduped
  // among themselves too — this export re-reports whole 460 filings (the
  // duplicate-period chains above), which duplicates their D rows.
  const s496Keys = new Set(s496Rows.map(rowKey));
  const dIndRows = input.scheduleD.filter((row) => row.expnCode === "IND");
  const dOnlyRows: OutsideRow[] = dIndRows
    .map((row) => ({
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
    }))
    .filter((row) => !s496Keys.has(rowKey(row)));
  const dedupedAdded = dedupeLatestReports(dOnlyRows);

  // --- Curated paper filings: synthetic rows with a "paper-496-" Tran_ID
  // namespace, so they can never collide with (or be deduped against) export
  // rows; validation already rejected duplicates within the list itself.
  const paperSupplements = input.paperSupplements ?? [];
  validateSanJosePaper496Supplements(paperSupplements);
  const supplementRows: OutsideRow[] = paperSupplements.map((entry) => ({
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

  // --- Per-candidate filter: name match first, then fail-closed vetoes. ---
  const diagnostics = {
    s496Rows: s496Rows.length,
    duplicateReportRowsExcluded:
      dedupedS496.duplicateReportRowsExcluded + dedupedAdded.duplicateReportRowsExcluded,
    sharedTranIdRowsKept:
      dedupedS496.sharedTranIdRowsKept + dedupedAdded.sharedTranIdRowsKept,
    scheduleDIndRows: dIndRows.length,
    scheduleDRowsAdded: dedupedAdded.rows.length,
    memoRowsExcluded: 0,
    nonCandidateTargetRows: 0,
    otherCandidateRows: 0,
    officeGateExcludedRows: 0,
    jurisdictionGateExcludedRows: 0,
    districtGateExcludedRows: 0,
    unknownDirectionRows: 0,
    unknownDirectionCents: 0,
    paperSupplementRows: supplementRows.length,
  };
  const included: { row: OutsideRow; direction: "support" | "oppose" }[] = [];
  for (const row of [...dedupedS496.rows, ...dedupedAdded.rows, ...supplementRows]) {
    if (row.memo) {
      diagnostics.memoRowsExcluded += 1;
      continue;
    }
    const name = targetName(row);
    if (name === null) {
      diagnostics.nonCandidateTargetRows += 1;
      continue;
    }
    if (!sanJosePersonNameMatchesCandidate(name, candidate.displayName)) {
      diagnostics.otherCandidateRows += 1;
      continue;
    }
    // Office veto: a same-named person in a different race must never book
    // money here. Blank Office_Cd on a name-matched row fails closed too.
    if (row.officeCd !== expectedOfficeCd) {
      diagnostics.officeGateExcludedRows += 1;
      continue;
    }
    // Jurisdiction veto when disclosed ("San Jose"/"CITY OF SAN JOSE"
    // variants observed; accents normalized). Blank passes — the office and
    // district gates still hold.
    if (
      row.jurisDscr !== null &&
      !normalizeSanJoseTextKey(row.jurisDscr).includes("SAN JOSE")
    ) {
      diagnostics.jurisdictionGateExcludedRows += 1;
      continue;
    }
    // District veto when disclosed; blank passes (observed live on rows that
    // are unambiguously ours by name + office). Council seats must agree;
    // a mayor row disclosing any district is malformed and fails closed.
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
    included.push({ row, direction: direction === "SUPPORT" ? "support" : "oppose" });
  }

  // --- Group by spender × direction. ---
  const groups = new Map<
    string,
    { spenderFilerId: string; names: Set<string>; direction: "support" | "oppose"; cents: number; count: number }
  >();
  for (const { row, direction } of included) {
    const key = JSON.stringify([spenderIdentity(row), direction]);
    const group = groups.get(key) ?? {
      spenderFilerId: row.filerId,
      names: new Set<string>(),
      direction,
      cents: 0,
      count: 0,
    };
    group.names.add(row.filerName);
    group.cents += row.amountCents;
    group.count += 1;
    groups.set(key, group);
  }
  const mapped: SanJoseOutsideSpendingGroup[] = [...groups.values()]
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
