// Direct-campaign aggregation for a resolved San José candidate committee
// (plan Phase 3), computing the cycle formulas proven live 2026-08-10 against
// the 2025+2026 exports (plan "Cycle formulas"):
//
//   total_raised   = Σ(F460 line 1 Amount_A) + Σ(F460 line 4 Amount_A)
//   loans_received = Σ(B1 summary line 1 Amount_A)   (gross loan receipts)
//   total_spent    = Σ(F460 line 11 Amount_A)
//   cash_on_hand   = latest filing's F460 line 16 Amount_A
//   debts_owed     = latest filing's F460 line 19 Amount_A
//   unitemized     = Σ(A line 2) + Σ(C line 2)
//
// Form 460 line 5 includes loans and is NEVER used (VoteApp keeps loans out
// of total_raised). Amount_B is calendar-year-to-date, not cycle-to-date, so
// only Amount_A is ever summed; the caller supplies every calendar-year
// workbook the cycle spans (2026 cycle = 2025 + 2026 files).
//
// Canonical filing selection is load-bearing, not defensive: the live
// `most_recent_only` export contains committees with TWO current filings for
// the same period — including two independent amendment chains (Van Le,
// orig 20746 rpt 002 vs orig 24993 rpt 001, both covering 2025-07-01..12-31,
// with their Schedule A rows duplicated too). Summing naively double-counts,
// so exactly one filing per period wins (latest Rpt_Date, then highest
// Report_Num, then highest e_filing_id) and the transaction sheets are
// filtered to the winners' e_filing_ids. Every dropped duplicate is a
// violation so sync can quarantine for manual reconciliation.
//
// Filed arithmetic can be internally wrong (Bien Doan's current 460 carries a
// $20,000 YTD line-3 error; Peter Ortiz restates beginning cash mid-cycle) —
// invariant violations are reported, never silently published from.
import type {
  EfileCalContributionRow,
  EfileCalLoanRow,
  EfileCalSummaryRow,
} from "../efileCalFinance/efileCalWorkbookParser.js";

export type SanJoseDirectViolationType =
  /** Filing rows disagree on period/report metadata, or the period is absent — filing excluded. */
  | "filing_unusable"
  /** More than one current filing covers the same period; losers excluded from totals. */
  | "duplicate_period_filings"
  /** One filing repeats a (Form_Type, Line_Item) summary row; first occurrence used. */
  | "duplicate_summary_line"
  /** A core F460 line (1, 2, 3, 4, 5, 11, 12, 16, 19) is absent from a filing. */
  | "missing_summary_line"
  /** line 3 ≠ line 1 + line 2 or line 5 ≠ line 3 + line 4 (per column). */
  | "line_arithmetic"
  /** Consecutive canonical filings overlap. */
  | "period_overlap"
  /** A day between consecutive canonical filings is covered by neither. */
  | "period_gap"
  /** Next filing's beginning cash (line 12) ≠ previous filing's ending cash (line 16). */
  | "cash_chain"
  /** First covered filing opens with nonzero cash — pre-cycle-file activity the export lacks. */
  | "prior_activity_uncovered"
  /** Σ non-memo Schedule A rows + Σ(A line 2) ≠ Σ(F460 line 1) (or the C/line-4 analogue). */
  | "contribution_reconciliation"
  /** Σ non-memo B1-sheet Loan_Amt1 ≠ Σ(B1 summary line 1). */
  | "loan_cross_check";

export type SanJoseDirectViolation = {
  type: SanJoseDirectViolationType;
  /** The filing the violation is anchored to; null for committee-level checks. */
  eFilingId: string | null;
  message: string;
};

export type SanJoseDirectBreakdown = {
  categoryType: "occupation" | "employer" | "contribution_size";
  categoryName: string;
  amountCents: number;
  /**
   * Positive contribution rows in this category, not distinct people —
   * repeat donors count once per contribution (the LA/SF semantics the
   * shared summary card already displays).
   */
  contributorCount: number;
};

export type SanJoseDirectFinanceAggregate = {
  totalRaisedCents: number;
  totalSpentCents: number;
  loansReceivedCents: number;
  /** Latest canonical filing's line 16/19; null when the committee has no usable filing. */
  cashOnHandCents: number | null;
  debtsOwedCents: number | null;
  /** Per-filing unitemized (<$100) monetary totals (A line 2). */
  unitemizedCents: number;
  /** Per-filing unitemized nonmonetary totals (C line 2). */
  unitemizedNonmonetaryCents: number;
  /** Latest covered Thru_Date — data recency, not sync time. */
  reportedThrough: string | null;
  /** Earliest covered From_Date. */
  coverageStart: string | null;
  /** Canonical filings in period order. Empty = nothing to publish. */
  filings: { eFilingId: string; reportNum: string; fromDate: string; thruDate: string }[];
  breakdowns: SanJoseDirectBreakdown[];
  violations: SanJoseDirectViolation[];
  diagnostics: {
    summaryRows: number;
    filingsSeen: number;
    duplicateFilingsExcluded: number;
    scheduleARows: number;
    scheduleCRows: number;
    refundRows: number;
    refundCents: number;
    memoRowsExcluded: number;
    memoCentsExcluded: number;
  };
};

function usd(cents: number): string {
  return (cents / 100).toFixed(2);
}

/** e_filing_id is numeric text; longer = larger, then lexicographic. */
function compareEFilingId(a: string, b: string): number {
  return a.length === b.length ? a.localeCompare(b) : a.length - b.length;
}

function nextIsoDay(isoDate: string): string {
  const [year, month, day] = isoDate.split("-").map(Number);
  const next = new Date(Date.UTC(year!, month! - 1, day! + 1));
  return next.toISOString().slice(0, 10);
}

type Filing = {
  eFilingId: string;
  reportNum: string;
  rptDate: string | null;
  fromDate: string;
  thruDate: string;
  /** First occurrence per (Form_Type, Line_Item). */
  lines: Map<string, EfileCalSummaryRow>;
};

const CORE_F460_LINES = ["1", "2", "3", "4", "5", "11", "12", "16", "19"] as const;

function sizeBucket(cents: number): string {
  if (cents < 10_000) return "$1-$99";
  if (cents < 25_000) return "$100-$249";
  if (cents < 50_000) return "$250-$499";
  if (cents < 100_000) return "$500-$999";
  if (cents < 500_000) return "$1,000-$4,999";
  return "$5,000+";
}

export function aggregateSanJoseDirectFinance(input: {
  filerId: string;
  /** Concatenated rows from every calendar-year workbook the cycle spans. */
  summary: readonly EfileCalSummaryRow[];
  scheduleA: readonly EfileCalContributionRow[];
  scheduleC: readonly EfileCalContributionRow[];
  scheduleB1: readonly EfileCalLoanRow[];
  maxBreakdownsPerCategory?: number;
}): SanJoseDirectFinanceAggregate {
  const limit = input.maxBreakdownsPerCategory ?? 20;
  if (!Number.isInteger(limit) || limit <= 0)
    throw new Error(`Invalid San José direct breakdown limit: ${limit}`);
  const violations: SanJoseDirectViolation[] = [];

  // --- Group summary rows into filings by e_filing_id. ---
  const summaryRows = input.summary.filter((row) => row.filerId === input.filerId);
  const byFiling = new Map<string, EfileCalSummaryRow[]>();
  for (const row of summaryRows) {
    const rows = byFiling.get(row.eFilingId) ?? [];
    rows.push(row);
    byFiling.set(row.eFilingId, rows);
  }
  const filings: Filing[] = [];
  for (const [eFilingId, rows] of byFiling) {
    const first = rows[0]!;
    const inconsistent = rows.some(
      (row) =>
        row.fromDate !== first.fromDate ||
        row.thruDate !== first.thruDate ||
        row.reportNum !== first.reportNum ||
        row.rptDate !== first.rptDate,
    );
    if (inconsistent || first.fromDate === null || first.thruDate === null) {
      violations.push({
        type: "filing_unusable",
        eFilingId,
        message: inconsistent
          ? `filing ${eFilingId} rows disagree on period/report metadata`
          : `filing ${eFilingId} has no filing period`,
      });
      continue;
    }
    const lines = new Map<string, EfileCalSummaryRow>();
    for (const row of rows) {
      const key = `${row.formType}|${row.lineItem}`;
      if (lines.has(key)) {
        violations.push({
          type: "duplicate_summary_line",
          eFilingId,
          message: `filing ${eFilingId} repeats summary line ${row.formType} ${row.lineItem}; first occurrence used`,
        });
        continue;
      }
      lines.set(key, row);
    }
    filings.push({
      eFilingId,
      reportNum: first.reportNum,
      rptDate: first.rptDate,
      fromDate: first.fromDate,
      thruDate: first.thruDate,
      lines,
    });
  }

  // --- Canonical selection: exactly one filing per period. ---
  // The live export contains same-period duplicates across independent
  // amendment chains; latest Rpt_Date wins (amendment semantics), then
  // highest Report_Num, then highest e_filing_id for determinism.
  const byPeriod = new Map<string, Filing[]>();
  for (const filing of filings) {
    const key = `${filing.fromDate}|${filing.thruDate}`;
    const group = byPeriod.get(key) ?? [];
    group.push(filing);
    byPeriod.set(key, group);
  }
  const canonical: Filing[] = [];
  let duplicateFilingsExcluded = 0;
  for (const group of byPeriod.values()) {
    group.sort(
      (a, b) =>
        (b.rptDate ?? "").localeCompare(a.rptDate ?? "") ||
        b.reportNum.localeCompare(a.reportNum) ||
        compareEFilingId(b.eFilingId, a.eFilingId),
    );
    const winner = group[0]!;
    canonical.push(winner);
    for (const loser of group.slice(1)) {
      duplicateFilingsExcluded += 1;
      violations.push({
        type: "duplicate_period_filings",
        eFilingId: loser.eFilingId,
        message: `filings ${winner.eFilingId} and ${loser.eFilingId} both cover ${winner.fromDate}..${winner.thruDate}; kept ${winner.eFilingId} (latest Rpt_Date), excluded ${loser.eFilingId}`,
      });
    }
  }
  canonical.sort(
    (a, b) => a.fromDate.localeCompare(b.fromDate) || a.thruDate.localeCompare(b.thruDate),
  );

  // --- Line accessors. Missing A|2 / C|2 / B1 blocks are legitimately absent
  // (observed live) and read as 0; a missing core F460 line is a violation.
  const line = (filing: Filing, formType: string, lineItem: string): EfileCalSummaryRow | null =>
    filing.lines.get(`${formType}|${lineItem}`) ?? null;
  for (const filing of canonical) {
    for (const lineItem of CORE_F460_LINES) {
      if (line(filing, "F460", lineItem) === null) {
        violations.push({
          type: "missing_summary_line",
          eFilingId: filing.eFilingId,
          message: `filing ${filing.eFilingId} has no F460 line ${lineItem} row`,
        });
      }
    }
  }
  const amountA = (filing: Filing, formType: string, lineItem: string): number =>
    line(filing, formType, lineItem)?.amountACents ?? 0;
  const sumA = (formType: string, lineItem: string): number =>
    canonical.reduce((sum, filing) => sum + amountA(filing, formType, lineItem), 0);

  // --- Per-filing arithmetic invariants: 3 = 1 + 2 and 5 = 3 + 4, checked on
  // both columns (Amount_B is where Bien Doan's live $20,000 error sits).
  for (const filing of canonical) {
    for (const [column, value] of [
      ["Amount_A", (row: EfileCalSummaryRow | null) => row?.amountACents ?? null],
      ["Amount_B", (row: EfileCalSummaryRow | null) => row?.amountBCents ?? null],
    ] as const) {
      for (const [sumLine, part1, part2] of [
        ["3", "1", "2"],
        ["5", "3", "4"],
      ] as const) {
        const total = value(line(filing, "F460", sumLine));
        const a = value(line(filing, "F460", part1));
        const b = value(line(filing, "F460", part2));
        if (total === null || a === null || b === null) continue;
        if (total !== a + b) {
          violations.push({
            type: "line_arithmetic",
            eFilingId: filing.eFilingId,
            message: `filing ${filing.eFilingId} ${column}: line ${sumLine} is ${usd(total)} but line ${part1} + line ${part2} = ${usd(a + b)}`,
          });
        }
      }
    }
  }

  // --- Committee-level period and cash-chain invariants. ---
  for (let i = 1; i < canonical.length; i += 1) {
    const prev = canonical[i - 1]!;
    const next = canonical[i]!;
    if (next.fromDate <= prev.thruDate) {
      violations.push({
        type: "period_overlap",
        eFilingId: next.eFilingId,
        message: `filing ${next.eFilingId} (${next.fromDate}..${next.thruDate}) overlaps filing ${prev.eFilingId} (${prev.fromDate}..${prev.thruDate})`,
      });
    } else if (next.fromDate !== nextIsoDay(prev.thruDate)) {
      violations.push({
        type: "period_gap",
        eFilingId: next.eFilingId,
        message: `no filing covers the days after ${prev.thruDate} (filing ${prev.eFilingId}) and before ${next.fromDate} (filing ${next.eFilingId})`,
      });
    }
    const prevEnding = line(prev, "F460", "16")?.amountACents ?? null;
    const nextBeginning = line(next, "F460", "12")?.amountACents ?? null;
    if (prevEnding !== null && nextBeginning !== null && prevEnding !== nextBeginning) {
      violations.push({
        type: "cash_chain",
        eFilingId: next.eFilingId,
        message: `filing ${next.eFilingId} begins with cash ${usd(nextBeginning)} but filing ${prev.eFilingId} ended with ${usd(prevEnding)}`,
      });
    }
  }
  const firstFiling = canonical[0] ?? null;
  const firstBeginning = firstFiling ? (line(firstFiling, "F460", "12")?.amountACents ?? null) : null;
  if (firstFiling && firstBeginning !== null && firstBeginning !== 0) {
    // Observed live: Genny Altwer's export history starts 2026-01-01 with
    // $47,353.73 already on hand — her 2025 activity is simply absent from
    // the export. Totals then undercount; sync must disclose via
    // direct_coverage_note, never publish silently.
    violations.push({
      type: "prior_activity_uncovered",
      eFilingId: firstFiling.eFilingId,
      message: `first covered filing ${firstFiling.eFilingId} (${firstFiling.fromDate}..) opens with cash ${usd(firstBeginning)}; earlier activity is not in the export`,
    });
  }

  // --- Transaction sheets, filtered to canonical filings (the Van Le
  // duplicate chains duplicate Schedule A rows too). ---
  const canonicalIds = new Set(canonical.map((filing) => filing.eFilingId));
  const scheduleA = input.scheduleA.filter(
    (row) => row.filerId === input.filerId && canonicalIds.has(row.eFilingId),
  );
  const scheduleC = input.scheduleC.filter(
    (row) => row.filerId === input.filerId && canonicalIds.has(row.eFilingId),
  );
  const scheduleB1 = input.scheduleB1.filter(
    (row) => row.filerId === input.filerId && canonicalIds.has(row.eFilingId),
  );
  const memoRows = [...scheduleA, ...scheduleC].filter((row) => row.memo);
  const nonMemoA = scheduleA.filter((row) => !row.memo);
  const nonMemoC = scheduleC.filter((row) => !row.memo);

  // --- Totals (the proven cycle formulas). ---
  const totalRaisedCents = sumA("F460", "1") + sumA("F460", "4");
  const totalSpentCents = sumA("F460", "11");
  const loansReceivedCents = sumA("B1", "1");
  const unitemizedCents = sumA("A", "2");
  const unitemizedNonmonetaryCents = sumA("C", "2");
  const latest = canonical.length > 0 ? canonical[canonical.length - 1]! : null;
  const cashOnHandCents = latest ? (line(latest, "F460", "16")?.amountACents ?? null) : null;
  const debtsOwedCents = latest ? (line(latest, "F460", "19")?.amountACents ?? null) : null;

  // --- Reconciliation: summary totals vs itemized rows (proven exact on all
  // six live committees; a mismatch means rows and covers disagree). ---
  const sumRows = (rows: readonly EfileCalContributionRow[]): number =>
    rows.reduce((sum, row) => sum + row.amountCents, 0);
  const monetaryFromRows = sumRows(nonMemoA) + unitemizedCents;
  const monetaryFromCovers = sumA("F460", "1");
  if (monetaryFromRows !== monetaryFromCovers) {
    violations.push({
      type: "contribution_reconciliation",
      eFilingId: null,
      message: `Schedule A rows + unitemized = ${usd(monetaryFromRows)} but F460 line 1 totals ${usd(monetaryFromCovers)}`,
    });
  }
  const nonmonetaryFromRows = sumRows(nonMemoC) + unitemizedNonmonetaryCents;
  const nonmonetaryFromCovers = sumA("F460", "4");
  if (nonmonetaryFromRows !== nonmonetaryFromCovers) {
    violations.push({
      type: "contribution_reconciliation",
      eFilingId: null,
      message: `Schedule C rows + unitemized nonmonetary = ${usd(nonmonetaryFromRows)} but F460 line 4 totals ${usd(nonmonetaryFromCovers)}`,
    });
  }
  const loansFromRows = scheduleB1
    .filter((row) => !row.memo)
    .reduce((sum, row) => sum + (row.loanAmt1Cents ?? 0), 0);
  if (loansFromRows !== loansReceivedCents) {
    violations.push({
      type: "loan_cross_check",
      eFilingId: null,
      message: `B1 sheet Loan_Amt1 sums to ${usd(loansFromRows)} but B1 summary line 1 totals ${usd(loansReceivedCents)}`,
    });
  }

  // --- Breakdowns from canonical itemized rows (SF/LA semantics: IND-only
  // occupation/employer; size buckets positive rows only — a refund does not
  // identify its original receipt's size; unitemized stays a separate field).
  const maps = new Map<
    SanJoseDirectBreakdown["categoryType"],
    Map<string, { name: string; cents: number; count: number }>
  >();
  const add = (
    type: SanJoseDirectBreakdown["categoryType"],
    rawName: string | null,
    cents: number,
  ): void => {
    const name = rawName?.trim().replace(/\s+/g, " ");
    if (!name) return;
    const values = maps.get(type) ?? new Map();
    const key = name.toUpperCase();
    const current = values.get(key) ?? { name, cents: 0, count: 0 };
    current.cents += cents;
    current.count += cents > 0 ? 1 : 0;
    values.set(key, current);
    maps.set(type, values);
  };
  for (const row of [...nonMemoA, ...nonMemoC]) {
    if (row.entityCd === "IND") {
      add("occupation", row.contributorOccupation, row.amountCents);
      add("employer", row.contributorEmployer, row.amountCents);
    }
    if (row.amountCents > 0) add("contribution_size", sizeBucket(row.amountCents), row.amountCents);
  }
  const breakdowns: SanJoseDirectBreakdown[] = [];
  for (const type of ["occupation", "employer", "contribution_size"] as const) {
    const categoryLimit = type === "contribution_size" ? Number.POSITIVE_INFINITY : limit;
    breakdowns.push(
      ...[...(maps.get(type)?.values() ?? [])]
        .filter((value) => value.cents > 0)
        .sort((a, b) => b.cents - a.cents || a.name.localeCompare(b.name))
        .slice(0, categoryLimit)
        .map((value) => ({
          categoryType: type,
          categoryName: value.name,
          amountCents: value.cents,
          contributorCount: value.count,
        })),
    );
  }

  const refundRows = [...nonMemoA, ...nonMemoC].filter((row) => row.amountCents < 0);
  return {
    totalRaisedCents,
    totalSpentCents,
    loansReceivedCents,
    cashOnHandCents,
    debtsOwedCents,
    unitemizedCents,
    unitemizedNonmonetaryCents,
    reportedThrough: latest?.thruDate ?? null,
    coverageStart: firstFiling?.fromDate ?? null,
    filings: canonical.map((filing) => ({
      eFilingId: filing.eFilingId,
      reportNum: filing.reportNum,
      fromDate: filing.fromDate,
      thruDate: filing.thruDate,
    })),
    breakdowns,
    violations,
    diagnostics: {
      summaryRows: summaryRows.length,
      filingsSeen: filings.length,
      duplicateFilingsExcluded,
      scheduleARows: nonMemoA.length,
      scheduleCRows: nonMemoC.length,
      refundRows: refundRows.length,
      refundCents: sumRows(refundRows),
      memoRowsExcluded: memoRows.length,
      memoCentsExcluded: sumRows(memoRows),
    },
  };
}
