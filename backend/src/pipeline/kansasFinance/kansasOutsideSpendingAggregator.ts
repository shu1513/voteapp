// Kansas outside spending (plan-kansas-finance.md, Phase 5 — path 1:
// dedicated independent-expenditure statements, K.S.A. 25-4150).
//
// The statements are scanned PDFs, transcribed row by row into
// ks_candidate_finance_outside_rows (migration 271). Each printed row names
// the candidate, the office, the direction the FILER chose (Supported /
// Opposed) and an amount; the statement's "Total this Period" is a
// cumulative control total within one reporting period (verified live:
// Kansas Comeback 370,443.63 -> 378,943.63 -> 383,943.63 inside 1/1-7/23,
// then a reset for 7/24-10/22). That is the transcription's checksum: a
// filer's statements of one period are ordered by their totals and the
// running sum of transcribed rows must equal every one of them, else that
// filer's period is quarantined and every candidate it names fails the
// outside leg closed. A row that names more than one candidate against a
// single amount, or a candidate the transcriber could not resolve, carries
// no target: it counts toward the statement total and toward no candidate
// (the plan's "unallocated" rule; the Koch GA fixture).
//
// A candidate's outside groups are the filers naming it, summed per
// direction; totals are the sums over those groups. A candidate no row
// names has no outside figures ("none found" is not $0). Paths 2 and 3 of
// the plan (committee last-minute IE reports, PAC Schedule C) are not
// built: they only corroborate or duplicate path 1.
//
// Pure aggregation; all arithmetic in integer cents.

import type { Pool, PoolClient } from "pg";

import { normalizeKansasFilerKey, normalizeKansasNameForStorage } from "./kansasFinanceWriter.js";
import { kansasNumericTextToCents } from "./kansasPaperCoverOverrides.js";

type Queryable = Pick<Pool | PoolClient, "query">;

export class KansasOutsideSpendingError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "KansasOutsideSpendingError";
  }
}

export type KansasOutsideRow = {
  /** The spender as the IE index lists it; the outside group's identity. */
  filerName: string;
  sourceFileName: string;
  sourceUrl: string;
  /** The checked "period covered" box, by due month ("202607"). */
  periodDueKey: string;
  /** The statement's printed "Total this Period" (cumulative within the period). */
  statementTotalCents: number;
  rowIndex: number;
  rowDate: string | null;
  vendorName: string | null;
  /** Target link recipe; null for an unallocated or unresolved row. */
  targetCommitteeId: string | null;
  targetAsFiled: string;
  supportOppose: "support" | "oppose" | null;
  amountCents: number;
};

export type KansasOutsideRowLoader = (electionYear: number) => Promise<KansasOutsideRow[]>;

const SELECT_OUTSIDE_ROWS_SQL = `
  SELECT
    filer_name,
    source_file_name,
    source_url,
    period_due_key,
    statement_total::text AS statement_total,
    row_index,
    row_date::text AS row_date,
    vendor_name,
    target_committee_id,
    target_as_filed,
    support_oppose,
    amount::text AS amount
  FROM public.ks_candidate_finance_outside_rows
  WHERE election_year = $1
  ORDER BY filer_name, period_due_key, source_file_name, row_index
`;

/** Every transcribed row of a cycle. Amounts are cast to text in SQL so no driver type parser can round them. */
export async function loadKansasOutsideRows(db: Queryable, electionYear: number): Promise<KansasOutsideRow[]> {
  const result = await db.query(SELECT_OUTSIDE_ROWS_SQL, [electionYear]);
  return result.rows.map((row: Record<string, unknown>) => {
    const label = `${String(row.source_file_name)} row ${String(row.row_index)}`;
    const direction = row.support_oppose;
    if (direction !== null && direction !== "support" && direction !== "oppose") {
      throw new KansasOutsideSpendingError(`${label}: unknown direction ${JSON.stringify(direction)}`);
    }
    return {
      filerName: String(row.filer_name),
      sourceFileName: String(row.source_file_name),
      sourceUrl: String(row.source_url),
      periodDueKey: String(row.period_due_key),
      statementTotalCents: kansasNumericTextToCents(row.statement_total, `${label} statement_total`),
      rowIndex: Number(row.row_index),
      rowDate: row.row_date === null ? null : String(row.row_date),
      vendorName: row.vendor_name === null ? null : String(row.vendor_name),
      targetCommitteeId: row.target_committee_id === null ? null : String(row.target_committee_id),
      targetAsFiled: String(row.target_as_filed),
      supportOppose: direction,
      amountCents: kansasNumericTextToCents(row.amount, `${label} amount`),
    };
  });
}

/** Per-run cache: the rows table is small and every candidate of a batch reads the same cycle. */
export function createKansasOutsideRowLoader(db: Queryable): KansasOutsideRowLoader {
  const years = new Map<number, Promise<KansasOutsideRow[]>>();
  return (electionYear) => {
    let rows = years.get(electionYear);
    if (rows === undefined) {
      rows = loadKansasOutsideRows(db, electionYear);
      years.set(electionYear, rows);
    }
    return rows;
  };
}

export type KansasOutsideGroup = {
  /** "IE:<FILER NAME normalized>" — the outside_groups committee_id. */
  committeeId: string;
  committeeName: string;
  supportOppose: "support" | "oppose";
  amountCents: number;
  sourceUrl: string;
};

export type KansasOutsideSpending =
  | { status: "none_found" }
  | {
      status: "unpublishable";
      /** One line per quarantined filer period naming this candidate. */
      reasons: string[];
    }
  | {
      status: "ok";
      supportCents: number;
      opposeCents: number;
      /** Oppose first then support, dollars desc, name asc. */
      groups: KansasOutsideGroup[];
      /** Statements (files) the candidate's rows came from. */
      statementCount: number;
    };

function filerPeriodKey(row: Pick<KansasOutsideRow, "filerName" | "periodDueKey">): string {
  return `${normalizeKansasNameForStorage(row.filerName)}|${row.periodDueKey}`;
}

/**
 * The transcription checksum. For each filer and period: rows group into
 * statements by file; every row of a statement must carry one total and
 * one period; statements sorted by total ascending must see the running
 * sum of their rows equal each total. Returns one reason per failing
 * filer period, keyed by filerPeriodKey.
 */
export function reconcileKansasOutsideStatements(rows: readonly KansasOutsideRow[]): Map<string, string> {
  const byFilerPeriod = new Map<string, Map<string, KansasOutsideRow[]>>();
  const labels = new Map<string, string>();
  for (const row of rows) {
    const key = filerPeriodKey(row);
    const statements = byFilerPeriod.get(key) ?? new Map<string, KansasOutsideRow[]>();
    statements.set(row.sourceFileName, [...(statements.get(row.sourceFileName) ?? []), row]);
    byFilerPeriod.set(key, statements);
    if (!labels.has(key)) labels.set(key, `${row.filerName.trim()} ${row.periodDueKey}`);
  }

  const reasons = new Map<string, string>();
  for (const [key, statements] of byFilerPeriod) {
    const label = labels.get(key)!;
    const totals: { fileName: string; totalCents: number; rowsCents: number }[] = [];
    let inconsistent: string | null = null;
    for (const [fileName, statementRows] of statements) {
      const totalCents = statementRows[0]!.statementTotalCents;
      if (statementRows.some((row) => row.statementTotalCents !== totalCents)) {
        inconsistent = `${label}: ${fileName} rows disagree on Total this Period`;
        break;
      }
      totals.push({ fileName, totalCents, rowsCents: statementRows.reduce((sum, row) => sum + row.amountCents, 0) });
    }
    if (inconsistent !== null) {
      reasons.set(key, inconsistent);
      continue;
    }
    totals.sort((left, right) => left.totalCents - right.totalCents || left.fileName.localeCompare(right.fileName));
    let running = 0;
    for (const statement of totals) {
      running += statement.rowsCents;
      if (running !== statement.totalCents) {
        reasons.set(
          key,
          `${label}: running total ${running} != ${statement.fileName} Total this Period ${statement.totalCents}`
        );
        break;
      }
    }
  }
  return reasons;
}

export function kansasOutsideGroupCommitteeId(filerName: string): string {
  const normalized = normalizeKansasNameForStorage(filerName);
  if (!normalized) throw new KansasOutsideSpendingError("outside group filer name is blank");
  return `IE:${normalized}`;
}

export function aggregateKansasOutsideSpending(input: {
  /** Every transcribed row of the cycle (the checksum needs a filer's whole period, not just this candidate's rows). */
  rows: readonly KansasOutsideRow[];
  /** The candidate's link recipe. */
  targetCommitteeId: string;
}): KansasOutsideSpending {
  const target = normalizeKansasFilerKey(input.targetCommitteeId);
  const own = input.rows.filter(
    (row) => row.targetCommitteeId !== null && normalizeKansasFilerKey(row.targetCommitteeId) === target
  );
  if (own.length === 0) return { status: "none_found" };

  const quarantined = reconcileKansasOutsideStatements(input.rows);
  const reasons = [...new Set(own.map(filerPeriodKey))]
    .filter((key) => quarantined.has(key))
    .map((key) => quarantined.get(key)!);
  if (reasons.length > 0) return { status: "unpublishable", reasons };

  const groups = new Map<string, KansasOutsideGroup>();
  for (const row of own) {
    if (row.supportOppose === null) continue; // unreachable by the table's CHECK; named so a regression surfaces
    const committeeId = kansasOutsideGroupCommitteeId(row.filerName);
    const key = `${committeeId}|${row.supportOppose}`;
    const group = groups.get(key) ?? {
      committeeId,
      committeeName: row.filerName.trim(),
      supportOppose: row.supportOppose,
      amountCents: 0,
      sourceUrl: row.sourceUrl,
    };
    group.amountCents += row.amountCents;
    groups.set(key, group);
  }
  const sorted = [...groups.values()].sort(
    (left, right) =>
      Number(left.supportOppose === "support") - Number(right.supportOppose === "support") ||
      right.amountCents - left.amountCents ||
      left.committeeName.localeCompare(right.committeeName)
  );
  return {
    status: "ok",
    supportCents: sorted.filter((group) => group.supportOppose === "support").reduce((sum, group) => sum + group.amountCents, 0),
    opposeCents: sorted.filter((group) => group.supportOppose === "oppose").reduce((sum, group) => sum + group.amountCents, 0),
    groups: sorted,
    statementCount: new Set(own.map((row) => row.sourceFileName)).size,
  };
}
