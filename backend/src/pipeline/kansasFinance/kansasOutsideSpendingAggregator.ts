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
// (the plan's "unallocated" rule; the Koch GA fixture). It does keep the
// recipes it names, and a candidate any such row names is "partial": the
// filing proves spending on that candidate whose amount is unknown, so
// the leg publishes nothing for them — not the explicit rows alone (an
// understatement) and never an arbitrary split. Only a candidate whose
// every naming row is explicit gets totals (the plan's
// complete_for_explicit_rows).
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
  /**
   * Transcriber-set identity shared by every page of a multi-page filing
   * (KPDC scans one PDF per page); null when the file is a whole filing.
   */
  filingKey: string | null;
  rowIndex: number;
  rowDate: string | null;
  vendorName: string | null;
  /** Target link recipe; null for an unallocated or unresolved row. */
  targetCommitteeId: string | null;
  /** For an unallocated row: the recipes of every candidate it names (normalized). Empty otherwise. */
  namedCommitteeIds: string[];
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
    filing_key,
    row_index,
    row_date::text AS row_date,
    vendor_name,
    target_committee_id,
    named_committee_ids,
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
    const named = row.named_committee_ids;
    if (named !== null && named !== undefined && !Array.isArray(named)) {
      throw new KansasOutsideSpendingError(`${label}: named_committee_ids is not an array`);
    }
    return {
      filerName: String(row.filer_name),
      sourceFileName: String(row.source_file_name),
      sourceUrl: String(row.source_url),
      periodDueKey: String(row.period_due_key),
      statementTotalCents: kansasNumericTextToCents(row.statement_total, `${label} statement_total`),
      filingKey: row.filing_key === null || row.filing_key === undefined ? null : String(row.filing_key),
      rowIndex: Number(row.row_index),
      rowDate: row.row_date === null ? null : String(row.row_date),
      vendorName: row.vendor_name === null ? null : String(row.vendor_name),
      targetCommitteeId: row.target_committee_id === null ? null : String(row.target_committee_id),
      namedCommitteeIds: (named ?? []).map((value: unknown) => normalizeKansasFilerKey(String(value))),
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
      /** Named by spending whose per-candidate amount the filing does not give: nothing publishes. */
      status: "partial_unallocated";
      /** One line per unallocated row naming this candidate. */
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
 * The transcription checksum. For each filer and period, rows group into
 * statements by file and every row of a statement must carry one total.
 * Kansas filers then use "Total this Period" in one of two ways, and a
 * period passes when EITHER reading reconciles (verified live on the 2026
 * cycle):
 *
 *   * cumulative — each statement's total is the filer's period-to-date
 *     figure, so statements ordered by total ascending must see the
 *     running sum of transcribed rows equal every total (Kansas Comeback:
 *     370,443.63 -> 378,943.63 -> 383,943.63);
 *   * per filing — each total covers only its own filing, so the rows
 *     carrying one total must sum to it (American Conservative Fund's
 *     8/13 filing of $2,211.69 beside its 8/20 filing of $309,228.84).
 *
 * A filing that runs over several pages is scanned as one PDF per page and
 * every page repeats the filing's total (the ACF 8/20 filing is five
 * files). Which files are pages of one filing is the transcriber's call,
 * recorded as a shared filing_key from what the pages show (signature date,
 * filing stamp) — never inferred from a matching total, since two separate
 * filings can print the same figure. Rows of one file must agree on their
 * key and rows of one filing on their total. Returns one reason per failing
 * filer period, keyed by filerPeriodKey; a period that fails both readings
 * is quarantined and every candidate it names fails the outside leg closed.
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
    // Filings: a file on its own, or every file sharing a transcriber-set key.
    const filings = new Map<string, { fileNames: string[]; totalCents: number; rowsCents: number }>();
    let inconsistent: string | null = null;
    for (const [fileName, statementRows] of statements) {
      const totalCents = statementRows[0]!.statementTotalCents;
      const filingKey = statementRows[0]!.filingKey;
      if (statementRows.some((row) => row.statementTotalCents !== totalCents)) {
        inconsistent = `${label}: ${fileName} rows disagree on Total this Period`;
        break;
      }
      if (statementRows.some((row) => row.filingKey !== filingKey)) {
        inconsistent = `${label}: ${fileName} rows disagree on filing_key`;
        break;
      }
      const filingId = filingKey === null ? `file:${fileName}` : `key:${filingKey}`;
      const filing = filings.get(filingId) ?? { fileNames: [], totalCents, rowsCents: 0 };
      if (filing.totalCents !== totalCents) {
        inconsistent = `${label}: filing_key ${JSON.stringify(filingKey)} pages disagree on Total this Period`;
        break;
      }
      filing.fileNames.push(fileName);
      filing.rowsCents += statementRows.reduce((sum, row) => sum + row.amountCents, 0);
      filings.set(filingId, filing);
    }
    if (inconsistent !== null) {
      reasons.set(key, inconsistent);
      continue;
    }
    const ordered = [...filings.values()].sort(
      (left, right) => left.totalCents - right.totalCents || left.fileNames[0]!.localeCompare(right.fileNames[0]!)
    );

    let running = 0;
    let cumulative: string | null = null;
    let perFiling: string | null = null;
    for (const filing of ordered) {
      const files = filing.fileNames.join(", ");
      running += filing.rowsCents;
      if (cumulative === null && running !== filing.totalCents) {
        cumulative = `running total ${running} != ${files} Total this Period ${filing.totalCents}`;
      }
      if (perFiling === null && filing.rowsCents !== filing.totalCents) {
        perFiling = `${files} rows ${filing.rowsCents} != Total this Period ${filing.totalCents}`;
      }
    }
    if (cumulative !== null && perFiling !== null) {
      reasons.set(key, `${label}: ${perFiling} (read as one filing) and ${cumulative} (read as cumulative)`);
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
  const shared = input.rows.filter((row) => row.targetCommitteeId === null && row.namedCommitteeIds.includes(target));
  if (own.length === 0 && shared.length === 0) return { status: "none_found" };

  // The checksum first: a quarantined filer period is evidence that cannot
  // be trusted at all; only then does the shape of the trusted rows matter.
  const quarantined = reconcileKansasOutsideStatements(input.rows);
  const reasons = [...new Set([...own, ...shared].map(filerPeriodKey))]
    .filter((key) => quarantined.has(key))
    .map((key) => quarantined.get(key)!);
  if (reasons.length > 0) return { status: "unpublishable", reasons };
  if (shared.length > 0) {
    return {
      status: "partial_unallocated",
      reasons: shared.map(
        (row) =>
          `${row.sourceFileName} row ${row.rowIndex}: ${row.amountCents} cents across ${row.namedCommitteeIds.length} candidates with no per-candidate amount`
      ),
    };
  }

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
