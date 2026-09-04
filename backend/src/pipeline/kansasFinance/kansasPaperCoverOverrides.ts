// Transcribed paper covers (plan-kansas-finance.md, Phase 4 — paper filers).
//
// A paper (scanned) report is a viewer row with no cover to open, so the
// aggregator fails the candidate closed ("no opened cover for the canonical
// paper version"). OCR of the scans stayed below the plan's gate, so the
// cover's seven lines are transcribed by hand into
// ks_candidate_finance_paper_covers (migration 270) and read from here.
//
// Identity is the KPDC filename: the same parse the paper inventory uses
// (kansasPaperFilingHeader) turns "H058AS_202607.pdf" into the date-less
// paper version of the period due 2026-07, so the transcribed cover carries
// the ledger's own header and matches its canonical version by
// kansasFilingHeaderKey — an original never stands in for an amendment. A
// filename that is not a report of one of the cycle's periods is an operator
// error and throws (fail closed for the candidate, not silently ignored).
//
// Totals only: the cover comes without schedules, so the aggregator publishes
// no breakdowns and no direct total for the candidate. No contributor names
// are read or stored (K.S.A. 25-4154(d)).

import type { Pool, PoolClient } from "pg";

import type { KansasReportCover } from "./kansasCfrViewerParsers.js";
import type { KansasOpenedCover } from "./kansasDirectContributionAggregator.js";
import { parseKansasKpdcFileName } from "./kansasKpdcIndexClient.js";
import { kansasPaperFilingHeader } from "./kansasPaperInventory.js";
import { kansasPeriodDueKey, type KansasReportingPeriod } from "./kansasReportInventory.js";

type Queryable = Pick<Pool | PoolClient, "query">;

export class KansasPaperCoverOverrideError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "KansasPaperCoverOverrideError";
  }
}

export type KansasPaperCoverOverride = {
  /** The link's viewer search recipe (ks_candidate_finance_links.committee_id). */
  committeeId: string;
  electionYear: number;
  /** KPDC filename of the scanned version ("H058AS_202607.pdf", "H058AS_amend202607.pdf"). */
  sourceFileName: string;
  sourceUrl: string;
  cashBeginningCents: number;
  totalContributionsCents: number;
  cashAvailableCents: number;
  totalExpendituresCents: number;
  cashCloseCents: number;
  inKindCents: number;
  otherTransactionsCents: number | null;
};

export type KansasPaperCoverLoader = (committeeId: string, electionYear: number) => Promise<KansasPaperCoverOverride[]>;

/** A numeric(16,2) column as text ("1234.50", "-5.00") -> integer cents; anything else throws. */
export function kansasNumericTextToCents(value: unknown, fieldName: string): number {
  const match = typeof value === "string" ? /^(-?)(\d+)\.(\d{2})$/.exec(value) : null;
  if (match === null) {
    throw new KansasPaperCoverOverrideError(`${fieldName}: expected a numeric(16,2) text value, got ${JSON.stringify(value)}`);
  }
  const cents = Number.parseInt(match[2]!, 10) * 100 + Number.parseInt(match[3]!, 10);
  if (!Number.isSafeInteger(cents)) throw new KansasPaperCoverOverrideError(`${fieldName}: ${value} is out of range`);
  return match[1] === "-" ? -cents : cents;
}

const SELECT_PAPER_COVERS_SQL = `
  SELECT
    committee_id,
    election_year,
    source_file_name,
    source_url,
    cash_beginning::text AS cash_beginning,
    total_contributions::text AS total_contributions,
    cash_available::text AS cash_available,
    total_expenditures::text AS total_expenditures,
    cash_close::text AS cash_close,
    in_kind::text AS in_kind,
    other_transactions::text AS other_transactions
  FROM public.ks_candidate_finance_paper_covers
  WHERE committee_id = $1 AND election_year = $2
  ORDER BY source_file_name
`;

/** Every transcribed cover of a link's cycle. Amounts are cast to text in SQL so no driver type parser can round them. */
export async function loadKansasPaperCoverOverrides(
  db: Queryable,
  committeeId: string,
  electionYear: number
): Promise<KansasPaperCoverOverride[]> {
  const result = await db.query(SELECT_PAPER_COVERS_SQL, [committeeId, electionYear]);
  return result.rows.map((row: Record<string, unknown>) => {
    const fileName = String(row.source_file_name);
    const cents = (column: string) => kansasNumericTextToCents(row[column], `${fileName} ${column}`);
    return {
      committeeId: String(row.committee_id),
      electionYear: Number(row.election_year),
      sourceFileName: fileName,
      sourceUrl: String(row.source_url),
      cashBeginningCents: cents("cash_beginning"),
      totalContributionsCents: cents("total_contributions"),
      cashAvailableCents: cents("cash_available"),
      totalExpendituresCents: cents("total_expenditures"),
      cashCloseCents: cents("cash_close"),
      inKindCents: cents("in_kind"),
      otherTransactionsCents: row.other_transactions === null ? null : cents("other_transactions"),
    };
  });
}

/**
 * Transcribed covers as the aggregator's opened covers (no schedules). The
 * header is the paper inventory's for the same filename, so it matches the
 * ledger's canonical paper version and only that. Pure.
 */
export function kansasPaperCoverOverridesToCovers(input: {
  overrides: readonly KansasPaperCoverOverride[];
  /** The cycle's required periods (kansasReportingPeriods); their due months key the filename tokens. */
  periods: readonly KansasReportingPeriod[];
}): KansasOpenedCover[] {
  const periodsByDueKey = new Map(input.periods.map((period) => [kansasPeriodDueKey(period), period]));
  return input.overrides.map((override) => {
    const info = parseKansasKpdcFileName(override.sourceFileName.trim());
    const period =
      (info.kind === "report" || info.kind === "termination") && info.periodKey !== null
        ? periodsByDueKey.get(info.periodKey)
        : undefined;
    if (period === undefined) {
      throw new KansasPaperCoverOverrideError(
        `transcribed cover ${override.sourceFileName}: not a report of a required period (${info.kind}${info.periodKey === null ? "" : ` due ${info.periodKey}`})`
      );
    }
    const header = kansasPaperFilingHeader(info, period);
    // Identity is the filename, so the cover's display fields stay blank.
    const cover: KansasReportCover = {
      candidateName: "",
      officeSought: "",
      district: "",
      periodStart: period.start,
      periodEnd: period.end,
      amended: header.amended,
      termination: header.termination,
      electronicallyFiledOn: null,
      cashBeginningCents: override.cashBeginningCents,
      totalContributionsCents: override.totalContributionsCents,
      cashAvailableCents: override.cashAvailableCents,
      totalExpendituresCents: override.totalExpendituresCents,
      cashCloseCents: override.cashCloseCents,
      inKindCents: override.inKindCents,
      otherTransactionsCents: override.otherTransactionsCents,
    };
    return { header, cover, scheduleA: null, scheduleB: null };
  });
}
