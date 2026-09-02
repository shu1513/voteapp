// Kansas per-candidate period ledger (plan-kansas-finance.md, Phase 2).
//
// K.S.A. 25-4148(a) fixes the reporting calendar from the election dates:
// - pre-primary: 1/1 of the election year through 12 days before the
//   primary, due 8 days before the primary;
// - pre-general: 11 days before the primary through 12 days before the
//   general, due 8 days before the general;
// - post-general: 11 days before the general through 12/31, due January 10;
// - every other cycle year ("when the candidate is not participating in a
//   primary or general election"): one annual report for the calendar year,
//   due the next January 10.
// Candidate last-minute reports (11 through 6 days before an election) are
// informational: they duplicate into the next regular report and never
// account for a period.
//
// Pure functions. The sync hands this the filings it already opened (cover
// period + grid file date), plus the Appointment of Treasurer and Affidavit
// of Exemption grid dates, and gets back one status per required period.
// Fail closed: a period is `missing_or_late` only when the calendar says a
// report was due, a filing that matches no period is reported rather than
// dropped, and two unflagged filings for one period are `ambiguous`.

import type { KansasCfrOffice } from "./kansasFinanceEligibleOffices.js";

export class KansasReportInventoryError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "KansasReportInventoryError";
  }
}

/** Viewer dates ("1/1/2026" on covers, "07/27/2026" on grids) -> YYYY-MM-DD; throws otherwise. */
export function kansasDateToIso(value: string): string {
  const match = /^\s*(\d{1,2})\/(\d{1,2})\/(\d{4})\s*$/.exec(value);
  if (!match) throw new KansasReportInventoryError(`unparseable Kansas date: "${value}"`);
  const month = Number.parseInt(match[1]!, 10);
  const day = Number.parseInt(match[2]!, 10);
  const year = Number.parseInt(match[3]!, 10);
  const date = new Date(Date.UTC(year, month - 1, day));
  if (date.getUTCFullYear() !== year || date.getUTCMonth() !== month - 1 || date.getUTCDate() !== day) {
    throw new KansasReportInventoryError(`invalid Kansas date: "${value}"`);
  }
  return isoDate(date);
}

function isoDate(date: Date): string {
  return date.toISOString().slice(0, 10);
}

function addDays(date: Date, days: number): Date {
  return new Date(date.getTime() + days * 86_400_000);
}

/** Kansas primary: first Tuesday in August (K.S.A. 25-203). */
export function kansasPrimaryDate(year: number): Date {
  const first = new Date(Date.UTC(year, 7, 1));
  return addDays(first, (2 - first.getUTCDay() + 7) % 7);
}

/** Kansas general: Tuesday after the first Monday in November. */
export function kansasGeneralDate(year: number): Date {
  const first = new Date(Date.UTC(year, 10, 1));
  const firstMonday = addDays(first, (1 - first.getUTCDay() + 7) % 7);
  return addDays(firstMonday, 1);
}

export type KansasReportingPeriodKind = "annual" | "pre_primary" | "pre_general" | "post_general";

export type KansasReportingPeriod = {
  /** e.g. "2025-annual", "2026-pre_primary". */
  key: string;
  kind: KansasReportingPeriodKind;
  start: string;
  end: string;
  due: string;
};

/**
 * Required periods for an office's cycle ending in `electionYear`: annual
 * reports for the earlier cycle years, then the election year's three.
 * Regular election dates are computed; a special election on other dates
 * needs its own calendar (the 2026 Senate specials share the regular dates).
 */
export function kansasReportingPeriods(office: KansasCfrOffice, electionYear: number): KansasReportingPeriod[] {
  if (!Number.isSafeInteger(electionYear) || electionYear < 2000 || electionYear > 2100) {
    throw new KansasReportInventoryError(`Invalid Kansas election year: ${electionYear}`);
  }
  const periods: KansasReportingPeriod[] = [];
  for (let year = electionYear - office.cycleYearsBefore; year < electionYear; year += 1) {
    periods.push({
      key: `${year}-annual`,
      kind: "annual",
      start: `${year}-01-01`,
      end: `${year}-12-31`,
      due: `${year + 1}-01-10`,
    });
  }
  const primary = kansasPrimaryDate(electionYear);
  const general = kansasGeneralDate(electionYear);
  periods.push(
    {
      key: `${electionYear}-pre_primary`,
      kind: "pre_primary",
      start: `${electionYear}-01-01`,
      end: isoDate(addDays(primary, -12)),
      due: isoDate(addDays(primary, -8)),
    },
    {
      key: `${electionYear}-pre_general`,
      kind: "pre_general",
      start: isoDate(addDays(primary, -11)),
      end: isoDate(addDays(general, -12)),
      due: isoDate(addDays(general, -8)),
    },
    {
      key: `${electionYear}-post_general`,
      kind: "post_general",
      start: isoDate(addDays(general, -11)),
      end: `${electionYear}-12-31`,
      due: `${electionYear + 1}-01-10`,
    }
  );
  return periods;
}

/** Candidate last-minute report windows (11 through 6 days before each election). */
export function kansasLastMinuteWindows(electionYear: number): { start: string; end: string }[] {
  return [kansasPrimaryDate(electionYear), kansasGeneralDate(electionYear)].map((election) => ({
    start: isoDate(addDays(election, -11)),
    end: isoDate(addDays(election, -6)),
  }));
}

export type KansasFilingHeader = {
  /** Cover period as rendered ("1/1/2026") or ISO. */
  periodStart: string;
  periodEnd: string;
  /** Grid file date ("07/27/2026") or ISO. An amendment keeps the ORIGINAL file date. */
  fileDate: string;
  /**
   * Grid amendment date ("07/27/2026") or ISO; null on an original. Live
   * (Governor 2026): every version is its own grid row, an amended version
   * keeps the original's file date and carries lblAmendmentDate, and its
   * cover has chkAmended checked. Two amendments can share one day.
   */
  amendmentDate: string | null;
  /** Cover chkAmended (e-file) or a grid amendment date (paper). */
  amended: boolean;
  /** Cover chkTermination: the committee closed with this report. */
  termination: boolean;
  channel: "efile" | "paper";
};

export type KansasPeriodStatus =
  | "report_filed"
  | "amended"
  | "affidavit_exempt"
  | "not_required"
  | "not_yet_due"
  | "terminated"
  | "missing_or_late"
  | "ambiguous";

export type KansasLedgerEntry = {
  period: KansasReportingPeriod;
  status: KansasPeriodStatus;
  /** Filings whose cover period equals this period, latest version first. */
  filings: KansasFilingHeader[];
  /** The filing whose figures count for the period (the latest amendment, or the only filing). */
  canonical: KansasFilingHeader | null;
};

export type KansasLedger = {
  entries: KansasLedgerEntry[];
  /** Filings inside a last-minute window (informational; never account for a period). */
  lastMinuteFilings: KansasFilingHeader[];
  /** Filings for periods that ended before the cycle's first period (a prior cycle's reports). */
  outOfCycleFilings: KansasFilingHeader[];
  /** Filings whose period matches neither a required period nor a last-minute window. */
  unexpectedFilings: KansasFilingHeader[];
  /** True when every period is accounted for and nothing unexpected was filed. */
  complete: boolean;
};

const ACCOUNTED: ReadonlySet<KansasPeriodStatus> = new Set([
  "report_filed",
  "amended",
  "affidavit_exempt",
  "not_required",
  "not_yet_due",
  "terminated",
]);

function normalizeHeader(filing: KansasFilingHeader): KansasFilingHeader {
  return {
    ...filing,
    periodStart: kansasDateToIso(filing.periodStart),
    periodEnd: kansasDateToIso(filing.periodEnd),
    fileDate: kansasDateToIso(filing.fileDate),
    amendmentDate: filing.amendmentDate === null ? null : kansasDateToIso(filing.amendmentDate),
  };
}

/** When a version took effect: its amendment date, else its file date. */
function effectiveDate(filing: KansasFilingHeader): string {
  return filing.amendmentDate ?? filing.fileDate;
}

/** Latest version first; on one day an amendment outranks an original. */
function compareVersionsDesc(left: KansasFilingHeader, right: KansasFilingHeader): number {
  return effectiveDate(right).localeCompare(effectiveDate(left)) || Number(right.amended) - Number(left.amended);
}

export function buildKansasReportLedger(input: {
  periods: readonly KansasReportingPeriod[];
  filings: readonly KansasFilingHeader[];
  /** Appointment of Treasurer grid file dates seen inside the cycle window. */
  appointmentOfTreasurerDates: readonly string[];
  /** Affidavit of Exemption grid file dates seen inside the cycle window. */
  affidavitDates: readonly string[];
  lastMinuteWindows: readonly { start: string; end: string }[];
  now: Date;
}): KansasLedger {
  if (Number.isNaN(input.now.getTime())) throw new KansasReportInventoryError("invalid now");
  const today = isoDate(input.now);
  const filings = input.filings.map(normalizeHeader);
  const appointmentDates = input.appointmentOfTreasurerDates.map(kansasDateToIso).sort();
  const affidavitDates = input.affidavitDates.map(kansasDateToIso).sort();

  // Pre-candidacy periods are not required: a committee whose first
  // Appointment of Treasurer falls inside the cycle window owes nothing for
  // periods that ended before it. A committee that filed a report for a
  // period that ENDED before that appointment predates it (a treasurer
  // change), so it is continuing and every period is required.
  const earliestAppointment = appointmentDates[0] ?? null;
  const committeePredatesAppointment =
    earliestAppointment !== null && filings.some((filing) => filing.periodEnd < earliestAppointment);
  const notRequiredBefore = committeePredatesAppointment ? null : earliestAppointment;
  // A termination report closes the committee, so later periods owe nothing
  // — until a new Appointment of Treasurer or a later filing reopens it
  // (live: Colyer terminated with an amended 2023 annual, then reappointed
  // a treasurer in 2025 and filed again). Only whole periods between the
  // termination and the reopening are `terminated`.
  const closures = filings
    .filter((filing) => filing.termination)
    .map((filing) => {
      const closedAt = filing.periodEnd;
      const reopenedAt =
        [
          ...appointmentDates.filter((date) => date > closedAt),
          ...filings.filter((other) => other.periodStart > closedAt).map((other) => other.periodStart),
        ].sort()[0] ?? null;
      return { closedAt, reopenedAt };
    });
  const isTerminated = (period: KansasReportingPeriod): boolean =>
    closures.some(
      (closure) => closure.closedAt < period.start && (closure.reopenedAt === null || period.end < closure.reopenedAt)
    );

  const entries: KansasLedgerEntry[] = [];
  const consumed = new Set<KansasFilingHeader>();
  for (const period of input.periods) {
    const matching = filings
      .filter((filing) => filing.periodStart === period.start && filing.periodEnd === period.end)
      .sort(compareVersionsDesc);
    for (const filing of matching) consumed.add(filing);

    let status: KansasPeriodStatus;
    let canonical: KansasFilingHeader | null = null;
    if (matching.length === 1) {
      canonical = matching[0]!;
      status = canonical.amended ? "amended" : "report_filed";
    } else if (matching.length > 1) {
      // Several versions of one period form an amendment chain only when
      // at most one of them is an unflagged original and nothing flagged
      // precedes it; two originals, or an original after an amendment,
      // cannot be ordered and nothing is trusted.
      const originals = matching.filter((filing) => !filing.amended);
      const chainOk = originals.length <= 1 && (originals.length === 0 || originals[0] === matching[matching.length - 1]);
      if (chainOk) {
        canonical = matching[0]!;
        status = "amended";
      } else {
        status = "ambiguous";
      }
    } else if (notRequiredBefore !== null && period.end < notRequiredBefore) {
      status = "not_required";
    } else if (isTerminated(period)) {
      status = "terminated";
    } else if (affidavitDates.some((date) => date <= period.due)) {
      status = "affidavit_exempt";
    } else if (period.due > today) {
      status = "not_yet_due";
    } else {
      status = "missing_or_late";
    }
    entries.push({ period, status, filings: matching, canonical });
  }

  const cycleStart = input.periods.map((period) => period.start).sort()[0] ?? null;
  const lastMinuteFilings: KansasFilingHeader[] = [];
  const outOfCycleFilings: KansasFilingHeader[] = [];
  const unexpectedFilings: KansasFilingHeader[] = [];
  for (const filing of filings) {
    if (consumed.has(filing)) continue;
    const lastMinute = input.lastMinuteWindows.some(
      (window) => filing.periodStart >= window.start && filing.periodEnd <= window.end
    );
    if (lastMinute) lastMinuteFilings.push(filing);
    else if (cycleStart !== null && filing.periodEnd < cycleStart) outOfCycleFilings.push(filing);
    else unexpectedFilings.push(filing);
  }

  return {
    entries,
    lastMinuteFilings,
    outOfCycleFilings,
    unexpectedFilings,
    complete: entries.every((entry) => ACCOUNTED.has(entry.status)) && unexpectedFilings.length === 0,
  };
}
