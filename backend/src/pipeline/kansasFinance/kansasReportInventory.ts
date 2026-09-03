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

import { kansasCfrCycleStartYear, type KansasCfrOffice } from "./kansasFinanceEligibleOffices.js";

export class KansasReportInventoryError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "KansasReportInventoryError";
  }
}

/** Viewer dates ("1/1/2026" on covers, "07/27/2026" on grids) or ISO "2026-01-01" -> YYYY-MM-DD; throws otherwise. */
export function kansasDateToIso(value: string): string {
  const viewer = /^\s*(\d{1,2})\/(\d{1,2})\/(\d{4})\s*$/.exec(value);
  const iso = viewer ? null : /^\s*(\d{4})-(\d{2})-(\d{2})\s*$/.exec(value);
  if (!viewer && !iso) throw new KansasReportInventoryError(`unparseable Kansas date: "${value}"`);
  const [monthText, dayText, yearText] = viewer ? [viewer[1]!, viewer[2]!, viewer[3]!] : [iso![2]!, iso![3]!, iso![1]!];
  const month = Number.parseInt(monthText, 10);
  const day = Number.parseInt(dayText, 10);
  const year = Number.parseInt(yearText, 10);
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
 * The cycle opens at kansasCfrCycleStartYear (a SPECIAL election runs on
 * the short cycle KPDC files it under: the 2026 Senate special archive
 * starts at the 2025 annual, not 2023) unless `cycleStartYear` overrides it.
 * Regular election dates are computed; a special election on other dates
 * needs its own calendar (the 2026 Senate specials share the regular dates).
 */
export function kansasReportingPeriods(
  office: KansasCfrOffice,
  electionYear: number,
  options: { cycleStartYear?: number } = {}
): KansasReportingPeriod[] {
  if (!Number.isSafeInteger(electionYear) || electionYear < 2000 || electionYear > 2100) {
    throw new KansasReportInventoryError(`Invalid Kansas election year: ${electionYear}`);
  }
  const cycleStartYear = options.cycleStartYear ?? kansasCfrCycleStartYear(office, electionYear);
  if (!Number.isSafeInteger(cycleStartYear) || cycleStartYear > electionYear || cycleStartYear < electionYear - 3) {
    throw new KansasReportInventoryError(`Invalid Kansas cycle start year: ${cycleStartYear} for ${electionYear}`);
  }
  const periods: KansasReportingPeriod[] = [];
  for (let year = cycleStartYear; year < electionYear; year += 1) {
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
  /**
   * Grid file date ("07/27/2026") or ISO; an amendment keeps the ORIGINAL
   * file date. Null for a version known only from the KPDC index, which
   * carries no dates (kansasPaperInventory.ts).
   */
  fileDate: string | null;
  /**
   * Grid amendment date ("07/27/2026") or ISO; null on an original. Live
   * (Governor 2026): every version is its own grid row, an amended version
   * keeps the original's file date and carries lblAmendmentDate, and its
   * cover has chkAmended checked. Two amendments can share one day.
   */
  amendmentDate: string | null;
  /** Cover chkAmended (e-file) or the KPDC amend prefix (paper). */
  amended: boolean;
  /** KPDC amend prefix (amend = 1, 2amend = 2, ...): orders date-less versions. Omitted or null when unknown. */
  amendmentOrdinal?: number | null;
  /** Cover chkTermination: the committee closed with this report. */
  termination: boolean;
  channel: "efile" | "paper";
};

/**
 * An Appointment of Treasurer grid row. The grid's "Amendment No." column
 * is blank on the ORIGINAL appointment and numbered on every later change
 * (live Governor 2026: Kelly's in-window appointments are #6 and #7 — a
 * continuing committee; Rogers 8/28/2024 blank then 9/18/2024 #1). Only an
 * original proves when the committee began.
 */
export type KansasAppointmentOfTreasurer = {
  /** Grid file date ("07/29/2026") or ISO. */
  fileDate: string;
  /** Grid lblAmendmentNo text; "" on an original appointment. */
  amendmentNo: string;
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
    fileDate: filing.fileDate === null ? null : kansasDateToIso(filing.fileDate),
    amendmentDate: filing.amendmentDate === null ? null : kansasDateToIso(filing.amendmentDate),
  };
}

/** When a version took effect: its amendment date, else its file date; null for a KPDC index version. */
function effectiveDate(filing: KansasFilingHeader): string | null {
  return filing.amendmentDate ?? filing.fileDate;
}

/** Replacement sequence: an original is 0, an amendment its KPDC ordinal (1 when unknown). */
function versionOrdinal(filing: KansasFilingHeader): number {
  return filing.amended ? (filing.amendmentOrdinal ?? 1) : 0;
}

/**
 * Latest version first. Two dated versions order by effective date, an
 * amendment outranking an original on one day. When either side is undated
 * (a KPDC index version) only the amend prefix orders them, so an undated
 * amendment against any other amendment is a tie — and a tie at the top
 * leaves the chain untrusted (groupVersions).
 */
function compareVersionsDesc(left: KansasFilingHeader, right: KansasFilingHeader): number {
  const leftDate = effectiveDate(left);
  const rightDate = effectiveDate(right);
  if (leftDate !== null && rightDate !== null) {
    return rightDate.localeCompare(leftDate) || Number(right.amended) - Number(left.amended);
  }
  return versionOrdinal(right) - versionOrdinal(left);
}

/** The filename token KPDC files a period's report under: its due month, "202601" for a report due 2026-01-10. */
export function kansasPeriodDueKey(period: Pick<KansasReportingPeriod, "due">): string {
  return period.due.slice(0, 4) + period.due.slice(5, 7);
}

type VersionGroup = {
  periodStart: string;
  periodEnd: string;
  /** Latest version first. */
  versions: KansasFilingHeader[];
  /** The version whose figures count; null when the chain cannot be ordered. */
  canonical: KansasFilingHeader | null;
};

/**
 * Every version filed for one cover period, resolved to a canonical one.
 * A chain is trusted only when at most one version is an unflagged
 * original, that original is the earliest, and the latest version is
 * strictly later than the next (two amendments on one day — live: Ward,
 * 2/9/2023 — cannot be ordered, so nothing is trusted).
 */
function groupVersions(filings: readonly KansasFilingHeader[]): Map<string, VersionGroup> {
  const groups = new Map<string, VersionGroup>();
  for (const filing of filings) {
    const key = `${filing.periodStart}|${filing.periodEnd}`;
    const group = groups.get(key) ?? { periodStart: filing.periodStart, periodEnd: filing.periodEnd, versions: [], canonical: null };
    group.versions.push(filing);
    groups.set(key, group);
  }
  for (const group of groups.values()) {
    group.versions.sort(compareVersionsDesc);
    const [latest, next] = group.versions;
    const originals = group.versions.filter((filing) => !filing.amended);
    const chainOk =
      originals.length <= 1 && (originals.length === 0 || originals[0] === group.versions[group.versions.length - 1]);
    const tie = next !== undefined && compareVersionsDesc(latest!, next) === 0;
    group.canonical = chainOk && !tie ? latest! : null;
  }
  return groups;
}

export function buildKansasReportLedger(input: {
  periods: readonly KansasReportingPeriod[];
  filings: readonly KansasFilingHeader[];
  /** Appointment of Treasurer grid rows seen inside the cycle window. */
  appointmentsOfTreasurer: readonly KansasAppointmentOfTreasurer[];
  /** Affidavit of Exemption grid file dates seen inside the cycle window. */
  affidavitDates: readonly string[];
  lastMinuteWindows: readonly { start: string; end: string }[];
  now: Date;
}): KansasLedger {
  if (Number.isNaN(input.now.getTime())) throw new KansasReportInventoryError("invalid now");
  const today = isoDate(input.now);
  const filings = input.filings.map(normalizeHeader);
  const groups = groupVersions(filings);
  const originalAppointmentDates = input.appointmentsOfTreasurer
    .filter((appointment) => appointment.amendmentNo.trim() === "")
    .map((appointment) => kansasDateToIso(appointment.fileDate))
    .sort();
  const affidavitDates = input.affidavitDates.map(kansasDateToIso).sort();

  // Pre-candidacy periods are not required: a committee whose ORIGINAL
  // Appointment of Treasurer falls inside the cycle window owes nothing for
  // periods that ended before it. Amended appointments (treasurer changes)
  // prove nothing about when the committee began, and a committee that
  // filed a report for a period that ended before its earliest original
  // appointment predates it anyway — either way it is continuing and every
  // period is required.
  const earliestOriginal = originalAppointmentDates[0] ?? null;
  const committeePredatesAppointment =
    earliestOriginal !== null && filings.some((filing) => filing.periodEnd < earliestOriginal);
  const notRequiredBefore = committeePredatesAppointment ? null : earliestOriginal;
  // A termination report closes the committee, so later periods owe nothing
  // — until a new original Appointment of Treasurer or a later filing
  // reopens it (live: Colyer terminated with an amended 2023 annual, then
  // reappointed a treasurer in 2025 and filed again). Only whole periods
  // between the termination and the reopening are `terminated`, and only a
  // CANONICAL version's termination counts — a superseded original's
  // checkbox, or one of two same-day amendments, proves nothing.
  const closures = [...groups.values()]
    .filter((group) => group.canonical?.termination === true)
    .map((group) => {
      const closedAt = group.periodEnd;
      const reopenedAt =
        [
          ...originalAppointmentDates.filter((date) => date > closedAt),
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
    const group = groups.get(`${period.start}|${period.end}`) ?? null;
    const matching = group?.versions ?? [];
    for (const filing of matching) consumed.add(filing);

    let status: KansasPeriodStatus;
    const canonical = group?.canonical ?? null;
    if (group !== null) {
      status = canonical === null ? "ambiguous" : canonical.amended ? "amended" : "report_filed";
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
