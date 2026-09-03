// Per-candidate period ledger from the live viewer (plan-kansas-finance.md,
// Phase 2 — ledger wiring). For one linked candidate: re-run the identity
// from the link's search recipe against the office's enumerated filings,
// open every e-filed report's cover for its period, take the Appointment of
// Treasurer and Affidavit of Exemption dates from the same enumeration, and
// hand it all to buildKansasReportLedger.
//
// Identity is the link, not the roster name: a link asserts
// "<officeCode>:<district>:<SURNAME>:<FIRST> is this candidate" and stores
// the filed spelling it was verified against as committee_name, so the
// resolver runs with "SURNAME, FIRST [MIDDLE...]" (kansasLedgerCandidateName)
// under the same fail-closed rules the auto-link used (exact district,
// full-name evidence, contradictions -> ambiguous). Nickname families are
// symmetric, so a recipe built from "BRUNK STEVEN" still folds "BRUNK STEVE".
//
// Paper (scanned) reports carry no period in the viewer (see
// kansasFilingSearch.ts), so they are returned unopened as `paperReports`
// and their periods come from the KPDC candidate trees instead
// (kansasPaperInventory.ts). The candidate is complete only when the tree
// explains at least every viewer paper row; a tree that knows less than the
// viewer leaves a period possibly filed but unassignable.

import {
  kansasDistrictNumberFromGrid,
  normalizeKansasPersonNameForMatching,
  resolveKansasCandidateFiler,
  type KansasFilerMatch,
  type KansasFilerRow,
} from "./kansasCandidateFilerResolver.js";
import { KANSAS_CFR_VIEWER_PAGES } from "./kansasCfrViewerClient.js";
import {
  parseKansasReportCover,
  parseKansasScheduleA,
  parseKansasScheduleB,
  type KansasCfrGridRow,
  type KansasReportCover,
  type KansasScheduleA,
  type KansasScheduleB,
} from "./kansasCfrViewerParsers.js";
import { KANSAS_COUNTED_PERIOD_STATUSES } from "./kansasDirectContributionAggregator.js";
import { kansasCfrCycleStartYear, type KansasCfrOffice } from "./kansasFinanceEligibleOffices.js";
import { normalizeKansasNameForStorage, parseKansasFilerKey } from "./kansasFinanceWriter.js";
import type { KansasFilingPoolLoader, KansasPooledFiling } from "./kansasFilingSearch.js";
import { buildKansasPaperInventory, type KansasKpdcRowLoader, type KansasPaperInventoryResult } from "./kansasPaperInventory.js";
import {
  buildKansasReportLedger,
  kansasFilingHeaderKey,
  kansasLastMinuteWindows,
  kansasReportingPeriods,
  type KansasAppointmentOfTreasurer,
  type KansasFilingHeader,
  type KansasLedger,
} from "./kansasReportInventory.js";

export class KansasCandidateLedgerError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "KansasCandidateLedgerError";
  }
}

export type KansasCandidateLedgerTarget = {
  /** Link committee_id: the viewer search recipe "<officeCode>:<district>:<SURNAME>:<FIRST>". */
  committeeId: string;
  /** Link committee_name: the filed spelling the link was verified against ("HOLLOWAY JOHN A"). */
  committeeName?: string;
  office: KansasCfrOffice;
  electionYear: number;
  /** Overrides kansasCfrCycleStartYear (a special on an unpinned calendar). */
  cycleStartYear?: number;
};

/**
 * The resolver's candidate name for a link. The recipe alone is
 * "SURNAME, FIRST", which would also align "HOLLOWAY JOHN B" for a link the
 * auto-link verified against "HOLLOWAY JOHN A" (roster "John A. Holloway")
 * and then report the pair as ambiguous. The link's committee_name is that
 * verified spelling, so when it starts with the recipe's surname and first
 * name its remaining tokens ride along as middle-name evidence; any other
 * committee_name (operator free text) is ignored and the recipe stands.
 * Comma form so the surname boundary is exact.
 */
export function kansasLedgerCandidateName(recipe: { surname: string; firstName: string }, committeeName?: string): string {
  const filed = normalizeKansasPersonNameForMatching(committeeName ?? "");
  const prefix = `${recipe.surname} ${recipe.firstName}`;
  const rest = filed === prefix || filed.startsWith(`${prefix} `) ? filed.slice(recipe.surname.length + 1) : recipe.firstName;
  return `${recipe.surname}, ${rest}`;
}

export type KansasCandidateReport = {
  row: KansasCfrGridRow;
  cover: KansasReportCover;
  header: KansasFilingHeader;
  /**
   * Schedules A and B, opened for the canonical version of each counted
   * period when `openSchedules` was set; null otherwise. The viewer serves
   * the schedules of the report opened LAST in the session, so each such
   * cover is reopened and its two schedules fetched before the next.
   */
  scheduleA: KansasScheduleA | null;
  scheduleB: KansasScheduleB | null;
};

export type KansasCandidateLedgerResult =
  | {
      status: "unresolved";
      reason:
        | "recipe_office_mismatch"
        | "recipe_district_mismatch"
        | "missing_candidate_name"
        | "no_matching_filer"
        | "conflicting_filed_names"
        | "filings_missing_district";
      filedNames?: string[];
    }
  | {
      status: "resolved";
      match: KansasFilerMatch;
      /** E-filed reports, cover opened; grid order. */
      reports: KansasCandidateReport[];
      /** Paper report rows: period unknown from the viewer, never opened. */
      paperReports: KansasCfrGridRow[];
      /** KPDC tree inventory for the paper rows; null when there are none or no tree loader was given. */
      paper: KansasPaperInventoryResult | null;
      appointments: KansasAppointmentOfTreasurer[];
      affidavitDates: string[];
      ledger: KansasLedger;
      /** Every period accounted for, nothing unexpected, and every paper row explained by the KPDC tree. */
      complete: boolean;
    };

export async function buildKansasCandidateLedger(input: {
  target: KansasCandidateLedgerTarget;
  now: Date;
  loadFilingPool: KansasFilingPoolLoader;
  /** KPDC candidate-tree rows per office + election year; consulted only when the viewer shows paper rows. */
  loadKpdcRows?: KansasKpdcRowLoader;
  /** Also open Schedules A and B of every counted period's canonical e-filed report (the aggregator's rows). */
  openSchedules?: boolean;
}): Promise<KansasCandidateLedgerResult> {
  const { office, electionYear, cycleStartYear } = input.target;
  const recipe = parseKansasFilerKey(input.target.committeeId);
  if (recipe.officeCode !== office.code) {
    return { status: "unresolved", reason: "recipe_office_mismatch" };
  }
  if (office.districted ? recipe.districtNumber === null : recipe.districtNumber !== null) {
    return { status: "unresolved", reason: "recipe_district_mismatch" };
  }

  const pool = await input.loadFilingPool(office, electionYear, cycleStartYear);
  const rows: KansasFilerRow[] = pool.map((filing) => ({
    filedName: filing.row.name,
    district: filing.row.district,
    officeSought: filing.row.officeSought,
    filingKind: filing.filingKind,
    fileDate: filing.row.fileDate,
  }));
  const candidateName = kansasLedgerCandidateName(recipe, input.target.committeeName);
  const resolution = resolveKansasCandidateFiler({ candidateName, districtNumber: recipe.districtNumber, rows });
  if (resolution.status !== "matched") {
    return {
      status: "unresolved",
      reason: resolution.reason,
      ...("filedNames" in resolution ? { filedNames: resolution.filedNames } : {}),
    };
  }

  // The resolver folded every aligned spelling into one person; take exactly
  // those rows, on the recipe's district when the office is districted
  // (statewide rows ignore the district column, as in the resolver).
  const aligned = new Set(resolution.match.filedNames.map(normalizeKansasNameForStorage));
  const own = pool.filter(
    (filing) =>
      aligned.has(normalizeKansasNameForStorage(filing.row.name)) &&
      (recipe.districtNumber === null || kansasDistrictNumberFromGrid(filing.row.district) === recipe.districtNumber)
  );

  const reports: KansasCandidateReport[] = [];
  const reportFilings = new Map<KansasCandidateReport, KansasPooledFiling>();
  const paperReports: KansasCfrGridRow[] = [];
  const appointments: KansasAppointmentOfTreasurer[] = [];
  const affidavitDates: string[] = [];
  for (const filing of own) {
    const { row } = filing;
    if (filing.filingKind === "appointment_of_treasurer") {
      appointments.push({ fileDate: row.fileDate, amendmentNo: row.amendmentNo });
    } else if (filing.filingKind === "affidavit") {
      affidavitDates.push(row.fileDate);
    } else if (row.channel === "paper") {
      paperReports.push(row);
    } else {
      const page = await filing.openReport();
      if (!page.url.endsWith(KANSAS_CFR_VIEWER_PAGES.reportCover)) {
        throw new KansasCandidateLedgerError(
          `report "${row.name}" filed ${row.fileDate} did not open a cover: landed on ${page.url}`
        );
      }
      const cover = parseKansasReportCover(page.html);
      if (cover.periodStart === "" || cover.periodEnd === "") {
        throw new KansasCandidateLedgerError(`report "${row.name}" filed ${row.fileDate}: cover has no period`);
      }
      const report: KansasCandidateReport = {
        row,
        cover,
        header: {
          periodStart: cover.periodStart,
          periodEnd: cover.periodEnd,
          fileDate: row.fileDate,
          amendmentDate: row.amendmentDate === "" ? null : row.amendmentDate,
          amended: cover.amended,
          termination: cover.termination,
          channel: "efile",
        },
        scheduleA: null,
        scheduleB: null,
      };
      reports.push(report);
      reportFilings.set(report, filing);
    }
  }

  const periods = kansasReportingPeriods(office, electionYear, cycleStartYear === undefined ? {} : { cycleStartYear });
  const efileFilings = reports.map((report) => report.header);

  // Paper rows: periods from the KPDC trees of this cycle and the prior one
  // (the prior post-general is filed inside the window), checked against
  // the viewer's row count.
  let paper: KansasPaperInventoryResult | null = null;
  if (paperReports.length > 0 && input.loadKpdcRows !== undefined) {
    const cycleStart = cycleStartYear ?? kansasCfrCycleStartYear(office, electionYear);
    const priorElectionYear = cycleStart - 1;
    const kpdcRows = [
      ...(await input.loadKpdcRows(office, electionYear)),
      ...(await input.loadKpdcRows(office, priorElectionYear)),
    ];
    paper = buildKansasPaperInventory({
      candidateName,
      districtNumber: recipe.districtNumber,
      office,
      periods: [...periods, ...kansasReportingPeriods(office, priorElectionYear)],
      windowStart: `${cycleStart}-01-01`,
      rows: kpdcRows,
      efileFilings,
    });
  }
  // The viewer lists last-minute paper reports as report rows too, so the
  // tree's in-window PLF/GLF scans explain rows alongside its period versions.
  const paperHeaders = paper?.status === "resolved" ? paper.headers : [];
  const paperExplained =
    paperReports.length === 0 ||
    (paper?.status === "resolved" && paper.unmapped.length === 0 && paperReports.length <= paper.headers.length + paper.lastMinute);

  const ledger = buildKansasReportLedger({
    periods,
    filings: [...efileFilings, ...paperHeaders],
    appointmentsOfTreasurer: appointments,
    affidavitDates,
    lastMinuteWindows: kansasLastMinuteWindows(electionYear),
    now: input.now,
  });

  // Schedules only for the versions whose figures count. The session holds
  // ONE open report, so each canonical cover is reopened (the results page's
  // state stays valid for repeated row postbacks) and must show the same
  // period before its schedules are read.
  if (input.openSchedules === true) {
    const canonicalKeys = new Set(
      ledger.entries
        .filter((entry) => entry.canonical !== null && KANSAS_COUNTED_PERIOD_STATUSES.has(entry.status))
        .map((entry) => kansasFilingHeaderKey(entry.canonical!))
    );
    for (const report of reports) {
      if (!canonicalKeys.has(kansasFilingHeaderKey(report.header))) continue;
      const filing = reportFilings.get(report)!;
      const page = await filing.openReport();
      // The whole cover, not just its period: the grid is re-queried on
      // postback, so a filing landing mid-run shifts rows by one, and an
      // amendment's neighbour is its own original — same period, possibly
      // the same totals. Name, office, flags, filing timestamp and every
      // line must all match.
      const reopened = page.url.endsWith(KANSAS_CFR_VIEWER_PAGES.reportCover) ? parseKansasReportCover(page.html) : null;
      if (reopened === null || JSON.stringify(reopened) !== JSON.stringify(report.cover)) {
        throw new KansasCandidateLedgerError(
          `report "${report.row.name}" filed ${report.row.fileDate}: reopening for its schedules did not land on the same cover`
        );
      }
      report.scheduleA = parseKansasScheduleA((await filing.openSchedule("A")).html);
      report.scheduleB = parseKansasScheduleB((await filing.openSchedule("B")).html);
    }
  }

  return {
    status: "resolved",
    match: resolution.match,
    reports,
    paperReports,
    paper,
    appointments,
    affidavitDates,
    ledger,
    complete: ledger.complete && paperExplained,
  };
}
