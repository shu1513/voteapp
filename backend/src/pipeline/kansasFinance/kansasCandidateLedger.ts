// Per-candidate period ledger from the live viewer (plan-kansas-finance.md,
// Phase 2 — ledger wiring). For one linked candidate: re-run the identity
// from the link's search recipe against the office's enumerated filings,
// open every e-filed report's cover for its period, take the Appointment of
// Treasurer and Affidavit of Exemption dates from the same enumeration, and
// hand it all to buildKansasReportLedger.
//
// Identity is the recipe, not the roster name: an operator's manual link
// asserts "<officeCode>:<district>:<SURNAME>:<FIRST> is this candidate", so
// the resolver runs with "FIRST SURNAME" and the same fail-closed rules the
// auto-link used (exact district, full-name evidence, contradictions ->
// ambiguous). Nickname families are symmetric, so a recipe built from
// "BRUNK STEVEN" still folds "BRUNK STEVE".
//
// Paper (scanned) reports carry no period in the viewer (see
// kansasFilingSearch.ts), so they are returned unopened as `paperReports`
// and keep the candidate incomplete until the KPDC index is wired.

import {
  kansasDistrictNumberFromGrid,
  resolveKansasCandidateFiler,
  type KansasFilerMatch,
  type KansasFilerRow,
} from "./kansasCandidateFilerResolver.js";
import { KANSAS_CFR_VIEWER_PAGES } from "./kansasCfrViewerClient.js";
import { parseKansasReportCover, type KansasCfrGridRow, type KansasReportCover } from "./kansasCfrViewerParsers.js";
import type { KansasCfrOffice } from "./kansasFinanceEligibleOffices.js";
import { normalizeKansasNameForStorage, parseKansasFilerKey } from "./kansasFinanceWriter.js";
import type { KansasFilingPoolLoader } from "./kansasFilingSearch.js";
import {
  buildKansasReportLedger,
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
  office: KansasCfrOffice;
  electionYear: number;
  /** A special election's short cycle; defaults to the office's term length. */
  cycleStartYear?: number;
};

export type KansasCandidateReport = {
  row: KansasCfrGridRow;
  cover: KansasReportCover;
  header: KansasFilingHeader;
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
      appointments: KansasAppointmentOfTreasurer[];
      affidavitDates: string[];
      ledger: KansasLedger;
      /** Every period accounted for, nothing unexpected, and no paper report awaiting the KPDC index. */
      complete: boolean;
    };

export async function buildKansasCandidateLedger(input: {
  target: KansasCandidateLedgerTarget;
  now: Date;
  loadFilingPool: KansasFilingPoolLoader;
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
  const resolution = resolveKansasCandidateFiler({
    candidateName: `${recipe.firstName} ${recipe.surname}`,
    districtNumber: recipe.districtNumber,
    rows,
  });
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
      reports.push({
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
      });
    }
  }

  const ledger = buildKansasReportLedger({
    periods: kansasReportingPeriods(office, electionYear, cycleStartYear === undefined ? {} : { cycleStartYear }),
    filings: reports.map((report) => report.header),
    appointmentsOfTreasurer: appointments,
    affidavitDates,
    lastMinuteWindows: kansasLastMinuteWindows(electionYear),
    now: input.now,
  });
  return {
    status: "resolved",
    match: resolution.match,
    reports,
    paperReports,
    appointments,
    affidavitDates,
    ledger,
    complete: ledger.complete && paperReports.length === 0,
  };
}
