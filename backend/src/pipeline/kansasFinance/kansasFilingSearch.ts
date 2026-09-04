// Kansas viewer Candidate-filings enumeration (plan-kansas-finance.md),
// shared by the Phase 3 auto-link and the Phase 2 per-candidate ledger: one
// search per filing type over the office's cycle window, every grid page,
// each row keeping a handle that opens its e-filed HTML report from the
// results page it was rendered on.
//
// Facts verified live 2026-09-02:
// - A results page's hidden state stays valid for row postbacks after other
//   reports were opened, across pages and in any order (Governor, 4 pages,
//   opened in the order 1-3-2-1-4), so ONE enumeration per office serves
//   every linked candidate of that office.
// - A PAPER row's name link answers 500 (there is no HTML report). Its pdf
//   link's postback answers 200 with a window.open(...) to a scan under
//   sos.ks.gov/srvimages/campaignfinance/filings/cyYYYY/cmMM/<id>.pdf whose
//   path carries the FILING month, not the period, and whose bytes differ
//   from the KPDC CFAScanned artifact of the same filing (Muter HD2: 383 KB
//   vs 1,057 KB). Paper rows are therefore never opened here; their period
//   must come from the KPDC index (not wired yet).

import {
  collectKansasCfrGridPages,
  createKansasCfrSession,
  getKansasReportSchedule,
  isKansasGridCountMismatch,
  KANSAS_CFR_VIEWER_PAGES,
  KansasCfrClientError,
  openKansasCfrCategory,
  postAndFollow,
  postbackAndFollow,
  type KansasCfrPage,
  type KansasCfrSessionOptions,
} from "./kansasCfrViewerClient.js";
import type { KansasCfrGridRow } from "./kansasCfrViewerParsers.js";
import type { KansasFilerFilingKind } from "./kansasCandidateFilerResolver.js";
import { kansasCfrFiledDateWindow, type KansasCfrOffice } from "./kansasFinanceEligibleOffices.js";

/**
 * The three Candidate-filings searches. The search requires a filing type
 * (blank -> "Filing Type Required" re-render), and each type answers in its
 * own grid (pinned live 2026-09-01).
 */
export const KANSAS_CFR_FILER_SEARCHES: readonly {
  filingType: string;
  gridId: string;
  filingKind: KansasFilerFilingKind;
}[] = [
  { filingType: "Receipts and Expenditures Report", gridId: "grdviewCfrResults", filingKind: "report" },
  { filingType: "Appointment of Treasurer", gridId: "grdviewApptOfTreas", filingKind: "appointment_of_treasurer" },
  { filingType: "Affidavit of Exemption Candidate", gridId: "grdviewAffidavitResults", filingKind: "affidavit" },
];

/**
 * Grid office text vs the searched office: the reports grid renders "State
 * Representative", the appointment/affidavit grids "STATE REPRESENTATIVE".
 */
export function kansasGridOfficeMatches(office: KansasCfrOffice, officeSought: string): boolean {
  const normalize = (value: string) => value.toUpperCase().replace(/\s+/g, " ").trim();
  return normalize(officeSought) === normalize(office.label);
}

export type KansasSearchedFiling = {
  row: KansasCfrGridRow;
  /**
   * Open the row's e-filed HTML report; lands on reports/exp_report_main.aspx
   * in the search's own session. Throws for a paper row or a row without a
   * postback link — neither has an HTML report.
   */
  openReport: () => Promise<KansasCfrPage>;
  /**
   * GET a schedule of the report this row's session opened LAST: the viewer
   * keeps the open report in session state, so call it right after this
   * row's openReport() and before any other row's. Throws for a paper row.
   */
  openSchedule: (schedule: "A" | "B" | "C" | "D") => Promise<KansasCfrPage>;
};

export type KansasFilingSearchInput = {
  office: KansasCfrOffice;
  filingType: string;
  gridId: string;
  startDate: string;
  endDate: string;
  sessionOptions?: KansasCfrSessionOptions;
};

export type KansasFilingSearch = (input: KansasFilingSearchInput) => Promise<KansasSearchedFiling[]>;

/**
 * One viewer session, one Candidate-filings search, every grid page. Fails
 * closed when the form re-renders instead of redirecting to the results page
 * (a validation message such as "Filing Type Required").
 */
export const searchKansasFilings: KansasFilingSearch = async (input) => {
  const session = createKansasCfrSession(input.sessionOptions);
  const form = await openKansasCfrCategory(session, "Candidate");
  const results = await postAndFollow(session, form, {
    txtFirstName: "",
    txtLastName: "",
    drpdownOffice: input.office.code,
    txtDistrictNo: "",
    drpdownFilingType: input.filingType,
    txtStartDate: input.startDate,
    txtEndDate: input.endDate,
    btnSearch: "Submit Search",
  });
  if (!results.url.endsWith(KANSAS_CFR_VIEWER_PAGES.searchResults)) {
    const message = /<span[^>]*style="[^"]*color:\s*Red[^"]*"[^>]*>([^<]*)</i.exec(results.html)?.[1]?.trim();
    throw new KansasCfrClientError(
      "bad_response",
      `Kansas candidate search for ${input.office.label} / ${input.filingType} did not reach results: ${message ?? results.url}`
    );
  }
  const { pages } = await collectKansasCfrGridPages(session, results, input.gridId);
  return pages.flatMap((page) =>
    page.rows.map((row) => {
      const noReport = () =>
        Promise.reject(
          new KansasCfrClientError(
            "invalid_request",
            `no HTML report for ${row.channel} filing "${row.name}" filed ${row.fileDate} (${input.filingType})`
          )
        );
      const isEfile = row.channel === "efile" && row.postbackTarget !== null;
      return {
        row,
        openReport: () => (isEfile ? postbackAndFollow(session, page.page, row.postbackTarget!) : noReport()),
        openSchedule: (schedule: "A" | "B" | "C" | "D") => (isEfile ? getKansasReportSchedule(session, schedule) : noReport()),
      };
    })
  );
};

export type KansasPooledFiling = KansasSearchedFiling & { filingKind: KansasFilerFilingKind };

export type KansasFilingPoolLoader = (
  office: KansasCfrOffice,
  electionYear: number,
  /** A special election's short cycle (the 2026 Senate specials start at 2025). */
  cycleStartYear?: number
) => Promise<KansasPooledFiling[]>;

/**
 * Per-run cache: one enumeration (three filing-type searches) per office +
 * cycle. Rows whose office text disagrees with the searched office are
 * dropped (fail closed against a search that ignored its office filter) and
 * counted through `onSkippedRows`.
 */
export function createKansasFilingPoolLoader(input: {
  now: Date;
  sessionOptions?: KansasCfrSessionOptions;
  search?: KansasFilingSearch;
  onSkippedRows?: (office: KansasCfrOffice, skipped: number) => void;
  /** An enumeration is being rerun after a mid-walk record-count mismatch (isKansasGridCountMismatch). */
  onEnumerationRetry?: (office: KansasCfrOffice, filingType: string) => void;
}): KansasFilingPoolLoader {
  const search = input.search ?? searchKansasFilings;
  const pools = new Map<string, Promise<KansasPooledFiling[]>>();
  return (office, electionYear, cycleStartYear) => {
    const key = `${office.code}|${electionYear}|${cycleStartYear ?? ""}`;
    let pool = pools.get(key);
    if (pool === undefined) {
      pool = (async () => {
        const window = kansasCfrFiledDateWindow({ office, electionYear, cycleStartYear, now: input.now });
        const filings: KansasPooledFiling[] = [];
        let skipped = 0;
        // Sequential on purpose: one request in flight per viewer session.
        for (const filerSearch of KANSAS_CFR_FILER_SEARCHES) {
          const request: KansasFilingSearchInput = {
            office,
            filingType: filerSearch.filingType,
            gridId: filerSearch.gridId,
            startDate: window.startDate,
            endDate: window.endDate,
            sessionOptions: input.sessionOptions,
          };
          let found: KansasSearchedFiling[];
          try {
            found = await search(request);
          } catch (error) {
            // A filing landing mid-walk fails the count once; the rerun is
            // clean. Anything else stays a failure, and it stays cached so a
            // viewer outage fails every candidate fast instead of re-walking.
            if (!isKansasGridCountMismatch(error)) throw error;
            input.onEnumerationRetry?.(office, filerSearch.filingType);
            found = await search(request);
          }
          for (const filing of found) {
            if (!kansasGridOfficeMatches(office, filing.row.officeSought)) {
              skipped += 1;
              continue;
            }
            filings.push({ ...filing, filingKind: filerSearch.filingKind });
          }
        }
        if (skipped > 0) input.onSkippedRows?.(office, skipped);
        return filings;
      })();
      pools.set(key, pool);
    }
    return pool;
  };
}
