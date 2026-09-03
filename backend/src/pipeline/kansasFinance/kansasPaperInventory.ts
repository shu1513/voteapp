// Paper (scanned) report inventory from the KPDC link trees
// (plan-kansas-finance.md, Phase 2 — paper inventory).
//
// The viewer lists a paper filer's reports with file dates but no period
// (kansasFilingSearch.ts); the KPDC candidate tree lists the same filer's
// scans as <code>_<dueYYYYMM>.pdf with no date. So the period inventory of a
// paper filer comes from the tree, and the viewer's rows are the check.
// Live 2026-09-02 (House): the two sources miss filings in BOTH directions
// — Stiens HD39's July 2026 scans (202607, PLF) are in the tree and absent
// from the viewer; Muter HD2's 1/13/2026 paper row is in the viewer while
// the tree shows "N/A" for 202601. A tree link is a scanned filing, so it
// counts; a viewer row the tree cannot explain means a period may be filed
// but unassignable, so the candidate stays incomplete (kansasCandidateLedger).
//
// The tree lists e-filers too (Helwig HD1 e-files; his 202601/202607 scans
// are in the tree), so for a filer with both channels the versions already
// opened as e-file covers are subtracted by (due month, amended) before the
// remainder is taken as paper.
//
// Rows of the PRIOR cycle's tree are read as well: the prior post-general
// report (due January 10 of the cycle's first year) is filed inside the
// enumeration window, so the viewer shows it and it must be explained;
// only versions due on or after the window start are taken from either tree.

import {
  kansasDistrictNumberFromGrid,
  resolveKansasCandidateFiler,
  type KansasFilerResolution,
  type KansasFilerRow,
} from "./kansasCandidateFilerResolver.js";
import type { KansasCfrOffice } from "./kansasFinanceEligibleOffices.js";
import { normalizeKansasNameForStorage } from "./kansasFinanceWriter.js";
import {
  fetchKansasKpdcIndexPage,
  kansasKpdcCandidateTreePath,
  kansasKpdcStatewideFilerPrefix,
  parseKansasKpdcCandidateRows,
  parseKansasKpdcFileName,
  type KansasKpdcCandidateRow,
  type KansasKpdcFetchOptions,
} from "./kansasKpdcIndexClient.js";
import { kansasDateToIso, kansasPeriodDueKey, type KansasFilingHeader, type KansasReportingPeriod } from "./kansasReportInventory.js";

export type KansasKpdcRowLoader = (office: KansasCfrOffice, electionYear: number) => Promise<KansasKpdcCandidateRow[]>;

/** Per-run cache: one tree fetch per office family + election year. */
export function createKansasKpdcRowLoader(
  input: { fetchOptions?: KansasKpdcFetchOptions; onOrphanLinks?: (treePath: string, count: number) => void } = {}
): KansasKpdcRowLoader {
  const trees = new Map<string, Promise<KansasKpdcCandidateRow[]>>();
  return (office, electionYear) => {
    const treePath = kansasKpdcCandidateTreePath(office, electionYear);
    let tree = trees.get(treePath);
    if (tree === undefined) {
      tree = (async () => {
        const page = await fetchKansasKpdcIndexPage(treePath, input.fetchOptions);
        const parsed = parseKansasKpdcCandidateRows(page.html, page.url);
        if (parsed.orphanLinks > 0) input.onOrphanLinks?.(treePath, parsed.orphanLinks);
        return parsed.rows;
      })();
      trees.set(treePath, tree);
    }
    return tree;
  };
}

export type KansasPaperInventoryResult =
  | {
      status: "unresolved";
      reason: Exclude<KansasFilerResolution, { status: "matched" }>["reason"];
      filedNames?: string[];
    }
  | {
      status: "resolved";
      /** Tree spellings that aligned with the candidate. */
      filedNames: string[];
      /** Date-less paper versions due inside the window, after the e-file subtraction. */
      headers: KansasFilingHeader[];
      /** Tree versions matched to an e-filed cover and left out. */
      explainedByEfile: number;
      /**
       * Last-minute (PLF/GLF) scans of election years inside the window:
       * informational, never a period, but each is a viewer paper row the
       * tree explains (kansasCandidateLedger).
       */
      lastMinute: number;
      /** Appointment and affidavit scans: the viewer's grids are their source. */
      skipped: number;
      /** Filenames the grammar or the calendar could not place; any of these keeps the candidate incomplete. */
      unmapped: string[];
    };

export function buildKansasPaperInventory(input: {
  /** The ledger's candidate name (kansasLedgerCandidateName): "SURNAME, FIRST [MIDDLE...]". */
  candidateName: string;
  /** Recipe district; null for statewide, whose rows carry no district. */
  districtNumber: number | null;
  office: KansasCfrOffice;
  /** Required periods of the cycle and of the prior cycle; their due months key the filename tokens. */
  periods: readonly KansasReportingPeriod[];
  /** Enumeration window start (ISO); only versions due on or after it are the viewer's rows. */
  windowStart: string;
  /** Rows of the cycle's tree and the prior cycle's tree. */
  rows: readonly KansasKpdcCandidateRow[];
  /** Headers of the e-filed covers already opened for this filer. */
  efileFilings: readonly KansasFilingHeader[];
}): KansasPaperInventoryResult {
  // Statewide trees hold all five offices; a row is the office's only when
  // every one of its links carries the office's SW0n prefix.
  const prefix = kansasKpdcStatewideFilerPrefix(input.office);
  const rows = input.rows.filter(
    (row) => prefix === null || (row.links.length > 0 && row.links.every((link) => link.fileName.startsWith(`${prefix}`)))
  );
  const resolverRows: KansasFilerRow[] = rows.map((row) => ({
    filedName: row.filedName,
    district: row.district === null ? "" : String(row.district),
    officeSought: input.office.label,
    filingKind: "report",
    fileDate: "",
  }));
  const resolution = resolveKansasCandidateFiler({
    candidateName: input.candidateName,
    districtNumber: input.districtNumber,
    rows: resolverRows,
  });
  if (resolution.status !== "matched") {
    return {
      status: "unresolved",
      reason: resolution.reason,
      ...("filedNames" in resolution ? { filedNames: resolution.filedNames } : {}),
    };
  }
  const aligned = new Set(resolution.match.filedNames.map(normalizeKansasNameForStorage));
  const own = rows.filter(
    (row) =>
      aligned.has(normalizeKansasNameForStorage(row.filedName)) &&
      (input.districtNumber === null || kansasDistrictNumberFromGrid(row.district === null ? "" : String(row.district)) === input.districtNumber)
  );

  const periodsByDueKey = new Map(input.periods.map((period) => [kansasPeriodDueKey(period), period]));
  const periodsByStart = new Map(input.periods.map((period) => [`${period.start}|${period.end}`, period]));
  const headers: KansasFilingHeader[] = [];
  let lastMinute = 0;
  let skipped = 0;
  const unmapped: string[] = [];
  const seen = new Set<string>();
  for (const row of own) {
    for (const link of row.links) {
      if (seen.has(link.url)) continue; // a filer duplicated across merged rows lists the same scans twice
      seen.add(link.url);
      const info = parseKansasKpdcFileName(link.fileName);
      if (info.kind === "appointment_of_treasurer" || info.kind === "affidavit") {
        skipped += 1;
      } else if (info.kind === "last_minute") {
        // "2026PLF": the election year is the token; the prior tree's
        // last-minute scans predate the window like its early reports do.
        if (`${/(\d{4})[PG]LF$/.exec(info.suffix)![1]!}-01-01` >= input.windowStart) lastMinute += 1;
      } else if (info.kind === "unknown" || info.periodKey === null) {
        unmapped.push(link.fileName);
      } else {
        const period = periodsByDueKey.get(info.periodKey);
        if (period === undefined) {
          unmapped.push(link.fileName);
        } else if (period.due >= input.windowStart) {
          headers.push({
            periodStart: period.start,
            periodEnd: period.end,
            fileDate: null,
            amendmentDate: null,
            amended: info.amendment,
            amendmentOrdinal: info.amendmentOrdinal,
            termination: info.kind === "termination",
            channel: "paper",
          });
        }
      }
    }
  }

  // Subtract the versions the viewer already served as e-file covers, one
  // tree version per cover with the same due month and amended flag.
  const efileKeys = new Map<string, number>();
  for (const filing of input.efileFilings) {
    const period = periodsByStart.get(`${kansasDateToIso(filing.periodStart)}|${kansasDateToIso(filing.periodEnd)}`);
    if (period === undefined) continue;
    const key = `${kansasPeriodDueKey(period)}|${filing.amended}`;
    efileKeys.set(key, (efileKeys.get(key) ?? 0) + 1);
  }
  let explainedByEfile = 0;
  const paperHeaders = headers.filter((header) => {
    const period = periodsByStart.get(`${header.periodStart}|${header.periodEnd}`)!;
    const key = `${kansasPeriodDueKey(period)}|${header.amended}`;
    const remaining = efileKeys.get(key) ?? 0;
    if (remaining === 0) return true;
    efileKeys.set(key, remaining - 1);
    explainedByEfile += 1;
    return false;
  });

  return {
    status: "resolved",
    filedNames: resolution.match.filedNames,
    headers: paperHeaders,
    explainedByEfile,
    lastMinute,
    skipped,
    unmapped,
  };
}
