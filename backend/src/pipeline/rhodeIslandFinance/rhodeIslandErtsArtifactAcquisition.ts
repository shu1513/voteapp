import {
  discoverErtsOrganization,
  fetchErtsContributionReport,
  fetchErtsExpenditureReport,
  fetchErtsFilingPdf,
  fetchErtsFilingVersions,
  fetchErtsOrganizationFilings,
  fetchErtsTransactionExportCsv,
  requireErtsUsDate,
  traverseErtsCf8Index,
  type ErtsTransport,
} from "./rhodeIslandErtsClient.js";
import {
  getErtsArtifactStatus,
  storeErtsArtifact,
  type ErtsArtifactSourceMetadata,
} from "./rhodeIslandErtsArtifactCache.js";
import {
  ertsUsDateToIso,
  parseErtsCf8FiledDate,
  parseErtsContributionExport,
  parseErtsSummaryGroupings,
  ERTS_CONTRIBUTION_SUMMARY_GRID_ID,
  ERTS_CONTRIBUTION_TYPE_CODES,
  type ErtsFilingRow,
} from "./rhodeIslandErtsParsers.js";

// Acquisition for ERTS portal artifacts (rhode_island_plan.md "Required
// artifacts per sync"). Retrieval only — nothing here writes to the database;
// the finance sync reads the cache only. Per organization, everything is
// fetched first and installed only after every fetch and every integrity gate
// has passed, so a failure mid-organization leaves the previous snapshot
// fully intact (north carolina acquisition discipline).
//
// Integrity gates (decision 3, proven by the PR 3 spike):
//   (a) Export truncation control: the portal renders no exported-row count
//       anywhere, so EVERY summary grouping must be accounted for — cent-
//       exact in the export, or proven summary-only by a typed search whose
//       "No Contributions were found" answer is required. Anything else
//       fails the organization.
//   (b) Filing-list stability: the org's filing list is snapshotted before
//       and after its fetches; if the filed set changed mid-fetch the whole
//       bundle is discarded (re-fetched next run — cheap consistency check,
//       no transactional install machinery).
//   (c) CF-8 index traversal must descend page-over-page by rendered page
//       label and reach the cycle boundary, or nothing from it installs.
//
// Freshness: report pages, exports, filing lists and version lists are
// re-fetched every run — the transaction data is current-ledger state (spike
// result 5) and an already-amended filing's grid row does NOT change when a
// further amendment lands, so the version list is the only re-amendment
// signal. Version PDFs are immutable (a new version gets a new GUID) and are
// the one artifact skipped when already cached.

export type ErtsAcquisitionOrganization = {
  // The canonical numeric Board key the caller expects; discovery must
  // confirm it (identity gate — the resolver owns name→org evidence, this
  // module only verifies it against the portal).
  orgId: string;
  // Search term for the portal's organization search (e.g. "McKee").
  searchLastName: string;
  // Exact organization name to select from the results (e.g. "DANIEL J MCKEE").
  organizationName: string;
};

export type ErtsCycleWindow = {
  // Portal-format US dates spanning the RI election cycle
  // (odd-year Jan 1 .. even-year Dec 31, § 17-25-3 / decision 2).
  beginUs: string;
  endUs: string;
};

export function ertsCycleWindowForYear(cycleYear: number): ErtsCycleWindow {
  if (!Number.isInteger(cycleYear) || cycleYear % 2 !== 0 || cycleYear < 1990 || cycleYear > 2100) {
    throw new Error(`Invalid ERTS cycle year (must be an even election year): ${cycleYear}`);
  }
  return { beginUs: `01/01/${cycleYear - 1}`, endUs: `12/31/${cycleYear}` };
}

// --- Cycle filing selection --------------------------------------------------

export type ErtsCycleFilingSelection = {
  // Filed CF-2 (FormName=RICF2) rows whose period overlaps the cycle window.
  selected: ErtsFilingRow[];
  unfiledRowCount: number;
  nonCf2FiledRowCount: number;
  outOfCycleRowCount: number;
  unusablePeriodRowCount: number;
};

export function selectErtsCycleCf2Filings(input: {
  rows: readonly ErtsFilingRow[];
  cycleBeginIso: string;
  cycleEndIso: string;
}): ErtsCycleFilingSelection {
  const selected: ErtsFilingRow[] = [];
  let unfiledRowCount = 0;
  let nonCf2FiledRowCount = 0;
  let outOfCycleRowCount = 0;
  let unusablePeriodRowCount = 0;
  for (const row of input.rows) {
    if (row.filedAt === "") {
      unfiledRowCount += 1;
      continue;
    }
    // Only RICF2 filings carry the CF-2 summary page the totals mapping
    // reads (decision 2); MPF forms and the rest are out of PR 4's scope.
    if (row.formName !== "RICF2" || row.filingId === null) {
      nonCf2FiledRowCount += 1;
      continue;
    }
    const beginIso = ertsUsDateToIso(row.periodBegin);
    const endIso = ertsUsDateToIso(row.periodEnd);
    if (beginIso === null || endIso === null) {
      unusablePeriodRowCount += 1;
      continue;
    }
    if (beginIso <= input.cycleEndIso && endIso >= input.cycleBeginIso) {
      selected.push(row);
    } else {
      outOfCycleRowCount += 1;
    }
  }
  return { selected, unfiledRowCount, nonCf2FiledRowCount, outOfCycleRowCount, unusablePeriodRowCount };
}

// --- Export truncation control (gate a) --------------------------------------

export type ErtsExportReconciliation = {
  exportRowCount: number;
  centExactTypeCount: number;
  // Summary groupings proven summary-only by a typed "No Contributions were
  // found" answer (e.g. `Other Receipt` — spike result 2).
  confirmedSummaryOnlyLabels: string[];
};

/**
 * Reconcile a period's export against its summary groupings. Fail-closed:
 * a grouping the export does not reproduce cent-exact needs a typed-search
 * proof that the portal holds no itemized rows for it — a summary label
 * outside the pinned search vocabulary cannot be proven and fails the
 * organization rather than weakening the only truncation control (an
 * `NSF Check`-style label here is a live finding to take back to the plan,
 * not a row to guess about).
 */
export async function reconcileErtsContributionExport(input: {
  transport: ErtsTransport;
  orgId: string;
  begin: string;
  end: string;
  summary: ReadonlyMap<string, number>;
  csvText: string;
}): Promise<ErtsExportReconciliation> {
  const rows = parseErtsContributionExport(input.csvText);
  const exportTotals = new Map<string, number>();
  for (const row of rows) {
    exportTotals.set(row.contributionType, (exportTotals.get(row.contributionType) ?? 0) + row.amountCents);
  }

  const failures: string[] = [];
  const confirmedSummaryOnlyLabels: string[] = [];
  let centExactTypeCount = 0;
  for (const [label, cents] of input.summary) {
    const exported = exportTotals.get(label);
    if (exported === cents) {
      centExactTypeCount += 1;
      continue;
    }
    if (exported !== undefined) {
      failures.push(`${label}: export ${exported} cents != summary ${cents} cents`);
      continue;
    }
    const code = ERTS_CONTRIBUTION_TYPE_CODES[label];
    if (code === undefined) {
      failures.push(`${label}: absent from the export and outside the pinned search vocabulary — cannot prove summary-only`);
      continue;
    }
    const typed = await fetchErtsContributionReport(input.transport, {
      orgId: input.orgId,
      begin: input.begin,
      end: input.end,
      contributionTypeCode: code,
    });
    if (typed.classification === "no_rows") {
      confirmedSummaryOnlyLabels.push(label);
    } else {
      failures.push(`${label}: absent from the export but the typed search returned itemized rows — the export dropped them`);
    }
  }
  for (const label of exportTotals.keys()) {
    if (!input.summary.has(label)) {
      failures.push(`${label}: in the export but missing from the summary groupings`);
    }
  }
  if (failures.length > 0) {
    throw new Error(
      `ERTS export reconciliation failed for OrgID ${input.orgId} ${input.begin}-${input.end}: ${failures.join("; ")}`
    );
  }
  return { exportRowCount: rows.length, centExactTypeCount, confirmedSummaryOnlyLabels };
}

// --- Per-organization acquisition --------------------------------------------

export type ErtsPeriodFetchSummary = {
  beginIso: string;
  endIso: string;
  contributionClassification: "rows" | "no_rows";
  expenditureClassification: "rows" | "no_rows";
  exportRowCount: number | null;
  confirmedSummaryOnlyLabels: string[];
};

export type ErtsOrganizationAcquisitionResult = {
  orgId: string;
  organizationName: string;
  filingRowCount: number;
  selectedFilingCount: number;
  unfiledRowCount: number;
  nonCf2FiledRowCount: number;
  outOfCycleRowCount: number;
  unusablePeriodRowCount: number;
  periods: ErtsPeriodFetchSummary[];
  fetchedPdfCount: number;
  skippedPdfCount: number;
};

function filedRowIdentity(row: ErtsFilingRow): string {
  return [row.filingId ?? "", row.reportType, row.periodBegin, row.periodEnd, row.filedAt, row.amended ? "Y" : "N"].join(
    "|"
  );
}

// The filed set — not the unfiled rows, whose due dates the portal may
// re-render — is what must be stable across the bundle (gate b).
function filedRowIdentitySet(rows: readonly ErtsFilingRow[]): Set<string> {
  return new Set(rows.filter((row) => row.filedAt !== "").map(filedRowIdentity));
}

function setsEqual(a: ReadonlySet<string>, b: ReadonlySet<string>): boolean {
  if (a.size !== b.size) return false;
  for (const value of a) if (!b.has(value)) return false;
  return true;
}

const PDF_GUID_PATTERN = /-([0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12})\.pdf$/i;

export function ertsPdfGuidFromUrl(pdfUrl: string): string {
  const guid = PDF_GUID_PATTERN.exec(pdfUrl)?.[1];
  if (!guid) {
    throw new Error(`ERTS filing PDF URL carries no GUID: ${pdfUrl}`);
  }
  return guid.toLowerCase();
}

/**
 * Fetch one organization's cycle artifact bundle and install it only after
 * every fetch and every gate has passed. Any failure throws — the caller
 * isolates it so one bad organization cannot abandon the run.
 */
export async function acquireErtsOrganizationArtifacts(input: {
  transport: ErtsTransport;
  cacheDir: string;
  organization: ErtsAcquisitionOrganization;
  cycle: ErtsCycleWindow;
  force?: boolean;
  retrievedAt?: Date;
  log?: (message: string) => void;
}): Promise<ErtsOrganizationAcquisitionResult> {
  const { transport, cacheDir, organization, cycle } = input;
  const cycleBeginIso = ertsUsDateToIso(requireErtsUsDate(cycle.beginUs, "cycle begin"));
  const cycleEndIso = ertsUsDateToIso(requireErtsUsDate(cycle.endUs, "cycle end"));
  if (cycleBeginIso === null || cycleEndIso === null || cycleBeginIso > cycleEndIso) {
    throw new Error(`Invalid ERTS cycle window: ${cycle.beginUs}-${cycle.endUs}`);
  }

  // --- Fetch phase: nothing is written until everything below succeeds. ---
  const discovery = await discoverErtsOrganization(transport, {
    lastName: organization.searchLastName,
    organizationName: organization.organizationName,
    begin: cycle.beginUs,
    end: cycle.endUs,
  });
  if (discovery.orgId !== organization.orgId) {
    throw new Error(
      `ERTS organization ${JSON.stringify(organization.organizationName)} resolved to OrgID ${discovery.orgId}, ` +
        `expected ${organization.orgId} — refusing to fetch under a mismatched identity`
    );
  }

  const filingsBefore = await fetchErtsOrganizationFilings(transport);
  const selection = selectErtsCycleCf2Filings({ rows: filingsBefore.rows, cycleBeginIso, cycleEndIso });

  // Two filings sharing a period window share report/export artifacts; the
  // period-overlap defect itself is PR 6's quarantine, not an acquisition
  // concern.
  const periodWindows = new Map<string, { beginIso: string; endIso: string; beginUs: string; endUs: string }>();
  for (const row of selection.selected) {
    const beginIso = ertsUsDateToIso(row.periodBegin)!;
    const endIso = ertsUsDateToIso(row.periodEnd)!;
    periodWindows.set(`${beginIso}_${endIso}`, {
      beginIso,
      endIso,
      beginUs: row.periodBegin,
      endUs: row.periodEnd,
    });
  }

  type PeriodFetch = ErtsPeriodFetchSummary & {
    contributionUrl: string;
    contributionHtml: string;
    exportUrl: string | null;
    exportCsv: string | null;
    expenditureUrl: string;
    expenditureHtml: string;
  };
  const periodFetches: PeriodFetch[] = [];
  for (const window of periodWindows.values()) {
    const contribution = await fetchErtsContributionReport(transport, {
      orgId: organization.orgId,
      begin: window.beginUs,
      end: window.endUs,
    });
    let exportUrl: string | null = null;
    let exportCsv: string | null = null;
    let exportRowCount: number | null = null;
    let confirmedSummaryOnlyLabels: string[] = [];
    if (contribution.classification === "rows") {
      const exported = await fetchErtsTransactionExportCsv(transport, {
        reportUrl: contribution.url,
        reportHtml: contribution.html,
      });
      const summary = parseErtsSummaryGroupings(contribution.html, ERTS_CONTRIBUTION_SUMMARY_GRID_ID);
      const reconciliation = await reconcileErtsContributionExport({
        transport,
        orgId: organization.orgId,
        begin: window.beginUs,
        end: window.endUs,
        summary,
        csvText: exported.csv,
      });
      exportUrl = exported.url;
      exportCsv = exported.csv;
      exportRowCount = reconciliation.exportRowCount;
      confirmedSummaryOnlyLabels = reconciliation.confirmedSummaryOnlyLabels;
    }
    const expenditure = await fetchErtsExpenditureReport(transport, {
      orgId: organization.orgId,
      begin: window.beginUs,
      end: window.endUs,
    });
    periodFetches.push({
      beginIso: window.beginIso,
      endIso: window.endIso,
      contributionClassification: contribution.classification,
      expenditureClassification: expenditure.classification,
      exportRowCount,
      confirmedSummaryOnlyLabels,
      contributionUrl: contribution.url,
      contributionHtml: contribution.html,
      exportUrl,
      exportCsv,
      expenditureUrl: expenditure.url,
      expenditureHtml: expenditure.html,
    });
    input.log?.(
      `${organization.orgId} ${window.beginUs}-${window.endUs}: contributions ${contribution.classification}` +
        `${exportRowCount === null ? "" : ` (${exportRowCount} export rows)`}, expenditures ${expenditure.classification}`
    );
  }

  type FilingFetch = {
    row: ErtsFilingRow;
    versionsUrl: string;
    versionsHtml: string;
    latestLabel: string;
    latestFiledAt: string;
    pdfGuid: string;
    pdfUrl: string;
    pdf: Uint8Array | null; // null = already cached (immutable by GUID)
  };
  const filingFetches: FilingFetch[] = [];
  for (const row of selection.selected) {
    const versions = await fetchErtsFilingVersions(transport, {
      filingId: row.filingId as string,
      formName: row.formName as string,
    });
    // `grdAmendments` lists oldest-first (spike-confirmed), so the last row
    // is the in-force version — the totals authority per decision 2.
    const latest = versions.versions[versions.versions.length - 1];
    const pdfGuid = ertsPdfGuidFromUrl(latest.pdfUrl);
    let pdf: Uint8Array | null = null;
    const cached = input.force
      ? null
      : await getErtsArtifactStatus({
          cacheDir,
          key: { type: "filing_pdf", filingId: row.filingId as string, guid: pdfGuid },
        });
    if (!cached || cached.status !== "ready") {
      pdf = (await fetchErtsFilingPdf(transport, latest.pdfUrl)).pdf;
    }
    filingFetches.push({
      row,
      versionsUrl: versions.url,
      versionsHtml: versions.html,
      latestLabel: latest.amendmentLabel,
      latestFiledAt: latest.filedAt,
      pdfGuid,
      pdfUrl: latest.pdfUrl,
      pdf,
    });
  }

  // Gate (b): the authoritative filing set must not have changed mid-fetch.
  const filingsAfter = await fetchErtsOrganizationFilings(transport);
  if (!setsEqual(filedRowIdentitySet(filingsBefore.rows), filedRowIdentitySet(filingsAfter.rows))) {
    throw new Error(
      `ERTS filing list for OrgID ${organization.orgId} changed mid-fetch — discarding the bundle; ` +
        "the next run re-fetches a consistent snapshot"
    );
  }

  // --- Install phase: only local validated writes remain. ---
  const orgSource: ErtsArtifactSourceMetadata = { organizationName: organization.organizationName };
  await storeErtsArtifact({
    cacheDir,
    key: { type: "organization_search", query: organization.searchLastName },
    url: discovery.searchResultsUrl,
    body: discovery.searchResultsHtml,
    source: orgSource,
    retrievedAt: input.retrievedAt,
  });
  // The after-snapshot is the state the stability gate verified twice.
  await storeErtsArtifact({
    cacheDir,
    key: { type: "organization_filings", orgId: organization.orgId },
    url: filingsAfter.url,
    body: filingsAfter.html,
    source: orgSource,
    retrievedAt: input.retrievedAt,
  });
  for (const period of periodFetches) {
    const periodSource: ErtsArtifactSourceMetadata = {
      organizationName: organization.organizationName,
      periodBegin: period.beginIso,
      periodEnd: period.endIso,
    };
    await storeErtsArtifact({
      cacheDir,
      key: { type: "contribution_report", orgId: organization.orgId, beginIso: period.beginIso, endIso: period.endIso },
      url: period.contributionUrl,
      body: period.contributionHtml,
      source: { ...periodSource, classification: period.contributionClassification },
      retrievedAt: input.retrievedAt,
    });
    if (period.exportCsv !== null && period.exportUrl !== null) {
      await storeErtsArtifact({
        cacheDir,
        key: {
          type: "contribution_export",
          orgId: organization.orgId,
          beginIso: period.beginIso,
          endIso: period.endIso,
        },
        url: period.exportUrl,
        body: period.exportCsv,
        source: periodSource,
        retrievedAt: input.retrievedAt,
      });
    }
    await storeErtsArtifact({
      cacheDir,
      key: { type: "expenditure_report", orgId: organization.orgId, beginIso: period.beginIso, endIso: period.endIso },
      url: period.expenditureUrl,
      body: period.expenditureHtml,
      source: { ...periodSource, classification: period.expenditureClassification },
      retrievedAt: input.retrievedAt,
    });
  }
  let fetchedPdfCount = 0;
  let skippedPdfCount = 0;
  for (const filing of filingFetches) {
    const filingSource: ErtsArtifactSourceMetadata = {
      organizationName: organization.organizationName,
      reportType: filing.row.reportType,
      periodBegin: filing.row.periodBegin,
      periodEnd: filing.row.periodEnd,
      filedAt: filing.row.filedAt,
      amended: filing.row.amended,
    };
    await storeErtsArtifact({
      cacheDir,
      key: { type: "filing_versions", filingId: filing.row.filingId as string },
      url: filing.versionsUrl,
      body: filing.versionsHtml,
      source: filingSource,
      retrievedAt: input.retrievedAt,
    });
    if (filing.pdf === null) {
      skippedPdfCount += 1;
      continue;
    }
    await storeErtsArtifact({
      cacheDir,
      key: { type: "filing_pdf", filingId: filing.row.filingId as string, guid: filing.pdfGuid },
      url: filing.pdfUrl,
      body: filing.pdf,
      source: { ...filingSource, amendmentLabel: filing.latestLabel, filedAt: filing.latestFiledAt },
      retrievedAt: input.retrievedAt,
    });
    fetchedPdfCount += 1;
  }

  return {
    orgId: organization.orgId,
    organizationName: organization.organizationName,
    filingRowCount: filingsAfter.rows.length,
    selectedFilingCount: selection.selected.length,
    unfiledRowCount: selection.unfiledRowCount,
    nonCf2FiledRowCount: selection.nonCf2FiledRowCount,
    outOfCycleRowCount: selection.outOfCycleRowCount,
    unusablePeriodRowCount: selection.unusablePeriodRowCount,
    periods: periodFetches.map(
      ({ beginIso, endIso, contributionClassification, expenditureClassification, exportRowCount, confirmedSummaryOnlyLabels }) => ({
        beginIso,
        endIso,
        contributionClassification,
        expenditureClassification,
        exportRowCount,
        confirmedSummaryOnlyLabels,
      })
    ),
    fetchedPdfCount,
    skippedPdfCount,
  };
}

// --- CF-8 index acquisition --------------------------------------------------

export type ErtsCf8AcquisitionResult = {
  pageCount: number;
  rowCount: number;
  cycleRowCount: number;
  independentExpenditureRowCount: number;
  missingScanLinkCount: number;
};

/**
 * Traverse and cache the CF-8 "Other Filings" index (the decision-5 diff
 * source). Gate (c): the traversal must descend and reach the cycle boundary
 * or nothing installs. Every page manifest records the run's page count so
 * the sync reads exactly pages 1..N of one vintage (equal retrievedAt) and a
 * stale higher-numbered page from an older, longer run is never mixed in.
 */
export async function acquireErtsCf8IndexArtifacts(input: {
  transport: ErtsTransport;
  cacheDir: string;
  cycle: ErtsCycleWindow;
  retrievedAt?: Date;
  log?: (message: string) => void;
}): Promise<ErtsCf8AcquisitionResult> {
  const cycleBeginIso = ertsUsDateToIso(requireErtsUsDate(input.cycle.beginUs, "cycle begin"));
  const cycleEndIso = ertsUsDateToIso(requireErtsUsDate(input.cycle.endUs, "cycle end"));
  const cycleStartMs = Date.parse(`${cycleBeginIso}T00:00:00Z`);
  // Inclusive upper bound (spike review fix c): a 2027 run must not count
  // next cycle's filings as this cycle's.
  const cycleEndMs = Date.parse(`${cycleEndIso}T00:00:00Z`);

  const traversal = await traverseErtsCf8Index(input.transport, { cycleStartMs });
  if (!traversal.descending || !traversal.reachedBoundary) {
    throw new Error(
      `ERTS CF-8 index traversal is not trustworthy (dates ${traversal.descending ? "descend" : "DO NOT descend"}, ` +
        `boundary ${traversal.reachedBoundary ? "reached" : "NOT reached"} after ${traversal.pages.length} pages) — ` +
        "nothing cached"
    );
  }

  for (const [index, page] of traversal.pages.entries()) {
    await storeErtsArtifact({
      cacheDir: input.cacheDir,
      key: { type: "cf8_index_page", page: index + 1 },
      url: page.url,
      body: page.html,
      source: { cf8PageCount: traversal.pages.length },
      retrievedAt: input.retrievedAt,
    });
  }

  const cycleRows = traversal.rows.filter((row) => {
    const filed = parseErtsCf8FiledDate(row.filedDate);
    return !Number.isNaN(filed) && filed >= cycleStartMs && filed <= cycleEndMs;
  });
  const result: ErtsCf8AcquisitionResult = {
    pageCount: traversal.pages.length,
    rowCount: traversal.rows.length,
    cycleRowCount: cycleRows.length,
    independentExpenditureRowCount: cycleRows.filter((row) => /INDEPENDENT EXPENDITURE/i.test(row.filingType)).length,
    missingScanLinkCount: cycleRows.filter((row) => row.scannedUrl === null).length,
  };
  input.log?.(
    `CF-8 index: ${result.pageCount} pages, ${result.rowCount} rows, ${result.cycleRowCount} in cycle, ` +
      `${result.independentExpenditureRowCount} independent expenditures`
  );
  return result;
}

// --- Whole-run orchestration -------------------------------------------------

export type RhodeIslandErtsAcquisitionResult = {
  cycle: ErtsCycleWindow;
  cacheDir: string;
  organizations: ErtsOrganizationAcquisitionResult[];
  organizationFailures: Array<{ orgId: string; message: string }>;
  cf8: ErtsCf8AcquisitionResult | null;
  cf8Failure: { message: string } | null;
};

export async function acquireRhodeIslandErtsArtifacts(input: {
  transport: ErtsTransport;
  cacheDir: string;
  cycle: ErtsCycleWindow;
  organizations: readonly ErtsAcquisitionOrganization[];
  includeCf8?: boolean;
  force?: boolean;
  retrievedAt?: Date;
  log?: (message: string) => void;
}): Promise<RhodeIslandErtsAcquisitionResult> {
  const organizations: ErtsOrganizationAcquisitionResult[] = [];
  const organizationFailures: Array<{ orgId: string; message: string }> = [];
  for (const organization of input.organizations) {
    try {
      organizations.push(
        await acquireErtsOrganizationArtifacts({
          transport: input.transport,
          cacheDir: input.cacheDir,
          organization,
          cycle: input.cycle,
          force: input.force,
          retrievedAt: input.retrievedAt,
          log: input.log,
        })
      );
    } catch (error) {
      // One bad organization must not abandon the rest; its cached snapshot,
      // if any, is left untouched.
      organizationFailures.push({ orgId: organization.orgId, message: (error as Error).message });
      input.log?.(`Organization ${organization.orgId}: FAILED — ${(error as Error).message}`);
    }
  }

  let cf8: ErtsCf8AcquisitionResult | null = null;
  let cf8Failure: { message: string } | null = null;
  if (input.includeCf8 ?? true) {
    try {
      cf8 = await acquireErtsCf8IndexArtifacts({
        transport: input.transport,
        cacheDir: input.cacheDir,
        cycle: input.cycle,
        retrievedAt: input.retrievedAt,
        log: input.log,
      });
    } catch (error) {
      // A CF-8 failure must not throw away the organization results — same
      // isolation as a failing organization.
      cf8Failure = { message: (error as Error).message };
      input.log?.(`CF-8 index acquisition: FAILED — ${(error as Error).message}`);
    }
  }

  return {
    cycle: input.cycle,
    cacheDir: input.cacheDir,
    organizations,
    organizationFailures,
    cf8,
    cf8Failure,
  };
}
