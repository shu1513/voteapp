// Alabama FCPA Phase 0 probe (plan-alabama-finance.md). Validates, against
// live public data only (no database, no cache, no snapshot):
//   1. race-search id scrape + race-row enumeration for several offices,
//   2. bulk-extract catalog/download/parse contracts with quarantine stats,
//   3. the authority contract: race totals == sum of every filed report cover,
//      cent-exact (extracts get a coverage ratio, since the annual files can
//      miss rows the covers contain — observed $150k on Tuberville),
//   4. amendment semantics (fixtures with amended filings must reconcile),
//   5. TLS with the pinned intermediate (every request runs verified).

import { pathToFileURL } from "node:url";

import {
  downloadAlabamaExtract,
  getAlabamaCommitteeFilings,
  getAlabamaExtractCatalog,
  getAlabamaFilingDetailHtml,
  getAlabamaRaceRows,
  getAlabamaRaceSearchIds,
  searchAlabamaPrincipalCampaignCommittees,
  type AlabamaFcpaClientOptions,
  type AlabamaRaceRow,
} from "../pipeline/alabamaFinance/alabamaFcpaClient.js";
import {
  parseAlabamaCashExtract,
  parseAlabamaExpenditureExtract,
  type AlabamaCashRow,
  type AlabamaExpenditureRow,
  type AlabamaQuarantinedRecord,
} from "../pipeline/alabamaFinance/alabamaFcpaCsv.js";
import {
  parseAlabamaFilingDetailCover,
  parseAlabamaWallClockMs,
  reconcileAlabamaCommittee,
  summarizeAlabamaCashRows,
  summarizeAlabamaExpenditureRows,
} from "../pipeline/alabamaFinance/alabamaPhaseZero.js";

const CYCLE_ELECTION_LABEL = "2026 ELECTION CYCLE";
// 2024 included: committees report contributions by transaction date, and
// 2025-registered committees have reported 2024-dated rows (Boyd, $20.00).
const EXTRACT_YEARS = [2024, 2025, 2026] as const;
const OFFICE_NAMES = ["Governor", "Attorney General", "Secretary of State"] as const;
// Fixture committees from the 2026 Governor race: one with amended filings
// (Jones), one large (Tuberville), one small (Boyd).
const FIXTURE_CANDIDATE_KEYS = ["jones", "tuberville", "boyd"] as const;
const MAX_QUARANTINE_RATIO = 0.05;
const MIN_EXTRACT_COVERAGE = 0.95;

const clientOptions: AlabamaFcpaClientOptions = { timeoutMs: 120_000 };

type ExtractBundle = {
  dataType: "Cash Contribution" | "Expenditure";
  year: number;
  lastUpdatedMs: number;
  lastUpdatedRaw: string;
  recordCount: number;
  quarantined: AlabamaQuarantinedRecord[];
};

async function main(): Promise<void> {
  const failures: string[] = [];

  // Gate 1: id scrape + race enumeration.
  const ids = await getAlabamaRaceSearchIds(clientOptions);
  const election = ids.elections.find((option) => option.label === CYCLE_ELECTION_LABEL);
  if (!election) throw new Error(`No election option labeled ${CYCLE_ELECTION_LABEL}`);
  const officeRows = new Map<string, AlabamaRaceRow[]>();
  for (const officeName of OFFICE_NAMES) {
    const office = ids.offices.find((option) => option.label === officeName);
    if (!office) {
      failures.push(`office_id_missing:${officeName}`);
      continue;
    }
    const rows = await getAlabamaRaceRows(
      { electionId: election.id, officeId: office.id },
      clientOptions
    );
    officeRows.set(officeName, rows);
    if (rows.length === 0) failures.push(`race_rows_empty:${officeName}`);
  }

  // Gate 2 setup: extracts.
  const catalog = await getAlabamaExtractCatalog(clientOptions);
  const cashRows: AlabamaCashRow[] = [];
  const expenditureRows: AlabamaExpenditureRow[] = [];
  const bundles: ExtractBundle[] = [];
  for (const dataType of ["Cash Contribution", "Expenditure"] as const) {
    for (const year of EXTRACT_YEARS) {
      const entry = catalog.find((row) => row.DATATYPE === dataType && row.YEAR === year);
      if (!entry) throw new Error(`Extract catalog has no ${dataType} ${year}`);
      const download = await downloadAlabamaExtract(entry.DOWNLOAD, clientOptions);
      const parsed =
        dataType === "Cash Contribution"
          ? parseAlabamaCashExtract(download.csvText)
          : parseAlabamaExpenditureExtract(download.csvText);
      // Plain loop: spreading >100k rows into push() overflows the call stack.
      if (dataType === "Cash Contribution") {
        for (const row of parsed.rows as AlabamaCashRow[]) cashRows.push(row);
      } else {
        for (const row of parsed.rows as AlabamaExpenditureRow[]) expenditureRows.push(row);
      }
      bundles.push({
        dataType,
        year,
        lastUpdatedMs: parseAlabamaWallClockMs(entry.LASTUPDATEDRAW),
        lastUpdatedRaw: entry.LASTUPDATEDRAW,
        recordCount: parsed.recordCount,
        quarantined: parsed.quarantined,
      });
      const ratio = parsed.quarantined.length / Math.max(parsed.recordCount, 1);
      if (ratio > MAX_QUARANTINE_RATIO) {
        failures.push(`quarantine_ratio:${dataType}:${year}:${ratio.toFixed(4)}`);
      }
    }
  }
  // Gates 2-4: fixture reconciliation.
  const governorOffice = ids.offices.find((option) => option.label === "Governor");
  if (!governorOffice) {
    throw new Error("Alabama race search returned no Governor office option");
  }
  const governorRows = officeRows.get("Governor") ?? [];
  // One office-scoped committee search maps internal ids to FCPA committee
  // numbers for every fixture (name criteria are untested portal behavior).
  const governorCommittees = await searchAlabamaPrincipalCampaignCommittees(
    { officeId: governorOffice.id },
    clientOptions
  );
  const fixtures: Array<Record<string, unknown>> = [];
  let amendedFixtureSeen = false;
  for (const key of FIXTURE_CANDIDATE_KEYS) {
    const matches = governorRows.filter((row) => row.CANDIDATE.toLowerCase().includes(key));
    if (matches.length !== 1) {
      failures.push(`fixture_race_row:${key}:${matches.length}`);
      continue;
    }
    const raceRow = matches[0]!;
    if (raceRow.BEGINNINGFUNDS !== 0) {
      // Reconciliation below assumes the committee's whole life fits in the
      // downloaded extract years.
      failures.push(`fixture_beginning_funds:${key}:${raceRow.BEGINNINGFUNDS}`);
      continue;
    }
    const committee = governorCommittees.find((row) => row.id === raceRow.COMMITTEEID);
    if (!committee) {
      failures.push(`fixture_committee_lookup:${key}`);
      continue;
    }
    // The authority gate sums the cover of EVERY filed report, so every cover
    // must parse; a fetch/parse failure fails the fixture rather than skewing
    // the sum silently.
    const filings = await getAlabamaCommitteeFilings(raceRow.COMMITTEEID, clientOptions);
    const covers = [];
    const unparsedFilingIds: number[] = [];
    for (const filing of filings) {
      let parsed = false;
      // The portal intermittently serves a System Exception page; retry before
      // failing the fixture.
      for (let attempt = 0; attempt < 3 && !parsed; attempt += 1) {
        try {
          const html = await getAlabamaFilingDetailHtml(filing.ID, clientOptions);
          covers.push(parseAlabamaFilingDetailCover(html));
          parsed = true;
        } catch {
          await new Promise((resolve) => setTimeout(resolve, 1_000 * (attempt + 1)));
        }
      }
      if (!parsed) unparsedFilingIds.push(filing.ID);
    }
    if (unparsedFilingIds.length > 0) failures.push(`fixture_unparsed_covers:${key}`);
    const cashSummary = summarizeAlabamaCashRows(cashRows, committee.committeeId);
    const expenditureSummary = summarizeAlabamaExpenditureRows(expenditureRows, committee.committeeId);
    const reconciliation = reconcileAlabamaCommittee({
      raceRow,
      cashSummary,
      expenditureSummary,
      covers,
    });
    const amendedFilingCount = filings.filter((filing) => filing.AMENDED === "Yes").length;
    if (cashSummary.amendedRowCount > 0 || amendedFilingCount > 0) amendedFixtureSeen = true;
    for (const component of ["cash", "inKind", "expenditure"] as const) {
      const result = reconciliation[component];
      if (result.authorityStatus === "mismatch") failures.push(`fixture_${component}_authority:${key}`);
      if (result.extractCoverage !== null && result.extractCoverage < MIN_EXTRACT_COVERAGE) {
        failures.push(`fixture_${component}_extract_coverage:${key}:${result.extractCoverage.toFixed(4)}`);
      }
    }
    fixtures.push({
      key,
      candidate: raceRow.CANDIDATE,
      internal_committee_id: raceRow.COMMITTEEID,
      fcpa_committee_id: committee.committeeId,
      filing_count: filings.length,
      amended_filing_count: amendedFilingCount,
      unparsed_filing_ids: unparsedFilingIds,
      cash_summary: cashSummary,
      expenditure_summary: expenditureSummary,
      reconciliation,
    });
  }
  if (!amendedFixtureSeen) failures.push("no_amended_fixture");

  const output = {
    type: "alabama_campaign_finance_phase_zero_probe" as const,
    ts: new Date().toISOString(),
    ok: failures.length === 0,
    failures,
    election: { id: election.id, label: election.label },
    offices: [...officeRows.entries()].map(([name, rows]) => ({ name, raceRowCount: rows.length })),
    extracts: bundles.map((bundle) => ({
      data_type: bundle.dataType,
      year: bundle.year,
      last_updated: bundle.lastUpdatedRaw,
      record_count: bundle.recordCount,
      quarantined_count: bundle.quarantined.length,
      quarantined_reasons: bundle.quarantined.reduce<Record<string, number>>((acc, record) => {
        acc[record.reason] = (acc[record.reason] ?? 0) + 1;
        return acc;
      }, {}),
    })),
    fixtures,
    tls: "pinned_intermediate_verification_on" as const,
    publication: "disabled_phase_zero" as const,
  };
  console.log(JSON.stringify(output, null, 2));
  if (!output.ok) process.exitCode = 1;
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    console.error(
      "Alabama campaign-finance Phase 0 probe failed:",
      error instanceof Error ? error.message : error
    );
    process.exitCode = 1;
  });
}
