// Arkansas CFIS Phase 0 probe (plan-arkansas-finance.md). Validates the
// acquisition contracts and computes the seven Phase 0 gates. No database, no
// cache promotion, no published snapshot.

import { readFile } from "node:fs/promises";
import { resolve } from "node:path";
import { pathToFileURL } from "node:url";

import {
  downloadArkansasCfisBulkCsvToFile,
  getAllArkansasFiledReports,
  getAllArkansasFilerRegistrations,
  getAllArkansasTransactions,
  getArkansasNextElectionYear,
  getArkansasOfficeLookup,
  ArkansasCfisClientError,
  type ArkansasCfisClientOptions,
  type ArkansasCfisTransactionTypeCode,
  type ArkansasFilerRegistrationRow,
} from "../pipeline/arkansasFinance/arkansasCfisClient.js";
import {
  forEachArkansasExpenditureCsvRow,
  forEachArkansasReceiptCsvRow,
} from "../pipeline/arkansasFinance/arkansasCfisCsv.js";
import {
  createArkansasExpenditureCsvAccumulator,
  createArkansasReceiptCsvAccumulator,
  findArkansasMultiCycleCandidates,
  reconcileArkansasRegistrationTotals,
  summarizeArkansasFiledReports,
  summarizeArkansasOfficeVocabulary,
  summarizeArkansasTransactionRows,
  type ArkansasReceiptCsvSummary,
} from "../pipeline/arkansasFinance/arkansasPhaseZero.js";

export type ArkansasPhaseZeroArgs = {
  filingYears: number[];
  goldEntityIds: number[];
  artifactDir: string;
  reuseArtifacts: boolean;
  dnsFallback: boolean;
  pageSize: number;
  timeoutMs: number;
};

export type ArkansasPhaseZeroClient = {
  getNextElectionYear: typeof getArkansasNextElectionYear;
  getOfficeLookup: typeof getArkansasOfficeLookup;
  getAllFilerRegistrations: typeof getAllArkansasFilerRegistrations;
  getAllTransactions: typeof getAllArkansasTransactions;
  getAllFiledReports: typeof getAllArkansasFiledReports;
  downloadBulkCsvToFile: typeof downloadArkansasCfisBulkCsvToFile;
};

const DEFAULT_ARGS: ArkansasPhaseZeroArgs = {
  filingYears: [2022, 2023, 2024, 2025, 2026],
  // Pinned gold filers (verified 2026-08-26): 1004 = Sanders (Governor,
  // statewide, high volume); 11847 = Wilson (loan-heavy + PAC receipts).
  goldEntityIds: [1004, 11847],
  artifactDir: process.env.ARKANSAS_CFIS_RAW_DATA_CACHE_DIR ?? "scratch/arkansas-campaign-finance/cfis",
  reuseArtifacts: false,
  dnsFallback: false,
  pageSize: 1_000,
  timeoutMs: 900_000,
};

const DEFAULT_CLIENT: ArkansasPhaseZeroClient = {
  getNextElectionYear: getArkansasNextElectionYear,
  getOfficeLookup: getArkansasOfficeLookup,
  getAllFilerRegistrations: getAllArkansasFilerRegistrations,
  getAllTransactions: getAllArkansasTransactions,
  getAllFiledReports: getAllArkansasFiledReports,
  downloadBulkCsvToFile: downloadArkansasCfisBulkCsvToFile,
};

// The Nov-2026 offices VoteApp rosters must all exist in the CFIS office
// vocabulary (gate 7).
const REQUIRED_OFFICES = [
  "Governor",
  "Lieutenant Governor",
  "Attorney General",
  "Secretary Of State",
  "State Treasurer",
  "Auditor Of State",
  "State Land Commissioner",
  "State Senate",
  "State Representative",
  "Supreme Court",
] as const;

function parseFlags(args: readonly string[], names: readonly string[]): Record<string, string | null> {
  const values = Object.fromEntries(names.map((name) => [name, null])) as Record<string, string | null>;
  for (let index = 0; index < args.length; index += 1) {
    const arg = args[index];
    const equalsIndex = arg.indexOf("=");
    const name = equalsIndex >= 0 ? arg.slice(0, equalsIndex) : arg;
    if (!names.includes(name)) throw new Error(`Unknown argument: ${name}`);
    if (values[name] !== null) throw new Error(`Provide ${name} at most once`);
    if (name === "--reuse-artifacts" || name === "--dns-fallback") {
      if (equalsIndex >= 0) throw new Error(`${name} takes no value`);
      values[name] = "true";
      continue;
    }
    if (equalsIndex >= 0) {
      const value = arg.slice(equalsIndex + 1).trim();
      if (!value) throw new Error(`Missing ${name} value`);
      values[name] = value;
    } else {
      const next = args[index + 1];
      if (!next || next.startsWith("--")) throw new Error(`Missing ${name} value`);
      const value = next.trim();
      if (!value) throw new Error(`Missing ${name} value`);
      values[name] = value;
      index += 1;
    }
  }
  return values;
}

function positiveInteger(value: string | null, fallback: number, name: string): number {
  const raw = value ?? String(fallback);
  if (!/^[1-9]\d*$/.test(raw)) throw new Error(`Invalid ${name}: ${raw}`);
  return Number(raw);
}

function integerList(value: string | null, fallback: readonly number[], name: string): number[] {
  if (value === null) return [...fallback];
  const entries = value.split(",").map((entry) => positiveInteger(entry.trim(), 0, name));
  if (new Set(entries).size !== entries.length) throw new Error(`${name} contains duplicates`);
  return entries;
}

export function parseProbeArkansasCampaignFinanceArgs(args: readonly string[]): ArkansasPhaseZeroArgs {
  const names = [
    "--filing-years",
    "--gold-entity-ids",
    "--artifact-dir",
    "--reuse-artifacts",
    "--dns-fallback",
    "--page-size",
    "--timeout-ms",
  ];
  const values = parseFlags(args, names);
  return {
    filingYears: integerList(values["--filing-years"], DEFAULT_ARGS.filingYears, "--filing-years"),
    goldEntityIds: integerList(values["--gold-entity-ids"], DEFAULT_ARGS.goldEntityIds, "--gold-entity-ids"),
    artifactDir: values["--artifact-dir"] ?? DEFAULT_ARGS.artifactDir,
    reuseArtifacts: values["--reuse-artifacts"] === "true",
    dnsFallback: values["--dns-fallback"] === "true",
    pageSize: positiveInteger(values["--page-size"], DEFAULT_ARGS.pageSize, "--page-size"),
    timeoutMs: positiveInteger(values["--timeout-ms"], DEFAULT_ARGS.timeoutMs, "--timeout-ms"),
  };
}

// The DNS defect makes the default resolver return NXDOMAIN on some networks
// (plan gate 1). The fallback resolves the CFIS hostnames through public DNS
// for THIS PROBE ONLY — production fixes the host resolver instead.
async function buildDnsFallbackFetch(): Promise<typeof fetch> {
  const { Agent, fetch: undiciFetch } = await import("undici");
  const { Resolver } = await import("node:dns");
  const resolver = new Resolver();
  resolver.setServers(["8.8.8.8", "1.1.1.1"]);
  const lookup = (
    hostname: string,
    options: { all?: boolean },
    callback: (error: NodeJS.ErrnoException | null, address?: unknown, family?: number) => void
  ): void => {
    resolver.resolve4(hostname, (error, addresses) => {
      if (error || !addresses || addresses.length === 0) {
        callback(error ?? Object.assign(new Error(`No A records for ${hostname}`), { code: "ENOTFOUND" }));
        return;
      }
      if (options.all) {
        callback(
          null,
          addresses.map((address) => ({ address, family: 4 }))
        );
        return;
      }
      callback(null, addresses[0], 4);
    });
  };
  const dispatcher = new Agent({ connect: { lookup: lookup as never } });
  const fetchWithDispatcher = (input: unknown, init?: unknown): Promise<unknown> =>
    undiciFetch(input as never, { ...((init as object) ?? {}), dispatcher } as never);
  return fetchWithDispatcher as unknown as typeof fetch;
}

function artifactPath(artifactDir: string, kind: ArkansasCfisTransactionTypeCode, year: number): string {
  return resolve(artifactDir, `${kind}_${year}.csv`);
}

async function readArtifact(path: string): Promise<string | null> {
  try {
    const content = await readFile(path, "utf8");
    return content.length > 0 ? content : null;
  } catch {
    return null;
  }
}

function latestCandidateRegistration(
  registrations: readonly ArkansasFilerRegistrationRow[],
  filingEntityId: number
): ArkansasFilerRegistrationRow | null {
  const rows = registrations.filter(
    (row) => row.filerEntityId === filingEntityId && row.filerType === "Candidate"
  );
  if (rows.length === 0) return null;
  return rows.reduce((best, row) => ((row.electionYear ?? 0) > (best.electionYear ?? 0) ? row : best));
}

function displayFilerName(registration: ArkansasFilerRegistrationRow): string {
  const name = [registration.lastName, registration.firstName].filter(Boolean).join(", ").trim();
  return name || registration.committeeName || String(registration.filerEntityId);
}

export async function runProbeArkansasCampaignFinance(input: {
  args: ArkansasPhaseZeroArgs;
  client?: ArkansasPhaseZeroClient;
  fetchImplFactory?: () => Promise<typeof fetch>;
  now?: Date;
}) {
  const client = input.client ?? DEFAULT_CLIENT;
  const args = input.args;

  // Gate 1 — access on the default resolver, DNS fallback only on request.
  let defaultResolverOk = false;
  let dnsDefectObserved = false;
  let usedDnsFallback = false;
  let clientOptions: ArkansasCfisClientOptions = { timeoutMs: args.timeoutMs };
  let nextElectionYear: number;
  try {
    nextElectionYear = await client.getNextElectionYear(clientOptions);
    defaultResolverOk = true;
  } catch (error) {
    const isDnsDefect =
      error instanceof ArkansasCfisClientError &&
      error.code === "network_error" &&
      /ENOTFOUND|EAI_AGAIN/i.test(error.message);
    if (!isDnsDefect || !args.dnsFallback) throw error;
    dnsDefectObserved = true;
    usedDnsFallback = true;
    const fetchImpl = await (input.fetchImplFactory ?? buildDnsFallbackFetch)();
    clientOptions = { timeoutMs: args.timeoutMs, fetchImpl };
    nextElectionYear = await client.getNextElectionYear(clientOptions);
  }

  // Registration sweep (gate 5 input + gold resolution).
  const registrations = await client.getAllFilerRegistrations({ pageSize: args.pageSize }, clientOptions);
  const filerTypeCounts: Record<string, number> = {};
  for (const row of registrations) {
    filerTypeCounts[row.filerTypeCode] = (filerTypeCounts[row.filerTypeCode] ?? 0) + 1;
  }
  const multiCycle = findArkansasMultiCycleCandidates(registrations);

  // Bulk artifacts (download or reuse), receipts pass A: global stats + the
  // amended-candidate fixture discovery.
  const artifacts: Array<{
    kind: ArkansasCfisTransactionTypeCode;
    filingYear: number;
    path: string;
    reused: boolean;
    bytes: number;
    sha256: string | null;
  }> = [];
  const loadArtifact = async (
    kind: ArkansasCfisTransactionTypeCode,
    filingYear: number
  ): Promise<string> => {
    const path = artifactPath(args.artifactDir, kind, filingYear);
    if (args.reuseArtifacts) {
      const existing = await readArtifact(path);
      if (existing !== null) {
        artifacts.push({ kind, filingYear, path, reused: true, bytes: Buffer.byteLength(existing), sha256: null });
        return existing;
      }
    }
    const download = await client.downloadBulkCsvToFile(
      { filingYear, transactionTypeCode: kind, outputPath: path },
      clientOptions
    );
    artifacts.push({
      kind,
      filingYear,
      path,
      reused: false,
      bytes: download.bytesWritten,
      sha256: download.sha256,
    });
    const content = await readArtifact(path);
    if (content === null) throw new Error(`Arkansas CFIS artifact unreadable after download: ${path}`);
    return content;
  };

  // Unrepairably mis-quoted source records (see arkansasCfisCsv.ts) are
  // quarantined with row/entity diagnostics — never previews, which would put
  // contributor addresses in output.
  type MalformedRecordDiagnostic = {
    filingYear: number;
    rowNumber: number;
    columnCount: number;
    entityIdPrefix: number | null;
  };
  const malformedReceipts: MalformedRecordDiagnostic[] = [];
  const malformedExpenditures: MalformedRecordDiagnostic[] = [];
  const entityPrefix = (recordPreview: string): number | null => {
    const match = /^(\d+),/.exec(recordPreview);
    return match ? Number(match[1]) : null;
  };

  const discovery = createArkansasReceiptCsvAccumulator(new Set());
  const receiptRowCounts: Record<number, number> = {};
  for (const filingYear of args.filingYears) {
    const csv = await loadArtifact("TCON", filingYear);
    receiptRowCounts[filingYear] = forEachArkansasReceiptCsvRow(
      csv,
      (row) => discovery.add(row),
      (malformed) =>
        malformedReceipts.push({
          filingYear,
          rowNumber: malformed.rowNumber,
          columnCount: malformed.columnCount,
          entityIdPrefix: entityPrefix(malformed.recordPreview),
        })
    );
  }
  const discoverySummary = discovery.result();

  // Extend the gold set: first amended candidate filer, one 2026 local filer,
  // one multi-cycle filer.
  const goldEntityIds = new Set(args.goldEntityIds);
  const amendedFixtureEntityId =
    discoverySummary.candidateEntitiesWithAmendedRows.find((id) => !goldEntityIds.has(id)) ?? null;
  if (amendedFixtureEntityId !== null) goldEntityIds.add(amendedFixtureEntityId);
  const localRegistration =
    registrations.find(
      (row) =>
        row.filerType === "Candidate" &&
        row.electionYear === 2026 &&
        (row.office === "Mayor" || row.office === "City Council Member") &&
        row.totalRaised > 0
    ) ?? null;
  if (localRegistration) goldEntityIds.add(localRegistration.filerEntityId);
  const multiCycleFixture = multiCycle.find((entity) => !goldEntityIds.has(entity.filingEntityId)) ?? null;
  if (multiCycleFixture) goldEntityIds.add(multiCycleFixture.filingEntityId);

  // Pass B: per-entity detail for the extended gold set (receipts +
  // expenditures) from the already-cached artifacts.
  const receipts = createArkansasReceiptCsvAccumulator(goldEntityIds);
  const expenditures = createArkansasExpenditureCsvAccumulator(goldEntityIds);
  for (const filingYear of args.filingYears) {
    const receiptCsv = await readArtifact(artifactPath(args.artifactDir, "TCON", filingYear));
    if (receiptCsv === null) throw new Error(`Arkansas CFIS receipt artifact missing for ${filingYear}`);
    forEachArkansasReceiptCsvRow(
      receiptCsv,
      (row) => receipts.add(row),
      () => {}
    );
    const expenditureCsv = await loadArtifact("TEXP", filingYear);
    forEachArkansasExpenditureCsvRow(
      expenditureCsv,
      (row) => expenditures.add(row),
      (malformed) =>
        malformedExpenditures.push({
          filingYear,
          rowNumber: malformed.rowNumber,
          columnCount: malformed.columnCount,
          entityIdPrefix: entityPrefix(malformed.recordPreview),
        })
    );
  }
  const receiptSummary: ArkansasReceiptCsvSummary = receipts.result();
  const expenditureSummary = expenditures.result();

  // Gates 2/3/4 per gold registration.
  type GoldReport =
    | { filingEntityId: number; status: "no_candidate_registration" }
    | {
        filingEntityId: number;
        status: "ok";
        registration: Record<string, unknown>;
        totals: ReturnType<typeof reconcileArkansasRegistrationTotals>;
        completeness: Record<string, unknown>;
        amendment: Record<string, unknown>;
      };
  const goldReports: GoldReport[] = [];
  for (const filingEntityId of [...goldEntityIds].sort((left, right) => left - right)) {
    const registration = latestCandidateRegistration(registrations, filingEntityId);
    if (!registration) {
      goldReports.push({ filingEntityId, status: "no_candidate_registration" });
      continue;
    }
    const receiptDetail = receiptSummary.entities[String(filingEntityId)];
    const expenditureDetail = expenditureSummary.entities[String(filingEntityId)];
    const totals = reconcileArkansasRegistrationTotals({ registration, receiptDetail, expenditureDetail });

    const apiReceipts = await client.getAllTransactions(
      {
        filerRegistrationGuid: registration.registrationGuid,
        transactionTypeCode: "TCON",
        pageSize: args.pageSize,
      },
      clientOptions
    );
    const apiExpenditures = await client.getAllTransactions(
      {
        filerRegistrationGuid: registration.registrationGuid,
        transactionTypeCode: "TEXP",
        pageSize: args.pageSize,
      },
      clientOptions
    );
    const apiReceiptSummary = summarizeArkansasTransactionRows(registration.registrationGuid, apiReceipts);
    const apiExpenditureSummary = summarizeArkansasTransactionRows(
      registration.registrationGuid,
      apiExpenditures
    );

    const filedReports = await client.getAllFiledReports(
      { filerName: displayFilerName(registration), pageSize: args.pageSize },
      clientOptions
    );
    const lineage = summarizeArkansasFiledReports(registration.registrationGuid, filedReports);

    goldReports.push({
      filingEntityId,
      status: "ok" as const,
      registration: {
        guid: registration.registrationGuid,
        filerName: displayFilerName(registration),
        office: registration.office,
        officeDistrictName: registration.officeDistrictName,
        jurisdictionName: registration.jurisdictionName,
        electionYear: registration.electionYear,
        totalRaised: registration.totalRaised,
        totalSpent: registration.totalSpent,
      },
      totals,
      completeness: {
        csvReceipts: receiptDetail?.total ?? { rowCount: 0, amountCents: 0 },
        apiReceipts: apiReceiptSummary,
        csvExpenditures: expenditureDetail?.total ?? { rowCount: 0, amountCents: 0 },
        apiExpenditures: apiExpenditureSummary,
        // CSV detail is keyed by filing entity; the API rows by registration.
        // A delta on a multi-cycle entity is a gate 5 finding, not gate 3.
        note: "csv_scope_is_entity_api_scope_is_registration",
      },
      amendment: {
        lineage,
        csvByReportAmended: receiptDetail?.byReportAmended ?? {},
      },
    });
  }

  // Gate verdicts (machine-checked where the plan defines them).
  const okGold = goldReports.filter(
    (report): report is Extract<GoldReport, { status: "ok" }> => report.status === "ok"
  );
  const raisedFormulaIntersection = okGold
    .map((report) => new Set(report.totals.raisedExactFormulas))
    .reduce<string[] | null>(
      (common, formulas) =>
        common === null ? [...formulas] : common.filter((formula) => formulas.has(formula)),
      null
    );
  const spentFormulaIntersection = okGold
    .map((report) => new Set(report.totals.spentExactFormulas))
    .reduce<string[] | null>(
      (common, formulas) =>
        common === null ? [...formulas] : common.filter((formula) => formulas.has(formula)),
      null
    );

  const offices = await client.getOfficeLookup(clientOptions);
  const officeVocabulary = summarizeArkansasOfficeVocabulary(offices, REQUIRED_OFFICES);

  return {
    type: "arkansas_campaign_finance_phase_zero_probe" as const,
    ts: (input.now ?? new Date()).toISOString(),
    ok: true,
    gate1_access: {
      defaultResolverOk,
      dnsDefectObserved,
      usedDnsFallback,
      nextElectionYear,
    },
    registrations: {
      totalCount: registrations.length,
      filerTypeCounts,
    },
    artifacts,
    receiptRowCounts,
    malformed_records: {
      receipts: {
        count: malformedReceipts.length,
        goldEntityRows: malformedReceipts.filter(
          (entry) => entry.entityIdPrefix !== null && goldEntityIds.has(entry.entityIdPrefix)
        ),
        sample: malformedReceipts.slice(0, 10),
      },
      expenditures: {
        count: malformedExpenditures.length,
        goldEntityRows: malformedExpenditures.filter(
          (entry) => entry.entityIdPrefix !== null && goldEntityIds.has(entry.entityIdPrefix)
        ),
        sample: malformedExpenditures.slice(0, 10),
      },
    },
    gate2_totals: {
      raisedExactFormulaIntersection: raisedFormulaIntersection ?? [],
      spentExactFormulaIntersection: spentFormulaIntersection ?? [],
    },
    gold: goldReports,
    gate4_amendment: {
      csvAmendedRowCount: discoverySummary.amendedRowCount,
      candidateEntitiesWithAmendedRows: discoverySummary.candidateEntitiesWithAmendedRows,
      selectedFixtureEntityId: amendedFixtureEntityId,
    },
    gate5_multi_cycle: {
      sample: multiCycle,
      selectedFixtureEntityId: multiCycleFixture?.filingEntityId ?? null,
    },
    gate6_occupation: receiptSummary.occupation,
    gate7_offices: officeVocabulary,
    receipts_global: {
      rowCount: receiptSummary.rowCount,
      unparseableAmountRowCount: receiptSummary.unparseableAmountRowCount,
      filerTypeCounts: receiptSummary.filerTypeCounts,
      transactionTypeCounts: receiptSummary.transactionTypeCounts,
      electionTypeCounts: receiptSummary.electionTypeCounts,
    },
    expenditures_global: {
      rowCount: expenditureSummary.rowCount,
      unparseableAmountRowCount: expenditureSummary.unparseableAmountRowCount,
      filerTypeCounts: expenditureSummary.filerTypeCounts,
      transactionTypeCounts: expenditureSummary.transactionTypeCounts,
    },
    independent_expenditure_scan: expenditureSummary.independentExpenditureFilers,
    publication: "disabled_phase_zero" as const,
  };
}

async function main(): Promise<void> {
  const output = await runProbeArkansasCampaignFinance({
    args: parseProbeArkansasCampaignFinanceArgs(process.argv.slice(2)),
  });
  console.log(JSON.stringify(output, null, 2));
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    console.error(
      "Arkansas campaign-finance Phase 0 probe failed:",
      error instanceof Error ? error.message : error
    );
    process.exitCode = 1;
  });
}
