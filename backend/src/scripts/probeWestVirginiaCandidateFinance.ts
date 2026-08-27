// Phase 0 only: validates the WV CFRS acquisition contracts (bulk CSVs,
// transaction/committee/document APIs, presigned downloads) and produces the
// evidence for the amendment/money-model/occupation/outside gates. No
// database, cache, scheduler, or published snapshot.

import { readFile } from "node:fs/promises";
import { join } from "node:path";
import { pathToFileURL } from "node:url";

import {
  WEST_VIRGINIA_ORG_TYPE_CODES,
  downloadWestVirginiaPresignedFile,
  getAllWestVirginiaCommittees,
  getAllWestVirginiaOrgDocuments,
  getAllWestVirginiaTransactions,
  getWestVirginiaDataDownloadCatalog,
  getWestVirginiaDataDownloadFileUrl,
  getWestVirginiaDocumentDownloadUrl,
  getWestVirginiaTlsFallbackUseCount,
  type WestVirginiaCfrsClientOptions,
  type WestVirginiaCommitteeRow,
  type WestVirginiaTransactionRow,
} from "../pipeline/westVirginiaFinance/westVirginiaCfrsClient.js";
import {
  decodeWestVirginiaCsvBytes,
  parseWestVirginiaContributionCsv,
  parseWestVirginiaExpenditureCsv,
  parseWestVirginiaRegistrationCsv,
  parseWestVirginiaReportingScheduleCsv,
  type WestVirginiaContributionCsvRow,
} from "../pipeline/westVirginiaFinance/westVirginiaCfrsCsv.js";
import {
  checkWestVirginiaRegistryJoin,
  isWestVirginiaIndependentExpenditureDocument,
  pdfHasFontMarker,
  reconcileWestVirginiaCommittee,
  summarizeWestVirginiaContributionCsv,
  summarizeWestVirginiaOccupations,
  summarizeWestVirginiaOutsideInventory,
  summarizeWestVirginiaReportingCycles,
} from "../pipeline/westVirginiaFinance/westVirginiaPhaseZero.js";

export type WestVirginiaPhaseZeroArgs = {
  years: number[];
  scratchDir: string;
  pageSize: number;
  timeoutMs: number;
  reconcileEntityIds: string[];
  outsideCommitteeLimit: number;
};

const DEFAULT_ARGS: WestVirginiaPhaseZeroArgs = {
  years: [2025, 2026],
  scratchDir: "scratch/west-virginia-campaign-finance/phase0",
  pageSize: 2_000,
  timeoutMs: 120_000,
  // Committee to Elect Dean Jeffries — the embedded-quote CSV fixture
  // (verified live 2026-08-27). Additional committees are auto-picked.
  reconcileEntityIds: ["1010003610"],
  outsideCommitteeLimit: 40,
};

// The category vocabulary the contributions bulk file carries (verified live);
// API rows outside this set (loan subtypes etc.) are excluded from the
// CSV-vs-API comparison and surfaced separately.
const CONTRIBUTION_FILE_CATEGORIES = new Set([
  "Monetary",
  "In-Kind",
  "Other Income",
  "Receipt of Transfer of Excess Funds",
  "Return",
]);

const COURTESY_DELAY_MS = 150;

function sleep(ms: number): Promise<void> {
  return new Promise((resolvePromise) => setTimeout(resolvePromise, ms));
}

function parseFlags(args: readonly string[], names: readonly string[]): Record<string, string | null> {
  const values = Object.fromEntries(names.map((name) => [name, null])) as Record<string, string | null>;
  for (let index = 0; index < args.length; index += 1) {
    const arg = args[index];
    const equalsIndex = arg.indexOf("=");
    const name = equalsIndex >= 0 ? arg.slice(0, equalsIndex) : arg;
    if (!names.includes(name)) throw new Error(`Unknown argument: ${name}`);
    if (values[name] !== null) throw new Error(`Provide ${name} at most once`);
    if (equalsIndex >= 0) {
      const value = arg.slice(equalsIndex + 1).trim();
      if (!value) throw new Error(`Missing ${name} value`);
      values[name] = value;
    } else {
      const next = args[index + 1];
      if (!next || next.startsWith("--")) throw new Error(`Missing ${name} value`);
      values[name] = next.trim();
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

export function parseProbeWestVirginiaCandidateFinanceArgs(
  args: readonly string[]
): WestVirginiaPhaseZeroArgs {
  const names = [
    "--years",
    "--scratch-dir",
    "--page-size",
    "--timeout-ms",
    "--reconcile-entity-ids",
    "--outside-committee-limit",
  ];
  const values = parseFlags(args, names);
  const years =
    values["--years"] === null
      ? [...DEFAULT_ARGS.years]
      : values["--years"].split(",").map((year) => positiveInteger(year.trim(), 0, "--years"));
  if (years.length === 0 || new Set(years).size !== years.length) {
    throw new Error("--years must be a non-empty comma list of distinct years");
  }
  const reconcileEntityIds =
    values["--reconcile-entity-ids"] === null
      ? [...DEFAULT_ARGS.reconcileEntityIds]
      : values["--reconcile-entity-ids"].split(",").map((id) => {
          const trimmed = id.trim();
          if (!/^\d{10}$/.test(trimmed)) throw new Error(`Invalid --reconcile-entity-ids entry: ${id}`);
          return trimmed;
        });
  return {
    years: years.sort((a, b) => a - b),
    scratchDir: values["--scratch-dir"] ?? DEFAULT_ARGS.scratchDir,
    pageSize: positiveInteger(values["--page-size"], DEFAULT_ARGS.pageSize, "--page-size"),
    timeoutMs: positiveInteger(values["--timeout-ms"], DEFAULT_ARGS.timeoutMs, "--timeout-ms"),
    reconcileEntityIds,
    outsideCommitteeLimit: positiveInteger(
      values["--outside-committee-limit"],
      DEFAULT_ARGS.outsideCommitteeLimit,
      "--outside-committee-limit"
    ),
  };
}

export type WestVirginiaPhaseZeroClient = {
  getCatalog: typeof getWestVirginiaDataDownloadCatalog;
  getDownloadFileUrl: typeof getWestVirginiaDataDownloadFileUrl;
  downloadPresignedFile: typeof downloadWestVirginiaPresignedFile;
  getAllCommittees: typeof getAllWestVirginiaCommittees;
  getAllTransactions: typeof getAllWestVirginiaTransactions;
  getAllOrgDocuments: typeof getAllWestVirginiaOrgDocuments;
  getDocumentDownloadUrl: typeof getWestVirginiaDocumentDownloadUrl;
};

const DEFAULT_CLIENT: WestVirginiaPhaseZeroClient = {
  getCatalog: getWestVirginiaDataDownloadCatalog,
  getDownloadFileUrl: getWestVirginiaDataDownloadFileUrl,
  downloadPresignedFile: downloadWestVirginiaPresignedFile,
  getAllCommittees: getAllWestVirginiaCommittees,
  getAllTransactions: getAllWestVirginiaTransactions,
  getAllOrgDocuments: getAllWestVirginiaOrgDocuments,
  getDocumentDownloadUrl: getWestVirginiaDocumentDownloadUrl,
};

async function downloadCatalogArtifact(input: {
  client: WestVirginiaPhaseZeroClient;
  clientOptions: WestVirginiaCfrsClientOptions;
  catalog: Awaited<ReturnType<typeof getWestVirginiaDataDownloadCatalog>>;
  dataType: string;
  year: number;
  scratchDir: string;
}): Promise<{ path: string; sha256: string; bytesWritten: number; text: string }> {
  const row = input.catalog.find(
    (entry) => entry.dataType === input.dataType && entry.year === String(input.year)
  );
  if (!row) {
    throw new Error(`West Virginia catalog is missing ${input.dataType} ${input.year}`);
  }
  const url = await input.client.getDownloadFileUrl(row.id, input.clientOptions);
  const outputPath = join(
    input.scratchDir,
    `${input.dataType.replaceAll(" ", "_")}_${input.year}.csv`
  );
  const download = await input.client.downloadPresignedFile({ url, outputPath }, input.clientOptions);
  const text = decodeWestVirginiaCsvBytes(await readFile(download.outputPath));
  return { path: download.outputPath, sha256: download.sha256, bytesWritten: download.bytesWritten, text };
}

function requireCleanParse<T>(
  label: string,
  result: { rows: T[]; errors: Array<{ line: number; reason: string }> }
): T[] {
  if (result.errors.length > 0) {
    const sample = result.errors
      .slice(0, 3)
      .map((error) => `line ${error.line}: ${error.reason}`)
      .join("; ");
    throw new Error(`West Virginia ${label} parse produced ${result.errors.length} errors (${sample})`);
  }
  return result.rows;
}

function pickReconciliationEntityIds(input: {
  requested: readonly string[];
  csvRows: readonly WestVirginiaContributionCsvRow[];
  apiRows: readonly WestVirginiaTransactionRow[];
}): string[] {
  const picked = new Set(input.requested);
  const totals = new Map<string, number>();
  for (const row of input.csvRows) {
    // The API harvest is org-101 (state candidates) only; the CSV also holds
    // PAC/party registrants — comparing those would produce false mismatches.
    if (!row.registrantId.startsWith(WEST_VIRGINIA_ORG_TYPE_CODES.stateCandidate)) continue;
    totals.set(row.registrantId, (totals.get(row.registrantId) ?? 0) + row.amountCents);
  }
  const topByTotal = [...totals.entries()].sort((a, b) => b[1] - a[1]);
  for (const [entityId] of topByTotal) {
    if (picked.size >= input.requested.length + 3) break;
    picked.add(entityId);
  }
  const amendedEntityIds = new Set(
    input.apiRows.filter((row) => row.amendedFlag).map((row) => row.entityID)
  );
  let amendedAdded = 0;
  for (const entityId of amendedEntityIds) {
    if (amendedAdded >= 2) break;
    if (!picked.has(entityId)) {
      picked.add(entityId);
      amendedAdded += 1;
    }
  }
  return [...picked].sort();
}

export async function runProbeWestVirginiaCandidateFinance(input: {
  args: WestVirginiaPhaseZeroArgs;
  client?: WestVirginiaPhaseZeroClient;
  now?: Date;
}) {
  const { args } = input;
  const client = input.client ?? DEFAULT_CLIENT;
  const clientOptions: WestVirginiaCfrsClientOptions = { timeoutMs: args.timeoutMs };
  const latestYear = args.years[args.years.length - 1];

  // Gate: catalog fetch (exact pagination body semantics live in the client).
  const catalog = await client.getCatalog({}, clientOptions);
  if (catalog.length < 80) {
    throw new Error(`West Virginia catalog returned only ${catalog.length} artifacts`);
  }

  // Gate: bulk artifacts download + malformed-CSV recovery.
  const contributionsByYear: Array<{
    year: number;
    sha256: string;
    bytes: number;
    recoveredRowCount: number;
    rows: WestVirginiaContributionCsvRow[];
  }> = [];
  for (const year of args.years) {
    const artifact = await downloadCatalogArtifact({
      client,
      clientOptions,
      catalog,
      dataType: "Contributions",
      year,
      scratchDir: args.scratchDir,
    });
    const parse = parseWestVirginiaContributionCsv(artifact.text);
    contributionsByYear.push({
      year,
      sha256: artifact.sha256,
      bytes: artifact.bytesWritten,
      recoveredRowCount: parse.recoveredRowCount,
      rows: requireCleanParse(`contributions ${year}`, parse),
    });
    await sleep(COURTESY_DELAY_MS);
  }

  const expenditureArtifact = await downloadCatalogArtifact({
    client,
    clientOptions,
    catalog,
    dataType: "Expenditures",
    year: latestYear,
    scratchDir: args.scratchDir,
  });
  const expenditureParse = parseWestVirginiaExpenditureCsv(expenditureArtifact.text);
  const expenditureRows = requireCleanParse(`expenditures ${latestYear}`, expenditureParse);

  const registrationArtifact = await downloadCatalogArtifact({
    client,
    clientOptions,
    catalog,
    dataType: "Registrations",
    year: latestYear,
    scratchDir: args.scratchDir,
  });
  const registrationRows = requireCleanParse(
    `registrations ${latestYear}`,
    parseWestVirginiaRegistrationCsv(registrationArtifact.text)
  );

  const reportingScheduleRows = [];
  for (const year of args.years) {
    const artifact = await downloadCatalogArtifact({
      client,
      clientOptions,
      catalog,
      dataType: "Reporting Schedules",
      year,
      scratchDir: args.scratchDir,
    });
    reportingScheduleRows.push(
      ...requireCleanParse(`reporting-schedules ${year}`, parseWestVirginiaReportingScheduleCsv(artifact.text))
    );
    await sleep(COURTESY_DELAY_MS);
  }

  // Gate: acquisition determinism — a second mint+download of the same nightly
  // object must be byte-identical.
  const determinismFirst = await downloadCatalogArtifact({
    client,
    clientOptions,
    catalog,
    dataType: "Reporting Schedules",
    year: latestYear,
    scratchDir: join(args.scratchDir, "determinism"),
  });
  const determinismOk =
    determinismFirst.sha256 ===
    (
      await downloadCatalogArtifact({
        client,
        clientOptions,
        catalog,
        dataType: "Reporting Schedules",
        year: latestYear,
        scratchDir: join(args.scratchDir, "determinism-repeat"),
      })
    ).sha256;
  if (!determinismOk) {
    throw new Error("West Virginia determinism gate failed: repeated download hashes differ");
  }

  // Gate: registry sweep + entityId<->RegistrantID join.
  const committees = await client.getAllCommittees({ pageSize: args.pageSize }, clientOptions);
  const latestContributions = contributionsByYear[contributionsByYear.length - 1];
  const registryJoin = checkWestVirginiaRegistryJoin({
    csvRegistrantIds: new Set(
      contributionsByYear.flatMap((entry) => entry.rows.map((row) => row.registrantId))
    ),
    committees,
  });

  // Gate: API transaction harvest + CSV<->API reconciliation + occupations.
  const apiRowsByYear = new Map<number, WestVirginiaTransactionRow[]>();
  for (const year of args.years) {
    apiRowsByYear.set(
      year,
      await client.getAllTransactions(
        {
          orgTypeCode: WEST_VIRGINIA_ORG_TYPE_CODES.stateCandidate,
          transactionCategory: "CON",
          transactionYear: year,
          pageSize: args.pageSize,
        },
        clientOptions
      )
    );
    await sleep(COURTESY_DELAY_MS);
  }
  const latestApiRows = apiRowsByYear.get(latestYear) ?? [];

  const reconciliations: Array<
    { year: number } & ReturnType<typeof reconcileWestVirginiaCommittee>
  > = [];
  for (const entityId of pickReconciliationEntityIds({
    requested: args.reconcileEntityIds,
    csvRows: latestContributions.rows,
    apiRows: latestApiRows,
  })) {
    reconciliations.push({
      year: latestYear,
      ...reconcileWestVirginiaCommittee({
        entityId,
        csvRows: latestContributions.rows,
        apiRows: latestApiRows,
        contributionCategories: CONTRIBUTION_FILE_CATEGORIES,
      }),
    });
  }
  // Amendment fixtures live wherever amendedFlag rows exist — reconcile up to
  // two amended state-candidate committees per earlier year as well.
  for (const entry of contributionsByYear.slice(0, -1)) {
    const yearApiRows = apiRowsByYear.get(entry.year) ?? [];
    const amendedIds = [
      ...new Set(
        yearApiRows
          .filter(
            (row) =>
              row.amendedFlag && row.entityID.startsWith(WEST_VIRGINIA_ORG_TYPE_CODES.stateCandidate)
          )
          .map((row) => row.entityID)
      ),
    ].slice(0, 2);
    for (const entityId of amendedIds) {
      reconciliations.push({
        year: entry.year,
        ...reconcileWestVirginiaCommittee({
          entityId,
          csvRows: entry.rows,
          apiRows: yearApiRows,
          contributionCategories: CONTRIBUTION_FILE_CATEGORIES,
        }),
      });
    }
  }

  const occupations = summarizeWestVirginiaOccupations(latestApiRows);
  const apiCategoryCounts: Record<string, number> = {};
  for (const row of latestApiRows) {
    const category = row.transactionCategoryDesc ?? "<null>";
    apiCategoryCounts[category] = (apiCategoryCounts[category] ?? 0) + 1;
  }

  // Gate: outside document inventory + PDF text-layer checks.
  const outsideCommittees = committees
    .filter(
      (committee) =>
        committee.orgType.includes("Independent Expenditure") ||
        committee.orgType.includes("Electioneering")
    )
    .filter(
      (committee) =>
        committee.registrationYear !== null && Number(committee.registrationYear) >= args.years[0]
    )
    .slice(0, args.outsideCommitteeLimit);
  const outsideEntries: Array<{
    committee: WestVirginiaCommitteeRow;
    documents: Awaited<ReturnType<typeof getAllWestVirginiaOrgDocuments>>;
  }> = [];
  for (const committee of outsideCommittees) {
    outsideEntries.push({
      committee,
      documents: await client.getAllOrgDocuments({ orgID: committee.orgID, pageSize: args.pageSize }, clientOptions),
    });
    await sleep(COURTESY_DELAY_MS);
  }
  const outsideInventory = summarizeWestVirginiaOutsideInventory(outsideEntries);

  let outsidePdf: { redactedUrl: string; bytes: number; hasFontMarker: boolean } | null = null;
  const firstIeDocument = outsideEntries
    .flatMap((entry) => entry.documents)
    .find(isWestVirginiaIndependentExpenditureDocument);
  if (firstIeDocument) {
    const url = await client.getDocumentDownloadUrl(firstIeDocument.s3DocName, clientOptions);
    const download = await client.downloadPresignedFile(
      { url, outputPath: join(args.scratchDir, "sample-independent-expenditure.pdf") },
      clientOptions
    );
    outsidePdf = {
      redactedUrl: download.redactedUrl,
      bytes: download.bytesWritten,
      hasFontMarker: pdfHasFontMarker(await readFile(download.outputPath)),
    };
  }

  // Cover extractability: one portal-generated filed-report PDF from the API.
  let coverPdf: { reportFileName: string | null; bytes: number; hasFontMarker: boolean } | null = null;
  const rowWithReport = latestApiRows.find((row) => row.s3ReportFilePath !== null);
  if (rowWithReport?.s3ReportFilePath) {
    const url = await client.getDocumentDownloadUrl(rowWithReport.s3ReportFilePath, clientOptions);
    const download = await client.downloadPresignedFile(
      { url, outputPath: join(args.scratchDir, "sample-filed-report.pdf") },
      clientOptions
    );
    coverPdf = {
      reportFileName: rowWithReport.reportFileName,
      bytes: download.bytesWritten,
      hasFontMarker: pdfHasFontMarker(await readFile(download.outputPath)),
    };
  }

  const election2026Candidates = committees.filter(
    (committee) => committee.election === "2026 Election" && committee.orgType === "State Candidate"
  );

  return {
    type: "west_virginia_campaign_finance_phase_zero_probe" as const,
    ts: (input.now ?? new Date()).toISOString(),
    ok: true,
    tls_fallback_uses: getWestVirginiaTlsFallbackUseCount(),
    catalog: { artifactCount: catalog.length },
    bulk: {
      contributions: contributionsByYear.map((entry) => ({
        year: entry.year,
        sha256: entry.sha256,
        bytes: entry.bytes,
        recoveredRowCount: entry.recoveredRowCount,
        summary: summarizeWestVirginiaContributionCsv(entry.rows),
      })),
      expenditures: {
        year: latestYear,
        rowCount: expenditureRows.length,
        recoveredRowCount: expenditureParse.recoveredRowCount,
        totalDollars: (expenditureRows.reduce((sum, row) => sum + row.amountCents, 0) / 100).toFixed(2),
      },
      registrations: { year: latestYear, rowCount: registrationRows.length },
      determinism: { ok: determinismOk, sha256: determinismFirst.sha256 },
    },
    reporting_cycles: summarizeWestVirginiaReportingCycles(reportingScheduleRows),
    registry: {
      committeeCount: committees.length,
      election2026StateCandidates: election2026Candidates.length,
      election2026Active: election2026Candidates.filter((committee) => committee.orgStatus === "Active").length,
      join: registryJoin,
    },
    api: {
      rowsByYear: [...apiRowsByYear.entries()].map(([year, rows]) => ({
        year,
        rowCount: rows.length,
        amendedRowCount: rows.filter((row) => row.amendedFlag).length,
        reportVersionCounts: rows.reduce<Record<string, number>>((counts, row) => {
          const version = row.reportVersionID ?? "<null>";
          counts[version] = (counts[version] ?? 0) + 1;
          return counts;
        }, {}),
      })),
      latestYearCategoryCounts: apiCategoryCounts,
    },
    reconciliation: reconciliations,
    occupations,
    outside: {
      inventory: outsideInventory,
      sampleIndependentExpenditurePdf: outsidePdf,
      sampleFiledReportPdf: coverPdf,
    },
    publication: "disabled_phase_zero" as const,
  };
}

async function main(): Promise<void> {
  const output = await runProbeWestVirginiaCandidateFinance({
    args: parseProbeWestVirginiaCandidateFinanceArgs(process.argv.slice(2)),
  });
  console.log(JSON.stringify(output, null, 2));
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    console.error(
      "West Virginia campaign-finance Phase 0 probe failed:",
      error instanceof Error ? error.message : error
    );
    process.exitCode = 1;
  });
}
