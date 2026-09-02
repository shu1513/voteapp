// Phase 0 only: validates the Idaho CFIS acquisition contracts and the
// totals strategy. No database, cache, scheduler, or published snapshot.

import { pathToFileURL } from "node:url";

import {
  downloadIdahoCfsBulkCsv,
  getAllIdahoCandidateRegistrations,
  getAllIdahoContributions,
  getAllIdahoIndependentExpenditures,
  idahoRegistrationSearchName,
  type IdahoCfsClientOptions,
} from "../pipeline/idahoFinance/idahoCfsClient.js";
import {
  assertIdahoCsvQuarantineTolerance,
  parseIdahoExpenditureCsv,
  parseIdahoReceiptCsv,
  type IdahoReceiptCsvRow,
} from "../pipeline/idahoFinance/idahoCfsCsv.js";
import {
  countIdahoBulkRowsOutsideSearch,
  reconcileIdahoRegistration,
  summarizeIdahoIndependentExpenditures,
} from "../pipeline/idahoFinance/idahoPhaseZero.js";
import { assertKnownCliFlags } from "./financeCliFlagGuard.js";

export type IdahoPhaseZeroArgs = {
  cycleYear: number;
  filingYears: number[];
  filerEntityId: number;
  pageSize: number;
  timeoutMs: number;
};

export type IdahoPhaseZeroClient = {
  downloadBulkCsv: typeof downloadIdahoCfsBulkCsv;
  getAllCandidateRegistrations: typeof getAllIdahoCandidateRegistrations;
  getAllContributions: typeof getAllIdahoContributions;
  getAllIndependentExpenditures: typeof getAllIdahoIndependentExpenditures;
};

const DEFAULT_ARGS: IdahoPhaseZeroArgs = {
  cycleYear: 2026,
  filingYears: [2025, 2026],
  // Todd Achilles: two registrations (2024 State Representative, 2026), the
  // 2024 one spanning filing years 2023+2024. Both reconcile cent-exact
  // (verified 2026-09-01).
  filerEntityId: 257,
  pageSize: 500,
  timeoutMs: 120_000,
};

const DEFAULT_CLIENT: IdahoPhaseZeroClient = {
  downloadBulkCsv: downloadIdahoCfsBulkCsv,
  getAllCandidateRegistrations: getAllIdahoCandidateRegistrations,
  getAllContributions: getAllIdahoContributions,
  getAllIndependentExpenditures: getAllIdahoIndependentExpenditures,
};

const BOOLEAN_FLAGS = new Set<string>();
const VALUE_FLAGS = new Set([
  "--cycle-year",
  "--filing-years",
  "--filer-entity-id",
  "--page-size",
  "--timeout-ms",
]);

function readValueFlag(args: readonly string[], name: string): string | undefined {
  let value: string | undefined;
  const prefix = `${name}=`;
  for (let index = 0; index < args.length; index += 1) {
    const arg = args[index]!;
    let found: string | undefined;
    if (arg.startsWith(prefix)) {
      found = arg.slice(prefix.length).trim();
    } else if (arg === name) {
      found = args[index + 1]!.trim();
      index += 1;
    }
    if (found === undefined) continue;
    if (value !== undefined) throw new Error(`Provide ${name} at most once`);
    value = found;
  }
  return value;
}

function positiveInteger(value: string | undefined, fallback: number, name: string): number {
  const raw = value ?? String(fallback);
  if (!/^[1-9]\d*$/.test(raw)) throw new Error(`Invalid ${name}: ${raw}`);
  return Number(raw);
}

function filingYears(value: string | undefined, fallback: readonly number[]): number[] {
  if (value === undefined) return [...fallback];
  const years = value.split(",").map((year) => positiveInteger(year.trim(), 0, "--filing-years"));
  if (new Set(years).size !== years.length) throw new Error("--filing-years contains duplicates");
  return years;
}

export function parseProbeIdahoCampaignFinanceArgs(args: readonly string[]): IdahoPhaseZeroArgs {
  assertKnownCliFlags(args, "Idaho campaign-finance Phase 0 probe", BOOLEAN_FLAGS, VALUE_FLAGS);
  return {
    cycleYear: positiveInteger(readValueFlag(args, "--cycle-year"), DEFAULT_ARGS.cycleYear, "--cycle-year"),
    filingYears: filingYears(readValueFlag(args, "--filing-years"), DEFAULT_ARGS.filingYears),
    filerEntityId: positiveInteger(
      readValueFlag(args, "--filer-entity-id"),
      DEFAULT_ARGS.filerEntityId,
      "--filer-entity-id"
    ),
    pageSize: positiveInteger(readValueFlag(args, "--page-size"), DEFAULT_ARGS.pageSize, "--page-size"),
    timeoutMs: positiveInteger(readValueFlag(args, "--timeout-ms"), DEFAULT_ARGS.timeoutMs, "--timeout-ms"),
  };
}

export async function runProbeIdahoCampaignFinance(input: {
  args: IdahoPhaseZeroArgs;
  client?: IdahoPhaseZeroClient;
  now?: Date;
}) {
  const client = input.client ?? DEFAULT_CLIENT;
  const clientOptions: IdahoCfsClientOptions = { timeoutMs: input.args.timeoutMs };

  const registrations = await client.getAllCandidateRegistrations({ pageSize: input.args.pageSize }, clientOptions);
  const fixtureRegistrations = registrations.filter(
    (registration) => registration.filerEntityId === input.args.filerEntityId
  );
  if (fixtureRegistrations.length === 0) {
    throw new Error(`Idaho CFIS candidate grid has no registration for entity ${input.args.filerEntityId}`);
  }

  const bulkRows: IdahoReceiptCsvRow[] = [];
  const bulkArtifacts: Array<{ filingYear: number; rowCount: number; quarantinedCount: number; byteCount: number }> = [];
  for (const filingYear of input.args.filingYears) {
    const csv = await client.downloadBulkCsv({ filingYear, transactionTypeCode: "TCON" }, clientOptions);
    const parsed = parseIdahoReceiptCsv(csv);
    assertIdahoCsvQuarantineTolerance(parsed, `receipt ${filingYear}`);
    for (const row of parsed.rows) bulkRows.push(row);
    bulkArtifacts.push({
      filingYear,
      rowCount: parsed.rows.length,
      quarantinedCount: parsed.quarantined.length,
      // windows-1252 decodes one byte to one character, so the decoded
      // length is the raw artifact size (re-encoding as UTF-8 would not be).
      byteCount: csv.length,
    });
  }

  // Outside spending comes from the IE search, but the expenditure export
  // header is still a Phase 0 contract. Parse one cycle-year file and discard.
  const expenditureCsv = await client.downloadBulkCsv(
    { filingYear: input.args.cycleYear, transactionTypeCode: "TEXP" },
    clientOptions
  );
  const expenditures = parseIdahoExpenditureCsv(expenditureCsv);
  assertIdahoCsvQuarantineTolerance(expenditures, `expenditure ${input.args.cycleYear}`);

  const reconciliations = [];
  const entitySearchRows = [];
  for (const registration of fixtureRegistrations) {
    const searchRows = await client.getAllContributions(
      { filerName: idahoRegistrationSearchName(registration), pageSize: input.args.pageSize },
      clientOptions
    );
    entitySearchRows.push(...searchRows);
    const reconciliation = reconcileIdahoRegistration({
      registration,
      searchRows,
      bulkRows,
      bulkFilingYears: input.args.filingYears,
    });
    if (reconciliation.status !== "match") {
      throw new Error(
        `Idaho registration ${registration.registrationGuid} (${registration.filerName} ${registration.electionYear}) search sum differs from grid totalRaised by ${reconciliation.deltaCents} cents`
      );
    }
    if (!reconciliation.bulkMatchesVersionOne) {
      throw new Error(
        `Idaho bulk export no longer equals the version-1 rows for registration ${registration.registrationGuid}; revisit the totals strategy`
      );
    }
    reconciliations.push(reconciliation);
  }
  const bulkRowsOutsideSearch = countIdahoBulkRowsOutsideSearch({
    filerEntityId: input.args.filerEntityId,
    bulkRows,
    searchRows: entitySearchRows,
  });
  if (bulkRowsOutsideSearch > 0) {
    throw new Error(
      `Idaho bulk export has ${bulkRowsOutsideSearch} contribution rows for entity ${input.args.filerEntityId} that the transaction search does not return; revisit the totals strategy`
    );
  }

  const independentExpenditures = await client.getAllIndependentExpenditures(
    { pageSize: input.args.pageSize },
    clientOptions
  );
  const outside = summarizeIdahoIndependentExpenditures(independentExpenditures, input.args.cycleYear);

  return {
    type: "idaho_campaign_finance_phase_zero_probe" as const,
    ts: (input.now ?? new Date()).toISOString(),
    ok: true,
    cycle_year: input.args.cycleYear,
    candidate_registrations: {
      total: registrations.length,
      cycle_year: registrations.filter((registration) => registration.electionYear === input.args.cycleYear).length,
    },
    bulk: {
      receipts: bulkArtifacts,
      expenditures: {
        filingYear: input.args.cycleYear,
        rowCount: expenditures.rows.length,
        quarantinedCount: expenditures.quarantined.length,
        byteCount: expenditureCsv.length,
      },
    },
    totals_fixture: {
      filer_entity_id: input.args.filerEntityId,
      registrations: reconciliations,
      bulk_rows_outside_search: bulkRowsOutsideSearch,
      strategy: "grid_totals_plus_search_rows_bulk_is_version_one_only" as const,
    },
    independent_expenditures: outside,
    publication: "disabled_phase_zero" as const,
  };
}

async function main(): Promise<void> {
  const output = await runProbeIdahoCampaignFinance({
    args: parseProbeIdahoCampaignFinanceArgs(process.argv.slice(2)),
  });
  console.log(JSON.stringify(output, null, 2));
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    console.error("Idaho campaign-finance Phase 0 probe failed:", error instanceof Error ? error.message : error);
    process.exitCode = 1;
  });
}
