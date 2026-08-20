// Phase 0 only: validates the NH CFS acquisition contracts and decides the
// amendment strategy. No database, cache, scheduler, or published snapshot.

import { pathToFileURL } from "node:url";

import {
  downloadNewHampshireCfsBulkCsv,
  getAllNewHampshireIndependentExpenditures,
  getAllNewHampshireReceipts,
  getNewHampshireElectionCycles,
  type NewHampshireCfsClientOptions,
} from "../pipeline/newHampshireFinance/newHampshireCfsClient.js";
import {
  parseNewHampshireExpenditureCsv,
  parseNewHampshireReceiptCsv,
  type NewHampshireReceiptCsvRow,
} from "../pipeline/newHampshireFinance/newHampshireCfsCsv.js";
import {
  reconcileNewHampshireAmendmentFixture,
  summarizeNewHampshireIndependentExpenditures,
} from "../pipeline/newHampshireFinance/newHampshirePhaseZero.js";

export type NewHampshirePhaseZeroArgs = {
  cycleYear: number;
  filingYears: number[];
  filingEntityId: number;
  filerName: string;
  pageSize: number;
  timeoutMs: number;
};

export type NewHampshirePhaseZeroClient = {
  getElectionCycles: typeof getNewHampshireElectionCycles;
  downloadBulkCsv: typeof downloadNewHampshireCfsBulkCsv;
  getAllReceipts: typeof getAllNewHampshireReceipts;
  getAllIndependentExpenditures: typeof getAllNewHampshireIndependentExpenditures;
};

const DEFAULT_ARGS: NewHampshirePhaseZeroArgs = {
  cycleYear: 2026,
  filingYears: [2025, 2026],
  // Public amended-report fixture verified 2026-08-19.
  filingEntityId: 50450,
  filerName: "Anita Burroughs for New Hampshire",
  pageSize: 200,
  timeoutMs: 120_000,
};

const DEFAULT_CLIENT: NewHampshirePhaseZeroClient = {
  getElectionCycles: getNewHampshireElectionCycles,
  downloadBulkCsv: downloadNewHampshireCfsBulkCsv,
  getAllReceipts: getAllNewHampshireReceipts,
  getAllIndependentExpenditures: getAllNewHampshireIndependentExpenditures,
};

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

function filingYears(value: string | null, fallback: readonly number[]): number[] {
  if (value === null) return [...fallback];
  const years = value.split(",").map((year) => positiveInteger(year.trim(), 0, "--filing-years"));
  if (new Set(years).size !== years.length) throw new Error("--filing-years contains duplicates");
  return years;
}

export function parseProbeNewHampshireCampaignFinanceArgs(
  args: readonly string[]
): NewHampshirePhaseZeroArgs {
  const names = [
    "--cycle-year",
    "--filing-years",
    "--filing-entity-id",
    "--filer-name",
    "--page-size",
    "--timeout-ms",
  ];
  const values = parseFlags(args, names);
  return {
    cycleYear: positiveInteger(values["--cycle-year"], DEFAULT_ARGS.cycleYear, "--cycle-year"),
    filingYears: filingYears(values["--filing-years"], DEFAULT_ARGS.filingYears),
    filingEntityId: positiveInteger(
      values["--filing-entity-id"],
      DEFAULT_ARGS.filingEntityId,
      "--filing-entity-id"
    ),
    filerName: values["--filer-name"] ?? DEFAULT_ARGS.filerName,
    pageSize: positiveInteger(values["--page-size"], DEFAULT_ARGS.pageSize, "--page-size"),
    timeoutMs: positiveInteger(values["--timeout-ms"], DEFAULT_ARGS.timeoutMs, "--timeout-ms"),
  };
}

export async function runProbeNewHampshireCampaignFinance(input: {
  args: NewHampshirePhaseZeroArgs;
  client?: NewHampshirePhaseZeroClient;
  now?: Date;
}) {
  const client = input.client ?? DEFAULT_CLIENT;
  const clientOptions: NewHampshireCfsClientOptions = { timeoutMs: input.args.timeoutMs };
  const cycles = await client.getElectionCycles(clientOptions);
  const expectedCycleName = `${input.args.cycleYear} Election Cycle`;
  const matches = cycles.filter((cycle) => cycle.name === expectedCycleName);
  if (matches.length !== 1) {
    throw new Error(`Expected one NH CFS cycle named ${expectedCycleName}; found ${matches.length}`);
  }
  const cycle = matches[0]!;

  const bulkRows: NewHampshireReceiptCsvRow[] = [];
  const bulkArtifacts: Array<{ filingYear: number; rowCount: number; byteCount: number }> = [];
  for (const filingYear of input.args.filingYears) {
    const csv = await client.downloadBulkCsv(
      { filingYear, transactionTypeCode: "TCON" },
      clientOptions
    );
    const rows = parseNewHampshireReceiptCsv(csv);
    for (const row of rows) bulkRows.push(row);
    bulkArtifacts.push({ filingYear, rowCount: rows.length, byteCount: Buffer.byteLength(csv) });
  }

  // TEXP cannot supply IE target/stance, but its exact header is still a Phase
  // 0 contract. Read one cycle-year artifact and discard its rows after parse.
  const expenditureCsv = await client.downloadBulkCsv(
    { filingYear: input.args.cycleYear, transactionTypeCode: "TEXP" },
    clientOptions
  );
  const expenditureRows = parseNewHampshireExpenditureCsv(expenditureCsv);

  const receiptRows = await client.getAllReceipts(
    {
      filerName: input.args.filerName,
      electionCycleId: cycle.value,
      pageSize: input.args.pageSize,
    },
    clientOptions
  );
  const amendment = reconcileNewHampshireAmendmentFixture({
    bulkRows,
    apiRows: receiptRows,
    filingEntityId: input.args.filingEntityId,
  });
  if (amendment.amendedReportCount === 0) {
    throw new Error(`NH CFS fixture ${input.args.filingEntityId} no longer contains an amended report`);
  }

  const independentExpenditures = await client.getAllIndependentExpenditures(
    { electionCycleId: cycle.value, pageSize: input.args.pageSize },
    clientOptions
  );
  const outside = summarizeNewHampshireIndependentExpenditures(
    independentExpenditures,
    expectedCycleName
  );

  return {
    type: "new_hampshire_campaign_finance_phase_zero_probe" as const,
    ts: (input.now ?? new Date()).toISOString(),
    ok: true,
    cycle: { year: input.args.cycleYear, id: cycle.value, name: cycle.name },
    bulk: {
      receipts: bulkArtifacts,
      expenditures: {
        filingYear: input.args.cycleYear,
        rowCount: expenditureRows.length,
        byteCount: Buffer.byteLength(expenditureCsv),
      },
    },
    amendment_fixture: {
      filing_entity_id: input.args.filingEntityId,
      filer_name: input.args.filerName,
      ...amendment,
    },
    independent_expenditures: outside,
    publication: "disabled_phase_zero" as const,
  };
}

async function main(): Promise<void> {
  const output = await runProbeNewHampshireCampaignFinance({
    args: parseProbeNewHampshireCampaignFinanceArgs(process.argv.slice(2)),
  });
  console.log(JSON.stringify(output, null, 2));
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    console.error("New Hampshire campaign-finance Phase 0 probe failed:", error instanceof Error ? error.message : error);
    process.exitCode = 1;
  });
}
