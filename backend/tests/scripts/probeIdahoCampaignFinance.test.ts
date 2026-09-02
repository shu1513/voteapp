import { readFile } from "node:fs/promises";

import { describe, expect, it, vi } from "vitest";

import {
  getIdahoCandidateRegistrationPage,
  getIdahoContributionPage,
  getIdahoIndependentExpenditurePage,
} from "../../src/pipeline/idahoFinance/idahoCfsClient.js";
import {
  parseProbeIdahoCampaignFinanceArgs,
  runProbeIdahoCampaignFinance,
  type IdahoPhaseZeroClient,
} from "../../src/scripts/probeIdahoCampaignFinance.js";

const fixtures = new URL("../fixtures/idahoFinance/", import.meta.url);

async function fixture(name: string): Promise<string> {
  return readFile(new URL(name, fixtures), "utf8");
}

function jsonFetch(body: string): typeof fetch {
  return vi.fn().mockResolvedValue(
    new Response(body, { status: 200, headers: { "Content-Type": "application/json" } })
  ) as unknown as typeof fetch;
}

async function buildClient(overrides: { totalRaised?: number; extraBulkLine?: string } = {}): Promise<IdahoPhaseZeroClient> {
  const registrations = (
    await getIdahoCandidateRegistrationPage({}, { fetchImpl: jsonFetch(await fixture("candidate-registrations-sanitized.json")) })
  ).items.map((registration) =>
    registration.registrationGuid.endsWith("126") && overrides.totalRaised !== undefined
      ? { ...registration, totalRaised: overrides.totalRaised }
      : registration
  );
  const contributions = (
    await getIdahoContributionPage(
      { filerName: "Todd Baker Achilles" },
      { fetchImpl: jsonFetch(await fixture("contributions-sanitized.json")) }
    )
  ).items;
  const independentExpenditures = (
    await getIdahoIndependentExpenditurePage({}, { fetchImpl: jsonFetch(await fixture("independent-expenditures-sanitized.json")) })
  ).items;
  // The probe enforces the 1% quarantine tolerance, so feed it the fixture
  // without its deliberately corrupted record.
  const receiptsCsv = [
    ...(await fixture("receipts-sanitized.csv")).split("\n").filter((line) => !line.startsWith("227,") && line !== ""),
    ...(overrides.extraBulkLine ? [overrides.extraBulkLine] : []),
    "",
  ].join("\n");
  const expendituresCsv = await fixture("expenditures-sanitized.csv");
  return {
    downloadBulkCsv: vi.fn().mockImplementation(async ({ transactionTypeCode }) =>
      transactionTypeCode === "TCON" ? receiptsCsv : expendituresCsv
    ),
    getAllCandidateRegistrations: vi.fn().mockResolvedValue(registrations),
    getAllContributions: vi.fn().mockImplementation(async ({ filerName }) =>
      contributions.filter((row) =>
        filerName === "Todd Baker Achilles"
          ? row.filerRegistrationGuid.endsWith("126")
          : row.filerRegistrationGuid.endsWith("124")
      )
    ),
    getAllIndependentExpenditures: vi.fn().mockResolvedValue(independentExpenditures),
  };
}

describe("probeIdahoCampaignFinance", () => {
  it("parses strict Phase 0 arguments", () => {
    expect(parseProbeIdahoCampaignFinanceArgs([])).toEqual({
      cycleYear: 2026,
      filingYears: [2025, 2026],
      filerEntityId: 257,
      pageSize: 500,
      timeoutMs: 120_000,
    });
    expect(
      parseProbeIdahoCampaignFinanceArgs([
        "--cycle-year=2028",
        "--filing-years",
        "2027,2028",
        "--filer-entity-id=123",
        "--page-size=50",
        "--timeout-ms=5000",
      ])
    ).toEqual({ cycleYear: 2028, filingYears: [2027, 2028], filerEntityId: 123, pageSize: 50, timeoutMs: 5000 });
    expect(() => parseProbeIdahoCampaignFinanceArgs(["--unknown=1"])).toThrow("Unknown Idaho campaign-finance Phase 0 probe flag");
    expect(() => parseProbeIdahoCampaignFinanceArgs(["--filing-years", "2025,2025"])).toThrow("duplicates");
    expect(() => parseProbeIdahoCampaignFinanceArgs(["--page-size"])).toThrow("Missing --page-size value");
    expect(() => parseProbeIdahoCampaignFinanceArgs(["dry-run"])).toThrow("Unexpected positional argument");
  });

  it("runs all Phase 0 gates without publishing", async () => {
    const client = await buildClient();
    const output = await runProbeIdahoCampaignFinance({
      args: { cycleYear: 2026, filingYears: [2025], filerEntityId: 257, pageSize: 500, timeoutMs: 5_000 },
      client,
      now: new Date("2026-09-01T00:00:00.000Z"),
    });

    expect(output).toMatchObject({
      type: "idaho_campaign_finance_phase_zero_probe",
      ts: "2026-09-01T00:00:00.000Z",
      ok: true,
      cycle_year: 2026,
      candidate_registrations: { total: 3, cycle_year: 2 },
      bulk: {
        receipts: [{ filingYear: 2025, rowCount: 5, quarantinedCount: 0 }],
        expenditures: { filingYear: 2026, rowCount: 4, quarantinedCount: 0 },
      },
      totals_fixture: {
        filer_entity_id: 257,
        bulk_rows_outside_search: 0,
        strategy: "grid_totals_plus_search_rows_bulk_is_version_one_only",
      },
      independent_expenditures: {
        rowCount: 4,
        support: { rowCount: 3, amountCents: 58_975 },
        oppose: { rowCount: 1, amountCents: 100_000 },
      },
      publication: "disabled_phase_zero",
    });
    expect(output.totals_fixture.registrations.map((entry) => [entry.electionYear, entry.status, entry.bulkMatchesVersionOne])).toEqual([
      [2024, "match", true],
      [2026, "match", true],
    ]);
    expect(client.downloadBulkCsv).toHaveBeenCalledTimes(2);
    expect(client.getAllContributions).toHaveBeenCalledWith(
      { filerName: "Todd Baker Achilles", pageSize: 500 },
      { timeoutMs: 5_000 }
    );
  });

  it("fails when the grid total and the search rows disagree", async () => {
    const client = await buildClient({ totalRaised: 1600 });
    await expect(
      runProbeIdahoCampaignFinance({
        args: { cycleYear: 2026, filingYears: [2025], filerEntityId: 257, pageSize: 500, timeoutMs: 5_000 },
        client,
      })
    ).rejects.toThrow("differs from grid totalRaised by -10000 cents");

    await expect(
      runProbeIdahoCampaignFinance({
        args: { cycleYear: 2026, filingYears: [2025], filerEntityId: 4242, pageSize: 500, timeoutMs: 5_000 },
        client,
      })
    ).rejects.toThrow("no registration for entity 4242");
  });

  it("fails when the bulk export carries a contribution the search does not return", async () => {
    const client = await buildClient({
      extraBulkLine:
        '257,"Achilles, Todd Baker",Todd Achilles for Idaho,Candidate,999999,Contribution,Itemized,Person,Sample,Donor Ghost,,REDACTED,,Boise,ID,="83702",03/08/2025,$75.00,$0.00,$0.00,2026,Primary,,N,,,2025 Annual Report,01/10/2026',
    });
    await expect(
      runProbeIdahoCampaignFinance({
        args: { cycleYear: 2026, filingYears: [2025], filerEntityId: 257, pageSize: 500, timeoutMs: 5_000 },
        client,
      })
    ).rejects.toThrow("has 1 contribution rows for entity 257 that the transaction search does not return");
  });
});
