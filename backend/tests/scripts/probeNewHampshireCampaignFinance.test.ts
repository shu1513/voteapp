import { readFile } from "node:fs/promises";

import { describe, expect, it, vi } from "vitest";

import {
  getNewHampshireIndependentExpenditurePage,
  getNewHampshireReceiptPage,
} from "../../src/pipeline/newHampshireFinance/newHampshireCfsClient.js";
import {
  parseProbeNewHampshireCampaignFinanceArgs,
  runProbeNewHampshireCampaignFinance,
  type NewHampshirePhaseZeroClient,
} from "../../src/scripts/probeNewHampshireCampaignFinance.js";

const fixtures = new URL("../fixtures/newHampshireFinance/", import.meta.url);

async function fixture(name: string): Promise<string> {
  return readFile(new URL(name, fixtures), "utf8");
}

function jsonResponse(body: string): Response {
  return new Response(body, { status: 200, headers: { "Content-Type": "application/json" } });
}

describe("probeNewHampshireCampaignFinance", () => {
  it("parses strict Phase 0 arguments", () => {
    expect(parseProbeNewHampshireCampaignFinanceArgs([])).toEqual({
      cycleYear: 2026,
      filingYears: [2025, 2026],
      filingEntityId: 50450,
      filerName: "Anita Burroughs for New Hampshire",
      pageSize: 200,
      timeoutMs: 120_000,
    });
    expect(
      parseProbeNewHampshireCampaignFinanceArgs([
        "--cycle-year=2028",
        "--filing-years",
        "2027,2028",
        "--filing-entity-id=123",
        "--filer-name",
        "Example",
        "--page-size=50",
        "--timeout-ms=5000",
      ])
    ).toMatchObject({ cycleYear: 2028, filingYears: [2027, 2028], filingEntityId: 123, filerName: "Example" });
    expect(() => parseProbeNewHampshireCampaignFinanceArgs(["--unknown=1"])).toThrow("Unknown argument");
  });

  it("runs all Phase 0 gates without publishing", async () => {
    const receiptsCsv = await fixture("receipts-sanitized.csv");
    const expendituresCsv = await fixture("expenditures-sanitized.csv");
    const receiptJson = await fixture("receipt-amendments-sanitized.json");
    const ieJson = await fixture("independent-expenditures-sanitized.json");
    const receiptRows = (
      await getNewHampshireReceiptPage(
        { filerName: "Example Committee", electionCycleId: 110, pageSize: 200 },
        { fetchImpl: vi.fn().mockResolvedValue(jsonResponse(receiptJson)) as unknown as typeof fetch }
      )
    ).items;
    const ieRows = (
      await getNewHampshireIndependentExpenditurePage(
        { electionCycleId: 110, pageSize: 200 },
        { fetchImpl: vi.fn().mockResolvedValue(jsonResponse(ieJson)) as unknown as typeof fetch }
      )
    ).items;
    const client: NewHampshirePhaseZeroClient = {
      getElectionCycles: vi.fn().mockResolvedValue([
        { value: 110, name: "2026 Election Cycle", dueDate: "2026-11-03T00:00:00" },
      ]),
      downloadBulkCsv: vi.fn().mockImplementation(async ({ transactionTypeCode }) =>
        transactionTypeCode === "TCON" ? receiptsCsv : expendituresCsv
      ),
      getAllReceipts: vi.fn().mockResolvedValue(receiptRows),
      getAllIndependentExpenditures: vi.fn().mockResolvedValue(ieRows),
    };

    const output = await runProbeNewHampshireCampaignFinance({
      args: {
        cycleYear: 2026,
        filingYears: [2026],
        filingEntityId: 50450,
        filerName: "Example Committee",
        pageSize: 200,
        timeoutMs: 5_000,
      },
      client,
      now: new Date("2026-08-19T00:00:00.000Z"),
    });

    expect(output).toMatchObject({
      type: "new_hampshire_campaign_finance_phase_zero_probe",
      ts: "2026-08-19T00:00:00.000Z",
      ok: true,
      cycle: { year: 2026, id: 110, name: "2026 Election Cycle" },
      amendment_fixture: {
        status: "mismatch",
        strategy: "search_api_current_report_versions_required",
      },
      independent_expenditures: {
        sourceRowCount: 6,
        rowCount: 4,
        supersededRowCount: 2,
        support: { rowCount: 2, amountCents: 22_500 },
        oppose: { rowCount: 1, amountCents: 10_000 },
        blankStance: { rowCount: 1, amountCents: 5_000 },
      },
      publication: "disabled_phase_zero",
    });
    expect(client.downloadBulkCsv).toHaveBeenCalledTimes(2);
  });
});
