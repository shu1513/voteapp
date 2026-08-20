import { readFile } from "node:fs/promises";

import { describe, expect, it, vi } from "vitest";

import {
  downloadNewHampshireCfsBulkCsv,
  getAllNewHampshireReceipts,
  getNewHampshireElectionCycles,
  getNewHampshireIndependentExpenditurePage,
  getNewHampshireReceiptPage,
  NewHampshireCfsClientError,
} from "../../../src/pipeline/newHampshireFinance/newHampshireCfsClient.js";
import {
  parseNewHampshireCurrencyCents,
  parseNewHampshireExpenditureCsv,
  parseNewHampshireReceiptCsv,
} from "../../../src/pipeline/newHampshireFinance/newHampshireCfsCsv.js";
import {
  reconcileNewHampshireAmendmentFixture,
  selectCurrentNewHampshireReceiptReportVersions,
  summarizeNewHampshireIndependentExpenditures,
} from "../../../src/pipeline/newHampshireFinance/newHampshirePhaseZero.js";

const fixtures = new URL("../../fixtures/newHampshireFinance/", import.meta.url);

async function fixture(name: string): Promise<string> {
  return readFile(new URL(name, fixtures), "utf8");
}

function response(body: string, contentType: string): Response {
  return new Response(body, { status: 200, headers: { "Content-Type": contentType } });
}

function bodyOf(fetchImpl: typeof fetch): Record<string, unknown> {
  return JSON.parse(String(vi.mocked(fetchImpl).mock.calls[0]?.[1]?.body)) as Record<string, unknown>;
}

describe("New Hampshire CFS Phase 0", () => {
  it("parses exact bulk headers, quoted money, and multiline fields", async () => {
    const receipts = parseNewHampshireReceiptCsv(await fixture("receipts-sanitized.csv"));
    const expenditures = parseNewHampshireExpenditureCsv(await fixture("expenditures-sanitized.csv"));

    expect(receipts).toHaveLength(5);
    expect(receipts[0]?.["Contributor Name"]).toBe('Sample "Nickname" Donor 1');
    expect(receipts[2]?.Description).toBe("First line\nsecond line");
    expect(receipts[3]?.["Amount of receipt"]).toBe("$1,200.50");
    expect(receipts[4]?.Description).toBe('"Malformed "INNER")');
    expect(expenditures).toHaveLength(2);
    expect(expenditures[1]?.["Transaction Type"]).toBe("Independent Expenditure");
    expect(expenditures[1]?.["Transaction Description"]).toBe('Vendor purpose with "INNER"');
    expect(parseNewHampshireCurrencyCents("$1,200.50")).toBe(120_050);

    expect(() => parseNewHampshireReceiptCsv("wrong,header\n1,2\n")).toThrow("header changed");
    expect(() => parseNewHampshireCurrencyCents("12.345")).toThrow("Invalid New Hampshire CFS currency");
  });

  it("posts the official cycle, receipt, IE, and bulk request contracts", async () => {
    const cyclesJson = await fixture("election-cycles-sanitized.json");
    const receiptJson = await fixture("receipt-amendments-sanitized.json");
    const ieJson = await fixture("independent-expenditures-sanitized.json");
    const receiptCsv = await fixture("receipts-sanitized.csv");

    const cycleFetch = vi.fn().mockResolvedValue(response(cyclesJson, "application/json; charset=utf-8")) as unknown as typeof fetch;
    await expect(getNewHampshireElectionCycles({ fetchImpl: cycleFetch })).resolves.toEqual([
      { value: 110, name: "2026 Election Cycle", dueDate: "2026-11-03T00:00:00" },
      { value: 27, name: "2024 Election Cycle", dueDate: "2024-11-05T00:00:00" },
    ]);
    expect(bodyOf(cycleFetch)).toEqual({ key: "" });

    const receiptFetch = vi.fn().mockResolvedValue(response(receiptJson, "application/json")) as unknown as typeof fetch;
    const receiptPage = await getNewHampshireReceiptPage(
      { filerName: " Example Committee ", electionCycleId: 110, pageNumber: 1, pageSize: 200 },
      { fetchImpl: receiptFetch }
    );
    expect(receiptPage.items).toHaveLength(5);
    expect(receiptPage.items[2]).toMatchObject({
      transactionId: 2001,
      filerReportId: 10,
      filerReportVersionId: 2,
      reportVersionFilter: "RPTAMD",
    });
    expect(bodyOf(receiptFetch)).toEqual({
      pageNumber: 1,
      pageSize: 200,
      sortBy: null,
      sortType: null,
      transactionTypeCode: "TCON",
      filerName: "Example Committee",
      electionCycle: "110",
    });

    const ieFetch = vi.fn().mockResolvedValue(response(ieJson, "application/json")) as unknown as typeof fetch;
    const iePage = await getNewHampshireIndependentExpenditurePage(
      { electionCycleId: 110, pageNumber: 2, pageSize: 3 },
      { fetchImpl: ieFetch }
    );
    expect(iePage.items[0]).toMatchObject({ candidateMeasure: "Candidate, Sample", stance: "Support" });
    expect(bodyOf(ieFetch)).toEqual({
      pageNumber: 2,
      pageSize: 3,
      sortBy: null,
      sortType: null,
      transactionTypeCode: "TEXP",
      transactionSearch: "TIE",
      electionCycle: "110",
    });

    const csvFetch = vi.fn().mockResolvedValue(response(receiptCsv, "text/csv")) as unknown as typeof fetch;
    await expect(
      downloadNewHampshireCfsBulkCsv(
        { filingYear: 2026, transactionTypeCode: "TCON" },
        { fetchImpl: csvFetch }
      )
    ).resolves.toBe(receiptCsv);
    expect(bodyOf(csvFetch)).toEqual({ type: "CSV", filingYear: 2026, transactionTypeCode: "TCON" });
  });

  it("fails closed on CDN HTML or API failure envelopes", async () => {
    const htmlFetch = vi.fn().mockResolvedValue(response("<html>Access Denied</html>", "text/html")) as unknown as typeof fetch;
    await expect(getNewHampshireElectionCycles({ fetchImpl: htmlFetch })).rejects.toBeInstanceOf(
      NewHampshireCfsClientError
    );

    const failedFetch = vi.fn().mockResolvedValue(
      response(JSON.stringify({ data: null, succeeded: false, error: { message: "An exception occurred." } }), "application/json")
    ) as unknown as typeof fetch;
    await expect(getNewHampshireElectionCycles({ fetchImpl: failedFetch })).rejects.toThrow(
      "An exception occurred."
    );
  });

  it("paginates by the request contract and rejects a changing total", async () => {
    const parsed = JSON.parse(await fixture("receipt-amendments-sanitized.json")) as {
      data: { items: unknown[]; totalItems: number; pageNumber: number; pageSize: number };
    };
    const pages = [parsed.data.items.slice(0, 3), parsed.data.items.slice(3)];
    const fetchImpl = vi.fn().mockImplementation(async (_url, init) => {
      const body = JSON.parse(String(init?.body)) as { pageNumber: number };
      return response(
        JSON.stringify({
          data: {
            items: pages[body.pageNumber - 1] ?? [],
            totalItems: 5,
            // The live service currently returns zero here; request paging is authoritative.
            pageNumber: 0,
            pageSize: 0,
          },
          succeeded: true,
          error: null,
        }),
        "application/json"
      );
    }) as unknown as typeof fetch;

    await expect(
      getAllNewHampshireReceipts(
        { filerName: "Example Committee", electionCycleId: 110, pageSize: 3 },
        { fetchImpl }
      )
    ).resolves.toHaveLength(5);
    expect(vi.mocked(fetchImpl).mock.calls.map((call) => JSON.parse(String(call[1]?.body)).pageNumber)).toEqual([1, 2]);

    const changingTotalFetch = vi.fn()
      .mockResolvedValueOnce(response(JSON.stringify({ data: { items: pages[0], totalItems: 5 }, succeeded: true }), "application/json"))
      .mockResolvedValueOnce(response(JSON.stringify({ data: { items: pages[1], totalItems: 6 }, succeeded: true }), "application/json")) as unknown as typeof fetch;
    await expect(
      getAllNewHampshireReceipts(
        { filerName: "Example Committee", electionCycleId: 110, pageSize: 3 },
        { fetchImpl: changingTotalFetch }
      )
    ).rejects.toThrow("totalItems changed during pagination");
  });

  it("selects max report versions and proves the sanitized bulk mismatch", async () => {
    const receiptJson = await fixture("receipt-amendments-sanitized.json");
    const fetchImpl = vi.fn().mockResolvedValue(response(receiptJson, "application/json")) as unknown as typeof fetch;
    const apiRows = (
      await getNewHampshireReceiptPage(
        { filerName: "Example Committee", electionCycleId: 110, pageSize: 200 },
        { fetchImpl }
      )
    ).items;
    const bulkRows = parseNewHampshireReceiptCsv(await fixture("receipts-sanitized.csv"));

    expect(selectCurrentNewHampshireReceiptReportVersions(apiRows).map((row) => row.transactionId)).toEqual([
      2001,
      2002,
      3001,
    ]);
    expect(
      reconcileNewHampshireAmendmentFixture({ bulkRows, apiRows, filingEntityId: 50450 })
    ).toEqual({
      bulk: { rowCount: 3, amountCents: 35_000 },
      apiAllVersions: { rowCount: 5, amountCents: 45_000 },
      apiCurrentVersions: { rowCount: 3, amountCents: 30_000 },
      amendedReportCount: 1,
      deltaCents: 5_000,
      status: "mismatch",
      strategy: "search_api_current_report_versions_required",
    });
  });

  it("separates support, oppose, and legally unusable blank-stance IE money", async () => {
    const ieJson = await fixture("independent-expenditures-sanitized.json");
    const fetchImpl = vi.fn().mockResolvedValue(response(ieJson, "application/json")) as unknown as typeof fetch;
    const rows = (
      await getNewHampshireIndependentExpenditurePage(
        { electionCycleId: 110, pageSize: 200 },
        { fetchImpl }
      )
    ).items;

    expect(summarizeNewHampshireIndependentExpenditures(rows, "2026 Election Cycle")).toEqual({
      rowCount: 3,
      support: { rowCount: 1, amountCents: 16_980 },
      oppose: { rowCount: 1, amountCents: 11_905 },
      blankStance: { rowCount: 1, amountCents: 5_000 },
      blankTargetCount: 1,
    });
  });
});
