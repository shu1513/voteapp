import { describe, expect, it, vi } from "vitest";

import {
  alabamaCycleWindowStart,
  createAlabamaCycleCoversLoader,
  parseAlabamaFilingDate,
} from "../../../src/pipeline/alabamaFinance/alabamaCommitteeCycleCovers.js";
import type { AlabamaCommitteeFilingRow } from "../../../src/pipeline/alabamaFinance/alabamaFcpaClient.js";

function filing(overrides: Partial<AlabamaCommitteeFilingRow>): AlabamaCommitteeFilingRow {
  return {
    ID: 1,
    DESCRIPTION: "2026 2026 ELECTION CYCLE Monthly Report",
    PERIODBEGIN: "08/01/2026",
    PERIODEND: "08/31/2026",
    FILEDDATE: "08/31/2026 10:42 AM",
    AMENDED: "No",
    ...overrides,
  };
}

function periodicHtml(amounts: {
  begin: string;
  itemizedCash: string;
  nonItemizedCash?: string;
  inKind?: string;
  other?: string;
  itemizedExp: string;
  end: string;
}): string {
  return `<html><body>
    <div>Beginning Balance $${amounts.begin}</div>
    <div>Itemized cash contributions $${amounts.itemizedCash}</div>
    <div>Non-itemized cash contributions $${amounts.nonItemizedCash ?? "0.00"}</div>
    <div>Non-itemized employee payroll contributions $0.00</div>
    <div>Itemized in-kind contributions $${amounts.inKind ?? "0.00"}</div>
    <div>Non-itemized in-kind contributions $0.00</div>
    <div>Itemized receipts from other sources $${amounts.other ?? "0.00"}</div>
    <div>Non-itemized receipts from other sources $0.00</div>
    <div>Itemized Expenditures $${amounts.itemizedExp}</div>
    <div>Non-itemized Expenditures $0.00</div>
    <div>Itemized Line of Credit Expenditures $0.00</div>
    <div>Non-itemized Line of Credit Expenditures $0.00</div>
    <div>Ending Balance $${amounts.end}</div>
  </body></html>`;
}

function majorHtml(cash: string): string {
  return `<html><body>
    <div>Beginning Balance $1,659,100.23</div>
    <div>Total Cash Contribution $${cash}</div>
    <div>Total In-Kind Contributions $0.00</div>
    <div>Total Receipt from Other Sources $0.00</div>
  </body></html>`;
}

describe("alabamaCycleWindowStart / parseAlabamaFilingDate", () => {
  it("opens the window on January 1 of the first term year", () => {
    expect(alabamaCycleWindowStart(2026, 4)).toBe("2023-01-01");
    expect(alabamaCycleWindowStart(2026, 6)).toBe("2021-01-01");
    expect(() => alabamaCycleWindowStart(2026, 0)).toThrow("invalid cycle window");
  });

  it("parses portal MM/DD/YYYY dates with or without a time", () => {
    expect(parseAlabamaFilingDate("01/09/2023 11:55 AM")).toBe("2023-01-09");
    expect(parseAlabamaFilingDate("12/31/2024")).toBe("2024-12-31");
    expect(parseAlabamaFilingDate(null)).toBeNull();
    expect(parseAlabamaFilingDate("2024-12-31")).toBeNull();
    expect(parseAlabamaFilingDate("13/01/2024")).toBeNull();
  });
});

describe("createAlabamaCycleCoversLoader", () => {
  it("sums only the reports whose period begins inside the window, majors included", async () => {
    // Stutts-shaped history: a 2022-election report and the 2022 annual report
    // sit outside the 2023-01-01 window; the 2023 annual, a 2026 monthly and
    // a major-contribution report sit inside it.
    const filings = [
      filing({ ID: 5, DESCRIPTION: "2026 2026 ELECTION CYCLE Monthly Report", PERIODBEGIN: "08/01/2026", PERIODEND: "08/31/2026" }),
      filing({ ID: 4, DESCRIPTION: " Major Contribution Report", PERIODBEGIN: "02/24/2026", PERIODEND: "02/24/2026" }),
      filing({ ID: 3, DESCRIPTION: "Annual Report", PERIODBEGIN: "01/01/2023", PERIODEND: "12/31/2023" }),
      filing({ ID: 2, DESCRIPTION: "Annual Report", PERIODBEGIN: "01/01/2022", PERIODEND: "12/31/2022" }),
      filing({ ID: 1, DESCRIPTION: "2022 ELECTION Weekly Report", PERIODBEGIN: "10/22/2022", PERIODEND: "10/28/2022" }),
    ];
    const html: Record<number, string> = {
      5: periodicHtml({ begin: "175,835.33", itemizedCash: "31,752.00", inKind: "2,173.91", itemizedExp: "25,170.15", end: "182,417.18" }),
      4: majorHtml("20,000.00"),
      3: periodicHtml({ begin: "169,975.55", itemizedCash: "0.00", itemizedExp: "98,069.55", end: "71,906.00" }),
      2: periodicHtml({ begin: "1.00", itemizedCash: "9,000.00", itemizedExp: "2,425.00", end: "6,576.00" }),
      1: periodicHtml({ begin: "1.00", itemizedCash: "99,600.00", itemizedExp: "74,568.14", end: "1.00" }),
    };
    const fetchCoverHtml = vi.fn(async (id: number) => html[id]!);
    const load = createAlabamaCycleCoversLoader({
      fetchFilings: vi.fn(async () => filings),
      fetchCoverHtml,
      retryDelayMs: 0,
    });
    const covers = await load(870, "2023-01-01");
    expect(covers).toEqual({
      windowStart: "2023-01-01",
      filingCount: 5,
      windowFilingCount: 3,
      cashCents: 31_752_00 + 20_000_00,
      inKindCents: 2_173_91,
      otherCents: 0,
      expenditureCents: 25_170_15 + 98_069_55,
      openingBalanceCents: 169_975_55,
      latestEndingBalanceCents: 182_417_18,
    });
    expect(fetchCoverHtml.mock.calls.map((call) => call[0])).toEqual([5, 4, 3]);
  });

  it("seeds the opening balance from the earliest window filing of any kind", async () => {
    // Robertson-shaped: majors filed before the first monthly report are
    // already inside that report's beginning balance, so the identity must
    // open from the first major (balance 0), not the first periodic.
    // The first major is dated INSIDE the monthly's period (06/30 vs
    // 06/01-06/30) yet filed first — period end + id ordering puts it first.
    const filings = [
      filing({ ID: 30, DESCRIPTION: "2026 ELECTION CYCLE Monthly Report", PERIODBEGIN: "06/01/2025", PERIODEND: "06/30/2025" }),
      filing({ ID: 20, DESCRIPTION: " Major Contribution Report", PERIODBEGIN: "06/30/2025", PERIODEND: "06/30/2025" }),
      filing({ ID: 10, DESCRIPTION: " Major Contribution Report", PERIODBEGIN: "06/30/2025", PERIODEND: "06/30/2025" }),
    ];
    const html: Record<number, string> = {
      30: periodicHtml({ begin: "50,000.00", itemizedCash: "1,000.00", itemizedExp: "100.00", end: "50,900.00" }),
      20: majorHtml("25,000.00").replace("$1,659,100.23", "$25,000.00"),
      10: majorHtml("25,000.00").replace("$1,659,100.23", "$0.00"),
    };
    const load = createAlabamaCycleCoversLoader({
      fetchFilings: vi.fn(async () => filings),
      fetchCoverHtml: vi.fn(async (id: number) => html[id]!),
      retryDelayMs: 0,
    });
    const covers = await load(6833, "2023-01-01");
    expect(covers).toMatchObject({
      windowFilingCount: 3,
      cashCents: 51_000_00,
      expenditureCents: 100_00,
      openingBalanceCents: 0,
      latestEndingBalanceCents: 50_900_00,
    });
    // 0 + 51,000 - 100 = 50,900 = the latest ending balance.
    expect(covers.openingBalanceCents! + covers.cashCents - covers.expenditureCents).toBe(
      covers.latestEndingBalanceCents
    );
  });

  it("retries a short-read filings list, then fails closed", async () => {
    let calls = 0;
    const load = createAlabamaCycleCoversLoader({
      fetchFilings: vi.fn(async () => {
        calls += 1;
        if (calls === 1) throw new Error("returned 16 of 17 rows");
        return [];
      }),
      fetchCoverHtml: vi.fn(),
      retryDelayMs: 0,
    });
    await expect(load(1, "2023-01-01")).resolves.toMatchObject({ filingCount: 0, windowFilingCount: 0 });
    expect(calls).toBe(2);
    const dead = createAlabamaCycleCoversLoader({
      fetchFilings: vi.fn(async () => {
        throw new Error("returned 16 of 17 rows");
      }),
      fetchCoverHtml: vi.fn(),
      coverAttempts: 2,
      retryDelayMs: 0,
    });
    await expect(dead(1, "2023-01-01")).rejects.toThrow("filings for committee 1 unavailable after 2 attempts");
  });

  it("retries a flaky filing-detail page, then fails closed", async () => {
    const filings = [filing({ ID: 9 })];
    let calls = 0;
    const flaky = createAlabamaCycleCoversLoader({
      fetchFilings: vi.fn(async () => filings),
      fetchCoverHtml: vi.fn(async () => {
        calls += 1;
        if (calls < 3) throw new Error("Filing detail 9 returned a System Exception page");
        return periodicHtml({ begin: "0.00", itemizedCash: "1.00", itemizedExp: "0.00", end: "1.00" });
      }),
      retryDelayMs: 0,
    });
    await expect(flaky(1, "2023-01-01")).resolves.toMatchObject({ cashCents: 100, windowFilingCount: 1 });
    expect(calls).toBe(3);

    const dead = createAlabamaCycleCoversLoader({
      fetchFilings: vi.fn(async () => filings),
      fetchCoverHtml: vi.fn(async () => {
        throw new Error("System Exception");
      }),
      coverAttempts: 2,
      retryDelayMs: 0,
    });
    await expect(dead(1, "2023-01-01")).rejects.toThrow("filing 9 cover unavailable after 2 attempts");
  });

  it("fails closed on a filing that cannot be placed in time", async () => {
    const load = createAlabamaCycleCoversLoader({
      fetchFilings: vi.fn(async () => [filing({ ID: 7, PERIODBEGIN: null, FILEDDATE: null })]),
      fetchCoverHtml: vi.fn(),
      retryDelayMs: 0,
    });
    await expect(load(1, "2023-01-01")).rejects.toThrow("filing 7");
  });
});
