import { describe, expect, it, vi } from "vitest";

import { reconcileKansasCoverArithmetic } from "../../../src/pipeline/kansasFinance/kansasCfrViewerParsers.js";
import { kansasCfrOfficeForRace } from "../../../src/pipeline/kansasFinance/kansasFinanceEligibleOffices.js";
import type { KansasKpdcCandidateRow } from "../../../src/pipeline/kansasFinance/kansasKpdcIndexClient.js";
import {
  kansasNumericTextToCents,
  kansasPaperCoverOverridesToCovers,
  loadKansasPaperCoverOverrides,
  type KansasPaperCoverOverride,
} from "../../../src/pipeline/kansasFinance/kansasPaperCoverOverrides.js";
import { buildKansasPaperInventory } from "../../../src/pipeline/kansasFinance/kansasPaperInventory.js";
import { kansasFilingHeaderKey, kansasReportingPeriods } from "../../../src/pipeline/kansasFinance/kansasReportInventory.js";

const HOUSE = kansasCfrOfficeForRace({ officeScope: "state_lower", officeCanonicalName: "State Lower Chamber Legislator" })!;
const periods = kansasReportingPeriods(HOUSE, 2026);
const RECIPE = "7:58:SAMPLE:ALEX";
const TREE = "https://www.kansas.gov/ethics/CFAScanned/House/2026ElecCycle";

// A synthetic filer; the amounts reconcile (1 + 2 = 3, 3 - 4 = 5).
function transcribed(fileName: string, overrides: Partial<KansasPaperCoverOverride> = {}): KansasPaperCoverOverride {
  return {
    committeeId: RECIPE,
    electionYear: 2026,
    sourceFileName: fileName,
    sourceUrl: `${TREE}/${fileName}`,
    cashBeginningCents: 10_000,
    totalContributionsCents: 50_000,
    cashAvailableCents: 60_000,
    totalExpendituresCents: 5_000,
    cashCloseCents: 55_000,
    inKindCents: 2_500,
    otherTransactionsCents: null,
    ...overrides,
  };
}

const treeRow = (fileNames: string[]): KansasKpdcCandidateRow => ({
  district: 58,
  filedName: "Sample, Alex",
  links: fileNames.map((fileName) => ({ url: `${TREE}/${fileName}`, fileName, linkText: fileName })),
});

describe("kansasPaperCoverOverridesToCovers", () => {
  it("carries the paper inventory's header for the same filename, so it matches the ledger's version and only that", () => {
    const fileNames = ["H058AS_202601.pdf", "H058AS_202607.pdf", "H058AS_amend202607.pdf", "H058AS_2amend2607.pdf", "H058AS_Term2610.pdf"];
    const inventory = buildKansasPaperInventory({
      candidateName: "SAMPLE, ALEX",
      districtNumber: 58,
      office: HOUSE,
      periods,
      windowStart: "2025-01-01",
      rows: [treeRow(fileNames)],
      efileFilings: [],
    });
    expect(inventory.status).toBe("resolved");
    const inventoryKeys = inventory.status === "resolved" ? inventory.headers.map(kansasFilingHeaderKey) : [];

    const covers = kansasPaperCoverOverridesToCovers({ overrides: fileNames.map((fileName) => transcribed(fileName)), periods });
    expect(covers.map((cover) => kansasFilingHeaderKey(cover.header))).toEqual(inventoryKeys);
    expect(new Set(inventoryKeys).size).toBe(fileNames.length);

    const [annual, original, amended, second, termination] = covers;
    expect(annual!.header).toMatchObject({ periodStart: "2025-01-01", periodEnd: "2025-12-31", fileDate: null, amended: false, amendmentOrdinal: null, channel: "paper" });
    expect(original!.header).toMatchObject({ periodStart: "2026-01-01", periodEnd: "2026-07-23", amended: false, termination: false });
    expect(amended!.header).toMatchObject({ periodStart: "2026-01-01", amended: true, amendmentOrdinal: 1 });
    expect(second!.header).toMatchObject({ periodStart: "2026-01-01", amended: true, amendmentOrdinal: 2 });
    expect(termination!.header).toMatchObject({ periodStart: "2026-07-24", termination: true, amended: false });
    for (const cover of covers) {
      expect(cover.scheduleA).toBeNull();
      expect(cover.scheduleB).toBeNull();
      expect(reconcileKansasCoverArithmetic(cover.cover)).toBe(true);
      expect(cover.cover).toMatchObject({
        periodStart: cover.header.periodStart,
        periodEnd: cover.header.periodEnd,
        amended: cover.header.amended,
        termination: cover.header.termination,
        electronicallyFiledOn: null,
        cashBeginningCents: 10_000,
        totalContributionsCents: 50_000,
        cashAvailableCents: 60_000,
        totalExpendituresCents: 5_000,
        cashCloseCents: 55_000,
        inKindCents: 2_500,
        otherTransactionsCents: null,
      });
    }
  });

  it("throws on a filename that is not a report of a required period", () => {
    const convert = (fileName: string) => () => kansasPaperCoverOverridesToCovers({ overrides: [transcribed(fileName)], periods });
    expect(convert("H058AS_AT.pdf")).toThrow("transcribed cover H058AS_AT.pdf: not a report of a required period (appointment_of_treasurer)");
    expect(convert("H058AS_2026PLF.pdf")).toThrow("not a report of a required period (last_minute)");
    expect(convert("H058AS_Aff2607.pdf")).toThrow("not a report of a required period (affidavit due 202607)");
    expect(convert("H058AS_202401.pdf")).toThrow("not a report of a required period (report due 202401)");
    expect(convert("scan.pdf")).toThrow("not a report of a required period (unknown)");
    expect(kansasPaperCoverOverridesToCovers({ overrides: [], periods })).toEqual([]);
  });

  it("passes transcribed figures through untouched, arithmetic included (the aggregator checks it)", () => {
    const [cover] = kansasPaperCoverOverridesToCovers({
      overrides: [transcribed("H058AS_202607.pdf", { cashCloseCents: 55_001, otherTransactionsCents: -100 })],
      periods,
    });
    expect(cover!.cover).toMatchObject({ cashCloseCents: 55_001, otherTransactionsCents: -100 });
    expect(reconcileKansasCoverArithmetic(cover!.cover)).toBe(false);
  });
});

describe("kansasNumericTextToCents", () => {
  it("reads numeric(16,2) text exactly and refuses anything else", () => {
    expect(kansasNumericTextToCents("1234.50", "x")).toBe(123_450);
    expect(kansasNumericTextToCents("0.00", "x")).toBe(0);
    expect(kansasNumericTextToCents("-5.00", "x")).toBe(-500);
    for (const bad of [1234.5, "1,234.50", "1234.5", "12.345", "$1.00", "", null, undefined]) {
      expect(() => kansasNumericTextToCents(bad, "line 2")).toThrow("line 2: expected a numeric(16,2) text value");
    }
  });
});

describe("loadKansasPaperCoverOverrides", () => {
  it("selects the link's cycle rows with amounts cast to text and maps them to cents", async () => {
    const db = {
      query: vi.fn(async () => ({
        rows: [
          {
            committee_id: RECIPE,
            election_year: 2026,
            source_file_name: "H058AS_202607.pdf",
            source_url: `${TREE}/H058AS_202607.pdf`,
            cash_beginning: "100.00",
            total_contributions: "500.00",
            cash_available: "600.00",
            total_expenditures: "50.00",
            cash_close: "550.00",
            in_kind: "25.00",
            other_transactions: null,
          },
        ],
        rowCount: 1,
      })),
    };
    const rows = await loadKansasPaperCoverOverrides(db as never, RECIPE, 2026);
    const [sql, params] = db.query.mock.calls[0] as unknown as [string, unknown[]];
    expect(sql).toContain("FROM public.ks_candidate_finance_paper_covers");
    expect(sql).toContain("cash_beginning::text");
    expect(sql).toContain("WHERE committee_id = $1 AND election_year = $2");
    expect(params).toEqual([RECIPE, 2026]);
    expect(rows).toEqual([transcribed("H058AS_202607.pdf")]);
  });

  it("fails closed on an amount the driver did not hand back as numeric text", async () => {
    const db = { query: vi.fn(async () => ({ rows: [{ source_file_name: "H058AS_202607.pdf", cash_beginning: 100 }], rowCount: 1 })) };
    await expect(loadKansasPaperCoverOverrides(db as never, RECIPE, 2026)).rejects.toThrow("H058AS_202607.pdf cash_beginning: expected a numeric(16,2) text value, got 100");
  });
});
