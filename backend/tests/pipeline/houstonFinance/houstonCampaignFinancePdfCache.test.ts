import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { cacheHoustonFinancePdf, readCachedHoustonFinancePdf, validateHoustonFinancePdf } from "../../../src/pipeline/houstonFinance/houstonCampaignFinancePdfCache.js";
import type { HoustonFinanceReportIndexRecord } from "../../../src/pipeline/houstonFinance/houstonFinanceTypes.js";

const dirs: string[] = [];
afterEach(async () => { await Promise.all(dirs.splice(0).map((dir) => rm(dir, { recursive: true, force: true }))); });
const report: HoustonFinanceReportIndexRecord = { sourceSystem: "ethics_efile", reportId: "28", filerId: "70", filerName: "Jane Doe", filerType: "COH", reportType: "SEMIJUL", receivedDate: "2026-01-01", filedAt: "2026-01-01", periodStart: null, periodEnd: null, officeDescription: "MAYOR", campaignYear: null, pdfUrl: "https://example.test/report.pdf" };

describe("Houston finance PDF cache", () => {
  it("writes and reads a validated PDF by immutable report identity", async () => {
    const dir = await mkdtemp(join(tmpdir(), "hou-finance-")); dirs.push(dir);
    const data = new TextEncoder().encode("%PDF-fixture");
    const path = await cacheHoustonFinancePdf(dir, report, data);
    expect(new Uint8Array(await readFile(path))).toEqual(data);
    expect(await readCachedHoustonFinancePdf(dir, report)).toEqual(data);
  });
  it("rejects non-PDF bytes", () => expect(() => validateHoustonFinancePdf(new TextEncoder().encode("html"))).toThrow("Invalid Houston"));
});
