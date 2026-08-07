import { readFileSync } from "node:fs";
import { mkdtemp } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

import { beforeEach, describe, expect, it } from "vitest";

import {
  acquireNcsbeCommitteeArtifacts,
  acquireNcsbeCycleArtifacts,
  selectNcsbeCycleReportRows,
} from "../../../src/pipeline/northCarolinaFinance/northCarolinaNcsbeArtifactAcquisition.js";
import { getNcsbeArtifactStatus } from "../../../src/pipeline/northCarolinaFinance/northCarolinaNcsbeArtifactCache.js";
import type { NcsbeTransport } from "../../../src/pipeline/northCarolinaFinance/northCarolinaNcsbeClient.js";
import {
  parseNcsbeDate,
  type NcsbeDocumentRow,
} from "../../../src/pipeline/northCarolinaFinance/northCarolinaNcsbeParsers.js";

const NOW = new Date("2026-08-07T17:00:00Z");

function fixture(name: string): string {
  return readFileSync(
    fileURLToPath(new URL(`../../fixtures/northCarolinaFinance/${name}`, import.meta.url)),
    "utf8"
  );
}

function documentRow(overrides: Partial<NcsbeDocumentRow>): NcsbeDocumentRow {
  return {
    committeeName: "TEST COMMITTEE",
    sboeId: "STA-TEST00-C-001",
    reportYear: 2026,
    documentType: "Disclosure Report",
    reportType: "First Quarter",
    isAmendment: false,
    imageReceiptDate: parseNcsbeDate("02/24/2026"),
    dataImportDate: parseNcsbeDate("02/24/2026"),
    periodStartDate: parseNcsbeDate("01/01/2026"),
    periodEndDate: parseNcsbeDate("02/14/2026"),
    dataLink: "1",
    imageLink: null,
    ...overrides,
  };
}

describe("selectNcsbeCycleReportRows", () => {
  it("selects structured disclosure reports overlapping the Y-1..Y window", () => {
    const inWindow = documentRow({ dataLink: "10" });
    const priorCycle = documentRow({
      dataLink: "11",
      periodStartDate: parseNcsbeDate("01/01/2023"),
      periodEndDate: parseNcsbeDate("06/30/2023"),
    });
    // Straddles the window boundary — one overlapping day is enough.
    const straddling = documentRow({
      dataLink: "12",
      periodStartDate: parseNcsbeDate("07/01/2024"),
      periodEndDate: parseNcsbeDate("01/01/2025"),
    });
    const imageOnly = documentRow({ dataLink: null });
    const correspondence = documentRow({ dataLink: "13", documentType: "Committee Correspondence - Incoming" });
    const { selected, unusablePeriodRowCount } = selectNcsbeCycleReportRows({
      rows: [inWindow, priorCycle, straddling, imageOnly, correspondence],
      cycleYear: 2026,
    });
    expect(selected.map((row) => row.dataLink)).toEqual(["10", "12"]);
    expect(unusablePeriodRowCount).toBe(0);
  });

  it("includes and counts rows whose period bounds are missing or implausible", () => {
    const missing = documentRow({ dataLink: "20", periodStartDate: parseNcsbeDate("") });
    // The live year-3026 landmine: a typo must widen the fetch, not narrow it.
    const landmine = documentRow({ dataLink: "21", periodEndDate: parseNcsbeDate("06/01/3026") });
    const { selected, unusablePeriodRowCount } = selectNcsbeCycleReportRows({
      rows: [missing, landmine],
      cycleYear: 2026,
    });
    expect(selected.map((row) => row.dataLink)).toEqual(["20", "21"]);
    expect(unusablePeriodRowCount).toBe(2);
  });
});

// Fake portal: serves the real Gadson fixtures for every discovered report id.
function fakeGadsonTransport(state: { requests: string[] }, overrides: Record<string, string> = {}): NcsbeTransport {
  return {
    fetchText: async (url: string) => {
      state.requests.push(url);
      if (url in overrides) {
        return overrides[url]!;
      }
      if (url.includes("/DocumentGeneralResult/")) {
        return fixture("document-inventory-gadson.html");
      }
      if (url.includes("/ReportDetail/")) {
        return fixture("report-cover-gadson-229931.html");
      }
      if (url.includes("GetReceipts")) {
        return fixture("receipts-gadson-229931-p0.json");
      }
      if (url.includes("GetExpenditures")) {
        return fixture("ie-expenditures-carolina-federation-p0.json");
      }
      if (url.includes("/CFDocLkup/DocumentResult/")) {
        return fixture("ie-doc-type-inventory-2026.html");
      }
      throw new Error(`Unexpected URL in fake transport: ${url}`);
    },
  };
}

let cacheDir: string;

beforeEach(async () => {
  cacheDir = await mkdtemp(join(tmpdir(), "ncsbe-acquisition-"));
});

const GADSON = { sboeId: "STA-JV516O-C-001", orgGroupId: 57190 };

describe("acquireNcsbeCommitteeArtifacts", () => {
  it("discovers reports from the inventory and installs cover + transaction artifacts", async () => {
    const state = { requests: [] as string[] };
    const result = await acquireNcsbeCommitteeArtifacts({
      transport: fakeGadsonTransport(state),
      cacheDir,
      cycleYear: 2026,
      committee: GADSON,
      retrievedAt: NOW,
    });

    expect(result.inventoryRowCount).toBe(10);
    // 229931 (Q1), 227042 (YE semi-annual), 226297 (Organizational) all
    // overlap 2025–2026.
    expect(result.selectedReportCount).toBe(3);
    expect(result.fetched.map((report) => report.reportId).sort()).toEqual(["226297", "227042", "229931"]);
    expect(result.failures).toEqual([]);
    // Per report: 1 cover + 1 receipts page + 1 expenditures page.
    expect(result.fetched.every((report) => report.requestCount === 3)).toBe(true);

    const inventoryStatus = await getNcsbeArtifactStatus({
      cacheDir,
      key: { type: "document_inventory", sboeId: GADSON.sboeId },
    });
    expect(inventoryStatus.status).toBe("ready");
    const coverStatus = await getNcsbeArtifactStatus({ cacheDir, key: { type: "report_cover", reportId: "229931" } });
    expect(coverStatus.status).toBe("ready");
    expect(coverStatus.manifest?.sourceDocument?.dataImportDate).toBe("02/24/2026");
    const receiptsStatus = await getNcsbeArtifactStatus({
      cacheDir,
      key: { type: "report_transactions", reportId: "229931", kind: "receipts", page: 0 },
    });
    expect(receiptsStatus.status).toBe("ready");
    expect(receiptsStatus.manifest?.recordCountKey).toBe(19);
  });

  it("skips fully cached reports while the inventory's DataImportDate is unchanged", async () => {
    const first = { requests: [] as string[] };
    await acquireNcsbeCommitteeArtifacts({
      transport: fakeGadsonTransport(first),
      cacheDir,
      cycleYear: 2026,
      committee: GADSON,
      retrievedAt: NOW,
    });

    const second = { requests: [] as string[] };
    const result = await acquireNcsbeCommitteeArtifacts({
      transport: fakeGadsonTransport(second),
      cacheDir,
      cycleYear: 2026,
      committee: GADSON,
      retrievedAt: NOW,
    });
    expect(result.fetched).toEqual([]);
    expect(result.skippedReportIds.sort()).toEqual(["226297", "227042", "229931"]);
    // Only the inventory itself was re-fetched.
    expect(second.requests).toHaveLength(1);
    expect(second.requests[0]).toContain("/DocumentGeneralResult/");
  });

  it("re-fetches a report when the inventory shows a new DataImportDate", async () => {
    const first = { requests: [] as string[] };
    await acquireNcsbeCommitteeArtifacts({
      transport: fakeGadsonTransport(first),
      cacheDir,
      cycleYear: 2026,
      committee: GADSON,
      retrievedAt: NOW,
    });

    const inventoryUrl = first.requests[0]!;
    const reimported = fixture("document-inventory-gadson.html").replace(
      '"DataImportDate":"02/24/2026"',
      '"DataImportDate":"03/01/2026"'
    );
    const second = { requests: [] as string[] };
    const result = await acquireNcsbeCommitteeArtifacts({
      transport: fakeGadsonTransport(second, { [inventoryUrl]: reimported }),
      cacheDir,
      cycleYear: 2026,
      committee: GADSON,
      retrievedAt: NOW,
    });
    expect(result.fetched.map((report) => report.reportId)).toEqual(["229931"]);
    expect(result.skippedReportIds.sort()).toEqual(["226297", "227042"]);
  });

  it("force re-fetches everything", async () => {
    const first = { requests: [] as string[] };
    await acquireNcsbeCommitteeArtifacts({
      transport: fakeGadsonTransport(first),
      cacheDir,
      cycleYear: 2026,
      committee: GADSON,
      retrievedAt: NOW,
    });
    const second = { requests: [] as string[] };
    const result = await acquireNcsbeCommitteeArtifacts({
      transport: fakeGadsonTransport(second),
      cacheDir,
      cycleYear: 2026,
      committee: GADSON,
      force: true,
      retrievedAt: NOW,
    });
    expect(result.fetched).toHaveLength(3);
    expect(result.skippedReportIds).toEqual([]);
  });

  it("isolates a failing report and keeps fetching the others", async () => {
    const state = { requests: [] as string[] };
    const transport = fakeGadsonTransport(state, {
      "https://cf.ncsbe.gov/CFOrgLkup/ReportDetail/?RID=227042&TP=ALL": "<html>An error occurred</html>",
    });
    const result = await acquireNcsbeCommitteeArtifacts({
      transport,
      cacheDir,
      cycleYear: 2026,
      committee: GADSON,
      retrievedAt: NOW,
    });
    expect(result.fetched.map((report) => report.reportId).sort()).toEqual(["226297", "229931"]);
    expect(result.failures).toHaveLength(1);
    expect(result.failures[0]).toMatchObject({ reportId: "227042" });
    const coverStatus = await getNcsbeArtifactStatus({ cacheDir, key: { type: "report_cover", reportId: "227042" } });
    expect(coverStatus.status).toBe("missing");
  });
});

describe("acquireNcsbeCycleArtifacts", () => {
  it("runs committees and both IE inventory years, deduplicating report ids", async () => {
    const state = { requests: [] as string[] };
    const result = await acquireNcsbeCycleArtifacts({
      transport: fakeGadsonTransport(state),
      cacheDir,
      cycleYear: 2026,
      committees: [GADSON],
      retrievedAt: NOW,
    });

    expect(result.committees).toHaveLength(1);
    expect(result.committeeFailures).toEqual([]);
    expect(result.ie).not.toBeNull();
    // The fake portal serves the same 2026 inventory for both years, so the
    // 72 structured reports appear twice and must be fetched once.
    expect(result.ie!.years).toEqual([2025, 2026]);
    expect(result.ie!.inventoryRowCount).toBe(190);
    expect(result.ie!.structuredReportCount).toBe(144);
    expect(result.ie!.imageOnlyReportCount).toBe(46);
    expect(result.ie!.fetched).toHaveLength(72);
    expect(result.ie!.failures).toEqual([]);

    for (const year of [2025, 2026]) {
      const status = await getNcsbeArtifactStatus({ cacheDir, key: { type: "ie_doc_type_inventory", year } });
      expect(status.status).toBe("ready");
    }
  });

  it("isolates a committee whose inventory fetch fails", async () => {
    const state = { requests: [] as string[] };
    const transport = fakeGadsonTransport(state, {
      "https://cf.ncsbe.gov/CFOrgLkup/DocumentGeneralResult/?OGID=99999&SID=STA-BROKEN-C-001":
        "<html>An error occurred</html>",
    });
    const result = await acquireNcsbeCycleArtifacts({
      transport,
      cacheDir,
      cycleYear: 2026,
      committees: [{ sboeId: "STA-BROKEN-C-001", orgGroupId: 99999 }, GADSON],
      includeIe: false,
      retrievedAt: NOW,
    });
    expect(result.committeeFailures).toHaveLength(1);
    expect(result.committeeFailures[0]).toMatchObject({ sboeId: "STA-BROKEN-C-001" });
    expect(result.committees).toHaveLength(1);
    expect(result.committees[0]?.sboeId).toBe(GADSON.sboeId);
    expect(result.ie).toBeNull();
  });
});
