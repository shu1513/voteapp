import { readFileSync } from "node:fs";
import { mkdtemp } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

import { beforeEach, describe, expect, it } from "vitest";

import {
  computeRhodeIslandCf2CycleTotals,
  findRhodeIslandCf2PeriodConflicts,
  selectRhodeIslandCf2CyclePeriods,
} from "../../../src/pipeline/rhodeIslandFinance/rhodeIslandCf2ReportSelector.js";
import { storeErtsArtifact } from "../../../src/pipeline/rhodeIslandFinance/rhodeIslandErtsArtifactCache.js";
import { ERTS_CF2_SUMMARY_LABELS } from "../../../src/pipeline/rhodeIslandFinance/rhodeIslandErtsParsers.js";
import { makeMinimalPdf } from "../../helpers/minimalPdf.js";

function fixture(name: string): string {
  return readFileSync(fileURLToPath(new URL(`../../fixtures/rhodeIslandFinance/${name}`, import.meta.url)), "utf8");
}

// The committed filing-list fixture holds four rows: one unfiled, one filed
// MPF-2, and two filed CF-2s — 239295 (07/01/2026-08/11/2026, unamended) and
// 230557 (10/01/2025-12/31/2025, amended; its version list is the committed
// fixture whose in-force GUID is c3881961-…).
const FILINGS_HTML = fixture("organization-filings.html");
const AMENDED_VERSIONS_HTML = fixture("filing-amendments-230557.html");
const IN_FORCE_230557_GUID = "c3881961-09d8-4b9c-8eda-4ead34a1f842";
const GUID_239295 = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";

const SINGLE_VERSION_239295_HTML = `
<table id="grdAmendments">
  <tr><td>Amendment</td><td>Desc</td><td>Date Filed</td><td></td></tr>
  <tr><td></td><td><a href="https://ricampaignfinance.com/ExportDocs/2235-RICF2-239295-${GUID_239295}.pdf">report</a></td><td>Aug 12 2026  7:48PM</td><td></td></tr>
</table>`;

/**
 * Build a CF-2-page-1-shaped PDF: each pinned label with its amount on the
 * same text baseline, to the right — the layout readErtsCf2SummaryValues is
 * pinned against.
 */
function cf2Pdf(values: Record<string, string>, omitLabels: readonly string[] = []): Uint8Array {
  const lines: { text: string; x: number; y: number }[] = [];
  let y = 700;
  for (const label of ERTS_CF2_SUMMARY_LABELS) {
    if (!omitLabels.includes(label)) {
      lines.push({ text: label, x: 48, y });
      lines.push({ text: values[label] ?? "0", x: 300, y });
    }
    y -= 20;
  }
  return makeMinimalPdf(lines);
}

// Real McKee Q4 2025 values, hand-read from the spike's in-force amendment
// PDF (cf2-230557-latest): receipts 1,235,145.87 - 1,019,993.37 = 215,152.50
// and disbursements 1,235,145.87 - 1,110,487.44 = 124,658.43, which matches
// the form's own "b. Campaign Expenses" line.
const Q4_2025_VALUES: Record<string, string> = {
  "1. Beginning Cash Balance": "$ 1,019,993.37",
  "2. Individuals": "202,028.63",
  "3. Political Parties": "0",
  "4. Political Action Committees": "13,250.00",
  "7. Interest Received": "3,373.87",
  "3. Total Cash": "1,235,145.87",
  "5. Ending Cash Balance": "$ 1,110,487.44",
  "6. Report of In-Kind Contributions": "31.00",
};

// Synthetic pre-primary period: receipts 10,000.00, disbursements 78,626.22.
const PRE_PRIMARY_2026_VALUES: Record<string, string> = {
  "1. Beginning Cash Balance": "668,626.22",
  "2. Individuals": "10,000.00",
  "3. Total Cash": "678,626.22",
  "5. Ending Cash Balance": "600,000.00",
};

let cacheDir: string;

async function installArtifacts(options: { pdf239295?: Uint8Array } = {}): Promise<void> {
  await storeErtsArtifact({
    cacheDir,
    key: { type: "organization_filings", orgId: "2235" },
    url: "https://www.ricampaignfinance.com/RIPublic/Filings.aspx",
    body: FILINGS_HTML,
  });
  await storeErtsArtifact({
    cacheDir,
    key: { type: "filing_versions", filingId: "230557" },
    url: "https://www.ricampaignfinance.com/RIPublic/FilingAmendmentSelect.aspx?X=T&FilingID=230557&FormName=RICF2",
    body: AMENDED_VERSIONS_HTML,
  });
  await storeErtsArtifact({
    cacheDir,
    key: { type: "filing_versions", filingId: "239295" },
    url: "https://www.ricampaignfinance.com/RIPublic/FilingAmendmentSelect.aspx?X=T&FilingID=239295&FormName=RICF2",
    body: SINGLE_VERSION_239295_HTML,
  });
  await storeErtsArtifact({
    cacheDir,
    key: { type: "filing_pdf", filingId: "230557", guid: IN_FORCE_230557_GUID },
    url: `https://ricampaignfinance.com/ExportDocs/2235-RICF2-230557-${IN_FORCE_230557_GUID}.pdf`,
    body: cf2Pdf(Q4_2025_VALUES),
  });
  await storeErtsArtifact({
    cacheDir,
    key: { type: "filing_pdf", filingId: "239295", guid: GUID_239295 },
    url: `https://ricampaignfinance.com/ExportDocs/2235-RICF2-239295-${GUID_239295}.pdf`,
    body: options.pdf239295 ?? cf2Pdf(PRE_PRIMARY_2026_VALUES),
  });
}

beforeEach(async () => {
  cacheDir = await mkdtemp(join(tmpdir(), "ri-cf2-selector-"));
});

describe("selectRhodeIslandCf2CyclePeriods", () => {
  it("selects both in-cycle CF-2 periods with the in-force version's arithmetic", async () => {
    await installArtifacts();
    const selection = await selectRhodeIslandCf2CyclePeriods({
      cacheDir,
      orgId: "2235",
      cycleBeginIso: "2025-01-01",
      cycleEndIso: "2026-12-31",
    });

    expect(selection.quarantineReasons).toEqual([]);
    expect(selection.filingSelection).toMatchObject({ unfiledRowCount: 1, nonCf2FiledRowCount: 1, outOfCycleRowCount: 0 });
    expect(selection.periods.map((period) => period.filingId)).toEqual(["230557", "239295"]);

    const q4 = selection.periods[0];
    // The amended family must be read from its LAST version row (the
    // in-force amendment), never the original.
    expect(q4).toMatchObject({
      beginIso: "2025-10-01",
      endIso: "2025-12-31",
      amendmentLabel: "Amended",
      versionCount: 2,
      beginningCashCents: 101_999_337,
      totalCashCents: 123_514_587,
      endingCashCents: 111_048_744,
      cashReceiptsCents: 21_515_250,
      disbursementsCents: 12_465_843,
    });
    expect(q4.pdfUrl).toContain(IN_FORCE_230557_GUID);
    expect(q4.values.get("2. Individuals")).toBe(20_202_863);
    expect(q4.values.get("6. Report of In-Kind Contributions")).toBe(3_100);

    expect(selection.periods[1]).toMatchObject({
      beginIso: "2026-07-01",
      endIso: "2026-08-11",
      versionCount: 1,
      cashReceiptsCents: 1_000_000,
      disbursementsCents: 7_862_622,
    });

    // Decision 2: sums across periods; cash on hand = latest period's ending.
    expect(selection.cycleTotals).toEqual({
      totalReceiptsCents: 22_515_250,
      totalDisbursementsCents: 20_328_465,
      cashOnHandCents: 60_000_000,
      cashOnHandAsOfIso: "2026-08-11",
    });
  });

  it("quarantines a PDF that fails to yield a pinned label instead of publishing partial arithmetic", async () => {
    await installArtifacts({ pdf239295: cf2Pdf(PRE_PRIMARY_2026_VALUES, ["3. Total Cash"]) });
    const selection = await selectRhodeIslandCf2CyclePeriods({
      cacheDir,
      orgId: "2235",
      cycleBeginIso: "2025-01-01",
      cycleEndIso: "2026-12-31",
    });
    expect(selection.quarantineReasons).toEqual([
      expect.objectContaining({ reason: "missing_cf2_label", detail: expect.stringContaining("3. Total Cash") }),
    ]);
    expect(selection.periods.map((period) => period.filingId)).toEqual(["230557"]);
    expect(selection.cycleTotals).toBeNull();
  });

  it("quarantines a period that straddles the cycle boundary — another cycle's money is never summed", async () => {
    await installArtifacts();
    const selection = await selectRhodeIslandCf2CyclePeriods({
      cacheDir,
      orgId: "2235",
      cycleBeginIso: "2025-01-01",
      cycleEndIso: "2026-07-15",
    });
    expect(selection.quarantineReasons).toEqual([
      expect.objectContaining({ reason: "period_outside_cycle", detail: expect.stringContaining("239295") }),
    ]);
    expect(selection.cycleTotals).toBeNull();
  });

  it("quarantines a filed CF-2 whose period is regex-shaped but not a calendar date", async () => {
    // "02/30/2026" passes the filing-list parser's MM/DD/YYYY digit pin but
    // fails the calendar round-trip, so the row lands in
    // unusablePeriodRowCount — publishing the surviving periods would
    // silently understate the cycle.
    const filingsWithBadDate =
      '<table id="grdSearchResults">' +
      "<tr><td>Report Type</td><td>Begin</td><td>End</td><td>Due</td><td>Report Status</td><td>Original Filed</td><td>Amended</td><td>&nbsp;</td></tr>" +
      "<tr><td>2026 On-Going Qrtly (1st)</td><td>02/30/2026</td><td>03/31/2026</td><td>04/30/2026</td>" +
      "<td>Received by BOE</td><td>Apr 30 2026  9:00AM</td><td>No</td>" +
      '<td><a href="https://secure.ricampaignfinance.com/RhodeIslandCF/Candidate/FilingAmendmentSelect.aspx?X=T&amp;FilingID=239999&amp;FormName=RICF2">View</a></td></tr>' +
      "</table>";
    await storeErtsArtifact({
      cacheDir,
      key: { type: "organization_filings", orgId: "2235" },
      url: "https://www.ricampaignfinance.com/RIPublic/Filings.aspx",
      body: filingsWithBadDate,
    });
    const selection = await selectRhodeIslandCf2CyclePeriods({
      cacheDir,
      orgId: "2235",
      cycleBeginIso: "2025-01-01",
      cycleEndIso: "2026-12-31",
    });
    expect(selection.filingSelection.unusablePeriodRowCount).toBe(1);
    expect(selection.quarantineReasons).toEqual([
      expect.objectContaining({ reason: "unusable_period_window", detail: expect.stringContaining("1 filed CF-2") }),
    ]);
    expect(selection.cycleTotals).toBeNull();
  });

  it("throws on a missing cached artifact — the sync must never aggregate without the full bundle", async () => {
    await storeErtsArtifact({
      cacheDir,
      key: { type: "organization_filings", orgId: "2235" },
      url: "https://www.ricampaignfinance.com/RIPublic/Filings.aspx",
      body: FILINGS_HTML,
    });
    await expect(
      selectRhodeIslandCf2CyclePeriods({
        cacheDir,
        orgId: "2235",
        cycleBeginIso: "2025-01-01",
        cycleEndIso: "2026-12-31",
      })
    ).rejects.toThrow(/missing/);
  });
});

describe("findRhodeIslandCf2PeriodConflicts", () => {
  it("reports duplicates and overlaps, and accepts adjacent disjoint periods", () => {
    const q3 = { filingId: "1", beginIso: "2025-07-01", endIso: "2025-09-30" };
    const q4 = { filingId: "2", beginIso: "2025-10-01", endIso: "2025-12-31" };
    expect(findRhodeIslandCf2PeriodConflicts([q3, q4])).toEqual([]);
    expect(findRhodeIslandCf2PeriodConflicts([q4, { ...q4, filingId: "3" }])).toEqual([
      expect.objectContaining({ reason: "duplicate_period_window" }),
    ]);
    expect(
      findRhodeIslandCf2PeriodConflicts([q3, { filingId: "4", beginIso: "2025-09-30", endIso: "2025-11-15" }])
    ).toEqual([expect.objectContaining({ reason: "overlapping_periods" })]);
  });
});

describe("computeRhodeIslandCf2CycleTotals", () => {
  it("returns null with no periods — a CF-5 deferral is not a zero", () => {
    expect(computeRhodeIslandCf2CycleTotals([])).toBeNull();
  });
});
