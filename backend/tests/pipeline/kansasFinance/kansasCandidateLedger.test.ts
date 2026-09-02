import { describe, expect, it, vi } from "vitest";

import type { KansasFilerFilingKind } from "../../../src/pipeline/kansasFinance/kansasCandidateFilerResolver.js";
import { buildKansasCandidateLedger } from "../../../src/pipeline/kansasFinance/kansasCandidateLedger.js";
import type { KansasCfrGridRow } from "../../../src/pipeline/kansasFinance/kansasCfrViewerParsers.js";
import { kansasCfrOfficeForRace } from "../../../src/pipeline/kansasFinance/kansasFinanceEligibleOffices.js";
import type { KansasPooledFiling } from "../../../src/pipeline/kansasFinance/kansasFilingSearch.js";

const NOW = new Date("2026-09-02T12:00:00.000Z");
const COVER_URL = "https://sos.ks.gov/elections/cfr_viewer/reports/exp_report_main.aspx";
const house = kansasCfrOfficeForRace({ officeScope: "state_lower", officeCanonicalName: "State Lower Chamber Legislator" })!;
const governor = kansasCfrOfficeForRace({ officeScope: "statewide", officeCanonicalName: "Governor" })!;

// Synthetic filer names on purpose (25-4154(d) posture).
function gridRow(overrides: Partial<KansasCfrGridRow>): KansasCfrGridRow {
  return {
    index: 0,
    fileDate: "07/27/2026",
    amendmentDate: "",
    amendmentNo: "",
    name: "HOLLOWAY MARGARET",
    officeSought: "State Representative",
    district: "85",
    channel: "efile",
    postbackTarget: "grdviewCfrResults$ctl02$lnkbtnName",
    ...overrides,
  };
}

function coverHtml(input: { start: string; end: string; amended?: boolean; termination?: boolean }): string {
  return `
    <span id="lblCandOrgName">Margaret Holloway</span>
    <span id="lblFileStartDate">${input.start}</span>
    <span id="lblFileEndDate">${input.end}</span>
    <input id="chkAmended" type="checkbox" ${input.amended ? "checked" : ""} disabled />
    <input id="chkTermination" type="checkbox" ${input.termination ? "checked" : ""} disabled />
    <span id="lblCashBeginning">$0.00</span>`;
}

type Fixture = {
  row: Partial<KansasCfrGridRow>;
  filingKind?: KansasFilerFilingKind;
  cover?: Parameters<typeof coverHtml>[0];
  landedUrl?: string;
};

function filing(fixture: Fixture): KansasPooledFiling & { openReport: ReturnType<typeof vi.fn> } {
  const row = gridRow(fixture.row);
  const openReport = vi.fn(async () => ({
    url: fixture.landedUrl ?? COVER_URL,
    html: fixture.cover ? coverHtml(fixture.cover) : "",
    hiddenFields: {},
  }));
  return { row, filingKind: fixture.filingKind ?? "report", openReport };
}

function poolOf(filings: KansasPooledFiling[]) {
  return vi.fn(async () => filings);
}

const houseTarget = { committeeId: "7:85:HOLLOWAY:MARGARET", office: house, electionYear: 2026 };

describe("buildKansasCandidateLedger", () => {
  it("opens every e-filed report of the recipe's filer and builds a complete ledger", async () => {
    const annual = filing({ row: { fileDate: "01/09/2026" }, cover: { start: "1/1/2025", end: "12/31/2025" } });
    const prePrimary = filing({ row: { index: 1, name: "HOLLOWAY MARGARET A" }, cover: { start: "1/1/2026", end: "7/23/2026" } });
    const otherDistrict = filing({ row: { index: 2, district: "86" }, cover: { start: "1/1/2026", end: "7/23/2026" } });
    const otherPerson = filing({ row: { index: 3, name: "HOLLOWAY DANIEL" }, cover: { start: "1/1/2026", end: "7/23/2026" } });
    const loadFilingPool = poolOf([annual, prePrimary, otherDistrict, otherPerson]);

    const result = await buildKansasCandidateLedger({ target: houseTarget, now: NOW, loadFilingPool });

    expect(loadFilingPool).toHaveBeenCalledWith(house, 2026, undefined);
    expect(result.status).toBe("resolved");
    if (result.status !== "resolved") return;
    expect(result.match.filedNames).toEqual(["HOLLOWAY MARGARET", "HOLLOWAY MARGARET A"]);
    expect(result.reports.map((report) => report.header)).toEqual([
      {
        periodStart: "1/1/2025",
        periodEnd: "12/31/2025",
        fileDate: "01/09/2026",
        amendmentDate: null,
        amended: false,
        termination: false,
        channel: "efile",
      },
      {
        periodStart: "1/1/2026",
        periodEnd: "7/23/2026",
        fileDate: "07/27/2026",
        amendmentDate: null,
        amended: false,
        termination: false,
        channel: "efile",
      },
    ]);
    expect(otherDistrict.openReport).not.toHaveBeenCalled();
    expect(otherPerson.openReport).not.toHaveBeenCalled();
    expect(result.ledger.entries.map((entry) => [entry.period.key, entry.status])).toEqual([
      ["2025-annual", "report_filed"],
      ["2026-pre_primary", "report_filed"],
      ["2026-pre_general", "not_yet_due"],
      ["2026-post_general", "not_yet_due"],
    ]);
    expect(result.paperReports).toEqual([]);
    expect(result.complete).toBe(true);
  });

  it("feeds appointment and affidavit rows to the ledger and passes grid amendment dates through", async () => {
    const appointment = filing({ row: { fileDate: "06/09/2026", officeSought: "STATE REPRESENTATIVE" }, filingKind: "appointment_of_treasurer" });
    const affidavit = filing({ row: { fileDate: "07/26/2026", officeSought: "STATE REPRESENTATIVE" }, filingKind: "affidavit" });
    const original = filing({ row: { index: 1, fileDate: "07/27/2026" }, cover: { start: "1/1/2026", end: "7/23/2026" } });
    const amendment = filing({
      row: { index: 2, fileDate: "07/27/2026", amendmentDate: "08/06/2026" },
      cover: { start: "1/1/2026", end: "7/23/2026", amended: true },
    });
    const result = await buildKansasCandidateLedger({
      target: houseTarget,
      now: NOW,
      loadFilingPool: poolOf([appointment, affidavit, original, amendment]),
    });
    expect(result.status).toBe("resolved");
    if (result.status !== "resolved") return;
    expect(appointment.openReport).not.toHaveBeenCalled();
    expect(affidavit.openReport).not.toHaveBeenCalled();
    expect(result.appointments).toEqual([{ fileDate: "06/09/2026", amendmentNo: "" }]);
    expect(result.affidavitDates).toEqual(["07/26/2026"]);
    const entries = Object.fromEntries(result.ledger.entries.map((entry) => [entry.period.key, entry]));
    expect(entries["2025-annual"]?.status).toBe("not_required");
    expect(entries["2026-pre_primary"]?.status).toBe("amended");
    expect(entries["2026-pre_primary"]?.canonical?.amendmentDate).toBe("2026-08-06");
    // The 7/26 affidavit exempts every later-due period.
    expect(entries["2026-pre_general"]?.status).toBe("affidavit_exempt");
    expect(result.complete).toBe(true);
  });

  it("never opens a paper report and keeps the candidate incomplete until its period is known", async () => {
    const paper = filing({ row: { channel: "paper", postbackTarget: "grdviewCfrResults$ctl02$lnkbtnName" } });
    const result = await buildKansasCandidateLedger({ target: houseTarget, now: NOW, loadFilingPool: poolOf([paper]) });
    expect(result.status).toBe("resolved");
    if (result.status !== "resolved") return;
    expect(paper.openReport).not.toHaveBeenCalled();
    expect(result.paperReports).toEqual([paper.row]);
    expect(result.reports).toEqual([]);
    expect(result.ledger.entries.map((entry) => entry.status)).toEqual([
      "missing_or_late",
      "missing_or_late",
      "not_yet_due",
      "not_yet_due",
    ]);
    expect(result.complete).toBe(false);
  });

  it("ignores the district column for a statewide recipe and honors a special cycle start", async () => {
    const report = filing({
      row: { name: "ROWAN STACY", officeSought: "Governor", district: "4" },
      cover: { start: "1/1/2026", end: "7/23/2026" },
    });
    const loadFilingPool = poolOf([report]);
    const result = await buildKansasCandidateLedger({
      target: { committeeId: "1::ROWAN:STACY", office: governor, electionYear: 2026, cycleStartYear: 2025 },
      now: NOW,
      loadFilingPool,
    });
    expect(loadFilingPool).toHaveBeenCalledWith(governor, 2026, 2025);
    expect(result.status).toBe("resolved");
    if (result.status !== "resolved") return;
    expect(result.reports).toHaveLength(1);
    expect(result.ledger.entries.map((entry) => entry.period.key)).toEqual([
      "2025-annual",
      "2026-pre_primary",
      "2026-pre_general",
      "2026-post_general",
    ]);
  });

  it("reports recipes that disagree with the office or district shape without searching", async () => {
    const loadFilingPool = vi.fn();
    expect(
      await buildKansasCandidateLedger({ target: { ...houseTarget, committeeId: "1::HOLLOWAY:MARGARET" }, now: NOW, loadFilingPool })
    ).toEqual({ status: "unresolved", reason: "recipe_office_mismatch" });
    expect(
      await buildKansasCandidateLedger({ target: { ...houseTarget, committeeId: "7::HOLLOWAY:MARGARET" }, now: NOW, loadFilingPool })
    ).toEqual({ status: "unresolved", reason: "recipe_district_mismatch" });
    expect(loadFilingPool).not.toHaveBeenCalled();
  });

  it("reports resolver outcomes: no filer, contradictory spellings, blank districts", async () => {
    const outcome = async (filings: KansasPooledFiling[]) =>
      buildKansasCandidateLedger({ target: houseTarget, now: NOW, loadFilingPool: poolOf(filings) });
    expect(await outcome([filing({ row: { name: "SOMEONE ELSE" } })])).toEqual({ status: "unresolved", reason: "no_matching_filer" });
    expect(
      await outcome([filing({ row: { name: "HOLLOWAY MARGARET B" } }), filing({ row: { index: 1, name: "HOLLOWAY MARGARET T" } })])
    ).toEqual({
      status: "unresolved",
      reason: "conflicting_filed_names",
      filedNames: ["HOLLOWAY MARGARET B", "HOLLOWAY MARGARET T"],
    });
    expect(await outcome([filing({ row: { district: "" } })])).toEqual({
      status: "unresolved",
      reason: "filings_missing_district",
      filedNames: ["HOLLOWAY MARGARET"],
    });
  });

  it("fails closed when a report does not land on a cover or the cover has no period", async () => {
    const stray = filing({ row: {}, landedUrl: "https://sos.ks.gov/elections/cfr_viewer/cfr_examiner_search_results.aspx" });
    await expect(
      buildKansasCandidateLedger({ target: houseTarget, now: NOW, loadFilingPool: poolOf([stray]) })
    ).rejects.toThrow("did not open a cover");
    const blank = filing({ row: {}, cover: { start: "", end: "" } });
    await expect(
      buildKansasCandidateLedger({ target: houseTarget, now: NOW, loadFilingPool: poolOf([blank]) })
    ).rejects.toThrow("cover has no period");
  });
});
