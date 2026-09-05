import { describe, expect, it, vi } from "vitest";

import type { KansasFilerFilingKind } from "../../../src/pipeline/kansasFinance/kansasCandidateFilerResolver.js";
import { buildKansasCandidateLedger, kansasLedgerCandidateName } from "../../../src/pipeline/kansasFinance/kansasCandidateLedger.js";
import type { KansasCfrGridRow } from "../../../src/pipeline/kansasFinance/kansasCfrViewerParsers.js";
import { kansasCfrOfficeForRace } from "../../../src/pipeline/kansasFinance/kansasFinanceEligibleOffices.js";
import type { KansasPooledFiling } from "../../../src/pipeline/kansasFinance/kansasFilingSearch.js";
import type { KansasKpdcCandidateRow } from "../../../src/pipeline/kansasFinance/kansasKpdcIndexClient.js";

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

function coverHtml(input: { start: string; end: string; amended?: boolean; termination?: boolean; receipts?: string }): string {
  return `
    <span id="lblCandOrgName">Margaret Holloway</span>
    <span id="lblFileStartDate">${input.start}</span>
    <span id="lblFileEndDate">${input.end}</span>
    <input id="chkAmended" type="checkbox" ${input.amended ? "checked" : ""} disabled />
    <input id="chkTermination" type="checkbox" ${input.termination ? "checked" : ""} disabled />
    <span id="lblTotalContributions">${input.receipts ?? "$0.00"}</span>
    <span id="lblCashBeginning">$0.00</span>`;
}

type Fixture = {
  row: Partial<KansasCfrGridRow>;
  filingKind?: KansasFilerFilingKind;
  cover?: Parameters<typeof coverHtml>[0];
  landedUrl?: string;
  /** Schedule pages by letter; a missing letter answers an empty page. */
  schedules?: Partial<Record<"A" | "B", string>>;
};

function filing(fixture: Fixture): KansasPooledFiling & { openReport: ReturnType<typeof vi.fn>; openSchedule: ReturnType<typeof vi.fn> } {
  const row = gridRow(fixture.row);
  const openReport = vi.fn(async () => ({
    url: fixture.landedUrl ?? COVER_URL,
    html: fixture.cover ? coverHtml(fixture.cover) : "",
    hiddenFields: {},
  }));
  const openSchedule = vi.fn(async (schedule: "A" | "B" | "C" | "D") => ({
    url: `https://sos.ks.gov/elections/cfr_viewer/reports/schedule_${schedule.toLowerCase()}_report.aspx`,
    html: (schedule === "A" || schedule === "B") && fixture.schedules?.[schedule] ? fixture.schedules[schedule]! : "",
    hiddenFields: {},
  }));
  return { row, filingKind: fixture.filingKind ?? "report", openReport, openSchedule };
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
    expect(result.reports.every((report) => report.scheduleA === null && report.scheduleB === null)).toBe(true);
    expect(annual.openSchedule).not.toHaveBeenCalled();
  });

  // Live: Proctor's 2026 pre-primary amendment is two grid rows (ctl07 and
  // ctl08) with one file date, one amendment date and covers equal line for
  // line. Kept as two versions they never order, so the period went
  // `ambiguous` and the candidate published nothing.
  it("drops a grid row that repeats a report already opened, and keeps a same-day pair whose covers differ", async () => {
    const amendedRow = { index: 1, fileDate: "07/27/2026", amendmentDate: "08/10/2026" };
    const amendedCover = { start: "1/1/2026", end: "7/23/2026", amended: true, receipts: "$42,480.00" };
    const annual = filing({ row: { fileDate: "01/09/2026" }, cover: { start: "1/1/2025", end: "12/31/2025" } });
    const original = filing({ row: { index: 3 }, cover: { start: "1/1/2026", end: "7/23/2026", receipts: "$37,480.00" } });
    const amendment = filing({ row: amendedRow, cover: amendedCover });
    const listedTwice = filing({ row: { ...amendedRow, index: 2, postbackTarget: "grdviewCfrResults$ctl08$lnkbtnName" }, cover: amendedCover });

    const result = await buildKansasCandidateLedger({
      target: houseTarget,
      now: NOW,
      loadFilingPool: poolOf([annual, amendment, listedTwice, original]),
    });

    expect(result.status).toBe("resolved");
    if (result.status !== "resolved") return;
    // Both rows are opened — the duplicate is only knowable from its cover.
    expect(listedTwice.openReport).toHaveBeenCalled();
    expect(result.duplicateReports.map((row) => row.postbackTarget)).toEqual(["grdviewCfrResults$ctl08$lnkbtnName"]);
    expect(result.reports).toHaveLength(3);
    const prePrimary = result.ledger.entries.find((entry) => entry.period.key === "2026-pre_primary")!;
    expect(prePrimary.status).toBe("amended");
    expect(prePrimary.canonical).toMatchObject({ amended: true, amendmentDate: "2026-08-10" });
    expect(result.complete).toBe(true);

    // A genuine same-day pair whose covers differ still cannot be ordered.
    const rival = filing({ row: { ...amendedRow, index: 2 }, cover: { ...amendedCover, receipts: "$42,481.00" } });
    const ambiguous = await buildKansasCandidateLedger({
      target: houseTarget,
      now: NOW,
      loadFilingPool: poolOf([annual, amendment, rival, original]),
    });
    expect(ambiguous.status).toBe("resolved");
    if (ambiguous.status !== "resolved") return;
    expect(ambiguous.duplicateReports).toEqual([]);
    expect(ambiguous.ledger.entries.find((entry) => entry.period.key === "2026-pre_primary")!.status).toBe("ambiguous");
    expect(ambiguous.complete).toBe(false);
  });

  it("with openSchedules, reopens each counted period's canonical cover and reads its Schedules A and B", async () => {
    const schedules = { A: `<span id="lblTotalReceipts">$12.34</span>`, B: `<span id="lblTotalInKind">$5.00</span>` };
    const original = filing({ row: { fileDate: "01/09/2026" }, cover: { start: "1/1/2025", end: "12/31/2025" }, schedules });
    const amendment = filing({
      row: { index: 1, fileDate: "01/09/2026", amendmentDate: "01/15/2026" },
      cover: { start: "1/1/2025", end: "12/31/2025", amended: true },
      schedules,
    });
    const prePrimary = filing({ row: { index: 2 }, cover: { start: "1/1/2026", end: "7/23/2026" }, schedules });
    const lastMinute = filing({ row: { index: 3, fileDate: "07/30/2026" }, cover: { start: "7/24/2026", end: "7/29/2026" }, schedules });
    const result = await buildKansasCandidateLedger({
      target: houseTarget,
      now: NOW,
      loadFilingPool: poolOf([original, amendment, prePrimary, lastMinute]),
      openSchedules: true,
    });
    expect(result.status).toBe("resolved");
    if (result.status !== "resolved") return;
    // Canonical versions: cover once for the ledger, once more before the schedules. Superseded and last-minute: cover only.
    expect([original, amendment, prePrimary, lastMinute].map((row) => row.openReport.mock.calls.length)).toEqual([1, 2, 2, 1]);
    expect([original, amendment, prePrimary, lastMinute].map((row) => row.openSchedule.mock.calls.map((call) => call[0]))).toEqual([
      [],
      ["A", "B"],
      ["A", "B"],
      [],
    ]);
    expect(result.reports.map((report) => [report.scheduleA?.totals.totalReceiptsCents ?? null, report.scheduleB?.totals.totalInKindCents ?? null])).toEqual([
      [null, null],
      [1234, 500],
      [1234, 500],
      [null, null],
    ]);
  });

  it("with openSchedules, fails closed when reopening a cover lands on any other cover (its own amendment included)", async () => {
    const annual = filing({ row: { fileDate: "01/09/2026" }, cover: { start: "1/1/2025", end: "12/31/2025" } });
    annual.openReport.mockResolvedValueOnce({ url: COVER_URL, html: coverHtml({ start: "1/1/2025", end: "12/31/2025" }), hiddenFields: {} });
    // Same period and lines, only the amended box differs: the neighbouring grid row after a mid-run shift.
    annual.openReport.mockResolvedValueOnce({ url: COVER_URL, html: coverHtml({ start: "1/1/2025", end: "12/31/2025", amended: true }), hiddenFields: {} });
    await expect(
      buildKansasCandidateLedger({ target: houseTarget, now: NOW, loadFilingPool: poolOf([annual]), openSchedules: true })
    ).rejects.toThrow("reopening for its schedules did not land on the same cover");
    expect(annual.openSchedule).not.toHaveBeenCalled();
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

  it("never opens a paper report and keeps the candidate incomplete without the KPDC trees", async () => {
    const paper = filing({ row: { channel: "paper", postbackTarget: "grdviewCfrResults$ctl02$lnkbtnName" } });
    const result = await buildKansasCandidateLedger({ target: houseTarget, now: NOW, loadFilingPool: poolOf([paper]) });
    expect(result.status).toBe("resolved");
    if (result.status !== "resolved") return;
    expect(paper.openReport).not.toHaveBeenCalled();
    expect(result.paperReports).toEqual([paper.row]);
    expect(result.reports).toEqual([]);
    expect(result.paper).toBeNull();
    expect(result.ledger.entries.map((entry) => entry.status)).toEqual([
      "missing_or_late",
      "missing_or_late",
      "not_yet_due",
      "not_yet_due",
    ]);
    expect(result.complete).toBe(false);
  });

  // KPDC candidate rows (synthetic): the 2026 House tree row and the 2024
  // tree row of the same paper filer.
  const kpdcLink = (year: number) => (fileName: string) => ({
    url: `https://www.kansas.gov/ethics/CFAScanned/House/${year}ElecCycle/${fileName}`,
    fileName,
    linkText: fileName,
  });
  const kpdcTrees: Record<number, KansasKpdcCandidateRow[]> = {
    2026: [
      { district: 85, filedName: "Holloway, Margaret", links: ["H085MH_AT.pdf", "H085MH_amend2601.pdf", "H085MH_202601.pdf", "H085MH_202607.pdf", "H085MH_2026PLF.pdf"].map(kpdcLink(2026)) },
      { district: 86, filedName: "Holloway, Margaret", links: ["H086MH_202607.pdf"].map(kpdcLink(2026)) },
    ],
    2024: [{ district: 85, filedName: "Holloway, Margaret", links: ["H085MH_AT.pdf", "H085MH_202410.pdf", "H085MH_2024PLF.pdf", "H085MH_202501.pdf"].map(kpdcLink(2024)) }],
  };
  const loadKpdcRows = vi.fn(async (_office: unknown, electionYear: number) => kpdcTrees[electionYear] ?? []);
  const paperRow = (index: number, fileDate: string) =>
    filing({ row: { index, fileDate, channel: "paper", postbackTarget: "grdviewCfrResults$ctl02$lnkbtnName" } });

  it("takes a paper filer's periods from the KPDC trees when they explain every viewer row", async () => {
    // Viewer: 1/10/2025 (2024 post-general), 1/10/2026 x2 (2025 annual + amendment), 7/27/2026 (pre-primary), 7/30/2026 (primary last-minute).
    const pool = [paperRow(0, "07/27/2026"), paperRow(1, "01/10/2026"), paperRow(2, "01/10/2026"), paperRow(3, "01/10/2025"), paperRow(4, "07/30/2026")];
    const result = await buildKansasCandidateLedger({ target: houseTarget, now: NOW, loadFilingPool: poolOf(pool), loadKpdcRows });
    expect(loadKpdcRows).toHaveBeenCalledWith(house, 2026);
    expect(loadKpdcRows).toHaveBeenCalledWith(house, 2024);
    expect(result.status).toBe("resolved");
    if (result.status !== "resolved") return;
    // The 2026 last-minute scan explains the 7/30 row; the 2024 one predates the window and counts for nothing.
    expect(result.paper).toMatchObject({ status: "resolved", filedNames: ["Holloway, Margaret"], explainedByEfile: 0, lastMinute: 1, skipped: 2, unmapped: [] });
    if (result.paper?.status !== "resolved") return;
    // 202410 was due before the window and is not taken; the other three are.
    expect(result.paper.headers.map((header) => [header.periodStart, header.amendmentOrdinal])).toEqual([
      ["2025-01-01", 1],
      ["2025-01-01", null],
      ["2026-01-01", null],
      ["2024-10-25", null],
    ]);
    expect(result.ledger.entries.map((entry) => [entry.period.key, entry.status])).toEqual([
      ["2025-annual", "amended"],
      ["2026-pre_primary", "report_filed"],
      ["2026-pre_general", "not_yet_due"],
      ["2026-post_general", "not_yet_due"],
    ]);
    expect(result.ledger.outOfCycleFilings).toHaveLength(1);
    expect(result.complete).toBe(true);
  });

  it("stays incomplete when the viewer shows more paper rows than the trees explain", async () => {
    // Six viewer rows against four period versions plus one last-minute scan: the 1/13 row has no scan.
    const pool = [
      paperRow(0, "07/27/2026"),
      paperRow(1, "01/13/2026"),
      paperRow(2, "01/10/2026"),
      paperRow(3, "01/10/2025"),
      paperRow(4, "07/30/2026"),
      paperRow(5, "01/10/2026"),
    ];
    const result = await buildKansasCandidateLedger({ target: houseTarget, now: NOW, loadFilingPool: poolOf(pool), loadKpdcRows });
    expect(result.status).toBe("resolved");
    if (result.status !== "resolved") return;
    expect(result.paper?.status).toBe("resolved");
    expect(result.ledger.complete).toBe(true);
    expect(result.complete).toBe(false);
  });

  it("subtracts the versions already opened as e-file covers before counting paper", async () => {
    // The 2025 annual and its amendment were e-filed; only the pre-primary is paper.
    const efileAnnual = filing({ row: { index: 0, fileDate: "01/10/2026" }, cover: { start: "1/1/2025", end: "12/31/2025" } });
    const efileAmendment = filing({ row: { index: 1, fileDate: "01/10/2026", amendmentDate: "01/12/2026" }, cover: { start: "1/1/2025", end: "12/31/2025", amended: true } });
    const pool = [efileAnnual, efileAmendment, paperRow(2, "07/27/2026"), paperRow(3, "01/10/2025")];
    const result = await buildKansasCandidateLedger({ target: houseTarget, now: NOW, loadFilingPool: poolOf(pool), loadKpdcRows });
    expect(result.status).toBe("resolved");
    if (result.status !== "resolved") return;
    expect(result.paper).toMatchObject({ status: "resolved", explainedByEfile: 2 });
    if (result.paper?.status !== "resolved") return;
    expect(result.paper.headers.map((header) => header.periodStart)).toEqual(["2026-01-01", "2024-10-25"]);
    expect(result.ledger.entries.map((entry) => entry.status).slice(0, 2)).toEqual(["amended", "report_filed"]);
    expect(result.complete).toBe(true);
  });

  it("reports a filer the trees do not carry and keeps the candidate incomplete", async () => {
    const result = await buildKansasCandidateLedger({
      target: { ...houseTarget, committeeId: "7:85:HOLLOWAY:DANIEL", committeeName: "HOLLOWAY DANIEL" },
      now: NOW,
      loadFilingPool: poolOf([filing({ row: { name: "HOLLOWAY DANIEL", channel: "paper" } })]),
      loadKpdcRows,
    });
    expect(result.status).toBe("resolved");
    if (result.status !== "resolved") return;
    expect(result.paper).toEqual({ status: "unresolved", reason: "no_matching_filer" });
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

  it("carries the link's verified filed spelling as middle-name evidence", async () => {
    const recipe = { surname: "HOLLOWAY", firstName: "MARGARET" };
    expect(kansasLedgerCandidateName(recipe)).toBe("HOLLOWAY, MARGARET");
    expect(kansasLedgerCandidateName(recipe, "HOLLOWAY MARGARET A")).toBe("HOLLOWAY, MARGARET A");
    // A suffix typed into the surname cell (live 2026) never reaches the name.
    expect(kansasLedgerCandidateName({ surname: "ROBERTSON", firstName: "BOBBY" }, "JR ROBERTSON BOBBY JOE")).toBe("ROBERTSON, BOBBY JOE");
    // Operator free text that is not the recipe's filer is ignored.
    expect(kansasLedgerCandidateName(recipe, "FRIENDS OF MARGARET")).toBe("HOLLOWAY, MARGARET");
    expect(kansasLedgerCandidateName(recipe, "HOLLOWAYS MARGARET")).toBe("HOLLOWAY, MARGARET");

    // Distinct receipts: these are separate filings, not one row listed twice.
    const cover = { start: "1/1/2026", end: "7/23/2026" };
    const filings = () => [
      filing({ row: { name: "HOLLOWAY MARGARET A" }, cover: { ...cover, receipts: "$1.00" } }),
      filing({ row: { index: 1, name: "HOLLOWAY MARGARET" }, cover: { ...cover, receipts: "$2.00" } }),
      filing({ row: { index: 2, name: "HOLLOWAY MARGARET B" }, cover: { ...cover, receipts: "$3.00" } }),
    ];
    // The recipe alone aligns both middles and must report the contradiction.
    expect(await buildKansasCandidateLedger({ target: houseTarget, now: NOW, loadFilingPool: poolOf(filings()) })).toMatchObject({
      status: "unresolved",
      reason: "conflicting_filed_names",
    });
    // The auto-link verified "HOLLOWAY MARGARET A" (roster "Margaret A. Holloway"): B is another person.
    const pool = filings();
    const result = await buildKansasCandidateLedger({
      target: { ...houseTarget, committeeName: "HOLLOWAY MARGARET A" },
      now: NOW,
      loadFilingPool: poolOf(pool),
    });
    expect(result.status).toBe("resolved");
    if (result.status !== "resolved") return;
    expect(result.match.filedNames).toEqual(["HOLLOWAY MARGARET", "HOLLOWAY MARGARET A"]);
    expect(result.reports).toHaveLength(2);
    expect(pool[2]!.openReport).not.toHaveBeenCalled();
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
