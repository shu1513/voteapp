import { describe, expect, it } from "vitest";

import { kansasCfrOfficeForRace } from "../../../src/pipeline/kansasFinance/kansasFinanceEligibleOffices.js";
import {
  buildKansasReportLedger,
  kansasDateToIso,
  kansasGeneralDate,
  kansasLastMinuteWindows,
  kansasPrimaryDate,
  kansasReportingPeriods,
  type KansasFilingHeader,
} from "../../../src/pipeline/kansasFinance/kansasReportInventory.js";

const HOUSE = kansasCfrOfficeForRace({ officeScope: "state_lower", officeCanonicalName: "State Lower Chamber Legislator" })!;
const GOVERNOR = kansasCfrOfficeForRace({ officeScope: "statewide", officeCanonicalName: "Governor" })!;

function filing(overrides: Partial<KansasFilingHeader> & Pick<KansasFilingHeader, "periodStart" | "periodEnd" | "fileDate">): KansasFilingHeader {
  return { amendmentDate: null, amended: false, termination: false, channel: "efile", ...overrides };
}

describe("kansasDateToIso", () => {
  it("accepts cover ('1/1/2026') and grid ('07/27/2026') shapes", () => {
    expect(kansasDateToIso("1/1/2026")).toBe("2026-01-01");
    expect(kansasDateToIso("07/27/2026")).toBe("2026-07-27");
    expect(kansasDateToIso(" 7/23/2026 ")).toBe("2026-07-23");
    expect(kansasDateToIso("2026-07-23")).toBe("2026-07-23");
  });

  it("rejects two-digit years, blanks, and impossible dates", () => {
    expect(() => kansasDateToIso("07/23/26")).toThrow("unparseable Kansas date");
    expect(() => kansasDateToIso("")).toThrow("unparseable Kansas date");
    expect(() => kansasDateToIso("02/30/2026")).toThrow("invalid Kansas date");
    expect(() => kansasDateToIso("2026-02-30")).toThrow("invalid Kansas date");
    expect(() => kansasDateToIso("2026-7-23")).toThrow("unparseable Kansas date");
  });
});

describe("kansasReportingPeriods", () => {
  it("computes the 2026 election dates (primary 8/4, general 11/3)", () => {
    expect(kansasPrimaryDate(2026).toISOString().slice(0, 10)).toBe("2026-08-04");
    expect(kansasGeneralDate(2026).toISOString().slice(0, 10)).toBe("2026-11-03");
    expect(kansasPrimaryDate(2024).toISOString().slice(0, 10)).toBe("2024-08-06");
    expect(kansasGeneralDate(2024).toISOString().slice(0, 10)).toBe("2024-11-05");
  });

  it("matches the official 2026 KPDC due-date sheet for a House cycle", () => {
    expect(kansasReportingPeriods(HOUSE, 2026)).toEqual([
      { key: "2025-annual", kind: "annual", start: "2025-01-01", end: "2025-12-31", due: "2026-01-10" },
      { key: "2026-pre_primary", kind: "pre_primary", start: "2026-01-01", end: "2026-07-23", due: "2026-07-27" },
      { key: "2026-pre_general", kind: "pre_general", start: "2026-07-24", end: "2026-10-22", due: "2026-10-26" },
      { key: "2026-post_general", kind: "post_general", start: "2026-10-23", end: "2026-12-31", due: "2027-01-10" },
    ]);
  });

  it("gives a statewide four-year cycle three annual reports (non-ballot years are annual-only)", () => {
    expect(kansasReportingPeriods(GOVERNOR, 2026).map((period) => period.key)).toEqual([
      "2023-annual",
      "2024-annual",
      "2025-annual",
      "2026-pre_primary",
      "2026-pre_general",
      "2026-post_general",
    ]);
  });

  it("runs a special election on the short cycle KPDC files it under", () => {
    const senate = kansasCfrOfficeForRace({ officeScope: "state_upper", officeCanonicalName: "State Senator" })!;
    expect(kansasReportingPeriods(senate, 2028).map((period) => period.key).slice(0, 3)).toEqual(["2025-annual", "2026-annual", "2027-annual"]);
    // A 2026 Senate race is a special; KPDC's Senate/2026SpecialElection archive starts at the 2025 annual.
    expect(kansasReportingPeriods(senate, 2026).map((period) => period.key)).toEqual(["2025-annual", "2026-pre_primary", "2026-pre_general", "2026-post_general"]);
    expect(kansasReportingPeriods(senate, 2026, { cycleStartYear: 2025 }).map((period) => period.key)).toEqual([
      "2025-annual",
      "2026-pre_primary",
      "2026-pre_general",
      "2026-post_general",
    ]);
    expect(() => kansasReportingPeriods(senate, 2026, { cycleStartYear: 2027 })).toThrow("Invalid Kansas cycle start year");
    expect(() => kansasReportingPeriods(senate, 2026, { cycleStartYear: 2022 })).toThrow("Invalid Kansas cycle start year");
  });

  it("computes the last-minute windows (7/24-7/29 and 10/23-10/28 in 2026)", () => {
    expect(kansasLastMinuteWindows(2026)).toEqual([
      { start: "2026-07-24", end: "2026-07-29" },
      { start: "2026-10-23", end: "2026-10-28" },
    ]);
  });

  it("rejects an out-of-range election year", () => {
    expect(() => kansasReportingPeriods(HOUSE, 1999)).toThrow("Invalid Kansas election year");
  });
});

describe("buildKansasReportLedger", () => {
  const periods = kansasReportingPeriods(HOUSE, 2026);
  const windows = kansasLastMinuteWindows(2026);
  const NOW = new Date("2026-09-02T12:00:00.000Z");
  const base = { periods, lastMinuteWindows: windows, now: NOW, appointmentsOfTreasurer: [], affidavitDates: [] };

  it("marks filed, not-yet-due, and last-minute filings for a continuing committee (live Helwig shape)", () => {
    const ledger = buildKansasReportLedger({
      ...base,
      filings: [
        filing({ periodStart: "1/1/2025", periodEnd: "12/31/2025", fileDate: "01/04/2026" }),
        filing({ periodStart: "1/1/2026", periodEnd: "7/23/2026", fileDate: "07/24/2026" }),
        filing({ periodStart: "7/24/2026", periodEnd: "7/29/2026", fileDate: "07/30/2026" }),
      ],
    });
    expect(ledger.entries.map((entry) => [entry.period.key, entry.status])).toEqual([
      ["2025-annual", "report_filed"],
      ["2026-pre_primary", "report_filed"],
      ["2026-pre_general", "not_yet_due"],
      ["2026-post_general", "not_yet_due"],
    ]);
    expect(ledger.lastMinuteFilings).toHaveLength(1);
    expect(ledger.unexpectedFilings).toEqual([]);
    expect(ledger.complete).toBe(true);
    expect(ledger.entries[0]!.canonical?.fileDate).toBe("2026-01-04");
  });

  it("flags a due period with no filing as missing_or_late and fails the ledger", () => {
    const ledger = buildKansasReportLedger({
      ...base,
      filings: [filing({ periodStart: "1/1/2025", periodEnd: "12/31/2025", fileDate: "01/09/2026" })],
    });
    expect(ledger.entries[1]).toMatchObject({ status: "missing_or_late", canonical: null });
    expect(ledger.complete).toBe(false);
  });

  it("treats periods that ended before the first in-window Appointment of Treasurer as not_required", () => {
    const ledger = buildKansasReportLedger({
      ...base,
      appointmentsOfTreasurer: [{ fileDate: "03/02/2026", amendmentNo: "" }],
      filings: [filing({ periodStart: "1/1/2026", periodEnd: "7/23/2026", fileDate: "07/27/2026" })],
    });
    expect(ledger.entries[0]!.status).toBe("not_required");
    expect(ledger.complete).toBe(true);
  });

  it("ignores amended appointments: a treasurer change proves nothing about when the committee began", () => {
    // Live Kelly shape: only appointment amendments #6 and #7 fall in the
    // window, and no report was filed — the committee is continuing, so the
    // 2025 annual is owed, not excused.
    const ledger = buildKansasReportLedger({
      ...base,
      appointmentsOfTreasurer: [{ fileDate: "09/05/2024", amendmentNo: "6" }, { fileDate: "01/08/2025", amendmentNo: "7" }],
      filings: [filing({ periodStart: "1/1/2026", periodEnd: "7/23/2026", fileDate: "07/27/2026" })],
    });
    expect(ledger.entries[0]!.status).toBe("missing_or_late");
    expect(ledger.complete).toBe(false);
  });

  it("does not excuse earlier periods when a report predates the appointment (treasurer change)", () => {
    const ledger = buildKansasReportLedger({
      ...base,
      appointmentsOfTreasurer: [{ fileDate: "03/02/2026", amendmentNo: "" }, { fileDate: "06/01/2026", amendmentNo: "1" }],
      filings: [filing({ periodStart: "1/1/2025", periodEnd: "12/31/2025", fileDate: "01/09/2026" })],
    });
    // The 2025 annual proves the committee predates 03/02/2026, so the
    // pre-primary period is owed — and it was not filed.
    expect(ledger.entries.map((entry) => entry.status)).toEqual(["report_filed", "missing_or_late", "not_yet_due", "not_yet_due"]);
  });

  it("orders versions by amendment date (live: an amendment keeps the original file date)", () => {
    // Live Governor 2026 shape: three grid rows for one 2025 annual — the
    // original and two amendments, one filed the same day as the original.
    const annual = { periodStart: "1/1/2025", periodEnd: "12/31/2025", fileDate: "01/12/2026" };
    const original = filing(annual);
    const sameDay = filing({ ...annual, amendmentDate: "01/12/2026", amended: true });
    const later = filing({ ...annual, amendmentDate: "01/15/2026", amended: true });
    const ledger = buildKansasReportLedger({ ...base, filings: [sameDay, original, later] });
    expect(ledger.entries[0]).toMatchObject({ status: "amended", canonical: { amendmentDate: "2026-01-15" } });
    expect(ledger.entries[0]!.filings.map((row) => [row.amendmentDate, row.amended])).toEqual([
      ["2026-01-15", true],
      ["2026-01-12", true],
      [null, false],
    ]);
  });

  it("reports two amendments on one day as ambiguous (live: Ward 2/9/2023)", () => {
    const annual = { periodStart: "1/1/2025", periodEnd: "12/31/2025", fileDate: "01/12/2026" };
    const first = filing({ ...annual, amendmentDate: "01/15/2026", amended: true, termination: true });
    const second = filing({ ...annual, amendmentDate: "01/15/2026", amended: true });
    for (const order of [[filing(annual), first, second], [second, first, filing(annual)]]) {
      const ledger = buildKansasReportLedger({ ...base, filings: order });
      expect(ledger.entries[0]).toMatchObject({ status: "ambiguous", canonical: null });
      // The unresolved termination checkbox must not close later periods.
      expect(ledger.entries[1]!.status).toBe("missing_or_late");
    }
  });

  it("ignores a termination checkbox on a superseded version", () => {
    const period = { periodStart: "1/1/2026", periodEnd: "7/23/2026", fileDate: "07/27/2026" };
    const ledger = buildKansasReportLedger({
      ...base,
      now: new Date("2027-02-01T00:00:00.000Z"),
      filings: [
        filing({ periodStart: "1/1/2025", periodEnd: "12/31/2025", fileDate: "01/09/2026" }),
        filing({ ...period, termination: true }),
        filing({ ...period, amendmentDate: "08/01/2026", amended: true }),
      ],
    });
    expect(ledger.entries.map((entry) => entry.status)).toEqual(["report_filed", "amended", "missing_or_late", "missing_or_late"]);
  });

  it("reports two unflagged originals, or an original after an amendment, as ambiguous", () => {
    const period = { periodStart: "1/1/2026", periodEnd: "7/23/2026" };
    const twoOriginals = buildKansasReportLedger({
      ...base,
      filings: [filing({ ...period, fileDate: "07/27/2026", channel: "paper" }), filing({ ...period, fileDate: "08/06/2026", channel: "paper" })],
    });
    expect(twoOriginals.entries[1]).toMatchObject({ status: "ambiguous", canonical: null });
    expect(twoOriginals.complete).toBe(false);

    const originalAfterAmendment = buildKansasReportLedger({
      ...base,
      filings: [
        filing({ ...period, fileDate: "07/27/2026", amendmentDate: "08/01/2026", amended: true }),
        filing({ ...period, fileDate: "08/06/2026" }),
      ],
    });
    expect(originalAfterAmendment.entries[1]!.status).toBe("ambiguous");
  });

  it("marks a single filing flagged amended as amended", () => {
    const ledger = buildKansasReportLedger({
      ...base,
      filings: [filing({ periodStart: "1/1/2026", periodEnd: "7/23/2026", fileDate: "08/06/2026", amended: true })],
    });
    expect(ledger.entries[1]!.status).toBe("amended");
  });

  it("ignores a prior cycle's filings but still reads their termination (live Governor shapes)", () => {
    const periods = kansasReportingPeriods(GOVERNOR, 2026);
    // Ward: terminated with an amended 2022 post-general report, never reopened.
    const closed = buildKansasReportLedger({
      ...base,
      periods,
      filings: [
        filing({ periodStart: "10/28/2022", periodEnd: "12/31/2022", fileDate: "02/06/2023" }),
        filing({ periodStart: "10/28/2022", periodEnd: "12/31/2022", fileDate: "02/06/2023", amendmentDate: "02/09/2023", amended: true, termination: true }),
      ],
    });
    expect(closed.outOfCycleFilings).toHaveLength(2);
    expect(closed.unexpectedFilings).toEqual([]);
    expect(closed.entries.every((entry) => entry.status === "terminated")).toBe(true);
    expect(closed.complete).toBe(true);

    // Colyer: terminated with an amended 2023 annual, reappointed a treasurer
    // in 2025, filed the 2025 annual and the 2026 pre-primary.
    const reopened = buildKansasReportLedger({
      ...base,
      periods,
      appointmentsOfTreasurer: [{ fileDate: "05/12/2025", amendmentNo: "" }],
      filings: [
        filing({ periodStart: "1/1/2022", periodEnd: "12/31/2022", fileDate: "01/03/2024" }),
        filing({ periodStart: "1/1/2023", periodEnd: "12/31/2023", fileDate: "01/03/2024" }),
        filing({ periodStart: "1/1/2023", periodEnd: "12/31/2023", fileDate: "01/03/2024", amendmentDate: "01/03/2024", amended: true, termination: true }),
        filing({ periodStart: "1/1/2025", periodEnd: "12/31/2025", fileDate: "01/09/2026" }),
        filing({ periodStart: "1/1/2025", periodEnd: "12/31/2025", fileDate: "01/09/2026", amendmentDate: "07/27/2026", amended: true }),
        filing({ periodStart: "1/1/2026", periodEnd: "7/23/2026", fileDate: "07/27/2026" }),
      ],
    });
    expect(reopened.entries.map((entry) => [entry.period.key, entry.status])).toEqual([
      ["2023-annual", "amended"],
      ["2024-annual", "terminated"],
      ["2025-annual", "amended"],
      ["2026-pre_primary", "report_filed"],
      ["2026-pre_general", "not_yet_due"],
      ["2026-post_general", "not_yet_due"],
    ]);
    expect(reopened.outOfCycleFilings).toHaveLength(1);
    expect(reopened.complete).toBe(true);
  });

  it("marks periods after a termination report as terminated", () => {
    const ledger = buildKansasReportLedger({
      ...base,
      filings: [
        filing({ periodStart: "1/1/2025", periodEnd: "12/31/2025", fileDate: "01/09/2026" }),
        filing({ periodStart: "1/1/2026", periodEnd: "7/23/2026", fileDate: "07/27/2026", termination: true }),
      ],
      now: new Date("2027-02-01T00:00:00.000Z"),
    });
    expect(ledger.entries.map((entry) => entry.status)).toEqual(["report_filed", "report_filed", "terminated", "terminated"]);
    expect(ledger.complete).toBe(true);
  });

  it("marks unfiled periods as affidavit_exempt once an affidavit is on file by the due date", () => {
    const ledger = buildKansasReportLedger({ ...base, filings: [], affidavitDates: ["06/01/2026"] });
    expect(ledger.entries.map((entry) => entry.status)).toEqual([
      "missing_or_late", // due 2026-01-10, before the affidavit
      "affidavit_exempt",
      "affidavit_exempt",
      "affidavit_exempt",
    ]);
  });

  it("reports a filing that matches no period or window instead of dropping it", () => {
    const ledger = buildKansasReportLedger({
      ...base,
      filings: [filing({ periodStart: "1/1/2026", periodEnd: "6/30/2026", fileDate: "07/27/2026" })],
    });
    expect(ledger.unexpectedFilings).toHaveLength(1);
    expect(ledger.complete).toBe(false);
  });

  it("rejects unparseable dates rather than guessing", () => {
    expect(() =>
      buildKansasReportLedger({ ...base, filings: [filing({ periodStart: "1/1/26", periodEnd: "7/23/2026", fileDate: "07/27/2026" })] })
    ).toThrow("unparseable Kansas date");
    expect(() => buildKansasReportLedger({ ...base, filings: [], now: new Date("nope") })).toThrow("invalid now");
  });
});

describe("date-less KPDC versions", () => {
  const house = kansasCfrOfficeForRace({ officeScope: "state_lower", officeCanonicalName: "State Lower Chamber Legislator" })!;
  const periods = kansasReportingPeriods(house, 2026);
  const now = new Date("2026-09-02T12:00:00.000Z");
  const paper = (amendmentOrdinal: number | null, termination = false): KansasFilingHeader => ({
    periodStart: "2026-01-01",
    periodEnd: "2026-07-23",
    fileDate: null,
    amendmentDate: null,
    amended: amendmentOrdinal !== null,
    amendmentOrdinal,
    termination,
    channel: "paper",
  });
  const build = (filings: KansasFilingHeader[]) =>
    buildKansasReportLedger({ periods, filings, appointmentsOfTreasurer: [], affidavitDates: [], lastMinuteWindows: [], now })
      .entries.find((entry) => entry.period.key === "2026-pre_primary")!;

  it("orders undated versions by the amend prefix", () => {
    const entry = build([paper(null), paper(2), paper(1)]);
    expect(entry.status).toBe("amended");
    expect(entry.canonical?.amendmentOrdinal).toBe(2);
    expect(entry.filings.map((filing) => filing.amendmentOrdinal)).toEqual([2, 1, null]);
  });

  it("does not trust two undated amendments with the same prefix, nor an undated one against a dated one", () => {
    expect(build([paper(null), paper(1), paper(1)]).status).toBe("ambiguous");
    const efileAmendment: KansasFilingHeader = {
      periodStart: "1/1/2026",
      periodEnd: "7/23/2026",
      fileDate: "07/27/2026",
      amendmentDate: "08/06/2026",
      amended: true,
      termination: false,
      channel: "efile",
    };
    expect(build([paper(null), efileAmendment, paper(1)]).status).toBe("ambiguous");
    // A dated original with one undated amendment is an ordinary chain.
    const efileOriginal = { ...efileAmendment, amendmentDate: null, amended: false };
    expect(build([efileOriginal, paper(1)]).status).toBe("amended");
  });
});
