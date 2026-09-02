import { describe, expect, it } from "vitest";

import { parseNorthDakotaReportingScheduleCsv } from "../../../src/pipeline/northDakotaFinance/northDakotaCfrsCsv.js";
import {
  isNorthDakotaDateInWindow,
  northDakotaCycleWindowYears,
  northDakotaScheduleYearsForElection,
  resolveNorthDakotaCandidateCycleWindow,
} from "../../../src/pipeline/northDakotaFinance/northDakotaReportingCycleWindows.js";

const HEADER = "ElectionName,ReportingCycle,ReportingPeriodDescription,FormType,ReportType,BeginDate,Enddate,DueDate";
// Verbatim shape of the live 2026 and 2027 schedule files (2026-09-01).
const FILE_2026 = [
  "2026 Election - Statewide,2025 REPORTING CYCLE,2025 Year End Report,Campaign Financial Statement,Year End,2025-01-01,2025-12-31,2026-01-31",
  "2026 Election - Statewide,2026 Reporting Cycle,2026 Pre-General Report,Campaign Financial Statement,Pre-General,2026-01-01,2026-09-24,2026-10-02",
  "2026 Election - Statewide,2026 Reporting Cycle,2026 Pre-Primary Report,Campaign Financial Statement,Pre-Primary,2026-01-01,2026-04-30,2026-05-08",
];
const FILE_2027 = [
  "2026 Election - Statewide,2026 Reporting Cycle,2026 Year End Report,Campaign Financial Statement,Year End,2026-01-01,2026-12-31,2027-01-31",
];

function rows(lines: string[]) {
  return parseNorthDakotaReportingScheduleCsv(`${HEADER}\n${lines.join("\n")}\n`).rows;
}

describe("resolveNorthDakotaCandidateCycleWindow", () => {
  it("unions the cumulative periods of both schedule files into 2025-01-01..2026-12-31", () => {
    const window = resolveNorthDakotaCandidateCycleWindow({
      scheduleRows: rows([...FILE_2026, ...FILE_2027]),
      electionYear: 2026,
    });
    expect(window).toMatchObject({ election: "2026 Election - Statewide", windowStart: "2025-01-01", windowEnd: "2026-12-31" });
    expect(window.periods.map((period) => period.description)).toEqual([
      "2025 Year End Report",
      "2026 Pre-Primary Report",
      "2026 Pre-General Report",
      "2026 Year End Report",
    ]);
    expect(northDakotaCycleWindowYears(window)).toEqual([2025, 2026]);
    expect(northDakotaScheduleYearsForElection(2026)).toEqual([2026, 2027]);
  });

  it("ignores other elections and collapses a period repeated across files", () => {
    const window = resolveNorthDakotaCandidateCycleWindow({
      scheduleRows: rows([
        ...FILE_2026,
        ...FILE_2027,
        ...FILE_2027,
        "2028 Election - Statewide,2027 REPORTING CYCLE,2027 Year End Report,Campaign Financial Statement,Year End,2027-01-01,2027-12-31,2028-01-31",
      ]),
      electionYear: 2026,
    });
    expect(window.periods).toHaveLength(4);
    expect(window.windowEnd).toBe("2026-12-31");
  });

  it("fails closed when the year-end period is missing (window would stop at the pre-general cutoff)", () => {
    expect(() => resolveNorthDakotaCandidateCycleWindow({ scheduleRows: rows(FILE_2026), electionYear: 2026 })).toThrow(
      /window ends 2026-09-24, not 2026-12-31: the election year's year-end period \(published in the 2027 schedule file\) is missing/
    );
  });

  it("fails closed on no rows, conflicting dates, invalid dates and bad years", () => {
    expect(() => resolveNorthDakotaCandidateCycleWindow({ scheduleRows: rows(FILE_2027), electionYear: 2028 })).toThrow(
      /no reporting periods found for 2028 Election - Statewide/
    );
    expect(() =>
      resolveNorthDakotaCandidateCycleWindow({
        scheduleRows: rows([
          ...FILE_2026,
          "2026 Election - Statewide,2026 Reporting Cycle,2026 Year End Report,Campaign Financial Statement,Year End,2026-01-01,2026-12-30,2027-01-31",
          ...FILE_2027,
        ]),
        electionYear: 2026,
      })
    ).toThrow(/listed with conflicting dates/);
    expect(() =>
      resolveNorthDakotaCandidateCycleWindow({
        scheduleRows: rows(["2026 Election - Statewide,2026 Reporting Cycle,Broken,Campaign Financial Statement,Year End,2026-12-31,2026-01-01,2027-01-31"]),
        electionYear: 2026,
      })
    ).toThrow(/invalid dates/);
    expect(() => resolveNorthDakotaCandidateCycleWindow({ scheduleRows: [], electionYear: 1999 })).toThrow(/invalid election year/);
  });

  it("tests dates inclusively and accepts API timestamps", () => {
    const window = { windowStart: "2025-01-01", windowEnd: "2026-12-31" };
    expect(isNorthDakotaDateInWindow("2025-01-01", window)).toBe(true);
    expect(isNorthDakotaDateInWindow("2026-12-31T00:00:00", window)).toBe(true);
    expect(isNorthDakotaDateInWindow("2024-12-31", window)).toBe(false);
    expect(isNorthDakotaDateInWindow("2027-01-01", window)).toBe(false);
    expect(isNorthDakotaDateInWindow("garbage", window)).toBe(false);
  });
});
