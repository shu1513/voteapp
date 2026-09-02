import { describe, expect, it } from "vitest";

import { parseWestVirginiaReportingScheduleCsv } from "../../../src/pipeline/westVirginiaFinance/westVirginiaCfrsCsv.js";
import {
  isWestVirginiaDateInWindow,
  resolveWestVirginiaCandidateCycleWindow,
  westVirginiaCycleWindowYears,
  westVirginiaScheduleYearsForElection,
} from "../../../src/pipeline/westVirginiaFinance/westVirginiaReportingCycleWindows.js";

// Rows copied from the live 2025 / 2026 / 2027 Reporting Schedules files
// (2026-09-01), trimmed to the cycles that matter for the rule.
const HEADER = "ElectionName,ReportingCycle,ReportingPeriodDescription,FormType,ReportType,BeginDate,Enddate,DueDate";
const ROWS_2025 = [
  "2026 Election,2026 Candidate Election Cycle,2025 3rd Quarter Report,Campaign Financial Statement,Quarterly,2025-07-01,2025-09-30,2025-10-07",
  "2026 Election,2026 Committee Election Cycle,2025 3rd Quarter Report,Campaign Financial Statement,Quarterly,2025-07-01,2025-09-30,2025-10-07",
  "2028 Election,2028 Candidate Election,2025 3rd Quarter Report,Campaign Financial Statement,Quarterly,2025-07-01,2025-09-30,2025-10-07",
];
const ROWS_2026 = [
  "2026 Election,2026 Candidate Election Cycle,2025 4th Quarter Report,Campaign Financial Statement,Quarterly,2025-10-01,2025-12-31,2026-01-07",
  "2026 Election,2026 Candidate Election Cycle,2026 1st Quarter Report,Campaign Financial Statement,Quarterly,2026-01-01,2026-03-31,2026-04-07",
  "2026 Non-Partisan Election During Primary,2026 Non-Partisan Candidate Election Cycle,2026 1st Quarter Report,Campaign Financial Statement,Quarterly,2026-01-01,2026-03-31,2026-04-07",
  "2026 Election,2026 Candidate Election Cycle,2026 2nd Quarter Report,Campaign Financial Statement,Quarterly,2026-04-27,2026-06-30,2026-07-07",
  "2026 Election,2026 Candidate Election Cycle,2026 3rd Quarter Report,Campaign Financial Statement,Quarterly,2026-07-01,2026-09-30,2026-10-07",
  "2026 Election,2026 Candidate Election Cycle,2026 General Report,Campaign Financial Statement,General,2026-10-01,2026-10-18,2026-10-23",
  "2026 Election,2026 Candidate Election Cycle,2026 Primary Report,Campaign Financial Statement,Primary,2026-04-01,2026-04-26,2026-05-01",
];
const ROWS_2027 = [
  "2026 Election,2026 Candidate Election Cycle,2026 4th Quarter Report,Campaign Financial Statement,Quarterly,2026-10-19,2026-12-31,2027-01-07",
  "2028 Election,2028 Candidate Election,2027 1st Quarter Report,Campaign Financial Statement,Quarterly,2027-01-01,2027-03-31,2027-04-07",
];

function rows(lines: string[]) {
  return parseWestVirginiaReportingScheduleCsv(`${HEADER}\n${lines.join("\n")}\n`).rows;
}

describe("resolveWestVirginiaCandidateCycleWindow", () => {
  it("unions the candidate cycle's periods across the three schedule files", () => {
    expect(westVirginiaScheduleYearsForElection(2026)).toEqual([2025, 2026, 2027]);
    const window = resolveWestVirginiaCandidateCycleWindow({
      scheduleRows: rows([...ROWS_2025, ...ROWS_2026, ...ROWS_2027]),
      electionYear: 2026,
    });
    expect(window.reportingCycle).toBe("2026 Candidate Election Cycle");
    expect(window.windowStart).toBe("2025-07-01");
    expect(window.windowEnd).toBe("2026-12-31");
    expect(window.periods.map((period) => period.description)).toEqual([
      "2025 3rd Quarter Report",
      "2025 4th Quarter Report",
      "2026 1st Quarter Report",
      "2026 Primary Report",
      "2026 2nd Quarter Report",
      "2026 3rd Quarter Report",
      "2026 General Report",
      "2026 4th Quarter Report",
    ]);
    expect(westVirginiaCycleWindowYears(window)).toEqual([2025, 2026]);
  });

  it("ignores committee and non-partisan cycles and other election years", () => {
    const window = resolveWestVirginiaCandidateCycleWindow({ scheduleRows: rows(ROWS_2025), electionYear: 2026 });
    expect(window.periods).toHaveLength(1);
    expect(() => resolveWestVirginiaCandidateCycleWindow({ scheduleRows: rows(ROWS_2025), electionYear: 2030 })).toThrow(
      /no reporting periods found for 2030 Candidate Election Cycle/
    );
  });

  it("collapses a period repeated across files and rejects conflicting or overlapping ones", () => {
    const repeated = rows([...ROWS_2026, ROWS_2026[0]!]);
    expect(resolveWestVirginiaCandidateCycleWindow({ scheduleRows: repeated, electionYear: 2026 }).periods).toHaveLength(6);
    const conflicting = rows([
      ...ROWS_2026,
      "2026 Election,2026 Candidate Election Cycle,2026 General Report,Campaign Financial Statement,General,2026-10-01,2026-10-20,2026-10-23",
    ]);
    expect(() => resolveWestVirginiaCandidateCycleWindow({ scheduleRows: conflicting, electionYear: 2026 })).toThrow(
      /conflicting dates/
    );
    const overlapping = rows([
      ...ROWS_2026,
      "2026 Election,2026 Candidate Election Cycle,2026 Special Report,Campaign Financial Statement,Special,2026-10-10,2026-10-30,2026-11-01",
    ]);
    expect(() => resolveWestVirginiaCandidateCycleWindow({ scheduleRows: overlapping, electionYear: 2026 })).toThrow(
      /periods overlap/
    );
  });

  it("tests dates inclusively in both ISO shapes", () => {
    const window = { windowStart: "2025-07-01", windowEnd: "2026-12-31" };
    expect(isWestVirginiaDateInWindow("2025-07-01", window)).toBe(true);
    expect(isWestVirginiaDateInWindow("2026-12-31T00:00:00", window)).toBe(true);
    expect(isWestVirginiaDateInWindow("2025-06-30", window)).toBe(false);
    expect(isWestVirginiaDateInWindow("garbage", window)).toBe(false);
  });
});
