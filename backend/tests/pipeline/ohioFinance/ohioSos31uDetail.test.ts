import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";

import { describe, expect, it } from "vitest";

import {
  ohioSos31uDetailUrl,
  parseOhioSos31uDetailCsv,
  parseOhioSos31uDetailTable,
  parseOhioSos31uDirection,
  reconcileOhioSos31uReport,
  OHIO_SOS_31U_DETAIL_HEADER,
  type OhioSos31uDetailRow,
} from "../../../src/pipeline/ohioFinance/ohioSos31uDetail.js";

const REPORT_KEY = "489367834";

async function readDetailFixture(): Promise<string> {
  return readFile(fileURLToPath(new URL("../../fixtures/ohioFinance/31u_detail_sample.csv", import.meta.url)), "latin1");
}

describe("ohioSos31uDetailUrl", () => {
  it("builds the session-less page-48 detail URL for a report key", () => {
    expect(ohioSos31uDetailUrl("489367834")).toBe(
      "https://www6.ohiosos.gov/ords/f?p=CFDISCLOSURE:48:::::P48_LISTTYPE,P48_REPORT_ID,P48_TYPE:simple,489367834,31U"
    );
  });

  it("rejects a non-numeric report key rather than building a bad URL", () => {
    expect(() => ohioSos31uDetailUrl("48936783X")).toThrow(/Invalid Ohio SoS 31-U report key/);
  });
});

describe("parseOhioSos31uDirection", () => {
  it("accepts only explicit support and oppose values", () => {
    expect(parseOhioSos31uDirection("SUPPORT")).toBe("support");
    expect(parseOhioSos31uDirection("Opposed")).toBe("oppose");
    expect(parseOhioSos31uDirection("")).toBeNull();
    expect(parseOhioSos31uDirection("ELECTIONEERING")).toBeNull();
    expect(parseOhioSos31uDirection(undefined)).toBeNull();
  });
});

describe("parseOhioSos31uDetailCsv", () => {
  it("parses the exported detail CSV, including quoted currency amounts", async () => {
    const rows = parseOhioSos31uDetailCsv(await readDetailFixture(), { reportKey: REPORT_KEY });

    expect(rows).toHaveLength(4);
    expect(rows[0]).toEqual({
      reportKey: REPORT_KEY,
      spenderCommitteeName: "V-PAC VICTORS NOT VICTIMS (SUPER PAC)",
      payeeName: null,
      payeeNonIndividual: "CREATIVE STRATEGIC SOLUTIONS LLC",
      payeeAddress: null,
      payeeCity: null,
      payeeState: null,
      payeeZip: null,
      reportType: "APRIL 15TH QUARTERLY",
      amountCents: 15_000_000,
      year: 2026,
      expendDateIso: "2026-01-05",
      eventDateIso: null,
      purpose: null,
      office: null,
      candidateNameOrBallotIssue: "VIVEK RAMASWAMY",
      direction: "support",
      rawDirection: "SUPPORT",
    });
  });

  it("leaves a blank direction unresolved instead of inferring one", async () => {
    const rows = parseOhioSos31uDetailCsv(await readDetailFixture(), { reportKey: REPORT_KEY });
    const blank = rows.filter((row) => row.direction === null);

    expect(blank).toHaveLength(1);
    expect(blank[0]?.rawDirection).toBeNull();
    expect(blank[0]?.amountCents).toBe(15_000_000);
  });

  it("keeps byte-identical duplicate filings as separate rows", async () => {
    // V-PAC filed three identical $150,000 rows; deduplicating them would
    // understate real spending (decision 3).
    const rows = parseOhioSos31uDetailCsv(await readDetailFixture(), { reportKey: REPORT_KEY });
    const identical = rows.filter(
      (row) => row.expendDateIso === "2026-01-05" && row.amountCents === 15_000_000 && row.direction === "support"
    );
    expect(identical).toHaveLength(2);
  });

  it("captures the office as absent on the rows that carry the most money", async () => {
    // Decision 5: office is blank on the largest targets, so it can only
    // confirm a match, never gate one.
    const rows = parseOhioSos31uDetailCsv(await readDetailFixture(), { reportKey: REPORT_KEY });
    expect(rows.filter((row) => row.office === null)).toHaveLength(3);
    expect(rows.find((row) => row.office !== null)?.office).toBe("HOUSE");
  });
});

describe("parseOhioSos31uDetailTable", () => {
  // The scraped page renders empty cells as a bare "-", unlike the CSV
  // export, which leaves them blank.
  const scrapedRow = [
    "-",
    "THE STRATEGY GROUP FOR MEDIA INC",
    "7669 STAGES LOOP",
    "DELAWARE",
    "OH",
    "43015",
    "12 DAY PRE PRIMARY (FEDERAL)",
    "$19,465.46",
    "2026",
    "04/08/2026",
    "-",
    "DIRECT MAIL",
    "OHIOANS FOR A HEALTHY ECONOMY ACTION FUND (SUPER PAC)",
    "HOUSE",
    "JASON STEPHENS",
    "OPPOSE",
  ];

  it("treats the APEX placeholder dash as an empty cell", () => {
    const rows = parseOhioSos31uDetailTable(
      { headers: [...OHIO_SOS_31U_DETAIL_HEADER], rows: [scrapedRow] },
      { reportKey: "500752705" }
    );

    expect(rows[0]).toMatchObject({
      payeeName: null,
      eventDateIso: null,
      payeeNonIndividual: "THE STRATEGY GROUP FOR MEDIA INC",
      amountCents: 1_946_546,
      expendDateIso: "2026-04-08",
      office: "HOUSE",
      candidateNameOrBallotIssue: "JASON STEPHENS",
      direction: "oppose",
    });
  });

  it("produces the same row from the scrape and the CSV export", async () => {
    const fromCsv = parseOhioSos31uDetailCsv(await readDetailFixture(), { reportKey: REPORT_KEY });
    const csvRow = fromCsv.find((row) => row.direction === "oppose");
    const [scraped] = parseOhioSos31uDetailTable(
      { headers: [...OHIO_SOS_31U_DETAIL_HEADER], rows: [scrapedRow] },
      { reportKey: REPORT_KEY }
    );
    expect(scraped).toEqual(csvRow);
  });

  it("rejects a table whose header no longer matches the pinned schema", () => {
    expect(() =>
      parseOhioSos31uDetailTable(
        { headers: [...OHIO_SOS_31U_DETAIL_HEADER.slice(0, 15)], rows: [] },
        { reportKey: REPORT_KEY }
      )
    ).toThrow(/table header does not match the pinned schema/);
  });

  it("rejects a row with the wrong cell count rather than shifting columns", () => {
    expect(() =>
      parseOhioSos31uDetailTable(
        { headers: [...OHIO_SOS_31U_DETAIL_HEADER], rows: [scrapedRow.slice(0, 15)] },
        { reportKey: REPORT_KEY }
      )
    ).toThrow(/has 15 cells; expected 16/);
  });
});

describe("reconcileOhioSos31uReport", () => {
  function row(overrides: Partial<OhioSos31uDetailRow>): OhioSos31uDetailRow {
    return {
      reportKey: REPORT_KEY,
      spenderCommitteeName: "SPENDER",
      payeeName: null,
      payeeNonIndividual: null,
      payeeAddress: null,
      payeeCity: null,
      payeeState: null,
      payeeZip: null,
      reportType: null,
      amountCents: 100,
      year: 2026,
      expendDateIso: "2026-01-05",
      eventDateIso: null,
      purpose: null,
      office: null,
      candidateNameOrBallotIssue: "A CANDIDATE",
      direction: "support",
      rawDirection: "SUPPORT",
      ...overrides,
    };
  }

  it("confirms a report whose detail total equals the annual total", async () => {
    const detailRows = parseOhioSos31uDetailCsv(await readDetailFixture(), { reportKey: REPORT_KEY });
    const result = reconcileOhioSos31uReport({
      reportKey: REPORT_KEY,
      annualTotalCents: 4 * 15_000_000 - 15_000_000 + 1_946_546,
      detailRows,
    });

    expect(result.detailTotalCents).toBe(46_946_546);
    expect(result.matches).toBe(true);
    expect(result.differenceCents).toBe(0);
  });

  it("splits directional dollars and excludes blank-direction rows", () => {
    const result = reconcileOhioSos31uReport({
      reportKey: REPORT_KEY,
      annualTotalCents: 600,
      detailRows: [
        row({ amountCents: 300, direction: "support" }),
        row({ amountCents: 200, direction: "oppose", rawDirection: "OPPOSE" }),
        row({ amountCents: 100, direction: null, rawDirection: null }),
      ],
    });

    expect(result).toMatchObject({
      detailTotalCents: 600,
      matches: true,
      supportCents: 300,
      opposeCents: 200,
      supportRowCount: 1,
      opposeRowCount: 1,
      excludedDirectionRowCount: 1,
      excludedDirectionCents: 100,
    });
  });

  it("flags a mismatch instead of quietly trusting the detail", () => {
    const result = reconcileOhioSos31uReport({
      reportKey: REPORT_KEY,
      annualTotalCents: 1000,
      detailRows: [row({ amountCents: 900 })],
    });

    expect(result.matches).toBe(false);
    expect(result.differenceCents).toBe(-100);
  });

  it("honours an explicit rounding tolerance", () => {
    const result = reconcileOhioSos31uReport({
      reportKey: REPORT_KEY,
      annualTotalCents: 1000,
      detailRows: [row({ amountCents: 999 })],
      toleranceCents: 1,
    });
    expect(result.matches).toBe(true);
  });

  it("counts unparseable amounts out of every total", () => {
    const result = reconcileOhioSos31uReport({
      reportKey: REPORT_KEY,
      annualTotalCents: 100,
      detailRows: [row({ amountCents: 100 }), row({ amountCents: null })],
    });

    expect(result.detailTotalCents).toBe(100);
    expect(result.unparseableAmountRowCount).toBe(1);
    expect(result.matches).toBe(true);
  });
});
