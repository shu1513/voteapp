import { describe, expect, it } from "vitest";
import {
  isPhoenixB6Page,
  parsePhoenixB6Entries,
  parsePhoenixReportCover,
  parsePhoenixReportPages,
  parsePhoenixScheduleEntries,
  phoenixCoverDateToIso,
  phoenixMoneyTokenToCents,
  type PhoenixPdfCell,
  type PhoenixPdfPage,
} from "../../../src/pipeline/phoenixFinance/phoenixReportPdfParser.js";

// Synthetic positioned-text pages: cell layouts mirror the live form
// coordinates the probe pinned (cash column x≈425, equity x≈505; B(6)
// blocks per the PAC-22-14 d7118529 dump). Raw PDFs are never committed —
// these fixtures are hand-derived from the live values.

let nextY = 10_000;
function line(cells: (string | PhoenixPdfCell)[], y?: number) {
  nextY -= 10;
  const resolved = cells.map((cell, index) =>
    typeof cell === "string" ? { text: cell, x: 40 + index * 60 } : cell,
  );
  return {
    y: y ?? nextY,
    cells: resolved,
    text: resolved.map((cell) => cell.text).join(" "),
  };
}
function page(pageNumber: number, lines: ReturnType<typeof line>[]): PhoenixPdfPage {
  return { pageNumber, lines };
}

function coverPage(over?: {
  officeSought?: string | null;
  receiptsCells?: PhoenixPdfCell[];
}): PhoenixPdfPage {
  return page(1, [
    line(["FINANCIAL SUMMARY (required)"]),
    ...(over?.officeSought === null
      ? []
      : [
          line([
            {
              text: `Office Sought: City Office: ${over?.officeSought ?? "Council Member District 4"}`,
              x: 40,
            },
          ]),
        ]),
    line([
      { text: "Q1 2026 Report: January 01, 2026 to March 31, 2026", x: 40 },
    ]),
    // The real form separates each anchor's amounts from the next anchor by
    // marker/label lines — nearbyMoney scans a 3-line window, so the
    // spacers below are load-bearing fixture realism.
    line([
      { text: "Committee value at the beginning of this reporting period", x: 40 },
      { text: "$259,595.51", x: 425 },
    ]),
    line([{ text: "(a)", x: 500 }]),
    line([{ text: "(b)", x: 500 }]),
    line(
      over?.receiptsCells !== undefined
        ? [{ text: "Total receipts this reporting period", x: 40 }, ...over.receiptsCells]
        : [
            { text: "Total receipts this reporting period", x: 40 },
            { text: "$72,621.00", x: 425 },
            { text: "$316,139.10", x: 505 },
          ],
    ),
    line([{ text: "(c)", x: 500 }]),
    line([{ text: "minus", x: 40 }]),
    line([
      { text: "Total disbursements this reporting period", x: 40 },
      { text: "$101,121.00", x: 425 },
      { text: "$150,000.00", x: 505 },
    ]),
    line([{ text: "(d)", x: 500 }]),
    line([{ text: "equals", x: 40 }]),
    line([
      { text: "Balance at close of reporting period", x: 40 },
      { text: "$231,095.51", x: 425 },
    ]),
  ]);
}

// Receipts summary consistent with the Hermes-derived fixture: 1(a)
// $41,695.00, 1(b) $26,441.00, 1(c) $5,485.00, others 0, 1(k) $73,621.00,
// 1(l) $1,000.00, 1(m) $72,621.00, no loans/other, line 13 $72,621.00.
// Every line carries an equity cell at x=505 the cash filter must ignore.
function receiptsPage(): PhoenixPdfPage {
  const cash = (label: string, amount: string) =>
    line([
      { text: label, x: 40 },
      { text: amount, x: 425 },
      { text: "$0.00", x: 505 },
    ]);
  return page(2, [
    line(["SUMMARY OF RECEIPTS (Schedule A)"]),
    cash("(a) In-State Individuals - More than $100", "$41,695.00"),
    cash("(b) In-State Individuals - $100 or Less", "$26,441.00"),
    cash("(c) Out-of-State Individuals", "$5,485.00"),
    cash("(d) Candidate Committees", "$0.00"),
    cash("(e) Political Action Committees", "$0.00"),
    cash("(f) Political Parties", "$0.00"),
    cash("(g) Partnerships", "$0.00"),
    cash("(h) Corporations & Limited Liability Companies", "$0.00"),
    cash("(i) Labor Organizations", "$0.00"),
    cash("(j) Candidate's Personal Monies", "$0.00"),
    cash("(k) Monetary Contributions Subtotal", "$73,621.00"),
    cash("(l) Refunds Given Back to Contributors", "$1,000.00"),
    cash("(m) Net Monetary Contributions", "$72,621.00"),
    cash("(e) Loans Subtotal", "$0.00"),
    cash("3. Rebates and Refunds Received", "$0.00"),
    cash("4. Interest Accrued", "$0.00"),
    cash("8. Joint Fundraising", "$0.00"),
    cash("9. Payments Received for Goods", "$0.00"),
    cash("11. Transfer In Surplus", "$0.00"),
    cash("12. Miscellaneous Receipts", "$0.00"),
    cash("13. Total Receipts", "$72,621.00"),
  ]);
}

function disbursementsPage(withLine6 = true): PhoenixPdfPage {
  return page(3, [
    line(["SUMMARY OF DISBURSEMENTS (Schedule B)"]),
    ...(withLine6
      ? [
          line([
            { text: "6. Independent Expenditures Made", x: 45 },
            { text: "$0.00", x: 421 },
          ]),
        ]
      : []),
    line([
      { text: "16. Total Disbursements (cash: add 1, 2(i), 3(f), 6-11 & 13-15)", x: 45 },
      { text: "$101,121.00", x: 421 },
      { text: "$0.00", x: 505 },
    ]),
  ]);
}

function a1aPage(): PhoenixPdfPage {
  return page(4, [
    line([
      {
        text: "MONETARY CONTRIBUTIONS RECEIVED FROM IN-STATE INDIVIDUALS - MORE THAN $100",
        x: 40,
      },
    ]),
    line([
      { text: "Name Date Contribution Received", x: 40 },
      { text: "$41,695.00", x: 360 },
      { text: "$41,695.00", x: 430 },
      { text: "$41,695.00", x: 500 },
    ]),
    line([
      { text: "Pat", x: 59 },
      { text: "Donor", x: 100 },
      { text: "01/15/2026", x: 260 },
    ]),
    line([{ text: "Occupation Employer", x: 59 }]),
    line([
      { text: "Attorney", x: 59 },
      { text: "Desert Law LLP", x: 190 },
    ]),
  ]);
}

function a1cPage(): PhoenixPdfPage {
  return page(5, [
    line([
      { text: "MONETARY CONTRIBUTIONS RECEIVED FROM OUT-OF-STATE INDIVIDUALS", x: 40 },
    ]),
    line([
      { text: "Name Date Contribution Received", x: 40 },
      { text: "$5,485.00", x: 360 },
    ]),
    line([
      { text: "Sam", x: 59 },
      { text: "Giver", x: 100 },
      { text: "02/20/2026", x: 260 },
    ]),
    line([{ text: "Occupation Employer", x: 59 }]),
    line([
      { text: "Teacher", x: 59 },
      { text: "Out of State School", x: 190 },
    ]),
  ]);
}

/** The live PAC-22-14 d7118529 layout, verbatim coordinates. */
function b6Page(over?: {
  supportedCells?: PhoenixPdfCell[];
  electionOfficeCells?: PhoenixPdfCell[];
}): PhoenixPdfPage {
  return page(6, [
    line([
      { text: "INDEPENDENT EXPENDITURES MADE:", x: 28 },
      { text: "SCHEDULE B(6)", x: 507 },
    ]),
    line([
      { text: "Recipient Name", x: 59 },
      { text: "Mode of Advertising(TV,mail,etc)", x: 260 },
    ]),
    line([
      { text: "$6,500.00", x: 369 },
      { text: "$6,500.00", x: 441 },
      { text: "$6,500.00", x: 510 },
    ]),
    line([
      { text: "TAB Services LLC", x: 59 },
      { text: "Digital Ad", x: 260 },
    ]),
    line([
      { text: "Candidate(s) Supported (including % Supported)", x: 59 },
      { text: "Candidate(s) Opposed (including % opposed)", x: 194 },
    ]),
    line(
      over?.supportedCells ?? [
        { text: "100", x: 194 },
        { text: "Credit", x: 377 },
      ],
    ),
    line([
      { text: "Date of First Publication, Display, Delivery, or Broadcast", x: 59 },
    ]),
    line([
      { text: "Election Month/Year", x: 194 },
      { text: "Office Sought", x: 261 },
    ]),
    line(
      over?.electionOfficeCells ?? [
        { text: "09/05/2024", x: 59 },
        { text: "2024", x: 194 },
        { text: "City Council", x: 261 },
      ],
    ),
    line([
      { text: "Enter total only if last page of schedule", x: 62 },
      { text: "$6,500.00", x: 441 },
    ]),
  ]);
}

describe("phoenixMoneyTokenToCents", () => {
  it("parses money tokens including parenthesised negatives", () => {
    expect(phoenixMoneyTokenToCents("$1,234.56")).toBe(123_456);
    expect(phoenixMoneyTokenToCents("($42.00)")).toBe(-4_200);
    expect(() => phoenixMoneyTokenToCents("1234")).toThrow(/Not a money token/);
  });
});

describe("phoenixCoverDateToIso", () => {
  it("converts cover dates and rejects garbage", () => {
    expect(phoenixCoverDateToIso("January 01, 2026")).toBe("2026-01-01");
    expect(phoenixCoverDateToIso("March 31, 2026")).toBe("2026-03-31");
    expect(() => phoenixCoverDateToIso("Not A Date")).toThrow(/Unparseable/);
  });
});

describe("parsePhoenixReportCover", () => {
  it("extracts the Hermes-derived fixture values", () => {
    expect(parsePhoenixReportCover(coverPage())).toEqual({
      reportName: "Q1 2026 Report",
      periodFrom: "2026-01-01",
      periodTo: "2026-03-31",
      officeSought: "Council Member District 4",
      beginCents: 25_959_551,
      receiptsPeriodCents: 7_262_100,
      receiptsCycleCents: 31_613_910,
      disbursementsPeriodCents: 10_112_100,
      disbursementsCycleCents: 15_000_000,
      closeCents: 23_109_551,
    });
  });

  it("reads a cover without Office Sought (PAC reports) as null", () => {
    expect(parsePhoenixReportCover(coverPage({ officeSought: null })).officeSought).toBeNull();
  });

  it("rejects a cover whose (b) line carries too many amounts", () => {
    expect(() =>
      parsePhoenixReportCover(
        coverPage({
          receiptsCells: [
            { text: "$1.00", x: 400 },
            { text: "$2.00", x: 450 },
            { text: "$3.00", x: 500 },
          ],
        }),
      ),
    ).toThrow(/Cover \(b\) expected 1-2 amounts/);
  });
});

describe("parsePhoenixScheduleEntries", () => {
  it("extracts amount, date, name, occupation, and employer", () => {
    expect(parsePhoenixScheduleEntries(a1aPage())).toEqual([
      {
        amountCents: 4_169_500,
        date: "01/15/2026",
        name: "Pat Donor",
        occupation: "Attorney",
        employer: "Desert Law LLP",
      },
    ]);
  });
});

describe("parsePhoenixB6Entries", () => {
  it("reproduces the pinned live entry: blank candidate names fail closed downstream", () => {
    const source = b6Page();
    expect(isPhoenixB6Page(source)).toBe(true);
    expect(parsePhoenixB6Entries(source)).toEqual([
      {
        amountCents: 650_000,
        supportedNames: [],
        supportedPercents: [],
        // The live "100" prints at the opposed block's x boundary — with no
        // name anywhere the entry stays unattributable either way.
        opposedNames: [],
        opposedPercents: [100],
        electionText: "2024",
        officeText: "City Council",
      },
    ]);
  });

  it("reads a filled supported block with name fragments and a percent", () => {
    const entries = parsePhoenixB6Entries(
      b6Page({
        supportedCells: [
          { text: "Jane", x: 59 },
          { text: "Doe", x: 95 },
          { text: "100", x: 160 },
        ],
        electionOfficeCells: [
          { text: "11/2026", x: 194 },
          { text: "City Council District 4", x: 261 },
        ],
      }),
    );
    expect(entries).toEqual([
      {
        amountCents: 650_000,
        supportedNames: ["Jane", "Doe"],
        supportedPercents: [100],
        opposedNames: [],
        opposedPercents: [],
        electionText: "11/2026",
        officeText: "City Council District 4",
      },
    ]);
  });

  it("throws when an entry has no period amount", () => {
    const broken = b6Page();
    broken.lines.splice(2, 1); // remove the amounts row
    expect(() => parsePhoenixB6Entries(broken)).toThrow(/has no period amount/);
  });
});

describe("parsePhoenixReportPages", () => {
  it("assembles the full report: cover, receipts, line 16/6, schedules", () => {
    const parsed = parsePhoenixReportPages([
      coverPage(),
      receiptsPage(),
      disbursementsPage(),
      a1aPage(),
      a1cPage(),
      b6Page(),
    ]);
    expect(parsed.cover.reportName).toBe("Q1 2026 Report");
    expect(parsed.receipts.line1.k).toBe(7_362_100);
    expect(parsed.receipts.line1.m).toBe(7_262_100);
    expect(parsed.receipts.line13CashCents).toBe(7_262_100);
    expect(parsed.line16CashCents).toBe(10_112_100);
    expect(parsed.line6CashCents).toBe(0);
    expect(parsed.a1aEntries).toHaveLength(1);
    expect(parsed.a1cEntries).toHaveLength(1);
    expect(parsed.b6Entries).toHaveLength(1);
  });

  it("reads a cover-only no-activity filing as a zero report (live CAN-22-10)", () => {
    // The form's own no-activity path: "only this cover page need be
    // filed", cover (b)/(c) both $0.00.
    const noActivity = coverPage();
    for (const target of ["Total receipts", "Total disbursements"]) {
      const row = noActivity.lines.find((line) => line.text.includes(target))!;
      row.cells = [
        row.cells[0]!,
        { text: "$0.00", x: 425 },
        { text: "$2,314.00", x: 505 },
      ];
      row.text = row.cells.map((cell) => cell.text).join(" ");
    }
    const closing = noActivity.lines.find((line) =>
      line.text.includes("Balance at close"),
    )!;
    closing.cells[1] = { text: "$259,595.51", x: 425 };
    closing.text = closing.cells.map((cell) => cell.text).join(" ");
    const parsed = parsePhoenixReportPages([noActivity]);
    expect(parsed.noActivity).toBe(true);
    expect(parsed.receipts.line1.m).toBe(0);
    expect(parsed.line16CashCents).toBe(0);
    expect(parsed.cover.closeCents).toBe(25_959_551);
  });

  it("still rejects a multi-page report that is merely missing its summary pages", () => {
    expect(() =>
      parsePhoenixReportPages([coverPage(), a1aPage()]),
    ).toThrow(/no SUMMARY OF RECEIPTS/);
  });

  it("reads a missing line 6 as null and a missing summary page as an error", () => {
    const parsed = parsePhoenixReportPages([
      coverPage(),
      receiptsPage(),
      disbursementsPage(false),
    ]);
    expect(parsed.line6CashCents).toBeNull();
    expect(() => parsePhoenixReportPages([coverPage(), receiptsPage()])).toThrow(
      /no SUMMARY OF DISBURSEMENTS/,
    );
  });
});
