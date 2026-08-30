import { readFile } from "node:fs/promises";
import { describe, expect, it } from "vitest";

import {
  MONTANA_CERS_CONTRIBUTION_EXPORT_HEADER,
  montanaCersJsonAmountToCents,
  parseMontanaCersCandidateSearchResults,
  parseMontanaCersContributionExport,
  parseMontanaCersCsvAmountCents,
  parseMontanaCersExpenditureExport,
  parseMontanaCersFinanceRepDetailList,
  parseMontanaCersIeCommitteeResults,
  parseMontanaCersIeSweepArtifact,
  parseMontanaCersIeTransactionRows,
  parseMontanaCersReportDetailArtifact,
  parseMontanaCersReportInventory,
} from "../../../src/pipeline/montanaFinance/montanaCersParsers.js";

const fixtures = new URL("../../fixtures/montanaFinance/", import.meta.url);

async function fixture(name: string): Promise<string> {
  return readFile(new URL(name, fixtures), "utf8");
}

describe("montanaCersParsers money", () => {
  it("parses CSV amounts to exact cents and rejects drift", () => {
    expect(parseMontanaCersCsvAmountCents("172.20")).toBe(17_220);
    expect(parseMontanaCersCsvAmountCents("10000")).toBe(1_000_000);
    expect(parseMontanaCersCsvAmountCents("0.3")).toBe(30);
    expect(parseMontanaCersCsvAmountCents("-25.00")).toBe(-2_500);
    for (const bad of ["1,234.56", "$5", "", "12.345", "1e3"]) {
      expect(() => parseMontanaCersCsvAmountCents(bad)).toThrow("Montana CERS");
    }
  });

  it("converts JSON float amounts to cents and rejects non-cent values", () => {
    expect(montanaCersJsonAmountToCents(17840.09, "primCashBeg")).toBe(1_784_009);
    expect(montanaCersJsonAmountToCents(0, "cashAmt")).toBe(0);
    expect(() => montanaCersJsonAmountToCents("12.34", "cashAmt")).toThrow("Non-numeric");
    expect(() => montanaCersJsonAmountToCents(Number.NaN, "cashAmt")).toThrow("Non-numeric");
    expect(() => montanaCersJsonAmountToCents(12.345, "cashAmt")).toThrow("not a cent value");
    // Sub-cent upstream precision drift must fail closed, not round away.
    expect(() => montanaCersJsonAmountToCents(12.34009, "cashAmt")).toThrow("not a cent value");
    // Real chain anchors survive the representation-only tolerance.
    expect(montanaCersJsonAmountToCents(9_999_999.99, "primCashBeg")).toBe(999_999_999);
  });
});

describe("montanaCersParsers CSV exports", () => {
  it("parses the contribution export fixture", async () => {
    const rows = parseMontanaCersContributionExport(await fixture("contributions-export-sanitized.csv"));
    expect(rows).toHaveLength(5);
    expect(rows[0]).toMatchObject({
      candidateId: 21020,
      entityName: "Doe, Jane",
      occupation: "Retired",
      employer: "Retired",
      lineItem: "Individual Contributions",
      amountCents: 17_220,
      electionType: "Primary",
      amountSubtype: "In-Kind",
    });
    // The 18-column shape covers every line-item family the plan classifies.
    expect(new Set(rows.map((row) => row.lineItem))).toEqual(
      new Set([
        "Individual Contributions",
        "Independent Committee Contributions",
        "Loans",
        "Debts and Loans Not Yet Paid",
      ])
    );
    const general = rows.filter((row) => row.electionType === "General");
    expect(general).toHaveLength(1);
    expect(general[0]!.amountCents).toBe(18_216);
  });

  it("parses the expenditure export fixture", async () => {
    const rows = parseMontanaCersExpenditureExport(await fixture("expenditures-export-sanitized.csv"));
    expect(rows).toHaveLength(2);
    expect(rows[0]!.entityName).toBe("Test Vendor");
    expect(rows[0]!.amountCents).toBe(59_060);
    expect(rows[1]!.electionType).toBe("General");
  });

  it("fails closed on empty bodies, HTML error pages, and header drift", async () => {
    expect(() => parseMontanaCersContributionExport("")).toThrow("Empty Montana CERS");
    expect(() => parseMontanaCersContributionExport("  \n ")).toThrow("Empty Montana CERS");
    expect(() => parseMontanaCersContributionExport("<html>IllegalStateException</html>")).toThrow(
      "not pipe-delimited"
    );
    const good = await fixture("contributions-export-sanitized.csv");
    expect(() => parseMontanaCersContributionExport(good.replace("Occupation", "Vocation"))).toThrow(
      "export header"
    );
    // A row with a stray pipe changes the column count — drift, not data.
    expect(() => parseMontanaCersContributionExport(`${good}extra|cells\n`)).toThrow("columns");
  });

  it("reassembles a logical row split by an embedded newline (unquoted export)", async () => {
    const good = await fixture("contributions-export-sanitized.csv");
    const lines = good.trimEnd().split("\n");
    const last = lines.pop()!;
    const cells = last.split("|");
    const expectedCents = parseMontanaCersCsvAmountCents(cells[14]!);
    // Break the contributor-address field (column 7) across two physical
    // lines — the shape observed live on an Eddy contributor row.
    cells[6] = `${cells[6]} Line One\nLine Two`;
    const rows = parseMontanaCersContributionExport(`${[...lines, cells.join("|")].join("\n")}\n`);
    expect(rows).toHaveLength(5);
    expect(rows.at(-1)!.amountCents).toBe(expectedCents);
  });

  it("keeps the pinned header at exactly 18 columns", () => {
    expect(MONTANA_CERS_CONTRIBUTION_EXPORT_HEADER).toHaveLength(18);
  });

  it("accepts live-observed nameless and zero-amount memo rows", async () => {
    // Candidate self-loans and payee-less bank fees carry a blank entity
    // name; a memo row (event ticket) leaves amount AND subtype empty.
    // All three shapes were live-hit in the Phase 3 full run.
    const good = await fixture("contributions-export-sanitized.csv");
    const lines = good.trimEnd().split("\n");
    const template = lines.at(-1)!.split("|");
    const nameless = [...template];
    nameless[5] = "";
    const memo = [...template];
    memo[5] = "BBER Economic Update";
    memo[14] = "";
    memo[16] = "";
    const rows = parseMontanaCersContributionExport(`${[...lines, nameless.join("|"), memo.join("|")].join("\n")}\n`);
    expect(rows.at(-2)!.entityName).toBeNull();
    expect(rows.at(-1)).toMatchObject({ amountCents: 0, amountSubtype: null });

    // An empty subtype on a row WITH money is still drift.
    const badSubtype = [...template];
    badSubtype[16] = "";
    expect(() =>
      parseMontanaCersContributionExport(`${[...lines, badSubtype.join("|")].join("\n")}\n`)
    ).toThrow("amount subtype");
  });
});

describe("montanaCersParsers report inventory", () => {
  it("parses the inventory fixture with chain anchors in cents", async () => {
    const rows = parseMontanaCersReportInventory(await fixture("report-inventory-sanitized.json"));
    expect(rows).toHaveLength(5);
    const filed = rows.find((row) => row.reportId === 76535)!;
    expect(filed).toMatchObject({
      formTypeCode: "C5",
      statusCode: "FILED",
      primCashBegCents: 1_784_009,
      genCashBegCents: 5_000,
      amendedDate: null,
    });
    const amended = rows.find((row) => row.reportId === 77491)!;
    expect(amended.statusCode).toBe("AMEND");
    expect(amended.amendedDate).toBe(1_787_255_665_000);
    const incorporated = rows.find((row) => row.reportId === 79526)!;
    expect(incorporated).toMatchObject({
      formTypeCode: "C7",
      statusCode: "INCRP",
      primCashBegCents: null,
      genCashBegCents: null,
    });
  });

  it("rejects a body without aaData", () => {
    expect(() => parseMontanaCersReportInventory("{}")).toThrow("no aaData");
    expect(() => parseMontanaCersReportInventory("")).toThrow("Empty Montana CERS");
  });

  it("fails closed when the DataTables page is truncated or uncounted", async () => {
    const body = JSON.parse(await fixture("report-inventory-sanitized.json")) as {
      aaData: unknown[];
      iTotalRecords: number;
      iTotalDisplayRecords: number;
    };
    // A page smaller than the reported total = the display-length cap bit.
    body.iTotalDisplayRecords = body.aaData.length + 90;
    body.iTotalRecords = body.aaData.length + 90;
    expect(() => parseMontanaCersReportInventory(JSON.stringify(body))).toThrow("truncated");
    // Missing counts are drift, not license to trust the page.
    expect(() => parseMontanaCersReportInventory(JSON.stringify({ aaData: [] }))).toThrow(
      "no iTotalDisplayRecords"
    );
  });
});

describe("montanaCersParsers report detail", () => {
  it("requires explicit classification flags", async () => {
    const artifact = JSON.parse(await fixture("report-detail-sanitized.json")) as {
      lists: Record<string, Record<string, unknown>[]>;
    };
    const row = { ...artifact.lists.individual![0]! };
    delete row["electioneeringInd"];
    // Schema drift dropping the flag must fail closed, never default to "N"
    // (that would classify electioneering money as ordinary spending).
    expect(() => parseMontanaCersFinanceRepDetailList(JSON.stringify([row]))).toThrow("electioneeringInd");
  });

  it("withholds JSON-ish bodies from parse errors but shows HTML error heads", () => {
    expect(() => parseMontanaCersFinanceRepDetailList('{"entityName":"Doe, Jane","entityAddress":"1 Main St"')).toThrow(
      /bytes withheld/
    );
    expect(() => parseMontanaCersFinanceRepDetailList('{"entityName":"Doe"')).not.toThrow(/Doe/);
    expect(() => parseMontanaCersFinanceRepDetailList("<html><title>Tomcat error</title></html>")).toThrow(
      /Tomcat error/
    );
  });

  it("accepts an untyped election side only on zero-amount placeholder rows", async () => {
    const artifact = JSON.parse(await fixture("report-detail-sanitized.json")) as {
      lists: Record<string, Record<string, unknown>[]>;
    };
    // Observed live (Eddy / Supreme Court): an all-zero Loans row with
    // amountTypeDescr "".
    const zeroRow = {
      ...artifact.lists.individual![0]!,
      amountTypeDescr: "",
      cashAmt: 0,
      inKindAmt: 0,
      totalAmt: 0,
      debtAmt: 0,
    };
    const [parsed] = parseMontanaCersFinanceRepDetailList(JSON.stringify([zeroRow]));
    expect(parsed!.amountTypeDescr).toBeNull();
    // Money without a side could land on the wrong side of the chain.
    const moneyRow = { ...zeroRow, cashAmt: 1, totalAmt: 1 };
    expect(() => parseMontanaCersFinanceRepDetailList(JSON.stringify([moneyRow]))).toThrow("amountTypeDescr");
  });

  it("distinguishes a legitimate empty list from an empty body", () => {
    expect(parseMontanaCersFinanceRepDetailList("[]")).toEqual([]);
    // CERS answers inapplicable listNames (e.g. expendIndependent on a
    // candidate report) with a ZERO-BYTE body — that is a failure, never an
    // empty list.
    expect(() => parseMontanaCersFinanceRepDetailList("")).toThrow("Empty Montana CERS");
  });

  it("parses the combined report-detail artifact fixture", async () => {
    const artifact = parseMontanaCersReportDetailArtifact(await fixture("report-detail-sanitized.json"));
    expect(artifact.reportId).toBe(76535);
    expect(artifact.lists.individual.length).toBeGreaterThan(0);
    expect(artifact.lists.payment).toEqual([]);
    const row = artifact.lists.individual[0]!;
    expect(row.amountTypeDescr).toBe("Primary");
    expect(row.entityName).toBe("Doe, Jane");
    expect(row.cashAmtCents + row.inKindAmtCents).toBe(row.totalAmtCents);
    expect(row.electioneeringInd).toBe("N");
  });

  it("fails closed on a missing or unknown detail list", async () => {
    const artifact = JSON.parse(await fixture("report-detail-sanitized.json")) as {
      reportId: number;
      lists: Record<string, unknown[]>;
    };
    const { loan: _loan, ...withoutLoan } = artifact.lists;
    expect(() =>
      parseMontanaCersReportDetailArtifact(JSON.stringify({ reportId: artifact.reportId, lists: withoutLoan }))
    ).toThrow("missing list loan");
    expect(() =>
      parseMontanaCersReportDetailArtifact(
        JSON.stringify({ reportId: artifact.reportId, lists: { ...artifact.lists, mystery: [] } })
      )
    ).toThrow("unknown lists: mystery");
  });
});

describe("montanaCersParsers candidate search", () => {
  it("parses DataTables candidate rows", () => {
    const body = JSON.stringify({
      iTotalRecords: 1,
      iTotalDisplayRecords: 1,
      aaData: [
        {
          candidateId: 21020,
          personDTO: { lastName: "Bedey", firstName: "David", middleInitial: "F." },
          electionYear: "2026",
          officeTitle: "Senate District No. 43",
          officeCode: "236",
          partyDescr: "Republican",
          candidateStatusDescr: "Amended",
          resCountyDescr: "Ravalli",
        },
      ],
    });
    const rows = parseMontanaCersCandidateSearchResults(body);
    expect(rows).toHaveLength(1);
    expect(rows[0]).toMatchObject({
      candidateId: 21020,
      lastName: "Bedey",
      electionYear: 2026,
      officeTitle: "Senate District No. 43",
    });
  });
});

describe("montanaCersParsers IE sweep surfaces", () => {
  it("parses IE committee results and trims names", async () => {
    const rows = parseMontanaCersIeCommitteeResults(await fixture("ie-committee-results-sanitized.json"));
    expect(rows).toEqual([
      expect.objectContaining({ committeeId: 100, committeeName: "Sanitized Prosperity PAC", committeeTypeDescr: "Independent", electionYear: null }),
      expect.objectContaining({ committeeId: 200, committeeName: "Sanitized Incidental Committee", electionYear: 2026 }),
    ]);
  });

  it("parses IE transaction rows, allowing a sideless all-zero placeholder only", async () => {
    const rows = parseMontanaCersIeTransactionRows(await fixture("ie-transactions-sanitized.json"));
    expect(rows).toHaveLength(3);
    expect(rows[0]).toMatchObject({
      transId: 1999001,
      transTypeDescr: "Independent Expenditure",
      amountTypeDescr: "Primary",
      cashAmtCents: 11_500,
      totalAmtCents: 11_500,
      candidateIssue: "SANITIZED NAME (SD-9)",
      electioneeringInd: "N",
    });
    expect(rows[1]).toMatchObject({ inKindAmtCents: 4_050, totalAmtCents: 4_050 });
    expect(rows[2]).toMatchObject({ amountTypeDescr: null, totalAmtCents: 0, candidateIssue: null });
  });

  it("fails closed on money-composition drift and on a sideless row carrying money", async () => {
    const body = JSON.parse(await fixture("ie-transactions-sanitized.json")) as {
      aaData: Record<string, unknown>[];
    };
    const drifted = structuredClone(body);
    drifted.aaData[0]!["totalAmt"] = 100.0;
    expect(() => parseMontanaCersIeTransactionRows(JSON.stringify(drifted))).toThrow("total");
    const sideless = structuredClone(body);
    sideless.aaData[2]!["cashAmt"] = 1.0;
    sideless.aaData[2]!["totalAmt"] = 1.0;
    expect(() => parseMontanaCersIeTransactionRows(JSON.stringify(sideless))).toThrow("amountTypeDescr");
  });

  it("validates the synthetic sweep artifact's identity invariants", async () => {
    const committeeSearch = JSON.parse(await fixture("ie-committee-results-sanitized.json")) as {
      aaData: unknown[];
    };
    const transactions = JSON.parse(await fixture("ie-transactions-sanitized.json")) as unknown;
    const empty = { sEcho: 1, iTotalRecords: 0, iTotalDisplayRecords: 0, aaData: [] };
    const good = {
      year: 2026,
      committeeSearch,
      committeeTransactions: [
        { committeeId: 100, resultCount: 3, list: transactions },
        { committeeId: 200, resultCount: 0, list: empty },
      ],
    };
    const sweep = parseMontanaCersIeSweepArtifact(JSON.stringify(good));
    expect(sweep.year).toBe(2026);
    expect(sweep.committees).toHaveLength(2);
    expect(sweep.transactionsByCommitteeId.get(100)).toHaveLength(3);
    expect(sweep.transactionsByCommitteeId.get(200)).toEqual([]);

    // resultCount is the viewFinancialEntities cross-check.
    const miscounted = structuredClone(good);
    miscounted.committeeTransactions[0]!.resultCount = 2;
    expect(() => parseMontanaCersIeSweepArtifact(JSON.stringify(miscounted))).toThrow("viewFinancialEntities said 2");

    const missing = structuredClone(good);
    missing.committeeTransactions.pop();
    expect(() => parseMontanaCersIeSweepArtifact(JSON.stringify(missing))).toThrow("missing committee 200");

    const unknown = structuredClone(good);
    unknown.committeeTransactions.push({ committeeId: 300, resultCount: 0, list: empty });
    expect(() => parseMontanaCersIeSweepArtifact(JSON.stringify(unknown))).toThrow("unknown committee 300");

    const repeated = structuredClone(good);
    repeated.committeeTransactions.push({ committeeId: 100, resultCount: 3, list: transactions });
    expect(() => parseMontanaCersIeSweepArtifact(JSON.stringify(repeated))).toThrow("repeats committee 100");
  });
});
