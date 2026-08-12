import { describe, expect, it, vi } from "vitest";
import type {
  EfileCalContributionRow,
  EfileCalS496Row,
  EfileCalS497Row,
  EfileCalSummaryRow,
  EfileCalWorkbook,
} from "../../../src/pipeline/efileCalFinance/efileCalWorkbookParser.js";
import {
  sanDiegoCityCycleYears,
  syncSanDiegoCityCandidateFinance,
} from "../../../src/pipeline/sanDiegoCityFinance/sanDiegoCityCandidateFinanceSync.js";

const FPPC_ID = "1234567";

const base = {
  filerId: FPPC_ID,
  filerName: "Jane Doe for City Council District 5 2026",
  reportNum: "000",
  eFilingId: "100",
  origEFilingId: "100",
  cmtteType: "C",
  rptDate: "2026-07-15",
  fromDate: "2026-01-01",
  thruDate: "2026-06-30",
  electDate: null,
};

function summaryRow(
  lineItem: string,
  amountACents: number,
  over: Partial<EfileCalSummaryRow> = {},
): EfileCalSummaryRow {
  return {
    ...base,
    formType: "F460",
    lineItem,
    amountACents,
    amountBCents: amountACents,
    amountCCents: null,
    ...over,
  };
}

function contributionRow(over: Partial<EfileCalContributionRow> = {}): EfileCalContributionRow {
  return {
    ...base,
    formType: "A",
    tranId: "A1",
    entityCd: "IND",
    contributorLastName: "Smith",
    contributorFirstName: "Ann",
    contributorOccupation: "Engineer",
    contributorEmployer: "Acme Inc",
    contributorSelfEmployed: false,
    amountCents: 50_000,
    cumulativeYtdCents: null,
    receiptDate: "2026-02-01",
    memo: false,
    ...over,
  };
}

function s496Row(over: Partial<EfileCalS496Row> = {}): EfileCalS496Row {
  return {
    ...base,
    filerId: "999",
    filerName: "Some PAC",
    cmtteType: null,
    formType: "S496",
    eFilingId: "200",
    origEFilingId: "200",
    tranId: "T1",
    amountCents: 12_345,
    expDate: "2026-03-01",
    candidateLastName: "Doe",
    candidateFirstName: "Jane",
    officeCd: "CCM",
    officeDscr: null,
    jurisCd: null,
    jurisDscr: "City of San Diego",
    distNo: "5",
    suppOppCd: "SUPPORT",
    memo: false,
    ...over,
  };
}

function s497Row(over: Partial<EfileCalS497Row> = {}): EfileCalS497Row {
  return {
    ...base,
    formType: "S497",
    tranId: "Q1",
    entityCd: "IND",
    entityLastName: "Smith",
    entityFirstName: "Ann",
    amountCents: 100_000,
    ctribDate: "2026-02-01",
    candidateLastName: null,
    candidateFirstName: null,
    officeCd: null,
    officeDscr: null,
    distNo: null,
    memo: false,
    ...over,
  };
}

// A consistent single-filing committee: F460 arithmetic holds, Schedule A
// reconciles to line 1 exactly, no loans, cash opens at zero.
function healthyWorkbook(over: Partial<EfileCalWorkbook> = {}): EfileCalWorkbook {
  return {
    summary: [
      summaryRow("1", 50_000),
      summaryRow("2", 0),
      summaryRow("3", 50_000),
      summaryRow("4", 0),
      summaryRow("5", 50_000),
      summaryRow("11", 20_000),
      summaryRow("12", 0),
      summaryRow("16", 30_000),
      summaryRow("19", 0),
    ],
    scheduleA: [contributionRow()],
    scheduleC: [],
    scheduleB1: [],
    scheduleD: [],
    s496: [s496Row()],
    s497: [],
    ...over,
  };
}

// db.query answers the anomaly SELECT (override) and the classification
// reads/writes with empty rows; the transaction client answers the link
// INSERT with an id.
function makeDb(storedSummary?: { total_raised: string | null; reported_through: string | null }) {
  const clientQuery = vi.fn().mockImplementation((sql: unknown) => {
    const s = String(sql);
    if (s.startsWith("INSERT INTO public.sdcity_candidate_finance_links"))
      return Promise.resolve({ rows: [{ id: "link-1" }] });
    return Promise.resolve({ rows: [] });
  });
  const release = vi.fn();
  const query = vi.fn().mockImplementation((sql: unknown) => {
    const s = String(sql);
    if (s.startsWith("SELECT summary.total_raised"))
      return Promise.resolve({ rows: storedSummary ? [storedSummary] : [] });
    return Promise.resolve({ rows: [] });
  });
  const connect = vi.fn().mockResolvedValue({ query: clientQuery, release });
  return { db: { query, connect }, query, clientQuery, connect };
}

const syncInput = {
  candidateId: "c",
  electionId: "e",
  electionYear: 2026,
  candidateDisplayName: "Jane Doe",
  officeName: "City Council Member" as const,
  seatNumber: 5,
  fppcId: FPPC_ID,
  now: new Date("2026-08-11T00:00:00Z"),
};

describe("sanDiegoCityCycleYears", () => {
  it("spans the election year and the year before", () => {
    expect(sanDiegoCityCycleYears(2026)).toEqual([2025, 2026]);
    expect(() => sanDiegoCityCycleYears(1990)).toThrow(/Implausible/);
  });
});

describe("San Diego candidate finance sync", () => {
  it("writes one full snapshot with exact dollar totals and outside groups", async () => {
    const { db, clientQuery, connect } = makeDb();
    const result = await syncSanDiegoCityCandidateFinance({
      db: db as never,
      ...syncInput,
      workbook: healthyWorkbook(),
    });
    expect(connect).toHaveBeenCalledTimes(1);
    const sql = clientQuery.mock.calls.map((call) => String(call[0]));
    expect(sql[0]).toBe("BEGIN");
    expect(sql.at(-1)).toBe("COMMIT");
    const summaryCall = clientQuery.mock.calls.find((call) =>
      String(call[0]).includes("sdcity_candidate_finance_summaries"),
    );
    // raised 500.00, spent 200.00, cash 300.00 — exact strings, and BOTH
    // always-on San Diego coverage notes reach the INSERT params.
    expect(summaryCall?.[1]).toEqual(
      expect.arrayContaining(["500.00", "200.00", "300.00"]),
    );
    expect(summaryCall?.[1]).toEqual(
      expect.arrayContaining([
        expect.stringContaining("may file on paper"),
        expect.stringContaining("Form 496 and Form 460 Schedule D"),
      ]),
    );
    const linkCall = clientQuery.mock.calls.find((call) =>
      String(call[0]).startsWith("INSERT INTO public.sdcity_candidate_finance_links"),
    );
    expect(linkCall?.[1]).toEqual(
      expect.arrayContaining([
        "efile_export",
        "Jane Doe for City Council District 5 2026",
        "JANE DOE",
      ]),
    );
    const outsideCall = clientQuery.mock.calls.find((call) =>
      String(call[0]).includes("sdcity_candidate_finance_outside_groups (link_id"),
    );
    expect(outsideCall?.[1]).toEqual(
      expect.arrayContaining(["999", "Some PAC", "support", "123.45", 1]),
    );
    expect(result).toMatchObject({
      linkWritten: true,
      totalRaisedCents: 50_000,
      totalSpentCents: 20_000,
      cashOnHandCents: 30_000,
      outsideSupportCents: 12_345,
      outsideOpposeCents: 0,
      reportedThrough: "2026-06-30",
      canonicalFilingCount: 1,
    });
    // The healthy case still carries the baselines and nothing more: no
    // uncovered prior activity, no vetoed name-matched rows.
    expect(result.directCoverageNote).toContain("may file on paper");
    expect(result.directCoverageNote).not.toContain("Form 460 disclosure statement");
    expect(result.outsideCoverageNote).toContain("paper filings are not included");
    expect(result.outsideCoverageNote).not.toContain("excluded");
  });

  it("discloses vetoed name-matched outside rows in the outside coverage note (Crosby case)", async () => {
    const { db } = makeDb();
    const result = await syncSanDiegoCityCandidateFinance({
      db: db as never,
      ...syncInput,
      workbook: healthyWorkbook({
        s496: [
          s496Row(),
          // Names this candidate but carries the wrong district (the live
          // CWFP-for-Crosby Dist_No=6 defect): excluded AND disclosed.
          s496Row({ tranId: "T2", eFilingId: "201", distNo: "6", amountCents: 8_194_67 }),
        ],
      }),
      dryRun: true,
    });
    expect(result.outsideSupportCents).toBe(12_345);
    expect(result.outsideCoverageNote).toContain(
      "1 expenditure naming this candidate was excluded",
    );
  });

  it("quarantines a committee with an unusable export row on a direct-side sheet; other committees' direct rows are irrelevant", async () => {
    // The live 2025 csd file carries a blank-Form_Type Major Donor block the
    // parser cannot type. Direct totals filter by the linked filer id, so a
    // direct-side unusable row matters only when it ties to THIS committee
    // by FPPC id or normalized committee name (blank-id rows carry the name).
    const { db, connect } = makeDb();
    const mine = {
      sheet: "F460-Summary",
      rowNumber: 994,
      reason: "missing Form_Type",
      filerId: null,
      filerName: "JANE DOE FOR CITY COUNCIL DISTRICT 5 2026",
      eFilingId: null,
    };
    await expect(
      syncSanDiegoCityCandidateFinance({
        db: db as never,
        ...syncInput,
        workbook: healthyWorkbook(),
        unusableRows: [mine],
      }),
    ).rejects.toThrow(/unusable export row.*missing Form_Type/);
    expect(connect).not.toHaveBeenCalled();
    const other = makeDb();
    await expect(
      syncSanDiegoCityCandidateFinance({
        db: other.db as never,
        ...syncInput,
        workbook: healthyWorkbook(),
        unusableRows: [
          { ...mine, filerName: "MARRIOTT INTERNATIONAL" },
          { ...mine, filerName: null },
        ],
      }),
    ).resolves.toMatchObject({ linkWritten: true });
  });

  it("quarantines EVERY candidate on an unusable S496 or Schedule D row (unattributable outside spending)", async () => {
    // An outside-sheet row belongs to some spender's expenditure about
    // somebody — unattributable precisely because it did not parse. Any
    // other committee's Filer_ID must NOT exempt it.
    for (const sheet of ["S496", "F460-D-ContribIndepExpn"]) {
      const { db, connect } = makeDb();
      await expect(
        syncSanDiegoCityCandidateFinance({
          db: db as never,
          ...syncInput,
          workbook: healthyWorkbook(),
          unusableRows: [
            {
              sheet,
              rowNumber: 42,
              reason: "missing Tran_ID",
              filerId: "999",
              filerName: "SOMEBODY ELSES PAC",
              eFilingId: null,
            },
          ],
        }),
      ).rejects.toThrow(/unusable outside-spending row.*missing Tran_ID/);
      expect(connect).not.toHaveBeenCalled();
    }
  });

  it("quarantines a committee with a blocking violation before any write", async () => {
    const { db, connect } = makeDb();
    const workbook = healthyWorkbook();
    // Drop the debts line: a missing core F460 line means the published
    // totals would silently omit a component.
    workbook.summary = workbook.summary.filter((row) => row.lineItem !== "19");
    await expect(
      syncSanDiegoCityCandidateFinance({ db: db as never, ...syncInput, workbook }),
    ).rejects.toThrow(/quarantined.*missing_summary_line/);
    expect(connect).not.toHaveBeenCalled();
  });

  it("publishes WITH a coverage note when the first filing opens with cash", async () => {
    const { db, clientQuery } = makeDb();
    const workbook = healthyWorkbook();
    for (const row of workbook.summary) {
      if (row.lineItem === "12") row.amountACents = 5_000;
      if (row.lineItem === "16") row.amountACents = 35_000;
    }
    const result = await syncSanDiegoCityCandidateFinance({
      db: db as never,
      ...syncInput,
      workbook,
    });
    expect(result.directCoverageNote).toContain("from 2026-01-01 onward");
    expect(result.directCoverageNote).toContain("not in the city's e-filing export");
    const summaryCall = clientQuery.mock.calls.find((call) =>
      String(call[0]).includes("sdcity_candidate_finance_summaries"),
    );
    expect(summaryCall?.[1]).toEqual(
      expect.arrayContaining([result.directCoverageNote]),
    );
  });

  it("writes a zero snapshot with a note for a registered committee with no Form 460 yet", async () => {
    const { db, clientQuery } = makeDb();
    // The committee exists in the export only through a 24-hour 497 report.
    const workbook = healthyWorkbook({
      summary: [],
      scheduleA: [],
      s496: [],
      s497: [s497Row()],
    });
    const result = await syncSanDiegoCityCandidateFinance({
      db: db as never,
      ...syncInput,
      workbook,
    });
    expect(result.directCoverageNote).toContain("has not filed a Form 460");
    expect(result).toMatchObject({
      totalRaisedCents: 0,
      totalSpentCents: 0,
      cashOnHandCents: null,
      canonicalFilingCount: 0,
    });
    const summaryCall = clientQuery.mock.calls.find((call) =>
      String(call[0]).includes("sdcity_candidate_finance_summaries"),
    );
    expect(summaryCall?.[1]).toEqual(expect.arrayContaining(["0.00"]));
  });

  it("aborts on Form 460 child rows with no usable summary (export inconsistency)", async () => {
    const { db, connect } = makeDb();
    // Schedule A rows prove a 460 was filed; missing summary rows are a
    // broken export, never affirmative zero activity.
    const workbook = healthyWorkbook({
      summary: [],
      scheduleA: [contributionRow()],
      s496: [],
    });
    await expect(
      syncSanDiegoCityCandidateFinance({ db: db as never, ...syncInput, workbook }),
    ).rejects.toThrow(/child-sheet rows but no usable Form 460 summary/);
    expect(connect).not.toHaveBeenCalled();
  });

  it("refuses to overwrite when the linked committee left the export entirely", async () => {
    const { db, connect } = makeDb();
    const workbook = healthyWorkbook({
      summary: [],
      scheduleA: [],
      s496: [],
      s497: [],
    });
    await expect(
      syncSanDiegoCityCandidateFinance({ db: db as never, ...syncInput, workbook }),
    ).rejects.toThrow(/no rows in the cycle export/);
    expect(connect).not.toHaveBeenCalled();
  });

  it("aborts when filing history goes backwards, bypass or not", async () => {
    const { db } = makeDb({
      total_raised: "500.00",
      reported_through: "2026-07-31",
    });
    await expect(
      syncSanDiegoCityCandidateFinance({
        db: db as never,
        ...syncInput,
        workbook: healthyWorkbook(),
        bypassAnomalyCheck: true,
      }),
    ).rejects.toThrow(/went backwards/);
  });

  it("aborts a raised collapse on an unchanged filing set unless bypassed", async () => {
    const stored = { total_raised: "6000.00", reported_through: "2026-06-30" };
    const first = makeDb(stored);
    await expect(
      syncSanDiegoCityCandidateFinance({
        db: first.db as never,
        ...syncInput,
        workbook: healthyWorkbook(),
      }),
    ).rejects.toThrow(/collapsed on an unchanged filing set/);
    expect(first.connect).not.toHaveBeenCalled();
    const second = makeDb(stored);
    await expect(
      syncSanDiegoCityCandidateFinance({
        db: second.db as never,
        ...syncInput,
        workbook: healthyWorkbook(),
        bypassAnomalyCheck: true,
      }),
    ).resolves.toMatchObject({ linkWritten: true });
  });

  it("computes without writing on dryRun", async () => {
    const { db, connect } = makeDb();
    const result = await syncSanDiegoCityCandidateFinance({
      db: db as never,
      ...syncInput,
      workbook: healthyWorkbook(),
      dryRun: true,
    });
    expect(result.linkWritten).toBe(false);
    expect(result.totalRaisedCents).toBe(50_000);
    expect(connect).not.toHaveBeenCalled();
  });

  it("applies only this election's paper-496 supplements to outside totals", async () => {
    const { db } = makeDb();
    const supplement = {
      electionYear: 2026,
      spenderFilerId: "941786",
      spenderName: "Some Paper PAC",
      candidateLastName: "Doe",
      candidateFirstName: "Jane",
      officeCd: "CCM" as const,
      jurisDscr: "City of San Diego",
      distNo: "5",
      direction: "OPPOSE" as const,
      amountCents: 5270_27,
      expenditureDate: "2026-05-11",
      eFilingId: "24823",
      sourceNote: "test",
    };
    const result = await syncSanDiegoCityCandidateFinance({
      db: db as never,
      ...syncInput,
      workbook: healthyWorkbook(),
      dryRun: true,
      paperSupplements: [
        supplement,
        // A different cycle's entry must never leak into 2026 totals.
        { ...supplement, electionYear: 2028, eFilingId: "99999" },
      ],
    });
    expect(result.outsideOpposeCents).toBe(5270_27);
    // The e-filed support row from healthyWorkbook is untouched.
    expect(result.outsideSupportCents).toBe(12_345);
    expect(result.outsideGroupCount).toBe(2);
    // With paper money in the totals, "paper filings are not included" would
    // be false — the note says what was included instead.
    expect(result.outsideCoverageNote).toContain(
      "plus 1 individually reviewed paper Form 496 expenditure;",
    );
    expect(result.outsideCoverageNote).not.toContain(
      "; paper filings are not included",
    );
  });

  it("keeps the paper-exclusion sentence when a supplement targets another candidate", async () => {
    // A pool entry that never entered THIS candidate's totals must not make
    // the note claim paper coverage for them.
    const { db } = makeDb();
    const result = await syncSanDiegoCityCandidateFinance({
      db: db as never,
      ...syncInput,
      workbook: healthyWorkbook(),
      dryRun: true,
      paperSupplements: [
        {
          electionYear: 2026,
          spenderFilerId: "941786",
          spenderName: "Some Paper PAC",
          candidateLastName: "Somebody",
          candidateFirstName: "Else",
          officeCd: "CCM" as const,
          jurisDscr: "City of San Diego",
          distNo: "5",
          direction: "OPPOSE" as const,
          amountCents: 5270_27,
          expenditureDate: "2026-05-11",
          eFilingId: "24823",
          sourceNote: "test",
        },
      ],
    });
    expect(result.outsideOpposeCents).toBe(0);
    expect(result.outsideCoverageNote).toContain("paper filings are not included");
    expect(result.outsideCoverageNote).not.toContain("individually reviewed");
  });
});
