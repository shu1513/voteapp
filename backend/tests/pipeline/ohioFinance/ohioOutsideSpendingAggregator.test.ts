import { describe, expect, it } from "vitest";

import { aggregateOhioOutsideSpending } from "../../../src/pipeline/ohioFinance/ohioOutsideSpendingAggregator.js";
import type { OhioSos31uDetailRow } from "../../../src/pipeline/ohioFinance/ohioSos31uDetail.js";
import type {
  OhioSosCoverPageRow,
  OhioSosExpenditureRow,
} from "../../../src/pipeline/ohioFinance/ohioSosBulkFiles.js";

function annualRow(overrides: Partial<OhioSosExpenditureRow> = {}): OhioSosExpenditureRow {
  return {
    committeeName: "MEGA PAC (SUPER PAC)",
    masterKey: "16182",
    reportYear: 2026,
    reportKey: "512315395",
    reportDescription: "APRIL 15TH QUARTERLY",
    shortDescription: "31-U  Ind Exp by committee",
    payeeFirstName: null,
    payeeMiddleName: null,
    payeeLastName: null,
    payeeSuffix: null,
    nonIndividual: "SOME VENDOR LLC",
    address: null,
    city: null,
    state: null,
    zip: null,
    expendDateIso: "2026-02-01",
    amountCents: null,
    eventDateIso: null,
    purpose: null,
    inkind: null,
    candidateFirstName: null,
    candidateLastName: null,
    office: null,
    district: null,
    party: null,
    ...overrides,
  };
}

function detailRow(overrides: Partial<OhioSos31uDetailRow> = {}): OhioSos31uDetailRow {
  return {
    reportKey: "512315395",
    spenderCommitteeName: "MEGA PAC (SUPER PAC)",
    payeeName: null,
    payeeNonIndividual: "SOME VENDOR LLC",
    payeeAddress: null,
    payeeCity: null,
    payeeState: null,
    payeeZip: null,
    reportType: "APRIL 15TH QUARTERLY",
    amountCents: 100_00,
    year: 2026,
    expendDateIso: "2026-02-01",
    eventDateIso: null,
    purpose: null,
    office: null,
    candidateNameOrBallotIssue: "AMY ACTON",
    direction: "support",
    rawDirection: "SUPPORT",
    ...overrides,
  };
}

function coverRow(overrides: Partial<OhioSosCoverPageRow> = {}): OhioSosCoverPageRow {
  return {
    committeeName: "MEGA PAC (SUPER PAC)",
    masterKey: "16182",
    candidateFirstName: null,
    candidateLastName: null,
    reportKey: "512315395",
    reportYear: 2026,
    reportDescription: "PRE-PRIMARY",
    dateReportFiledIso: "2026-04-23",
    amountForwardCents: null,
    totalContributionsCents: null,
    totalOtherIncomeCents: null,
    totalFundsCents: null,
    totalExpendituresCents: null,
    balanceOnHandCents: null,
    valueInkindReceivedCents: null,
    valueInkindMadeCents: null,
    outstandingLoansOwedCents: null,
    outstandingDebtOwedCents: null,
    outstandingLoansToCents: null,
    valueIndependentExpendituresCents: null,
    ...overrides,
  };
}

const ACTON = { candidateKey: "acton", candidateName: "Amy Acton", officeName: "Governor" };
const RAMASWAMY = { candidateKey: "ramaswamy", candidateName: "Vivek Ramaswamy", officeName: "Governor" };
const STEPHENS = {
  candidateKey: "stephens",
  candidateName: "Jason Stephens",
  officeName: "State Lower Chamber Legislator",
};

describe("aggregateOhioOutsideSpending", () => {
  it("attributes directional detail rows to candidates with the spender pinned from the annual file", () => {
    const result = aggregateOhioOutsideSpending({
      electionYear: 2026,
      annualExpenditureRows: [
        annualRow({ amountCents: 300_00 }),
        annualRow({ amountCents: 150_00 }),
        // Non-31-U row is ignored even with a matching report key.
        annualRow({ shortDescription: "31-B Stmt of Expenditures", amountCents: 999_00 }),
      ],
      detailReports: [
        {
          reportKey: "512315395",
          rows: [
            detailRow({ amountCents: 300_00, candidateNameOrBallotIssue: "AMY ACTON", direction: "oppose", rawDirection: "OPPOSE" }),
            detailRow({ amountCents: 150_00, candidateNameOrBallotIssue: "VIVEK RAMASWAMY" }),
          ],
        },
      ],
      coverRows: [],
      candidates: [ACTON, RAMASWAMY, STEPHENS],
      sourceUrl: "https://example.test/31u",
    });

    expect(result.quarantinedReportCount).toBe(0);
    expect(result.attributedRowCount).toBe(2);
    expect(result.attributedCents).toBe(450_00);
    expect(result.candidates).toEqual([
      {
        candidateKey: "acton",
        supportTotal: 0,
        opposeTotal: 300,
        groups: [
          {
            committeeId: "16182",
            committeeName: "MEGA PAC (SUPER PAC)",
            supportOppose: "oppose",
            amount: 300,
            sourceUrl: "https://example.test/31u",
          },
        ],
      },
      {
        candidateKey: "ramaswamy",
        supportTotal: 150,
        opposeTotal: 0,
        groups: [
          {
            committeeId: "16182",
            committeeName: "MEGA PAC (SUPER PAC)",
            supportOppose: "support",
            amount: 150,
            sourceUrl: "https://example.test/31u",
          },
        ],
      },
    ]);
  });

  it("counts identical rows separately — legitimate repeats are never deduplicated", () => {
    const result = aggregateOhioOutsideSpending({
      electionYear: 2026,
      annualExpenditureRows: [annualRow({ amountCents: 150_000_00 }), annualRow({ amountCents: 150_000_00 })],
      detailReports: [
        {
          reportKey: "512315395",
          rows: [detailRow({ amountCents: 150_000_00 }), detailRow({ amountCents: 150_000_00 })],
        },
      ],
      coverRows: [],
      candidates: [ACTON],
      sourceUrl: null,
    });
    expect(result.candidates[0]?.supportTotal).toBe(300_000);
    expect(result.attributedRowCount).toBe(2);
  });

  it("excludes blank-direction rows from attribution while they still reconcile the total", () => {
    const result = aggregateOhioOutsideSpending({
      electionYear: 2026,
      annualExpenditureRows: [annualRow({ amountCents: 250_00 })],
      detailReports: [
        {
          reportKey: "512315395",
          rows: [
            detailRow({ amountCents: 100_00 }),
            detailRow({ amountCents: 150_00, direction: null, rawDirection: null }),
          ],
        },
      ],
      coverRows: [],
      candidates: [ACTON],
    });
    expect(result.quarantinedReportCount).toBe(0);
    expect(result.candidates[0]?.supportTotal).toBe(100);
    const report = result.reports[0]!;
    expect(report.reconciliation.excludedDirectionRowCount).toBe(1);
    expect(report.reconciliation.excludedDirectionCents).toBe(150_00);
  });

  it("quarantines unmatched targets (ballot issues, junk, unknown people) with amounts", () => {
    const result = aggregateOhioOutsideSpending({
      electionYear: 2026,
      annualExpenditureRows: [annualRow({ amountCents: 300_00 })],
      detailReports: [
        {
          reportKey: "512315395",
          rows: [
            detailRow({ amountCents: 100_00, candidateNameOrBallotIssue: "Weisburg for Sheriff" }),
            detailRow({ amountCents: 150_00, candidateNameOrBallotIssue: "N/A" }),
            detailRow({ amountCents: 50_00, candidateNameOrBallotIssue: null }),
          ],
        },
      ],
      coverRows: [],
      candidates: [ACTON],
    });
    expect(result.candidates).toHaveLength(0);
    expect(result.attributedRowCount).toBe(0);
    expect(result.unmatchedTargets).toEqual([
      { value: "N/A", rowCount: 1, amountCents: 150_00 },
      { value: "Weisburg for Sheriff", rowCount: 1, amountCents: 100_00 },
      { value: "(blank)", rowCount: 1, amountCents: 50_00 },
    ]);
  });

  it("quarantines a name matching two candidates unless the row's office disambiguates", () => {
    const twins = [
      { candidateKey: "house-lee", candidateName: "Jordan Lee", officeName: "State Lower Chamber Legislator" },
      { candidateKey: "senate-lee", candidateName: "Jordan Lee", officeName: "State Senator" },
    ];
    const ambiguous = aggregateOhioOutsideSpending({
      electionYear: 2026,
      annualExpenditureRows: [annualRow({ amountCents: 100_00 })],
      detailReports: [{ reportKey: "512315395", rows: [detailRow({ candidateNameOrBallotIssue: "JORDAN LEE" })] }],
      coverRows: [],
      candidates: twins,
    });
    expect(ambiguous.candidates).toHaveLength(0);
    expect(ambiguous.ambiguousTargets).toEqual([{ value: "JORDAN LEE", rowCount: 1, amountCents: 100_00 }]);

    const confirmed = aggregateOhioOutsideSpending({
      electionYear: 2026,
      annualExpenditureRows: [annualRow({ amountCents: 100_00 })],
      detailReports: [
        { reportKey: "512315395", rows: [detailRow({ candidateNameOrBallotIssue: "JORDAN LEE", office: "HOUSE" })] },
      ],
      coverRows: [],
      candidates: twins,
    });
    expect(confirmed.candidates.map((candidate) => candidate.candidateKey)).toEqual(["house-lee"]);
  });

  it("rejects a unique name match whose stated office contradicts the candidate's", () => {
    // A US Senate row naming JON HUSTED must not attach to a same-named
    // gubernatorial candidate: the stated office contradicts.
    const result = aggregateOhioOutsideSpending({
      electionYear: 2026,
      annualExpenditureRows: [annualRow({ amountCents: 100_00 })],
      detailReports: [
        {
          reportKey: "512315395",
          rows: [detailRow({ candidateNameOrBallotIssue: "PAT MILLER", office: "SENATE" })],
        },
      ],
      coverRows: [],
      candidates: [{ candidateKey: "miller", candidateName: "Pat Miller", officeName: "Governor" }],
    });
    expect(result.candidates).toHaveLength(0);
    expect(result.unmatchedTargets).toEqual([{ value: "PAT MILLER", rowCount: 1, amountCents: 100_00 }]);
  });

  it("applies the strict name-conflict guard to targets", () => {
    const result = aggregateOhioOutsideSpending({
      electionYear: 2026,
      annualExpenditureRows: [annualRow({ amountCents: 100_00 })],
      detailReports: [
        { reportKey: "512315395", rows: [detailRow({ candidateNameOrBallotIssue: "JANE ANN DOE" })] },
      ],
      coverRows: [],
      candidates: [{ candidateKey: "doe", candidateName: "Jane Marie Doe", officeName: "Governor" }],
    });
    expect(result.candidates).toHaveLength(0);
    expect(result.unmatchedTargets[0]?.value).toBe("JANE ANN DOE");
  });

  it("quarantines a report whose detail total disagrees with the annual bulk total", () => {
    const result = aggregateOhioOutsideSpending({
      electionYear: 2026,
      annualExpenditureRows: [annualRow({ amountCents: 999_00 })],
      detailReports: [{ reportKey: "512315395", rows: [detailRow({ amountCents: 100_00 })] }],
      coverRows: [],
      candidates: [ACTON],
    });
    expect(result.quarantinedReportCount).toBe(1);
    expect(result.reports[0]?.quarantineReason).toBe("annual_detail_mismatch");
    expect(result.candidates).toHaveLength(0);
  });

  it("quarantines on a cover-page IE mismatch but not on a missing cover row", () => {
    const mismatch = aggregateOhioOutsideSpending({
      electionYear: 2026,
      annualExpenditureRows: [annualRow({ amountCents: 100_00 })],
      detailReports: [{ reportKey: "512315395", rows: [detailRow()] }],
      coverRows: [coverRow({ valueIndependentExpendituresCents: 55_00 })],
      candidates: [ACTON],
    });
    expect(mismatch.reports[0]?.quarantineReason).toBe("cover_mismatch");
    expect(mismatch.reports[0]?.coverIeCents).toBe(55_00);
    expect(mismatch.candidates).toHaveLength(0);

    const noCover = aggregateOhioOutsideSpending({
      electionYear: 2026,
      annualExpenditureRows: [annualRow({ amountCents: 100_00 })],
      detailReports: [{ reportKey: "512315395", rows: [detailRow()] }],
      coverRows: [coverRow({ reportKey: "111", valueIndependentExpendituresCents: 55_00 })],
      candidates: [ACTON],
    });
    expect(noCover.quarantinedReportCount).toBe(0);
    expect(noCover.reports[0]?.coverIeCents).toBeNull();
    expect(noCover.candidates).toHaveLength(1);
  });

  it("accepts a matching cover row and reports it", () => {
    const result = aggregateOhioOutsideSpending({
      electionYear: 2026,
      annualExpenditureRows: [annualRow({ amountCents: 100_00 })],
      detailReports: [{ reportKey: "512315395", rows: [detailRow()] }],
      coverRows: [coverRow({ valueIndependentExpendituresCents: 100_00 })],
      candidates: [ACTON],
    });
    expect(result.quarantinedReportCount).toBe(0);
    expect(result.reports[0]?.coverIeCents).toBe(100_00);
  });

  it("reads a blank IE cell on an otherwise-filled cover row as zero and quarantines", () => {
    // A present cover page that claims no independent spending contradicts
    // a nonzero annual 31-U total — that must hit the gate, not bypass it.
    const result = aggregateOhioOutsideSpending({
      electionYear: 2026,
      annualExpenditureRows: [annualRow({ amountCents: 100_00 })],
      detailReports: [{ reportKey: "512315395", rows: [detailRow()] }],
      coverRows: [
        coverRow({ totalExpendituresCents: 100_00, balanceOnHandCents: 0, valueIndependentExpendituresCents: null }),
      ],
      candidates: [ACTON],
    });
    expect(result.reports[0]?.coverIeCents).toBe(0);
    expect(result.reports[0]?.quarantineReason).toBe("cover_mismatch");
    expect(result.candidates).toHaveLength(0);
  });

  it("ignores a fully blank cover row exactly like a missing one", () => {
    // Every money column blank = e-filing damage, not a claim of zero.
    const result = aggregateOhioOutsideSpending({
      electionYear: 2026,
      annualExpenditureRows: [annualRow({ amountCents: 100_00 })],
      detailReports: [{ reportKey: "512315395", rows: [detailRow()] }],
      coverRows: [coverRow()],
      candidates: [ACTON],
    });
    expect(result.quarantinedReportCount).toBe(0);
    expect(result.reports[0]?.coverIeCents).toBeNull();
    expect(result.candidates).toHaveLength(1);
  });

  it("quarantines a detail report whose key never appears in the annual files", () => {
    const result = aggregateOhioOutsideSpending({
      electionYear: 2026,
      annualExpenditureRows: [],
      detailReports: [{ reportKey: "512315395", rows: [detailRow()] }],
      coverRows: [],
      candidates: [ACTON],
    });
    expect(result.reports[0]?.quarantineReason).toBe("unknown_report_key");
    expect(result.reports[0]?.spenderCommitteeId).toBeNull();
    expect(result.candidates).toHaveLength(0);
  });

  it("quarantines a report key claimed by two different spender master keys", () => {
    const result = aggregateOhioOutsideSpending({
      electionYear: 2026,
      annualExpenditureRows: [
        annualRow({ amountCents: 50_00 }),
        annualRow({ amountCents: 50_00, masterKey: "999" }),
      ],
      detailReports: [{ reportKey: "512315395", rows: [detailRow()] }],
      coverRows: [],
      candidates: [ACTON],
    });
    expect(result.reports[0]?.quarantineReason).toBe("invalid_spender");
    expect(result.candidates).toHaveLength(0);
  });

  it("lists annual report keys with no fetched detail — their money stays invisible", () => {
    const result = aggregateOhioOutsideSpending({
      electionYear: 2026,
      annualExpenditureRows: [annualRow(), annualRow({ reportKey: "600000001", amountCents: 42_00 })].map(
        (row, index) => (index === 0 ? { ...row, amountCents: 100_00 } : row)
      ),
      detailReports: [{ reportKey: "512315395", rows: [detailRow()] }],
      coverRows: [],
      candidates: [ACTON],
    });
    expect(result.missingDetailReportKeys).toEqual(["600000001"]);
  });

  it("counts skipped annual rows (blank amount, non-numeric master key)", () => {
    const result = aggregateOhioOutsideSpending({
      electionYear: 2026,
      annualExpenditureRows: [
        annualRow({ amountCents: null }),
        annualRow({ masterKey: "ABC", amountCents: 10_00 }),
        annualRow({ amountCents: 100_00 }),
      ],
      detailReports: [{ reportKey: "512315395", rows: [detailRow()] }],
      coverRows: [],
      candidates: [ACTON],
    });
    expect(result.annualSkippedRowCount).toBe(2);
    expect(result.quarantinedReportCount).toBe(0);
  });

  it("throws on a duplicate detail report key", () => {
    expect(() =>
      aggregateOhioOutsideSpending({
        electionYear: 2026,
        annualExpenditureRows: [annualRow({ amountCents: 100_00 })],
        detailReports: [
          { reportKey: "512315395", rows: [detailRow()] },
          { reportKey: "512315395", rows: [] },
        ],
        coverRows: [],
        candidates: [ACTON],
      })
    ).toThrow(/Duplicate Ohio 31-U detail report key/);
  });

  it("caps groups per candidate at maxGroups, keeping the largest", () => {
    const annualRows = [
      annualRow({ amountCents: 100_00 }),
      annualRow({ reportKey: "600000001", masterKey: "200", committeeName: "PAC TWO", amountCents: 200_00 }),
      annualRow({ reportKey: "600000002", masterKey: "300", committeeName: "PAC THREE", amountCents: 300_00 }),
    ];
    const result = aggregateOhioOutsideSpending({
      electionYear: 2026,
      annualExpenditureRows: annualRows,
      detailReports: [
        { reportKey: "512315395", rows: [detailRow({ amountCents: 100_00 })] },
        { reportKey: "600000001", rows: [detailRow({ reportKey: "600000001", amountCents: 200_00 })] },
        { reportKey: "600000002", rows: [detailRow({ reportKey: "600000002", amountCents: 300_00 })] },
      ],
      coverRows: [],
      candidates: [ACTON],
      maxGroups: 2,
    });
    expect(result.candidates[0]?.groups.map((group) => group.committeeId)).toEqual(["300", "200"]);
    // The summary totals still include the dropped group's money.
    expect(result.candidates[0]?.supportTotal).toBe(600);
  });
});
