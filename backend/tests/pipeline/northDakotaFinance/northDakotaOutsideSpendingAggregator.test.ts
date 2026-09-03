import { describe, expect, it } from "vitest";

import type { NorthDakotaCommitteeRow, NorthDakotaTransactionRow } from "../../../src/pipeline/northDakotaFinance/northDakotaCfrsClient.js";
import {
  aggregateNorthDakotaOutsideSpending,
  normalizeNorthDakotaIeTargetName,
  resolveNorthDakotaIeTargetName,
} from "../../../src/pipeline/northDakotaFinance/northDakotaOutsideSpendingAggregator.js";

const STRONG_ND = "1040001626";
const PUBLIC_SCHOOLS = "1040001621";

/** Live shape 2026-09-03: payee id null, YTD = committee x payee running total. */
function ieRow(overrides: Partial<NorthDakotaTransactionRow>): NorthDakotaTransactionRow {
  return {
    transactionID: 1,
    entityID: STRONG_ND,
    orgID: 1626,
    committeeName: "StrongND Fund",
    candidateName: null,
    transactionAmount: 2000,
    transactionDate: "2026-06-04T00:00:00",
    filedDate: "2026-06-08T00:00:00",
    entityTypeDesc: "Business or Organization",
    transactionCategoryDesc: "Monetary",
    transactionTypeDesc: "Independent Expenditures",
    transactionPurpose: null,
    contributorPayeeName: "Edgerton Media",
    contributorPayeeID: null,
    employerName: null,
    employerOccupation: null,
    transactionTotalYTD: "4000.0000",
    amendedFlag: false,
    reportVersionID: "1",
    reportFileName: "IE Report",
    s3ReportFilePath: "nd-cfs/Reports/1626/report.pdf",
    stanceDescription: "Support",
    candidateNameAssocation: "Lee, Judy",
    electionYear: 2026,
    orgType: "Independent Expenditure Committee",
    ...overrides,
  };
}

function committee(overrides: Partial<NorthDakotaCommitteeRow>): NorthDakotaCommitteeRow {
  return {
    orgID: 1478,
    entityId: "1010001478",
    orgName: "Judy Lee for Senate",
    candidateName: "Lee, Judy",
    orgType: "Candidate/Candidate Committee",
    orgTypeCode: "101",
    orgSubType: null,
    orgSubTypeCode: null,
    election: "2026 Election - Statewide",
    office: "State Senator",
    district: "District 13",
    party: "Republican",
    orgStatus: "Active",
    registrationYear: "2025",
    ...overrides,
  };
}

const JUDY = normalizeNorthDakotaIeTargetName("Lee, Judy");

describe("normalizeNorthDakotaIeTargetName", () => {
  it("compares registry labels and IE target labels in one form", () => {
    expect(normalizeNorthDakotaIeTargetName("Mr. Wrigley, Drew H")).toBe("WRIGLEY DREW H");
    expect(normalizeNorthDakotaIeTargetName("  Haugen-Hoffart,  Sheri ")).toBe("HAUGEN HOFFART SHERI");
    expect(normalizeNorthDakotaIeTargetName("O'Riley, Christine")).toBe("O RILEY CHRISTINE");
  });
});

describe("resolveNorthDakotaIeTargetName", () => {
  it("uses the linked committee's registry candidate label, honorific dropped", () => {
    expect(
      resolveNorthDakotaIeTargetName({
        entityId: "1010001478",
        electionYear: 2026,
        committees: [committee({ candidateName: "Hon. Lee, Judy" }), committee({ entityId: "1010009999", candidateName: "Roe, Rick" })],
      })
    ).toEqual({ status: "resolved", targetName: "LEE JUDY", registryCandidateName: "Hon. Lee, Judy" });
  });

  it("fails closed when the committee is missing from the registry or has no candidate label", () => {
    expect(resolveNorthDakotaIeTargetName({ entityId: "1010001478", electionYear: 2026, committees: [] })).toMatchObject({
      status: "unresolved",
      reason: "committee_not_in_registry",
    });
    expect(
      resolveNorthDakotaIeTargetName({ entityId: "1010001478", electionYear: 2026, committees: [committee({ candidateName: "  " })] })
    ).toMatchObject({ status: "unresolved", reason: "committee_has_no_candidate_name" });
  });

  it("is ambiguous only when the same label names a different office or seat on the same election", () => {
    const linked = committee({});
    // Same person re-registered for the same seat: one target.
    expect(
      resolveNorthDakotaIeTargetName({
        entityId: "1010001478",
        electionYear: 2026,
        committees: [linked, committee({ entityId: "1010001479", orgID: 1479, registrationYear: "2026" })],
      })
    ).toMatchObject({ status: "resolved" });
    // A different-seat committee with the same label on the 2026 label.
    expect(
      resolveNorthDakotaIeTargetName({
        entityId: "1010001478",
        electionYear: 2026,
        committees: [linked, committee({ entityId: "1010001480", district: "District 27" })],
      })
    ).toMatchObject({ status: "unresolved", reason: "ambiguous_name", detail: expect.stringContaining("1010001480") });
    // Another cycle, a PAC, or a different label never clashes.
    expect(
      resolveNorthDakotaIeTargetName({
        entityId: "1010001478",
        electionYear: 2026,
        committees: [
          linked,
          committee({ entityId: "1010001481", district: "District 27", election: "2024 Election - Statewide" }),
          committee({ entityId: "1020001482", district: "District 27", orgType: "Political Action Committee", orgTypeCode: "102" }),
          committee({ entityId: "1010001483", candidateName: "Lee, Judith" }),
        ],
      })
    ).toMatchObject({ status: "resolved" });
  });
});

describe("aggregateNorthDakotaOutsideSpending", () => {
  it("sums unique rows per spender and stance for the candidate's election, keeping equal slate allocations", () => {
    const rows = [
      // Two equal allocations from one buy, both to Judy: legitimate, both count.
      ieRow({ transactionID: 10, transactionAmount: 2000 }),
      ieRow({ transactionID: 11, transactionAmount: 2000 }),
      // Same committee, another target, same payee: part of the YTD control only.
      ieRow({ transactionID: 12, transactionAmount: 500, candidateNameAssocation: "Roe, Rick", transactionTotalYTD: "4500.0000" }),
      // Another committee opposing Judy with a different-cased label.
      ieRow({ transactionID: 20, entityID: PUBLIC_SCHOOLS, orgID: 1621, committeeName: "North Dakotans for Public Schools", transactionAmount: 300.5, stanceDescription: "Oppose", candidateNameAssocation: "LEE, JUDY", contributorPayeeName: "Print Co", transactionTotalYTD: "300.5" }),
      // A 2028 row naming Judy belongs to another election.
      ieRow({ transactionID: 30, entityID: PUBLIC_SCHOOLS, orgID: 1621, committeeName: "North Dakotans for Public Schools", transactionAmount: 999, electionYear: 2028, contributorPayeeName: "Future Co", transactionTotalYTD: "999" }),
    ];
    // The Judy-only rows sum to 4000 for Edgerton; with Roe's 500 the payee's max YTD is 4500.
    rows[0]!.transactionTotalYTD = "4500.0000";
    rows[1]!.transactionTotalYTD = "4500.0000";
    const result = aggregateNorthDakotaOutsideSpending({ targetName: JUDY, electionYear: 2026, rows });
    expect(result).toEqual({
      supportTotal: 4000,
      opposeTotal: 300.5,
      groups: [
        { entityId: STRONG_ND, committeeName: "StrongND Fund", supportOppose: "support", amount: 4000 },
        { entityId: PUBLIC_SCHOOLS, committeeName: "North Dakotans for Public Schools", supportOppose: "oppose", amount: 300.5 },
      ],
      sourceRowCount: 5,
      targetRowCount: 4,
      includedRowCount: 3,
      ytdCheckedCommitteeCount: 2,
      // The 2028 row's payee has a control; nothing is missing.
      ytdMissingControlGroupCount: 0,
    });
  });

  it("returns clean zeros when no row names the candidate", () => {
    expect(
      aggregateNorthDakotaOutsideSpending({ targetName: JUDY, electionYear: 2026, rows: [ieRow({ candidateNameAssocation: "Roe, Rick" })] })
    ).toMatchObject({ supportTotal: 0, opposeTotal: 0, groups: [], sourceRowCount: 1, targetRowCount: 0, includedRowCount: 0, ytdCheckedCommitteeCount: 0 });
  });

  it("quarantines a spender whose payee YTD control does not equal its unique-row sum", () => {
    const rows = [
      ieRow({ transactionID: 10, transactionAmount: 2000, transactionTotalYTD: "5000.0000" }),
      ieRow({ transactionID: 11, transactionAmount: 2000, transactionTotalYTD: "5000.0000" }),
    ];
    expect(() => aggregateNorthDakotaOutsideSpending({ targetName: JUDY, electionYear: 2026, rows })).toThrow(
      /IE committee 1040001626 fails the payee YTD control in 1 group\(s\) \(2026 name#[a-f0-9]{12}: rows sum 400000c, max YTD 500000c\)/
    );
    // The control is per calendar year: a prior-year row to the same payee does not join the sum.
    const acrossYears = [
      ieRow({ transactionID: 10, transactionAmount: 2000, transactionTotalYTD: "2000.0000" }),
      ieRow({ transactionID: 11, transactionAmount: 700, transactionDate: "2025-11-01T00:00:00", transactionTotalYTD: "700.0000" }),
    ];
    expect(aggregateNorthDakotaOutsideSpending({ targetName: JUDY, electionYear: 2026, rows: acrossYears })).toMatchObject({
      supportTotal: 2700,
      ytdMissingControlGroupCount: 0,
    });
    // A payee with no control at all is counted, not failed.
    expect(
      aggregateNorthDakotaOutsideSpending({ targetName: JUDY, electionYear: 2026, rows: [ieRow({ transactionTotalYTD: null })] })
    ).toMatchObject({ supportTotal: 2000, ytdMissingControlGroupCount: 1 });
  });

  it("fails closed on every unobserved shape", () => {
    const run = (rows: NorthDakotaTransactionRow[]) => () => aggregateNorthDakotaOutsideSpending({ targetName: JUDY, electionYear: 2026, rows });
    expect(run([ieRow({ transactionID: 5 }), ieRow({ transactionID: 5, candidateNameAssocation: "Roe, Rick" })])).toThrow(/repeats transactionID 5/);
    expect(run([ieRow({ stanceDescription: null })])).toThrow(/no Support\/Oppose stance \(null\)/);
    expect(run([ieRow({ stanceDescription: "Neutral" })])).toThrow(/no Support\/Oppose stance \("Neutral"\)/);
    expect(run([ieRow({ electionYear: null })])).toThrow(/carries no election year/);
    expect(run([ieRow({ transactionAmount: 0 })])).toThrow(/non-positive amount \(0\)/);
    expect(run([ieRow({ transactionAmount: -10 })])).toThrow(/non-positive amount \(-10\)/);
    expect(run([ieRow({ committeeName: " " })])).toThrow(/no committee name for entityId 1040001626/);
    expect(run([ieRow({ transactionTypeDesc: "Expenditures" })])).toThrow(/is typed "Expenditures"/);
    // The same shapes on another candidate's rows do not touch this candidate.
    expect(run([ieRow({ candidateNameAssocation: "Roe, Rick", stanceDescription: null, transactionAmount: -10 })])).not.toThrow();
    expect(() => aggregateNorthDakotaOutsideSpending({ targetName: "", electionYear: 2026, rows: [] })).toThrow(/targetName is required/);
  });
});
