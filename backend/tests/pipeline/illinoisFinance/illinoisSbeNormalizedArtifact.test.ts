import { describe, expect, it } from "vitest";

import { aggregateIllinoisD2Summaries } from "../../../src/pipeline/illinoisFinance/illinoisD2SummaryAggregator.js";
import {
  parseIllinoisSbeNormalizedArtifact,
  type IllinoisSbeD2ReportSummary,
} from "../../../src/pipeline/illinoisFinance/illinoisSbeNormalizedArtifact.js";

const SOURCE_URL = "https://www.elections.il.gov/CampaignDisclosure/CandidateSearch.aspx";

function manifest(overrides: Record<string, unknown> = {}): string {
  return JSON.stringify({
    schemaVersion: 1,
    complete: true,
    source: "illinois_sbe",
    acquiredAt: "2026-07-11T12:00:00.000Z",
    sourceUrl: SOURCE_URL,
    candidateCommitteeRelations: [
      {
        candidateId: "123",
        candidateName: "Jane Doe",
        electionYear: 2025,
        districtType: "City",
        district: "Aurora",
        office: "Mayor",
        isAtLarge: false,
        committeeId: "456",
        committeeName: "Aurora Forward",
        committeeStatus: "active",
        sourceUrl: SOURCE_URL,
      },
    ],
    d2ReportSummaries: [
      {
        reportId: "r1",
        committeeId: "456",
        periodStart: "2024-01-01",
        periodEnd: "2024-03-31",
        filedAt: "2024-04-15T12:00:00.000Z",
        totalReceipts: 1000,
        totalDisbursements: 500,
        cashOnHand: 700,
        debtsOwed: 100,
        sourceUrl: SOURCE_URL,
      },
    ],
    ...overrides,
  });
}

describe("Illinois normalized SBE artifacts", () => {
  it("parses a complete, versioned artifact", () => {
    expect(parseIllinoisSbeNormalizedArtifact(manifest())).toMatchObject({
      schemaVersion: 1,
      complete: true,
      source: "illinois_sbe",
      candidateCommitteeRelations: [
        {
          candidateId: "123",
          committeeId: "456",
          districtType: "City",
          district: "Aurora",
          office: "Mayor",
        },
      ],
    });
  });

  it("fails closed for incomplete or duplicate records", () => {
    expect(() => parseIllinoisSbeNormalizedArtifact(manifest({ complete: false }))).toThrow(
      "complete=true"
    );
    const duplicate = JSON.parse(manifest()) as Record<string, unknown>;
    duplicate.candidateCommitteeRelations = [
      ...(duplicate.candidateCommitteeRelations as unknown[]),
      ...(duplicate.candidateCommitteeRelations as unknown[]),
    ];
    expect(() => parseIllinoisSbeNormalizedArtifact(JSON.stringify(duplicate))).toThrow(
      "duplicate candidate/committee relation"
    );
  });

  it("rejects records without explicit at-large evidence", () => {
    const invalid = JSON.parse(manifest()) as Record<string, unknown>;
    const relation = { ...((invalid.candidateCommitteeRelations as Record<string, unknown>[])[0] ?? {}) };
    delete relation.isAtLarge;
    invalid.candidateCommitteeRelations = [relation];
    expect(() => parseIllinoisSbeNormalizedArtifact(JSON.stringify(invalid))).toThrow("isAtLarge must be boolean");
  });
});

function report(overrides: Partial<IllinoisSbeD2ReportSummary> = {}): IllinoisSbeD2ReportSummary {
  return {
    reportId: "q1-original",
    committeeId: "456",
    periodStart: "2024-01-01",
    periodEnd: "2024-03-31",
    filedAt: "2024-04-15T12:00:00.000Z",
    totalReceipts: 1000,
    totalDisbursements: 500,
    cashOnHand: 700,
    debtsOwed: 100,
    sourceUrl: SOURCE_URL,
    ...overrides,
  };
}

describe("Illinois D-2 summary aggregation", () => {
  it("uses the latest amendment per period and latest balance", () => {
    const result = aggregateIllinoisD2Summaries({
      electionYear: 2025,
      committeeId: "456",
      reports: [
        report(),
        report({
          reportId: "q1-amended",
          filedAt: "2024-05-01T12:00:00.000Z",
          totalReceipts: 1200,
          totalDisbursements: 550,
          cashOnHand: 800,
          debtsOwed: 90,
        }),
        report({
          reportId: "q2",
          periodStart: "2024-04-01",
          periodEnd: "2024-06-30",
          filedAt: "2024-07-15T12:00:00.000Z",
          totalReceipts: 300,
          totalDisbursements: 200,
          cashOnHand: 900,
          debtsOwed: 25,
        }),
        report({ reportId: "old", periodStart: "2023-01-01", periodEnd: "2023-03-31" }),
      ],
    });

    expect(result).toEqual({
      totalReceipts: 1500,
      totalDisbursements: 750,
      cashOnHand: 900,
      debtsOwed: 25,
      sourceUrl: SOURCE_URL,
      includedReportCount: 2,
    });
  });

  it("returns null when no complete D-2 period falls in the cycle", () => {
    expect(
      aggregateIllinoisD2Summaries({
        electionYear: 2025,
        committeeId: "456",
        reports: [report({ periodStart: "2023-01-01", periodEnd: "2023-03-31" })],
      })
    ).toBeNull();
  });
});
