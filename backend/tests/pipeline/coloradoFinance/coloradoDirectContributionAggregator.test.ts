import { describe, expect, it } from "vitest";

import {
  aggregateColoradoDirectContributions,
  coloradoElectionCycleStartYear,
} from "../../../src/pipeline/coloradoFinance/coloradoDirectContributionAggregator.js";
import type { ColoradoTracerContributionRow } from "../../../src/pipeline/coloradoFinance/coloradoTracerContributionReader.js";

function contribution(overrides: Partial<ColoradoTracerContributionRow> = {}): ColoradoTracerContributionRow {
  return {
    CO_ID: "202450001",
    ContributionAmount: "100.00",
    ContributionDate: "01/10/2024",
    LastName: "Doe",
    FirstName: "Jane",
    MI: "",
    Suffix: "",
    Address1: "",
    Address2: "",
    City: "Denver",
    State: "CO",
    Zip: "80203",
    Explanation: "",
    RecordID: "R1",
    FiledDate: "02/01/2024",
    ContributionType: "Monetary",
    ReceiptType: "Contribution",
    ContributorType: "Individual",
    Electioneering: "",
    CommitteeType: "Candidate Committee",
    CommitteeName: "Jane Doe for Colorado",
    CandidateName: "Jane Doe",
    Employer: "Acme Inc",
    Occupation: "Engineer",
    Amended: "False",
    Amendment: "",
    AmendedRecordID: "",
    Jurisdiction: "STATEWIDE",
    OccupationComments: "",
    ...overrides,
  };
}

describe("coloradoDirectContributionAggregator", () => {
  it("aggregates direct contributions by occupation, employer, and contribution size", () => {
    const result = aggregateColoradoDirectContributions({
      committeeId: "202450001",
      electionYear: 2024,
      contributionRows: [
        contribution({ RecordID: "R1", Employer: "Acme Inc", Occupation: "Engineer", ContributionAmount: "100.00" }),
        contribution({ RecordID: "R2", Employer: "Acme Inc", Occupation: "Engineer", ContributionAmount: "$250.00" }),
        contribution({ RecordID: "R3", Employer: "Mega Corp", Occupation: "Teacher", ContributionAmount: "5,000.00" }),
      ],
      sourceUrl: "https://tracer.sos.colorado.gov/PublicSite/Docs/BulkDataDownloads/2024_ContributionData.csv.zip",
    });

    expect(result).toEqual({
      summary: {
        totalReceipts: 5350,
        sourceUrl: "https://tracer.sos.colorado.gov/PublicSite/Docs/BulkDataDownloads/2024_ContributionData.csv.zip",
      },
      directBreakdowns: [
        {
          categoryType: "occupation",
          categoryName: "Teacher",
          amount: 5000,
          contributorCount: 1,
          sourceUrl: "https://tracer.sos.colorado.gov/PublicSite/Docs/BulkDataDownloads/2024_ContributionData.csv.zip",
        },
        {
          categoryType: "occupation",
          categoryName: "Engineer",
          amount: 350,
          contributorCount: 2,
          sourceUrl: "https://tracer.sos.colorado.gov/PublicSite/Docs/BulkDataDownloads/2024_ContributionData.csv.zip",
        },
        {
          categoryType: "employer",
          categoryName: "Mega Corp",
          amount: 5000,
          contributorCount: 1,
          sourceUrl: "https://tracer.sos.colorado.gov/PublicSite/Docs/BulkDataDownloads/2024_ContributionData.csv.zip",
        },
        {
          categoryType: "employer",
          categoryName: "Acme Inc",
          amount: 350,
          contributorCount: 2,
          sourceUrl: "https://tracer.sos.colorado.gov/PublicSite/Docs/BulkDataDownloads/2024_ContributionData.csv.zip",
        },
        {
          categoryType: "contribution_size",
          categoryName: "$5,000+",
          amount: 5000,
          contributorCount: 1,
          sourceUrl: "https://tracer.sos.colorado.gov/PublicSite/Docs/BulkDataDownloads/2024_ContributionData.csv.zip",
        },
        {
          categoryType: "contribution_size",
          categoryName: "$250-$499",
          amount: 250,
          contributorCount: 1,
          sourceUrl: "https://tracer.sos.colorado.gov/PublicSite/Docs/BulkDataDownloads/2024_ContributionData.csv.zip",
        },
        {
          categoryType: "contribution_size",
          categoryName: "$100-$249",
          amount: 100,
          contributorCount: 1,
          sourceUrl: "https://tracer.sos.colorado.gov/PublicSite/Docs/BulkDataDownloads/2024_ContributionData.csv.zip",
        },
      ],
      matchedContributionRowCount: 3,
      includedContributionRowCount: 3,
      skippedContributionRowCount: 0,
    });
  });

  it("matches contributions by CO_ID case-insensitively", () => {
    const result = aggregateColoradoDirectContributions({
      committeeId: "abc123",
      electionYear: 2024,
      contributionRows: [
        contribution({ CO_ID: " ABC123 ", ContributionAmount: "300", Occupation: "Attorney", Employer: "Law Firm" }),
        contribution({ CO_ID: "OTHER", ContributionAmount: "900", Occupation: "Doctor", Employer: "Hospital" }),
      ],
    });

    expect(result.summary.totalReceipts).toBe(300);
    expect(result.matchedContributionRowCount).toBe(1);
    expect(result.includedContributionRowCount).toBe(1);
    expect(result.directBreakdowns).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ categoryType: "occupation", categoryName: "Attorney", amount: 300 }),
        expect.objectContaining({ categoryType: "employer", categoryName: "Law Firm", amount: 300 }),
      ])
    );
  });

  it("filters to the two-year election cycle ending in the election year", () => {
    expect(coloradoElectionCycleStartYear(2024)).toBe(2023);

    const result = aggregateColoradoDirectContributions({
      committeeId: "202450001",
      electionYear: 2024,
      contributionRows: [
        contribution({ ContributionDate: "12/31/2022", ContributionAmount: "100" }),
        contribution({ ContributionDate: "1/1/2023", ContributionAmount: "200" }),
        contribution({ ContributionDate: "2024-11-01", ContributionAmount: "300" }),
        contribution({ ContributionDate: "1/1/2025", ContributionAmount: "400" }),
      ],
    });

    expect(result.summary.totalReceipts).toBe(500);
    expect(result.matchedContributionRowCount).toBe(4);
    expect(result.includedContributionRowCount).toBe(2);
    expect(result.skippedContributionRowCount).toBe(2);
  });

  it("skips malformed, zero, negative, missing-date, and wrong-committee rows", () => {
    const result = aggregateColoradoDirectContributions({
      committeeId: "202450001",
      electionYear: 2024,
      contributionRows: [
        contribution({ ContributionAmount: "0" }),
        contribution({ ContributionAmount: "-10" }),
        contribution({ ContributionAmount: "not money" }),
        contribution({ ContributionDate: "", ContributionAmount: "100" }),
        contribution({ CO_ID: "999999", ContributionAmount: "500" }),
        contribution({ ContributionAmount: "250" }),
      ],
    });

    expect(result.summary.totalReceipts).toBe(250);
    expect(result.matchedContributionRowCount).toBe(5);
    expect(result.includedContributionRowCount).toBe(1);
    expect(result.skippedContributionRowCount).toBe(4);
  });

  it("limits occupation and employer breakdowns without dropping contribution-size buckets", () => {
    const result = aggregateColoradoDirectContributions({
      committeeId: "202450001",
      electionYear: 2024,
      maxBreakdownsPerCategory: 1,
      contributionRows: [
        contribution({ Occupation: "Engineer", Employer: "Acme", ContributionAmount: "100" }),
        contribution({ Occupation: "Teacher", Employer: "School", ContributionAmount: "300" }),
        contribution({ Occupation: "Doctor", Employer: "Hospital", ContributionAmount: "600" }),
      ],
    });

    expect(result.directBreakdowns.filter((row) => row.categoryType === "occupation")).toEqual([
      expect.objectContaining({ categoryName: "Doctor", amount: 600 }),
    ]);
    expect(result.directBreakdowns.filter((row) => row.categoryType === "employer")).toEqual([
      expect.objectContaining({ categoryName: "Hospital", amount: 600 }),
    ]);
    expect(result.directBreakdowns.filter((row) => row.categoryType === "contribution_size")).toHaveLength(3);
  });

  it("validates required aggregation inputs", () => {
    expect(() =>
      aggregateColoradoDirectContributions({
        committeeId: " ",
        electionYear: 2024,
        contributionRows: [],
      })
    ).toThrow("Colorado committee id is required");

    expect(() =>
      aggregateColoradoDirectContributions({
        committeeId: "202450001",
        electionYear: 1899,
        contributionRows: [],
      })
    ).toThrow("Invalid Colorado direct contribution aggregation election year");

    expect(() =>
      aggregateColoradoDirectContributions({
        committeeId: "202450001",
        electionYear: 2024,
        contributionRows: [],
        maxBreakdownsPerCategory: 0,
      })
    ).toThrow("Invalid Colorado direct contribution aggregation maxBreakdownsPerCategory");
  });
});
