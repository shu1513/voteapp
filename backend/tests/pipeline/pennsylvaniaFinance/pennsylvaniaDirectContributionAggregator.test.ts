import { describe, expect, it } from "vitest";

import {
  aggregatePennsylvaniaDirectContributions,
  isPennsylvaniaDirectContributionEvent,
  pennsylvaniaElectionCycleStartYear,
} from "../../../src/pipeline/pennsylvaniaFinance/pennsylvaniaDirectContributionAggregator.js";
import type { PennsylvaniaCampaignFinanceContributionRow } from "../../../src/pipeline/pennsylvaniaFinance/pennsylvaniaCampaignFinanceReader.js";

function contribution(
  overrides: Partial<PennsylvaniaCampaignFinanceContributionRow> = {}
): PennsylvaniaCampaignFinanceContributionRow {
  return {
    CampaignFinanceID: "100",
    FilerID: "12345",
    EYEAR: "2026",
    SubmittedDate: "20260501",
    CYCLE: "2",
    Section: "IA",
    CONTRIBUTOR: "JANE ROE",
    ADDRESS1: "1 Main",
    ADDRESS2: "",
    CITY: "Harrisburg",
    STATE: "PA",
    ZIPCODE: "17101",
    OCCUPATION: "Attorney",
    ENAME: "Law Firm",
    EADDRESS1: "",
    EADDRESS2: "",
    ECITY: "",
    ESTATE: "",
    EZIPCODE: "",
    CONTDATE1: "20260115",
    CONTAMT1: "100.00",
    CONTDATE2: "",
    CONTAMT2: "",
    CONTDATE3: "",
    CONTAMT3: "",
    CONTDESC: "",
    ...overrides,
  };
}

describe("pennsylvaniaDirectContributionAggregator", () => {
  it("aggregates direct PA contribution events by occupation and contribution size", () => {
    const sourceUrl = "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/resources/voting-and-elections/campaign-finance/campaign-finance-data/2026.zip";
    const result = aggregatePennsylvaniaDirectContributions({
      filerId: "12345",
      electionYear: 2026,
      sourceUrl,
      contributionRows: [
        contribution({ CONTAMT1: "100.00", OCCUPATION: "Attorney" }),
        contribution({
          CampaignFinanceID: "101",
          CONTRIBUTOR: "JOHN SMITH",
          CONTAMT1: "$250.00",
          OCCUPATION: "Teacher",
        }),
        contribution({
          CampaignFinanceID: "102",
          CONTRIBUTOR: "MEG BROWN",
          OCCUPATION: "Engineer",
          CONTDATE1: "20250101",
          CONTAMT1: "0.10",
          CONTDATE2: "20260202",
          CONTAMT2: "0.20",
          CONTDATE3: "20270101",
          CONTAMT3: "999.00",
        }),
      ],
    });

    expect(result).toEqual({
      summary: {
        totalReceipts: 350.3,
        directContributionTotal: 350.3,
        sourceUrl,
      },
      directBreakdowns: [
        {
          categoryType: "occupation",
          categoryName: "Teacher",
          amount: 250,
          contributorCount: 1,
          sourceUrl,
        },
        {
          categoryType: "occupation",
          categoryName: "Attorney",
          amount: 100,
          contributorCount: 1,
          sourceUrl,
        },
        {
          categoryType: "occupation",
          categoryName: "Engineer",
          amount: 0.3,
          contributorCount: 1,
          sourceUrl,
        },
        {
          categoryType: "contribution_size",
          categoryName: "$250-$499",
          amount: 250,
          contributorCount: 1,
          sourceUrl,
        },
        {
          categoryType: "contribution_size",
          categoryName: "$100-$249",
          amount: 100,
          contributorCount: 1,
          sourceUrl,
        },
        {
          categoryType: "contribution_size",
          categoryName: "$1-$99",
          amount: 0.3,
          contributorCount: 1,
          sourceUrl,
        },
      ],
      matchedContributionRowCount: 3,
      includedContributionEventCount: 4,
      skippedContributionEventCount: 1,
    });
  });

  it("matches filer IDs case-insensitively and counts distinct contributors", () => {
    const result = aggregatePennsylvaniaDirectContributions({
      filerId: " abc123 ",
      electionYear: 2026,
      contributionRows: [
        contribution({ FilerID: "ABC123", CampaignFinanceID: "1", CONTAMT1: "100" }),
        contribution({ FilerID: "ABC123", CampaignFinanceID: "2", CONTAMT1: "200" }),
        contribution({
          FilerID: "ABC123",
          CampaignFinanceID: "3",
          CONTRIBUTOR: "JOHN SMITH",
          CONTAMT1: "300",
        }),
        contribution({ FilerID: "OTHER", CampaignFinanceID: "4", CONTAMT1: "900" }),
      ],
    });

    expect(result.summary.totalReceipts).toBe(600);
    expect(result.matchedContributionRowCount).toBe(3);
    expect(result.directBreakdowns).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ categoryType: "occupation", categoryName: "Attorney", amount: 600, contributorCount: 2 }),
      ])
    );
  });

  it("filters to the two-year election cycle ending in the election year", () => {
    expect(pennsylvaniaElectionCycleStartYear(2026)).toBe(2025);

    const result = aggregatePennsylvaniaDirectContributions({
      filerId: "12345",
      electionYear: 2026,
      contributionRows: [
        contribution({ CONTDATE1: "20241231", CONTAMT1: "100" }),
        contribution({ CONTDATE1: "2025-01-01", CONTAMT1: "200" }),
        contribution({ CONTDATE1: "11/01/2026", CONTAMT1: "300" }),
        contribution({ CONTDATE1: "20270101", CONTAMT1: "400" }),
      ],
    });

    expect(result.summary.totalReceipts).toBe(500);
    expect(result.includedContributionEventCount).toBe(2);
    expect(result.skippedContributionEventCount).toBe(2);
  });

  it("classifies only positive same-cycle events as direct contributions", () => {
    expect(
      isPennsylvaniaDirectContributionEvent({
        event: { rawDate: "20260101", rawAmount: "250" },
        electionYear: 2026,
      })
    ).toBe(true);
    expect(
      isPennsylvaniaDirectContributionEvent({
        event: { rawDate: "20240101", rawAmount: "250" },
        electionYear: 2026,
      })
    ).toBe(false);
    expect(
      isPennsylvaniaDirectContributionEvent({
        event: { rawDate: "20260101", rawAmount: "-1" },
        electionYear: 2026,
      })
    ).toBe(false);
  });

  it("skips blank dates and blank amount triplets without failing the row", () => {
    const result = aggregatePennsylvaniaDirectContributions({
      filerId: "12345",
      electionYear: 2026,
      contributionRows: [
        contribution({
          CONTDATE1: "",
          CONTAMT1: "100",
          CONTDATE2: "20260101",
          CONTAMT2: "",
          CONTDATE3: "",
          CONTAMT3: "",
        }),
      ],
    });

    expect(result).toMatchObject({
      summary: {
        totalReceipts: 0,
        directContributionTotal: 0,
      },
      matchedContributionRowCount: 1,
      includedContributionEventCount: 0,
      skippedContributionEventCount: 2,
    });
  });
});
