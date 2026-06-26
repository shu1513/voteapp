import { describe, expect, it } from "vitest";

import { aggregatePennsylvaniaOutsideGroupContributions } from "../../../src/pipeline/pennsylvaniaFinance/pennsylvaniaOutsideGroupContributionAggregator.js";
import type {
  PennsylvaniaCampaignFinanceContributionRow,
  PennsylvaniaCampaignFinanceFilerRow,
} from "../../../src/pipeline/pennsylvaniaFinance/pennsylvaniaCampaignFinanceReader.js";

function filerRow(overrides: Partial<PennsylvaniaCampaignFinanceFilerRow> = {}): PennsylvaniaCampaignFinanceFilerRow {
  return {
    CampaignfinanceID: "100",
    FILERID: "PAC123",
    EYEAR: "2026",
    SubmittedDate: "20260501",
    CYCLE: "2",
    AMMEND: "",
    TERMINATE: "",
    FILERTYPE: "4",
    FILERNAME: "PENNSYLVANIANS FOR ACTION",
    OFFICE: "",
    DISTRICT: "",
    PARTY: "",
    ADDRESS1: "",
    ADDRESS2: "",
    CITY: "",
    STATE: "PA",
    ZIPCODE: "",
    COUNTY: "",
    PHONE: "",
    BEGINNING: "",
    MONETARY: "",
    INKIND: "",
    ...overrides,
  };
}

function contribution(
  overrides: Partial<PennsylvaniaCampaignFinanceContributionRow> = {}
): PennsylvaniaCampaignFinanceContributionRow {
  return {
    CampaignFinanceID: "100",
    FilerID: "PAC123",
    EYEAR: "2026",
    SubmittedDate: "20260501",
    CYCLE: "2",
    Section: "IA",
    CONTRIBUTOR: "Energy Transfer LLC",
    ADDRESS1: "1 Main",
    ADDRESS2: "",
    CITY: "Harrisburg",
    STATE: "PA",
    ZIPCODE: "17101",
    OCCUPATION: "",
    ENAME: "",
    EADDRESS1: "",
    EADDRESS2: "",
    ECITY: "",
    ESTATE: "",
    EZIPCODE: "",
    CONTDATE1: "20260115",
    CONTAMT1: "25000.00",
    CONTDATE2: "",
    CONTAMT2: "",
    CONTDATE3: "",
    CONTAMT3: "",
    CONTDESC: "",
    ...overrides,
  };
}

describe("pennsylvaniaOutsideGroupContributionAggregator", () => {
  it("backtraces explicit outside-group organization donors into donor and industry breakdowns", () => {
    const sourceUrl = "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/resources/voting-and-elections/campaign-finance/campaign-finance-data/2026.zip";
    const result = aggregatePennsylvaniaOutsideGroupContributions({
      electionYear: 2026,
      sourceUrl,
      outsideGroups: [
        {
          groupId: "PENNSYLVANIANS FOR ACTION",
          groupName: "Pennsylvanians for Action",
          supportOppose: "support",
          amount: 100000,
          sourceUrl,
        },
      ],
      filerRows: [filerRow()],
      contributionRows: [
        contribution({ CONTAMT1: "25000.00" }),
        contribution({
          CampaignFinanceID: "101",
          CONTRIBUTOR: "Acme Real Estate LLC",
          CONTAMT1: "50000.00",
        }),
        contribution({
          CampaignFinanceID: "102",
          CONTRIBUTOR: "Jane Roe",
          OCCUPATION: "Attorney",
          CONTAMT1: "75000.00",
        }),
        contribution({
          CampaignFinanceID: "103",
          FilerID: "OTHER",
          CONTRIBUTOR: "Other Energy LLC",
          CONTAMT1: "90000.00",
        }),
      ],
    });

    expect(result).toEqual({
      outsideGroupBreakdowns: [
        {
          groupId: "PAC123",
          supportOppose: "support",
          categoryType: "donor",
          categoryName: "Acme Real Estate LLC",
          amount: 50000,
          contributorCount: 1,
          sourceUrl,
        },
        {
          groupId: "PAC123",
          supportOppose: "support",
          categoryType: "donor",
          categoryName: "Energy Transfer LLC",
          amount: 25000,
          contributorCount: 1,
          sourceUrl,
        },
        {
          groupId: "PAC123",
          supportOppose: "support",
          categoryType: "industry",
          categoryName: "real_estate",
          amount: 50000,
          contributorCount: 1,
          sourceUrl,
        },
        {
          groupId: "PAC123",
          supportOppose: "support",
          categoryType: "industry",
          categoryName: "oil_gas_energy",
          amount: 25000,
          contributorCount: 1,
          sourceUrl,
        },
      ],
      matchedContributionRowCount: 3,
      includedContributionEventCount: 2,
      skippedContributionEventCount: 1,
    });
  });

  it("filters outside group donor events to the two-year election cycle", () => {
    const result = aggregatePennsylvaniaOutsideGroupContributions({
      electionYear: 2026,
      outsideGroups: [
        {
          groupId: "PENNSYLVANIANS FOR ACTION",
          groupName: "Pennsylvanians for Action",
          supportOppose: "oppose",
          amount: 100000,
          sourceUrl: null,
        },
      ],
      filerRows: [filerRow()],
      contributionRows: [
        contribution({ CONTDATE1: "20241231", CONTAMT1: "10000.00" }),
        contribution({ CONTDATE1: "20250101", CONTAMT1: "20000.00" }),
        contribution({ CONTDATE1: "20270101", CONTAMT1: "30000.00" }),
      ],
    });

    expect(result.outsideGroupBreakdowns).toEqual([
      expect.objectContaining({
        groupId: "PAC123",
        supportOppose: "oppose",
        categoryType: "donor",
        amount: 20000,
      }),
    ]);
    expect(result.matchedContributionRowCount).toBe(3);
    expect(result.includedContributionEventCount).toBe(1);
    expect(result.skippedContributionEventCount).toBe(2);
  });

  it("skips donor and industry breakdowns when the IE organization cannot be mapped to a PA filer", () => {
    const result = aggregatePennsylvaniaOutsideGroupContributions({
      electionYear: 2026,
      outsideGroups: [
        {
          groupId: "UNMAPPED ACTION",
          groupName: "Unmapped Action",
          supportOppose: "support",
          amount: 100000,
          sourceUrl: null,
        },
      ],
      filerRows: [filerRow()],
      contributionRows: [contribution()],
    });

    expect(result).toEqual({
      outsideGroupBreakdowns: [],
      matchedContributionRowCount: 0,
      includedContributionEventCount: 0,
      skippedContributionEventCount: 0,
    });
  });
});
