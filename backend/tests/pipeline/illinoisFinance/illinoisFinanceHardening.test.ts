import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";

import {
  aggregateIllinoisDirectContributions,
  aggregateIllinoisOutsideGroupContributions,
  aggregateIllinoisOutsideSpending,
} from "../../../src/pipeline/illinoisFinance/illinoisFinanceAggregators.js";
import {
  getIllinoisSbeExportCapStatus,
  parseIllinoisSbeContributionRecordsCsv,
  parseIllinoisSbeExpenditureRecordsCsv,
  planIllinoisSbeExportPartitions,
  splitIllinoisSbeAmountWindow,
  splitIllinoisSbeDateWindow,
} from "../../../src/pipeline/illinoisFinance/illinoisSbeClient.js";

const CONTRIBUTIONS_FIXTURE = readFileSync(
  new URL("../../fixtures/illinoisFinance/contributions.csv", import.meta.url),
  "utf8"
);
const EXPENDITURES_FIXTURE = readFileSync(
  new URL("../../fixtures/illinoisFinance/expenditures.csv", import.meta.url),
  "utf8"
);

describe("illinoisFinance hardening fixtures", () => {
  it("parses contribution and expenditure CSV fixtures with multiline SBE cells", () => {
    const contributions = parseIllinoisSbeContributionRecordsCsv(CONTRIBUTIONS_FIXTURE, "fixture://contributions.csv");
    const expenditures = parseIllinoisSbeExpenditureRecordsCsv(EXPENDITURES_FIXTURE, "fixture://expenditures.csv");

    expect(contributions).toHaveLength(3);
    expect(contributions[0]).toMatchObject({
      contributorName: "Alpha Attorney",
      contributorAddress: "1 Main St",
      occupation: "Attorney",
      employer: "Law LLP",
      contributionType: "Individual Contributions",
      recipientCommitteeName: "Friends of Jane Doe",
      amount: 1000,
      receivedDate: "3/1/2022",
      sourceUrl: "fixture://contributions.csv",
    });
    expect(contributions[1]).toMatchObject({
      contributorName: "Sierra Club",
      contributionType: "Transfers In",
      recipientCommitteeName: "Illinois Conservation Action",
      amount: 30000,
    });

    expect(expenditures).toHaveLength(3);
    expect(expenditures[0]).toMatchObject({
      payeeName: "Media Vendor",
      payeeAddress: "10 Ad St",
      expenditureType: "Independent Expenditures",
      expendingCommitteeName: "Illinois Conservation Action",
      amount: 10000,
      expendedDate: "10/1/2022",
      candidateName: "Jane Doe",
      officeDistrict: "Governor",
      supportOppose: "support",
      purpose: "Digital ads",
      sourceUrl: "fixture://expenditures.csv",
    });
  });

  it("filters direct receipts by cycle and positive included contribution rows", () => {
    const result = aggregateIllinoisDirectContributions({
      electionYear: 2022,
      contributionRecords: parseIllinoisSbeContributionRecordsCsv(CONTRIBUTIONS_FIXTURE),
      maxBreakdownsPerCategory: 5,
      sourceUrl: "fixture://contributions.csv",
    });

    expect(result).toMatchObject({
      matchedContributionRowCount: 3,
      includedContributionRowCount: 2,
      skippedContributionRowCount: 1,
      summary: {
        totalReceipts: 31000,
        directContributionTotal: 31000,
        sourceUrl: "fixture://contributions.csv",
      },
    });
    expect(result.directBreakdowns).toEqual([
      {
        categoryType: "occupation",
        categoryName: "Attorney",
        amount: 1000,
        contributorCount: 1,
        sourceUrl: "fixture://contributions.csv",
      },
      {
        categoryType: "contribution_size",
        categoryName: "$5,000+",
        amount: 30000,
        contributorCount: 1,
        sourceUrl: "fixture://contributions.csv",
      },
      {
        categoryType: "contribution_size",
        categoryName: "$1,000-$4,999",
        amount: 1000,
        contributorCount: 1,
        sourceUrl: "fixture://contributions.csv",
      },
    ]);
  });

  it("filters independent expenditures by cycle and support/opposition inclusion", () => {
    const result = aggregateIllinoisOutsideSpending({
      electionYear: 2022,
      expenditureRecords: parseIllinoisSbeExpenditureRecordsCsv(EXPENDITURES_FIXTURE),
      sourceUrl: "fixture://expenditures.csv",
      maxGroups: 5,
    });

    expect(result).toMatchObject({
      matchedExpenditureRowCount: 3,
      includedExpenditureRowCount: 2,
      skippedExpenditureRowCount: 1,
      summary: {
        supportTotal: 10000,
        opposeTotal: 7000,
        sourceUrl: "fixture://expenditures.csv",
      },
    });
    expect(result.summary?.groups).toEqual([
      {
        committeeKey: "ILLINOIS CONSERVATION ACTION",
        committeeName: "Illinois Conservation Action",
        supportOppose: "support",
        amount: 10000,
        expenditureCount: 1,
        sourceUrl: "fixture://expenditures.csv",
      },
      {
        committeeKey: "PEOPLE AGAINST JANE",
        committeeName: "People Against Jane",
        supportOppose: "oppose",
        amount: 7000,
        expenditureCount: 1,
        sourceUrl: "fixture://expenditures.csv",
      },
    ]);
  });

  it("detects capped exports and partitions date and amount windows", () => {
    expect(
      getIllinoisSbeExportCapStatus({
        csvRowCount: 25_000,
        resultText: "The maximum number of records available for download is 25,000.",
      })
    ).toEqual({
      rowCount: 25000,
      cap: 25000,
      capped: true,
      warningTextPresent: true,
      reason: "row_count_reached_cap",
    });
    expect(
      getIllinoisSbeExportCapStatus({
        csvRowCount: 100,
        resultText: "The maximum number of records available for download is 25,000.",
      })
    ).toMatchObject({
      capped: true,
      warningTextPresent: true,
      reason: "warning_text_present",
    });
    expect(splitIllinoisSbeDateWindow({ fromDate: "1/1/2021", toDate: "12/31/2022" })).toEqual([
      { fromDate: "1/1/2021", toDate: "12/31/2021" },
      { fromDate: "1/1/2022", toDate: "12/31/2022" },
    ]);
    expect(splitIllinoisSbeDateWindow({ fromDate: "1/1/2022", toDate: "1/1/2022" })).toBeNull();
    expect(splitIllinoisSbeAmountWindow({ minAmount: 0, maxAmount: 1000 })).toEqual([
      { minAmount: 0, maxAmount: 500 },
      { minAmount: 500.01, maxAmount: 1000 },
    ]);
    expect(splitIllinoisSbeAmountWindow({ minAmount: 100, maxAmount: 100 })).toBeNull();
    expect(
      planIllinoisSbeExportPartitions({
        csvRowCount: 25_000,
        fromDate: "1/1/2021",
        toDate: "12/31/2022",
      })
    ).toMatchObject({
      strategy: "date",
      partitions: [
        { fromDate: "1/1/2021", toDate: "12/31/2021" },
        { fromDate: "1/1/2022", toDate: "12/31/2022" },
      ],
    });
  });

  it("aggregates outside group donor and industry evidence from fixture funders", () => {
    const outsideSpending = aggregateIllinoisOutsideSpending({
      electionYear: 2022,
      expenditureRecords: parseIllinoisSbeExpenditureRecordsCsv(EXPENDITURES_FIXTURE),
      sourceUrl: "fixture://expenditures.csv",
    });
    const result = aggregateIllinoisOutsideGroupContributions({
      electionYear: 2022,
      outsideGroups: outsideSpending.summary?.groups ?? [],
      contributionRecords: parseIllinoisSbeContributionRecordsCsv(CONTRIBUTIONS_FIXTURE),
      sourceUrl: "fixture://contributions.csv",
      minIndustryAmount: 25_000,
      maxBreakdownsPerCategory: 5,
    });

    expect(result).toMatchObject({
      matchedContributionRowCount: 1,
      includedContributionRowCount: 1,
      skippedContributionRowCount: 0,
    });
    expect(result.outsideGroupBreakdowns).toEqual([
      {
        committeeKey: "ILLINOIS CONSERVATION ACTION",
        supportOppose: "support",
        categoryType: "donor",
        categoryName: "Sierra Club",
        amount: 30000,
        contributorCount: 1,
        sourceUrl: "fixture://contributions.csv",
      },
      {
        committeeKey: "ILLINOIS CONSERVATION ACTION",
        supportOppose: "support",
        categoryType: "industry",
        categoryName: "environmental_group",
        amount: 30000,
        contributorCount: 1,
        sourceUrl: "fixture://contributions.csv",
      },
    ]);
  });
});
