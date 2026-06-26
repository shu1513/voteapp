import { describe, expect, it } from "vitest";

import {
  aggregateNewJerseyOutsideSpendingFromElecFilings,
  aggregateNewJerseyOutsideSpending,
  parseNewJerseyElecIndependentExpenditureAllocations,
  type NewJerseyOutsideSpendingGroup,
} from "../../../src/pipeline/newJerseyFinance/newJerseyOutsideSpendingAggregator.js";
import {
  aggregateNewJerseyOutsideGroupContributions,
  aggregateNewJerseyOutsideGroupContributionsFromElec,
} from "../../../src/pipeline/newJerseyFinance/newJerseyOutsideGroupContributionAggregator.js";
import type { NewJerseyElecContributionRow, NewJerseyElecFiling } from "../../../src/pipeline/newJerseyFinance/newJerseyElecClient.js";

const REPORT_TEXT = `
  Date Amount
  Check No Payee Name And Address Date Balance Amount
  Disbursed Disbursed
  DELIVER STRATEGIES LLC 10/22/2025 $100,082.02
  Purpose MEDIA - HANDOUTS/FLYERS/PALM CARDS Comments
  ALLOCATION OF EXPENDITURES BENEFITING CANDIDATE(S) / COMMITTEE(S)
  Office Candidate/Committee Name Election Date Location Amount
  NJ Gubernatorial MIKIE SHERRILL FOR GOVERNOR 11/04/2025 STATEWIDE $100,082.02
  Candidates/Committees
  ALLOCATION OF EXPENDITURES OPPOSING CANDIDATE(S) / COMMITTEE(S)
  Office Candidate/Committee Name Election Date Location Amount
  NJ Gubernatorial JACK CIATTARELLI FOR GOVERNOR 11/04/2025 STATEWIDE $25,000.00
  Candidates/Committees
  ALLOCATION OF EXPENDITURES BENEFITING CANDIDATE(S) / COMMITTEE(S)
  Office Candidate/Committee Name Election Date Location Amount
  NJ Gubernatorial MIKIE SHERRILL FOR GOVERNOR 11/04/2021 STATEWIDE $99,999.00
`;

function contribution(overrides: Partial<NewJerseyElecContributionRow> = {}): NewJerseyElecContributionRow {
  return {
    contribS: 1001,
    entityS: 477267,
    electionYear: 2025,
    recipientName: "ONE GIANT LEAP PAC - OGL PAC",
    contributorName: "Jane Street Capital LLC",
    contributorFirstName: null,
    contributorLastName: null,
    contributorNonIndividualName: "Jane Street Capital LLC",
    isIndividual: false,
    contributorType: "Business",
    contributionType: "Monetary",
    contributionDate: "10/01/2025",
    amount: 100_000,
    employerName: null,
    occupationCode: null,
    occupationName: null,
    sourceUrl: "https://www.njelecefilesearch.com/SearchContributionToEntity?eid=477267",
    ...overrides,
  };
}

function filing(overrides: Partial<NewJerseyElecFiling> = {}): NewJerseyElecFiling {
  return {
    entityS: 477267,
    docId: 3909738,
    period: 3,
    amendmentNumber: 1,
    dateReceived: "2025-12-29T00:00:00",
    publicAccess: true,
    hour48NoticePublicAccess: false,
    amountReceived: 6_301_050,
    amountDisbursed: 5_084_060.08,
    filingStatusFormCode: "R",
    linkTabFormType: "R",
    formName: "R-1",
    sortSequence: 300,
    reportDownloadUrl: "https://www.njelecefilesearch.com/SearchIndExpReports/?handler=DownloadReport&DocId=3909738",
    sourceUrl: "https://www.njelecefilesearch.com/api/VWEntity/GetEntityFilingData?ENTITY_S=477267",
    ...overrides,
  };
}

describe("newJerseyOutsideSpendingAggregator", () => {
  it("parses support and opposition allocations from ELEC report text", () => {
    expect(
      parseNewJerseyElecIndependentExpenditureAllocations({
        text: REPORT_TEXT,
        sourceUrl: "https://www.njelecefilesearch.com/SearchIndExpReports/?handler=DownloadReport&DocId=3909738",
        docId: 3909738,
      })
    ).toEqual([
      {
        supportOppose: "support",
        office: "NJ Gubernatorial",
        candidateOrCommitteeName: "MIKIE SHERRILL FOR GOVERNOR",
        electionDate: "11/04/2025",
        location: "STATEWIDE",
        amount: 100082.02,
        sourceUrl: "https://www.njelecefilesearch.com/SearchIndExpReports/?handler=DownloadReport&DocId=3909738",
        docId: 3909738,
      },
      {
        supportOppose: "oppose",
        office: "NJ Gubernatorial",
        candidateOrCommitteeName: "JACK CIATTARELLI FOR GOVERNOR",
        electionDate: "11/04/2025",
        location: "STATEWIDE",
        amount: 25000,
        sourceUrl: "https://www.njelecefilesearch.com/SearchIndExpReports/?handler=DownloadReport&DocId=3909738",
        docId: 3909738,
      },
      {
        supportOppose: "support",
        office: "NJ Gubernatorial",
        candidateOrCommitteeName: "MIKIE SHERRILL FOR GOVERNOR",
        electionDate: "11/04/2021",
        location: "STATEWIDE",
        amount: 99999,
        sourceUrl: "https://www.njelecefilesearch.com/SearchIndExpReports/?handler=DownloadReport&DocId=3909738",
        docId: 3909738,
      },
    ]);
  });

  it("aggregates outside support groups by target candidate and election year", () => {
    const result = aggregateNewJerseyOutsideSpending({
      candidateName: "Mikie Sherrill",
      electionYear: 2025,
      outsideGroupEntityS: 477267,
      outsideGroupName: "ONE GIANT LEAP PAC - OGL PAC",
      sourceUrl: "https://www.njelecefilesearch.com/SearchIndExpReports/?handler=DownloadReport&DocId=3909738",
      reportTexts: [{ text: REPORT_TEXT, docId: 3909738 }],
    });

    expect(result).toEqual({
      summary: {
        supportTotal: 100082.02,
        opposeTotal: 0,
        groups: [
          {
            entityS: 477267,
            entityName: "ONE GIANT LEAP PAC - OGL PAC",
            supportOppose: "support",
            amount: 100082.02,
            sourceUrl: "https://www.njelecefilesearch.com/SearchIndExpReports/?handler=DownloadReport&DocId=3909738",
            docIds: [3909738],
          },
        ],
        sourceUrl: "https://www.njelecefilesearch.com/SearchIndExpReports/?handler=DownloadReport&DocId=3909738",
      },
      matchedAllocationRowCount: 2,
      includedAllocationRowCount: 1,
      skippedAllocationRowCount: 1,
    });
  });

  it("backtraces outside group contributions into donor and industry breakdowns", () => {
    const outsideGroups: NewJerseyOutsideSpendingGroup[] = [
      {
        entityS: 477267,
        entityName: "ONE GIANT LEAP PAC - OGL PAC",
        supportOppose: "support",
        amount: 100082.02,
        sourceUrl: "https://www.njelecefilesearch.com/SearchIndExpReports/?handler=DownloadReport&DocId=3909738",
        docIds: [3909738],
      },
    ];

    const result = aggregateNewJerseyOutsideGroupContributions({
      electionYear: 2025,
      outsideGroups,
      minIndustryAmount: 0,
      contributions: [
        contribution({ amount: 100_000 }),
        contribution({
          contribS: 1002,
          contributorName: "Acme Properties LLC",
          contributorNonIndividualName: "Acme Properties LLC",
          amount: 75_000,
        }),
        contribution({
          contribS: 1003,
          contributorName: "Jane Donor",
          contributorFirstName: "Jane",
          contributorLastName: "Donor",
          contributorNonIndividualName: null,
          isIndividual: true,
          contributorType: "Individual",
          amount: 50_000,
          employerName: "Google",
          occupationName: "Software Engineer",
        }),
        contribution({
          contribS: 1004,
          contributorName: "Refund Row LLC",
          contributionType: "Refund",
          amount: 10_000,
        }),
        contribution({
          contribS: 1005,
          entityS: 999999,
          contributorName: "Wrong PAC LLC",
          amount: 999_999,
        }),
      ],
    });

    expect(result.matchedContributionRowCount).toBe(4);
    expect(result.includedContributionRowCount).toBe(3);
    expect(result.skippedContributionRowCount).toBe(1);
    expect(result.outsideGroupBreakdowns).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          entityS: 477267,
          supportOppose: "support",
          categoryType: "donor",
          categoryName: "Jane Street Capital LLC",
          amount: 100000,
          contributorCount: 1,
        }),
        expect.objectContaining({
          categoryType: "donor",
          categoryName: "Acme Properties LLC",
          amount: 75000,
        }),
        expect.objectContaining({
          categoryType: "contributor_type",
          categoryName: "Business",
          amount: 175000,
          contributorCount: 2,
        }),
        expect.objectContaining({
          categoryType: "contributor_type",
          categoryName: "Individual",
          amount: 50000,
          contributorCount: 1,
        }),
        expect.objectContaining({
          categoryType: "occupation",
          categoryName: "Software Engineer",
          amount: 50000,
          contributorCount: 1,
        }),
        expect.objectContaining({
          categoryType: "industry",
          categoryName: "finance_investment",
          amount: 100000,
          contributorCount: 1,
        }),
        expect.objectContaining({
          categoryType: "industry",
          categoryName: "real_estate",
          amount: 75000,
          contributorCount: 1,
        }),
        expect.objectContaining({
          categoryType: "industry",
          categoryName: "technology",
          amount: 50000,
          contributorCount: 1,
        }),
      ])
    );
    expect(result.outsideGroupBreakdowns.some((row) => row.categoryName === "Refund Row LLC")).toBe(false);
    expect(result.outsideGroupBreakdowns.some((row) => row.categoryName === "Wrong PAC LLC")).toBe(false);
  });

  it("does not allocate funders when the same PAC both supports and opposes the target", () => {
    const result = aggregateNewJerseyOutsideGroupContributions({
      electionYear: 2025,
      outsideGroups: [
        {
          entityS: 477267,
          entityName: "Mixed PAC",
          supportOppose: "support",
          amount: 100,
          sourceUrl: null,
          docIds: [],
        },
        {
          entityS: 477267,
          entityName: "Mixed PAC",
          supportOppose: "oppose",
          amount: 50,
          sourceUrl: null,
          docIds: [],
        },
      ],
      contributions: [contribution({ amount: 100_000 })],
    });

    expect(result.outsideGroupBreakdowns).toEqual([]);
    expect(result.matchedContributionRowCount).toBe(1);
    expect(result.includedContributionRowCount).toBe(0);
    expect(result.skippedContributionRowCount).toBe(1);
  });

  it("pulls outside group contribution rows from ELEC before aggregating funders", async () => {
    const outsideGroups: NewJerseyOutsideSpendingGroup[] = [
      {
        entityS: 477267,
        entityName: "ONE GIANT LEAP PAC - OGL PAC",
        supportOppose: "support",
        amount: 100082.02,
        sourceUrl: "https://www.njelecefilesearch.com/SearchIndExpReports/?handler=DownloadReport&DocId=3909738",
        docIds: [3909738],
      },
    ];
    const getContributionRows = async (input: { entityS: number; electionYear: number }) => ({
      recordsTotal: 1,
      recordsFiltered: 1,
      rows: [contribution({ entityS: input.entityS, electionYear: input.electionYear, amount: 100_000 })],
      sourceUrl: `https://www.njelecefilesearch.com/SearchContributionToEntity?eid=${input.entityS}`,
    });

    const result = await aggregateNewJerseyOutsideGroupContributionsFromElec({
      electionYear: 2025,
      outsideGroups,
      minIndustryAmount: 0,
      elecClient: { getContributionRows },
    });

    expect(result).toMatchObject({
      fetchedOutsideGroupCount: 1,
      fetchedContributionRowCount: 1,
      skippedOutsideGroupCount: 0,
      matchedContributionRowCount: 1,
      includedContributionRowCount: 1,
      skippedContributionRowCount: 0,
    });
    expect(result.outsideGroupBreakdowns).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          entityS: 477267,
          supportOppose: "support",
          categoryType: "donor",
          categoryName: "Jane Street Capital LLC",
          amount: 100000,
        }),
        expect.objectContaining({
          categoryType: "industry",
          categoryName: "finance_investment",
          amount: 100000,
        }),
      ])
    );
  });

  it("applies donor and industry caps independently", () => {
    const outsideGroups: NewJerseyOutsideSpendingGroup[] = [
      {
        entityS: 477267,
        entityName: "ONE GIANT LEAP PAC - OGL PAC",
        supportOppose: "support",
        amount: 100,
        sourceUrl: null,
        docIds: [],
      },
    ];

    const result = aggregateNewJerseyOutsideGroupContributions({
      electionYear: 2025,
      outsideGroups,
      minIndustryAmount: 0,
      maxBreakdownsPerCategory: 1,
      contributions: [
        contribution({ contributorName: "Acme Properties LLC", contributorNonIndividualName: "Acme Properties LLC", amount: 75_000 }),
        contribution({ contributorName: "Jane Street Capital LLC", contributorNonIndividualName: "Jane Street Capital LLC", amount: 100_000 }),
      ],
    });

    expect(result.outsideGroupBreakdowns.filter((row) => row.categoryType === "donor")).toEqual([
      expect.objectContaining({ categoryName: "Jane Street Capital LLC", amount: 100000 }),
    ]);
    expect(result.outsideGroupBreakdowns.filter((row) => row.categoryType === "contributor_type")).toEqual([
      expect.objectContaining({ categoryName: "Business", amount: 175000 }),
    ]);
    expect(result.outsideGroupBreakdowns.filter((row) => row.categoryType === "industry")).toEqual([
      expect.objectContaining({ categoryName: "finance_investment", amount: 100000 }),
    ]);
  });

  it("aggregates outside spending from ELEC filing rows using a report text extractor", async () => {
    const result = await aggregateNewJerseyOutsideSpendingFromElecFilings({
      candidateName: "Mikie Sherrill",
      electionYear: 2025,
      outsideGroupEntityS: 477267,
      outsideGroupName: "ONE GIANT LEAP PAC - OGL PAC",
      filings: [filing(), filing({ docId: 3909739, period: 4 })],
      elecClient: {
        getReportDownload: async (docId) => ({
          docId,
          fileNameWithSas: `https://storage.example/${docId}.pdf?sv=short-lived`,
          sourceUrl: `https://www.njelecefilesearch.com/SearchIndExpReports/?handler=DownloadReport&DocId=${docId}`,
        }),
      },
      textExtractor: async ({ docId }) => (docId === 3909738 ? REPORT_TEXT : "MIKIE SHERRILL FOR GOVERNOR unreadable amount"),
    });

    expect(result).toEqual({
      summary: {
        supportTotal: 100082.02,
        opposeTotal: 0,
        groups: [
          {
            entityS: 477267,
            entityName: "ONE GIANT LEAP PAC - OGL PAC",
            supportOppose: "support",
            amount: 100082.02,
            sourceUrl: "https://www.njelecefilesearch.com/SearchIndExpReports/?handler=DownloadReport&DocId=3909738",
            docIds: [3909738],
          },
        ],
        sourceUrl: null,
      },
      matchedAllocationRowCount: 2,
      includedAllocationRowCount: 1,
      skippedAllocationRowCount: 2,
      filingRowCount: 2,
      downloadedReportCount: 2,
      extractedReportTextCount: 2,
      skippedFilingRowCount: 1,
    });
  });

  it("uses only the latest public amendment for the same ELEC report identity", async () => {
    const result = await aggregateNewJerseyOutsideSpendingFromElecFilings({
      candidateName: "Mikie Sherrill",
      electionYear: 2025,
      outsideGroupEntityS: 477267,
      outsideGroupName: "ONE GIANT LEAP PAC - OGL PAC",
      filings: [
        filing({
          docId: 3909737,
          amendmentNumber: 0,
          dateReceived: "2025-12-01T00:00:00",
        }),
        filing({
          docId: 3909738,
          amendmentNumber: 1,
          dateReceived: "2025-12-29T00:00:00",
        }),
      ],
      elecClient: {
        getReportDownload: async (docId) => ({
          docId,
          fileNameWithSas: `https://storage.example/${docId}.pdf?sv=short-lived`,
          sourceUrl: `https://www.njelecefilesearch.com/SearchIndExpReports/?handler=DownloadReport&DocId=${docId}`,
        }),
      },
      textExtractor: async ({ docId }) => (docId === 3909738 ? REPORT_TEXT : "MIKIE SHERRILL FOR GOVERNOR unreadable amount"),
    });

    expect(result.filingRowCount).toBe(1);
    expect(result.downloadedReportCount).toBe(1);
    expect(result.summary?.groups[0]?.docIds).toEqual([3909738]);
    expect(result.summary?.supportTotal).toBe(100082.02);
  });
});
