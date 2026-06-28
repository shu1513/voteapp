import { describe, expect, it, vi } from "vitest";

import {
  aggregateVermontOutsideGroupContributions,
  fetchAndAggregateVermontOutsideGroupContributions,
} from "../../../src/pipeline/vermontFinance/vermontOutsideGroupContributionAggregator.js";
import type { VermontContributionRow } from "../../../src/pipeline/vermontFinance/vermontCampaignFinanceClient.js";
import type { VermontOutsideSpendingGroup } from "../../../src/pipeline/vermontFinance/vermontOutsideSpendingAggregator.js";

function contribution(overrides: Partial<VermontContributionRow> = {}): VermontContributionRow {
  return {
    transactionId: 1,
    transactionVersionId: 1,
    guid: "contribution-guid-1",
    filerRegistrationGuid: "pac-guid",
    filerName: "VERMONT FUTURE PAC",
    transactionAmount: 25000,
    transactionDate: "08/01/2024",
    sourceName: "Sierra Club",
    sourceFirstName: null,
    sourceLastName: null,
    sourceMiddleName: null,
    transactionSource: "Business/Group/Organization",
    transactionSourceTypeCode: "TBSN",
    transactionSubTypeCode: "ITMY",
    transactionSubTypeDescription: "Monetary Contribution",
    filerTypeCode: "PAC",
    filerTypeDescription: "Political Action Committee",
    electionYear: 2024,
    electionCycle: "2024 General",
    electionId: 35,
    officeId: null,
    officeType: null,
    entityId: 90001,
    reportName: "10/01/2024 - GENERAL",
    candidateFirstName: null,
    candidateLastName: null,
    candidateMiddleName: null,
    occupation: null,
    employer: null,
    filingYear: 2024,
    addressLine1: "1 Main St",
    addressLine2: null,
    city: "Montpelier",
    stateCode: "VT",
    zipCode: "05602",
    ...overrides,
  };
}

function outsideGroup(overrides: Partial<VermontOutsideSpendingGroup> = {}): VermontOutsideSpendingGroup {
  return {
    filerRegistrationGuid: "pac-guid",
    filerName: "VERMONT FUTURE PAC",
    supportOppose: "support",
    supportMechanism: "vt_pac_contribution_to_registrant",
    amount: 1000,
    expenditureCount: 1,
    entityId: 90001,
    sourceUrl: "https://campaignfinance.vermont.gov/",
    ...overrides,
  };
}

function jsonResponse(payload: unknown): Response {
  return new Response(JSON.stringify(payload), { status: 200, statusText: "OK" });
}

describe("vermontOutsideGroupContributionAggregator", () => {
  it("backtraces supporting PAC receipts into organization donors and industries", () => {
    const result = aggregateVermontOutsideGroupContributions({
      electionYear: 2024,
      minIndustryAmount: 1000,
      sourceUrl: "https://campaignfinance.vermont.gov/",
      outsideGroups: [outsideGroup()],
      contributionRows: [
        contribution({ transactionAmount: 20000, sourceName: "Sierra Club" }),
        contribution({ transactionId: 2, guid: "row-2", transactionAmount: 15000, sourceName: "Sierra Club" }),
        contribution({ transactionId: 3, guid: "row-3", transactionAmount: 30000, sourceName: "IBEW Local 300", transactionSourceTypeCode: "TPAC" }),
        contribution({ transactionId: 4, guid: "individual", transactionAmount: 50000, sourceName: "DOE, JANE", transactionSource: "Individual", transactionSourceTypeCode: "TIND" }),
      ],
    });

    expect(result).toEqual({
      matchedContributionRowCount: 4,
      includedContributionRowCount: 3,
      skippedContributionRowCount: 1,
      outsideGroupBreakdowns: [
        {
          filerRegistrationGuid: "pac-guid",
          supportOppose: "support",
          categoryType: "donor",
          categoryName: "Sierra Club",
          amount: 35000,
          contributorCount: 1,
          sourceUrl: "https://campaignfinance.vermont.gov/",
        },
        {
          filerRegistrationGuid: "pac-guid",
          supportOppose: "support",
          categoryType: "donor",
          categoryName: "IBEW Local 300",
          amount: 30000,
          contributorCount: 1,
          sourceUrl: "https://campaignfinance.vermont.gov/",
        },
        {
          filerRegistrationGuid: "pac-guid",
          supportOppose: "support",
          categoryType: "industry",
          categoryName: "environmental_group",
          amount: 35000,
          contributorCount: 1,
          sourceUrl: "https://campaignfinance.vermont.gov/",
        },
        {
          filerRegistrationGuid: "pac-guid",
          supportOppose: "support",
          categoryType: "industry",
          categoryName: "labor_unions",
          amount: 30000,
          contributorCount: 1,
          sourceUrl: "https://campaignfinance.vermont.gov/",
        },
      ],
    });
  });

  it("skips ambiguous support side when one PAC has support and oppose groups", () => {
    const result = aggregateVermontOutsideGroupContributions({
      electionYear: 2024,
      outsideGroups: [outsideGroup({ supportOppose: "support" }), outsideGroup({ supportOppose: "oppose" })],
      contributionRows: [contribution()],
    });

    expect(result.matchedContributionRowCount).toBe(1);
    expect(result.includedContributionRowCount).toBe(0);
    expect(result.skippedContributionRowCount).toBe(1);
    expect(result.outsideGroupBreakdowns).toEqual([]);
  });

  it("fetches PAC incoming contributions and aggregates them", async () => {
    const fetchImpl = vi.fn().mockImplementation((_url: URL | RequestInfo, init?: RequestInit) => {
      const body = JSON.parse(String(init?.body ?? "{}")) as {
        filerRegistrationGuid?: string;
        pageNumber?: number;
      };
      if (body.filerRegistrationGuid === "pac-guid" && body.pageNumber === 1) {
        return Promise.resolve(
          jsonResponse({
            data: {
              items: [
                {
                  transactionID: 1,
                  guid: "contribution-guid-1",
                  filerRegistrationGuid: "pac-guid",
                  filerName: "VERMONT FUTURE PAC",
                  transactionAmount: 25000,
                  sourceName: "Sierra Club",
                  transactionSource: "Business/Group/Organization",
                  transactionSourceTypeCode: "TBSN",
                  filerTypeCode: "PAC",
                  filerTypeDescription: "Political Action Committee",
                  electionYear: 2024,
                },
              ],
              totalItems: 2,
            },
            succeeded: true,
            error: null,
          })
        );
      }
      if (body.filerRegistrationGuid === "pac-guid" && body.pageNumber === 2) {
        return Promise.resolve(
          jsonResponse({
            data: {
              items: [
                {
                  transactionID: 2,
                  guid: "contribution-guid-2",
                  filerRegistrationGuid: "pac-guid",
                  filerName: "VERMONT FUTURE PAC",
                  transactionAmount: 5000,
                  sourceName: "Sierra Club",
                  transactionSource: "Business/Group/Organization",
                  transactionSourceTypeCode: "TBSN",
                  filerTypeCode: "PAC",
                  filerTypeDescription: "Political Action Committee",
                  electionYear: 2024,
                },
              ],
              totalItems: 2,
            },
            succeeded: true,
            error: null,
          })
        );
      }
      return Promise.resolve(jsonResponse({ data: { items: [], totalItems: 0 }, succeeded: true, error: null }));
    }) as unknown as typeof fetch;

    await expect(
      fetchAndAggregateVermontOutsideGroupContributions(
        {
          electionYear: 2024,
          minIndustryAmount: 1000,
          pageSize: 1,
          outsideGroups: [outsideGroup()],
        },
        { fetchImpl, timeoutMs: 1000 }
      )
    ).resolves.toMatchObject({
      fetchedContributionRowCount: 2,
      matchedContributionRowCount: 2,
      includedContributionRowCount: 2,
      outsideGroupBreakdowns: expect.arrayContaining([
        expect.objectContaining({ categoryType: "donor", categoryName: "Sierra Club", amount: 30000 }),
        expect.objectContaining({ categoryType: "industry", categoryName: "environmental_group", amount: 30000 }),
      ]),
    });

    expect(fetchImpl).toHaveBeenCalledTimes(2);
    expect(JSON.parse(String(vi.mocked(fetchImpl).mock.calls[0]?.[1]?.body))).toMatchObject({
      filerRegistrationGuid: "pac-guid",
      electionYear: 2024,
      transactionTypeCode: "TCON",
      pageNumber: 1,
    });
    expect(JSON.parse(String(vi.mocked(fetchImpl).mock.calls[1]?.[1]?.body))).toMatchObject({
      filerRegistrationGuid: "pac-guid",
      electionYear: 2024,
      transactionTypeCode: "TCON",
      pageNumber: 2,
    });
  });

  it("validates inputs", () => {
    expect(() =>
      aggregateVermontOutsideGroupContributions({ electionYear: 1999, outsideGroups: [], contributionRows: [] })
    ).toThrow("Invalid Vermont outside group contribution election year");
    expect(() =>
      aggregateVermontOutsideGroupContributions({
        electionYear: 2024,
        outsideGroups: [],
        contributionRows: [],
        maxBreakdownsPerCategory: 0,
      })
    ).toThrow("maxBreakdownsPerCategory");
    expect(() =>
      aggregateVermontOutsideGroupContributions({
        electionYear: 2024,
        outsideGroups: [],
        contributionRows: [],
        minIndustryAmount: -1,
      })
    ).toThrow("minIndustryAmount");
  });
});
