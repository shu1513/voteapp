import { afterEach, describe, expect, it, vi } from "vitest";

import {
  buildFloridaContributionExportCacheKey,
  buildFloridaContributionExportFormData,
  buildFloridaContributionExportTransportRequest,
  createFloridaContributionExportRateLimiter,
  exportFloridaContributionRows,
  floridaContributionExportFormDataObject,
  type FloridaContributionExportTransport,
} from "../../../src/pipeline/floridaFinance/floridaCampaignFinanceClient.js";

const ORIGINAL_FLORIDA_FINANCE_VALUE = process.env.FLORIDA_CAMPAIGN_FINANCE_ENABLED;
const ORIGINAL_FLORIDA_BROWSER_EXPORT_VALUE = process.env.FLORIDA_CAMPAIGN_FINANCE_BROWSER_EXPORT_ENABLED;

const SAMPLE_TSV = [
  "Candidate/Committee\tDate\tAmount\tTyp\tContributor Name\tAddress\tCity\tState\tZip\tOccupation\tInkind Desc",
  "Friends of Jane Doe\t9/15/2026\t100\tCHE\tSmith, Pat\t1 Main St\tTallahassee\tFL\t32301\tAttorney\t",
].join("\n");

afterEach(() => {
  vi.restoreAllMocks();
  if (ORIGINAL_FLORIDA_FINANCE_VALUE === undefined) {
    delete process.env.FLORIDA_CAMPAIGN_FINANCE_ENABLED;
  } else {
    process.env.FLORIDA_CAMPAIGN_FINANCE_ENABLED = ORIGINAL_FLORIDA_FINANCE_VALUE;
  }
  if (ORIGINAL_FLORIDA_BROWSER_EXPORT_VALUE === undefined) {
    delete process.env.FLORIDA_CAMPAIGN_FINANCE_BROWSER_EXPORT_ENABLED;
  } else {
    process.env.FLORIDA_CAMPAIGN_FINANCE_BROWSER_EXPORT_ENABLED = ORIGINAL_FLORIDA_BROWSER_EXPORT_VALUE;
  }
});

describe("floridaCampaignFinanceClient", () => {
  it("builds deterministic candidate contribution export form data", () => {
    const input = {
      searchType: "candidate_detail" as const,
      electionCode: "20261103-GEN",
      candidateFirstName: " Jane ",
      candidateLastName: " Doe ",
      rowLimit: 2500,
    };
    const params = buildFloridaContributionExportFormData(input);

    expect(floridaContributionExportFormDataObject(params)).toEqual({
      search_on: "2",
      queryformat: "2",
      rowlimit: "2500",
      Election: "20261103-GEN",
      CanFName: "Jane",
      CanLName: "Doe",
    });
    expect(buildFloridaContributionExportCacheKey(input)).toMatch(
      /^fl-contrib-candidate-20261103-gen-doe-jane-[a-f0-9]{12}$/
    );
    expect(buildFloridaContributionExportCacheKey(input)).toBe(buildFloridaContributionExportCacheKey(input));
  });

  it("builds deterministic committee contribution export form data", () => {
    const params = buildFloridaContributionExportFormData({
      searchType: "committee_detail",
      electionCode: "20261103-GEN",
      committeeType: "PAC",
      committeeName: " Floridians for Jane ",
      dateFrom: "01/01/2025",
      dateTo: "12/31/2026",
    });

    expect(floridaContributionExportFormDataObject(params)).toEqual({
      search_on: "4",
      queryformat: "2",
      rowlimit: "10000",
      Election: "20261103-GEN",
      date_from: "01/01/2025",
      date_to: "12/31/2026",
      committee: "PAC",
      ComName: "Floridians for Jane",
    });
  });

  it("rejects broad candidate and committee export requests", () => {
    expect(() =>
      buildFloridaContributionExportFormData({
        searchType: "candidate_detail",
        candidateFirstName: "Jane",
      })
    ).toThrow("requires candidateFirstName and candidateLastName");
    expect(() =>
      buildFloridaContributionExportFormData({
        searchType: "committee_detail",
      })
    ).toThrow("requires committeeName");
    expect(() =>
      buildFloridaContributionExportFormData({
        searchType: "committee_detail",
        committeeName: "PAC",
        rowLimit: 100_001,
      })
    ).toThrow("rowLimit must be an integer");
  });

  it("requires the browser export feature flag before invoking the transport", async () => {
    delete process.env.FLORIDA_CAMPAIGN_FINANCE_ENABLED;
    delete process.env.FLORIDA_CAMPAIGN_FINANCE_BROWSER_EXPORT_ENABLED;
    const transport = vi.fn<FloridaContributionExportTransport>();

    await expect(
      exportFloridaContributionRows({
        searchType: "candidate_detail",
        candidateFirstName: "Jane",
        candidateLastName: "Doe",
        transport,
      })
    ).rejects.toThrow("Florida campaign finance browser export is disabled");
    expect(transport).not.toHaveBeenCalled();
  });

  it("invokes the injected browser transport and parses returned TSV rows", async () => {
    process.env.FLORIDA_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.FLORIDA_CAMPAIGN_FINANCE_BROWSER_EXPORT_ENABLED = "false";
    const transport = vi.fn<FloridaContributionExportTransport>(async (request) => {
      expect(request.formData.get("search_on")).toBe("2");
      expect(request.cacheKey).toMatch(/^fl-contrib-candidate-20261103-gen-doe-jane-[a-f0-9]{12}$/);
      return {
        tsv: SAMPLE_TSV,
        finalUrl: `${request.exportUrl}?download=1`,
        retrievedAt: new Date("2026-06-20T20:00:00.000Z"),
      };
    });

    const result = await exportFloridaContributionRows({
      searchType: "candidate_detail",
      electionCode: "20261103-GEN",
      candidateFirstName: "Jane",
      candidateLastName: "Doe",
      transport,
      force: true,
    });

    expect(transport).toHaveBeenCalledTimes(1);
    expect(result).toMatchObject({
      sourceUrl: "https://dos.elections.myflorida.com/cgi-bin/contrib.exe?download=1",
      rowCount: 1,
      formData: {
        search_on: "2",
        queryformat: "2",
        rowlimit: "10000",
        Election: "20261103-GEN",
        CanFName: "Jane",
        CanLName: "Doe",
      },
    });
    expect(result.retrievedAt.toISOString()).toBe("2026-06-20T20:00:00.000Z");
    expect(result.rows[0]).toMatchObject({
      contributorName: "Smith, Pat",
      occupation: "Attorney",
      electionCode: "20261103-GEN",
      sourceUrl: "https://dos.elections.myflorida.com/cgi-bin/contrib.exe?download=1",
    });
  });

  it("runs an optional rate limiter before invoking the injected browser transport", async () => {
    process.env.FLORIDA_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.FLORIDA_CAMPAIGN_FINANCE_BROWSER_EXPORT_ENABLED = "true";
    const order: string[] = [];
    const rateLimiter = vi.fn(async () => {
      order.push("rate-limit");
    });
    const transport = vi.fn<FloridaContributionExportTransport>(async () => {
      order.push("transport");
      return {
        tsv: SAMPLE_TSV,
        retrievedAt: new Date("2026-06-20T20:00:00.000Z"),
      };
    });

    await exportFloridaContributionRows({
      searchType: "candidate_detail",
      candidateFirstName: "Jane",
      candidateLastName: "Doe",
      transport,
      rateLimiter,
    });

    expect(order).toEqual(["rate-limit", "transport"]);
    expect(rateLimiter).toHaveBeenCalledWith(expect.objectContaining({ cacheKey: expect.any(String) }));
  });

  it("creates a min-interval export rate limiter", async () => {
    let currentTimeMs = 1000;
    const sleepCalls: number[] = [];
    const rateLimiter = createFloridaContributionExportRateLimiter({
      minIntervalMs: 500,
      now: () => currentTimeMs,
      sleep: async (ms) => {
        sleepCalls.push(ms);
        currentTimeMs += ms;
      },
    });
    const request = buildFloridaContributionExportTransportRequest({
      searchType: "candidate_detail",
      candidateFirstName: "Jane",
      candidateLastName: "Doe",
    });

    await rateLimiter(request);
    currentTimeMs += 200;
    await rateLimiter(request);
    currentTimeMs += 500;
    await rateLimiter(request);

    expect(sleepCalls).toEqual([300]);
  });

  it("builds the transport request without calling the live Florida site", () => {
    const request = buildFloridaContributionExportTransportRequest({
      searchType: "committee_detail",
      committeeName: "Floridians for Jane",
    });

    expect(request.searchPageUrl).toBe("https://dos.elections.myflorida.com/campaign-finance/contributions/");
    expect(request.exportUrl).toBe("https://dos.elections.myflorida.com/cgi-bin/contrib.exe");
    expect(request.formData.get("search_on")).toBe("4");
  });
});
