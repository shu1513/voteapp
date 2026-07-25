import { describe, expect, it, vi } from "vitest";

import {
  ALASKA_APOC_CAMPAIGN_INCOME_URL,
  ALASKA_APOC_IE_CONTRIBUTIONS_URL,
  ALASKA_APOC_IE_EXPENDITURES_URL,
  fetchAlaskaApocCsv,
  fetchAlaskaApocFinanceCsvBundle,
  parseAlaskaApocAmount,
  parseAlaskaApocCampaignIncomeCsv,
  parseAlaskaApocDateYear,
  parseAlaskaApocIndependentContributionCsv,
  parseAlaskaApocIndependentExpenditureCsv,
} from "../../../src/pipeline/alaskaFinance/alaskaApocClient.js";

describe("alaskaApocClient", () => {
  it("parses APOC campaign income CSV exports", () => {
    const rows = parseAlaskaApocCampaignIncomeCsv(
      [
        "Filer,Filer Type,Name,Date,Type,Contributor/Vendor,Address,City,State,Zip,Country,Payment Type,Payment Detail,Occupation,Employer,Purpose,Amount,Submitted,Status",
        "\"Doe, Jane\",Candidate,\"Doe, Jane\",10/01/2026,Income,\"Smith, Pat\",\"1 Main St\",Juneau,AK,99801,USA,Check,1001,Attorney,\"Law Firm, LLP\",Contribution,\"$1,200.50\",10/02/2026,\"Complete, Not Amended\"",
        "\"Doe, Jane\",Candidate,\"Doe, Jane\",10/02/2026,Income,Bad Amount,,,,,,,,,,,bad,,Complete",
      ].join("\n"),
      { sourceUrl: ALASKA_APOC_CAMPAIGN_INCOME_URL }
    );

    expect(rows).toEqual([
      expect.objectContaining({
        filerName: "Doe, Jane",
        name: "Doe, Jane",
        contributor: "Smith, Pat",
        occupation: "Attorney",
        employer: "Law Firm, LLP",
        amount: 1200.5,
        reportYear: 2026,
        status: "Complete, Not Amended",
        sourceUrl: ALASKA_APOC_CAMPAIGN_INCOME_URL,
      }),
    ]);
  });

  // Header rows below are copied from real APOC "Export All Pages / .CSV"
  // downloads, which use different column names than the legacy fixtures above.
  it("parses the official APOC campaign income export column names", () => {
    const rows = parseAlaskaApocCampaignIncomeCsv(
      [
        "Result,Date,Transaction Type,Payment Type,Payment Detail,Amount,Last/Business Name,First Name,Address,City,State,Zip,Country,Occupation,Employer,Purpose of Expenditure,--------,Report Type,Election Name,Election Type,Municipality,Office,Filer Type,Name,Report Year,Submitted",
        "1,12/24/2024,Income,Check,2408,\"$1,000.00\",\"Public Employees Local 71\",,\"2510 Arctic Blvd\",Anchorage,Alaska,99503,USA,PAC,PAC,,,\"Previous Year Start Report\",\"2026 - Anchorage Municipal Election\",\"Anchorage Municipal\",\"Anchorage, City and Borough\",Assembly,Candidate,\"Dave Donley\",2026,2/9/2025",
      ].join("\n")
    );

    expect(rows).toEqual([
      expect.objectContaining({
        // The export has no Filer ID or Filer Name column; "Name" carries the
        // filing candidate and the resolver keys off it.
        filerId: "",
        filerName: "Dave Donley",
        name: "Dave Donley",
        filerType: "Candidate",
        type: "Income",
        contributor: "Public Employees Local 71",
        occupation: "PAC",
        amount: 1000,
        reportYear: 2026,
      }),
    ]);
  });

  it("parses the official APOC independent expenditure export column names", () => {
    const rows = parseAlaskaApocIndependentExpenditureCsv(
      [
        "Result,Date,Payment Type,Payment Detail,Amount,Recipient,Recipient Address,Recipient City,Recipient State,Recipient Zip,Recipient Country,Election Year,Election Name,Candidate/Proposition,Position,--------,Report Year,Filer Type,Filer Name,Submitted,Status",
        "1,5/28/2026,\"Debit Card\",,\"$3,150.00\",\"A.T. Publishing\",\"1720 Abbott Rd\",Anchorage,Alaska,99507,USA,2026,\"State General\",\"2 - Repeal\",Supports,,2026,\"Registered Group\",\"2026 - Repeal Now\",6/1/2026,Complete",
      ].join("\n")
    );

    expect(rows).toEqual([
      expect.objectContaining({
        filerName: "2026 - Repeal Now",
        recipient: "A.T. Publishing",
        city: "Anchorage",
        state: "Alaska",
        zip: "99507",
        election: "State General",
        candidateProposition: "2 - Repeal",
        position: "Supports",
        amount: 3150,
        reportYear: 2026,
      }),
    ]);
  });

  it("parses APOC independent expenditure CSV exports", () => {
    const rows = parseAlaskaApocIndependentExpenditureCsv(
      [
        "Filer Name,Filer,Filer Type,Report Year,Business Phone,Business Type,Type,Date,Recipient,Address,City,State,Zip,Country,Position,Candidate/Proposition,Description,Report Type,Election,Payment Type,Payment Detail,Amount,Submitted,Status",
        "Alaska Future PAC,8001,Group,2026,907-555-0100,Super PAC,Expenditure,09/15/2026,Vendor,1 Main,Anchorage,AK,99501,USA,Support,Jane Doe,Mailers,24-hour,General,Card,ad buy,\"$25,000.00\",09/16/2026,Complete",
      ].join("\n"),
      { sourceUrl: ALASKA_APOC_IE_EXPENDITURES_URL }
    );

    expect(rows).toEqual([
      expect.objectContaining({
        filerId: "8001",
        filerName: "Alaska Future PAC",
        reportYear: 2026,
        position: "Support",
        candidateProposition: "Jane Doe",
        amount: 25000,
        sourceUrl: ALASKA_APOC_IE_EXPENDITURES_URL,
      }),
    ]);
  });

  it("parses APOC independent expenditure contribution CSV exports", () => {
    const rows = parseAlaskaApocIndependentContributionCsv(
      [
        "Filer Name,Filer,Filer Type,Report Year,Business Phone,Business Type,Type,Date,Contributor,Contributor Address,Contributor City,Contributor State,Contributor Zip,Contributor Country,Employer,Occupation,Report Type,Election,Officers,Amount,Submitted,Status",
        "Alaska Future PAC,8001,Group,2026,907-555-0100,Super PAC,Contribution,09/01/2026,Energy Transfer LLC,2 Energy Rd,Dallas,TX,75001,USA,,,24-hour,General,,\"$30,000.00\",09/02/2026,Complete",
      ].join("\n"),
      { sourceUrl: ALASKA_APOC_IE_CONTRIBUTIONS_URL }
    );

    expect(rows).toEqual([
      expect.objectContaining({
        filerId: "8001",
        filerName: "Alaska Future PAC",
        contributor: "Energy Transfer LLC",
        amount: 30000,
        sourceUrl: ALASKA_APOC_IE_CONTRIBUTIONS_URL,
      }),
    ]);
  });

  it("parses APOC amount and date primitives conservatively", () => {
    expect(parseAlaskaApocAmount("$1,234.56")).toBe(1234.56);
    expect(parseAlaskaApocAmount("($25.00)")).toBe(-25);
    expect(parseAlaskaApocAmount("bad")).toBeNull();
    expect(parseAlaskaApocDateYear("10/01/2026")).toBe(2026);
    expect(parseAlaskaApocDateYear("2026-10-01T00:00:00")).toBe(2026);
    expect(parseAlaskaApocDateYear("")).toBeNull();
  });

  it("rejects blank APOC CSV exports instead of treating them as valid empty data", () => {
    expect(() => parseAlaskaApocCampaignIncomeCsv(" \n \n")).toThrow(
      "Alaska APOC CSV export is missing a header row"
    );
  });

  it("fetches APOC CSV exports with retry and timeout options", async () => {
    const fetchFn = vi
      .fn()
      .mockResolvedValueOnce(new Response("temporary", { status: 503 }))
      .mockResolvedValueOnce(new Response("Name,Amount\nJane,$1.00\n", { status: 200 }));

    await expect(
      fetchAlaskaApocCsv(ALASKA_APOC_CAMPAIGN_INCOME_URL, {
        fetchFn,
        timeoutMs: 1000,
        retryCount: 1,
        retryDelayMs: 0,
      })
    ).resolves.toBe("Name,Amount\nJane,$1.00\n");
    expect(fetchFn).toHaveBeenCalledTimes(2);
  });

  it("rejects non-HTTPS APOC CSV URLs", async () => {
    const fetchFn = vi.fn();

    await expect(
      fetchAlaskaApocCsv("http://aws.state.ak.us/ApocReports/CampaignDisclosure/CDIncome.aspx", {
        fetchFn,
        timeoutMs: 1000,
        retryCount: 0,
      })
    ).rejects.toThrow("Only https is allowed");
    expect(fetchFn).not.toHaveBeenCalled();
  });

  it("rejects APOC HTML report pages instead of treating them as empty CSV exports", async () => {
    const fetchFn = vi.fn().mockResolvedValue(
      new Response("<!doctype html><html><body><form><table><tr><td>No CSV here</td></tr></table></form></body></html>", {
        status: 200,
        headers: { "content-type": "text/html; charset=utf-8" },
      })
    );

    await expect(
      fetchAlaskaApocCsv(ALASKA_APOC_CAMPAIGN_INCOME_URL, {
        fetchFn,
        timeoutMs: 1000,
        retryCount: 0,
      })
    ).rejects.toThrow("returned an HTML report page instead of a CSV export");
  });

  it("does not reject CSV fields that contain HTML-like text", async () => {
    const fetchFn = vi.fn().mockResolvedValue(
      new Response("Name,Amount\n\"<form value>\",$1.00\n", {
        status: 200,
        headers: { "content-type": "text/csv" },
      })
    );

    await expect(
      fetchAlaskaApocCsv(ALASKA_APOC_CAMPAIGN_INCOME_URL, {
        fetchFn,
        timeoutMs: 1000,
        retryCount: 0,
      })
    ).resolves.toBe("Name,Amount\n\"<form value>\",$1.00\n");
  });

  it("fetches an APOC CSV bundle with source URL provenance", async () => {
    const fetchFn = vi
      .fn()
      .mockResolvedValueOnce(new Response("Name,Amount\nJane,$1.00\n", { status: 200 }))
      .mockResolvedValueOnce(new Response("Name,Amount\nPAC,$2.00\n", { status: 200 }));

    const bundle = await fetchAlaskaApocFinanceCsvBundle({
      fetchFn,
      retryDelayMs: 0,
      requestSpacingMs: 0,
      includeIndependentContributions: false,
    });

    expect(bundle).toMatchObject({
      incomeSourceUrl: ALASKA_APOC_CAMPAIGN_INCOME_URL,
      independentExpenditureSourceUrl: ALASKA_APOC_IE_EXPENDITURES_URL,
      independentContributionSourceUrl: null,
    });
    expect(fetchFn).toHaveBeenCalledTimes(2);
  });

  it("does not validate disabled independent expenditure or contribution URLs", async () => {
    const fetchFn = vi.fn().mockResolvedValue(new Response("Name,Amount\nJane,$1.00\n", { status: 200 }));

    const bundle = await fetchAlaskaApocFinanceCsvBundle({
      fetchFn,
      retryDelayMs: 0,
      requestSpacingMs: 0,
      includeIndependentExpenditures: false,
      includeIndependentContributions: false,
      independentExpenditureUrl: "not a url",
      independentContributionUrl: "also not a url",
    });

    expect(bundle).toMatchObject({
      independentExpenditureSourceUrl: null,
      independentContributionSourceUrl: null,
    });
    expect(fetchFn).toHaveBeenCalledTimes(1);
  });
});
