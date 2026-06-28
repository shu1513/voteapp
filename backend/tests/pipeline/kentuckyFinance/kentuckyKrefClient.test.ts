import { afterEach, describe, expect, it, vi } from "vitest";

import {
  KENTUCKY_KREF_IE_ONLY_ORGANIZATION_TYPE,
  KentuckyKrefClientError,
  buildKentuckyKrefContributionExportUrl,
  buildKentuckyKrefIndependentExpenditureExportUrl,
  buildKentuckyKrefPublicSearchPageUrl,
  downloadKentuckyKrefCandidateContributions,
  downloadKentuckyKrefIeOnlyCommitteeContributions,
  downloadKentuckyKrefIndependentExpenditures,
  fetchKentuckyKrefCandidateElectionDateOptions,
  fetchKentuckyKrefIndependentExpenditureElectionDateOptions,
  kentuckyKrefContributionRecordFromRow,
  kentuckyKrefIndependentExpenditureRecordFromRow,
  parseKentuckyKrefCsvRows,
  parseKentuckyKrefDropdownOptions,
  parseKentuckyKrefElectionDateOptions,
} from "../../../src/pipeline/kentuckyFinance/kentuckyKrefClient.js";

function responseWithText(body: string, init: ResponseInit = {}): Response {
  return new Response(body, {
    status: 200,
    statusText: "OK",
    ...init,
  });
}

describe("kentuckyKrefClient", () => {
  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  it("builds KREF contribution export URLs with official query names", () => {
    const url = new URL(
      buildKentuckyKrefContributionExportUrl({
        contributionSearchType: "Candidate",
        candidateFirstName: "Andy",
        candidateLastName: "Beshear",
        electionDate: "5/16/2023",
        contributionTypes: ["INDIVIDUAL", "KYPAC"],
        paymentCodes: ["MONETARY"],
        minAmount: "25.50",
      })
    );

    expect(url.origin + url.pathname).toBe("https://secure.kentucky.gov/kref/publicsearch/ExportContributors");
    expect(url.searchParams.get("ContributionSearchType")).toBe("Candidate");
    expect(url.searchParams.get("CandidateFirstName")).toBe("Andy");
    expect(url.searchParams.get("CandidateLastName")).toBe("Beshear");
    expect(url.searchParams.get("ElectionDate")).toBe("5/16/2023");
    expect(url.searchParams.getAll("ContributionTypes")).toEqual(["INDIVIDUAL", "KYPAC"]);
    expect(url.searchParams.getAll("PaymentCodes")).toEqual(["MONETARY"]);
    expect(url.searchParams.get("MinAmount")).toBe("25.5");
  });

  it("builds KREF independent expenditure export URLs", () => {
    const url = new URL(
      buildKentuckyKrefIndependentExpenditureExportUrl({
        candidateFirstName: "Andy",
        candidateLastName: "Beshear",
        isSupported: true,
        minimalElectionDate: "5/16/2023",
      })
    );

    expect(url.origin + url.pathname).toBe(
      "https://secure.kentucky.gov/kref/publicsearch/IndependentExpenditureSearch/ExportIndependentExpenditures"
    );
    expect(url.searchParams.get("CandidateFirstName")).toBe("Andy");
    expect(url.searchParams.get("CandidateLastName")).toBe("Beshear");
    expect(url.searchParams.get("IsSupported")).toBe("true");
    expect(url.searchParams.get("MinimalElectionDate")).toBe("5/16/2023");
  });

  it("builds KREF public search page URLs for resolver dropdown discovery", () => {
    expect(buildKentuckyKrefPublicSearchPageUrl("candidate_contributions")).toBe(
      "https://secure.kentucky.gov/kref/publicsearch/ToCandidateSearch"
    );
    expect(buildKentuckyKrefPublicSearchPageUrl("organization_contributions")).toBe(
      "https://secure.kentucky.gov/kref/publicsearch/ToOrganizationSearch"
    );
    expect(buildKentuckyKrefPublicSearchPageUrl("independent_expenditures")).toBe(
      "https://secure.kentucky.gov/kref/publicsearch/IndependentExpenditureSearch"
    );
  });

  it("rejects unsupported contribution search types and negative amount filters", () => {
    expect(() =>
      buildKentuckyKrefContributionExportUrl({
        contributionSearchType: "Other" as "Candidate",
      })
    ).toThrow(KentuckyKrefClientError);

    expect(() =>
      buildKentuckyKrefIndependentExpenditureExportUrl({
        minAmount: -1,
      })
    ).toThrow("nonnegative amount");
  });

  it("parses quoted KREF CSV rows and normalizes headers", () => {
    expect(
      parseKentuckyKrefCsvRows(
        '"To Organization","Contributor Last Name","Contributor First Name","Amount","Receipt Date"\n' +
          '"Kentucky Future Project Action Fund","Doe","Jane","$1,250.50","6/10/2026"\n'
      )
    ).toEqual([
      {
        to_organization: "Kentucky Future Project Action Fund",
        contributor_last_name: "Doe",
        contributor_first_name: "Jane",
        amount: "$1,250.50",
        receipt_date: "6/10/2026",
      },
    ]);
  });

  it("parses KREF dropdown options from search-page HTML", () => {
    const html = `
      <select id="ElectionDate" name="ElectionDate">
        <option value="">Select Election Date</option>
        <option value="11/7/2023">11/7/2023</option>
        <option value="5/16/2023">May &amp; Primary</option>
      </select>
      <select id="OfficeSought" name="OfficeSought">
        <option value="Governor">Governor</option>
      </select>
    `;

    expect(parseKentuckyKrefDropdownOptions(html, "ElectionDate")).toEqual([
      { value: "", label: "Select Election Date" },
      { value: "11/7/2023", label: "11/7/2023" },
      { value: "5/16/2023", label: "May & Primary" },
    ]);
    expect(parseKentuckyKrefElectionDateOptions(html)).toEqual([
      { value: "11/7/2023", label: "11/7/2023" },
      { value: "5/16/2023", label: "May & Primary" },
    ]);
  });

  it("falls back to select id when dropdown name is absent", () => {
    const html = `
      <select id="ElectionDate">
        <option value="11/7/2023">11/7/2023</option>
      </select>
    `;

    expect(parseKentuckyKrefDropdownOptions(html, "ElectionDate")).toEqual([{ value: "11/7/2023", label: "11/7/2023" }]);
  });

  it("maps candidate contribution exports to positive contribution records", () => {
    expect(
      kentuckyKrefContributionRecordFromRow({
        recipient_first_name: "Andy",
        recipient_last_name: "Beshear",
        office_sought: "GOVERNOR",
        location: "STATEWIDE",
        election_date: "5/16/2023",
        election_type: "PRIMARY",
        contributor_first_name: "Ashley",
        contributor_last_name: "Adkins",
        amount: "-25.00",
        contribution_type: "INDIVIDUAL",
        contribution_mode: "DIRECT",
        occupation: "Owner",
        employer: "Adkins & Ferguson LLC",
        city: "Morehead",
        state: "KY",
        zip: "40351",
        receipt_date: "11/18/2023",
        statement_type: "ANNUAL",
      })
    ).toEqual({
      candidateName: "Andy Beshear",
      candidateFirstName: "Andy",
      candidateLastName: "Beshear",
      recipientName: "Andy Beshear",
      office: "GOVERNOR",
      location: "STATEWIDE",
      electionDate: "5/16/2023",
      electionYear: 2023,
      electionType: "PRIMARY",
      contributorName: "Ashley Adkins",
      contributorType: "INDIVIDUAL",
      contributionMode: "DIRECT",
      occupation: "Owner",
      employer: "Adkins & Ferguson LLC",
      city: "Morehead",
      state: "KY",
      zip: "40351",
      amount: 25,
      receiptDate: "11/18/2023",
      statementType: "ANNUAL",
    });

    expect(kentuckyKrefContributionRecordFromRow({ amount: "0.00" })).toBeNull();
  });

  it("maps IE-only organization contribution exports", () => {
    expect(
      kentuckyKrefContributionRecordFromRow({
        to_organization: "Kentucky Future Project Action Fund",
        from_organization_name: "DBL Law",
        amount: "245.00",
        contribution_type: "OTHER",
        contribution_mode: "DIRECT",
        receipt_date: "6/1/2026",
      })
    ).toEqual({
      toOrganizationName: "Kentucky Future Project Action Fund",
      recipientName: "Kentucky Future Project Action Fund",
      contributorName: "DBL Law",
      contributorType: "OTHER",
      contributionMode: "DIRECT",
      amount: 245,
      receiptDate: "6/1/2026",
    });
  });

  it("maps independent expenditure exports including KREF's To Whome Made typo", () => {
    expect(
      kentuckyKrefIndependentExpenditureRecordFromRow({
        to_whome_made: "Media Vendor",
        name: "Kentucky Future Project Action Fund",
        date: "10/15/2023",
        candidate_name: "Andy Beshear",
        support_oppose: "Supported",
        office_ballot_measure: "GOVERNOR",
        election_date: "11/7/2023",
        amount: "$10,000.00",
      })
    ).toEqual({
      toWhomMade: "Media Vendor",
      spenderName: "Kentucky Future Project Action Fund",
      date: "10/15/2023",
      candidateName: "Andy Beshear",
      supportOppose: "support",
      officeOrBallotMeasure: "GOVERNOR",
      electionDate: "11/7/2023",
      electionYear: 2023,
      amount: 10000,
    });
  });

  it("downloads candidate contribution exports", async () => {
    const csv =
      '"Recipient First Name","Recipient Last Name","Amount","Contribution Type","Receipt Date"\n' +
      '"Andy","Beshear","-25.00","INDIVIDUAL","11/18/2023"\n';
    const fetchImpl = vi.fn().mockResolvedValue(responseWithText(csv)) as unknown as typeof fetch;

    await expect(
      downloadKentuckyKrefCandidateContributions(
        { candidateFirstName: "Andy", candidateLastName: "Beshear" },
        { fetchImpl, timeoutMs: 1000 }
      )
    ).resolves.toEqual([
      {
        candidateName: "Andy Beshear",
        candidateFirstName: "Andy",
        candidateLastName: "Beshear",
        recipientName: "Andy Beshear",
        contributorType: "INDIVIDUAL",
        amount: 25,
        receiptDate: "11/18/2023",
      },
    ]);
    const url = new URL(String(vi.mocked(fetchImpl).mock.calls[0]?.[0]));
    expect(url.origin + url.pathname).toBe("https://secure.kentucky.gov/kref/publicsearch/ExportContributors");
    expect(url.searchParams.get("ContributionSearchType")).toBe("Candidate");
    expect(url.searchParams.get("CandidateFirstName")).toBe("Andy");
    expect(url.searchParams.get("CandidateLastName")).toBe("Beshear");
  });

  it("downloads IE-only committee contribution exports through the official organization type", async () => {
    const fetchImpl = vi
      .fn()
      .mockResolvedValue(responseWithText('"To Organization","Amount"\n"Kentucky Future Project Action Fund","245.00"\n')) as unknown as typeof fetch;

    await expect(downloadKentuckyKrefIeOnlyCommitteeContributions({}, { fetchImpl, timeoutMs: 1000 })).resolves.toEqual([
      {
        toOrganizationName: "Kentucky Future Project Action Fund",
        recipientName: "Kentucky Future Project Action Fund",
        amount: 245,
      },
    ]);
    const url = new URL(String(vi.mocked(fetchImpl).mock.calls[0]?.[0]));
    expect(url.searchParams.get("ContributionSearchType")).toBe("Organization");
    expect(url.searchParams.get("OrganizationType")).toBe(KENTUCKY_KREF_IE_ONLY_ORGANIZATION_TYPE);
  });

  it("downloads independent expenditure exports", async () => {
    const fetchImpl = vi
      .fn()
      .mockResolvedValue(
        responseWithText(
          '"To Whome Made","Name","Candidate Name","Support / Oppose","Election Date","Amount"\n' +
            '"Media Vendor","Kentucky Future Project Action Fund","Andy Beshear","Opposed","11/7/2023","$1,000.00"\n'
        )
      ) as unknown as typeof fetch;

    await expect(
      downloadKentuckyKrefIndependentExpenditures(
        { candidateFirstName: "Andy", candidateLastName: "Beshear" },
        { fetchImpl, timeoutMs: 1000 }
      )
    ).resolves.toEqual([
      {
        toWhomMade: "Media Vendor",
        spenderName: "Kentucky Future Project Action Fund",
        candidateName: "Andy Beshear",
        supportOppose: "oppose",
        electionDate: "11/7/2023",
        electionYear: 2023,
        amount: 1000,
      },
    ]);
    const url = new URL(String(vi.mocked(fetchImpl).mock.calls[0]?.[0]));
    expect(url.origin + url.pathname).toBe(
      "https://secure.kentucky.gov/kref/publicsearch/IndependentExpenditureSearch/ExportIndependentExpenditures"
    );
    expect(url.searchParams.get("CandidateFirstName")).toBe("Andy");
    expect(url.searchParams.get("CandidateLastName")).toBe("Beshear");
  });

  it("fetches candidate and independent-expenditure election dropdown options", async () => {
    const html =
      '<select name="ElectionDate"><option value="">Select Election Date</option><option value="11/7/2023">11/7/2023</option></select>';
    const fetchImpl = vi
      .fn()
      .mockResolvedValueOnce(responseWithText(html))
      .mockResolvedValueOnce(responseWithText(html)) as unknown as typeof fetch;

    await expect(fetchKentuckyKrefCandidateElectionDateOptions({ fetchImpl, timeoutMs: 1000 })).resolves.toEqual([
      { value: "11/7/2023", label: "11/7/2023" },
    ]);
    await expect(
      fetchKentuckyKrefIndependentExpenditureElectionDateOptions({ fetchImpl, timeoutMs: 1000 })
    ).resolves.toEqual([{ value: "11/7/2023", label: "11/7/2023" }]);

    const candidateUrl = new URL(String(vi.mocked(fetchImpl).mock.calls[0]?.[0]));
    const independentExpenditureUrl = new URL(String(vi.mocked(fetchImpl).mock.calls[1]?.[0]));
    expect(candidateUrl.origin + candidateUrl.pathname).toBe(
      "https://secure.kentucky.gov/kref/publicsearch/ToCandidateSearch"
    );
    expect(independentExpenditureUrl.origin + independentExpenditureUrl.pathname).toBe(
      "https://secure.kentucky.gov/kref/publicsearch/IndependentExpenditureSearch"
    );
    expect(vi.mocked(fetchImpl).mock.calls[0]?.[1]).toMatchObject({
      headers: { accept: "text/html,text/plain;q=0.9,*/*;q=0.1" },
    });
  });

  it("raises typed errors for failed KREF export requests", async () => {
    const fetchImpl = vi.fn().mockResolvedValue(responseWithText("nope", { status: 503, statusText: "Unavailable" })) as unknown as typeof fetch;

    await expect(
      downloadKentuckyKrefCandidateContributions({ candidateLastName: "Beshear" }, { fetchImpl, timeoutMs: 1000 })
    ).rejects.toMatchObject({
      code: "http_error",
      status: 503,
    });
  });
});
