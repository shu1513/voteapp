import { describe, expect, it, vi } from "vitest";

import {
  AUSTIN_SOCRATA_CONTRIBUTIONS_DATASET,
  AustinSocrataClientError,
  austinCommitteePurposeRowFromRecord,
  austinContributionRowFromRecord,
  austinDirectCampaignExpenditureRowFromRecord,
  austinFormTypeCode,
  austinReportDetailRowFromRecord,
  austinReportIdFromTransactionId,
  buildAustinSocrataDatasetUrl,
  getAustinContributionRowsByRecipient,
  getAustinReportDetailRowCounts,
  parseAustinMoneyCents,
  selectAustinEffectiveReports,
  soqlString,
  type AustinReportDetailRow,
} from "../../../src/pipeline/austinFinance/austinSocrataClient.js";

function jsonResponse(payload: unknown, init: ResponseInit = {}): Response {
  return new Response(JSON.stringify(payload), { status: 200, statusText: "OK", ...init });
}

/** Raw Report Detail row as the live dataset returns it (2026-08-18 probe). */
function rawReportDetail(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    filer_name: "Watson, Kirk P.",
    form_type: "COH - Candidate /Officeholder Campaign Finance Report",
    report_type: "30th day before election",
    link_to_report: { url: "https://services.austintexas.gov/edims/document.cfm?id=439000", description: "View Report" },
    date_filed: "2024-10-07T00:00:00.000",
    filer_address_2: "PO Box 1", // PII-ish: must not survive mapping
    filer_city: "Austin",
    report_id: "R20240701100718647",
    treasurer_name: "Someone",
    date_due: "2026-07-15T00:00:00.000",
    period_from: "2024-07-01T00:00:00.000",
    period_to: "2024-09-26T00:00:00.000",
    election_date: "2024-11-05T00:00:00.000",
    election_type: "GENERAL",
    office_held: "MAYOR",
    office_sought: "MAYOR",
    contrib_total: "216483.00",
    expend_total: "488657.64",
    contrib_balance: "266891.61",
    outstand_loan: "0",
    ...overrides,
  };
}

/** Raw Contributions row (live shape) — includes the address fields the mapping must drop. */
function rawContribution(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    donor: "Barnes, Kelly",
    recipient: "Watson, Kirk P.",
    contribution_amount: "450.00",
    contribution_date: "2024-01-19T00:00:00.000",
    donor_type: "INDIVIDUAL",
    donor_address: "123 Main St",
    city_state_zip: "Austin, TX, 78701",
    contribution_year: "2024",
    donor_reported_occupation: "Government Affairs",
    donor_reported_employer: "HillCo Partners",
    contribution_type: "Monetary Political Contribution",
    date_reported: "2024-07-15T00:00:00.000",
    report_filed: "COH: Candidate /Officeholder Campaign Finance Report",
    view_report: { url: "https://services.austintexas.gov/edims/document.cfm?id=432411", description: "View Report" },
    transaction_id: "R20240101100718324-A00387",
    ...overrides,
  };
}

function report(overrides: Partial<AustinReportDetailRow>): AustinReportDetailRow {
  return {
    reportId: "R1",
    filerName: "Watson, Kirk P.",
    formTypeCode: "COH",
    formType: "COH - Candidate /Officeholder Campaign Finance Report",
    reportType: null,
    dateFiled: "2024-07-15",
    periodFrom: "2024-01-01",
    periodTo: "2024-06-30",
    electionDate: "2024-11-05",
    electionType: "GENERAL",
    officeSought: "MAYOR",
    officeHeld: null,
    contribTotalCents: 0,
    expendTotalCents: 0,
    contribBalanceCents: 0,
    outstandingLoanCents: null,
    reportUrl: null,
    ...overrides,
  };
}

describe("parseAustinMoneyCents", () => {
  it("parses decimal strings exactly", () => {
    expect(parseAustinMoneyCents("710580.84", "x")).toBe(71_058_084);
    expect(parseAustinMoneyCents("0", "x")).toBe(0);
    expect(parseAustinMoneyCents("450", "x")).toBe(45_000);
    expect(parseAustinMoneyCents("23749.7", "x")).toBe(2_374_970);
    expect(parseAustinMoneyCents("-12.05", "x")).toBe(-1_205);
    // 1.15 * 100 = 114.99999 in floating point; the string parser must not care.
    expect(parseAustinMoneyCents("1.15", "x")).toBe(115);
  });

  it("returns null for absent values and rejects malformed ones", () => {
    expect(parseAustinMoneyCents(undefined, "x")).toBeNull();
    expect(parseAustinMoneyCents(null, "x")).toBeNull();
    expect(parseAustinMoneyCents("", "x")).toBeNull();
    expect(() => parseAustinMoneyCents("1.234", "x")).toThrow(AustinSocrataClientError);
    expect(() => parseAustinMoneyCents("$1,000.00", "x")).toThrow(AustinSocrataClientError);
    expect(() => parseAustinMoneyCents("abc", "x")).toThrow(AustinSocrataClientError);
  });
});

describe("row helpers", () => {
  it("derives report ids and form codes", () => {
    expect(austinReportIdFromTransactionId("R20240101100718990-A01448")).toBe("R20240101100718990");
    expect(austinReportIdFromTransactionId("R20241021100718843-F00001-DCE00004")).toBe("R20241021100718843");
    expect(() => austinReportIdFromTransactionId("A01448")).toThrow(AustinSocrataClientError);
    expect(austinFormTypeCode("CORCOH - Correction Affidavit For Candidate/Officeholder")).toBe("CORCOH");
    expect(austinFormTypeCode("COHFR - ")).toBe("COHFR");
    expect(() => austinFormTypeCode(" - nothing")).toThrow(AustinSocrataClientError);
  });

  it("escapes SoQL strings and validates dataset ids", () => {
    expect(soqlString("O'Brien")).toBe("'O''Brien'");
    expect(buildAustinSocrataDatasetUrl(AUSTIN_SOCRATA_CONTRIBUTIONS_DATASET, { $where: "a = 'b'", $limit: 5 })).toBe(
      "https://data.austintexas.gov/resource/3kfv-biw6.json?%24where=a+%3D+%27b%27&%24limit=5"
    );
    expect(() => buildAustinSocrataDatasetUrl("bad", {})).toThrow(AustinSocrataClientError);
  });
});

describe("row mappers", () => {
  it("maps a Report Detail row to cents and dates, dropping filer/treasurer PII", () => {
    const row = austinReportDetailRowFromRecord(rawReportDetail());
    expect(row).toEqual({
      reportId: "R20240701100718647",
      filerName: "Watson, Kirk P.",
      formTypeCode: "COH",
      formType: "COH - Candidate /Officeholder Campaign Finance Report",
      reportType: "30th day before election",
      dateFiled: "2024-10-07",
      periodFrom: "2024-07-01",
      periodTo: "2024-09-26",
      electionDate: "2024-11-05",
      electionType: "GENERAL",
      officeSought: "MAYOR",
      officeHeld: "MAYOR",
      contribTotalCents: 21_648_300,
      expendTotalCents: 48_865_764,
      contribBalanceCents: 26_689_161,
      outstandingLoanCents: 0,
      reportUrl: "https://services.austintexas.gov/edims/document.cfm?id=439000",
    });
    expect(Object.keys(row).some((key) => /address|treasurer|city|due/i.test(key))).toBe(false);
  });

  it("keeps absent cover totals as null and requires identity fields", () => {
    const row = austinReportDetailRowFromRecord(
      rawReportDetail({ contrib_total: undefined, expend_total: undefined, election_date: undefined, office_sought: undefined })
    );
    expect(row.contribTotalCents).toBeNull();
    expect(row.expendTotalCents).toBeNull();
    expect(row.electionDate).toBeNull();
    expect(row.officeSought).toBeNull();
    expect(() => austinReportDetailRowFromRecord(rawReportDetail({ report_id: undefined }))).toThrow(AustinSocrataClientError);
    expect(austinReportDetailRowFromRecord(rawReportDetail({ filer_name: undefined })).filerName).toBeNull();
    expect(() => austinReportDetailRowFromRecord(rawReportDetail({ date_filed: undefined }))).toThrow(AustinSocrataClientError);
    expect(() => austinReportDetailRowFromRecord(rawReportDetail({ contrib_total: "1.234" }))).toThrow(AustinSocrataClientError);
  });

  it("maps a Contributions row, deriving the report id and dropping address fields", () => {
    const row = austinContributionRowFromRecord(rawContribution());
    expect(row).toEqual({
      transactionId: "R20240101100718324-A00387",
      reportId: "R20240101100718324",
      recipient: "Watson, Kirk P.",
      donor: "Barnes, Kelly",
      donorType: "INDIVIDUAL",
      contributionType: "Monetary Political Contribution",
      amountCents: 45_000,
      contributionDate: "2024-01-19",
      occupation: "Government Affairs",
      employer: "HillCo Partners",
      reportFiled: "COH: Candidate /Officeholder Campaign Finance Report",
      correction: false,
      reportUrl: "https://services.austintexas.gov/edims/document.cfm?id=432411",
    });
    expect(Object.keys(row).some((key) => /address|zip|city/i.test(key))).toBe(false);
    expect(austinContributionRowFromRecord(rawContribution({ correction: "X" })).correction).toBe(true);
    expect(austinContributionRowFromRecord(rawContribution({ donor_reported_occupation: "  " })).occupation).toBeNull();
    expect(() => austinContributionRowFromRecord(rawContribution({ contribution_amount: undefined }))).toThrow(
      AustinSocrataClientError
    );
  });

  it("maps Direct Campaign Expenditure and Committee Purpose rows", () => {
    const dce = austinDirectCampaignExpenditureRowFromRecord({
      payee: "CounterPoint Messaging, LLC",
      paid_by: "The Real Estate Council of Austin, Inc. Advancing Democracy PAC",
      payment_date: "2024-10-21T00:00:00.000",
      payment_amount: "71000.00",
      candidate_or_measure: "Watson, Kirk",
      office_sought_info: "MAYOR",
      office_held_info: "MAYOR",
      view_report: { url: "https://services.austintexas.gov/edims/document.cfm?id=439637" },
      parent_transaction: "R20241021100718843-F00001",
      dce_id: "R20241021100718843-F00001-DCE00005",
      geom: { type: "Point", coordinates: [-97.7, 30.2] },
      payee_address: "1 Some St",
    });
    expect(dce).toEqual({
      dceId: "R20241021100718843-F00001-DCE00005",
      parentTransaction: "R20241021100718843-F00001",
      reportId: "R20241021100718843",
      paidBy: "The Real Estate Council of Austin, Inc. Advancing Democracy PAC",
      payee: "CounterPoint Messaging, LLC",
      paymentDate: "2024-10-21",
      amountCents: 7_100_000,
      candidateOrMeasure: "Watson, Kirk",
      officeSoughtInfo: "MAYOR",
      officeHeldInfo: "MAYOR",
      correction: false,
      reportUrl: "https://services.austintexas.gov/edims/document.cfm?id=439637",
    });
    const purpose = austinCommitteePurposeRowFromRecord({
      report: "R20240701100718654",
      purpose_id: "C00001",
      committee_purp_id: "R20240701100718654-C00001",
      filer_name: "Austin Leadership PAC",
      link_to_report: { url: "https://services.austintexas.gov/edims/document.cfm?id=1" },
      committee_activity: "SUPPORT",
      purpose_type: "CANDIDATE",
      recipient: "Kirk,Watson",
      office_sought: "MAYOR",
      election_date: "2024-11-05T00:00:00.000",
    });
    expect(purpose).toEqual({
      committeePurposeId: "R20240701100718654-C00001",
      reportId: "R20240701100718654",
      filerName: "Austin Leadership PAC",
      committeeActivity: "SUPPORT",
      purposeType: "CANDIDATE",
      recipient: "Kirk,Watson",
      officeSought: "MAYOR",
      officeHeld: null,
      electionDate: "2024-11-05",
      measureDescription: null,
      correction: false,
      reportUrl: "https://services.austintexas.gov/edims/document.cfm?id=1",
    });
  });
});

describe("fetch helpers", () => {
  it("pages contributions with a stable order and stops on a short page", async () => {
    const calls: string[] = [];
    const page = (count: number, start: number) =>
      Array.from({ length: count }, (_, index) => rawContribution({ transaction_id: `R20240101100718324-A${String(start + index).padStart(5, "0")}` }));
    const fetchImpl = vi.fn(async (input: string | URL | Request) => {
      const url = String(input);
      calls.push(url);
      const offset = Number(new URL(url).searchParams.get("$offset"));
      return jsonResponse(offset === 0 ? page(2, 1) : page(1, 3));
    });
    const rows = await getAustinContributionRowsByRecipient("Watson, Kirk P.", { fetchImpl: fetchImpl as typeof fetch, pageLimit: 2 });
    expect(rows.map((row) => row.transactionId)).toEqual([
      "R20240101100718324-A00001",
      "R20240101100718324-A00002",
      "R20240101100718324-A00003",
    ]);
    expect(calls).toHaveLength(2);
    const first = new URL(calls[0]!);
    expect(first.searchParams.get("$where")).toBe("recipient = 'Watson, Kirk P.'");
    expect(first.searchParams.get("$order")).toBe("transaction_id");
    expect(first.searchParams.get("$limit")).toBe("2");
    expect(new URL(calls[1]!).searchParams.get("$offset")).toBe("2");
  });

  it("fails closed on http errors and on non-array payloads", async () => {
    await expect(
      getAustinContributionRowsByRecipient("Watson, Kirk P.", {
        fetchImpl: (async () => jsonResponse({}, { status: 429, statusText: "Too Many Requests" })) as typeof fetch,
      })
    ).rejects.toMatchObject({ code: "http_error", status: 429 });
    await expect(
      getAustinContributionRowsByRecipient("Watson, Kirk P.", { fetchImpl: (async () => jsonResponse({ rows: [] })) as typeof fetch })
    ).rejects.toMatchObject({ code: "bad_response" });
    await expect(getAustinContributionRowsByRecipient("  ", {})).rejects.toMatchObject({ code: "invalid_request" });
  });

  it("reads the Report Detail count query", async () => {
    const fetchImpl = vi.fn(async () => jsonResponse([{ total_rows: "1086", distinct_report_ids: "786" }]));
    await expect(getAustinReportDetailRowCounts({ fetchImpl: fetchImpl as typeof fetch })).resolves.toEqual({
      totalRows: 1086,
      distinctReportIds: 786,
    });
    expect(new URL(String(fetchImpl.mock.calls[0]![0])).searchParams.get("$select")).toBe(
      "count(*) as total_rows, count(distinct report_id) as distinct_report_ids"
    );
  });
});

describe("selectAustinEffectiveReports", () => {
  it("dedupes by report id, lets a later correction supersede, drops covered specials, ignores PAC rows", () => {
    const rows: AustinReportDetailRow[] = [
      report({ reportId: "R20240101100718324", periodFrom: "2024-01-01", periodTo: "2024-06-30", dateFiled: "2024-07-15" }),
      report({ reportId: "R20240101100718324", periodFrom: "2024-01-01", periodTo: "2024-06-30", dateFiled: "2024-07-15" }), // exact duplicate row
      report({
        reportId: "R20240101100718990",
        formTypeCode: "CORCOH",
        periodFrom: "2024-01-01",
        periodTo: "2024-06-30",
        dateFiled: "2024-12-02",
      }),
      report({ reportId: "R20241027100719293", periodFrom: "2024-10-27", periodTo: "2024-12-31", dateFiled: "2025-01-15" }),
      report({
        reportId: "R20241027100718930",
        formTypeCode: "COHATX7",
        periodFrom: "2024-10-27",
        periodTo: "2024-10-30",
        dateFiled: "2024-10-31",
      }),
      report({
        reportId: "R20250101100720000",
        formTypeCode: "COHATX7",
        periodFrom: "2025-01-01",
        periodTo: "2025-01-04",
        dateFiled: "2025-01-05",
      }),
      report({ reportId: "R20250101100720001", formTypeCode: "COHFR", periodFrom: "2025-01-01", periodTo: "2025-03-01", dateFiled: "2025-03-01" }),
      report({ reportId: "R20240701100718654", formTypeCode: "GPAC", filerName: "Austin Leadership PAC" }),
      report({ reportId: "R20240701100718655", periodFrom: null, periodTo: null }),
    ];
    const selection = selectAustinEffectiveReports(rows);
    expect(selection.duplicateRowCount).toBe(1);
    expect(selection.effective.map((row) => row.reportId)).toEqual([
      "R20240101100718990",
      "R20241027100719293",
      "R20250101100720001",
    ]);
    expect(selection.superseded.map((row) => row.reportId)).toEqual(["R20240101100718324"]);
    expect(selection.droppedSpecial.map((row) => row.reportId)).toEqual(["R20241027100718930", "R20250101100720000"]);
    expect(selection.keptSpecial).toEqual([]);
    expect(selection.ignored.map((row) => row.reportId)).toEqual(["R20240701100718654", "R20240701100718655"]);
  });

  it("keeps a special report whose period no regular report covers, and breaks same-day ties toward the correction", () => {
    const rows: AustinReportDetailRow[] = [
      report({ reportId: "R2", formTypeCode: "CORCOH", dateFiled: "2024-07-15" }),
      report({ reportId: "R1", formTypeCode: "COH", dateFiled: "2024-07-15" }),
      report({ reportId: "R3", formTypeCode: "COHATX7", periodFrom: "2024-10-27", periodTo: "2024-10-30", dateFiled: "2024-10-31" }),
    ];
    const selection = selectAustinEffectiveReports(rows);
    expect(selection.effective.map((row) => row.reportId)).toEqual(["R2"]);
    expect(selection.superseded.map((row) => row.reportId)).toEqual(["R1"]);
    expect(selection.keptSpecial.map((row) => row.reportId)).toEqual(["R3"]);
  });
});
