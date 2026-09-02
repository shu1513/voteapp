import { readFile } from "node:fs/promises";

import { describe, expect, it, vi } from "vitest";

import {
  downloadIdahoCfsBulkCsv,
  getAllIdahoContributions,
  getIdahoCandidateRegistrationPage,
  getIdahoContributionPage,
  getIdahoIndependentExpenditurePage,
  IdahoCfsClientError,
  idahoRegistrationSearchName,
} from "../../../src/pipeline/idahoFinance/idahoCfsClient.js";
import {
  assertIdahoCsvQuarantineTolerance,
  decodeIdahoCfsCsv,
  idahoCsvElectionStage,
  idahoCsvElectionYear,
  normalizeIdahoCsvZipCode,
  parseIdahoCurrencyCents,
  parseIdahoExpenditureCsv,
  parseIdahoReceiptCsv,
} from "../../../src/pipeline/idahoFinance/idahoCfsCsv.js";
import {
  countIdahoBulkRowsOutsideSearch,
  reconcileIdahoRegistration,
  selectIdahoRegistrationContributions,
  summarizeIdahoIndependentExpenditures,
} from "../../../src/pipeline/idahoFinance/idahoPhaseZero.js";

const fixtures = new URL("../../fixtures/idahoFinance/", import.meta.url);

async function fixture(name: string): Promise<string> {
  return readFile(new URL(name, fixtures), "utf8");
}

function response(body: string | Uint8Array, contentType: string): Response {
  return new Response(body, { status: 200, headers: { "Content-Type": contentType } });
}

function bodyOf(fetchImpl: typeof fetch): Record<string, unknown> {
  return JSON.parse(String(vi.mocked(fetchImpl).mock.calls[0]?.[1]?.body)) as Record<string, unknown>;
}

const REGISTRATION_2026 = "11111111-1111-4111-8111-111111111126";
const REGISTRATION_2024 = "11111111-1111-4111-8111-111111111124";

describe("Idaho CFS Phase 0", () => {
  it("parses the receipt export: split records, quoted fields, quarantine, swapped election columns", async () => {
    const parsed = parseIdahoReceiptCsv(await fixture("receipts-sanitized.csv"));

    expect(parsed.rows).toHaveLength(5);
    // Line 6: comma-corrupted apostrophe (29 cells). Line 9: 28 cells but the
    // content is shifted four columns left, so "Primary" sits in Transaction Amount.
    expect(parsed.quarantined).toEqual([
      { lineNumber: 6, columnCount: 29, reason: "column_count" },
      { lineNumber: 9, columnCount: 28, reason: "amount" },
    ]);
    expect(parsed.rows.map((row) => row["Transaction Id"])).toEqual(["313559", "313560", "313561", "313570", "400001"]);
    // Record split by a raw newline inside an unquoted address is re-joined.
    expect(parsed.rows[1]?.["Contributor Address Line 1"]).toBe("13456 N Smith Rd\n");
    expect(parsed.rows[1]?.["Contributor Address City"]).toBe("Pocatello");
    expect(parsed.rows[1]?.["Transaction Amount"]).toBe("$250.00");
    expect(parsed.rows[2]?.["Transaction Description"]).toBe('Fundraiser "Gala" dinner, table');
    expect(parsed.rows[0]?.["Transaction Amount"]).toBe("$1,000.00");
    expect(parseIdahoCurrencyCents(parsed.rows[0]!["Transaction Amount"])).toBe(100_000);
    expect(normalizeIdahoCsvZipCode(parsed.rows[0]!["Contributor Address Zip Code"])).toBe("83702");
    expect(idahoCsvElectionYear(parsed.rows[0]!)).toBe(2026);
    expect(idahoCsvElectionStage(parsed.rows[0]!)).toBe("Primary");
    expect(idahoCsvElectionStage(parsed.rows[2]!)).toBe("General");

    expect(() => parseIdahoReceiptCsv("wrong,header\n1,2\n")).toThrow("header changed");
    expect(() => parseIdahoReceiptCsv("")).toThrow("is empty");
    expect(() => parseIdahoCurrencyCents("$12.345")).toThrow("Invalid Idaho CFS currency");
    expect(() => parseIdahoCurrencyCents("2026")).toThrow("Invalid Idaho CFS currency");
    expect(parseIdahoCurrencyCents(" $0.05 ")).toBe(5);
    expect(idahoCsvElectionYear({ "Election Type": "", "Election Year": "" })).toBeNull();
    expect(idahoCsvElectionStage({ "Election Type": "", "Election Year": "" })).toBeNull();
  });

  it("parses the expenditure export with its trailing-space header and IE allocation rows", async () => {
    const parsed = parseIdahoExpenditureCsv(await fixture("expenditures-sanitized.csv"));

    expect(parsed.quarantined).toEqual([]);
    expect(parsed.rows).toHaveLength(4);
    expect(parsed.rows[0]?.["Filing Entity Name "]).toBe("Achilles, Todd Baker");
    expect(parsed.rows[1]?.["Transaction Sub Type"]).toBe("Independent Expenditure");
    expect(parsed.rows[1]?.["Purpose"]).toBe("L - Literature, Brochures, Printing");
    // Parent row carries the transaction; allocation rows carry the target.
    expect(parsed.rows[1]?.["Amount Applied"]).toBe("");
    expect(parsed.rows[2]).toMatchObject({
      "Transaction Id": "398000",
      "Candidate Supported/Opposed": "Candidate, Sample",
      Stance: "Support",
      "Amount Applied": "$77.41",
    });
    expect(parsed.rows[3]).toMatchObject({ "Transaction Sub Type": "Electioneering Communication", Stance: "N/A" });
  });

  it("decodes windows-1252 bytes and enforces the quarantine tolerance separately", () => {
    const bytes = new Uint8Array([0xef, 0xbb, 0xbf, 0x47, 0xa0, 0x2d, 0xa0, 0x47, 0xe9]);
    expect(decodeIdahoCfsCsv(bytes)).toBe("G\u00a0-\u00a0G\u00e9");

    const quarantined = [{ lineNumber: 5, columnCount: 29, reason: "column_count" as const }];
    const clean = { rows: new Array<unknown>(200).fill({}), quarantined };
    expect(() => assertIdahoCsvQuarantineTolerance(clean, "receipt")).not.toThrow();
    const broken = { rows: new Array<unknown>(50).fill({}), quarantined };
    expect(() => assertIdahoCsvQuarantineTolerance(broken, "receipt")).toThrow("export shape has changed");
  });

  it("posts the candidate-grid, contribution-search, IE, and bulk request contracts", async () => {
    const registrationsJson = await fixture("candidate-registrations-sanitized.json");
    const contributionsJson = await fixture("contributions-sanitized.json");
    const ieJson = await fixture("independent-expenditures-sanitized.json");

    const gridFetch = vi.fn().mockResolvedValue(response(registrationsJson, "application/json; charset=utf-8")) as unknown as typeof fetch;
    const grid = await getIdahoCandidateRegistrationPage({ pageNumber: 1, pageSize: 5000 }, { fetchImpl: gridFetch });
    expect(grid.totalItems).toBe(3);
    expect(grid.items[1]).toEqual({
      registrationGuid: REGISTRATION_2026,
      entityGuid: "c5bce9c1-7e0a-481a-ad35-0dd807bba517",
      filerEntityId: 257,
      filerRegistrationId: 1698,
      filerName: "Achilles, Todd Baker",
      firstName: "Todd",
      middleName: "Baker",
      lastName: "Achilles",
      committeeName: "Todd Achilles for Idaho",
      office: "State Representative",
      districtType: "Legislative",
      district: "Legislative District 16",
      jurisdiction: "Idaho State",
      party: "Democratic Party",
      partyCode: "DEM",
      electionYear: 2026,
      filingCycleId: 6,
      status: "Active",
      statusCode: "ACTV",
      totalRaised: 1500,
      totalSpent: 50,
      balanceOfFunds: 1450,
      isLegacyRecord: false,
    });
    expect(grid.items[2]?.committeeName).toBeNull();
    expect(String(vi.mocked(gridFetch).mock.calls[0]?.[0])).toBe(
      "https://api-sunshine.voteidaho.gov/api/PublicFilerDetails/GetCandidateDetails"
    );
    expect(bodyOf(gridFetch)).toEqual({ pageNumber: 1, pageSize: 5000, sortBy: null, sortType: null });
    expect(idahoRegistrationSearchName(grid.items[1]!)).toBe("Todd Baker Achilles");
    expect(idahoRegistrationSearchName(grid.items[0]!)).toBe("Todd Achilles");

    const contributionFetch = vi.fn().mockResolvedValue(response(contributionsJson, "application/json")) as unknown as typeof fetch;
    const page = await getIdahoContributionPage(
      { filerName: " Todd Baker Achilles ", pageNumber: 1, pageSize: 500 },
      { fetchImpl: contributionFetch }
    );
    expect(page.items).toHaveLength(5);
    expect(page.items[3]).toEqual({
      guid: "a1a1a1a1-0000-4000-8000-000000313562",
      transactionId: 313562,
      transactionVersionId: 2,
      filerReportId: 19943,
      filerReportVersionId: 2,
      filerReportGuid: "b2b2b2b2-0000-4000-8000-000000019944",
      filerRegistrationGuid: REGISTRATION_2026,
      filerEntityId: 257,
      filerName: "Achilles, Todd Baker",
      transactionAmount: 200,
      transactionDate: "03/20/2025",
      transactionTypeCode: "TCON",
      transactionSubTypeCode: "ITMY",
      sourceTypeCode: "TIND",
      sourceName: "Sample, Donor Amended",
      contributorCity: "Portland",
      contributorState: "OR",
      stateType: "OTST",
      electionYear: 2026,
      electionTypeCode: "PRMELEC",
      reportName: "2025 Annual Report",
      timedReport: null,
      filedDate: "2026-02-01T10:00:00",
    });
    expect(bodyOf(contributionFetch)).toEqual({
      pageNumber: 1,
      pageSize: 500,
      sortBy: "TransactionDate",
      sortType: "desc",
      transactionTypeCode: "TCON",
      filerName: "Todd Baker Achilles",
      sourceName: null,
      transactionAmountMax: null,
      transactionAmountMin: null,
      sourceTypeCode: null,
      committeeType: null,
      transactionSubTypeCode: null,
      electionID: null,
      reportName: null,
      toDate: null,
      fromDate: null,
      electionType: null,
      electionYear: null,
      filerRegistrationGuid: null,
    });
    await expect(getIdahoContributionPage({ filerName: "  " }, { fetchImpl: contributionFetch })).rejects.toThrow(
      "requires filerName"
    );

    const ieFetch = vi.fn().mockResolvedValue(response(ieJson, "application/json")) as unknown as typeof fetch;
    const iePage = await getIdahoIndependentExpenditurePage({ pageNumber: 1, pageSize: 10_000 }, { fetchImpl: ieFetch });
    expect(iePage.items[1]).toMatchObject({
      candidateMeasure: "Candidate, Sample",
      stance: "Oppose",
      amountApplied: 1000,
      isNonRegisteredEntity: true,
      filerRegistrationGuid: null,
      transactionTypeCode: "TEXP",
    });
    expect(iePage.items[3]?.timedReport).toBeNull();
    expect(bodyOf(ieFetch)).toEqual({ pageNumber: 1, pageSize: 10_000, sortBy: null, sortType: null });

    const csvBytes = new Uint8Array([0x41, 0x2c, 0x42, 0x0a, 0x31, 0x2c, 0xa0, 0x0a]);
    const csvFetch = vi.fn().mockResolvedValue(response(csvBytes, "text/csv")) as unknown as typeof fetch;
    await expect(
      downloadIdahoCfsBulkCsv({ filingYear: 2026, transactionTypeCode: "TCON" }, { fetchImpl: csvFetch })
    ).resolves.toBe("A,B\n1,\u00a0\n");
    expect(bodyOf(csvFetch)).toEqual({ type: "CSV", filingYear: 2026, transactionTypeCode: "TCON" });
    const headers = vi.mocked(csvFetch).mock.calls[0]?.[1]?.headers as Record<string, string>;
    expect(headers["User-Agent"]).toBe("Mozilla/5.0");
    expect(headers.Origin).toBe("https://sunshine.voteidaho.gov");
    await expect(
      downloadIdahoCfsBulkCsv({ filingYear: 2019, transactionTypeCode: "TCON" }, { fetchImpl: csvFetch })
    ).rejects.toThrow("Invalid Idaho CFS filing year");
  });

  it("fails closed on edge HTML, failure envelopes, and inconsistent pagination", async () => {
    const htmlFetch = vi.fn().mockResolvedValue(response("<html>Access Denied</html>", "text/html")) as unknown as typeof fetch;
    await expect(getIdahoCandidateRegistrationPage({}, { fetchImpl: htmlFetch })).rejects.toBeInstanceOf(
      IdahoCfsClientError
    );

    const forbiddenFetch = vi.fn().mockResolvedValue(new Response("", { status: 403 })) as unknown as typeof fetch;
    await expect(getIdahoCandidateRegistrationPage({}, { fetchImpl: forbiddenFetch })).rejects.toMatchObject({
      code: "http_error",
      status: 403,
    });

    const failedFetch = vi.fn().mockResolvedValue(
      response(JSON.stringify({ data: null, succeeded: false, error: { message: "An exception occurred." } }), "application/json")
    ) as unknown as typeof fetch;
    await expect(getIdahoCandidateRegistrationPage({}, { fetchImpl: failedFetch })).rejects.toThrow(
      "An exception occurred."
    );

    const parsed = JSON.parse(await fixture("contributions-sanitized.json")) as { data: { items: unknown[] } };
    const pages = [parsed.data.items.slice(0, 3), parsed.data.items.slice(3)];
    const pagedFetch = vi.fn().mockImplementation(async (_url, init) => {
      const body = JSON.parse(String(init?.body)) as { pageNumber: number };
      return response(
        JSON.stringify({
          data: { items: pages[body.pageNumber - 1] ?? [], totalItems: 5, pageNumber: 0, pageSize: 0 },
          succeeded: true,
          error: null,
        }),
        "application/json"
      );
    }) as unknown as typeof fetch;
    await expect(
      getAllIdahoContributions({ filerName: "Todd Baker Achilles", pageSize: 3 }, { fetchImpl: pagedFetch })
    ).resolves.toHaveLength(5);
    expect(vi.mocked(pagedFetch).mock.calls.map((call) => JSON.parse(String(call[1]?.body)).pageNumber)).toEqual([1, 2]);

    const changingTotalFetch = vi
      .fn()
      .mockResolvedValueOnce(response(JSON.stringify({ data: { items: pages[0], totalItems: 5 }, succeeded: true }), "application/json"))
      .mockResolvedValueOnce(response(JSON.stringify({ data: { items: pages[1], totalItems: 6 }, succeeded: true }), "application/json")) as unknown as typeof fetch;
    await expect(
      getAllIdahoContributions({ filerName: "Todd Baker Achilles", pageSize: 3 }, { fetchImpl: changingTotalFetch })
    ).rejects.toThrow("totalItems changed during pagination");
  });

  it("reconciles grid totals against search rows and proves the bulk export is version-1 only", async () => {
    const registrations = (
      await getIdahoCandidateRegistrationPage(
        {},
        { fetchImpl: vi.fn().mockResolvedValue(response(await fixture("candidate-registrations-sanitized.json"), "application/json")) as unknown as typeof fetch }
      )
    ).items;
    const searchRows = (
      await getIdahoContributionPage(
        { filerName: "Todd Baker Achilles" },
        { fetchImpl: vi.fn().mockResolvedValue(response(await fixture("contributions-sanitized.json"), "application/json")) as unknown as typeof fetch }
      )
    ).items;
    const bulkRows = parseIdahoReceiptCsv(await fixture("receipts-sanitized.csv")).rows;

    expect(selectIdahoRegistrationContributions(searchRows, REGISTRATION_2026).map((row) => row.transactionId)).toEqual([
      313559, 313560, 313561, 313562,
    ]);

    const current = reconcileIdahoRegistration({
      registration: registrations[1]!,
      searchRows,
      bulkRows,
      bulkFilingYears: [2025, 2026],
    });
    expect(current).toEqual({
      registrationGuid: REGISTRATION_2026,
      filerEntityId: 257,
      electionYear: 2026,
      filerName: "Achilles, Todd Baker",
      gridTotalRaisedCents: 150_000,
      search: { rowCount: 4, amountCents: 150_000 },
      searchVersionOne: { rowCount: 3, amountCents: 130_000 },
      bulk: { rowCount: 3, amountCents: 130_000 },
      bulkFilingYears: [2025, 2026],
      deltaCents: 0,
      status: "match",
      bulkMatchesVersionOne: true,
    });

    // The 2024 registration's rows fall outside the downloaded filing years.
    const previous = reconcileIdahoRegistration({
      registration: registrations[0]!,
      searchRows,
      bulkRows,
      bulkFilingYears: [2025, 2026],
    });
    expect(previous).toMatchObject({
      registrationGuid: REGISTRATION_2024,
      gridTotalRaisedCents: 30_000,
      search: { rowCount: 1, amountCents: 30_000 },
      searchVersionOne: { rowCount: 0, amountCents: 0 },
      bulk: { rowCount: 0, amountCents: 0 },
      status: "match",
      bulkMatchesVersionOne: true,
    });

    const mismatch = reconcileIdahoRegistration({
      registration: { ...registrations[1]!, totalRaised: 1600 },
      searchRows,
      bulkRows,
      bulkFilingYears: [2025, 2026],
    });
    expect(mismatch).toMatchObject({ deltaCents: -10_000, status: "mismatch" });

    const bulkDrift = reconcileIdahoRegistration({
      registration: registrations[1]!,
      searchRows,
      bulkRows: bulkRows.filter((row) => row["Transaction Id"] !== "313561"),
      bulkFilingYears: [2025, 2026],
    });
    expect(bulkDrift).toMatchObject({ status: "match", bulkMatchesVersionOne: false });

    // Same row count and total, different transaction identity: must not pass.
    const bulkSwapped = reconcileIdahoRegistration({
      registration: registrations[1]!,
      searchRows,
      bulkRows: bulkRows.map((row) => (row["Transaction Id"] === "313561" ? { ...row, "Transaction Id": "313562" } : row)),
      bulkFilingYears: [2025, 2026],
    });
    expect(bulkSwapped).toMatchObject({
      bulk: { rowCount: 3, amountCents: 130_000 },
      searchVersionOne: { rowCount: 3, amountCents: 130_000 },
      bulkMatchesVersionOne: false,
    });

    // Bulk contribution rows the search never returned are counted, loans are not.
    expect(countIdahoBulkRowsOutsideSearch({ filerEntityId: 257, bulkRows, searchRows })).toBe(0);
    expect(
      countIdahoBulkRowsOutsideSearch({
        filerEntityId: 257,
        bulkRows: [...bulkRows, { ...bulkRows[0]!, "Transaction Id": "999999" }],
        searchRows,
      })
    ).toBe(1);
    expect(countIdahoBulkRowsOutsideSearch({ filerEntityId: 257, bulkRows, searchRows: [] })).toBe(3);

    expect(() =>
      reconcileIdahoRegistration({
        registration: registrations[1]!,
        searchRows: searchRows.map((row) => ({ ...row, filerEntityId: 999 })),
        bulkRows,
        bulkFilingYears: [2025, 2026],
      })
    ).toThrow("received rows for entity 999");
  });

  it("summarizes independent expenditures for one election year and rejects unknown stances", async () => {
    const rows = (
      await getIdahoIndependentExpenditurePage(
        {},
        { fetchImpl: vi.fn().mockResolvedValue(response(await fixture("independent-expenditures-sanitized.json"), "application/json")) as unknown as typeof fetch }
      )
    ).items;

    expect(summarizeIdahoIndependentExpenditures(rows, 2026)).toEqual({
      sourceRowCount: 5,
      rowCount: 4,
      support: { rowCount: 3, amountCents: 58_975 },
      oppose: { rowCount: 1, amountCents: 100_000 },
      candidateTargetRowCount: 3,
      measureTargetRowCount: 1,
      registrationResolvedRowCount: 2,
      nonRegisteredCandidateRowCount: 1,
      registeredFilerRowCount: 3,
      nonRegisteredFilerRowCount: 1,
    });
    expect(() => summarizeIdahoIndependentExpenditures([{ ...rows[0]!, stance: "N/A" }], 2026)).toThrow(
      "unknown stance"
    );
    expect(() => summarizeIdahoIndependentExpenditures([{ ...rows[0]!, transactionTypeCode: "TECOM" }], 2026)).toThrow(
      "unknown transaction type"
    );
  });
});
