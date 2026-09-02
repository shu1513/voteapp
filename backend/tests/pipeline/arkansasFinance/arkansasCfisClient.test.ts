import { describe, expect, it, vi } from "vitest";

import {
  ARKANSAS_CFIS_API_BASE_URL,
  ArkansasCfisClientError,
  getAllArkansasFilerRegistrations,
  getAllArkansasTransactions,
  getArkansasFilerRegistrationPage,
  getArkansasNextElectionYear,
  getArkansasTransactionPage,
} from "../../../src/pipeline/arkansasFinance/arkansasCfisClient.js";

const GUID = "689c554c-5120-46a4-828e-6798f3298f22";

function jsonResponse(payload: unknown): Response {
  return new Response(JSON.stringify(payload), {
    status: 200,
    headers: { "Content-Type": "application/json" },
  });
}

function registrationItem(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    guid: GUID,
    filerEntityID: 1004,
    filerEntityVersionID: 1,
    filerType: "Candidate",
    filerTypeCode: "CAN",
    filerStatus: "Active",
    filerName: "Sanders, Governor. Sarah H.",
    firstName: "Sarah",
    lastName: "Sanders",
    suffix: null,
    committeeName: null,
    office: "Governor",
    officeDistrictName: null,
    jurisdictionName: "Arkansas",
    politicalParty: "Republican Party",
    electionYear: "2026",
    filingYear: "2026",
    isPaperFiler: false,
    totalRaised: 7870507.53,
    totalSpent: 5717191.87,
    balanceofFunds: 2153315.66,
    totalRows: 1,
    ...overrides,
  };
}

describe("Arkansas CFIS client", () => {
  it("parses a filer-registration page, including string years", async () => {
    const fetchImpl = vi.fn(async () =>
      jsonResponse({
        succeeded: true,
        error: null,
        data: { items: [registrationItem()], totalItems: 1 },
      })
    ) as unknown as typeof fetch;
    const page = await getArkansasFilerRegistrationPage(
      { pageNumber: 1, pageSize: 10, filerName: "Sanders" },
      { fetchImpl }
    );
    expect(page.totalItems).toBe(1);
    expect(page.items[0]).toMatchObject({
      registrationGuid: GUID,
      filerEntityId: 1004,
      electionYear: 2026,
      filingYear: 2026,
      totalRaised: 7870507.53,
    });
    const [url, init] = vi.mocked(fetchImpl).mock.calls[0]!;
    expect(String(url)).toBe(
      `${ARKANSAS_CFIS_API_BASE_URL}/PublicFilerDetails/GetCandidateCommitteDetails`
    );
    expect(JSON.parse(String((init as RequestInit).body))).toEqual({
      pageNumber: 1,
      pageSize: 10,
      filerName: "Sanders",
    });
  });

  it("treats an empty electionYear as null", async () => {
    const fetchImpl = vi.fn(async () =>
      jsonResponse({
        succeeded: true,
        error: null,
        data: {
          items: [registrationItem({ electionYear: "", filerType: "SFI Filer", filerTypeCode: "SFIFILER" })],
          totalItems: 1,
        },
      })
    ) as unknown as typeof fetch;
    const page = await getArkansasFilerRegistrationPage({}, { fetchImpl });
    expect(page.items[0]!.electionYear).toBeNull();
  });

  it("rejects an unsuccessful envelope", async () => {
    const fetchImpl = vi.fn(async () =>
      jsonResponse({ succeeded: false, error: { message: "nope" }, data: null })
    ) as unknown as typeof fetch;
    await expect(getArkansasNextElectionYear({ fetchImpl })).rejects.toThrow(/failed: nope/);
  });

  it("appends the DNS-defect hint on ENOTFOUND", async () => {
    const fetchImpl = vi.fn(async () => {
      throw new Error("getaddrinfo ENOTFOUND api-ethics-disclosures.sos.arkansas.gov");
    }) as unknown as typeof fetch;
    await expect(getArkansasNextElectionYear({ fetchImpl })).rejects.toThrow(/DNS resolvers/);
  });

  it("requires a valid registration guid for transaction search", async () => {
    const fetchImpl = vi.fn() as unknown as typeof fetch;
    await expect(
      getArkansasTransactionPage(
        { filerRegistrationGuid: "not-a-guid", transactionTypeCode: "TCON" },
        { fetchImpl }
      )
    ).rejects.toThrow(ArkansasCfisClientError);
    expect(vi.mocked(fetchImpl)).not.toHaveBeenCalled();
  });

  const transactionItem = (guid: string, transactionDate: string): Record<string, unknown> => ({
    guid,
    filerName: "Sanders, Sarah",
    filerRegistrationGuid: GUID,
    transactionAmount: 10,
    transactionDate,
    sourceName: "Donor",
    employerName: null,
    occupation: null,
    transactionSource: "Individual",
    reportName: "2026 July Monthly Report",
    transactionSubTypeDesc: "Itemized Monetary",
    transactionCategory: null,
    hasChild: false,
  });
  const page = (items: Record<string, unknown>[], totalItems: number): Response =>
    jsonResponse({ succeeded: true, error: null, data: { items, totalItems } });
  const bodyAt = (fetchImpl: typeof fetch, index: number): Record<string, unknown> =>
    JSON.parse(String((vi.mocked(fetchImpl).mock.calls[index]![1] as RequestInit).body)) as Record<string, unknown>;

  it("rejects duplicate guids on a single-page transaction pull", async () => {
    const dup = "aaaaaaaa-0000-4000-8000-000000000001";
    const fetchImpl = vi.fn(async () =>
      page([transactionItem(dup, "01/02/2026"), transactionItem(dup, "01/03/2026")], 2)
    ) as unknown as typeof fetch;
    await expect(
      getAllArkansasTransactions({ filerRegistrationGuid: GUID, transactionTypeCode: "TCON", pageSize: 10 }, { fetchImpl })
    ).rejects.toThrow(/duplicate guids/);
  });

  it("partitions an oversized pull by inclusive date windows until each fits one page", async () => {
    // 3 rows on 01/01, 01/02, 01/04 with pageSize 2: unfiltered total 3 > 2,
    // bounds via sorted single-row requests, then windows [01/01-01/02] (2 rows)
    // and [01/03-01/04] (1 row).
    const rows = [
      transactionItem("aaaaaaaa-0000-4000-8000-000000000001", "01/01/2026"),
      transactionItem("aaaaaaaa-0000-4000-8000-000000000002", "01/02/2026"),
      transactionItem("aaaaaaaa-0000-4000-8000-000000000004", "01/04/2026"),
    ];
    const inWindow = (from: string, to: string) =>
      rows.filter((row) => {
        const d = String(row.transactionDate);
        return d >= from && d <= to;
      });
    const fetchImpl = vi.fn(async (_url: unknown, init?: RequestInit) => {
      const body = JSON.parse(String(init?.body)) as Record<string, unknown>;
      if (body.sortBy === "TransactionDate") {
        return page([body.sortType === "asc" ? rows[0]! : rows[2]!], 3);
      }
      if (typeof body.fromDate === "string" && typeof body.toDate === "string") {
        const selected = inWindow(body.fromDate, body.toDate);
        return page(selected.slice(0, Number(body.pageSize)), selected.length);
      }
      return page(rows.slice(0, 2), 3);
    }) as unknown as typeof fetch;

    const result = await getAllArkansasTransactions(
      { filerRegistrationGuid: GUID, transactionTypeCode: "TCON", pageSize: 2 },
      { fetchImpl }
    );
    expect(result.map((row) => row.transactionDate)).toEqual(["01/01/2026", "01/02/2026", "01/04/2026"]);
    // unfiltered, asc bound, desc bound, full window (overflows), left, right
    expect(vi.mocked(fetchImpl)).toHaveBeenCalledTimes(6);
    expect(bodyAt(fetchImpl, 1)).toMatchObject({ sortBy: "TransactionDate", sortType: "asc", pageSize: 1 });
    expect(bodyAt(fetchImpl, 3)).toMatchObject({ fromDate: "01/01/2026", toDate: "01/04/2026" });
    expect(bodyAt(fetchImpl, 4)).toMatchObject({ fromDate: "01/01/2026", toDate: "01/02/2026" });
    expect(bodyAt(fetchImpl, 5)).toMatchObject({ fromDate: "01/03/2026", toDate: "01/04/2026" });
  });

  it("fails closed when a window split loses rows", async () => {
    const fetchImpl = vi.fn(async (_url: unknown, init?: RequestInit) => {
      const body = JSON.parse(String(init?.body)) as Record<string, unknown>;
      if (body.sortBy === "TransactionDate") {
        return page([transactionItem("aaaaaaaa-0000-4000-8000-000000000001", body.sortType === "asc" ? "01/01/2026" : "01/04/2026")], 3);
      }
      if (typeof body.fromDate === "string") {
        // Children report fewer rows than the parent window claimed.
        return body.fromDate === "01/01/2026" && body.toDate === "01/04/2026"
          ? page([transactionItem("aaaaaaaa-0000-4000-8000-000000000001", "01/01/2026")], 3)
          : page([], 0);
      }
      return page([transactionItem("aaaaaaaa-0000-4000-8000-000000000001", "01/01/2026")], 3);
    }) as unknown as typeof fetch;
    await expect(
      getAllArkansasTransactions({ filerRegistrationGuid: GUID, transactionTypeCode: "TCON", pageSize: 2 }, { fetchImpl })
    ).rejects.toThrow(/split returned 0 rows for totalItems 3/);
  });

  it("fails closed when totalItems changes during pagination", async () => {
    const fetchImpl = vi
      .fn()
      .mockResolvedValueOnce(
        jsonResponse({
          succeeded: true,
          error: null,
          data: { items: [registrationItem()], totalItems: 2 },
        })
      )
      .mockResolvedValueOnce(
        jsonResponse({
          succeeded: true,
          error: null,
          data: { items: [registrationItem()], totalItems: 3 },
        })
      ) as unknown as typeof fetch;
    await expect(getAllArkansasFilerRegistrations({ pageSize: 1 }, { fetchImpl })).rejects.toThrow(
      /totalItems changed/
    );
  });
});
