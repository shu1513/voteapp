import { describe, expect, it, vi } from "vitest";

import {
  ARKANSAS_CFIS_API_BASE_URL,
  ArkansasCfisClientError,
  getAllArkansasFilerRegistrations,
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
