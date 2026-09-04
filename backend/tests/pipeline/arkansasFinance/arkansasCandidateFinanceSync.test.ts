import { describe, expect, it, vi } from "vitest";

import type {
  ArkansasFilerRegistrationRow,
  ArkansasTransactionRow,
} from "../../../src/pipeline/arkansasFinance/arkansasCfisClient.js";
import {
  selectArkansasCandidateRegistration,
  syncArkansasCandidateFinance,
} from "../../../src/pipeline/arkansasFinance/arkansasCandidateFinanceSync.js";

const CANDIDATE_ID = "11111111-1111-4111-8111-111111111111";
const ELECTION_ID = "22222222-2222-4222-8222-222222222222";
const LINK_ID = "33333333-3333-4333-8333-333333333333";
const REGISTRATION_GUID = "7c482853-2ec8-4435-94ea-ae709b14e7ed";
const OTHER_GUID = "01e37c68-aa22-4559-a14d-f23a617a415b";

function registration(overrides: Partial<ArkansasFilerRegistrationRow> = {}): ArkansasFilerRegistrationRow {
  return {
    registrationGuid: REGISTRATION_GUID,
    filerEntityId: 7968,
    filerEntityVersionId: 1,
    filerType: "Candidate",
    filerTypeCode: "CAN",
    filerStatus: "Active",
    filerName: "Doe, Jane A.",
    firstName: "Jane",
    lastName: "Doe",
    suffix: null,
    committeeName: null,
    office: "State Representative",
    officeDistrictName: "59",
    jurisdictionName: "Arkansas",
    politicalParty: "Republican",
    electionYear: 2026,
    filingYear: 2026,
    isPaperFiler: false,
    totalRaised: 400,
    totalSpent: 120,
    balanceOfFunds: 280,
    ...overrides,
  };
}

function receipt(overrides: Partial<ArkansasTransactionRow> = {}): ArkansasTransactionRow {
  return {
    guid: "00000000-0000-4000-8000-000000000001",
    filerName: "Doe, Jane A.",
    filerRegistrationGuid: REGISTRATION_GUID,
    transactionAmount: 400,
    transactionDate: "03/01/2026",
    sourceName: "Ann Early",
    employerName: "Acme Farms",
    occupation: "Agriculture",
    transactionSource: "Individual",
    reportName: "Q1 2026",
    transactionSubTypeDescription: "Itemized Monetary",
    transactionCategory: null,
    hasChild: false,
    ...overrides,
  };
}

function writingDb() {
  const client = {
    query: vi.fn((sql: unknown) => {
      if (String(sql).includes("INSERT INTO public.ar_candidate_finance_links")) {
        return Promise.resolve({ rows: [{ id: LINK_ID }], rowCount: 1 });
      }
      return Promise.resolve({ rows: [], rowCount: 0 });
    }),
    release: vi.fn(),
  };
  const db = { query: vi.fn(), connect: vi.fn().mockResolvedValue(client) };
  return { db, client };
}

function baseInput(db: { query: unknown; connect: unknown }, rows: ArkansasTransactionRow[]) {
  const fetchTransactions = vi.fn(async () => rows);
  return {
    input: {
      db: db as never,
      candidateId: CANDIDATE_ID,
      electionId: ELECTION_ID,
      candidateName: "Jane Doe",
      electionYear: 2026,
      officeScope: "state_lower",
      officeName: "State Lower Chamber Legislator",
      district: "State House District 59",
      link: {
        filingEntityId: 7968,
        filerName: "Doe, Jane A.",
        linkSource: "cfis_registration" as const,
        sourceUrl: "https://api-ethics-disclosures.sos.arkansas.gov/api/PublicFilerDetails/GetCandidateCommitteDetails",
      },
      now: new Date("2026-09-02T12:00:00Z"),
      loadRegistrations: vi.fn(async () => [registration({ electionYear: 2024, registrationGuid: OTHER_GUID }), registration()]),
      fetchTransactions,
    },
    fetchTransactions,
  };
}

const HOUSE_59 = { cfisOfficeName: "State Representative", district: "59" };

describe("selectArkansasCandidateRegistration", () => {
  it("picks the one candidate registration for the entity, office, district and cycle", () => {
    const rows = [
      registration({ electionYear: 2024, registrationGuid: OTHER_GUID }),
      registration({ filerTypeCode: "SFIFILER", registrationGuid: OTHER_GUID }),
      registration(),
    ];
    expect(selectArkansasCandidateRegistration(rows, 7968, 2026, HOUSE_59).registrationGuid).toBe(REGISTRATION_GUID);
    expect(() => selectArkansasCandidateRegistration(rows, 7968, 2028, HOUSE_59)).toThrow(/no candidate registration/);
    expect(() => selectArkansasCandidateRegistration([...rows, registration()], 7968, 2026, HOUSE_59)).toThrow(
      /2 times/
    );
  });

  it("ignores the entity's registrations for another office or district", () => {
    const mayor = registration({ office: "Mayor", officeDistrictName: null, registrationGuid: OTHER_GUID });
    const house60 = registration({ officeDistrictName: "60", registrationGuid: OTHER_GUID });
    expect(() => selectArkansasCandidateRegistration([mayor, house60], 7968, 2026, HOUSE_59)).toThrow(
      /no candidate registration for entity 7968 as State Representative district 59/
    );
    expect(selectArkansasCandidateRegistration([mayor, house60, registration()], 7968, 2026, HOUSE_59).registrationGuid).toBe(
      REGISTRATION_GUID
    );
    expect(
      selectArkansasCandidateRegistration([mayor], 7968, 2026, { cfisOfficeName: "Mayor", district: null }).registrationGuid
    ).toBe(OTHER_GUID);
  });

  it("falls back to the office's year-less registration only when no cycle row exists", () => {
    const yearless = registration({ electionYear: null, registrationGuid: OTHER_GUID });
    expect(selectArkansasCandidateRegistration([yearless], 7968, 2026, HOUSE_59).registrationGuid).toBe(OTHER_GUID);
    expect(selectArkansasCandidateRegistration([yearless, registration()], 7968, 2026, HOUSE_59).registrationGuid).toBe(
      REGISTRATION_GUID
    );
    expect(() => selectArkansasCandidateRegistration([yearless, yearless], 7968, 2026, HOUSE_59)).toThrow(/2 times/);
    expect(() =>
      selectArkansasCandidateRegistration(
        [registration({ electionYear: 2024, registrationGuid: OTHER_GUID })],
        7968,
        2026,
        HOUSE_59
      )
    ).toThrow(/no candidate registration/);
    // A year-less registration for a different office never stands in for the linked race.
    const yearlessMayor = registration({
      electionYear: null,
      filingYear: 2028,
      office: "Mayor",
      officeDistrictName: null,
      registrationGuid: OTHER_GUID,
    });
    expect(() => selectArkansasCandidateRegistration([yearlessMayor], 7968, 2026, HOUSE_59)).toThrow(
      /no candidate registration/
    );
  });
});

describe("syncArkansasCandidateFinance", () => {
  it("pulls the cycle registration's receipts and replaces the snapshot with the link passed through", async () => {
    const { db, client } = writingDb();
    const { input, fetchTransactions } = baseInput(db, [receipt()]);

    const result = await syncArkansasCandidateFinance(input);

    expect(fetchTransactions).toHaveBeenCalledWith(
      { filerRegistrationGuid: REGISTRATION_GUID, transactionTypeCode: "TCON", pageSize: 1000 },
      undefined
    );
    expect(result).toMatchObject({
      dryRun: false,
      filingEntityId: 7968,
      registrationGuid: REGISTRATION_GUID,
      receiptRowCount: 1,
      summaryWritten: true,
      directBreakdownsWritten: 2,
    });
    expect(result.aggregation.reconciliation.status).toBe("reconciled");

    const linkInsert = client.query.mock.calls.find(([sql]) => String(sql).includes("INSERT INTO public.ar_candidate_finance_links"));
    expect(linkInsert?.[1]).toEqual([
      CANDIDATE_ID,
      ELECTION_ID,
      2026,
      "JANE DOE",
      "State Lower Chamber Legislator",
      "State House District 59",
      "7968",
      "Doe, Jane A.",
      "active",
      "cfis_registration",
      "https://api-ethics-disclosures.sos.arkansas.gov/api/PublicFilerDetails/GetCandidateCommitteDetails",
      "2026-09-02T12:00:00.000Z",
    ]);
    const summaryInsert = client.query.mock.calls.find(([sql]) => String(sql).includes("INSERT INTO public.ar_candidate_finance_summaries"));
    expect(summaryInsert?.[1]).toEqual([
      LINK_ID,
      2026,
      400,
      400,
      120,
      280,
      null,
      null,
      "https://ethics-disclosures.sos.arkansas.gov/",
      "2026-09-02T12:00:00.000Z",
    ]);
  });

  it("writes the totals but clears breakdowns when receipts do not reconcile", async () => {
    const { db, client } = writingDb();
    const { input } = baseInput(db, [receipt(), receipt({ guid: "00000000-0000-4000-8000-000000000002", transactionAmount: 50 })]);

    const result = await syncArkansasCandidateFinance(input);

    expect(result.aggregation.reconciliation).toMatchObject({ status: "unreconciled", deltaCents: 5_000 });
    expect(result.summaryWritten).toBe(true);
    expect(result.directBreakdownsWritten).toBe(0);
    const breakdownDelete = client.query.mock.calls.find(([sql]) =>
      String(sql).includes("DELETE FROM public.ar_candidate_finance_direct_breakdowns")
    );
    expect(breakdownDelete).toBeDefined();
    expect(client.query.mock.calls.some(([sql]) => String(sql).includes("INSERT INTO public.ar_candidate_finance_direct_breakdowns"))).toBe(false);
  });

  it("dry run aggregates without touching the database", async () => {
    const { db, client } = writingDb();
    const { input } = baseInput(db, [receipt()]);

    const result = await syncArkansasCandidateFinance({ ...input, dryRun: true });

    expect(result.dryRun).toBe(true);
    expect(result.summaryWritten).toBe(false);
    expect(result.aggregation.directBreakdowns).toHaveLength(2);
    expect(db.connect).not.toHaveBeenCalled();
    expect(client.query).not.toHaveBeenCalled();
  });

  it("fails before fetching receipts when the cycle registration is missing", async () => {
    const { db, client } = writingDb();
    const { input, fetchTransactions } = baseInput(db, [receipt()]);

    await expect(syncArkansasCandidateFinance({ ...input, electionYear: 2028 })).rejects.toThrow(
      /no candidate registration for entity 7968 as State Representative district 59 in the 2028 cycle/
    );
    expect(fetchTransactions).not.toHaveBeenCalled();
    expect(client.query).not.toHaveBeenCalled();
  });

  it("lets a failed receipt pull propagate so the prior snapshot survives", async () => {
    const { db, client } = writingDb();
    const { input } = baseInput(db, []);
    const fetchTransactions = vi.fn(async () => {
      throw new Error("Arkansas CFIS transaction pull returned 3 duplicate guids");
    });

    await expect(syncArkansasCandidateFinance({ ...input, fetchTransactions })).rejects.toThrow(/duplicate guids/);
    expect(client.query).not.toHaveBeenCalled();
  });
});
