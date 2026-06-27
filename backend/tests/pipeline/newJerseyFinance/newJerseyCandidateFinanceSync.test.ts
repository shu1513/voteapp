import { describe, expect, it, vi } from "vitest";

import {
  syncNewJerseyCandidateFinance,
  syncNewJerseyCandidateFinanceFromElec,
} from "../../../src/pipeline/newJerseyFinance/newJerseyCandidateFinanceSync.js";
import type {
  NewJerseyElecContributionRow,
  NewJerseyElecEntity,
} from "../../../src/pipeline/newJerseyFinance/newJerseyElecClient.js";

const CANDIDATE_ID = "11111111-1111-1111-1111-111111111111";
const ELECTION_ID = "22222222-2222-2222-2222-222222222222";
const LINK_ID = "33333333-3333-3333-3333-333333333333";
const DIRECT_SOURCE_URL = "https://www.njelecefilesearch.com/SearchContributionToEntity?eid=473742";
const OUTSIDE_SOURCE_URL = "https://www.njelecefilesearch.com/SearchContributionToEntity?eid=477267";
const REPORT_SOURCE_URL = "https://www.njelecefilesearch.com/SearchIndExpReports/?handler=DownloadReport&DocId=3909738";

const REPORT_TEXT = `
  ALLOCATION OF EXPENDITURES BENEFITING CANDIDATE(S) / COMMITTEE(S)
  Office Candidate/Committee Name Election Date Location Amount
  NJ Gubernatorial MIKIE SHERRILL FOR GOVERNOR 11/04/2025 STATEWIDE $100,082.02
`;

function entity(overrides: Partial<NewJerseyElecEntity> = {}): NewJerseyElecEntity {
  return {
    entityS: 473742,
    entityName: "SHERRILL, MIKIE",
    firstName: "MIKIE",
    middleInitial: null,
    lastName: "SHERRILL",
    suffix: null,
    nonIndividualName: null,
    pacName: null,
    electionYear: 2025,
    sequenceNumber: null,
    officeCode: "1",
    office: "Governor",
    partyCode: "D",
    party: "Democratic",
    locationCode: 0,
    location: "Statewide",
    electionTypeCode: "G",
    electionType: "General",
    entityType: "Candidate",
    sourceUrl: "https://www.njelecefilesearch.com/api/VWEntity/GetEntityList?LastName=SHERRILL",
    ...overrides,
  };
}

function createMockDb() {
  const query = vi.fn().mockResolvedValue({ rows: [{ id: LINK_ID }], rowCount: 1 });
  const client = { query, release: vi.fn() };
  return {
    query,
    connect: vi.fn().mockResolvedValue(client),
    client,
  };
}

function contribution(overrides: Partial<NewJerseyElecContributionRow> = {}): NewJerseyElecContributionRow {
  return {
    contribS: 1001,
    entityS: 473742,
    electionYear: 2025,
    recipientName: "SHERRILL, MIKIE",
    contributorName: "Jane Doe",
    contributorFirstName: "Jane",
    contributorLastName: "Doe",
    contributorNonIndividualName: null,
    isIndividual: true,
    contributorType: "Individual",
    contributionType: "Monetary",
    contributionDate: "06/01/2025",
    amount: 100,
    employerName: "Acme Law",
    occupationCode: "1100",
    occupationName: "Attorney",
    sourceUrl: DIRECT_SOURCE_URL,
    ...overrides,
  };
}

function baseInput() {
  return {
    candidateId: CANDIDATE_ID,
    electionId: ELECTION_ID,
    candidateName: "Mikie Sherrill",
    electionYear: 2025,
    officeName: "Governor",
    candidateEntityS: 473742,
    candidateEntityName: "SHERRILL, MIKIE",
    electionTypeCode: "G",
    sourceUrl: "https://www.njelecefilesearch.com/api/VWEntity/GetEntityList?LastName=Sherrill",
    contributionSourceUrl: DIRECT_SOURCE_URL,
    now: new Date("2026-06-25T13:30:00.000Z"),
    contributions: [
      contribution({ contribS: 1001, amount: 100 }),
      contribution({ contribS: 1002, contributorName: "John Roe", amount: 250 }),
      contribution({
        contribS: 1003,
        contributorName: "Organization PAC",
        isIndividual: false,
        contributorType: "PAC",
        amount: 5_000,
      }),
    ],
    outsideGroups: [
      {
        entityS: 477267,
        entityName: "ONE GIANT LEAP PAC - OGL PAC",
        sourceUrl: REPORT_SOURCE_URL,
        reportTexts: [{ text: REPORT_TEXT, sourceUrl: REPORT_SOURCE_URL, docId: 3909738 }],
        contributions: [
          contribution({
            contribS: 2001,
            entityS: 477267,
            recipientName: "ONE GIANT LEAP PAC - OGL PAC",
            contributorName: "Jane Street Capital LLC",
            contributorFirstName: null,
            contributorLastName: null,
            contributorNonIndividualName: "Jane Street Capital LLC",
            isIndividual: false,
            contributorType: "Business",
            amount: 100_000,
            employerName: null,
            occupationName: null,
            sourceUrl: OUTSIDE_SOURCE_URL,
          }),
          contribution({
            contribS: 2002,
            entityS: 477267,
            recipientName: "ONE GIANT LEAP PAC - OGL PAC",
            contributorName: "Acme Properties LLC",
            contributorFirstName: null,
            contributorLastName: null,
            contributorNonIndividualName: "Acme Properties LLC",
            isIndividual: false,
            contributorType: "Business",
            amount: 75_000,
            employerName: null,
            occupationName: null,
            sourceUrl: OUTSIDE_SOURCE_URL,
          }),
          contribution({
            contribS: 2003,
            entityS: 477267,
            recipientName: "ONE GIANT LEAP PAC - OGL PAC",
            contributorName: "Jane Donor",
            amount: 50_000,
            employerName: "Google",
            occupationName: "Software Engineer",
            sourceUrl: OUTSIDE_SOURCE_URL,
          }),
          contribution({
            contribS: 2004,
            entityS: 477267,
            recipientName: "ONE GIANT LEAP PAC - OGL PAC",
            contributorName: "Refund Row LLC",
            isIndividual: false,
            contributionType: "Refund",
            amount: 10_000,
            sourceUrl: OUTSIDE_SOURCE_URL,
          }),
        ],
      },
    ],
  };
}

describe("newJerseyCandidateFinanceSync", () => {
  it("aggregates direct/outside NJ finance and writes a snapshot", async () => {
    const db = createMockDb();

    const result = await syncNewJerseyCandidateFinance({
      db,
      ...baseInput(),
      outsideMinIndustryAmount: 0,
    });

    expect(result).toMatchObject({
      candidateId: CANDIDATE_ID,
      electionId: ELECTION_ID,
      electionYear: 2025,
      dryRun: false,
      linkWritten: true,
      summaryWritten: true,
      directBreakdownsWritten: 4,
      outsideGroupsWritten: 1,
      outsideGroupBreakdownsWritten: 8,
      totalReceipts: 350,
      directContributionTotal: 350,
      outsideSupportTotal: 100_082.02,
      outsideOpposeTotal: 0,
      matchedContributionRowCount: 3,
      includedContributionRowCount: 2,
      skippedContributionRowCount: 1,
      matchedAllocationRowCount: 1,
      includedAllocationRowCount: 1,
      skippedAllocationRowCount: 0,
      matchedOutsideContributionRowCount: 4,
      includedOutsideContributionRowCount: 3,
      skippedOutsideContributionRowCount: 1,
      outsideReportTextCount: 1,
    });

    expect(db.query.mock.calls.some((call) => call[0] === "BEGIN")).toBe(true);
    expect(db.query.mock.calls.at(-1)?.[0]).toBe("COMMIT");

    const linkCall = db.query.mock.calls.find((call) =>
      String(call[0]).includes("INSERT INTO public.nj_candidate_finance_links")
    );
    expect(linkCall?.[1]).toEqual([
      CANDIDATE_ID,
      ELECTION_ID,
      2025,
      "MIKIE SHERRILL",
      "Governor",
      null,
      473742,
      "SHERRILL, MIKIE",
      "G",
      "active",
      "elec_api",
      "https://www.njelecefilesearch.com/api/VWEntity/GetEntityList?LastName=Sherrill",
      "2026-06-25T13:30:00.000Z",
    ]);

    const summaryCall = db.query.mock.calls.find((call) =>
      String(call[0]).includes("INSERT INTO public.nj_candidate_finance_summaries")
    );
    expect(summaryCall?.[1]).toEqual([
      LINK_ID,
      2025,
      350,
      350,
      null,
      null,
      100_082.02,
      0,
      DIRECT_SOURCE_URL,
      "2026-06-25T13:30:00.000Z",
    ]);

    expect(
      db.query.mock.calls.filter((call) => String(call[0]).includes("INSERT INTO public.nj_candidate_finance_direct_breakdowns"))
    ).toHaveLength(4);
    expect(
      db.query.mock.calls.filter((call) => String(call[0]).includes("INSERT INTO public.nj_candidate_finance_outside_groups"))
    ).toHaveLength(1);
    expect(
      db.query.mock.calls.filter((call) => String(call[0]).includes("INSERT INTO public.nj_candidate_finance_outside_group_breakdowns"))
    ).toHaveLength(8);
  });

  it("does not write in dry-run mode but returns aggregation counts", async () => {
    const db = createMockDb();

    const result = await syncNewJerseyCandidateFinance({
      db,
      ...baseInput(),
      dryRun: true,
      outsideMinIndustryAmount: 0,
    });

    expect(result).toMatchObject({
      dryRun: true,
      linkWritten: false,
      summaryWritten: false,
      directBreakdownsWritten: 0,
      outsideGroupsWritten: 0,
      outsideGroupBreakdownsWritten: 0,
      totalReceipts: 350,
      directContributionTotal: 350,
      outsideSupportTotal: 100_082.02,
    });
    expect(db.query).not.toHaveBeenCalled();
  });

  it("resolves the candidate, pulls ELEC contributions, and syncs direct finance", async () => {
    const db = createMockDb();
    const rows = [contribution({ contribS: 3001, amount: 100 }), contribution({ contribS: 3002, amount: 250 })];
    const elecClient = {
      searchEntities: vi.fn().mockResolvedValue([entity()]),
      getContributionRows: vi.fn().mockResolvedValue({
        recordsTotal: 2,
        recordsFiltered: 2,
        rows,
        sourceUrl: DIRECT_SOURCE_URL,
      }),
    };

    const result = await syncNewJerseyCandidateFinanceFromElec({
      db,
      candidateId: CANDIDATE_ID,
      electionId: ELECTION_ID,
      candidateName: "Mikie Sherrill",
      electionYear: 2025,
      officeScope: "statewide",
      officeName: "Governor",
      electionTypeCode: "G",
      now: new Date("2026-06-25T13:30:00.000Z"),
      elecClient,
    });

    expect(result.status).toBe("matched");
    expect(elecClient.searchEntities).toHaveBeenCalledWith({ lastName: "SHERRILL", nonPacOnly: true }, undefined);
    expect(elecClient.getContributionRows).toHaveBeenCalledWith(
      {
        entityS: 473742,
        electionYear: 2025,
        firstName: "MIKIE",
        lastName: "SHERRILL",
        officeCode: "1",
        partyCode: "D",
        locationCode: 0,
        electionTypeCode: "G",
        nonPacOnly: true,
      },
      undefined
    );
    expect(result.syncResult).toMatchObject({
      directContributionTotal: 350,
      outsideSupportTotal: 0,
      outsideOpposeTotal: 0,
      directBreakdownsWritten: 4,
      outsideGroupsWritten: 0,
      outsideGroupBreakdownsWritten: 0,
    });
  });

  it("does not pull contributions or write when candidate resolution is ambiguous", async () => {
    const db = createMockDb();
    const elecClient = {
      searchEntities: vi.fn().mockResolvedValue([
        entity({ entityS: 473742, electionTypeCode: "P", electionType: "Primary" }),
        entity({ entityS: 473743, electionTypeCode: "G", electionType: "General" }),
      ]),
      getContributionRows: vi.fn(),
    };

    const result = await syncNewJerseyCandidateFinanceFromElec({
      db,
      candidateId: CANDIDATE_ID,
      electionId: ELECTION_ID,
      candidateName: "Mikie Sherrill",
      electionYear: 2025,
      officeScope: "statewide",
      officeName: "Governor",
      now: new Date("2026-06-25T13:30:00.000Z"),
      elecClient,
    });

    expect(result.status).toBe("ambiguous");
    expect(elecClient.getContributionRows).not.toHaveBeenCalled();
    expect(db.query).not.toHaveBeenCalled();
  });
});
