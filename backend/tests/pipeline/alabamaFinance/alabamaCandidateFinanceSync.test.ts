import { describe, expect, it, vi } from "vitest";

import type {
  AlabamaCommitteeSearchRow,
  AlabamaRaceRow,
} from "../../../src/pipeline/alabamaFinance/alabamaFcpaClient.js";
import type { AlabamaCashRow } from "../../../src/pipeline/alabamaFinance/alabamaFcpaCsv.js";
import {
  alabamaBucketExtractYears,
  syncAlabamaCandidateFinance,
} from "../../../src/pipeline/alabamaFinance/alabamaCandidateFinanceSync.js";

const LINK_ID = "33333333-3333-4333-8333-333333333333";

function raceRow(overrides: Partial<AlabamaRaceRow>): AlabamaRaceRow {
  return {
    COMMITTEEID: 7962,
    CANDIDATE: "Doug Jones",
    CANDIDATESTATUS: "Active",
    BEGINNINGFUNDS: 0,
    MONETARYCONTRIB: 100,
    MONETARYEXP: 40,
    NONMONETARYCONTRIB: 5,
    OTHERSOURCES: 20,
    ENDINGFUNDS: 85,
    YEAR: null,
    ...overrides,
  };
}

function cashRow(overrides: Partial<AlabamaCashRow>): AlabamaCashRow {
  return {
    committeeId: "32837",
    amountCents: 10_000,
    contributionDate: "2026-03-01",
    lastName: "Smith",
    firstName: "Ann",
    contributionId: "1",
    filedDate: "2026-03-02",
    contributionType: "Cash (Itemized)",
    contributorType: "Individual",
    committeeType: "Principal Campaign Committee",
    committeeName: "Friends of Jones",
    candidateName: "Doug Jones",
    amended: "N",
    ...overrides,
  };
}

function writingDb() {
  const client = {
    query: vi.fn((sql: unknown) => {
      if (String(sql).includes("INSERT INTO public.al_candidate_finance_links")) {
        return Promise.resolve({ rows: [{ id: LINK_ID }], rowCount: 1 });
      }
      return Promise.resolve({ rows: [], rowCount: 0 });
    }),
    release: vi.fn(),
  };
  const db = { query: vi.fn(), connect: vi.fn().mockResolvedValue(client) };
  return { db, client };
}

function officeContext(rows: AlabamaRaceRow[]) {
  return vi.fn(async () => ({
    raceRows: rows,
    committeeRowsByInternalId: new Map<number, AlabamaCommitteeSearchRow>(),
  }));
}

function cashLoader(rowsByYear: Record<number, AlabamaCashRow[]>) {
  return vi.fn(async (year: number) => ({
    rows: rowsByYear[year] ?? [],
    quarantinedCount: 0,
  }));
}

function baseInput(db: { query: unknown; connect: unknown }) {
  return {
    db: db as never,
    candidateId: "candidate-1",
    electionId: "election-1",
    candidateName: "Doug Jones",
    electionYear: 2026,
    officeScope: "statewide",
    officeName: "Governor",
    ballotTitle: "Governor",
    district: null,
    link: {
      internalCommitteeId: 7962,
      committeeName: "Doug Jones",
      fcpaCommitteeNumber: "32837" as string | null,
      linkSource: "fcpa_race_search" as const,
      sourceUrl: "https://fcpa.alabamavotes.gov/",
    },
    now: new Date("2026-09-01T00:00:00Z"),
  };
}

describe("alabamaBucketExtractYears", () => {
  it("spans the term-length cycle, clamped to the first extract year", () => {
    expect(alabamaBucketExtractYears(2026, 4)).toEqual([2023, 2024, 2025, 2026]);
    // Alabama appellate judges serve six-year terms.
    expect(alabamaBucketExtractYears(2026, 6)).toEqual([2021, 2022, 2023, 2024, 2025, 2026]);
    expect(alabamaBucketExtractYears(2014, 4)).toEqual([2013, 2014]);
  });
});

describe("syncAlabamaCandidateFinance", () => {
  it("writes the race-row summary and coverage-passing buckets", async () => {
    const { db, client } = writingDb();
    const loadCashRows = cashLoader({ 2026: [cashRow({})] });
    const result = await syncAlabamaCandidateFinance({
      ...baseInput(db),
      loadOfficeRaceContext: officeContext([raceRow({})]),
      loadCashRows,
    });
    expect(result).toMatchObject({
      status: "synced",
      totalReceipts: 125,
      directContributionTotal: 105,
      totalDisbursements: 40,
      cashOnHand: 85,
      coverageRatio: 1,
      bucketDiagnostics: [],
      summaryWritten: true,
      bucketsWritten: 1,
    });
    expect(loadCashRows).toHaveBeenCalledTimes(4);
    const summaryInsert = client.query.mock.calls.find((call) =>
      String(call[0]).includes("INSERT INTO public.al_candidate_finance_summaries")
    );
    expect(summaryInsert).toBeDefined();
    const breakdownInsert = client.query.mock.calls.find((call) =>
      String(call[0]).includes("INSERT INTO public.al_candidate_finance_direct_breakdowns")
    );
    expect(breakdownInsert).toBeDefined();
  });

  it("dry run computes without touching the database", async () => {
    const { db, client } = writingDb();
    const result = await syncAlabamaCandidateFinance({
      ...baseInput(db),
      dryRun: true,
      loadOfficeRaceContext: officeContext([raceRow({})]),
      loadCashRows: cashLoader({ 2026: [cashRow({})] }),
    });
    expect(result).toMatchObject({ dryRun: true, summaryWritten: false, bucketsWritten: 0 });
    expect(db.connect).not.toHaveBeenCalled();
    expect(client.query).not.toHaveBeenCalled();
  });

  it("throws and writes nothing when the race row is missing", async () => {
    const { db, client } = writingDb();
    await expect(
      syncAlabamaCandidateFinance({
        ...baseInput(db),
        loadOfficeRaceContext: officeContext([raceRow({ COMMITTEEID: 1 })]),
        loadCashRows: cashLoader({}),
      })
    ).rejects.toThrow("race row for internal committee id 7962 not found");
    expect(client.query).not.toHaveBeenCalled();
  });

  it("writes the summary with cleared buckets when the FCPA number is missing", async () => {
    const { db } = writingDb();
    const loadCashRows = cashLoader({ 2026: [cashRow({})] });
    const base = baseInput(db);
    const result = await syncAlabamaCandidateFinance({
      ...base,
      link: { ...base.link, fcpaCommitteeNumber: null },
      loadOfficeRaceContext: officeContext([raceRow({})]),
      loadCashRows,
    });
    expect(result).toMatchObject({
      summaryWritten: true,
      bucketsWritten: 0,
      bucketDiagnostics: ["fcpa_committee_number_missing"],
    });
    expect(loadCashRows).not.toHaveBeenCalled();
  });

  it("self-heals a NULL FCPA number from the committee-search join and persists it", async () => {
    const { db } = writingDb();
    const base = baseInput(db);
    const result = await syncAlabamaCandidateFinance({
      ...base,
      link: { ...base.link, fcpaCommitteeNumber: null },
      loadOfficeRaceContext: vi.fn(async () => ({
        raceRows: [raceRow({})],
        committeeRowsByInternalId: new Map([
          [7962, { id: 7962, committeeId: "32837" } as never],
        ]),
      })),
      loadCashRows: cashLoader({ 2026: [cashRow({})] }),
    });
    expect(result).toMatchObject({
      fcpaCommitteeNumber: "32837",
      bucketDiagnostics: [],
      bucketsWritten: 1,
    });
    const backfill = db.query.mock.calls.find((call) =>
      String(call[0]).includes("SET fcpa_committee_number")
    );
    expect(backfill?.[1]).toEqual(["candidate-1", "election-1", "7962", "32837"]);
  });

  it("uses the six-year window for appellate-court candidates", async () => {
    const { db } = writingDb();
    const loadCashRows = cashLoader({ 2026: [cashRow({})] });
    const base = baseInput(db);
    const result = await syncAlabamaCandidateFinance({
      ...base,
      officeName: "State Level Judge",
      ballotTitle: "Associate Justice of the Alabama Supreme Court, Place 7",
      dryRun: true,
      loadOfficeRaceContext: officeContext([raceRow({})]),
      loadCashRows,
    });
    expect(result.bucketExtractYears).toEqual([2021, 2022, 2023, 2024, 2025, 2026]);
    expect(loadCashRows).toHaveBeenCalledTimes(6);
  });

  it("gates buckets off when a window artifact is unreadable, keeping the summary", async () => {
    const { db } = writingDb();
    const result = await syncAlabamaCandidateFinance({
      ...baseInput(db),
      loadOfficeRaceContext: officeContext([raceRow({})]),
      loadCashRows: vi.fn(async (year: number) => {
        if (year === 2024) throw new Error("no cached metadata");
        return { rows: [cashRow({})], quarantinedCount: 0 };
      }),
    });
    expect(result.summaryWritten).toBe(true);
    expect(result.bucketsWritten).toBe(0);
    expect(result.bucketDiagnostics[0]).toContain("artifact_unavailable:2024");
  });

  it("gates buckets off outside the coverage tolerance, keeping the summary", async () => {
    const { db } = writingDb();
    const result = await syncAlabamaCandidateFinance({
      ...baseInput(db),
      loadOfficeRaceContext: officeContext([raceRow({ MONETARYCONTRIB: 1_000 })]),
      loadCashRows: cashLoader({ 2026: [cashRow({})] }),
    });
    expect(result.summaryWritten).toBe(true);
    expect(result.bucketsWritten).toBe(0);
    expect(result.bucketDiagnostics[0]).toContain("cash_coverage_out_of_tolerance");
  });

  it("rejects ineligible offices", async () => {
    const { db } = writingDb();
    await expect(
      syncAlabamaCandidateFinance({
        ...baseInput(db),
        officeScope: "county",
        officeName: "Sheriff",
        loadOfficeRaceContext: officeContext([raceRow({})]),
        loadCashRows: cashLoader({}),
      })
    ).rejects.toThrow("not Alabama-finance eligible");
  });
});
