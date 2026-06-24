import { describe, expect, it, vi } from "vitest";

import {
  listDueMassachusettsCandidateFinanceSyncRows,
  syncDueMassachusettsCandidateFinance,
} from "../../../src/pipeline/massachusettsFinance/massachusettsCandidateFinanceBatchSync.js";
import type { MassachusettsCandidateCommitteeResolution } from "../../../src/pipeline/massachusettsFinance/massachusettsCandidateCommitteeResolver.js";

const NOW = new Date("2026-06-01T00:00:00.000Z");
const SOURCE_URL = "https://api.ocpf.us/filers/listings/A?searchPhrase=Maura%20Healey";
const CANDIDATE_ID = "11111111-1111-4111-8111-111111111111";
const ELECTION_ID = "22222222-2222-4222-8222-222222222222";

function dueRow(overrides: Record<string, unknown> = {}) {
  return {
    candidate_id: CANDIDATE_ID,
    election_id: ELECTION_ID,
    candidate_name: "Maura Healey",
    election_year: 2022,
    office_scope: "statewide",
    office_name: "Governor",
    district: null,
    candidate_cpf_id: "15710",
    filer_name: "Healey, Maura T.",
    committee_name: "Healey Committee",
    source_url: SOURCE_URL,
    last_synced_at: null,
    total_due_rows: "1",
    ...overrides,
  };
}

function matchedResolution(): Extract<MassachusettsCandidateCommitteeResolution, { status: "matched" }> {
  return {
    status: "matched",
    candidateCpfId: "15710",
    filerName: "Healey, Maura T.",
    committeeName: "Healey Committee",
    officeSought: "Statewide, Governor",
    confidence: "exact",
    source: "ocpf_api",
    sourceUrl: SOURCE_URL,
    matchedFilerRowCount: 1,
  };
}

describe("massachusettsCandidateFinanceBatchSync", () => {
  it("lists active Massachusetts finance links that are due for sync", async () => {
    const db = {
      query: vi.fn(async () => ({
        rows: [
          dueRow({ total_due_rows: "2" }),
          dueRow({
            candidate_id: "33333333-3333-4333-8333-333333333333",
            election_id: "44444444-4444-4444-8444-444444444444",
            candidate_name: "Jane Doe",
            election_year: 2026,
            office_scope: "state_lower",
            office_name: "State Lower Chamber Legislator",
            district: "09",
            candidate_cpf_id: "20001",
            filer_name: "Doe, Jane",
            committee_name: "Doe Committee",
            source_url: null,
            last_synced_at: "2026-01-01 00:00:00+00",
            total_due_rows: "2",
          }),
        ],
        rowCount: 2,
      })),
    };

    await expect(
      listDueMassachusettsCandidateFinanceSyncRows(db, {
        now: NOW,
        staleAfterDays: 7,
        maxCandidates: 25,
        electionLookbackDays: 1,
        electionLookaheadDays: 730,
      })
    ).resolves.toEqual({
      totalDueRows: 2,
      rows: [
        {
          candidateId: CANDIDATE_ID,
          electionId: ELECTION_ID,
          candidateName: "Maura Healey",
          electionYear: 2022,
          officeScope: "statewide",
          officeName: "Governor",
          district: null,
          candidateCpfId: "15710",
          filerName: "Healey, Maura T.",
          committeeName: "Healey Committee",
          sourceUrl: SOURCE_URL,
          lastSyncedAt: null,
        },
        {
          candidateId: "33333333-3333-4333-8333-333333333333",
          electionId: "44444444-4444-4444-8444-444444444444",
          candidateName: "Jane Doe",
          electionYear: 2026,
          officeScope: "state_lower",
          officeName: "State Lower Chamber Legislator",
          district: "09",
          candidateCpfId: "20001",
          filerName: "Doe, Jane",
          committeeName: "Doe Committee",
          sourceUrl: null,
          lastSyncedAt: "2026-01-01 00:00:00+00",
        },
      ],
    });

    const sql = String(db.query.mock.calls[0]?.[0]);
    expect(sql).toContain("FROM public.ma_candidate_finance_links AS link");
    expect(sql).toContain("link.link_status = 'active'");
    expect(sql).toContain("district.state = 'MA'");
    expect(sql).toContain("election.race_type = 'office'");
    expect(sql).toContain("candidate_election.status NOT IN ('withdrawn', 'lost')");
    expect(sql).toContain("(office.scope || '::' || office.canonical_name) = ANY($6::text[])");
    expect(db.query.mock.calls[0]?.[1]).toEqual([
      "2026-06-01T00:00:00.000Z",
      7,
      25,
      1,
      730,
      expect.arrayContaining([
        "statewide::Governor",
        "statewide::Lieutenant Governor",
        "statewide::Attorney General",
        "state_upper::State Senator",
        "state_lower::State Lower Chamber Legislator",
      ]),
    ]);
  });

  it("uses one post-election grace day by default", async () => {
    const db = { query: vi.fn(async () => ({ rows: [], rowCount: 0 })), connect: vi.fn() };
    const syncMassachusettsCandidateFinanceFn = vi.fn();

    await syncDueMassachusettsCandidateFinance({
      db,
      now: NOW,
      autoLinkMissingLinks: false,
      syncMassachusettsCandidateFinanceFn: syncMassachusettsCandidateFinanceFn as never,
    });

    expect(db.query.mock.calls[0]?.[1]).toEqual([
      "2026-06-01T00:00:00.000Z",
      7,
      25,
      1,
      730,
      expect.any(Array),
    ]);
    expect(syncMassachusettsCandidateFinanceFn).not.toHaveBeenCalled();
  });

  it("syncs selected due candidates with trusted linked CPF identities and continues after a candidate failure", async () => {
    const db = {
      query: vi.fn(async () => ({
        rows: [
          dueRow({ total_due_rows: "2" }),
          dueRow({
            candidate_id: "33333333-3333-4333-8333-333333333333",
            election_id: "44444444-4444-4444-8444-444444444444",
            candidate_name: "Jane Doe",
            candidate_cpf_id: "20001",
            filer_name: "Doe, Jane",
            committee_name: "Doe Committee",
            source_url: null,
            total_due_rows: "2",
          }),
        ],
        rowCount: 2,
      })),
      connect: vi.fn(),
    };
    const successfulSync = {
      candidateId: CANDIDATE_ID,
      electionId: ELECTION_ID,
      electionYear: 2022,
      dryRun: false,
      resolution: matchedResolution(),
      linkWritten: true,
      summaryWritten: true,
      directBreakdownsWritten: 1,
      outsideGroupsWritten: 0,
      outsideGroupBreakdownsWritten: 0,
      totalReceipts: 100,
      directContributionTotal: 100,
      outsideSupportTotal: 0,
      outsideOpposeTotal: 0,
      matchedContributionRowCount: 1,
      includedContributionRowCount: 1,
      skippedContributionRowCount: 0,
      matchedExpenditureRowCount: 0,
      includedExpenditureRowCount: 0,
      skippedExpenditureRowCount: 0,
      matchedReceiptRowCount: 0,
      includedReceiptRowCount: 0,
      skippedReceiptRowCount: 0,
      iepacReportCount: 0,
      iepacReportDetailCount: 0,
    };
    const syncMassachusettsCandidateFinanceFn = vi
      .fn()
      .mockResolvedValueOnce(successfulSync)
      .mockRejectedValueOnce(new Error("OCPF unavailable"));

    const result = await syncDueMassachusettsCandidateFinance({
      db,
      now: NOW,
      maxCandidates: 2,
      staleAfterDays: 3,
      electionLookbackDays: 30,
      autoLinkMissingLinks: false,
      syncMassachusettsCandidateFinanceFn: syncMassachusettsCandidateFinanceFn as never,
    });

    expect(result).toMatchObject({
      dryRun: false,
      dueCandidateCount: 2,
      selectedCandidateCount: 2,
      syncedCandidateCount: 1,
      failedCandidateCount: 1,
      autoLinkAttemptedCount: 0,
      autoLinkLinkedCount: 0,
    });
    expect(result.results[0]).toMatchObject({ ok: true, result: successfulSync });
    expect(result.results[1]).toMatchObject({ ok: false, error: "OCPF unavailable" });
    expect(syncMassachusettsCandidateFinanceFn).toHaveBeenCalledWith(
      expect.objectContaining({
        candidateId: CANDIDATE_ID,
        electionId: ELECTION_ID,
        candidateName: "Maura Healey",
        electionYear: 2022,
        officeScope: "statewide",
        officeName: "Governor",
        district: null,
        sourceUrl: SOURCE_URL,
        trustedCommittee: {
          candidateCpfId: "15710",
          filerName: "Healey, Maura T.",
          committeeName: "Healey Committee",
          sourceUrl: SOURCE_URL,
        },
        dryRun: false,
        now: NOW,
      })
    );
  });

  it("auto-links missing eligible candidate elections before selecting due links", async () => {
    const db = {
      query: vi.fn(async (sql: string) => {
        const text = String(sql);
        if (text.includes("INSERT INTO public.ma_candidate_finance_links")) {
          return { rows: [{ id: "link-1" }], rowCount: 1 };
        }
        if (text.includes("FROM public.candidate_elections AS candidate_election") && !text.includes("WITH due AS")) {
          return {
            rows: [
              {
                candidate_id: CANDIDATE_ID,
                election_id: ELECTION_ID,
                candidate_name: "Maura Healey",
                election_year: 2022,
                office_scope: "statewide",
                office_name: "Governor",
                district: null,
              },
            ],
            rowCount: 1,
          };
        }
        if (text.includes("FROM public.ma_candidate_finance_links AS link")) {
          return { rows: [], rowCount: 0 };
        }
        return { rows: [], rowCount: 0 };
      }),
      connect: vi.fn(),
    };
    const resolveCandidateCommittee = vi.fn(async () => matchedResolution());

    const result = await syncDueMassachusettsCandidateFinance({
      db,
      now: NOW,
      maxCandidates: 5,
      resolveCandidateCommittee,
      syncMassachusettsCandidateFinanceFn: vi.fn() as never,
    });

    expect(result).toMatchObject({
      dueCandidateCount: 0,
      selectedCandidateCount: 0,
      autoLinkAttemptedCount: 1,
      autoLinkLinkedCount: 1,
    });
    expect(resolveCandidateCommittee).toHaveBeenCalledWith(
      {
        candidateName: "Maura Healey",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2022,
        district: null,
      },
      undefined
    );
    expect(
      db.query.mock.calls.some((call) => String(call[0]).includes("INSERT INTO public.ma_candidate_finance_links"))
    ).toBe(true);
  });
});
