import { describe, expect, it, vi } from "vitest";

import {
  loadVirginiaReportDataForCommittee,
  listDueVirginiaCandidateFinanceSyncRows,
  syncDueVirginiaCandidateFinance,
  type VirginiaCandidateFinanceReportData,
} from "../../../src/pipeline/virginiaFinance/virginiaCandidateFinanceBatchSync.js";
import type { VirginiaCandidateCommitteeResolution } from "../../../src/pipeline/virginiaFinance/virginiaCandidateCommitteeResolver.js";
import type { VirginiaScheduleAContribution } from "../../../src/pipeline/virginiaFinance/virginiaCampaignFinanceClient.js";

const NOW = new Date("2026-06-01T00:00:00.000Z");
const SOURCE_URL = "https://cfreports.elections.virginia.gov/Committee/Index/60e10dc7-c59e-4a79-afca-e688c1efed65";
const CANDIDATE_ID = "11111111-1111-4111-8111-111111111111";
const ELECTION_ID = "22222222-2222-4222-8222-222222222222";

function dueRow(overrides: Record<string, unknown> = {}) {
  return {
    candidate_id: CANDIDATE_ID,
    election_id: ELECTION_ID,
    candidate_name: "Abigail Spanberger",
    election_year: 2025,
    office_scope: "statewide",
    office_name: "Governor",
    district: null,
    committee_id: "60e10dc7-c59e-4a79-afca-e688c1efed65",
    committee_code: "CC-23-02436",
    committee_name: "Spanberger for Governor",
    link_source: "cfreports_search",
    source_url: SOURCE_URL,
    last_synced_at: null,
    total_due_rows: "1",
    ...overrides,
  };
}

function contribution(overrides: Partial<VirginiaScheduleAContribution> = {}): VirginiaScheduleAContribution {
  return {
    contributorName: "Jane Voter",
    isIndividual: true,
    employer: "Acme Law",
    occupationOrTypeOfBusiness: "Attorney",
    transactionDate: "10/01/2025",
    amount: 250,
    totalToDate: 250,
    ...overrides,
  };
}

function reportData(overrides: Partial<VirginiaCandidateFinanceReportData> = {}): VirginiaCandidateFinanceReportData {
  return {
    committeeId: "60e10dc7-c59e-4a79-afca-e688c1efed65",
    sourceUrl: SOURCE_URL,
    contributions: [contribution()],
    scheduledReportCount: 1,
    ...overrides,
  };
}

function matchedResolution(): Extract<VirginiaCandidateCommitteeResolution, { status: "matched" }> {
  return {
    status: "matched",
    committeeId: "60e10dc7-c59e-4a79-afca-e688c1efed65",
    committeeName: "Spanberger for Governor",
    committeeCode: "CC-23-02436",
    candidateName: "Abigail Spanberger",
    confidence: "exact",
    source: "cfreports_search",
    sourceUrl: SOURCE_URL,
    matchedReportHeaderCount: 1,
  };
}

describe("virginiaCandidateFinanceBatchSync", () => {
  it("lists active Virginia finance links that are due for sync", async () => {
    const db = {
      query: vi.fn(async () => ({
        rows: [
          dueRow({ total_due_rows: "2" }),
          dueRow({
            candidate_id: "33333333-3333-4333-8333-333333333333",
            election_id: "44444444-4444-4444-8444-444444444444",
            candidate_name: "Jane Doe",
            election_year: 2027,
            office_scope: "state_lower",
            office_name: "State Lower Chamber Legislator",
            district: "09",
            committee_id: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
            committee_code: null,
            committee_name: "Jane Doe for Delegate",
            source_url: null,
            last_synced_at: "2026-01-01 00:00:00+00",
            total_due_rows: "2",
          }),
        ],
        rowCount: 2,
      })),
    };

    await expect(
      listDueVirginiaCandidateFinanceSyncRows(db, {
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
          candidateName: "Abigail Spanberger",
          electionYear: 2025,
          officeScope: "statewide",
          officeName: "Governor",
          district: null,
          committeeId: "60e10dc7-c59e-4a79-afca-e688c1efed65",
          committeeCode: "CC-23-02436",
          committeeName: "Spanberger for Governor",
          linkSource: "cfreports_search",
          sourceUrl: SOURCE_URL,
          lastSyncedAt: null,
        },
        {
          candidateId: "33333333-3333-4333-8333-333333333333",
          electionId: "44444444-4444-4444-8444-444444444444",
          candidateName: "Jane Doe",
          electionYear: 2027,
          officeScope: "state_lower",
          officeName: "State Lower Chamber Legislator",
          district: "09",
          committeeId: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
          committeeCode: null,
          committeeName: "Jane Doe for Delegate",
          linkSource: "cfreports_search",
          sourceUrl: null,
          lastSyncedAt: "2026-01-01 00:00:00+00",
        },
      ],
    });

    const sql = String(db.query.mock.calls[0]?.[0]);
    expect(sql).toContain("FROM public.va_candidate_finance_links AS link");
    expect(sql).toContain("link.link_status = 'active'");
    expect(sql).toContain("district.state = 'VA'");
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

  it("uses a one-day post-election grace window by default for due selection", async () => {
    const db = { query: vi.fn(async () => ({ rows: [], rowCount: 0 })) };
    const syncVirginiaCandidateFinanceFn = vi.fn();

    await syncDueVirginiaCandidateFinance({
      db,
      now: NOW,
      autoLinkMissingLinks: false,
      syncVirginiaCandidateFinanceFn: syncVirginiaCandidateFinanceFn as never,
    });

    expect(db.query.mock.calls[0]?.[1]).toEqual([
      "2026-06-01T00:00:00.000Z",
      7,
      25,
      1,
      730,
      expect.any(Array),
    ]);
    expect(syncVirginiaCandidateFinanceFn).not.toHaveBeenCalled();
  });

  it("syncs selected due candidates with fetched scheduled report contributions and continues after failures", async () => {
    const db = {
      query: vi.fn(async () => ({
        rows: [
          dueRow({ total_due_rows: "2" }),
          dueRow({
            candidate_id: "33333333-3333-4333-8333-333333333333",
            election_id: "44444444-4444-4444-8444-444444444444",
            candidate_name: "Jane Doe",
            committee_id: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
            committee_code: null,
            committee_name: "Jane Doe for Delegate",
            source_url: null,
            total_due_rows: "2",
          }),
        ],
        rowCount: 2,
      })),
    };
    const successfulSync = {
      candidateId: CANDIDATE_ID,
      electionId: ELECTION_ID,
      electionYear: 2025,
      dryRun: false,
      linkWritten: true,
      summaryWritten: true,
      directBreakdownsWritten: 2,
      totalReceipts: 250,
      directContributionTotal: 250,
      matchedContributionRowCount: 1,
      includedContributionRowCount: 1,
      skippedContributionRowCount: 0,
    };
    const loadReportDataForCommittee = vi.fn().mockResolvedValueOnce(reportData()).mockRejectedValueOnce(new Error("XML unavailable"));
    const syncVirginiaCandidateFinanceFn = vi.fn().mockResolvedValueOnce(successfulSync);

    const result = await syncDueVirginiaCandidateFinance({
      db,
      now: NOW,
      maxCandidates: 2,
      staleAfterDays: 3,
      electionLookbackDays: 30,
      autoLinkMissingLinks: false,
      loadReportDataForCommittee,
      syncVirginiaCandidateFinanceFn: syncVirginiaCandidateFinanceFn as never,
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
    expect(result.results[1]).toMatchObject({ ok: false, error: "XML unavailable" });
    expect(loadReportDataForCommittee).toHaveBeenCalledWith({
      committeeId: "60e10dc7-c59e-4a79-afca-e688c1efed65",
      clientOptions: undefined,
    });
    expect(syncVirginiaCandidateFinanceFn).toHaveBeenCalledWith(
      expect.objectContaining({
        candidateId: CANDIDATE_ID,
        electionId: ELECTION_ID,
        candidateName: "Abigail Spanberger",
        electionYear: 2025,
        officeName: "Governor",
        district: null,
        committeeId: "60e10dc7-c59e-4a79-afca-e688c1efed65",
        committeeCode: "CC-23-02436",
        committeeName: "Spanberger for Governor",
        linkSource: "cfreports_search",
        sourceUrl: SOURCE_URL,
        contributions: [contribution()],
        contributionSourceUrl: SOURCE_URL,
        dryRun: false,
        now: NOW,
      })
    );
  });

  it("reuses fetched report data when multiple due rows share a committee", async () => {
    const db = {
      query: vi.fn(async () => ({
        rows: [
          dueRow({ total_due_rows: "2" }),
          dueRow({
            candidate_id: "33333333-3333-4333-8333-333333333333",
            election_id: "44444444-4444-4444-8444-444444444444",
            candidate_name: "Abigail Spanberger",
            total_due_rows: "2",
          }),
        ],
        rowCount: 2,
      })),
    };
    const successfulSync = {
      candidateId: CANDIDATE_ID,
      electionId: ELECTION_ID,
      electionYear: 2025,
      dryRun: false,
      linkWritten: true,
      summaryWritten: true,
      directBreakdownsWritten: 2,
      totalReceipts: 250,
      directContributionTotal: 250,
      matchedContributionRowCount: 1,
      includedContributionRowCount: 1,
      skippedContributionRowCount: 0,
    };
    const loadReportDataForCommittee = vi.fn().mockResolvedValue(reportData());
    const syncVirginiaCandidateFinanceFn = vi.fn().mockResolvedValue(successfulSync);

    const result = await syncDueVirginiaCandidateFinance({
      db,
      now: NOW,
      autoLinkMissingLinks: false,
      loadReportDataForCommittee,
      syncVirginiaCandidateFinanceFn: syncVirginiaCandidateFinanceFn as never,
    });

    expect(result).toMatchObject({
      selectedCandidateCount: 2,
      syncedCandidateCount: 2,
      failedCandidateCount: 0,
    });
    expect(loadReportDataForCommittee).toHaveBeenCalledTimes(1);
    expect(loadReportDataForCommittee).toHaveBeenCalledWith({
      committeeId: "60e10dc7-c59e-4a79-afca-e688c1efed65",
      clientOptions: undefined,
    });
    expect(syncVirginiaCandidateFinanceFn).toHaveBeenCalledTimes(2);
  });

  it("loads only scheduled report XML and ignores large-contribution report IDs", async () => {
    const fetchImpl = vi
      .fn()
      .mockResolvedValueOnce(
        new Response(
          `
            <h2 title="Reports for Jane Doe for Governor (CC-25-00001)">Reports for Jane Doe for Governor (CC-25-00001)</h2>
            <div class="pagetabs" id="ScheduledReports">
              <a href="/Report/Index/1001">View Report</a>
            </div>
            <div class="pagetabs" id="LargeContributionReports">
              <a href="/Report/Index/9001">View Report</a>
            </div>
          `,
          { status: 200, statusText: "OK" }
        )
      )
      .mockResolvedValueOnce(
        new Response(
          `<?xml version="1.0" encoding="utf-8"?>
            <Report>
              <ScheduleA>
                <LiA>
                  <Contributor IsIndividual="true">
                    <FirstName>Jane</FirstName>
                    <LastName>Voter</LastName>
                    <OccupationOrTypeOfBusiness>Teacher</OccupationOrTypeOfBusiness>
                  </Contributor>
                  <TransactionDate>2025-09-01</TransactionDate>
                  <Amount>100.00</Amount>
                </LiA>
              </ScheduleA>
            </Report>`,
          { status: 200, statusText: "OK" }
        )
      ) as unknown as typeof fetch;

    await expect(
      loadVirginiaReportDataForCommittee({
        committeeId: "60e10dc7-c59e-4a79-afca-e688c1efed65",
        clientOptions: { fetchImpl, timeoutMs: 1000 },
      })
    ).resolves.toMatchObject({
      scheduledReportCount: 1,
      contributions: [expect.objectContaining({ contributorName: "Jane Voter", amount: 100 })],
    });

    expect(vi.mocked(fetchImpl)).toHaveBeenCalledTimes(2);
    expect(String(vi.mocked(fetchImpl).mock.calls[1]?.[0])).toBe(
      "https://cfreports.elections.virginia.gov/Report/ReportXML/1001"
    );
    expect(vi.mocked(fetchImpl).mock.calls.map((call) => String(call[0])).join("\n")).not.toContain("9001");
  });

  it("auto-links missing eligible candidate elections before selecting due links", async () => {
    const db = {
      query: vi.fn(async (sql: string) => {
        const text = String(sql);
        if (text.includes("INSERT INTO public.va_candidate_finance_links")) {
          return { rows: [{ id: "link-1" }], rowCount: 1 };
        }
        if (text.includes("FROM public.candidate_elections AS candidate_election") && !text.includes("WITH due AS")) {
          return {
            rows: [
              {
                candidate_id: CANDIDATE_ID,
                election_id: ELECTION_ID,
                candidate_name: "Abigail Spanberger",
                election_year: 2025,
                office_scope: "statewide",
                office_name: "Governor",
                district: null,
              },
            ],
            rowCount: 1,
          };
        }
        if (text.includes("FROM public.va_candidate_finance_links AS link")) {
          return { rows: [], rowCount: 0 };
        }
        return { rows: [], rowCount: 0 };
      }),
    };
    const resolveCandidateCommittee = vi.fn(async () => matchedResolution());

    const result = await syncDueVirginiaCandidateFinance({
      db,
      now: NOW,
      resolveCandidateCommittee,
      syncVirginiaCandidateFinanceFn: vi.fn() as never,
    });

    expect(result).toMatchObject({
      autoLinkAttemptedCount: 1,
      autoLinkLinkedCount: 1,
      selectedCandidateCount: 0,
    });
    expect(resolveCandidateCommittee).toHaveBeenCalledTimes(1);
    expect(String(db.query.mock.calls[1]?.[0])).toContain("INSERT INTO public.va_candidate_finance_links");
  });

  it("does not auto-link in dry-run mode", async () => {
    const db = {
      query: vi.fn(async (sql: string) => {
        const text = String(sql);
        if (text.includes("FROM public.va_candidate_finance_links AS link")) {
          return { rows: [], rowCount: 0 };
        }
        throw new Error("unexpected query");
      }),
    };

    const result = await syncDueVirginiaCandidateFinance({
      db,
      now: NOW,
      dryRun: true,
      syncVirginiaCandidateFinanceFn: vi.fn() as never,
    });

    expect(result).toMatchObject({
      dryRun: true,
      autoLinkAttemptedCount: 0,
      autoLinkLinkedCount: 0,
      selectedCandidateCount: 0,
    });
    expect(db.query).toHaveBeenCalledTimes(1);
  });
});
