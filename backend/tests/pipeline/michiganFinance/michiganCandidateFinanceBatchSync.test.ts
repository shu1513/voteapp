import { describe, expect, it, vi } from "vitest";

import {
  listDueMichiganCandidateFinanceSyncRows,
  syncDueMichiganCandidateFinance,
} from "../../../src/pipeline/michiganFinance/michiganCandidateFinanceBatchSync.js";

import { buildMitnExportXlsx } from "./mitnXlsxTestFixture.js";

const CANDIDATE_ID = "11111111-1111-4111-8111-111111111111";
const ELECTION_ID = "22222222-2222-4222-8222-222222222222";
const SOURCE_URL = "https://mi-boe.entellitrak.com/etk-mi-boe-prod/page.request.do?page=page.miboeContributionPublicSearch";

function createMockDb(rows: unknown[] = []) {
  return {
    query: vi.fn().mockResolvedValue({ rows }),
  };
}

function dueRow(overrides: Record<string, unknown> = {}) {
  return {
    candidate_id: CANDIDATE_ID,
    election_id: ELECTION_ID,
    candidate_name: "Aric Nesbitt",
    election_year: 2026,
    office_scope: "statewide",
    office_name: "Governor",
    district: null,
    committee_id: "521877",
    committee_name: "ARIC NESBITT FOR GOVERNOR",
    source_url: SOURCE_URL,
    last_synced_at: null,
    total_due_rows: "1",
    ...overrides,
  };
}

describe("michiganCandidateFinanceBatchSync", () => {
  it("lists due Michigan finance sync rows from explicit active links", async () => {
    const db = createMockDb([dueRow()]);

    const result = await listDueMichiganCandidateFinanceSyncRows(db, {
      now: new Date("2026-07-26T00:00:00.000Z"),
      staleAfterDays: 7,
      maxCandidates: 25,
      electionLookbackDays: 30,
      electionLookaheadDays: 730,
    });

    expect(result).toEqual({
      totalDueRows: 1,
      rows: [
        {
          candidateId: CANDIDATE_ID,
          electionId: ELECTION_ID,
          candidateName: "Aric Nesbitt",
          electionYear: 2026,
          officeScope: "statewide",
          officeName: "Governor",
          district: null,
          committeeId: "521877",
          committeeName: "ARIC NESBITT FOR GOVERNOR",
          sourceUrl: SOURCE_URL,
          lastSyncedAt: null,
        },
      ],
    });

    const sql = String(db.query.mock.calls[0]?.[0]);
    expect(sql).toContain("FROM public.mi_candidate_finance_links AS link");
    expect(sql).toContain("link.link_status = 'active'");
    expect(sql).toContain("district.state = 'MI'");
    expect(sql).toContain("election.race_type = 'office'");
    expect(sql).toContain("candidate_election.status NOT IN ('withdrawn', 'lost')");
    expect(sql).toContain("(office.scope || '::' || office.canonical_name) = ANY($6::text[])");
    expect(db.query.mock.calls[0]?.[1]).toEqual([
      "2026-07-26T00:00:00.000Z",
      7,
      25,
      30,
      730,
      expect.arrayContaining(["statewide::Governor", "state_upper::State Senator"]),
      2025,
    ]);
  });

  it("uses a one-day post-election grace window by default for due selection", async () => {
    const db = createMockDb();

    await syncDueMichiganCandidateFinance({
      db,
      syncMichiganCandidateFinanceFn: vi.fn(),
      now: new Date("2026-07-26T00:00:00.000Z"),
      autoLinkMissingLinks: false,
    });

    expect(String(db.query.mock.calls[0]?.[0])).toContain(
      "election.election_date >= ($1::date - make_interval(days => $4::int))"
    );
    expect(db.query.mock.calls[0]?.[1]).toEqual([
      "2026-07-26T00:00:00.000Z",
      7,
      25,
      1,
      730,
      expect.arrayContaining(["statewide::Governor", "state_upper::State Senator"]),
      2025,
    ]);
  });

  it("auto-links and syncs through the MiTN public search", async () => {
    vi.stubEnv("MICHIGAN_CAMPAIGN_FINANCE_ENABLED", "true");
    vi.stubEnv("MICHIGAN_MITN_RAW_DATA_REFRESH_ENABLED", "true");
    try {
      const exportXlsx = buildMitnExportXlsx([
        [
          "1",
          "26-1",
          "C",
          "Campaign Statements",
          "2026",
          "July CS",
          "ARIC NESBITT FOR GOVERNOR",
          "521877",
          "Candidate",
          "ARIC",
          "NESBITT",
          "Direct Contributions",
          "RAYMOND",
          "KOUZA",
          "RETIRED",
          "",
          "06/24/2026",
          "1000.00",
          "1000.00",
        ],
      ]);
      const fetchFn = vi.fn(async (url: string) => {
        if (url.includes("page.miboeCommitteePublicSearch")) {
          return {
            ok: true,
            status: 200,
            text: async () =>
              `<table><tr><td>521877</td><td>Candidate</td><td>ARIC NESBITT FOR GOVERNOR</td><td>Active</td></tr></table>`,
            arrayBuffer: async () => new ArrayBuffer(0),
          };
        }
        return {
          ok: true,
          status: 200,
          text: async () => "",
          arrayBuffer: async () =>
            exportXlsx.buffer.slice(exportXlsx.byteOffset, exportXlsx.byteOffset + exportXlsx.byteLength),
        };
      });

      const db = {
        query: vi.fn(async (sql: string) => {
          const text = String(sql);
          if (text.includes("FROM public.candidate_elections AS candidate_election")) {
            return {
              rows: [
                {
                  candidate_id: CANDIDATE_ID,
                  election_id: ELECTION_ID,
                  candidate_name: "Aric Nesbitt",
                  election_year: 2026,
                  office_scope: "statewide",
                  office_name: "Governor",
                  district: null,
                },
              ],
              rowCount: 1,
            };
          }
          if (text.includes("INSERT INTO public.mi_candidate_finance_links")) {
            return { rows: [{ id: "link-1" }], rowCount: 1 };
          }
          if (text.includes("FROM public.mi_candidate_finance_links AS link")) {
            return {
              rows: [
                dueRow({
                  candidate_name: "Aric Nesbitt",
                  election_year: 2026,
                  committee_id: "521877",
                  committee_name: "ARIC NESBITT FOR GOVERNOR",
                  source_url: null,
                }),
              ],
              rowCount: 1,
            };
          }
          throw new Error(`Unexpected query: ${text}`);
        }),
      };
      const syncMichiganCandidateFinanceFn = vi.fn().mockResolvedValue({ ok: true });

      const result = await syncDueMichiganCandidateFinance({
        db,
        syncMichiganCandidateFinanceFn,
        mitnPublicSearchFetchFn: fetchFn,
        now: new Date("2026-07-26T00:00:00.000Z"),
      });

      // auto-link wrote the public-search link
      const insertCall = db.query.mock.calls.find((call) =>
        String(call[0]).includes("INSERT INTO public.mi_candidate_finance_links")
      );
      expect(insertCall?.[1]).toContain("mitn_public_search");

      // the sync consumed export-mapped rows with a trusted committee and no expenditures
      expect(syncMichiganCandidateFinanceFn).toHaveBeenCalledWith(
        expect.objectContaining({
          electionYear: 2026,
          trustedCommittee: expect.objectContaining({ committeeId: "521877" }),
          linkSource: "mitn_public_search",
          // one export per cycle statement year (2025 + 2026), mapped rows
          contributionRows: expect.arrayContaining([
            expect.objectContaining({
              cfr_com_id: "521877",
              com_type: "CAN",
              amount: "1000.00",
              received_date: "06/24/2026",
            }),
          ]),
        })
      );
      // expenditureRows must be OMITTED — a defined array (even empty) marks
      // outside data available and would persist $0 totals
      expect("expenditureRows" in syncMichiganCandidateFinanceFn.mock.calls[0]![0]).toBe(false);
      const exportYears = fetchFn.mock.calls
        .filter(([url]) => String(url).includes("action=export"))
        .map(([, init]) => new URLSearchParams((init as { body: string }).body).get("form.campaignStatementYear"));
      expect(exportYears.sort()).toEqual(["69", "76"]);
      expect(result.syncedCandidateCount).toBe(1);
    } finally {
      vi.unstubAllEnvs();
    }
  });

  it("fails loudly when a cycle statement-year id is unknown instead of writing zeros", async () => {
    vi.stubEnv("MICHIGAN_CAMPAIGN_FINANCE_ENABLED", "true");
    vi.stubEnv("MICHIGAN_MITN_RAW_DATA_REFRESH_ENABLED", "true");
    try {
      const db = {
        query: vi.fn(async (sql: string) => {
          const text = String(sql);
          if (text.includes("FROM public.candidate_elections AS candidate_election")) {
            return { rows: [], rowCount: 0 };
          }
          if (text.includes("FROM public.mi_candidate_finance_links AS link")) {
            return {
              rows: [dueRow({ election_year: 2028, committee_id: "521877", source_url: null })],
              rowCount: 1,
            };
          }
          throw new Error(`Unexpected query: ${text}`);
        }),
      };
      const syncMichiganCandidateFinanceFn = vi.fn();

      const result = await syncDueMichiganCandidateFinance({
        db,
        syncMichiganCandidateFinanceFn,
        mitnPublicSearchFetchFn: vi.fn(),
        now: new Date("2028-06-01T00:00:00.000Z"),
      });

      expect(syncMichiganCandidateFinanceFn).not.toHaveBeenCalled();
      expect(result.failedCandidateCount).toBe(1);
      expect(result.results[0]?.error).toContain("No Michigan MiTN statement-year id for 2027");
    } finally {
      vi.unstubAllEnvs();
    }
  });

  it("fails syncs loudly when the MiTN fetch flag is off", async () => {
    vi.stubEnv("MICHIGAN_MITN_RAW_DATA_REFRESH_ENABLED", "false");
    try {
      const fetchFn = vi.fn();
      const db = {
        query: vi.fn(async (sql: string) => {
          const text = String(sql);
          if (text.includes("FROM public.candidate_elections AS candidate_election")) {
            return { rows: [], rowCount: 0 };
          }
          if (text.includes("FROM public.mi_candidate_finance_links AS link")) {
            return {
              rows: [dueRow({ election_year: 2026, committee_id: "521877", source_url: null })],
              rowCount: 1,
            };
          }
          throw new Error(`Unexpected query: ${text}`);
        }),
      };
      const syncMichiganCandidateFinanceFn = vi.fn();

      const result = await syncDueMichiganCandidateFinance({
        db,
        syncMichiganCandidateFinanceFn,
        mitnPublicSearchFetchFn: fetchFn,
        now: new Date("2026-07-26T00:00:00.000Z"),
      });

      expect(fetchFn).not.toHaveBeenCalled();
      expect(syncMichiganCandidateFinanceFn).not.toHaveBeenCalled();
      expect(result.failedCandidateCount).toBe(1);
      expect(result.results[0]?.error).toContain("MICHIGAN_MITN_RAW_DATA_REFRESH_ENABLED");
    } finally {
      vi.unstubAllEnvs();
    }
  });

  it("refuses pre-MiTN election years now that the archive path is removed", async () => {
    vi.stubEnv("MICHIGAN_CAMPAIGN_FINANCE_ENABLED", "true");
    vi.stubEnv("MICHIGAN_MITN_RAW_DATA_REFRESH_ENABLED", "true");
    try {
      const fetchFn = vi.fn();
      const db = {
        query: vi.fn(async (sql: string) => {
          const text = String(sql);
          if (text.includes("FROM public.candidate_elections AS candidate_election")) {
            return {
              rows: [
                {
                  candidate_id: CANDIDATE_ID,
                  election_id: ELECTION_ID,
                  candidate_name: "Gretchen Whitmer",
                  election_year: 2022,
                  office_scope: "statewide",
                  office_name: "Governor",
                  district: null,
                },
              ],
              rowCount: 1,
            };
          }
          if (text.includes("FROM public.mi_candidate_finance_links AS link")) {
            return {
              rows: [dueRow({ candidate_name: "Gretchen Whitmer", election_year: 2022, committee_id: "514456" })],
              rowCount: 1,
            };
          }
          throw new Error(`Unexpected query: ${text}`);
        }),
      };
      const syncMichiganCandidateFinanceFn = vi.fn();

      const result = await syncDueMichiganCandidateFinance({
        db,
        syncMichiganCandidateFinanceFn,
        mitnPublicSearchFetchFn: fetchFn,
        now: new Date("2022-06-01T00:00:00.000Z"),
      });

      // no MiTN network call, no link insert, no sync — a loud refusal instead
      expect(fetchFn).not.toHaveBeenCalled();
      expect(syncMichiganCandidateFinanceFn).not.toHaveBeenCalled();
      expect(
        db.query.mock.calls.map((call) => String(call[0])).some((sql) => sql.includes("INSERT INTO"))
      ).toBe(false);
      expect(result.failedCandidateCount).toBe(1);
      expect(result.results[0]?.error).toContain("predates MiTN public-search coverage");
    } finally {
      vi.unstubAllEnvs();
    }
  });

  it("does not auto-link missing finance links during dry-run", async () => {
    const db = createMockDb();

    await syncDueMichiganCandidateFinance({
      db,
      dryRun: true,
      syncMichiganCandidateFinanceFn: vi.fn(),
      now: new Date("2026-07-26T00:00:00.000Z"),
    });

    // dry-run skips the auto-link block entirely: the first (and only) query
    // is the due-row listing, and nothing is inserted
    expect(db.query).toHaveBeenCalledTimes(1);
    expect(String(db.query.mock.calls[0]?.[0])).toContain("FROM public.mi_candidate_finance_links AS link");
  });

  it("validates positive integer options", async () => {
    const db = createMockDb();

    await expect(
      syncDueMichiganCandidateFinance({
        db,
        maxCandidates: 0,
        autoLinkMissingLinks: false,
      })
    ).rejects.toThrow("Invalid Michigan finance batch sync maxCandidates");
    await expect(
      syncDueMichiganCandidateFinance({
        db,
        staleAfterDays: -1,
        autoLinkMissingLinks: false,
      })
    ).rejects.toThrow("Invalid Michigan finance batch sync staleAfterDays");
  });
});
