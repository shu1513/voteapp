import { describe, expect, it, vi } from "vitest";

import {
  createStandardStateFinanceDueListQuery,
  type StandardStateFinanceDueQueryRow,
} from "../../../src/pipeline/finance/standardStateFinanceDueListQuery.js";
import { listDueTexasCandidateFinanceSyncRows } from "../../../src/pipeline/texasFinance/texasCandidateFinanceBatchSync.js";
import { TEXAS_FINANCE_ELIGIBLE_OFFICE_KEYS } from "../../../src/pipeline/texasFinance/texasFinanceEligibleOffices.js";

const CANDIDATE_ID = "11111111-1111-4111-8111-111111111111";
const ELECTION_ID = "22222222-2222-4222-8222-222222222222";

const DUE_LIST_INPUT = {
  now: new Date("2026-06-01T00:00:00.000Z"),
  staleAfterDays: 7,
  maxCandidates: 25,
  electionLookbackDays: 30,
  electionLookaheadDays: 730,
};

function createMockDb(rows: unknown[] = []) {
  return {
    query: vi.fn().mockResolvedValue({ rows }),
  };
}

function dueRow(overrides: Record<string, unknown> = {}) {
  return {
    candidate_id: CANDIDATE_ID,
    election_id: ELECTION_ID,
    candidate_name: "Jane Doe",
    election_year: 2026,
    office_scope: "statewide",
    office_name: "Governor",
    district: null,
    committee_id: "C-100",
    committee_name: "Friends of Jane Doe",
    source_url: "https://example.gov/committee/C-100",
    last_synced_at: null,
    total_due_rows: "1",
    ...overrides,
  };
}

function normalizeSql(sql: string): string {
  return sql.replace(/\s+/g, " ").trim();
}

function createCanonicalQuery() {
  return createStandardStateFinanceDueListQuery({
    state: "TX",
    tables: {
      links: "tx_candidate_finance_links",
      summaries: "tx_candidate_finance_summaries",
    },
    eligibleOfficeKeys: TEXAS_FINANCE_ELIGIBLE_OFFICE_KEYS,
  });
}

describe("createStandardStateFinanceDueListQuery", () => {
  it("issues the canonical due-list query and maps rows to camelCase", async () => {
    const db = createMockDb([dueRow()]);
    const listDueRows = createCanonicalQuery();

    const result = await listDueRows(db, DUE_LIST_INPUT);

    expect(result).toEqual({
      totalDueRows: 1,
      rows: [
        {
          candidateId: CANDIDATE_ID,
          electionId: ELECTION_ID,
          candidateName: "Jane Doe",
          electionYear: 2026,
          officeScope: "statewide",
          officeName: "Governor",
          district: null,
          committeeId: "C-100",
          committeeName: "Friends of Jane Doe",
          sourceUrl: "https://example.gov/committee/C-100",
          lastSyncedAt: null,
        },
      ],
    });

    const sql = String(db.query.mock.calls[0]?.[0]);
    expect(sql).toContain("FROM public.tx_candidate_finance_links AS link");
    expect(sql).toContain("LEFT JOIN public.tx_candidate_finance_summaries AS summary");
    expect(sql).toContain("link.link_status = 'active'");
    expect(sql).toContain("candidate.deleted_at IS NULL");
    expect(sql).toContain("district.state = 'TX'");
    expect(sql).toContain("election.race_type = 'office'");
    expect(sql).toContain(
      "election.election_date >= (($1::timestamptz AT TIME ZONE 'UTC')::date - make_interval(days => $4::int))"
    );
    expect(sql).toContain(
      "election.election_date <= (($1::timestamptz AT TIME ZONE 'UTC')::date + make_interval(days => $5::int))"
    );
    expect(sql).toContain("candidate_election.status NOT IN ('withdrawn', 'lost')");
    expect(sql).toContain("(office.scope || '::' || office.canonical_name) = ANY($6::text[])");
    expect(sql).toContain("OR summary.last_synced_at < ($1::timestamptz - make_interval(days => $2::int))");
    expect(sql).toContain("LIMIT $3::int");
    expect(normalizeSql(sql)).toContain(
      "ORDER BY summary.last_synced_at ASC NULLS FIRST, election.election_date ASC, link.candidate_name_normalized ASC, link.id ASC"
    );
    expect(db.query.mock.calls[0]?.[1]).toEqual([
      "2026-06-01T00:00:00.000Z",
      7,
      25,
      30,
      730,
      [...TEXAS_FINANCE_ELIGIBLE_OFFICE_KEYS],
    ]);
  });

  it("matches the bespoke Texas due-list query, parameters, and mapping", async () => {
    const rows = [
      dueRow(),
      dueRow({
        candidate_id: "33333333-3333-4333-8333-333333333333",
        candidate_name: "John Roe",
        district: "District 7",
        last_synced_at: "2026-05-01T00:00:00.000Z",
        total_due_rows: "2",
      }),
    ];
    const bespokeDb = createMockDb(rows.map((row) => ({ ...row })));
    const builderDb = createMockDb(rows.map((row) => ({ ...row })));
    const listDueRows = createCanonicalQuery();

    const bespokeResult = await listDueTexasCandidateFinanceSyncRows(bespokeDb, DUE_LIST_INPUT);
    const builderResult = await listDueRows(builderDb, DUE_LIST_INPUT);

    expect(normalizeSql(String(builderDb.query.mock.calls[0]?.[0]))).toBe(
      normalizeSql(String(bespokeDb.query.mock.calls[0]?.[0]))
    );
    expect(builderDb.query.mock.calls[0]?.[1]).toEqual(bespokeDb.query.mock.calls[0]?.[1]);
    expect(builderResult).toEqual(bespokeResult);
  });

  it("selects overridden link columns and maps rows through the provided mapRow", async () => {
    const db = createMockDb([
      dueRow({
        committee_id: undefined,
        committee_name: undefined,
        committee_key: "PCC 123",
        link_source: "ocf_export",
      }),
    ]);
    const listDueRows = createStandardStateFinanceDueListQuery({
      state: "DC",
      tables: {
        links: "dc_candidate_finance_links",
        summaries: "dc_candidate_finance_summaries",
      },
      eligibleOfficeKeys: ["citywide::Mayor"],
      linkColumns: ["committee_key", "committee_name", "link_source"],
      mapRow: (row: StandardStateFinanceDueQueryRow) => ({
        candidateId: row.candidate_id,
        committeeKey: row.committee_key as string,
        committeeName: row.committee_name as string,
        linkSource: row.link_source as string,
      }),
    });

    const result = await listDueRows(db, DUE_LIST_INPUT);

    expect(result).toEqual({
      totalDueRows: 1,
      rows: [
        {
          candidateId: CANDIDATE_ID,
          committeeKey: "PCC 123",
          committeeName: undefined,
          linkSource: "ocf_export",
        },
      ],
    });

    const sql = String(db.query.mock.calls[0]?.[0]);
    expect(sql).toContain("link.committee_key,");
    expect(sql).toContain("link.link_source,");
    expect(sql).not.toContain("link.committee_id");
    expect(sql).toContain("district.state = 'DC'");
    expect(sql).toContain("FROM public.dc_candidate_finance_links AS link");
    // Canonical columns stay in place around the overridden identity columns.
    expect(normalizeSql(sql)).toContain("link.district, link.committee_key, link.committee_name, link.link_source, link.source_url,");
  });

  it("returns zero totals for an empty result and tolerates malformed counts", async () => {
    const listDueRows = createCanonicalQuery();

    const emptyResult = await listDueRows(createMockDb(), DUE_LIST_INPUT);
    expect(emptyResult).toEqual({ rows: [], totalDueRows: 0 });

    const malformedResult = await listDueRows(
      createMockDb([dueRow({ total_due_rows: "not-a-number" })]),
      DUE_LIST_INPUT
    );
    expect(malformedResult.totalDueRows).toBe(0);

    const numericResult = await listDueRows(createMockDb([dueRow({ total_due_rows: 41 })]), DUE_LIST_INPUT);
    expect(numericResult.totalDueRows).toBe(41);
  });

  it("passes a fresh eligible-office array on every call", async () => {
    const db = createMockDb();
    const listDueRows = createCanonicalQuery();

    await listDueRows(db, DUE_LIST_INPUT);
    await listDueRows(db, DUE_LIST_INPUT);

    const firstKeys = db.query.mock.calls[0]?.[1]?.[5];
    const secondKeys = db.query.mock.calls[1]?.[1]?.[5];
    expect(firstKeys).toEqual([...TEXAS_FINANCE_ELIGIBLE_OFFICE_KEYS]);
    expect(secondKeys).toEqual(firstKeys);
    expect(secondKeys).not.toBe(firstKeys);
  });

  it("rejects invalid configuration at construction", () => {
    const base = {
      state: "TX",
      tables: {
        links: "tx_candidate_finance_links",
        summaries: "tx_candidate_finance_summaries",
      },
      eligibleOfficeKeys: ["statewide::Governor"],
    };
    const mapRow = (row: StandardStateFinanceDueQueryRow) => row;

    expect(() => createStandardStateFinanceDueListQuery({ ...base, state: "Texas" })).toThrow(
      "Invalid standard finance due-list state: Texas"
    );
    expect(() => createStandardStateFinanceDueListQuery({ ...base, state: "tx" })).toThrow(
      "Invalid standard finance due-list state: tx"
    );
    expect(() =>
      createStandardStateFinanceDueListQuery({
        ...base,
        tables: { ...base.tables, links: "tx_links; DROP TABLE" },
      })
    ).toThrow("Invalid standard finance table identifier: tx_links; DROP TABLE");
    expect(() =>
      createStandardStateFinanceDueListQuery({
        ...base,
        tables: { ...base.tables, summaries: "Bad-Name" },
      })
    ).toThrow("Invalid standard finance table identifier: Bad-Name");
    expect(() =>
      createStandardStateFinanceDueListQuery({ ...base, linkColumns: ["committee_id, committee_name"], mapRow })
    ).toThrow("Invalid standard finance due-list link column: committee_id, committee_name");
    expect(() => createStandardStateFinanceDueListQuery({ ...base, linkColumns: [], mapRow })).toThrow(
      "Standard finance due-list link columns must not be empty"
    );
    expect(() =>
      createStandardStateFinanceDueListQuery({
        ...base,
        linkColumns: ["committee_key"],
      } as Parameters<typeof createStandardStateFinanceDueListQuery>[0])
    ).toThrow("Standard finance due-list linkColumns require a mapRow");
    expect(() => createStandardStateFinanceDueListQuery({ ...base, eligibleOfficeKeys: [] })).toThrow(
      "Standard finance due-list eligible office keys must not be empty"
    );
  });
});
