import { readFileSync } from "node:fs";

import { afterEach, describe, expect, it, vi } from "vitest";

import { loadCaliforniaCandidateFinanceSummariesByCandidateElection } from "../../../src/pipeline/californiaFinance/californiaBallotLookupFinanceLoader.js";

const MIGRATION_SQL = readFileSync(
  new URL("../../../../db/migrations/111_add_california_campaign_finance_tables.sql", import.meta.url),
  "utf8"
);

afterEach(() => {
  vi.unstubAllEnvs();
});

const CANDIDATE_ID = "11111111-1111-4111-8111-111111111111";
const ELECTION_ID = "22222222-2222-4222-8222-222222222222";

function migrationTableColumns(tableName: string): Set<string> {
  const start = MIGRATION_SQL.indexOf(`CREATE TABLE IF NOT EXISTS public.${tableName} (`);
  expect(start).toBeGreaterThanOrEqual(0);
  const block = MIGRATION_SQL.slice(start, MIGRATION_SQL.indexOf("\n);", start));
  const columns = new Set<string>();
  for (const line of block.split("\n").slice(1)) {
    const match = /^ {2}([a-z_]+) /.exec(line);
    if (match) {
      columns.add(match[1]);
    }
  }
  expect(columns.size).toBeGreaterThan(0);
  return columns;
}

describe("californiaBallotLookupFinanceLoader", () => {
  // Regression guard for the schema drift where the outside-groups query
  // selected outside_group.expenditure_count, a column migration 111 never
  // created (only the Tennessee table has it). Unit tests mock db.query, so
  // this cross-checks every column the query references against the actual
  // CREATE TABLE statement.
  it("only references outside-group columns that migration 111 creates", async () => {
    vi.stubEnv("CALIFORNIA_CAMPAIGN_FINANCE_ENABLED", "true");
    const queries: string[] = [];
    const query = vi.fn(async (sql: string) => {
      queries.push(sql);
      if (queries.length === 1) {
        return { rows: [{ candidate_id: CANDIDATE_ID, election_id: ELECTION_ID }] };
      }
      return { rows: [] };
    });

    await loadCaliforniaCandidateFinanceSummariesByCandidateElection(
      { query },
      [{ candidate_id: CANDIDATE_ID, election_id: ELECTION_ID }],
      [{ election_id: ELECTION_ID, state: "CA", office_scope: "statewide", office_canonical_name: "Governor" }]
    );

    const outsideGroupSql = queries.find((sql) => sql.includes("ca_candidate_finance_outside_groups"));
    expect(outsideGroupSql).toBeDefined();

    const referencedColumns = new Set(
      [...outsideGroupSql!.matchAll(/outside_group\.([a-z_]+)/g)].map((match) => match[1])
    );
    expect(referencedColumns.size).toBeGreaterThan(0);

    const schemaColumns = migrationTableColumns("ca_candidate_finance_outside_groups");
    for (const column of referencedColumns) {
      expect(
        schemaColumns.has(column),
        `outside_group.${column} is not a column of ca_candidate_finance_outside_groups`
      ).toBe(true);
    }
  });
});
