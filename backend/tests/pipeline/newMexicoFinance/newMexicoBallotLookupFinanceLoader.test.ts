import { afterEach, describe, expect, it, vi } from "vitest";

import { migrationTableColumns } from "../../helpers/migrationTableColumns.js";
import { loadNewMexicoCandidateFinanceSummariesByCandidateElection } from "../../../src/pipeline/newMexicoFinance/newMexicoBallotLookupFinanceLoader.js";

afterEach(() => {
  vi.unstubAllEnvs();
});

const CANDIDATE_ID = "11111111-1111-4111-8111-111111111111";
const ELECTION_ID = "22222222-2222-4222-8222-222222222222";

describe("newMexicoBallotLookupFinanceLoader", () => {
  // Regression guard for the schema drift where the outside-groups query
  // selected outside_group.expenditure_count, a column no migration ever
  // created for New Mexico (only the Tennessee table has it). Unit tests
  // mock db.query, so this cross-checks every column the query references
  // against the columns the migrations actually build.
  it("only references outside-group columns that the migrations create", async () => {
    vi.stubEnv("NEW_MEXICO_CAMPAIGN_FINANCE_ENABLED", "true");
    const queries: string[] = [];
    const query = vi.fn(async (sql: string) => {
      queries.push(sql);
      if (queries.length === 1) {
        return { rows: [{ candidate_id: CANDIDATE_ID, election_id: ELECTION_ID }] };
      }
      return { rows: [] };
    });

    await loadNewMexicoCandidateFinanceSummariesByCandidateElection(
      { query },
      [{ candidate_id: CANDIDATE_ID, election_id: ELECTION_ID }],
      [{ election_id: ELECTION_ID, state: "NM", office_scope: "statewide", office_canonical_name: "Governor" }]
    );

    const outsideGroupSql = queries.find((sql) => sql.includes("nm_candidate_finance_outside_groups"));
    expect(outsideGroupSql).toBeDefined();

    const referencedColumns = new Set(
      [...outsideGroupSql!.matchAll(/outside_group\.([a-z_]+)/g)].map((match) => match[1])
    );
    expect(referencedColumns.size).toBeGreaterThan(0);

    const schemaColumns = migrationTableColumns("nm_candidate_finance_outside_groups");
    expect(schemaColumns.size).toBeGreaterThan(0);
    for (const column of referencedColumns) {
      expect(
        schemaColumns.has(column),
        `outside_group.${column} is not a column of nm_candidate_finance_outside_groups`
      ).toBe(true);
    }
  });
});
