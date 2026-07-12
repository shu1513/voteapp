import { afterEach, describe, expect, it, vi } from "vitest";

import { migrationTableColumns } from "../../helpers/migrationTableColumns.js";
import { loadTexasCandidateFinanceSummariesByCandidateElection } from "../../../src/pipeline/texasFinance/texasBallotLookupFinanceLoader.js";

afterEach(() => {
  vi.unstubAllEnvs();
});

const CANDIDATE_ID = "11111111-1111-4111-8111-111111111111";
const ELECTION_ID = "22222222-2222-4222-8222-222222222222";

describe("texasBallotLookupFinanceLoader", () => {
  it.each(["Mayor", "Municipal Controller", "City Council Member"])(
    "does not query Texas TEC tables for Houston local finance office %s",
    async (officeCanonicalName) => {
    vi.stubEnv("TEXAS_CAMPAIGN_FINANCE_ENABLED", "true");
    const query = vi.fn();

    const result = await loadTexasCandidateFinanceSummariesByCandidateElection(
      { query },
      [{ candidate_id: CANDIDATE_ID, election_id: ELECTION_ID }],
      [{ election_id: ELECTION_ID, state: "TX", office_scope: "place", office_canonical_name: officeCanonicalName }]
    );

    expect(result.size).toBe(0);
    expect(query).not.toHaveBeenCalled();
    }
  );

  // Regression guard for the schema drift where the outside-groups query
  // selected outside_group.expenditure_count, a column no migration ever
  // created for Texas (only the Tennessee table has it). Unit tests mock
  // db.query, so this cross-checks every column the query references
  // against the columns the migrations actually build.
  it("only references outside-group columns that the migrations create", async () => {
    vi.stubEnv("TEXAS_CAMPAIGN_FINANCE_ENABLED", "true");
    const queries: string[] = [];
    const query = vi.fn(async (sql: string) => {
      queries.push(sql);
      if (queries.length === 1) {
        return { rows: [{ candidate_id: CANDIDATE_ID, election_id: ELECTION_ID }] };
      }
      return { rows: [] };
    });

    await loadTexasCandidateFinanceSummariesByCandidateElection(
      { query },
      [{ candidate_id: CANDIDATE_ID, election_id: ELECTION_ID }],
      [{ election_id: ELECTION_ID, state: "TX", office_scope: "statewide", office_canonical_name: "Governor" }]
    );

    const outsideGroupSql = queries.find((sql) => sql.includes("tx_candidate_finance_outside_groups"));
    expect(outsideGroupSql).toBeDefined();

    const referencedColumns = new Set(
      [...outsideGroupSql!.matchAll(/outside_group\.([a-z_]+)/g)].map((match) => match[1])
    );
    expect(referencedColumns.size).toBeGreaterThan(0);

    const schemaColumns = migrationTableColumns("tx_candidate_finance_outside_groups");
    expect(schemaColumns.size).toBeGreaterThan(0);
    for (const column of referencedColumns) {
      expect(
        schemaColumns.has(column),
        `outside_group.${column} is not a column of tx_candidate_finance_outside_groups`
      ).toBe(true);
    }
  });
});
