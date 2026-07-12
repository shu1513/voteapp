import { afterEach, describe, expect, it, vi } from "vitest";
import { migrationTableColumns } from "../../helpers/migrationTableColumns.js";
import { loadHoustonCandidateFinanceSummariesByCandidateElection } from "../../../src/pipeline/houstonFinance/houstonBallotLookupFinanceLoader.js";

afterEach(() => vi.unstubAllEnvs());
const CANDIDATE_ID = "11111111-1111-4111-8111-111111111111";
const ELECTION_ID = "22222222-2222-4222-8222-222222222222";
const election = { election_id: ELECTION_ID, state: "TX", district_type: "place", geoid_compact: "4835000", office_scope: "place", office_canonical_name: "Mayor" };

describe("Houston ballot finance loader", () => {
  it("queries standard Houston tables only for exact Houston Mayor elections", async () => {
    vi.stubEnv("HOUSTON_CAMPAIGN_FINANCE_ENABLED", "true");
    const queries: string[] = [];
    const query = vi.fn(async (sql: string) => { queries.push(sql); return { rows: queries.length === 1 ? [{ candidate_id: CANDIDATE_ID, election_id: ELECTION_ID }] : [] }; });
    await loadHoustonCandidateFinanceSummariesByCandidateElection({ query }, [{ candidate_id: CANDIDATE_ID, election_id: ELECTION_ID }], [election]);
    const outsideSql = queries.find((sql) => sql.includes("hou_candidate_finance_outside_groups"));
    expect(outsideSql).toBeDefined();
    const schema = migrationTableColumns("hou_candidate_finance_outside_groups");
    for (const column of [...outsideSql!.matchAll(/outside_group\.([a-z_]+)/g)].map((match) => match[1])) expect(schema.has(column!)).toBe(true);
  });

  it("does not query for another Texas city", async () => {
    vi.stubEnv("HOUSTON_CAMPAIGN_FINANCE_ENABLED", "true");
    const query = vi.fn();
    await loadHoustonCandidateFinanceSummariesByCandidateElection({ query }, [{ candidate_id: CANDIDATE_ID, election_id: ELECTION_ID }], [{ ...election, geoid_compact: "4819000" }]);
    expect(query).not.toHaveBeenCalled();
  });
});
