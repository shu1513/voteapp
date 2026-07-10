import { describe, expect, it } from "vitest";

import { migrationTableColumns } from "./migrationTableColumns.js";

describe("migrationTableColumns", () => {
  it("reads columns from a CREATE TABLE statement", () => {
    const columns = migrationTableColumns("ca_candidate_finance_outside_groups");
    expect(columns.has("committee_id")).toBe(true);
    expect(columns.has("amount")).toBe(true);
    // The schema drift this helper exists to catch: only the Tennessee
    // outside-groups table has expenditure_count (migration 125).
    expect(columns.has("expenditure_count")).toBe(false);
    expect(migrationTableColumns("tn_candidate_finance_outside_groups").has("expenditure_count")).toBe(true);
  });

  it("applies ALTER TABLE ADD COLUMN from later migrations", () => {
    // elections is created in 001 and gains is_partisan in 046.
    expect(migrationTableColumns("elections").has("is_partisan")).toBe(true);
  });

  it("applies ALTER TABLE DROP COLUMN from later migrations", () => {
    // propositions drops yes_percentage/no_percentage in 034.
    const columns = migrationTableColumns("propositions");
    expect(columns.size).toBeGreaterThan(0);
    expect(columns.has("yes_percentage")).toBe(false);
    expect(columns.has("no_percentage")).toBe(false);
  });

  it("returns an empty set for an unknown table", () => {
    expect(migrationTableColumns("no_such_table").size).toBe(0);
  });
});
