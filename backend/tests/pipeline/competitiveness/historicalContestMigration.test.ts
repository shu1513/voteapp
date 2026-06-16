import { readFileSync } from "node:fs";

import { describe, expect, it } from "vitest";

import { HISTORICAL_CONTEST_OFFICE_TYPES } from "../../../src/pipeline/competitiveness/historicalContestKeys.js";

const MIGRATION_108_SQL = readFileSync(
  new URL("../../../../db/migrations/108_expand_historical_contest_margin_constraints.sql", import.meta.url),
  "utf8"
);

describe("historical contest margin migrations", () => {
  it("keeps the expanded DB office constraint aligned with supported office types", () => {
    for (const officeType of HISTORICAL_CONTEST_OFFICE_TYPES) {
      expect(MIGRATION_108_SQL).toContain(`'${officeType}'`);
    }
  });

  it("allows county historical contest margin districts in the expanded DB constraint", () => {
    expect(MIGRATION_108_SQL).toContain(
      "CHECK (district_type IN ('statewide', 'us_house', 'state_upper', 'state_lower', 'county'))"
    );
  });
});
