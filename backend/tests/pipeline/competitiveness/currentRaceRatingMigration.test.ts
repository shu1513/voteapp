import { readFileSync } from "node:fs";

import { describe, expect, it } from "vitest";

import { COMPETITIVENESS_LABELS } from "../../../src/pipeline/competitiveness/competitivenessLabels.js";
import {
  CURRENT_RACE_RATING_CONFIDENCES,
  CURRENT_RACE_RATING_EVIDENCE_STATUSES,
  CURRENT_RACE_RATING_METHODS,
  CURRENT_RACE_RATING_SCHEMA_VERSION,
} from "../../../src/pipeline/competitiveness/currentRaceRatingConsensus.js";

const MIGRATION_248_SQL = readFileSync(
  new URL("../../../../db/migrations/248_add_current_race_ratings.sql", import.meta.url),
  "utf8"
);

describe("current race ratings migration", () => {
  it("keeps the DB label constraint aligned with the shared label enum", () => {
    for (const label of COMPETITIVENESS_LABELS) {
      expect(MIGRATION_248_SQL).toContain(`'${label}'`);
    }
  });

  it("keeps schema version, methods, statuses, and confidences aligned with the code enums", () => {
    expect(MIGRATION_248_SQL).toContain(`schema_version = '${CURRENT_RACE_RATING_SCHEMA_VERSION}'`);
    for (const method of CURRENT_RACE_RATING_METHODS) {
      expect(MIGRATION_248_SQL).toContain(`'${method}'`);
    }
    for (const status of CURRENT_RACE_RATING_EVIDENCE_STATUSES) {
      expect(MIGRATION_248_SQL).toContain(`'${status}'`);
    }
    for (const confidence of CURRENT_RACE_RATING_CONFIDENCES) {
      expect(MIGRATION_248_SQL).toContain(`'${confidence}'`);
    }
  });

  it("pairs derived fields with evidence_status in one CHECK", () => {
    expect(MIGRATION_248_SQL).toContain("current_race_ratings_rated_fields_check");
    expect(MIGRATION_248_SQL).toContain("evidence_status = 'rated'");
    expect(MIGRATION_248_SQL).toContain("evidence_status = 'none_found'");
  });
});
