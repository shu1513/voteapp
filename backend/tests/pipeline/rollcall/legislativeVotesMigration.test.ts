import { readFileSync } from "node:fs";

import { describe, expect, it } from "vitest";

import { CANDIDATE_RECORD_ORIGINS } from "../../../src/pipeline/candidates/candidateRecordStore.js";
import {
  LEGISLATIVE_VOTE_CHAMBERS,
  LEGISLATIVE_VOTE_REVIEW_STATUSES,
} from "../../../src/pipeline/rollcall/legislativeVotes.js";
import { migrationTableColumns } from "../../helpers/migrationTableColumns.js";

const MIGRATION_251_SQL = readFileSync(
  new URL("../../../../db/migrations/251_add_legislative_votes.sql", import.meta.url),
  "utf8"
);
const MIGRATION_252_SQL = readFileSync(
  new URL("../../../../db/migrations/252_add_rollcall_import_record_origin.sql", import.meta.url),
  "utf8"
);

describe("legislative votes migration", () => {
  it("keeps chamber and review_status CHECKs aligned with the code enums", () => {
    for (const chamber of LEGISLATIVE_VOTE_CHAMBERS) {
      expect(MIGRATION_251_SQL).toContain(`'${chamber}'`);
    }
    for (const status of LEGISLATIVE_VOTE_REVIEW_STATUSES) {
      expect(MIGRATION_251_SQL).toContain(`'${status}'`);
    }
  });

  it("builds every column the plan's data model names", () => {
    const columns = migrationTableColumns("legislative_votes");
    for (const column of [
      "jurisdiction",
      "chamber",
      "session",
      "roll_number",
      "vote_date",
      "measure_id",
      "exact_question",
      "voted_text_version",
      "is_floor_vote",
      "result",
      "yeas",
      "nays",
      "display_url",
      "machine_url",
      "bill_url",
      "source_sha256",
      "fetched_at",
      "yea_description",
      "nay_description",
      "labels_json",
      "review_status",
      "reviewed_at",
      "importer_version",
    ]) {
      expect(columns.has(column), column).toBe(true);
    }
  });

  it("only lets a judged floor vote be approved", () => {
    expect(MIGRATION_251_SQL).toContain("legislative_votes_approved_fields_check");
    expect(MIGRATION_251_SQL).toContain("review_status <> 'approved'");
    expect(MIGRATION_251_SQL).toContain("is_floor_vote = true");
  });
});

describe("candidate_records origin migration", () => {
  it("keeps the origin CHECK aligned with CandidateRecordOrigin", () => {
    const checkLine = MIGRATION_252_SQL.split("\n").find((line) => line.includes("origin IN ("));
    expect(checkLine).toBeDefined();
    for (const origin of CANDIDATE_RECORD_ORIGINS) {
      expect(checkLine).toContain(`'${origin}'`);
    }
  });
});
