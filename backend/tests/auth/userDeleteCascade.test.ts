import { readdirSync, readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";

// deleteAccount hard-deletes the users row and relies on the schema to remove
// every associated record: the UI and privacy policy promise that deleting an
// account permanently removes districts, follows, and preferences. This test
// replays the migration files and fails if any table stores a user_id without
// declaring what happens on user deletion — the way a new table would silently
// start leaking personal data past account deletion.
//
// Tables linked to a user without a user_id column (today:
// user_push_notification_receipts via expo_push_token, and
// content_reports.reporter_email beyond its nulled user_id) are outside what
// a schema scan can prove; deleteAccount scrubs those explicitly in its
// transaction, covered by the authService unit tests.

const MIGRATIONS_DIR = new URL("../../../db/migrations/", import.meta.url);

// Reports are kept for content moderation with the reporter anonymized.
const SET_NULL_TABLES = new Set(["content_reports"]);

type UserReference = {
  table: string;
  onDelete: "CASCADE" | "SET NULL" | null;
};

function parseOnDelete(sqlFragment: string): UserReference["onDelete"] {
  const reference = /REFERENCES\s+(?:public\.)?users\s*\(id\)[\s,]*(ON DELETE (?:CASCADE|SET NULL))?/.exec(sqlFragment);
  if (!reference?.[1]) {
    return null;
  }
  return reference[1].endsWith("CASCADE") ? "CASCADE" : "SET NULL";
}

function collectUserReferences(): Map<string, UserReference> {
  const files = readdirSync(MIGRATIONS_DIR)
    .filter((file) => file.endsWith(".sql"))
    .sort((a, b) => Number.parseInt(a, 10) - Number.parseInt(b, 10));

  const references = new Map<string, UserReference>();

  for (const file of files) {
    const sql = readFileSync(new URL(file, MIGRATIONS_DIR), "utf8");

    for (const match of sql.matchAll(/CREATE TABLE (?:IF NOT EXISTS )?(?:public\.)?([a-z_]+) \(/g)) {
      const table = match[1];
      const block = sql.slice(match.index, sql.indexOf("\n);", match.index));
      if (table === "users" || !/^\s+user_id /m.test(block)) {
        continue;
      }
      references.set(table, { table, onDelete: parseOnDelete(block) });
    }

    // A user_id column bolted onto an existing table arrives via ALTER TABLE
    // instead of a CREATE TABLE block; hold it to the same requirement.
    for (const statement of sql.split(";")) {
      const alterTarget = /ALTER TABLE\s+(?:ONLY\s+)?(?:IF EXISTS\s+)?(?:public\.)?([a-z_]+)/.exec(statement);
      if (!alterTarget || alterTarget[1] === "users") {
        continue;
      }
      if (/ADD COLUMN (?:IF NOT EXISTS )?user_id\b/.test(statement)) {
        references.set(alterTarget[1], { table: alterTarget[1], onDelete: parseOnDelete(statement) });
      }
    }
  }

  return references;
}

describe("account deletion cascade coverage", () => {
  const references = collectUserReferences();

  it("finds the user-owned tables (guards the parser against silent no-ops)", () => {
    expect([...references.keys()]).toEqual(
      expect.arrayContaining(["user_districts", "user_candidate_follows", "user_ballot_preferences"])
    );
    expect(references.size).toBeGreaterThanOrEqual(12);
  });

  it("declares ON DELETE for every table that stores a user_id", () => {
    for (const { table, onDelete } of references.values()) {
      const expected = SET_NULL_TABLES.has(table) ? "SET NULL" : "CASCADE";
      expect({ table, onDelete }).toEqual({ table, onDelete: expected });
    }
  });
});
