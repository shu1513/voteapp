import { readdirSync, readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";

// deleteAccount hard-deletes the users row and relies on the schema to remove
// every associated record: the UI and privacy policy promise that deleting an
// account permanently removes districts, follows, and preferences. This test
// replays the migration files and fails if any table stores a user_id without
// declaring what happens on user deletion — the way a new table would silently
// start leaking personal data past account deletion.

const MIGRATIONS_DIR = new URL("../../../db/migrations/", import.meta.url);

// Reports are kept for content moderation with the reporter anonymized.
const SET_NULL_TABLES = new Set(["content_reports"]);

type UserReference = {
  table: string;
  onDelete: "CASCADE" | "SET NULL" | null;
};

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

      const reference = /REFERENCES\s+(?:public\.)?users\s*\(id\)[\s,]*(ON DELETE (?:CASCADE|SET NULL))?/.exec(block);
      const onDelete = reference?.[1]?.endsWith("CASCADE")
        ? ("CASCADE" as const)
        : reference?.[1]?.endsWith("SET NULL")
          ? ("SET NULL" as const)
          : null;
      references.set(table, { table, onDelete: reference ? onDelete : null });
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
