import { readdirSync, readFileSync } from "node:fs";

const MIGRATIONS_DIR = new URL("../../../db/migrations/", import.meta.url);

/**
 * Computes the effective column set of a table by replaying every migration
 * in order: the CREATE TABLE column list plus any later ALTER TABLE
 * ADD COLUMN / DROP COLUMN statements that target the table. Lets schema
 * drift tests validate query columns against what the migrations actually
 * build, not just the migration that first created the table.
 */
export function migrationTableColumns(tableName: string): Set<string> {
  const files = readdirSync(MIGRATIONS_DIR)
    .filter((file) => file.endsWith(".sql"))
    .sort((a, b) => Number.parseInt(a, 10) - Number.parseInt(b, 10));

  const columns = new Set<string>();
  const createTablePattern = new RegExp(
    `CREATE TABLE (?:IF NOT EXISTS )?(?:public\\.)?${tableName} \\(`
  );
  const alterTargetPattern = /ALTER TABLE\s+(?:ONLY\s+)?(?:IF EXISTS\s+)?(?:public\.)?([a-z_]+)/;

  for (const file of files) {
    const sql = readFileSync(new URL(file, MIGRATIONS_DIR), "utf8");

    const createMatch = createTablePattern.exec(sql);
    if (createMatch) {
      const block = sql.slice(createMatch.index, sql.indexOf("\n);", createMatch.index));
      for (const line of block.split("\n").slice(1)) {
        const column = /^ +([a-z_]+) /.exec(line);
        if (column) {
          columns.add(column[1]);
        }
      }
    }

    for (const statement of sql.split(";")) {
      const target = alterTargetPattern.exec(statement);
      if (!target || target[1] !== tableName) {
        continue;
      }
      for (const added of statement.matchAll(/ADD COLUMN (?:IF NOT EXISTS )?([a-z_]+)/g)) {
        columns.add(added[1]);
      }
      for (const dropped of statement.matchAll(/DROP COLUMN (?:IF EXISTS )?([a-z_]+)/g)) {
        columns.delete(dropped[1]);
      }
    }
  }

  return columns;
}
