import { readdir } from "node:fs/promises";
import { existsSync } from "node:fs";
import { resolve } from "node:path";
import { Pool } from "pg";

import { loadProjectEnv } from "../config/env.js";

type RequiredColumn = {
  table: string;
  column: string;
};

type RequiredUniqueObject = {
  table: string;
  name: string;
};

const REQUIRED_COLUMNS: RequiredColumn[] = [
  { table: "staging_items", column: "ingest_key" },
  { table: "staging_items", column: "ai_raw_debug" },
  { table: "candidates", column: "display_name" },
  { table: "candidates", column: "official_website_url" },
  { table: "candidates", column: "fec_ids" },
  { table: "candidates", column: "state_filing_ids" },
  { table: "candidate_elections", column: "candidate_id" },
  { table: "candidate_elections", column: "election_id" },
  { table: "candidate_elections", column: "updated_at" },
  { table: "candidate_records", column: "candidate_id" },
  { table: "candidate_records", column: "description" },
  { table: "candidate_records", column: "source_url" },
  { table: "candidate_records", column: "event_date" },
  { table: "candidate_records", column: "record_identity_key" },
  { table: "candidate_records", column: "updated_at" },
  { table: "candidate_record_area_tags", column: "candidate_record_id" },
  { table: "candidate_record_area_tags", column: "research_area_id" },
  { table: "candidate_record_area_tags", column: "stance" },
  { table: "candidate_record_area_tags", column: "updated_at" },
  { table: "ballot_measures", column: "election_id" },
  { table: "ballot_measures", column: "summary" },
  { table: "ballot_measures", column: "what_yes_means" },
  { table: "ballot_measures", column: "what_no_means" },
  { table: "ballot_measures", column: "source_url" },
  { table: "ballot_measures", column: "official_measure_url" },
  { table: "ballot_measures", column: "research_area_tags_researched_at" },
  { table: "ballot_measures", column: "updated_at" },
  { table: "ballot_measure_research_area_tags", column: "ballot_measure_id" },
  { table: "ballot_measure_research_area_tags", column: "research_area_id" },
  { table: "ballot_measure_research_area_tags", column: "stance" },
  { table: "ballot_measure_research_area_tags", column: "updated_at" },
  { table: "office_research_areas", column: "office_id" },
  { table: "research_areas", column: "slug" },
];

const REQUIRED_UNIQUE_OBJECTS: RequiredUniqueObject[] = [
  { table: "candidate_elections", name: "uq_candidate_elections_candidate_id_election_id" },
  { table: "candidate_records", name: "uq_candidate_records_candidate_identity_key" },
  { table: "ballot_measures", name: "uq_ballot_measures_election_id" },
];

const LEGACY_DUPLICATE_MIGRATION_FILES_BY_PREFIX = new Map<string, string[]>([
  [
    "075",
    ["075_add_judge_mapping_research_areas.sql", "075_consolidate_judicial_offices_by_scope.sql"],
  ],
  [
    "125",
    ["125_add_tennessee_campaign_finance_tables.sql", "125_add_user_research_area_preferences.sql"],
  ],
  [
    "127",
    [
      "127_add_florida_campaign_finance_tables.sql",
      "127_add_maryland_campaign_finance_tables.sql",
      "127_add_pennsylvania_campaign_finance_tables.sql",
      "127_add_utah_campaign_finance_tables.sql",
    ],
  ],
  [
    "128",
    [
      "128_add_florida_outside_group_support_links.sql",
      "128_add_oregon_campaign_finance_tables.sql",
      "128_add_utah_supporting_committee_finance_tables.sql",
    ],
  ],
]);

function resolveMigrationsDir(): string {
  const candidates = [
    resolve(process.cwd(), "db/migrations"),
    resolve(process.cwd(), "../db/migrations"),
  ];
  const match = candidates.find((path) => existsSync(path));
  if (!match) {
    throw new Error(`Could not find db/migrations from cwd=${process.cwd()}`);
  }
  return match;
}

function usage(): string {
  return [
    "Usage:",
    "  npm run manual:research:preflight",
    "",
    "Checks local DB/schema assumptions needed by manual research writer scripts.",
  ].join("\n");
}

function duplicateMigrationNumbers(files: readonly string[]): Record<string, string[]> {
  const byNumber = new Map<string, string[]>();
  for (const file of files) {
    const match = /^(\d{3})_.*\.sql$/.exec(file);
    if (!match) {
      continue;
    }
    const number = match[1]!;
    const group = byNumber.get(number) ?? [];
    group.push(file);
    byNumber.set(number, group);
  }

  const duplicates: Record<string, string[]> = {};
  for (const [number, group] of byNumber.entries()) {
    if (group.length > 1) {
      duplicates[number] = group.sort();
    }
  }
  return duplicates;
}

function isLegacyAllowedDuplicate(prefix: string, filenames: readonly string[]): boolean {
  const allowed = LEGACY_DUPLICATE_MIGRATION_FILES_BY_PREFIX.get(prefix);
  if (!allowed || allowed.length !== filenames.length) {
    return false;
  }
  const allowedSet = new Set(allowed);
  return filenames.every((filename) => allowedSet.has(filename));
}

async function loadExistingColumns(pool: Pool): Promise<Set<string>> {
  const result = await pool.query<{ table_name: string; column_name: string }>(
    `
      SELECT table_name, column_name
      FROM information_schema.columns
      WHERE table_schema = 'public'
    `
  );
  return new Set(result.rows.map((row) => `${row.table_name}.${row.column_name}`));
}

async function loadExistingConstraints(pool: Pool): Promise<Set<string>> {
  const result = await pool.query<{ table_name: string; constraint_name: string }>(
    `
      SELECT table_name, constraint_name
      FROM information_schema.table_constraints
      WHERE table_schema = 'public'
    `
  );
  return new Set(result.rows.map((row) => `${row.table_name}.${row.constraint_name}`));
}

async function loadExistingIndexes(pool: Pool): Promise<Set<string>> {
  const result = await pool.query<{ tablename: string; indexname: string }>(
    `
      SELECT tablename, indexname
      FROM pg_indexes
      WHERE schemaname = 'public'
    `
  );
  return new Set(result.rows.map((row) => `${row.tablename}.${row.indexname}`));
}

async function main(): Promise<void> {
  loadProjectEnv();

  if (process.argv.includes("--help") || process.argv.includes("-h")) {
    console.log(usage());
    return;
  }

  const migrationFiles = await readdir(resolveMigrationsDir());
  const migrationNumberDuplicates = duplicateMigrationNumbers(migrationFiles);
  const unallowedMigrationNumberDuplicates = Object.fromEntries(
    Object.entries(migrationNumberDuplicates).filter(
      ([prefix, filenames]) => !isLegacyAllowedDuplicate(prefix, filenames)
    )
  );

  const pool = new Pool({
    connectionString: process.env.DATABASE_URL ?? "postgresql://localhost:5432/voteapp",
  });

  try {
    const [columns, constraints, indexes] = await Promise.all([
      loadExistingColumns(pool),
      loadExistingConstraints(pool),
      loadExistingIndexes(pool),
    ]);

    const missingColumns = REQUIRED_COLUMNS.filter(
      (entry) => !columns.has(`${entry.table}.${entry.column}`)
    );
    const missingUniqueObjects = REQUIRED_UNIQUE_OBJECTS.filter(
      (entry) =>
        !constraints.has(`${entry.table}.${entry.name}`) &&
        !indexes.has(`${entry.table}.${entry.name}`)
    );

    const ok =
      missingColumns.length === 0 &&
      missingUniqueObjects.length === 0 &&
      Object.keys(unallowedMigrationNumberDuplicates).length === 0;

    const report = {
      ok,
      missingColumns,
      missingUniqueObjects,
      unallowedMigrationNumberDuplicates,
      migrationNumberDuplicates,
    };
    console.log(JSON.stringify(report, null, 2));

    if (!ok) {
      process.exitCode = 1;
    }
  } finally {
    await pool.end();
  }
}

main().catch((error) => {
  const message = error instanceof Error ? error.message : String(error);
  console.error("manual research preflight failed:", message);
  process.exitCode = 1;
});
