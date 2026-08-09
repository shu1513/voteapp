// Shared allowlist of migration-number collisions that predate (or slipped past)
// duplicate-prefix enforcement. Both the migrator (dbMigrate.ts) and the manual
// research preflight check (checkManualResearchPreflight.ts) read this list, so
// keep it here — two copies drifted once already and broke preflight.
//
// These exact duplicate sets were merged before duplicate-prefix enforcement existed.
// Keep the filenames stable so applied databases do not replay renamed migrations.
export const LEGACY_DUPLICATE_MIGRATION_FILES_BY_PREFIX = new Map<string, string[]>([
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
  // PRs #582 and #583 merged the same day, each claiming 215; both files were
  // already applied by filename to local databases before the collision was
  // noticed, so renumbering would replay one of them. The two migrations
  // touch unrelated objects (SF link tables vs. a district-research check
  // constraint), so filename-order application is safe.
  [
    "215",
    [
      "215_add_san_francisco_campaign_finance_link_tables.sql",
      "215_widen_manual_district_research_trigger_source.sql",
    ],
  ],
]);

export function isLegacyDuplicateMigrationSet(
  prefix: string,
  filenames: readonly string[]
): boolean {
  const legacyFilenames = LEGACY_DUPLICATE_MIGRATION_FILES_BY_PREFIX.get(prefix);
  if (!legacyFilenames || legacyFilenames.length !== filenames.length) {
    return false;
  }

  const legacyFilenameSet = new Set(legacyFilenames);
  return filenames.every((filename) => legacyFilenameSet.has(filename));
}
