// One spelling per party in candidates.party. The set of parties is open —
// state affiliates, minor parties, and write-in labels appear all the time —
// so this canonicalizes SPELLING, never membership: a variant either matches
// the exact-match table below and gets its canonical form, or passes through
// with whitespace cleaned. Nothing is rejected, nothing is guessed, no AI.
//
// Deliberately exact-match only. Generic affix rules ("Registered X" → X,
// "X Party" → X) look attractive but collapse real distinctions:
//   - "Tea Party" is not the "Tea" party, "Kentucky Party" is not "Kentucky"
//   - "Independent Party" is a registered party in several states (OR, CT),
//     which is NOT the same as "Independent" (no party)
// An unknown spelling stored as-is is a cosmetic bug; a wrong merge is a
// factual one. New variants get a table row when they show up.
//
// Seeded from every variant observed in the database as of 2026-08-01
// (63 distinct values; SELECT party, count(*) FROM candidates GROUP BY 1).
// Left alone on purpose:
//   - real distinct parties: Democratic-Farmer-Labor (MN), Democratic-NPL
//     (ND), state Greens ("Wisconsin Green"), "Libertarian Party of Florida",
//     "No Political Party" (NV's official phrasing ≠ "No Party Affiliation")
//   - "Registered Republican" / "Registered Democrat" / "Registered
//     Libertarian": Alaska's official candidate labels. AK's top-four system
//     puts the candidate's voter REGISTRATION on the ballot precisely
//     because it is not a party nomination — collapsing "Registered
//     Republican" to "Republican" would turn a registration disclosure into
//     an affiliation claim
//   - minor parties whose official name carries the "Party" suffix:
//     "Constitution Party" (ID), "Independent American Party" (NV, distinct
//     from UT's "Independent American" rows). The major-party mappings below
//     ("Democratic Party" → "Democratic") do not generalize: for majors the
//     adjective form is the universal candidate-label convention; for minors
//     the full name IS the name, and affiliates vary by state
//   - self-descriptions: "Moderate Democrat", "Pro Gun Liberal"
//   - parenthesis-mangled import defects ("Independent) (Write-in"): an
//     upstream splitting bug, not a spelling; the backfill script flags them
//     for manual repair instead of guessing.

const PARTY_CANON = new Map<string, string>([
  // Canonical names map to themselves so any casing variant ("DEMOCRATIC",
  // "No party preference") lands on the canonical casing.
  ["democratic", "Democratic"],
  ["republican", "Republican"],
  ["libertarian", "Libertarian"],
  ["green", "Green"],
  ["independent", "Independent"],
  ["nonpartisan", "Nonpartisan"],
  ["constitution", "Constitution"],
  ["independent american", "Independent American"],
  ["no party preference", "No Party Preference"],
  ["no party affiliation", "No Party Affiliation"],
  ["unaffiliated", "Unaffiliated"],
  ["unenrolled", "Unenrolled"],
  ["undeclared", "Undeclared"],
  ["unknown", "Unknown"],
  // Observed spelling variants.
  ["dem", "Democratic"],
  ["democrat", "Democratic"],
  ["democratic party", "Democratic"],
  ["rep", "Republican"],
  ["republican party", "Republican"],
  ["lib", "Libertarian"],
  ["libertarian party", "Libertarian"],
  ["gre", "Green"],
  ["ind", "Independent"],
  ["states no party preference", "No Party Preference"],
]);

/**
 * Canonicalizes a party label's spelling: trims, collapses internal
 * whitespace, and maps known variants (case-insensitive) to one canonical
 * form. Unknown labels pass through cleaned but otherwise untouched.
 * Idempotent: every canonical form is its own table key.
 */
export function canonicalizeParty(raw: string): string {
  const cleaned = raw.trim().replace(/\s+/g, " ");
  return PARTY_CANON.get(cleaned.toLowerCase()) ?? cleaned;
}
