// Filter buckets for the candidate-list party filter (web + mobile).
//
// Bucketing is FILTER RELEVANCE, deliberately separate from storage:
// candidates.party keeps the authority's label ("Democratic-Farmer-Labor",
// "Registered Republican"), while the filter groups labels by which major
// party a viewer filtering "Democrats"/"Republicans" would expect to see.
// That is why registration labels and state affiliates map to their party
// here even though the write path never collapses them.
//
// Exact-match only, same reasoning as the backend canonicalizer
// (candidatePartyCanonicalization.ts): substring rules would misfile
// "Independent Party" or "Moderate Democrat" (a self-description in a
// nonpartisan context, not a Democratic Party label). Labels not listed —
// minor parties, Independent, Nonpartisan, Unknown — bucket as "other".

export type PartyBucket = "democratic" | "republican" | "other";

const DEMOCRATIC_LABELS = new Set([
  "democratic",
  // MN and ND affiliates of the national party.
  "democratic-farmer-labor",
  "democratic-npl",
  // Alaska's top-four registration label: a registration disclosure, not an
  // affiliation claim — but a viewer filtering "Democrats" expects it here.
  "registered democrat",
  // Legacy spelling variants the backend canonicalizer rewrites at write
  // time. They stay here because this function runs against whatever a
  // deployment's data currently holds — a database whose backfill has not
  // run yet (production today) would otherwise file these under "Other".
  "dem",
  "democrat",
  "democratic party",
]);

const REPUBLICAN_LABELS = new Set([
  "republican",
  "registered republican",
  // Legacy variants, same reasoning as the Democratic set.
  "rep",
  "republican party",
]);

export function partyBucket(party: string | null | undefined): PartyBucket {
  if (!party) {
    return "other";
  }
  const key = party.trim().toLowerCase();
  if (DEMOCRATIC_LABELS.has(key)) {
    return "democratic";
  }
  if (REPUBLICAN_LABELS.has(key)) {
    return "republican";
  }
  return "other";
}
