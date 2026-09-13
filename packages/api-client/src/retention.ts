/**
 * Judicial retention races ("Shall Judge X be retained in office?") are
 * catalogued as office races with the judge as the single candidate, but the
 * ballot asks Yes/No and the app records the answer as measure_position.
 * One regex, shared by web, mobile, and (as a copy) the backend's
 * electionPartisanshipPolicy.isJudicialRetentionTitle — keep them identical.
 */
export function isJudicialRetentionTitle(title: string): boolean {
  // California's retention question is prescribed wording used only for
  // judges ("Shall [Associate Justice] X be elected to the office for the
  // term provided by law?"), and the ballot sometimes omits the office
  // word ("Shall DAVID B. SAPP be elected to the office ..."), so the
  // phrase alone is enough.
  if (/\bbe elected to the office for the term provided by law\b/i.test(title)) {
    return true;
  }
  // Otherwise both halves are required: the retention verb alone would also
  // catch a non-judicial office such as "Water Retention District Director".
  return /\b(retention|retain(?:ed|ing)?)\b/i.test(title) && /\b(judge|justice|court|judicial|magistrate)\b/i.test(title);
}

/** True when an office race is answered Yes/No instead of by picking a candidate. */
export function isRetentionRace(election: { race_type: string; official_ballot_title: string }): boolean {
  return election.race_type === "office" && isJudicialRetentionTitle(election.official_ballot_title);
}
