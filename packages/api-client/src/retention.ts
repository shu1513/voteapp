/**
 * Judicial retention races ("Shall Judge X be retained in office?") are
 * catalogued as office races with the judge as the single candidate, but the
 * ballot asks Yes/No and the app records the answer as measure_position.
 * One regex, shared by web, mobile, and (as a copy) the backend's
 * electionPartisanshipPolicy.isJudicialRetentionTitle — keep them identical.
 */
export function isJudicialRetentionTitle(title: string): boolean {
  return /\b(retention|retain(?:ed|ing)?|be retained)\b/i.test(title);
}

/** True when an office race is answered Yes/No instead of by picking a candidate. */
export function isRetentionRace(election: { race_type: string; official_ballot_title: string }): boolean {
  return election.race_type === "office" && isJudicialRetentionTitle(election.official_ballot_title);
}
