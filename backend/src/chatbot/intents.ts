// Deterministic intent detection — docs/plans/chatbot-rag.md component 5.
// Zero AI: pure pattern matching, unit-testable without a database. The ask
// service renders the matched intent into a template answer (fetching state
// resources when needed).
//
// Policy intents implement BEHAVIOR.md rule 1 (no endorsements, ever) and
// rule 5/6 (logistics and time-sensitive answers are deterministic — they
// never reach retrieval, a cache, or an LLM).

export type IntentKind =
  | "policy_refusal"       // endorsement/recommendation ask → neutral refusal
  | "untracked_data"       // social-media posts etc. — data we never index
  | "out_of_cycle"         // election ask about a year outside the covered cycle
  | "needs_scope"          // scopeless time-sensitive ask ("when is the runoff?") → clarify
  | "ballot_lookup"        // "what's on my ballot" → deep link
  | "where_to_vote"        // polling place → state link or deep link
  | "election_date"        // "when is the 2026 general election"
  | "other_election_date"  // primary/runoff/special date with a state named
  | "voter_registration"   // how/deadline to register
  | "voter_id"             // ID requirements
  | "mail_voting"          // vote by mail / absentee
  | "results";             // who won / was it decided

export type IntentMatch = {
  kind: IntentKind;
  /** Two-letter state when the question names one; logistics templates use
   * it to link that state's official resources. */
  state: string | null;
};

const STATE_ABBREVIATIONS: Record<string, string> = {
  alabama: "AL", alaska: "AK", arizona: "AZ", arkansas: "AR", california: "CA",
  colorado: "CO", connecticut: "CT", delaware: "DE",
  "district of columbia": "DC", florida: "FL", georgia: "GA", hawaii: "HI",
  idaho: "ID", illinois: "IL", indiana: "IN", iowa: "IA", kansas: "KS",
  kentucky: "KY", louisiana: "LA", maine: "ME", maryland: "MD",
  massachusetts: "MA", michigan: "MI", minnesota: "MN", mississippi: "MS",
  missouri: "MO", montana: "MT", nebraska: "NE", nevada: "NV",
  "new hampshire": "NH", "new jersey": "NJ", "new mexico": "NM",
  "new york": "NY", "north carolina": "NC", "north dakota": "ND", ohio: "OH",
  oklahoma: "OK", oregon: "OR", pennsylvania: "PA", "rhode island": "RI",
  "south carolina": "SC", "south dakota": "SD", tennessee: "TN", texas: "TX",
  utah: "UT", vermont: "VT", virginia: "VA", washington: "WA",
  "west virginia": "WV", wisconsin: "WI", wyoming: "WY",
};

const KNOWN_ABBREVIATIONS = new Set(Object.values(STATE_ABBREVIATIONS));

/**
 * Two-letter state from the question, or null. Full names win over
 * abbreviations. Abbreviations must be standalone uppercase tokens AND carry
 * place context — preceded by "in/for/from/near", a comma, or a capitalized
 * word ("Atlanta GA") — because many are ordinary words in caps: "voter ID"
 * is not Idaho, "OK, who is running?" is not Oklahoma. Tokens right before
 * city-ish words are skipped too ("LA mayor" is Los Angeles, not Louisiana).
 * "Washington" alone means the state here — the corpus is elections, where
 * DC races say "District of Columbia".
 */
export function detectStateInQuestion(question: string): string | null {
  const lower = question.toLowerCase();
  // Longest names first so "west virginia" is not read as "virginia".
  const names = Object.keys(STATE_ABBREVIATIONS).sort((a, b) => b.length - a.length);
  for (const name of names) {
    if (new RegExp(`\\b${name.replace(/ /g, "\\s+")}\\b`, "i").test(lower)) {
      return STATE_ABBREVIATIONS[name] as string;
    }
  }
  const abbrevMatches = question.matchAll(/\b([A-Z]{2})\b(?!\s+(?:mayor|city|county|metro|unified))/g);
  for (const match of abbrevMatches) {
    const abbrev = match[1] as string;
    if (!KNOWN_ABBREVIATIONS.has(abbrev)) {
      continue;
    }
    const before = question.slice(0, match.index).trimEnd();
    const hasPlaceContext =
      /(?:\b(?:in|for|from|near)|,|\b[A-Z][a-z]+)$/.test(before);
    if (hasPlaceContext) {
      return abbrev;
    }
  }
  return null;
}

const POLICY_PATTERNS: RegExp[] = [
  /\bwho\s+(?:should|do|would)\s+(?:i|you|we)\s+(?:vote|pick|choose|support)\b/i,
  /\bshould\s+(?:i|we)\s+vote\b/i,
  /\bwho(?:'s| is)?\s+(?:the\s+)?(?:best|better|worst)\b/i,
  /\bis\s+.{2,80}\s+better\s+than\b/i,
  /\b(?:best|smartest|right|correct)\s+(?:vote|choice|pick|candidate)\b/i,
  /\brank\s+.{2,80}\b(?:candidates?|from\s+best)\b/i,
  /\bwhich\s+(?:party|candidate)\s+(?:should|do)\s+(?:i|we)\s+(?:support|vote|pick|choose)\b/i,
  /\b(?:endorse|endorsement\s+from\s+you|recommend(?:ation)?)\b.{0,40}\b(?:candidate|vote|who)\b/i,
  /\btell\s+me\s+who\s+to\s+vote\b/i,
  /\bwho\s+to\s+vote\s+for\b/i,
];

// Order matters: policy first (an endorsement ask phrased as logistics must
// still refuse), then the most specific templates.
export function detectIntent(question: string): IntentMatch | null {
  const state = detectStateInQuestion(question);
  const q = question.trim();

  if (POLICY_PATTERNS.some((pattern) => pattern.test(q))) {
    return { kind: "policy_refusal", state };
  }
  // Social feeds are a data class we deliberately never index, and a strong
  // candidate-name match would otherwise push these through the entity gate
  // as profile cards. Deterministic honest refusal instead.
  if (/\b(?:tweet(?:s|ed)?|retweet(?:s|ed)?|instagram|tiktok|facebook\s+post)\b/i.test(q)) {
    return { kind: "untracked_data", state };
  }
  // Election questions about a year the corpus does not cover ("who is
  // running for president in 2028?") refuse honestly instead of retrieving
  // whatever shares a word. Scoped to election-ish phrasing so a stray year
  // in a record question does not trip it.
  const yearMatch =
    /\b(?:running|election|ballot|race|president|primary|primaries|runoff)\b.{0,40}?\b(20\d\d)\b/i.exec(q) ??
    /\b(20\d\d)\b.{0,40}?\b(?:running|election|ballot|race|president|primary|primaries|runoff)\b/i.exec(q);
  if (yearMatch && yearMatch[1] !== "2026") {
    return { kind: "out_of_cycle", state };
  }
  if (/\b(?:who\s+won|did\s+.{2,60}\s+win|been\s+decided|election\s+results?|results\s+of)\b/i.test(q)) {
    return { kind: "results", state };
  }
  if (/\b(?:my\s+ballot|on\s+the\s+ballot\s+at\s+my|ballot\s+lookup)\b/i.test(q)) {
    return { kind: "ballot_lookup", state };
  }
  if (/\b(?:where\s+(?:do|can)\s+i\s+vote|polling\s+(?:place|location)|vote\s+in\s+person)\b/i.test(q)) {
    return { kind: "where_to_vote", state };
  }
  // Primary/runoff/special date asks must NEVER receive the fixed general-
  // election date ("When is the Texas primary?" ≠ November 3) — checked
  // before the general date frame because both match "when is … election".
  // With a state → template pointing at official resources, no invented
  // date; without one → clarify (rule 7 + rule 6: time-sensitive, never
  // guessed).
  if (/\bwhen\s+(?:is|are)\b.{0,60}\b(?:runoff|primar(?:y|ies)|special\s+election)\b/i.test(q)) {
    return state ? { kind: "other_election_date", state } : { kind: "needs_scope", state };
  }
  // Deliberately requires the "when is" frame: a bare "election day" mention
  // ("what will the weather be on election day?") is not a date question.
  if (/\bwhen\s+is\b.{0,40}\belection\b/i.test(q)) {
    return { kind: "election_date", state };
  }
  if (/\bregister(?:ing|ed)?\s+to\s+vote\b|\bvoter\s+registration\b|\bregistration\s+deadline\b/i.test(q)) {
    return { kind: "voter_registration", state };
  }
  if (/\b(?:id|identification)\b.{0,30}\bvote\b|\bvoter\s+id\b/i.test(q)) {
    return { kind: "voter_id", state };
  }
  if (/\b(?:vote|voting|ballot)\s+by\s+mail\b|\bmail[- ]in\s+(?:ballot|voting)\b|\babsentee\b/i.test(q)) {
    return { kind: "mail_voting", state };
  }
  return null;
}
