// Deterministic intent detection — docs/plans/chatbot-rag.md component 5.
// Zero AI: pure pattern matching, unit-testable without a database. The ask
// service renders the matched intent into a template answer (fetching state
// resources when needed).
//
// Policy intents implement BEHAVIOR.md rule 1 (no endorsements, ever) and
// rule 5/6 (logistics and time-sensitive answers are deterministic — they
// never reach retrieval, a cache, or an LLM).

export type IntentKind =
  | "greeting"             // whole-message "hi"/"hello" → welcome, no retrieval
  | "thanks"               // whole-message "thank you" → acknowledgement
  | "goodbye"              // whole-message "bye" → sign-off
  | "help"                 // whole-message "help"/"what can you do" → capabilities
  | "election_countdown"   // "how many days until the election" → date math
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

/** Intents whose template is parameterized by a state — the set a bare-state
 * follow-up ("California") can complete, and the set worth resolving from the
 * asker's saved districts when the question names no state. needs_scope is
 * the state-less form of other_election_date, so it maps there. */
export const STATE_TEMPLATE_INTENTS: ReadonlySet<IntentKind> = new Set([
  "where_to_vote",
  "voter_registration",
  "voter_id",
  "mail_voting",
  "other_election_date",
  "needs_scope",
]);

/**
 * Two-letter state when the message is ONLY a state — "California",
 * "in California", "I vote in GA" — the natural reply to "tell me which
 * state you vote in". Anything with substance beyond the state (and a short
 * lead-in) is not a bare reply and must stand on its own.
 */
export function detectBareStateReply(question: string): string | null {
  const q = question.trim().replace(/[\s.!?,]+$/, "");
  const core = q
    .replace(/^(?:i\s+(?:vote|live)\s+in|i'?m\s+(?:in|from)|in|from|it'?s|my\s+state\s+is)\s+/i, "")
    .trim();
  if (/^washington,?\s+d\.?\s?c\.?$/i.test(core)) {
    return "DC";
  }
  const byName = STATE_ABBREVIATIONS[core.toLowerCase()];
  if (byName) {
    return byName;
  }
  // Abbreviations must be typed in caps to count ("in" is not Indiana; "ok"
  // alone is just okay) — same caution as detectStateInQuestion.
  if (/^[A-Z]{2}$/.test(core) && KNOWN_ABBREVIATIONS.has(core)) {
    return core;
  }
  return null;
}

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
  // DC aliases BEFORE the state-name loop: "Washington, DC" contains
  // "washington" and would otherwise resolve to Washington state.
  if (/\bwashington,?\s+d\.?\s?c\.?\b/.test(lower)) {
    return "DC";
  }
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

// Smalltalk — matched against the WHOLE message only (anchored both ends), so
// "hi, who is running in GA?" never routes here. Zero data: greetings must
// not reach retrieval, where "hi" only matches noise.
const SMALLTALK_PATTERNS: ReadonlyArray<{ kind: IntentKind; pattern: RegExp }> = [
  {
    kind: "greeting",
    pattern:
      /^(?:hi|hiya|hello|hey|heya|howdy|yo|sup|what'?s\s+up|good\s+(?:morning|afternoon|evening)|(?:hi|hello|hey)\s+there)[\s!.,?]*$/i,
  },
  {
    kind: "thanks",
    pattern:
      /^(?:thanks|thank\s+you|thanks\s+(?:a\s+lot|so\s+much|again)|thank\s+you\s+(?:so|very)\s+much|thx|ty|tysm|much\s+appreciated|appreciate\s+it)[\s!.,?]*$/i,
  },
  {
    kind: "goodbye",
    pattern:
      /^(?:bye|goodbye|bye\s*bye|see\s+(?:you|ya)(?:\s+later)?|good\s*night|take\s+care|later)[\s!.,?]*$/i,
  },
  {
    kind: "help",
    pattern:
      /^(?:help|help\s+me|what\s+can\s+(?:you|this)\s+do|what\s+can\s+i\s+ask(?:\s+you)?|what\s+do\s+you\s+do|how\s+do(?:es)?\s+(?:this|it|you)\s+work|what\s+is\s+this|who\s+are\s+you|what\s+are\s+you)[\s!.,?]*$/i,
  },
];

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

  // Whole-message smalltalk first: cheapest check, and "HI there" must not
  // fall through to state detection or retrieval.
  for (const { kind, pattern } of SMALLTALK_PATTERNS) {
    if (pattern.test(q)) {
      return { kind, state: null };
    }
  }
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
  // Covers the common phrasings, not just "when is": "What date is the
  // Texas primary?", "Texas primary date?". With a state → template pointing
  // at official resources, no invented date; without one → clarify (rule 7 +
  // rule 6: time-sensitive, never guessed).
  const OTHER_RACE = "(?:runoff|primar(?:y|ies)|special\\s+election)";
  if (
    new RegExp(
      `\\b(?:when|what\\s+(?:date|day)|date|day|how\\s+(?:many\\s+days|long)|days\\s+(?:left|remaining|until|till))\\b.{0,60}\\b${OTHER_RACE}\\b`,
      "i"
    ).test(q) ||
    new RegExp(`\\b${OTHER_RACE}\\b.{0,40}\\b(?:date|day|when|schedule)\\b`, "i").test(q)
  ) {
    return state ? { kind: "other_election_date", state } : { kind: "needs_scope", state };
  }
  // Countdown before the general date frame: "how many days until the
  // election" carries no "when is". Primary/runoff countdowns were already
  // caught above — this only ever means the fixed general-election date.
  if (/\b(?:how\s+(?:many\s+days|long)|days\s+(?:left|remaining|until|till))\b.{0,40}\b(?:election|voting|vote)\b/i.test(q)) {
    return { kind: "election_countdown", state };
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
  // "My area" questions ("who is running in my area?", "races near me") LAST:
  // the corpus has no idea where the asker lives, but the saved-ballot page
  // does — deep-link it rather than refuse. Checked after every specific
  // frame so "when is the runoff in my area" still clarifies as a date ask.
  if (/\b(?:in|for|near|around)\s+my\s+(?:area|city|town|county|district|neighborhood|state)\b|\bnear\s+me\b/i.test(q)) {
    return { kind: "ballot_lookup", state };
  }
  return null;
}
