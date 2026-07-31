export type CandidateRecordSourceTier = "blocked" | "listed" | "unlisted";

// Why a domain is blocked, not just that it is. The three classes need three
// DIFFERENT repairs, and the audit path over already-stored rows has no other
// way to tell them apart — it classifies a bare URL and never sees the
// rejection message the write path produces.
export type BlockedSourceKind =
  | "ugc_social"
  | "generated_candidate_directory"
  | "bot_check_interstitial";

// Discriminated on tier so `blockedKind` is REQUIRED whenever tier is
// "blocked" and absent otherwise. An optional field would invite
// `blockedKind ?? "ugc_social"` at call sites, which is precisely the
// mislabeling this type exists to prevent.
export type CandidateRecordSourceDomainClassification =
  | { tier: "listed" | "unlisted"; hostname: string }
  | { tier: "blocked"; hostname: string; blockedKind: BlockedSourceKind };

// One sentence per class, shared by the write-time rejection message and the
// stored-row audit. Both readers need the same instruction, and an operator
// handed only the enum token still has to know what it means.
export const BLOCKED_SOURCE_KIND_REPAIR: Record<BlockedSourceKind, string> = {
  ugc_social:
    "cite an official (.gov/court), news, or research-grade source instead; a UGC post is citable only through secondary coverage by an accountable publisher",
  generated_candidate_directory:
    "use it as a lead only and cite the primary source it leads you to; its claims may well be true, but they are machine-generated and unaudited",
  bot_check_interstitial:
    "the real page URL is carried inside the interstitial's query string (Radware uses 'ssc='), so URL-decode it and cite that page directly",
};

const BLOCKED_SOURCE_KIND_LABEL: Record<BlockedSourceKind, string> = {
  ugc_social: "a user-generated/social platform",
  generated_candidate_directory:
    "an auto-generated candidate directory that cites no sources and has been observed inventing officeholding history",
  bot_check_interstitial: "a bot-check interstitial, not a publisher",
};

export type CandidateRecordSourcePolicyResult =
  | { ok: true; tier: CandidateRecordSourceTier }
  | { ok: false; reason: string };

// Social / UGC / self-publishing platforms: anyone can post without editorial
// accountability, which makes them the cheapest astroturfing surface (the
// exact "seed Reddit so the AI repeats it" playbook). A record may summarize
// what such a post SAID only via secondary coverage from an accountable
// publisher — never by citing the platform directly. Shortener domains owned
// by these platforms (t.co, redd.it, youtu.be) are blocked too so a redirect
// hop cannot launder the citation.
const BLOCKED_SOURCE_DOMAINS: readonly string[] = [
  "4chan.org",
  "4channel.org",
  "8kun.top",
  "bitchute.com",
  "blogspot.com",
  "bsky.app",
  "bsky.social",
  "discord.com",
  "discord.gg",
  "facebook.com",
  "fandom.com",
  "fb.com",
  "gab.com",
  "gettr.com",
  "instagram.com",
  "kick.com",
  "linkedin.com",
  "mastodon.social",
  "medium.com",
  "nextdoor.com",
  "parler.com",
  "patreon.com",
  "pinterest.com",
  "quora.com",
  "redd.it",
  "reddit.com",
  "rumble.com",
  "snapchat.com",
  "substack.com",
  "t.co",
  "t.me",
  "telegram.me",
  "threads.net",
  "tiktok.com",
  "truthsocial.com",
  "tumblr.com",
  "twitch.tv",
  "twitter.com",
  "wordpress.com",
  "x.com",
  "youtu.be",
  "youtube-nocookie.com",
  "youtube.com",
];

// Auto-generated candidate directories: sites that machine-generate a page for
// every filed candidate, invite the candidate to claim it, and fill the gaps
// with generated prose. They cite nothing, so no individual claim on them is
// auditable, and they publish confident biographical detail that no human ever
// checked. Distinct from the UGC list above — the failure mode is fabrication
// by generation, not astroturfing by posting.
//
// civoren.com, verified 2026-07-30: its Matthew Smith page states "First
// Elected: 2018", describes "his legislative career", and places him in Battle
// Creek / Calhoun County. Michigan HD 62 is a Macomb County seat (Fraser,
// Harrison Twp, Chesterfield, Clinton Twp, Roseville, St. Clair Shores) held by
// Alicia St. Germaine since 2023; Smith is a challenger who has never held
// office. The page invented a county, a year, and an entire incumbency.
//
// Blocking is citation-only by design. These sites are still worth READING for
// leads: an audit of the 13 stored records that cited civoren found all 13
// claims true once chased to primaries, and one (an insurance career confirmed
// at FL DFS licensee D029506) appeared on no other site. Chase the lead, cite
// the primary.
//
// Second-order reason this class needs a hard block rather than a doc note:
// civoren's robots.txt explicitly Allows GPTBot, ClaudeBot, PerplexityBot,
// CCBot and Google-Extended. Its pages are built to be ingested by AI systems,
// and a 2026-07-30 web search restated its invented incumbency as fact with no
// attribution — so a researcher can absorb the fabrication without ever
// visiting the domain, and two "independent" summaries can share one poisoned
// well.
const GENERATED_CANDIDATE_DIRECTORY_DOMAINS: readonly string[] = ["civoren.com"];

// Bot-check interstitials are not publishers at all. A researcher who copies
// the URL out of the address bar while a WAF challenge is showing captures the
// challenge, not the article: the real page survives only inside a query
// parameter, and the interstitial URL is session-bound, opaque, and expires.
// Live: 11 stored records cite validate.perfdrive.com (Radware Bot Manager)
// while their actual source is a Minnesota Secretary of State press release
// carried in the 'ssc=' parameter. Blocking also closes a laundering hole —
// an interstitial URL classifies on the interstitial's hostname, so it can
// carry a blocked or unlisted origin past a tier check.
const BOT_CHECK_INTERSTITIAL_DOMAINS: readonly string[] = [
  "validate.perfdrive.com",
];

// Single accept tier (user decision: no A/B split): sources with official or
// editorial accountability. This is a STARTER list, not a gate — unlisted
// domains are still accepted (see evaluateCandidateRecordSourcePolicy); the
// list's only hard effect is on damaging claims, and it grows by trivial PR
// whenever research hits a legitimate domain that is missing. Any .gov/.mil
// hostname and the legacy *.state.XX.us pattern are listed by rule, so
// legislatures, courts, SoS offices, and agencies need no enumeration.
const LISTED_SOURCE_DOMAINS: readonly string[] = [
  // Civic / legal data
  "ballotpedia.org",
  "ballotready.org",
  "c-span.org",
  "courtlistener.com",
  "followthemoney.org",
  "govtrack.us",
  "justia.com",
  "legiscan.com",
  "michiganvotes.org",
  "openstates.org",
  "opensecrets.org",
  "oyez.org",
  "vote411.org",
  "votesmart.org",
  // Official government records hosted off .gov: agenda/minutes/legislation
  // vendors that municipalities publish through, gov bulletin delivery, and
  // state/local government sites registered on .com/.org (all observed in the
  // live corpus).
  "civicclerk.com",
  "civicplus.com",
  "civicweb.net",
  "cpsboe.org",
  "diligentoneplatform.com",
  "govdelivery.com",
  "granicus.com",
  "legistar.com",
  "macombgov.org",
  "mccinnovations.com",
  "municode.com",
  "muni.org",
  "myflorida.com",
  "tbpr.org",
  // Wires + national outlets
  "abcnews.go.com",
  "apnews.com",
  "axios.com",
  "bbc.co.uk",
  "bbc.com",
  "bloomberg.com",
  "cbsnews.com",
  "cnbc.com",
  "cnn.com",
  "foxnews.com",
  "nbcnews.com",
  "npr.org",
  "nytimes.com",
  "pbs.org",
  "politico.com",
  "propublica.org",
  "reuters.com",
  "theguardian.com",
  "thehill.com",
  "usatoday.com",
  "washingtonpost.com",
  "wsj.com",
  // Nonprofit state-coverage networks
  "calmatters.org",
  "governing.com",
  "montanafreepress.org",
  "newsfromthestates.com",
  "stateline.org",
  "texastribune.org",
  // Regional/local news observed carrying real records in the live corpus
  // (local TV affiliates are the primary reporters of local-government
  // scandal — exactly the rows the damaging rule guards).
  "13newsnow.com",
  "actionnews5.com",
  "bizjournals.com",
  "clarkcountytoday.com",
  "clickondetroit.com",
  "floridapolitics.com",
  "fox17online.com",
  "fox2detroit.com",
  "kcci.com",
  "kjzz.org",
  "komu.com",
  "mauinow.com",
  "semafor.com",
  "stlpr.org",
  "suntimes.com",
  "thebanner.com",
  "thereflector.com",
  "webcenterfairbanks.com",
  "wcpo.com",
  "wibw.com",
  // Major regional papers (roughly state-covering starter set)
  "adn.com",
  "ajc.com",
  "al.com",
  "abqjournal.com",
  "arkansasonline.com",
  "azcentral.com",
  "baltimoresun.com",
  "bangordailynews.com",
  "billingsgazette.com",
  "bostonglobe.com",
  "burlingtonfreepress.com",
  "charlotteobserver.com",
  "chicagotribune.com",
  "clarionledger.com",
  "cleveland.com",
  "courant.com",
  "courier-journal.com",
  "dallasnews.com",
  "delawareonline.com",
  "denverpost.com",
  "deseret.com",
  "desmoinesregister.com",
  "detroitnews.com",
  "freep.com",
  "houstonchronicle.com",
  "idahostatesman.com",
  "inquirer.com",
  "jsonline.com",
  "kansascity.com",
  "latimes.com",
  "mercurynews.com",
  "miamiherald.com",
  "missoulian.com",
  "mlive.com",
  "newsobserver.com",
  "nj.com",
  "nola.com",
  "oklahoman.com",
  "omaha.com",
  "oregonlive.com",
  "orlandosentinel.com",
  "pilotonline.com",
  "post-gazette.com",
  "postandcourier.com",
  "pressherald.com",
  "providencejournal.com",
  "reviewjournal.com",
  "richmond.com",
  "seattletimes.com",
  "sfchronicle.com",
  "sltrib.com",
  "spokesman.com",
  "staradvertiser.com",
  "startribune.com",
  "stltoday.com",
  "tampabay.com",
  "tennessean.com",
  "unionleader.com",
];

// Actor-context guard for the damaging-claim heuristic: a live-corpus run
// (18,888 stored records, 2026-07-24) showed the accusation patterns firing
// on candidates ACTING as judge, prosecutor, or legislator over third-party
// misconduct — "sentenced JuJuan Parks, who pleaded guilty", "co-prosecuted
// a caregiver who was convicted", "Sponsored HB 5551 barring people
// convicted of election crimes", "filed ethics complaints against Governor
// Bentley". Those are the candidate's own official actions, not claims
// against the candidate, so any match here exempts the description. An
// attacker pairing a real vote with a smear in one row gives up the game
// anyway: the smear needs its own source, and audit detectors (plan PR 3)
// see cross-candidate bursts.
const DAMAGING_CLAIM_ACTOR_EXEMPT_PATTERNS: readonly RegExp[] = [
  // Legislative verbs are clause-anchored to their crime-term object
  // ("sponsored a bill barring people CONVICTED", "voted to strengthen
  // penalties for ACCEPTED BRIBES"): the accusation vocabulary must appear
  // in the same clause as the verb, so a bare "Sponsored HB 1" cannot cancel
  // an accusation elsewhere in the description, and the [^.;] gap keeps the
  // anchor from reaching across "; was accused of ..." tack-ons.
  /\b(?:sponsored|co-sponsored|introduced|authored|voted\s+(?:for|against|to))\b[^.;]{0,160}?\b(?:convicted|accused|charged|arrested|indicted|crimes?|felon\w*|misdemeanors?|fraud|bribe\w*|embezzlement|theft|corruption|harassment|misconduct|abuse|assault|violations?|perjury|funds?|money)\b/i,
  /\b(?:co-)?prosecuted\b/i,
  // Judge-as-actor only: "sentenced" must take a direct object ("sentenced a
  // man", "sentenced JuJuan Parks", "sentenced 50 offenders"). A bare
  // \bsentenced\b would also exempt the passive "was sentenced to probation"
  // — the candidate being sentenced — and cancel the damaging check for any
  // description containing the word.
  /\bsentenced\s+(?:a|an|the)\b/i,
  /\bsentenced\s+(?:\d|[A-Z])/,
  // Third-party subject directly attached to the accusation verb: "a man
  // convicted of...", "an officer who pleaded guilty", "people accused of".
  /\b(?:a|an|the|people|those)\s+(?:[\w-]+\s+){0,3}?(?:man|woman|men|women|people|caregiver|officers?|deput(?:y|ies)|defendants?|suspects?|residents?|retailers|operatives?)\b[^.;]{0,80}\b(?:pleaded|pled|convicted|accused|charged|arrested|indicted)\b/i,
  /\bfiled\s+(?:[\w-]+\s+){0,3}?ethics\s+complaints?\s+(?:in\s+\d{4}\s+)?against\b/i,
  /\breleased\s+an?\s+ethics\s+complaint\b/i,
  // Prosecutor/attorney career bios: "handling felony and misdemeanor
  // matters including domestic violence, sexual assault" is a caseload
  // description, not an accusation.
  /\bhandl(?:ing|ed)\s+(?:[\w,\s-]{0,60}?)(?:felony|misdemeanor|domestic\s+violence|child\s+abuse|sexual\s+assault)\b/i,
];

// Damaging-claim heuristic: accusation/enforcement content AIMED AT the
// candidate. Deliberately narrower than the quality classifier's misconduct
// verbs — "investigated"/"audited" describe legitimate actions BY a candidate
// (a comptroller auditing agencies) and standalone crime nouns ("fraud",
// "corruption") appear constantly in legislation ABOUT crime, so both are
// excluded. Passive/prepositional anchoring keeps prosecutor records
// ("convicted 50 felons", "arrested the suspects") out, and
// DAMAGING_CLAIM_ACTOR_EXEMPT_PATTERNS above clears candidate-as-official
// records about third parties. A false positive is cheap: the row is dropped
// with a clear reason and the repair pass (AI) or repair report (manual)
// asks for a listed source — which real scandals always have — so the cost
// is one repair cycle, never lost data.
const DAMAGING_CLAIM_PATTERNS: readonly RegExp[] = [
  /\b(?:was|were|been)\s+(?:indicted|arrested|convicted|sentenced|fined|sanctioned|censured|disbarred|impeached|recalled|sued)\b/i,
  /\bindicted\s+(?:on|for|by)\b/i,
  /\barrested\s+(?:on|for|amid)\b/i,
  /\bconvicted\s+of\b/i,
  /\bpleaded?\s+(?:guilty|no\s+contest)\b|\bpled\s+guilty\b/i,
  /\bcharged\s+with\s+(?:\w+\s+){0,2}?(?:counts?|felon(?:y|ies)|misdemeanors?|crimes?|fraud|embezzlement|bribery|perjury|assault|theft|corruption)\b/i,
  // Active/prepositional accusation forms that the passive anchors above
  // miss: "faces three counts of fraud", "under indictment", "under criminal
  // investigation", "named in an indictment".
  /\bfaces?\s+(?:\w+\s+){0,2}?(?:criminal\s+)?(?:charges?|counts?|prosecution|indictment)\b/i,
  /\bunder\s+(?:criminal\s+|federal\s+|state\s+)?(?:indictment|investigation)\b/i,
  /\bnamed\s+in\s+(?:an?\s+)?(?:indictment|lawsuit|ethics\s+complaint)\b/i,
  /\baccused\s+of\b/i,
  /\ballegedly\b|\ballegations?\s+(?:of|that|against)\b/i,
  /\bethics\s+(?:violation|complaint|charge|probe|inquiry)s?\b/i,
  /\bresigned\s+(?:amid|following|after|in\s+disgrace)\b/i,
  /\bsexual\s+(?:harassment|assault|abuse|misconduct)\b/i,
  /\bfalsified\b/i,
  /\bconcealed\s+(?:contributions?|donations?|payments?|income|assets|funds)\b/i,
  // Corruption/misuse phrasings that skip enforcement verbs entirely:
  // "diverted public funds for personal use", "accepted bribes from
  // contractors", "committed campaign-finance fraud", adjudicated findings
  // ("found guilty/liable", "found probable cause"). Standalone crime nouns
  // stay excluded (legislation ABOUT crime), and the actor-exempt patterns
  // above still clear legislative/prosecutorial records.
  /\b(?:diverted|misused|misappropriated|embezzled|misspent)\b[^.;]{0,40}?\b(?:funds?|money|donations?|contributions?)\b/i,
  /\b(?:accepted|took|solicited|paid)\s+(?:\w+\s+){0,2}?bribes?\b|\bbribed\b/i,
  /\bcommitted\s+(?:[\w-]+\s+){0,3}?(?:fraud|perjury|misconduct|violations?)\b/i,
  // Pronoun-only gap ("found him liable" = the candidate); a named third
  // party ("found the caregiver guilty") deliberately does not match.
  /\bfound\s+(?:(?:him|her|them)\s+)?(?:guilty|liable)\b|\bfound\s+probable\s+cause\b/i,
];

function hostnameMatchesDomain(hostname: string, domain: string): boolean {
  return hostname === domain || hostname.endsWith(`.${domain}`);
}

function matchesAnyDomain(hostname: string, domains: readonly string[]): boolean {
  return domains.some((domain) => hostnameMatchesDomain(hostname, domain));
}

// One registry rather than a hand-maintained if-chain, so the lists and the
// resolver cannot drift apart and the mutual-exclusion invariant below is
// testable. Resolution is order-independent PROVIDED no hostname can match two
// registries — and because matching is suffix-based, that requires more than
// distinct strings: "civoren.com" in one registry and "www.civoren.com" in
// another are different strings that match the same host. The policy test
// asserts the stronger suffix-overlap property.
export const BLOCKED_SOURCE_DOMAIN_REGISTRY: Record<BlockedSourceKind, readonly string[]> = {
  bot_check_interstitial: BOT_CHECK_INTERSTITIAL_DOMAINS,
  generated_candidate_directory: GENERATED_CANDIDATE_DIRECTORY_DOMAINS,
  ugc_social: BLOCKED_SOURCE_DOMAINS,
};

function resolveBlockedSourceKind(hostname: string): BlockedSourceKind | null {
  for (const [kind, domains] of Object.entries(BLOCKED_SOURCE_DOMAIN_REGISTRY)) {
    if (matchesAnyDomain(hostname, domains)) {
      return kind as BlockedSourceKind;
    }
  }
  return null;
}

export function describeBlockedSource(hostname: string, kind: BlockedSourceKind): string {
  return `source domain '${hostname}' is ${BLOCKED_SOURCE_KIND_LABEL[kind]}; ${BLOCKED_SOURCE_KIND_REPAIR[kind]}`;
}

// Legacy state-government hostnames predating .gov migration, e.g.
// courts.state.mn.us, sos.state.tx.us.
const STATE_US_HOSTNAME_PATTERN = /(?:^|\.)state\.[a-z]{2}\.us$/;

export function classifyCandidateRecordSourceDomain(
  sourceUrl: string
): CandidateRecordSourceDomainClassification {
  let hostname: string;
  try {
    hostname = new URL(sourceUrl).hostname.toLowerCase().replace(/\.$/, "");
  } catch {
    // Callers validate URL shape before policy runs (normalizeHttpUrl in the
    // payload contract); an unparseable value here is classified unlisted and
    // left for the schema/reachability layers to reject.
    return { tier: "unlisted", hostname: "" };
  }

  const blockedKind = resolveBlockedSourceKind(hostname);
  if (blockedKind) {
    return { tier: "blocked", hostname, blockedKind };
  }

  if (
    hostname.endsWith(".gov") ||
    hostname.endsWith(".mil") ||
    STATE_US_HOSTNAME_PATTERN.test(hostname)
  ) {
    return { tier: "listed", hostname };
  }

  for (const domain of LISTED_SOURCE_DOMAINS) {
    if (hostnameMatchesDomain(hostname, domain)) {
      return { tier: "listed", hostname };
    }
  }

  return { tier: "unlisted", hostname };
}

// Sentence boundaries only (not ";"): an exemption earned in one sentence
// must not cancel an accusation made in another ("Sponsored a highway bill
// in 2019. Was indicted on bribery charges in 2024."). Semicolon-joined
// clauses stay in one segment so a legitimate compound like "filed ethics
// complaints against X; the commission found probable cause against X"
// keeps its exemption — the clause-anchored legislative patterns above
// already refuse to reach across ";". The uppercase lookahead keeps
// abbreviations ("Blanton Jr. and ...") from splitting a sentence away from
// its exemption.
const DAMAGING_CLAIM_SEGMENT_SPLIT = /(?<=[.!?])\s+(?=["'A-Z])/;

export function matchesDamagingClaimPattern(description: string): boolean {
  return description.split(DAMAGING_CLAIM_SEGMENT_SPLIT).some(
    (segment) =>
      DAMAGING_CLAIM_PATTERNS.some((pattern) => pattern.test(segment)) &&
      !DAMAGING_CLAIM_ACTOR_EXEMPT_PATTERNS.some((pattern) => pattern.test(segment))
  );
}

export function evaluateCandidateRecordSourcePolicy(input: {
  description: string;
  sourceUrl: string;
}): CandidateRecordSourcePolicyResult {
  const classification = classifyCandidateRecordSourceDomain(input.sourceUrl);
  const { tier, hostname } = classification;

  if (classification.tier === "blocked") {
    return {
      ok: false,
      reason: describeBlockedSource(hostname, classification.blockedKind),
    };
  }

  if (tier === "unlisted" && matchesDamagingClaimPattern(input.description)) {
    return {
      ok: false,
      reason: `damaging claim requires an official (.gov/court) or listed news source; '${hostname}' is not a listed source domain`,
    };
  }

  return { ok: true, tier };
}
