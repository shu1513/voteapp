export type CandidateRecordSourceTier = "blocked" | "listed" | "unlisted";

export type CandidateRecordSourceDomainClassification = {
  tier: CandidateRecordSourceTier;
  hostname: string;
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
  /\b(?:sponsored|co-sponsored|introduced|authored)\b/i,
  /\bvoted\s+(?:for|against|to)\b/i,
  /\b(?:co-)?prosecuted\b/i,
  /\bsentenced\b/i,
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
  /\b(?:was|were|been)\s+(?:indicted|arrested|convicted|fined|sanctioned|censured|disbarred|impeached|recalled|sued)\b/i,
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

  for (const domain of BLOCKED_SOURCE_DOMAINS) {
    if (hostnameMatchesDomain(hostname, domain)) {
      return { tier: "blocked", hostname };
    }
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

export function matchesDamagingClaimPattern(description: string): boolean {
  if (!DAMAGING_CLAIM_PATTERNS.some((pattern) => pattern.test(description))) {
    return false;
  }
  return !DAMAGING_CLAIM_ACTOR_EXEMPT_PATTERNS.some((pattern) => pattern.test(description));
}

export function evaluateCandidateRecordSourcePolicy(input: {
  description: string;
  sourceUrl: string;
}): CandidateRecordSourcePolicyResult {
  const { tier, hostname } = classifyCandidateRecordSourceDomain(input.sourceUrl);

  if (tier === "blocked") {
    return {
      ok: false,
      reason: `source domain '${hostname}' is a user-generated/social platform; cite an official (.gov/court), news, or research-grade source instead`,
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
