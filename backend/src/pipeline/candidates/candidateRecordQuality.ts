export type CandidateRecordQualityClass =
  | "substantive"
  | "neutral_context"
  | "disallowed_thin";

export type CandidateRecordQualityReason =
  | "actual_record_action"
  | "fallback_context"
  | "pure_candidacy"
  | "future_promise"
  | "unclassified_context";

export type CandidateRecordQualityInput = {
  description: string;
  sourceUrl?: string | null;
};

export type CandidateRecordQualityResult = {
  classification: CandidateRecordQualityClass;
  reason: CandidateRecordQualityReason;
};

const PURE_CANDIDACY_PATTERNS = [
  /\b(?:is|was|are|were)\s+(?:a\s+)?candidate\s+for\b/i,
  /\b(?:running|ran)\s+for\b/i,
  /\bfiled\s+(?:paperwork\s+)?(?:to\s+run|as\s+a\s+candidate|for\s+office)\b/i,
  /\bqualified\s+for\s+(?:the\s+)?ballot\b/i,
  /\bappears?\s+on\s+(?:the\s+)?ballot\b/i,
  /\blist(?:s|ed)\s+(?:by|on|as)?\b.*\b(?:candidate|ballot|roster)\b/i,
  /\b(?:candidate|campaign)\s+(?:announcement|launch|filing)\b/i,
  /\bdeclared\s+(?:a\s+)?candidacy\b/i,
  // Routine candidacy-machinery filings are candidacy facts, not governance
  // records: a periodic campaign-finance report, the ballot-qualifying fee,
  // and qualifying as a candidate are steps every filer takes to run — 38
  // Orange County FL candidates were made to look researched by "filed a P2
  // campaign-finance report" rows. The filing pattern is article-anchored
  // ("filed a/the/his ... report") so a participle noun phrase like
  // "A filed campaign-finance statement ... reported a contribution from a
  // sitting judge" — a real integrity record — does not match. Substantive
  // verbs are checked first, so legislation ABOUT fees, reports, or
  // "qualifying" programs (health plans, veterans) stays substantive, and
  // finance-filing MISCONDUCT ("concealed", "illegally reimbursed",
  // "fined") is rescued by the past-tense misconduct verbs in
  // SUBSTANTIVE_ACTION_PATTERNS. The qualifying-fee pattern is deliberately
  // verb-unanchored: fee waivers, payments, and disputes are candidacy
  // machinery unless an enforcement/misconduct verb makes them a record
  // (verified against the full live corpus — zero legitimate rows match).
  /\bfiled\s+(?:a|an|the|his|her|its|their)\s+(?:[\w-]+\s+){0,3}?campaign[-\s]finance\s+(?:report|statement|disclosure)s?\b/i,
  /\bqualifying\s+fees?\b/i,
  /\bqualified\s+as\s+an?\s+(?:[\w-]+\s+){0,3}?candidate\b/i,
  // Primary/nomination RESULTS are roster evidence, not records: a November
  // state-legislative repair pass found 22 live "Won the Democratic primary
  // for X with N votes" rows across 22 candidates — for nine of them it was
  // the candidate's ONLY row. Deliberately anchored on primary/runoff-stage
  // words: winning a GENERAL or special election confers office and stays a
  // service fact (and rows pairing a primary win with a real action are
  // rescued by the substantive-verbs-first ordering). "advanced to the
  // general election" is the same fact phrased from the other side.
  // "primary" must head its noun phrase — followed by a preposition,
  // punctuation, the end, or an election word — because as an adjective it
  // describes something else entirely: "lost its primary care clinic",
  // "won a primary school construction award" are real service records.
  // Gap widened {0,5}→{0,7}: "Won the 2026 Alabama House District 25
  // Democratic primary" carries six qualifier words and slipped through at
  // five (live row, retired 2026-08-04).
  /\b(?:won|lost)\s+(?:the\s+|an?\s+)?(?:[\w'’.,-]+\s+){0,7}?primar(?:y|ies)\b(?=\s*(?:for\b|in\b|on\b|to\b|against\b|with\b|by\b|and\b|over\b|at\b|[.;,)]|$)|\s+(?:elections?|runoffs?|contests?|races?|bid|nominations?)\b)/i,
  // Runoffs split by stage the same way. LOSING any runoff is roster
  // evidence (losing confers nothing). WINNING one is only candidacy when a
  // party adjective marks it as a primary runoff — general runoffs are
  // nonpartisan and winning one confers office ("Won the 2021 Anchorage
  // mayoral runoff ... beginning a three-year term as mayor" is a live true
  // record). "won the Democratic primary runoff" is already caught by the
  // primary pattern above.
  /\blost\s+(?:the\s+|an?\s+)?(?:[\w'’.,-]+\s+){0,5}?runoffs?\b/i,
  /\bwon\s+(?:the\s+|an?\s+)?(?:\d{4}\s+)?(?:Democratic|Republican|GOP|Libertarian|Green)\s+runoffs?\b/i,
  /\badvanced\s+to\s+the\s+(?:\w+\s+){0,2}?general\s+election\b/i,
  // "filed as THE Republican candidate" — the older pattern above demanded
  // the literal article "a", so party-qualified filings slipped through.
  // Gap words are capitalized (party names) or known qualifiers ONLY, and
  // never digit-led: "the sworn financial disclosure she filed as a 2026
  // candidate for Alaska House District 8" is an APOC employment record whose
  // "filed" object is the disclosure, and "2026" is what distinguishes it.
  // Case-sensitive on purpose (no /i): the gap class relies on [A-Z] to mean
  // "a party name", so the verb's own casing is spelled out instead.
  /\b[Ff]iled\s+[Aa]s\s+(?:[Aa]n?|[Tt]he)\s+(?:(?:[A-Z][\w.'’-]*|qualified|incumbent|write-in|independent|nonpartisan|party)\s+){0,4}?candidate\b/,
  // Election-office qualification language ("qualified by fee on June 8,
  // 2026", "Division of Elections recorded X as the … candidate", "remains
  // active on the general-election candidate list") is exactly the
  // "election-office listing" the prompt calls roster evidence. "candidate"
  // must head its noun phrase — followed by for/in/on, punctuation, or the
  // end — because as an attributive adjective it describes something else
  // entirely: "the ethics commission recorded the payment as an illegal
  // candidate contribution" is an enforcement record, not a candidacy row.
  /\bqualified\s+by\s+(?:fee|petition)\b/i,
  /\brecorded\b[^.;]{0,60}\bas\b[^.;]{0,40}\bcandidate\b(?=\s*(?:for\b|in\b|on\b|[.;,)]|$))/i,
  // The exemption is HIRING nouns only: "a candidate list OF FINALISTS for
  // the police chief job" is an appointment process, but "candidate list of
  // the Democratic Party" is still an election roster — a bare "of" gap
  // exempted those too.
  /\bcandidate\s+list\b(?!\s+of\s+(?:semi)?finalists?\b|\s+of\s+applicants?\b)/i,
  // Candidacy/nominee STATUS restatements: "is the qualified incumbent
  // Republican candidate in the 2026 general election", "is the Democratic
  // nominee for county surrogate", "is the sole printed Republican candidate
  // for Senate District 15". A 2026-08-04 database-wide repair pass retired
  // fifteen live rows of this shape — all ballot qualification, none conduct.
  // Same head-noun anchoring as the recorded-as pattern above: "candidate/
  // nominee" must be followed by for/in, punctuation, or the end, so an
  // attributive use ("was the first candidate to publish tax returns" —
  // "candidate to" — or a hiring-process "leading candidate to succeed the
  // chief") stays untouched. Gap tokens borrow the filed-as pattern's
  // technique above — capitalized words (party names), years, or known
  // qualifiers ONLY, at least one of them, case-sensitive with the verbs'
  // casing spelled out — because ANY-word gaps swallowed non-electoral
  // nominations (review findings on this PR): "was the governor's nominee
  // for U.S. Attorney" is an appointment record ("governor's" is lowercase
  // and possessive, so it is not a party token), and "was the nominee in
  // the Best Documentary category" is an award (a bare zero-gap "the
  // nominee" no longer matches; every corpus true positive carries a party
  // or qualifier word).
  /\b(?:[Ii]s|[Ww]as|[Rr]emains)\s+the\s+(?:(?:[A-Z][\w.'’-]*|\d{4}|qualified|certified|incumbent|incumbent-position|sole|printed|presumptive|apparent|write-in|independent|nonpartisan|party)\s+){1,4}?(?:candidate|nominee)\b(?=\s*(?:for\b|in\b|[.;,)]|$))/,
  // The candidacy-machinery FILING documents by name. These nouns are ballot
  // access whatever verb carries them ("filed a Statement of Candidacy with
  // the FEC", "ruling her declaration of candidacy invalid", "filed her
  // candidate affidavit", "a challenge to her affidavit of identity") —
  // substantive-verbs-first ordering still rescues a row that pairs one with
  // a real completed action.
  /\bstatement\s+of\s+candidacy\b/i,
  /\bdeclaration\s+of\s+candidacy\b/i,
  /\bcandidate\s+affidavit\b/i,
  /\baffidavit\s+of\s+(?:identity|candidacy)\b/i,
  // Ballot-access outcomes: being removed from, kept off, or disqualified
  // from a ballot is a proceeding about candidacy, which references/records.md
  // bars as a record in either direction. Two shapes, both PERSON-anchored so
  // a MEASURE kept off the ballot (a live petition-drive record: "the measure
  // was kept off the ballot over canvasser paperwork") stays untouched:
  // active with a pronoun or capitalized-name object ("kept him off the
  // August 6, 2024 Democratic primary ballot", "removed Beavers from the 2022
  // primary ballot") — case-sensitive so "removed the measure from" cannot
  // match, and measure NAMES ("kept Initiative 976 off") are excluded from
  // the name class — and passive with the candidacy verbs but NOT "kept",
  // whose passive subject in the corpus is always the measure. The passive
  // form also refuses a measure-noun subject ("her ballot initiative was
  // removed from the 2026 ballot" is measure advocacy, not the candidate's
  // ballot access — review finding on this PR). An official's act about a
  // ballot is rescued by substantive-verbs-first ordering.
  /\b(?:removed|kept|barred|struck|stricken)\s+(?:him|her|them|(?!(?:Initiative|Measure|Amendment|Proposition|Referendum|Question|Ordinance)\b)[A-Z][\w'’-]+)\s+(?:off|from)\s+the\s+[^.;]{0,60}?\bballot\b/,
  /(?<!\b(?:measures?|initiatives?|amendments?|referendums?|referenda|propositions?|proposals?|questions?|petitions?|ordinances?|levy|levies|bonds?|issues?)\s)\b(?:was|were)\s+(?:removed|stricken|disqualified|barred)\s+from\s+the\s+(?:[\w'’.,-]+\s+){0,6}?ballot\b/i,
] as const;

// "promises/pledges/vows" is a verb in "She pledges lower taxes" and a noun in
// "unfunded promises", "Wilson's promises", "a series of pledges", "should not
// make pledges". Earlier versions tried to enumerate the words that precede
// the NOUN sense; that list can never be complete, and every gap in it
// silently REJECTED a real record — the worst direction for this gate to fail.
//
// Inverted here: a promissory word counts as a verb only when a plausible
// SUBJECT precedes it — sentence start, a pronoun, one of the nouns that
// actually head these sentences, or a capitalized name. Anything unrecognized
// is treated as the noun sense, so an unfamiliar construction leaves the
// record alone instead of suppressing it.
//
// The capitalized-name branch deliberately excludes possessives by omitting
// the apostrophe from the character class: "Wilson's promises" cannot match,
// because a match would have to begin at the lowercase "s". Capitalized
// determiners and conjunctions are excluded explicitly, so "The promises of
// reform" stays a noun.
const SUBJECT_BEFORE_PROMISSORY =
  /(?:^|[.;:]\s+|\b(?:[Hh]e|[Ss]he|[Tt]hey|[Ii]t|[Ww]ho|[Ww]hich|profile|[Cc]andidate|platform|website|page|biography|statement|incumbent|challenger|nominee|[Gg]overnor|[Mm]ayor|[Ss]enator|[Rr]epresentative)\s+|\b(?!The\b|A\b|An\b|His\b|Her\b|Its\b|Their\b|Our\b|This\b|That\b|These\b|Those\b|Of\b|In\b|On\b|For\b|From\b|About\b|And\b|But\b|As\b|No\b|Any\b)[A-Z][a-zA-Z.-]*[a-zA-Z]\s+)$/;

// Case-insensitive so a sentence-initial "Promises a full audit ..." is seen;
// SUBJECT_BEFORE_PROMISSORY above stays case-SENSITIVE on purpose, since
// capitalization is the signal that distinguishes a name from a common noun.
const PROMISSORY_WORD_SCAN = /\b(?:promis(?:es|ed|ing)|pledg(?:es|ed|ing)|vow(?:s|ed|ing))\b/gi;

function isPromissoryVerbUse(text: string, matchIndex: number): boolean {
  return SUBJECT_BEFORE_PROMISSORY.test(text.slice(0, matchIndex));
}

function containsPromissoryVerbUse(value: string): boolean {
  PROMISSORY_WORD_SCAN.lastIndex = 0;
  let match: RegExpExecArray | null;
  while ((match = PROMISSORY_WORD_SCAN.exec(value)) !== null) {
    if (isPromissoryVerbUse(value, match.index)) {
      return true;
    }
  }
  return false;
}

// Signing an advocacy organization's pledge is the promise itself in written
// form — a commitment to future conduct, which references/records.md line 148
// bars as a record ("drop the pledge clause, keep the completed action").
// This REVERSES an earlier decision, pinned in a test, that "signing a pledge
// IS a completed action": on 2026-08-04 the user approved retiring all 16
// live signed-pledge rows — including the test's own example, "Cloud signed
// the U.S. Term Limits convention pledge." — so the write gate now agrees
// with the reject list. Mechanics mirror the promissory complements below:
// the signing phrase is MASKED before the substantive check (so "signed"
// cannot vouch for the row) and the raw text is then matched as a future
// promise. A row that pairs the pledge with an independent completed action
// keeps its other verb and survives, same as every masked family.
//
// The object is "pledge" or "convention commitment" — not bare "commitment",
// which appears in real records ("signed a sister-city commitment"). The gap
// tokens allow dots and apostrophes because the canonical offender is
// "U.S. Term Limits' convention pledge". The gap must not contain a
// LEGISLATIVE object: in "Signed a law honoring the term-limits pledge he
// made in 2020" the thing signed is the law, the pledge is downstream of it,
// and masking the phrase would eat the completed action (review finding on
// this PR — the promissory-infinitive mask happens to rescue "...pledge to
// expand X", but any non-infinitive tail rejected the row).
const PLEDGE_SIGNING_PATTERN =
  /\bsign(?:ed|s|ing)\s+(?:(?!(?:bills?|laws?|legislation|orders?|ordinances?|resolutions?|measures?|acts?|budgets?|statutes?|proclamations?|contracts?|agreements?|treaty|treaties|letters?|vetoes?)\b)[\w.,'’&-]+\s+){0,7}?(?:pledges?|convention\s+commitments?)\b/gi;

function withoutPledgeSignings(value: string): string {
  PLEDGE_SIGNING_PATTERN.lastIndex = 0;
  return value.replace(PLEDGE_SIGNING_PATTERN, " ");
}

const FUTURE_PROMISE_PATTERNS = [
  /\b(?:campaign|platform|website)\b(?!-).*\b(?:promises?|pledges?|vows?|plans?|proposes?)\b/i,
  // Stance verbs joined the promissory ones on 2026-08-04: a database-wide
  // repair pass retired 28 platform rows shaped "the campaign supports X" /
  // "her published platform supported Z", none of which the promissory verbs
  // caught. Unlike the promissory pattern above, the verb must sit WITHIN TWO
  // words of the campaign/platform/website subject — a corpus sweep showed
  // loose cooccurrence flags real records ("during his 2022 campaign and
  // after taking office, Sorrell supported transferring public examiners" is
  // in-office advocacy). The noun exclusions keep enforcement and machinery
  // rows anchored on the wrong noun out: a "campaign committee" under PDC
  // enforcement, a "campaign contribution" accepted, a "campaign-finance
  // report" (also the (?!-) compound guard, pinned by the Karen McDonald
  // test), a "campaign event" someone hosted, a "campaign biography" that
  // merely describes a career. The indefinite-article lookbehind excludes an
  // ISSUE campaign someone ran as real work — "launched a Transparency in
  // Plea Bargain campaign calling for prosecutorial disclosure" is an
  // advocacy record — but an electoral campaign IS sometimes "a campaign"
  // ("launched a gubernatorial campaign supporting X", review finding on
  // this PR), so the exemption's gap tokens must not be electoral markers or
  // a year, which put the phrase back inside the pattern.
  /(?<!\ban?\s(?:(?!(?:gubernatorial|mayoral|senatorial|presidential|congressional|legislative|judicial|electoral|political|statewide|citywide|countywide|primary|general|write-in|reelection|re-election|senate|house|assembly|council|\d+)\b)[\w'’-]+\s){0,4})\b(?:campaign|platform|website)(?:'s)?\b(?!-)(?!\s+(?:events?|rall(?:y|ies)|kickoffs?|stops?|committees?|contributions?|finance|reports?|statements?|biograph(?:y|ies)|mailers?|ads?|advertisements?)\b)(?:\s+[\w'’-]+){0,2}?\s+(?:supports?|supported|supporting|opposes?|opposed|opposing|calls?\s+for|called\s+for|calling\s+for|prioritizes?|prioritized|prioritizing|backs\b)/i,
  // "identifies" needs a priority-shaped object as well as proximity:
  // "campaign identifies safeguarding civil rights ... as a current policy
  // priority" is platform, while "campaign biography identifies him as
  // founder of ..." is a career descriptor.
  /(?<!\ban?\s(?:(?!(?:gubernatorial|mayoral|senatorial|presidential|congressional|legislative|judicial|electoral|political|statewide|citywide|countywide|primary|general|write-in|reelection|re-election|senate|house|assembly|council|\d+)\b)[\w'’-]+\s){0,4})\b(?:campaign|platform|website)(?:'s)?\b(?!-)(?:\s+[\w'’-]+){0,2}?\s+identif(?:ies|ied)\b[^.;]{0,160}\b(?:priorit|concerns?\b|issues?\b|focus)/i,
  new RegExp(PLEDGE_SIGNING_PATTERN.source, "i"),
  // Past-tense promissory verbs are still promises, in any position:
  // "Promised as a judicial candidate to uphold ..." slipped past the
  // present-tense handling and became a canonical record. Substantive
  // completed-action verbs are matched first, so a description that pairs a
  // real action with its promise is still kept.
  //
  // The lookbehinds exclude the ATTRIBUTIVE participle where the promise
  // demonstrably belongs to someone else: a timeshare-fraud suit described as
  // "without delivering the promised service" was rejected as the candidate's
  // own future promise. Articles ("a/an/the promised X"), "its" (a person
  // cannot be an "its", so the possessor is always an org), and hyphenated
  // compound modifiers ("long-promised reforms") are safe — a verb use never
  // follows any of them.
  //
  // PERSONAL possessives are deliberately NOT excluded: descriptions are
  // third-person about the candidate, so "outlined his promised tax cuts" is
  // usually the candidate's own promise ("their" includes singular-they
  // candidates; "my/your" would be the speaker's own promise in a quote).
  // "that promised X" also stays rejected — "that" is indistinguishable from
  // the relativizer in "a company that promised refunds", a real verb use.
  /(?<!\b(?:a|an|the|its)\s)(?<!\w-)\b(?:promised|pledged|vowed)\b/i,
  /\b(?:says|said)\s+(?:he|she|they)\s+(?:will|would)\b/i,
  /\b(?:will|would)\s+(?:fight|work|cut|raise|support|oppose|create|expand|reduce|protect)\b/i,
] as const;

// Keeping, breaking or abandoning a promise is a COMPLETED action and one of
// the more voter-relevant records there is — "broke a pledge a month later" is
// an integrity record, not a campaign promise.
//
// Matched against the MASKED text, alongside the other substantive patterns.
// An earlier version ran these against the raw description first, which let a
// promise ABOUT someone else's broken pledge pass as a completed action:
// "promised to investigate whether the governor broke his campaign pledge"
// scored substantive on the governor's conduct. Masking removes the promise
// clause before these are consulted, so only an outcome the record itself
// asserts survives. The masking leaves these intact because "broke A pledge"
// is the noun sense and is never treated as a promissory verb use.
//
// Object-anchored on purpose: bare "kept"/"broke" are far too common
// ("broke ground", "kept the seat") to treat as promise outcomes.
const PROMISE_OUTCOME_PATTERNS = [
  /\b(?:kept|broke|fulfilled|honored|honoured|violated|abandoned|reversed)\s+(?:(?:a|an|the|his|her|its|their|our|multiple|several|\d+)\s+)?(?:campaign\s+|signed\s+)?(?:promises?|pledges?|vows?|commitments?)\b/i,
  /\breneged\s+on\s+(?:(?:a|an|the|his|her|its|their|our)\s+)?(?:campaign\s+)?(?:promises?|pledges?|vows?|commitments?)\b/i,
] as const;

const SUBSTANTIVE_ACTION_PATTERNS = [
  /\b(?:voted|signed|vetoed|sponsored|co-sponsored|introduced|authored|passed|enacted)\b/i,
  /\b(?:issued|ordered|appointed|oversaw|implemented|managed|directed|founded|led|chaired)\b/i,
  /\b(?:served|serves|serving)\s+as\b/i,
  // Seat-holding service without "as": "has served on the board since 2022".
  // Rescues rows that pair real service with a primary-result clause — the
  // candidacy patterns below would otherwise drop the whole row.
  /\b(?:served|serves|serving)\s+on\s+the\s+(?:board|council|commission|committee)\b/i,
  // Present-tense chairing is current service, same as "serves as": without
  // it, "chairs the Vermont Commission on Women and was the 2024 Democratic
  // nominee" lost its real service to the nominee-status pattern below.
  // Determiner-anchored so the plural noun ("arranged the chairs") is safe.
  /\b(?:co-)?chairs\s+(?:the|a|an|its)\b/i,
  /\b(?:held|holds)\s+(?:public\s+)?office\b/i,
  /\b(?:was|were|is|are)\s+elected\s+to\b/i,
  /\b(?:ruled|sentenced|prosecuted|defended|settled|sued)\b/i,
  // Misconduct/enforcement verbs: campaign-finance wrongdoing is a real
  // integrity record even when its sentence also matches a candidacy-
  // machinery pattern ("filed a false campaign-finance report that
  // CONCEALED a contribution", "illegally REIMBURSED the qualifying fee").
  // Past-tense forms only — a tenseless adjective ("illegal", "false")
  // would rescue future promises like "will fight illegal dumping" out of
  // future_promise, which the past-tense anchoring of this whole list
  // deliberately prevents. "reimbursed" is additionally adverb-anchored:
  // bare "reimbursed" is routine campaign bookkeeping and must not rescue
  // machinery rows like "the campaign reimbursed the qualifying fee".
  /\b(?:fined|charged|indicted|convicted|sanctioned|censured|investigated|audited|falsified|concealed|misreported|omitted)\b/i,
  /\b(?:illegally|improperly|unlawfully)\s+reimbursed\b/i,
  /\b(?:endorsed|received\s+an?\s+endorsement)\b/i,
  /\b(?:published|released)\s+(?:a\s+)?(?:report|study|audit|decision|opinion)\b/i,
] as const;

const FALLBACK_CONTEXT_PATTERNS = [
  /\b(?:biography|bio|profile)\b/i,
  /\b(?:graduated|earned\s+(?:a\s+)?degree|attended)\b/i,
  /\b(?:worked|works)\s+as\b/i,
  /\b(?:occupation|profession|professional\s+background)\b/i,
] as const;

function normalizeDescription(value: string): string {
  return value.trim().replace(/\s+/g, " ");
}

function matchesAny(value: string, patterns: readonly RegExp[]): boolean {
  return patterns.some((pattern) => pattern.test(value));
}

// A substantive verb inside a promissory infinitive complement is not a
// completed action: in "promised to veto any tax increase passed by the
// legislature", both "veto" and "passed" describe the promise's content.
// Blank the complement through the end of its clause (period/semicolon/comma)
// before the substantive check; the promise patterns still see the original
// text. A clause that mixes a promise with a real action in the same
// unpunctuated breath loses the action and rejects — the operator then leads
// with the completed action, which is the phrasing the contract wants anyway.
const PROMISSORY_INFINITIVE_PATTERN =
  /\b(?:promis(?:es?|ed|ing)|pledg(?:es?|ed|ing)|vow(?:s|ed|ing)?)\s+(?:\w+\s+){0,3}?to\s+[^.;,]*/gi;

// The same reasoning applies to a promise with a NOUN object: in "pledges a
// ban on contractors convicted of fraud" the contractors were convicted, not
// the candidate, and in "promises a commission led by an independent chair"
// nobody has led anything yet. Both classified as substantive purely because a
// completed-action verb appeared inside the promised thing.
//
// Only the -s verb forms and unambiguous -ed/-ing forms are masked, behind the
// same determiner lookbehind the promise patterns use, so the NOUN sense is
// left alone: "signed the taxpayer protection pledge" must keep its "signed".
// The promised thing is frequently a LIST — "promises a ban on convicted
// contractors, a commission led by independent experts" — and stopping at the
// first comma left later items exposed, so "led" in the second item scored the
// whole prospective sentence as substantive.
//
// Continuation across a comma is allowed only while the next item still looks
// like part of the list (an optional and/or, then a determiner or a number).
// A new clause starts with a subject instead, which is what keeps the action
// in "As promised during the campaign, she sponsored the bill" intact — an
// earlier version consumed commas unconditionally and swallowed "sponsored".
const PROMISED_OBJECT_TAIL = /^[^.;,]*(?:,\s*(?:and\s+|or\s+)?(?:an?|the|\d)\b[^.;,]*)*/;

function withoutPromissoryComplements(value: string): string {
  const withoutInfinitives = value.replace(PROMISSORY_INFINITIVE_PATTERN, " ");

  let result = "";
  let cursor = 0;
  PROMISSORY_WORD_SCAN.lastIndex = 0;
  let match: RegExpExecArray | null;
  while ((match = PROMISSORY_WORD_SCAN.exec(withoutInfinitives)) !== null) {
    if (match.index < cursor || !isPromissoryVerbUse(withoutInfinitives, match.index)) {
      continue;
    }
    const afterWord = match.index + match[0].length;
    const rest = withoutInfinitives.slice(afterWord);
    if (/^\s+to\b/.test(rest)) {
      continue; // infinitive complements are handled above
    }
    const objectLength = rest.match(PROMISED_OBJECT_TAIL)?.[0].length ?? 0;
    result += `${withoutInfinitives.slice(cursor, match.index)} `;
    cursor = afterWord + objectLength;
    PROMISSORY_WORD_SCAN.lastIndex = cursor;
  }
  return result + withoutInfinitives.slice(cursor);
}

export function classifyCandidateRecordQuality(
  input: CandidateRecordQualityInput
): CandidateRecordQualityResult {
  const description = normalizeDescription(input.description);
  if (description.length === 0) {
    return { classification: "disallowed_thin", reason: "unclassified_context" };
  }

  // Both completed-action families are judged on the masked text, so a verb
  // sitting inside a promised thing — or the "signed" of a signed pledge —
  // cannot vouch for the record.
  const withoutPromises = withoutPledgeSignings(withoutPromissoryComplements(description));
  if (
    matchesAny(withoutPromises, PROMISE_OUTCOME_PATTERNS) ||
    matchesAny(withoutPromises, SUBSTANTIVE_ACTION_PATTERNS)
  ) {
    return { classification: "substantive", reason: "actual_record_action" };
  }

  if (matchesAny(description, PURE_CANDIDACY_PATTERNS)) {
    return { classification: "disallowed_thin", reason: "pure_candidacy" };
  }

  // The verb-use scan replaces the old "pledges TO" and noun-object patterns,
  // which is what stops "should not make pledges to decide pending cases" —
  // a noun, and the opposite of a promise — from being rejected as one.
  if (matchesAny(description, FUTURE_PROMISE_PATTERNS) || containsPromissoryVerbUse(description)) {
    return { classification: "disallowed_thin", reason: "future_promise" };
  }

  if (matchesAny(description, FALLBACK_CONTEXT_PATTERNS)) {
    return { classification: "neutral_context", reason: "fallback_context" };
  }

  return { classification: "neutral_context", reason: "unclassified_context" };
}

export function isDisallowedThinCandidateRecord(input: CandidateRecordQualityInput): boolean {
  return classifyCandidateRecordQuality(input).classification === "disallowed_thin";
}
