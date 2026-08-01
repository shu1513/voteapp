import { describe, expect, it } from "vitest";

import {
  BLOCKED_SOURCE_DOMAIN_REGISTRY,
  classifyCandidateRecordSourceDomain,
  evaluateCandidateRecordSourcePolicy,
  matchesDamagingClaimPattern,
} from "../../src/pipeline/candidates/candidateRecordSourcePolicy.js";

describe("BLOCKED_SOURCE_DOMAIN_REGISTRY", () => {
  it("keeps the blocked registries mutually exclusive under SUFFIX matching", () => {
    // Distinct strings are not enough: matching is suffix-based, so
    // "civoren.com" in one registry and "www.civoren.com" in another would
    // both match the same host and make resolution order-dependent — silently
    // attaching the wrong repair instruction. Assert no domain in one registry
    // is equal to, or a subdomain of, a domain in any other.
    const entries = Object.entries(BLOCKED_SOURCE_DOMAIN_REGISTRY);
    for (const [kind, domains] of entries) {
      for (const [otherKind, otherDomains] of entries) {
        if (kind === otherKind) {
          continue;
        }
        for (const domain of domains) {
          for (const other of otherDomains) {
            const overlaps = domain === other || domain.endsWith(`.${other}`);
            expect(overlaps, `${kind}:${domain} overlaps ${otherKind}:${other}`).toBe(false);
          }
        }
      }
    }
  });

  it("has no duplicate domains inside a single registry", () => {
    for (const [kind, domains] of Object.entries(BLOCKED_SOURCE_DOMAIN_REGISTRY)) {
      expect(new Set(domains).size, `${kind} contains a duplicate`).toBe(domains.length);
    }
  });
});

describe("classifyCandidateRecordSourceDomain", () => {
  it("blocks social/UGC platforms including subdomains", () => {
    expect(classifyCandidateRecordSourceDomain("https://www.reddit.com/r/politics/x").tier).toBe(
      "blocked"
    );
    expect(classifyCandidateRecordSourceDomain("https://old.reddit.com/r/x").tier).toBe("blocked");
    expect(classifyCandidateRecordSourceDomain("https://x.com/user/status/1").tier).toBe("blocked");
    expect(classifyCandidateRecordSourceDomain("https://someone.substack.com/p/post").tier).toBe(
      "blocked"
    );
    expect(classifyCandidateRecordSourceDomain("https://youtu.be/abc").tier).toBe("blocked");
    expect(classifyCandidateRecordSourceDomain("https://t.co/abc").tier).toBe("blocked");
  });

  it("does not block lookalike domains by substring", () => {
    // xreddit.com is NOT reddit.com; suffix matching must anchor on a dot.
    expect(classifyCandidateRecordSourceDomain("https://xreddit.com/a").tier).toBe("unlisted");
    expect(classifyCandidateRecordSourceDomain("https://notx.company/a").tier).toBe("unlisted");
  });

  it("blocks additional fixed-domain UGC platforms", () => {
    for (const url of [
      "https://bsky.app/profile/someone",
      "https://nextdoor.com/p/abc",
      "https://www.twitch.tv/somestream",
      "https://www.youtube-nocookie.com/embed/abc",
      "https://www.snapchat.com/add/someone",
    ]) {
      expect(classifyCandidateRecordSourceDomain(url).tier, url).toBe("blocked");
    }
  });

  it("reports WHY a domain is blocked, not just that it is", () => {
    const ugc = classifyCandidateRecordSourceDomain("https://www.reddit.com/r/x");
    const directory = classifyCandidateRecordSourceDomain("https://www.civoren.com/candidate/x");
    const interstitial = classifyCandidateRecordSourceDomain(
      "https://validate.perfdrive.com/?ssc=https%3A%2F%2Fwww.sos.mn.gov%2Fnews%2Fx"
    );

    expect(ugc.tier === "blocked" && ugc.blockedKind).toBe("ugc_social");
    expect(directory.tier === "blocked" && directory.blockedKind).toBe(
      "generated_candidate_directory"
    );
    expect(interstitial.tier === "blocked" && interstitial.blockedKind).toBe(
      "bot_check_interstitial"
    );
  });

  it("blocks auto-generated candidate directories including subdomains", () => {
    expect(classifyCandidateRecordSourceDomain("https://www.civoren.com/candidate/x").tier).toBe(
      "blocked"
    );
    expect(classifyCandidateRecordSourceDomain("https://civoren.com/candidate/x").tier).toBe(
      "blocked"
    );
  });

  it("exempts the reviewed PAGES on institutional publishers", () => {
    // Each is one reviewed page on which the organisation reports its own act:
    // Equality Arizona's endorsement release, the State Bar of Arizona
    // magazine's article permalink, the Thurston coalition's award page.
    for (const url of [
      "https://equalityarizona.substack.com/p/equality-arizona-2026-primary-election",
      "https://azatty.wordpress.com/2014/10/07/ariz-court-reporting-changes-will-affect-attorneys/",
      "https://thurstonecc.wordpress.com/angel-award/",
    ]) {
      expect(classifyCandidateRecordSourceDomain(url).tier, url).toBe("unlisted");
    }
  });

  it("does NOT exempt other pages on the same exempt hosts", () => {
    // The whole point of a per-page exemption. A host-wide one would unblock
    // future posts, archives, tag pages and unrelated authors that nobody
    // reviewed — including the tag page this record used to cite.
    for (const url of [
      "https://equalityarizona.substack.com/p/unrelated-candidate-rumor",
      "https://azatty.wordpress.com/tag/politics/",
      "https://azatty.wordpress.com/tag/trial/",
      "https://thurstonecc.wordpress.com/new-post-by-unknown-author/",
      "https://thurstonecc.wordpress.com/",
    ]) {
      expect(classifyCandidateRecordSourceDomain(url).tier, url).toBe("blocked");
    }
  });

  it("matches an exempt page regardless of trailing slash, case or query", () => {
    // Stored citations arrive post-redirect and often carry tracking
    // parameters; the exemption must survive that without becoming loose.
    for (const url of [
      "https://thurstonecc.wordpress.com/angel-award",
      "https://thurstonecc.wordpress.com/angel-award/",
      "https://thurstonecc.wordpress.com/Angel-Award/?utm_source=newsletter",
      "https://thurstonecc.wordpress.com/angel-award/#winners",
    ]) {
      expect(classifyCandidateRecordSourceDomain(url).tier, url).toBe("unlisted");
    }
  });

  it("keeps the exemption off the platform, look-alikes and subdomains", () => {
    for (const url of [
      "https://someone.substack.com/p/post",
      "https://randomblog.wordpress.com/2020/01/01/post",
      "https://notequalityarizona.substack.com/p/equality-arizona-2026-primary-election",
      "https://evil.equalityarizona.substack.com/p/equality-arizona-2026-primary-election",
    ]) {
      expect(classifyCandidateRecordSourceDomain(url).tier, url).toBe("blocked");
    }
  });

  it("lifts only the ugc_social block, never another blocked kind", () => {
    // The exemption is resolved inside the matching kind. Were it checked
    // first, an exempt page would also bypass bot_check_interstitial and
    // generated_candidate_directory — kinds this exemption must not touch.
    const interstitial = classifyCandidateRecordSourceDomain(
      "https://validate.perfdrive.com/?ssc=https%3A%2F%2Fthurstonecc.wordpress.com%2Fangel-award"
    );
    expect(interstitial.tier).toBe("blocked");
    expect(interstitial.tier === "blocked" && interstitial.blockedKind).toBe(
      "bot_check_interstitial"
    );

    const directory = classifyCandidateRecordSourceDomain("https://www.civoren.com/candidate/x");
    expect(directory.tier).toBe("blocked");
    expect(directory.tier === "blocked" && directory.blockedKind).toBe(
      "generated_candidate_directory"
    );
  });

  it("treats dishonesty and theft accusations as damaging, anywhere", () => {
    // These carry no enforcement verb, so every earlier pattern missed them
    // and they were accepted on ANY unlisted domain — the exempted pages were
    // never special. An accusation aimed at a candidate belongs behind the
    // same listed-source requirement as an indictment.
    for (const description of [
      "She lied about her résumé and plagiarized her policy plan.",
      "Called the candidate a corrupt liar who stole taxpayer money.",
      "Was accused of lying about his military service.",
      "Siphoned campaign contributions into a personal account.",
    ]) {
      expect(matchesDamagingClaimPattern(description), description).toBe(true);
      expect(
        evaluateCandidateRecordSourcePolicy({
          description,
          sourceUrl: "https://smalltownweekly.com/news/1",
        }).ok,
        description
      ).toBe(false);
    }
  });

  it("does not flag the candidate accusing someone ELSE of dishonesty", () => {
    // Live corpus row: a column the candidate published. The accusation runs
    // outward, so it is his own public action, not a smear against him.
    for (const description of [
      "Published a signed political commentary column that accused national news organizations of lying to Americans about Crimea.",
      "Accused his opponent of lying about the budget during a debate.",
    ]) {
      expect(matchesDamagingClaimPattern(description), description).toBe(false);
    }
  });

  it("does not let an exempt page carry a claim the platform block used to stop", () => {
    // The damaging-claim detector is deliberately incomplete, so it cannot be
    // the only guard on an exempt page. Scoping the exemption to reviewed
    // pages is what keeps this text out.
    const smear = evaluateCandidateRecordSourcePolicy({
      description: "Called the candidate a corrupt liar who stole taxpayer money.",
      sourceUrl: "https://equalityarizona.substack.com/p/unrelated-candidate-rumor",
    });
    expect(smear.ok).toBe(false);
  });

  it("does not promote exempt hosts to listed, so damaging claims still fail", () => {
    // These are self-reporting advocacy and professional bodies, not
    // newsrooms. An ordinary factual claim is fine; an accusation is not.
    const ordinary = evaluateCandidateRecordSourcePolicy({
      description: "Equality Arizona endorsed her for State Senate in Legislative District 1.",
      sourceUrl: "https://equalityarizona.substack.com/p/equality-arizona-2026-primary-election",
    });
    expect(ordinary.ok).toBe(true);

    const damaging = evaluateCandidateRecordSourcePolicy({
      description: "Was indicted on bribery charges in March 2026.",
      sourceUrl: "https://equalityarizona.substack.com/p/equality-arizona-2026-primary-election",
    });
    expect(damaging.ok).toBe(false);
  });

  it("blocks bot-check interstitials", () => {
    expect(
      classifyCandidateRecordSourceDomain(
        "https://validate.perfdrive.com/?ssa=abc&ssc=https%3A%2F%2Fwww.sos.mn.gov%2Fnews%2Fx"
      ).tier
    ).toBe("blocked");
  });

  it("does not block lookalike directory or interstitial domains by substring", () => {
    expect(classifyCandidateRecordSourceDomain("https://notcivoren.com/a").tier).toBe("unlisted");
    expect(classifyCandidateRecordSourceDomain("https://perfdrive.com/a").tier).toBe("unlisted");
  });

  it("lists any .gov or .mil hostname", () => {
    expect(classifyCandidateRecordSourceDomain("https://sos.ca.gov/elections").tier).toBe("listed");
    expect(classifyCandidateRecordSourceDomain("https://www.congress.gov/bill/x").tier).toBe(
      "listed"
    );
    expect(classifyCandidateRecordSourceDomain("https://www.army.mil/article/1").tier).toBe(
      "listed"
    );
  });

  it("does not treat .gov.example lookalikes as listed", () => {
    expect(classifyCandidateRecordSourceDomain("https://sos.ca.gov.example.com/a").tier).toBe(
      "unlisted"
    );
  });

  it("lists legacy *.state.XX.us government hostnames", () => {
    expect(classifyCandidateRecordSourceDomain("https://www.courts.state.mn.us/x").tier).toBe(
      "listed"
    );
    expect(classifyCandidateRecordSourceDomain("https://sos.state.tx.us/x").tier).toBe("listed");
    expect(classifyCandidateRecordSourceDomain("https://fakestate.mn.us/x").tier).toBe("unlisted");
  });

  it("lists curated news and civic-data domains including subdomains", () => {
    expect(classifyCandidateRecordSourceDomain("https://apnews.com/article/x").tier).toBe("listed");
    expect(classifyCandidateRecordSourceDomain("https://www.nytimes.com/2026/x").tier).toBe(
      "listed"
    );
    expect(classifyCandidateRecordSourceDomain("https://ballotpedia.org/Jane_Doe").tier).toBe(
      "listed"
    );
    expect(classifyCandidateRecordSourceDomain("https://abcnews.go.com/Politics/x").tier).toBe(
      "listed"
    );
  });

  it("classifies everything else as unlisted (accepted), including Wikipedia", () => {
    expect(classifyCandidateRecordSourceDomain("https://en.wikipedia.org/wiki/Jane_Doe").tier).toBe(
      "unlisted"
    );
    expect(classifyCandidateRecordSourceDomain("https://smalltownweekly.com/news/1").tier).toBe(
      "unlisted"
    );
  });

  it("is case-insensitive on hostnames and tolerates trailing dots", () => {
    expect(classifyCandidateRecordSourceDomain("https://WWW.REDDIT.COM/r/x").tier).toBe("blocked");
    expect(classifyCandidateRecordSourceDomain("https://sos.ca.gov./elections").tier).toBe(
      "listed"
    );
  });

  it("classifies unparseable input as unlisted without throwing", () => {
    expect(classifyCandidateRecordSourceDomain("not a url").tier).toBe("unlisted");
  });
});

describe("matchesDamagingClaimPattern", () => {
  it("matches accusation/enforcement content aimed at the candidate", () => {
    const damaging = [
      "Was indicted on federal bribery charges in 2024.",
      "Pleaded guilty to one count of wire fraud.",
      "Was fined by the state ethics commission for late disclosures.",
      "Accused of misusing campaign funds for personal travel.",
      "Faces an ethics complaint over undisclosed gifts.",
      "Resigned amid a procurement scandal.",
      "Was censured by the city council in 2023.",
      "Allegedly concealed contributions from a state contractor.",
      "Charged with two counts of felony theft.",
      "Was sued over unpaid campaign vendor invoices.",
      "Falsified timesheets while serving as county clerk.",
      "Settled a sexual harassment complaint filed by a former aide.",
      "Diverted public funds for personal use while serving as treasurer.",
      "Accepted bribes from contractors seeking city permits.",
      "An ethics commission found that she committed campaign-finance fraud.",
      "Is under investigation for corruption in the licensing office.",
      "Now faces three counts of felony fraud.",
      "Remains under indictment on federal charges.",
      "Was named in an indictment for wire fraud.",
      "A jury found him liable for defamation.",
    ];
    for (const description of damaging) {
      expect(matchesDamagingClaimPattern(description), description).toBe(true);
    }
  });

  it("does not match candidate-as-official records about third-party misconduct (live-corpus false positives)", () => {
    const actorRecords = [
      "Judge Christopher M. Blount sentenced JuJuan Parks to 46 to 60 years in prison after Parks pleaded guilty to second-degree murder.",
      "Holliday co-prosecuted a caregiver who was convicted of exploiting an 81-year-old woman.",
      "Sponsored HB 5551, barring people convicted of specified election crimes from serving on canvassing boards.",
      "Introduced 2023 House Bill 4121, permanently revoking the license of a health professional convicted of sexual conduct.",
      "Voted for an expanded interim policy covering nondiscrimination, harassment, and sexual harassment.",
      "Zeigler filed ethics complaints in 2016 against Governor Robert Bentley.",
      "Alongside Representative Gina Mitten, publicly released an ethics complaint alleging campaign-finance violations.",
      "Imposed the maximum 10-year sentence on a man convicted of second-degree assault.",
      "Has served as a Senior Deputy Prosecutor handling domestic violence, child abuse, and sexual assault cases.",
    ];
    for (const description of actorRecords) {
      expect(matchesDamagingClaimPattern(description), description).toBe(false);
    }
  });

  it("does not match the Sexual Assault Nurse Examiner credential", () => {
    // Live wave-19 false positive: SANE is a nursing certification, not an
    // accusation. The lookahead skips only the credential phrase — any other
    // "sexual assault"/"sexual misconduct" wording in the same sentence still
    // matches, which a whole-sentence exemption pattern could not guarantee.
    const credential = [
      "Became certified as a Sexual Assault Nurse Examiner at Providence Alaska Medical Center.",
      "Trained sexual assault nurse examiners across rural clinics.",
    ];
    for (const description of credential) {
      expect(matchesDamagingClaimPattern(description), description).toBe(false);
    }

    const stillDamaging = [
      "Was accused of sexual assault by a former staffer.",
      "While working as a sexual assault nurse examiner, faced sexual misconduct allegations.",
      // The credential exemption must not swallow a pronoun-object
      // accusation — the base pattern only matched this sentence's job
      // title by accident, so the accusation itself needs its own pattern.
      "A sexual assault nurse examiner accused him of misconduct.",
    ];
    for (const description of stillDamaging) {
      expect(matchesDamagingClaimPattern(description), description).toBe(true);
    }
  });

  it("matches a pronoun-object accusation and keeps the candidate-as-accuser exemption", () => {
    // Pronoun object = the candidate; no prior pattern covered "accused
    // <pronoun> of" (\baccused\s+of\b needs adjacency), so these passed as
    // non-damaging even without any other trigger word.
    const damaging = [
      "A former aide accused him of misconduct.",
      "Accused her of diverting funds during the audit.",
      "Colleagues accused them of harassment in 2022.",
      // The dishonesty exemption's word-gap must not clear a pronoun object.
      "A watchdog group accused him of lying to voters about the budget.",
    ];
    for (const description of damaging) {
      expect(matchesDamagingClaimPattern(description), description).toBe(true);
    }

    const accuser = [
      "Accused national news organizations of lying to Americans about Crimea.",
      "Accused her opponent of lying about endorsements.",
    ];
    for (const description of accuser) {
      expect(matchesDamagingClaimPattern(description), description).toBe(false);
    }
  });

  it("still matches accusations against the candidate even in official contexts", () => {
    const damaging = [
      "While a sitting judge, was censured by the judicial conduct commission.",
      "Was convicted of felony forgery in 2018 while a sitting councilmember.",
      "The former prosecutor pleaded guilty to falsifying case records.",
      // Passive "was sentenced" is about the candidate; the judge-as-actor
      // exemption must not cancel it (bare \bsentenced\b bypass).
      "Was sentenced to 18 months in prison for tax fraud.",
      "Pleaded guilty to campaign-finance violations and was sentenced to probation.",
      // Mixed descriptions: a legislative action in one sentence must not
      // cancel a personal accusation in another (or a ";"-joined clause).
      "Sponsored a highway funding bill in 2019. Was indicted on bribery charges in 2024.",
      "Voted for the state budget; was accused of diverting campaign donations.",
      "Introduced a parks bill in 2021. Faces two counts of felony fraud.",
    ];
    for (const description of damaging) {
      expect(matchesDamagingClaimPattern(description), description).toBe(true);
    }
  });

  it("does not match legitimate actions BY the candidate or legislation about crime", () => {
    const benign = [
      "As comptroller, audited state agencies and published annual reports.",
      "As district attorney, convicted 50 violent offenders.",
      "Sheriff's office arrested the suspects within a week under her direction.",
      "Sponsored a bill increasing penalties for embezzlement and fraud.",
      "Voted for the concealed carry permit reform bill.",
      "Charged with leading the city's homelessness task force.",
      "Sued the federal government over water rights on behalf of the state.",
      "Chaired the House ethics committee for two terms.",
      "Investigated consumer complaints as head of the agency.",
      "Fined polluters a record $2 million as attorney general.",
      "Sponsored a bill barring officials who diverted public funds from future office.",
      "Voted to strengthen penalties for officials who accepted bribes.",
      "Faces a well-funded challenger in the primary.",
    ];
    for (const description of benign) {
      expect(matchesDamagingClaimPattern(description), description).toBe(false);
    }
  });
});

describe("evaluateCandidateRecordSourcePolicy", () => {
  it("rejects blocked domains regardless of description", () => {
    const result = evaluateCandidateRecordSourcePolicy({
      description: "Voted for the state budget in 2025.",
      sourceUrl: "https://www.reddit.com/r/politics/comments/abc",
    });
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.reason).toContain("user-generated/social platform");
      expect(result.reason).toContain("www.reddit.com");
    }
  });

  it("tells the caller which blocked class it hit so the fix is actionable", () => {
    const directory = evaluateCandidateRecordSourcePolicy({
      description: "Served as a city council member from 2018 to 2022.",
      sourceUrl: "https://www.civoren.com/candidate/some-person",
    });
    expect(directory.ok).toBe(false);
    if (!directory.ok) {
      expect(directory.reason).toContain("auto-generated candidate directory");
      expect(directory.reason).toContain("lead only");
      expect(directory.reason).not.toContain("user-generated/social platform");
    }

    const interstitial = evaluateCandidateRecordSourcePolicy({
      description: "Appointed a new elections director in 2025.",
      sourceUrl:
        "https://validate.perfdrive.com/?ssa=abc&ssc=https%3A%2F%2Fwww.sos.mn.gov%2Fnews%2Fx",
    });
    expect(interstitial.ok).toBe(false);
    if (!interstitial.ok) {
      expect(interstitial.reason).toContain("bot-check interstitial");
      expect(interstitial.reason).toContain("ssc=");
    }
  });

  it("rejects damaging claims sourced only to unlisted domains", () => {
    const result = evaluateCandidateRecordSourcePolicy({
      description: "Was indicted on bribery charges in March 2026.",
      sourceUrl: "https://patriot-eagle-news-watch.com/exclusive",
    });
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.reason).toContain("damaging claim");
      expect(result.reason).toContain("patriot-eagle-news-watch.com");
    }
  });

  it("accepts damaging claims from listed sources", () => {
    const official = evaluateCandidateRecordSourcePolicy({
      description: "Was indicted on bribery charges in March 2026.",
      sourceUrl: "https://www.justice.gov/usao/pr/indictment",
    });
    expect(official).toEqual({ ok: true, tier: "listed" });

    const news = evaluateCandidateRecordSourcePolicy({
      description: "Was censured by the state senate over misuse of funds.",
      sourceUrl: "https://apnews.com/article/censure",
    });
    expect(news).toEqual({ ok: true, tier: "listed" });
  });

  it("accepts neutral records from unlisted domains", () => {
    const result = evaluateCandidateRecordSourcePolicy({
      description: "Served two terms on the Maplewood school board.",
      sourceUrl: "https://smalltownweekly.com/news/school-board",
    });
    expect(result).toEqual({ ok: true, tier: "unlisted" });
  });

  it("rejects the candidate's own campaign or personal site when the display name is provided", () => {
    // Every shape below is a live host from the 307-row November incident.
    const owned: Array<[string, string]> = [
      ["https://www.sarahernandez.com/dolores_huerta", "Sara Hernandez"],
      ["https://www.electscott.com/about", "Scott Sakakihara"], // FIRST name
      ["https://www.electjimmooney.com", 'James Vernon "Jim" Mooney Jr.'], // nickname + suffix
      ["https://electlo.com/platform", "William Lo"], // two-letter surname
      ["https://halpinforillinois.com/news", "Michael W. Halpin"],
      ["https://senatorhalpin.com/about", "Michael W. Halpin"],
      ["https://www.billmoskalforhd80.com", "William Moskal"], // nickname not in display name
      ["https://wendyhoyforchange.com", "Wendy Hoy"],
      ["https://www.saradeenforca.com", "Sara Deen"],
      ["https://votecouch.com", "David Couch"],
      ["https://reppauljacobs.com", "Paul Jacobs"],
      ["https://www.tcmueller.com", "Tamiko T.C. Mueller"],
    ];
    for (const [sourceUrl, candidateDisplayName] of owned) {
      const result = evaluateCandidateRecordSourcePolicy({
        description: "Voted for the annual budget.",
        sourceUrl,
        candidateDisplayName,
      });
      expect(result.ok, `${sourceUrl} should be rejected for ${candidateDisplayName}`).toBe(false);
    }
  });

  it("never flags independent publishers whose names merely contain a name token", () => {
    const independent: Array<[string, string]> = [
      ["https://aberdeennews.com/story", "Sara Deen"], // "deen" inside "aberdeen"
      ["https://www.fordfoundation.org/report", "Gerald Ford"], // "ford" then no "for" tail
      ["https://votecommongood.com/candidates", "Leigh Estes"], // vote-prefix, no name tokens
      ["https://www.vote411.org/ballot", "Sara Hernandez"], // listed civic domain
      ["https://justfacts.votesmart.org/candidate/1", "William Smart"], // listed civic domain
      ["https://smalltownweekly.com/news", "Small Town"],
      // Front-anchored prefix coincidences: the name token starts the label
      // and "for" continues an unrelated word. The tail gate must hold on
      // the FRONT branch too, not just mid-label.
      ["https://www.hartfordcourant.com/politics/x", "Blake Hart"], // hart|ford…
      ["https://californiainsider.com/story", "Cali Binks"], // cali|fornia…
      ["https://marchforourlives.org/about", "Tom March"], // "for our lives" is a cause, not an office
      ["https://fosterforward.org/programs", "Bill Foster"], // foster|forward
      ["https://johnhartford.com/bio", "John Hart"], // john+hart|ford — two tokens consumed
      // English words ending in "city" must not satisfy the <place>county
      // composition rule — that rule is county-only on purpose.
      ["https://marchforpublicity.org/services", "Tom March"],
      ["https://smithforelectricity.org/co-op", "John Smith"],
    ];
    for (const [sourceUrl, candidateDisplayName] of independent) {
      const result = evaluateCandidateRecordSourcePolicy({
        description: "Voted for the annual budget.",
        sourceUrl,
        candidateDisplayName,
      });
      expect(result.ok, `${sourceUrl} must stay acceptable for ${candidateDisplayName}`).toBe(true);
    }

    // Without a display name the check simply does not run.
    expect(
      evaluateCandidateRecordSourcePolicy({
        description: "Voted for the annual budget.",
        sourceUrl: "https://www.sarahernandez.com/about",
      }).ok
    ).toBe(true);
  });

  it("rejects meeting/agenda index pages on every tier", () => {
    // Live case: reachable, HTTPS, correct official domain — and a JS nav
    // list of every meeting 2019-2025 that carries no claim at all.
    const portal = evaluateCandidateRecordSourcePolicy({
      description: "Cast a dissenting vote on a personnel item; it carried 4-3 over her objection.",
      sourceUrl: "https://laccd.community.diligentoneplatform.com/Portal/MeetingInformation.aspx?Id=67",
    });
    expect(portal.ok).toBe(false);
    if (!portal.ok) {
      expect(portal.reason).toMatch(/index page/);
    }

    // Bare path-end index forms, .gov included — trust tier is irrelevant.
    for (const url of [
      "https://www.cityofexample.gov/meetings",
      "https://council.example.org/agendas/",
      "https://borough.example.gov/calendar",
      "https://borough.example.gov/calendars",
      "https://city.example.com/minutes?year=2026",
      // A year-month segment is a MONTHLY index — only a full date (with the
      // day) addresses one meeting's materials.
      "https://city.example.gov/2026/08/calendar",
    ]) {
      expect(evaluateCandidateRecordSourcePolicy({ description: "Voted no on the item.", sourceUrl: url }).ok).toBe(
        false
      );
    }

    // Deeper paths under those segments are real documents and must pass —
    // and a DATE-bearing segment anywhere in the path addresses one meeting's
    // materials even when the path still ENDS in an index word.
    for (const url of [
      "https://www.cityofexample.gov/minutes/2024-06-12.pdf",
      "https://council.example.org/agendas/2026/agenda-packet-06-12.pdf",
      "https://laccd.community.diligentoneplatform.com/document/4969",
      "https://city.example.gov/2024/06/12/minutes",
      "https://city.example.gov/city-council/2024-06-12/agenda",
    ]) {
      expect(evaluateCandidateRecordSourcePolicy({ description: "Voted no on the item.", sourceUrl: url }).ok).toBe(
        true
      );
    }

    // A selector-style query routes an index path to a specific item on many
    // CMSes — those are detail pages, not indexes. Navigation-only queries
    // (paging, sorting, date windows) keep the page an index.
    for (const url of [
      "https://www.cityofexample.gov/calendar?event=123",
      "https://council.example.org/meetings?id=42",
    ]) {
      expect(evaluateCandidateRecordSourcePolicy({ description: "Voted no on the item.", sourceUrl: url }).ok).toBe(
        true
      );
    }
    for (const url of [
      "https://www.cityofexample.gov/minutes?page=2",
      "https://council.example.org/meetings?year=2026&sort=desc",
      // Tracking parameters say nothing about what the page shows — the URL
      // still cites the index.
      "https://www.cityofexample.gov/minutes?utm_source=email&utm_campaign=x",
      "https://council.example.org/calendar?fbclid=abc123",
      // MeetingInformation.aspx stays rejected even with a selector: the live
      // case carried ?Id=67 and was still a JavaScript nav shell.
      "https://laccd.community.diligentoneplatform.com/Portal/MeetingInformation.aspx?Id=99",
    ]) {
      expect(evaluateCandidateRecordSourcePolicy({ description: "Voted no on the item.", sourceUrl: url }).ok).toBe(
        false
      );
    }
  });

  it("gates every surname+for branch on an office/district/state tail", () => {
    // True campaign compositions — nickname or initial hides the name from
    // front consumption, but the tail is an office, district, state, place,
    // or campaign-cause word from the corpus census.
    for (const [sourceUrl, candidateDisplayName] of [
      ["https://www.billmoskalforhd80.com", "William Moskal"],
      ["https://www.ashawforsenate.com/about", "Abraham Shaw"],
      ["https://www.leslyeforwestchester.com", "Leslye A. Oquendo-Thomas"], // curated place
      ["https://www.frankforchange.com", "Frank William Collige"], // campaign-cause word
      ["https://waynerogersforalsecretaryofstate.com", "Wayne Rogers"], // state abbrev + office compound
      ["https://www.cooleyfor35.com", "Jeff Cooley"], // bare district number
      ["https://www.millerforjacksoncounty.com", "Tony Miller"], // <place>county composition
      ["https://www.billmoskalforhd80.co.uk", "William Moskal"], // multi-part TLD
    ] as const) {
      expect(
        evaluateCandidateRecordSourcePolicy({
          description: "Voted for the annual budget.",
          sourceUrl,
          candidateDisplayName,
        }).ok,
        `${sourceUrl} should be rejected for ${candidateDisplayName}`
      ).toBe(false);
    }

    // Substring coincidences: the surname appears mid-word and the "for" tail
    // is not an office — independent organizations, not campaign sites.
    for (const [sourceUrl, candidateDisplayName] of [
      ["https://stanfordfordemocracy.org/report", "Gerald Ford"], // "ford" inside "stanford"
      ["https://www.stanfordforum.org/events", "Gerald Ford"], // tail "um" is no office
    ] as const) {
      expect(
        evaluateCandidateRecordSourcePolicy({
          description: "Voted for the annual budget.",
          sourceUrl,
          candidateDisplayName,
        }).ok,
        `${sourceUrl} must stay acceptable for ${candidateDisplayName}`
      ).toBe(true);
    }
  });
});
