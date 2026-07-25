import { describe, expect, it } from "vitest";

import {
  classifyCandidateRecordSourceDomain,
  evaluateCandidateRecordSourcePolicy,
  matchesDamagingClaimPattern,
} from "../../src/pipeline/candidates/candidateRecordSourcePolicy.js";

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

  it("still matches accusations against the candidate even in official contexts", () => {
    const damaging = [
      "While a sitting judge, was censured by the judicial conduct commission.",
      "Was convicted of felony forgery in 2018 while a sitting councilmember.",
      "The former prosecutor pleaded guilty to falsifying case records.",
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
});
