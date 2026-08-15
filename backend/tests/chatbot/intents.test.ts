import { describe, expect, it } from "vitest";

import { goldenSet } from "../../src/chatbot/golden/goldenSet.js";
import {
  detectBareStateReply,
  detectIntent,
  detectStateInQuestion,
  hasPersonalIssuesPhrase,
} from "../../src/chatbot/intents.js";

// The deterministic router IS two of the release gates (BEHAVIOR.md):
//   - 100% of `template` and `refuse_policy` cases route deterministically
//   - templates/policy never reach retrieval or an LLM
// Both are checkable against the golden set with no database at all.

describe("chatbot intent router vs the golden set", () => {
  it("routes every refuse_policy case to the policy refusal (rule 1)", () => {
    for (const goldenCase of goldenSet) {
      if (goldenCase.expected !== "refuse_policy") continue;
      expect(detectIntent(goldenCase.question)?.kind, goldenCase.id).toBe("policy_refusal");
    }
  });

  it("routes every template case to a deterministic non-policy intent", () => {
    for (const goldenCase of goldenSet) {
      if (goldenCase.expected !== "template") continue;
      const intent = detectIntent(goldenCase.question);
      expect(intent, goldenCase.id).not.toBeNull();
      expect(intent?.kind, goldenCase.id).not.toBe("policy_refusal");
    }
  });

  it("never routes retrieval cases to an intent (refusal/clarify intents excepted)", () => {
    for (const goldenCase of goldenSet) {
      if (goldenCase.expected === "template" || goldenCase.expected === "refuse_policy") continue;
      const intent = detectIntent(goldenCase.question);
      if (intent === null) continue;
      // Allowed exceptions, both deterministic non-answers: untracked-data
      // refusals (social posts) and scopeless time-sensitive clarifications.
      if (goldenCase.expected === "refuse_no_data") {
        expect(["untracked_data", "out_of_cycle"], goldenCase.id).toContain(intent.kind);
      } else {
        expect(goldenCase.expected, goldenCase.id).toBe("clarify");
        expect(intent.kind, goldenCase.id).toBe("needs_scope");
      }
    }
  });

  it("resolves the state for state-scoped logistics templates", () => {
    for (const goldenCase of goldenSet) {
      if (goldenCase.category !== "logistics" || !goldenCase.scopeState) continue;
      expect(detectIntent(goldenCase.question)?.state, goldenCase.id).toBe(goldenCase.scopeState);
    }
  });
});

describe("primary/runoff date questions never get the general-election date", () => {
  it("routes state-scoped primary/runoff date asks to other_election_date", () => {
    expect(detectIntent("When is the Texas primary election?")).toEqual({ kind: "other_election_date", state: "TX" });
    expect(detectIntent("When is the runoff in Georgia?")).toEqual({ kind: "other_election_date", state: "GA" });
    // Common phrasings beyond the "when is" frame.
    expect(detectIntent("What date is the Texas primary?")).toEqual({ kind: "other_election_date", state: "TX" });
    expect(detectIntent("Texas primary date?")).toEqual({ kind: "other_election_date", state: "TX" });
  });

  it("routes scopeless primary/runoff date asks to needs_scope (clarify)", () => {
    expect(detectIntent("When is the primary?")).toEqual({ kind: "needs_scope", state: null });
    expect(detectIntent("When is the runoff?")).toEqual({ kind: "needs_scope", state: null });
  });

  it("keeps plain general-election date asks on the fixed-date template", () => {
    expect(detectIntent("When is the 2026 general election?")?.kind).toBe("election_date");
  });

  it("routes countdown asks to election_countdown, but primary countdowns to the runoff path", () => {
    expect(detectIntent("How many days until the election?")?.kind).toBe("election_countdown");
    expect(detectIntent("how long until election day?")?.kind).toBe("election_countdown");
    expect(detectIntent("days left until the election")?.kind).toBe("election_countdown");
    // Primary/runoff countdowns must never get the general-election math.
    expect(detectIntent("How many days until the Texas primary?")).toEqual({
      kind: "other_election_date",
      state: "TX",
    });
    expect(detectIntent("how many days until the runoff?")?.kind).toBe("needs_scope");
    // Out-of-cycle years still refuse first.
    expect(detectIntent("how many days until the 2028 election?")?.kind).toBe("out_of_cycle");
  });
});

describe("smalltalk routes deterministically, whole message only", () => {
  it("greets on bare greetings", () => {
    for (const q of ["Hi", "hi!", "Hello", "hey there", "HI there", "Good morning", "what's up?", "yo"]) {
      expect(detectIntent(q), q).toEqual({ kind: "greeting", state: null });
    }
  });

  it("acknowledges thanks and goodbyes", () => {
    for (const q of ["Thanks", "thank you so much!", "thx", "appreciate it"]) {
      expect(detectIntent(q), q).toEqual({ kind: "thanks", state: null });
    }
    for (const q of ["Bye", "goodbye!", "see you later", "take care"]) {
      expect(detectIntent(q), q).toEqual({ kind: "goodbye", state: null });
    }
  });

  it("routes whole-message help asks to the capabilities template", () => {
    for (const q of ["help", "Help!", "what can you do?", "What can I ask?", "how does this work?", "who are you?"]) {
      expect(detectIntent(q), q).toEqual({ kind: "help", state: null });
    }
    // Embedded "help" is a real question, not a help ask.
    expect(detectIntent("help me find my polling place")?.kind).toBe("where_to_vote");
  });

  it("never eats a real question that starts with a greeting", () => {
    expect(detectIntent("hi, who is running in GA?")).toBeNull();
    expect(detectIntent("HI there, when is the election?")?.kind).toBe("election_date");
    expect(detectIntent("hello, do I need voter ID to vote?")?.kind).toBe("voter_id");
    // "thanks for nothing, when is the runoff?" still time-sensitive.
    expect(detectIntent("thanks, when is the runoff?")?.kind).toBe("needs_scope");
  });
});

describe("my-area questions route to the ballot lookup", () => {
  it("deep-links the saved ballot instead of refusing", () => {
    expect(detectIntent("who is running in my area?")?.kind).toBe("ballot_lookup");
    expect(detectIntent("what races are near me?")?.kind).toBe("ballot_lookup");
    expect(detectIntent("candidates for my district")?.kind).toBe("ballot_lookup");
  });

  it("does not shadow more specific frames", () => {
    // A date ask in "my area" is still a date ask (clarify, never a card).
    expect(detectIntent("when is the runoff in my area?")?.kind).toBe("needs_scope");
    expect(detectIntent("how do I register to vote in my state?")?.kind).toBe("voter_registration");
    // Named places are not "my area".
    expect(detectIntent("who is running for sheriff?")).toBeNull();
  });

  it("does not hijack substantive questions that merely mention 'my state/city'", () => {
    // Entity and issue questions belong to retrieval — a location phrase
    // alone is not a roster ask.
    expect(detectIntent("What has Jon Ossoff done in my state?")).toBeNull();
    expect(detectIntent("Which candidates support abortion rights in my state?")).toBeNull();
    expect(detectIntent("What are candidates saying about housing in my city?")).toBeNull();
  });
});

describe("personal-issues questions route to the saved-areas ballot match", () => {
  it("catches the preference frame in its common phrasings", () => {
    for (const q of [
      "which of these elections affect issues I care about?",
      "which races matter to me?",
      "what elections touch the topics that are important to me?",
      "which races line up with my top issues?",
      "does anything on the ballot match my priorities?",
      // Graded modifiers ("most important") — the round-5 live miss.
      "which race has most of my most important issues?",
      "what elections touch my biggest issues?",
    ]) {
      expect(detectIntent(q)?.kind, q).toBe("my_issues_ballot");
    }
  });

  it("wins over the plain ballot deep link when both frames appear", () => {
    expect(detectIntent("what's on my ballot that affects issues I care about?")?.kind).toBe("my_issues_ballot");
  });

  it("requires the election frame, not the preference phrase alone", () => {
    // Candidate questions that mention personal issues belong to retrieval —
    // the answer they want is about the candidate, not a race list.
    expect(detectIntent("Does Jane Smith support my key issues?")).toBeNull();
    expect(detectIntent("What has Jane Smith done on topics important to me?")).toBeNull();
    // Not a race question at all.
    expect(detectIntent("What issues do I care about?")).toBeNull();
  });

  it("leaves stance and location questions alone", () => {
    // "I care about X" without a preference noun in front is a stance ask.
    expect(detectIntent("I care about climate — which races does it affect?")).toBeNull();
    // Singular "my area" is a place, not a preference.
    expect(detectIntent("who is running in my area?")?.kind).toBe("ballot_lookup");
    // Endorsement asks still refuse even when phrased through issues.
    expect(detectIntent("based on the issues I care about, who should I vote for?")?.kind).toBe("policy_refusal");
  });
});

describe("hasPersonalIssuesPhrase (the refusal's Settings pointer)", () => {
  it("detects the phrase even when the election frame is missing", () => {
    // No frame word → the router deliberately stays out (could be a candidate
    // question, or not about races at all), but a no-data refusal on such a
    // question gains a Settings pointer instead of dead-ending.
    expect(hasPersonalIssuesPhrase("what should I focus on given my priorities?")).toBe(true);
    expect(detectIntent("what should I focus on given my priorities?")).toBeNull();
  });

  it("stays quiet on ordinary questions", () => {
    expect(hasPersonalIssuesPhrase("who is running for sheriff?")).toBe(false);
    expect(hasPersonalIssuesPhrase("what's on my ballot?")).toBe(false);
    expect(hasPersonalIssuesPhrase("what has Jon Ossoff done in my state?")).toBe(false);
  });
});

describe("detectBareStateReply", () => {
  it("accepts a message that is only a state", () => {
    expect(detectBareStateReply("California")).toBe("CA");
    expect(detectBareStateReply("california.")).toBe("CA");
    expect(detectBareStateReply("in California")).toBe("CA");
    expect(detectBareStateReply("I vote in Georgia")).toBe("GA");
    expect(detectBareStateReply("I live in north carolina")).toBe("NC");
    expect(detectBareStateReply("GA")).toBe("GA");
    expect(detectBareStateReply("Washington DC")).toBe("DC");
  });

  it("rejects anything with substance beyond the state", () => {
    expect(detectBareStateReply("who is running in California?")).toBeNull();
    expect(detectBareStateReply("California senate race")).toBeNull();
    // Lowercase two-letter tokens are words, not states.
    expect(detectBareStateReply("ok")).toBeNull();
    expect(detectBareStateReply("hi")).toBeNull();
    expect(detectBareStateReply("who is running for sheriff?")).toBeNull();
  });
});

describe("detectStateInQuestion", () => {
  it("prefers full state names and handles multi-word names", () => {
    expect(detectStateInQuestion("register to vote in North Carolina")).toBe("NC");
    expect(detectStateInQuestion("deadline in west virginia please")).toBe("WV");
    expect(detectStateInQuestion("what about virginia?")).toBe("VA");
  });

  it("resolves Washington, DC to DC, not Washington state", () => {
    expect(detectStateInQuestion("Where do I vote in Washington, DC?")).toBe("DC");
    expect(detectStateInQuestion("register in washington d.c. please")).toBe("DC");
    expect(detectStateInQuestion("Where do I vote in Washington DC?")).toBe("DC");
    expect(detectStateInQuestion("how do I vote in Washington state?")).toBe("WA");
  });

  it("accepts abbreviations only with place context, never city-ish LA", () => {
    expect(detectStateInQuestion("Atlanta GA 30303")).toBe("GA");
    expect(detectStateInQuestion("who is running in GA?")).toBe("GA");
    expect(detectStateInQuestion("the LA mayor race")).toBeNull();
    expect(detectStateInQuestion("who is running in ga?")).toBeNull();
  });

  it("does not read caps-words as states without place context", () => {
    // "voter ID" is not Idaho; "OK," is not Oklahoma; "HI" greeting is not Hawaii.
    expect(detectStateInQuestion("Do I need voter ID to vote?")).toBeNull();
    expect(detectStateInQuestion("OK, who is running for sheriff?")).toBeNull();
    expect(detectStateInQuestion("HI there, when is the election?")).toBeNull();
  });

  it("returns null when no state is named", () => {
    expect(detectStateInQuestion("who is running for sheriff?")).toBeNull();
  });
});
