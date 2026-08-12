import { describe, expect, it } from "vitest";

import { goldenSet } from "../../src/chatbot/golden/goldenSet.js";
import { detectBareStateReply, detectIntent, detectStateInQuestion } from "../../src/chatbot/intents.js";

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
