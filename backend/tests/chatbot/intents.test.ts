import { describe, expect, it } from "vitest";

import { goldenSet } from "../../src/chatbot/golden/goldenSet.js";
import { detectIntent, detectStateInQuestion } from "../../src/chatbot/intents.js";

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

describe("detectStateInQuestion", () => {
  it("prefers full state names and handles multi-word names", () => {
    expect(detectStateInQuestion("register to vote in North Carolina")).toBe("NC");
    expect(detectStateInQuestion("deadline in west virginia please")).toBe("WV");
    expect(detectStateInQuestion("what about virginia?")).toBe("VA");
  });

  it("accepts standalone uppercase abbreviations but not city-ish LA", () => {
    expect(detectStateInQuestion("Atlanta GA 30303")).toBe("GA");
    expect(detectStateInQuestion("the LA mayor race")).toBeNull();
    expect(detectStateInQuestion("who is running in ga?")).toBeNull();
  });

  it("returns null when no state is named", () => {
    expect(detectStateInQuestion("who is running for sheriff?")).toBeNull();
  });
});
