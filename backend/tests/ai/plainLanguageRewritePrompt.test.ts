import { describe, expect, it } from "vitest";

import { buildPlainLanguageRewritePrompt } from "../../src/ai/providers/plainLanguageRewritePrompt.js";
import { buildPlainLanguageRewriteVerifyPrompt } from "../../src/ai/providers/plainLanguageRewriteVerifyPrompt.js";
import { PLAIN_LANGUAGE_STYLE_RULES } from "../../src/ai/providers/promptWritingStyle.js";

describe("buildPlainLanguageRewritePrompt", () => {
  it("includes the shared style rules and the never-change-facts rules", () => {
    const prompt = buildPlainLanguageRewritePrompt({
      kind: "record_description",
      text: "Repeatedly rebuffed subpoenas to appear before the commission.",
    });

    for (const rule of PLAIN_LANGUAGE_STYLE_RULES) {
      expect(prompt).toContain(rule);
    }
    expect(prompt).toContain("rewrite the wording only, never the facts");
    expect(prompt).toContain("keep every name, amount, date, and outcome exactly as stated");
    expect(prompt).toContain("Keep every sentence's content; only the wording changes.");
    expect(prompt).not.toContain("Remove clauses that name the contest");
  });

  it("allows contest and horse-race removal only for candidate summaries", () => {
    const prompt = buildPlainLanguageRewritePrompt({
      kind: "candidate_summary",
      text: "Jane Doe is running for Sheriff and led the primary with 42.04%.",
      contestContext: {
        officialBallotTitle: "Sheriff",
        districtName: "Los Angeles County, California",
        electionDate: "2026-11-03",
      },
    });

    expect(prompt).toContain('- official_ballot_title: "Sheriff"');
    expect(prompt).toContain("Remove clauses that name the contest shown above");
    expect(prompt).toContain("the ONLY permitted content removal");
    expect(prompt).toContain("Do not mention the candidacy, the primary, the runoff, or the general election at all");
    expect(prompt).toContain(
      "Offices the person currently holds or previously held are facts to keep — including the exact office name, place, and district, and that they are the incumbent."
    );
    expect(prompt).toContain("If nothing substantive remains after those removals");
    expect(prompt).not.toContain("Keep every sentence's content; only the wording changes.");
  });
});

describe("buildPlainLanguageRewriteVerifyPrompt", () => {
  it("demands strict claim-by-claim comparison with doubt as mismatch", () => {
    const prompt = buildPlainLanguageRewriteVerifyPrompt({
      kind: "record_description",
      originalText: "original",
      rewrittenText: "rewrite",
    });

    expect(prompt).toContain("You did not write the rewrite. Judge it strictly.");
    expect(prompt).toContain("same direction (for/against, won/lost, yes/no)");
    expect(prompt).toContain('A lost negation, a flipped stance, a changed number, a changed name, or a new or dropped factual claim is a "mismatch".');
    expect(prompt).toContain('When in doubt, answer "mismatch".');
    expect(prompt).toContain("The rewrite may not drop any claim");
    expect(prompt).not.toContain("ALLOWED — and expected — to drop everything");
  });

  it("allows contest-clause drops only for candidate summaries", () => {
    const prompt = buildPlainLanguageRewriteVerifyPrompt({
      kind: "candidate_summary",
      originalText: "original",
      rewrittenText: "rewrite",
    });

    expect(prompt).toContain("the rewrite is ALLOWED — and expected — to drop everything about the contest the candidate is currently seeking");
    expect(prompt).toContain('"The current X" and "the incumbent X" state the same fact.');
    expect(prompt).toContain("current and past offices held, career history, and qualifications must be preserved");
    expect(prompt).not.toContain("The rewrite may not drop any claim");
  });
});
