import { describe, expect, it } from "vitest";

import {
  listPlainLanguageWarnings,
  PLAIN_LANGUAGE_MAX_SENTENCE_WORDS,
} from "../../src/pipeline/candidates/candidateRecordPlainLanguageLint.js";

function words(count: number): string {
  return Array.from({ length: count }, (_, index) => `word${index}`).join(" ");
}

describe("listPlainLanguageWarnings", () => {
  it("passes short plain records", () => {
    const records = [
      { description: "Voted no on raising the state gas tax. The Senate passed it 24-12." },
      {
        description:
          "Wrote a law creating a bottle-deposit enforcement fund. Governor Whitmer signed it as Public Act 139 of 2021.",
      },
    ];
    expect(listPlainLanguageWarnings(records)).toEqual([]);
  });

  it("passes normal semicolon compounds (canonical gate example)", () => {
    const records = [
      {
        description:
          "Voted no on a $26,349,041 contract with the county sheriff's office for campus security; the motion failed 2-5.",
      },
    ];
    expect(listPlainLanguageWarnings(records)).toEqual([]);
  });

  it("does not split on semicolons or dashes: a run-on hiding behind them still warns", () => {
    // Regression for the calibration finding: an 80-word minutes dump made of
    // ~30-word semicolon clauses must flag as ONE overlong sentence.
    const runOn = `${words(30)}; ${words(30)} - ${words(25)}`;
    const warnings = listPlainLanguageWarnings([{ description: `${runOn}. The motions passed.` }]);
    expect(warnings).toHaveLength(1);
    // 30 + 30 + 25 words plus the free-standing dash token.
    expect(warnings[0]?.wordCount).toBe(86);
  });

  it("warns on a sentence over the word limit with index, count, and bounded excerpt", () => {
    const warnings = listPlainLanguageWarnings([
      { description: "Voted yes on the 2025 budget. It passed 6-1." },
      { description: `${words(PLAIN_LANGUAGE_MAX_SENTENCE_WORDS + 5)}.` },
    ]);
    expect(warnings).toHaveLength(1);
    expect(warnings[0]?.recordIndex).toBe(1);
    expect(warnings[0]?.wordCount).toBe(PLAIN_LANGUAGE_MAX_SENTENCE_WORDS + 5);
    expect(warnings[0]?.sentenceExcerpt.length).toBeLessThanOrEqual(90);
  });

  it("reports one warning per record even when several sentences are long", () => {
    const warnings = listPlainLanguageWarnings([
      { description: `${words(45)}. ${words(50)}.` },
    ]);
    expect(warnings).toHaveLength(1);
    expect(warnings[0]?.wordCount).toBe(50);
  });

  it("does not warn at exactly the limit", () => {
    expect(
      listPlainLanguageWarnings([{ description: `${words(PLAIN_LANGUAGE_MAX_SENTENCE_WORDS)}.` }])
    ).toEqual([]);
  });

  it("respects a custom threshold", () => {
    const records = [{ description: `${words(30)}.` }];
    expect(listPlainLanguageWarnings(records, 25)).toHaveLength(1);
    expect(listPlainLanguageWarnings(records, 35)).toEqual([]);
  });
});
