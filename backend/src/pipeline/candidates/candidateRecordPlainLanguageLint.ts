// Warn-only plain-language lint for candidate-record descriptions.
//
// The plain-language gate (PLAIN_LANGUAGE_STYLE_RULES in
// src/ai/providers/promptWritingStyle.ts) is enforced by the researcher, not
// by any validator — and a 1000-row corpus audit (2026-08-13) showed it gets
// skipped in practice: manually-written rows were no more readable than
// legacy rows. This lint is the one wording check that needs no meaning
// judgment: counting words in a sentence. Anything semantic (does the
// description state the bill's plain effect? is the item trivia?) stays a
// human/AI judgment — an earlier materiality regex sweep false-positived on
// 50 of 54 rows and is deliberately not repeated here.
//
// Contract with the writer: warnings never block a write. The operator
// policy (documented in the manual-research skill) is one rewrite bounce;
// a row that still warns after its rewrite is accepted as-is.

export type PlainLanguageWarning = {
  recordIndex: number;
  wordCount: number;
  sentenceExcerpt: string;
};

// Calibrated against 300 human-verified GOOD rows (a 100-row tuning set and
// a 200-row fresh holdout) plus the full 1000-row audit sample (2026-08-13):
// sentence-level splitting at >45 words flags 2-2.5% of verified-good rows —
// every flag a genuine run-on — while catching both known-egregious rows (a
// 60-word legalese opinion summary, an 80-word minutes dump). 40 words
// flagged 8% of the holdout; clause-level splitting (on ;/—) at 25 words was
// tried first and REJECTED: 21% false flags AND it let the egregious rows
// pass as several short clauses.
export const PLAIN_LANGUAGE_MAX_SENTENCE_WORDS = 45;

const EXCERPT_MAX_LENGTH = 90;

// Sentences end at . ! ? or a newline — semicolons and dashes do NOT split,
// on purpose: a 47-word semicolon compound is exactly the heavy reading the
// warn should surface. Abbreviation and decimal dots ("U.S.", "$30.2")
// over-split, which only shortens fragments — a safe direction for a
// warn-only check (it can suppress a warning, never invent one).
const SENTENCE_BREAK = /[.!?\n]+/;

function countWords(sentence: string): number {
  return sentence.split(/\s+/).filter((token) => token.length > 0).length;
}

function excerpt(sentence: string): string {
  const trimmed = sentence.trim();
  return trimmed.length <= EXCERPT_MAX_LENGTH
    ? trimmed
    : `${trimmed.slice(0, EXCERPT_MAX_LENGTH - 1)}…`;
}

/**
 * Returns one warning per record for its longest over-limit sentence (a row
 * with several long sentences needs one rewrite, not several warnings).
 */
export function listPlainLanguageWarnings(
  records: ReadonlyArray<{ description: string }>,
  maxSentenceWords: number = PLAIN_LANGUAGE_MAX_SENTENCE_WORDS
): PlainLanguageWarning[] {
  const warnings: PlainLanguageWarning[] = [];
  records.forEach((record, recordIndex) => {
    let worst: { wordCount: number; sentence: string } | null = null;
    for (const sentence of record.description.split(SENTENCE_BREAK)) {
      const wordCount = countWords(sentence);
      if (wordCount > maxSentenceWords && (!worst || wordCount > worst.wordCount)) {
        worst = { wordCount, sentence };
      }
    }
    if (worst) {
      warnings.push({
        recordIndex,
        wordCount: worst.wordCount,
        sentenceExcerpt: excerpt(worst.sentence),
      });
    }
  });
  return warnings;
}
