// "Did you mean …?" for misspelled candidate names — pure functions, no I/O.
//
// The entity branch (retrieval.ts) collects fuzzy name matches down to
// word_similarity 0.45, but the answerability gate demands 0.75, so a typo
// like "Jon Osoff" lands in between and the question would flatly refuse.
// Before refusing, the ask service checks whether some question SPAN is
// close to a matched candidate's WHOLE name and offers those as a
// clarification instead.
//
// Whole-name similarity is the discriminator word_similarity lacks:
// word_similarity("Tim Taylor", "…Taylor Swift…") ≈ 0.7 because one word
// matches exactly, but "taylor swift" vs "tim taylor" as whole strings share
// far fewer trigrams (≈0.41). Genuine typos ("jon osoff" vs "jon ossoff",
// ≈0.75) stay high. The trigram math mirrors pg_trgm: lowercase, split into
// alphanumeric words, pad each with two leading spaces and one trailing,
// similarity = shared/union.

import { firstLastKey, type CandidateEntityMatch } from "./retrieval.js";

export const SUGGESTION_MIN_SIMILARITY = 0.5;
const MAX_SUGGESTIONS = 3;
const SPAN_MAX_WORDS = 3;

// Function/question words never part of a person's name. Spans are runs of
// consecutive NON-stop tokens, so "is Taylor" (which scores 0.5 against
// "Tim Taylor") can never form a span for the Taylor Swift question.
const STOPWORDS = new Set([
  "a", "an", "the", "is", "are", "was", "were", "be", "been",
  "who", "whos", "what", "whats", "when", "where", "which", "how", "why",
  "do", "does", "did", "has", "have", "had", "can", "could", "will", "would",
  "of", "in", "for", "on", "to", "at", "by", "from", "with", "about",
  "and", "or", "not", "no",
  "i", "me", "my", "you", "your", "we", "us",
  "tell", "more", "much", "many", "please",
  "s", "d", "t", "m", "ll", "re", "ve",
]);

/** Shared tokenizer: diacritics stripped (NFD), lowercased, Unicode letters/
 * digits kept whole — "José Muñoz" tokenizes as ["jose", "munoz"], not the
 * ASCII shrapnel ["jos", "mu", "oz"], so unaccented typing still matches
 * accented names. */
function tokensOf(text: string): string[] {
  return (
    text
      .normalize("NFD")
      .replace(/\p{M}+/gu, "")
      .toLowerCase()
      .match(/[\p{L}\p{N}]+/gu) ?? []
  );
}

function trigramsOf(text: string): Set<string> {
  const trigrams = new Set<string>();
  const words = tokensOf(text);
  for (const word of words) {
    const padded = `  ${word} `;
    for (let i = 0; i + 3 <= padded.length; i += 1) {
      trigrams.add(padded.slice(i, i + 3));
    }
  }
  return trigrams;
}

/** pg_trgm-style similarity: |A∩B| / |A∪B| over padded word trigrams. */
export function trigramSimilarity(a: string, b: string): number {
  const trigramsA = trigramsOf(a);
  const trigramsB = trigramsOf(b);
  if (trigramsA.size === 0 || trigramsB.size === 0) {
    return 0;
  }
  let shared = 0;
  for (const trigram of trigramsA) {
    if (trigramsB.has(trigram)) {
      shared += 1;
    }
  }
  return shared / (trigramsA.size + trigramsB.size - shared);
}

/** Candidate name spans of the question: 2–3 consecutive non-stopword
 * tokens. Single tokens are excluded on purpose — one matching surname is
 * exactly the word_similarity noise this module exists to filter out. */
function nameSpans(question: string): string[] {
  const tokens = tokensOf(question);
  const runs: string[][] = [];
  let current: string[] = [];
  for (const token of tokens) {
    if (STOPWORDS.has(token)) {
      if (current.length > 0) {
        runs.push(current);
        current = [];
      }
    } else {
      current.push(token);
    }
  }
  if (current.length > 0) {
    runs.push(current);
  }
  const spans: string[] = [];
  for (const run of runs) {
    for (let size = 2; size <= SPAN_MAX_WORDS; size += 1) {
      for (let start = 0; start + size <= run.length; start += 1) {
        spans.push(run.slice(start, start + size).join(" "));
      }
    }
  }
  return spans;
}

/** Highest span-vs-whole-name similarity, also trying the first+last form so
 * "Micheal Smith" still finds "Michael L. Smith". */
export function bestNameSimilarity(question: string, displayName: string): number {
  const variants = [displayName];
  const firstLast = firstLastKey(displayName);
  if (firstLast !== displayName.toLowerCase()) {
    variants.push(firstLast);
  }
  let best = 0;
  for (const span of nameSpans(question)) {
    for (const variant of variants) {
      best = Math.max(best, trigramSimilarity(span, variant));
    }
  }
  return best;
}

/**
 * Near-miss candidates worth offering as "did you mean". Call on the refusal
 * path only (the gate already failed, so every match is below
 * GATE_MIN_ENTITY_SIMILARITY). Best first, capped, and NOT deduped by name:
 * every match is a distinct candidate (distinct id), and same-name people in
 * different states must all be offered — collapsing them would silently pick
 * one, the thing rule 7 forbids. The rendered option shows state and office,
 * which is what tells them apart.
 */
export function suggestClosestCandidates(
  question: string,
  matches: readonly CandidateEntityMatch[]
): CandidateEntityMatch[] {
  return matches
    .map((match) => ({ match, nameSimilarity: bestNameSimilarity(question, match.displayName) }))
    .filter((entry) => entry.nameSimilarity >= SUGGESTION_MIN_SIMILARITY)
    .sort((a, b) => b.nameSimilarity - a.nameSimilarity || b.match.similarity - a.match.similarity)
    .slice(0, MAX_SUGGESTIONS)
    .map((entry) => entry.match);
}
