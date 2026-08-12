// Pieces shared by the Phase 1 card pipeline (askService.ts) and the Phase 2
// LLM answer path (answer.ts). Import-leaf module: imports only retrieval
// types, so askService → answer → shared never forms a runtime cycle.

import type { RetrievedChunk } from "./retrieval.js";

export const REFUSAL_NO_DATA_ANSWER =
  "I don't have that in my data. I can answer questions about the November 2026 elections we cover: candidates, their records, campaign finance, elections, and ballot measures.";

export type AskResultCard = {
  title: string;
  /** Server-constructed, site-relative page URL — never model- or
   * content-authored (BEHAVIOR.md rule 9). */
  url: string;
  snippet: string;
  source_type: string;
};

/** Site-relative page URL for a chunk, from its metadata ONLY (rule 9: URLs
 * are server-constructed, never model-written). */
export function chunkPageUrl(chunk: RetrievedChunk): string | null {
  if (
    chunk.sourceType === "candidate_profile" ||
    chunk.sourceType === "candidate_record" ||
    chunk.sourceType === "finance_summary"
  ) {
    return chunk.sourceId ? `/candidates/${chunk.sourceId}` : null;
  }
  if (chunk.sourceType === "election") {
    return chunk.sourceId ? `/elections/${chunk.sourceId}` : null;
  }
  if (chunk.sourceType === "ballot_measure") {
    return chunk.electionId ? `/elections/${chunk.electionId}` : null;
  }
  return null;
}

export function toResultCards(chunks: readonly RetrievedChunk[]): AskResultCard[] {
  const cards: AskResultCard[] = [];
  const seenKeys = new Set<string>();
  for (const chunk of chunks) {
    const url = chunkPageUrl(chunk);
    if (!url) {
      continue;
    }
    // Dedupe per (page, chunk kind), and never across a candidate's records:
    // one candidate's profile, finance, and each record all share the same
    // page URL but answer different parts of the question — collapsing them
    // to one card hid everything but the profile.
    const key = chunk.sourceType === "candidate_record" ? `record:${chunk.id}` : `${chunk.sourceType}:${url}`;
    if (seenKeys.has(key)) {
      continue;
    }
    seenKeys.add(key);
    cards.push({
      title: chunk.title,
      url,
      snippet: chunk.content.length > 240 ? `${chunk.content.slice(0, 239).trimEnd()}…` : chunk.content,
      source_type: chunk.sourceType,
    });
  }
  return cards;
}
