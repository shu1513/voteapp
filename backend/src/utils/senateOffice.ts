import type { ElectionEntryPayload } from "../types/election.js";

function normalize(text: string): string {
  return text.toLowerCase().replace(/\s+/g, " ").trim();
}

export function isUsSenateOfficeTitle(title: string): boolean {
  const text = normalize(title);
  return (
    /\bunited states senator\b/.test(text) ||
    /\bu\.?\s*s\.?\s+senator\b/.test(text) ||
    /\bunited states senate\b/.test(text) ||
    /\bu\.?\s*s\.?\s+senate\b/.test(text)
  );
}

export function hasSpecialSeatMarker(entry: Pick<ElectionEntryPayload, "official_ballot_title" | "election_stage">): boolean {
  const combined = normalize(entry.official_ballot_title);
  return (
    entry.election_stage === "special" ||
    /\bunexpired term\b/.test(combined) ||
    /\bspecial election\b/.test(combined) ||
    /\bvacancy\b/.test(combined) ||
    /\bremainder of (the )?term\b/.test(combined)
  );
}
