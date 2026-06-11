import type { ElectionEntryPayload } from "../types/election.js";

export type PresidentialElectionEntryFilterResult = {
  entries: ElectionEntryPayload[];
  removedTitles: string[];
};

export function isPresidentialOfficeTitle(title: string): boolean {
  const text = title.toLowerCase().replace(/\s+/g, " ").trim();
  return /\bpresident of the united states\b/.test(text) ||
    /\bu\.?s\.?\s+president\b/.test(text) ||
    /\bunited states president\b/.test(text) ||
    /\bpresident\s+(and|&)\s+vice[-\s]+president\b/.test(text) ||
    /\bpresident\s*\/\s*vice[-\s]+president\b/.test(text) ||
    /\bvice[-\s]+president\s+(and|&)\s+president\b/.test(text) ||
    /\bpresidential electors?\b/.test(text) ||
    /\belectors?\s+for\s+president\b/.test(text) ||
    /\bpresidential preference\b/.test(text) ||
    /\bpresidential primary\b/.test(text);
}

export function filterPresidentialElectionEntries(
  entries: readonly ElectionEntryPayload[]
): PresidentialElectionEntryFilterResult {
  const keptEntries: ElectionEntryPayload[] = [];
  const removedTitles: string[] = [];

  for (const entry of entries) {
    if (entry.race_type === "office" && isPresidentialOfficeTitle(entry.official_ballot_title)) {
      removedTitles.push(entry.official_ballot_title);
      continue;
    }
    keptEntries.push(entry);
  }

  return { entries: keptEntries, removedTitles };
}
