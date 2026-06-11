export function isPresidentialOfficeTitle(title: string): boolean {
  const text = title.toLowerCase().replace(/\s+/g, " ").trim();
  return /\bpresident of the united states\b/.test(text) ||
    /\bu\.?s\.?\s+president\b/.test(text) ||
    /\bunited states president\b/.test(text) ||
    /\bpresident\s+(and|&)\s+vice president\b/.test(text) ||
    /\bpresident\s*\/\s*vice president\b/.test(text) ||
    /\bvice president\s+(and|&)\s+president\b/.test(text) ||
    /\bpresidential electors?\b/.test(text) ||
    /\belectors?\s+for\s+president\b/.test(text) ||
    /\bpresidential preference\b/.test(text) ||
    /\bpresidential primary\b/.test(text);
}
