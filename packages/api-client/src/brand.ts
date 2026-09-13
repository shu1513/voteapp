// The product name as readers see it. Kept in one place because it appears
// in the wordmark, every tab title, the push/email "from" name, and the
// legal copy — the 2026-07 rename from "VoteApp" had to touch all of them.
// This is the DISPLAY name only: package names, the deep-link scheme, the
// iOS/Android bundle ids, storage keys, and the database still say voteapp,
// and renaming those would break installs and sessions for no reader gain.
export const APP_NAME = "Elections Simplified";

// The legal entity behind the product. Unlike APP_NAME it never appears in
// running copy — only in the copyright line, which every surface (web footer,
// mobile Settings, email footers) builds through copyrightLine() so the year
// and spelling cannot drift. Mirrored in backend/src/constants/brand.ts.
export const LEGAL_ENTITY_NAME = "Elections Simplified Inc.";

export function copyrightLine(year: number = new Date().getFullYear()): string {
  return `© ${year} ${LEGAL_ENTITY_NAME}`;
}
