// The product name as readers see it. Kept in one place because it appears
// in the wordmark, every tab title, the push/email "from" name, and the
// legal copy — the 2026-07 rename from "VoteApp" had to touch all of them.
// This is the DISPLAY name only: package names, the deep-link scheme, the
// iOS/Android bundle ids, storage keys, and the database still say voteapp,
// and renaming those would break installs and sessions for no reader gain.
export const APP_NAME = "Elections Simplified";

// The legal entity behind the product. Unlike APP_NAME it never appears in
// running copy — only in the copyright line, which every surface (web footer,
// mobile Settings, email footers) renders from COPYRIGHT_LINE so the spelling
// cannot drift. Mirrored in backend/src/constants/brand.ts.
//
// The year is a fixed literal, not new Date(): a copyright notice names the
// year of first publication, so it does not need to tick, and a live clock
// would make the SSR/prerendered HTML disagree with the browser around New
// Year (server UTC vs. reader local time, build year vs. load year) and force
// a hydration re-render. Bump the literal by hand if the notice should read
// as a range later.
export const LEGAL_ENTITY_NAME = "Elections Simplified Inc.";
export const COPYRIGHT_YEAR = 2026;
export const COPYRIGHT_LINE = `© ${COPYRIGHT_YEAR} ${LEGAL_ENTITY_NAME}`;
