/**
 * The product name as readers see it, used wherever the backend speaks to a
 * user or identifies the app to an outside service: email subjects and
 * from-names, push notification titles, scraper user-agents. Mailers accept
 * an `appName` override; this is the fallback they actually ship with.
 *
 * Display name only — the package name, database, cookies, and the mobile
 * bundle ids still say voteapp, and renaming those would break live sessions
 * and installs for no reader gain. The frontend has its own copy in
 * packages/api-client/src/brand.ts (backend is not a workspace member, so it
 * cannot import it); keep the two in step.
 */
export const APP_NAME = "Elections Simplified";
