/**
 * Version of the Terms of Use / Disclaimer bundle the frontend must present
 * at signup. Must match the Version header in docs/legal/disclaimer.md —
 * bump both together. Registration rejects any other value so a stale
 * frontend cannot record acceptance of superseded terms; the git history of
 * docs/legal/ is the authoritative archive of what each version said.
 */
export const CURRENT_TERMS_VERSION = "1.1";
