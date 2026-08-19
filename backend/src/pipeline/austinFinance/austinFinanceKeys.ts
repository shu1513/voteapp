// Identity keys for the Austin finance tables (plan-austin-finance.md Phase 1).
//
// The City Clerk's Socrata datasets carry no filer or spender ids — Report
// Detail keys candidate reports by the `filer_name` string, Direct Campaign
// Expenditures key spenders by `paid_by` — so both link identity (filer_key)
// and outside-group identity (spender_key) are normalized name strings.
// One normalizer, applied at every write and compare site, so a filer can
// never be linked twice under two spellings of one name. The exact source
// spelling travels alongside (filer_name / spender_name) because the sync
// queries Socrata by exact string equality.

/**
 * Shape every key must satisfy — mirrored by the schema CHECKs in migration
 * 243, so a raw name landing in a key column fails loudly at the DB too.
 */
export const AUSTIN_FINANCE_TEXT_KEY_PATTERN = /^[A-Z0-9]+( [A-Z0-9]+)*$/;

/**
 * NFKD, diacritics stripped, upper-cased, `&` spelled out, every run of
 * non-alphanumerics collapsed to one space, trimmed. "Watson, Kirk P." →
 * "WATSON KIRK P"; "Velásquez, José" → "VELASQUEZ JOSE". Empty in → "" out
 * (callers decide whether empty is an error).
 */
export function normalizeAustinFinanceTextKey(value: string | null | undefined): string {
  return (value ?? "")
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toUpperCase()
    .replace(/&/g, " AND ")
    .replace(/[^A-Z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}
