// Legal strings rendered VERBATIM from docs/legal/checkbox-copy.md.
// Do not edit here without updating that file (and vice versa); the version
// must track docs/legal/disclaimer.md and the backend's
// CURRENT_TERMS_VERSION in lockstep.

export const TERMS_VERSION = "1.1";

export const PRE_SEARCH_CHECKBOX_LABEL =
  "I have read and agree to the Terms of Use, Privacy Policy, and AI Research and Election Information " +
  "Disclaimer. I understand that VoteApp provides AI-assisted informational research only; it is not an " +
  "official election source; results may be inaccurate, incomplete, outdated, or misleading; and I must " +
  "verify voting, registration, ballot, district, polling-place, deadline, and election-result information " +
  "with official election authorities before relying on it. I agree that disputes are resolved by binding " +
  "individual arbitration with a class-action waiver as described in Section 12 of the Terms of Use, " +
  "unless I opt out as described there.";

export const SIGNUP_CHECKBOX_LABEL =
  "I am at least 18 years old, and I have read and agree to the Terms of Use, Privacy Policy, and AI " +
  "Research and Election Information Disclaimer. I consent to enter this agreement electronically. I " +
  "understand that VoteApp is not an official election source, does not register voters or cast ballots, " +
  "and may display AI-assisted content that must be independently verified with official election " +
  "authorities. I agree that disputes are resolved by binding individual arbitration with a class-action " +
  "waiver as described in Section 12 of the Terms of Use, unless I opt out as described there.";

export const PRIVACY_NOTICE =
  "Privacy notice: we collect the address you enter, account information if you sign up, and device and " +
  "usage information, to generate results, operate and secure the Service, and comply with law — as " +
  "described in our Privacy Policy. Your address is used to find your districts and is not sold.";

/** localStorage key for the anonymous pre-search acceptance, keyed by version
 * so a terms bump re-prompts every visitor. */
export const PRE_SEARCH_ACCEPTANCE_STORAGE_KEY = `voteapp_terms_accepted_v${TERMS_VERSION}`;
