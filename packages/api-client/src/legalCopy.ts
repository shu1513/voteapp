// Legal strings rendered VERBATIM from docs/legal/checkbox-copy.md.
// Do not edit here without updating that file (and vice versa); the version
// must track docs/legal/disclaimer.md and the backend's
// CURRENT_TERMS_VERSION in lockstep.

export const TERMS_VERSION = "1.2";

// Identifies the exact consent screen/copy/action arrangement, independently
// of the legal-document bundle. Stored with every acceptance event so later
// evidence can reconstruct both what the user agreed to and how assent was
// obtained.
export const LEGAL_PRESENTATION_VERSION = "1.2";

export const PRE_SEARCH_CHECKBOX_LABEL =
  "I have read and agree to the Terms of Use, Privacy Policy, and AI Research and Election Information " +
  "Disclaimer. I understand that Elections Simplified provides AI-assisted informational research only; it is not an " +
  "official election source; results may be inaccurate, incomplete, outdated, or misleading; and I must " +
  "verify voting, registration, ballot, district, polling-place, deadline, and election-result information " +
  "with official election authorities before relying on it. I agree that disputes are resolved by binding " +
  "individual arbitration with a class-action waiver as described in Section 12 of the Terms of Use, " +
  "unless I opt out as described there.";

export const SIGNUP_CHECKBOX_LABEL =
  "I am at least 18 years old, and I have read and agree to the Terms of Use, Privacy Policy, and AI " +
  "Research and Election Information Disclaimer. I consent to enter this agreement electronically. I " +
  "understand that Elections Simplified is not an official election source, does not register voters or cast ballots, " +
  "and may display AI-assisted content that must be independently verified with official election " +
  "authorities. I agree that disputes are resolved by binding individual arbitration with a class-action " +
  "waiver as described in Section 12 of the Terms of Use, unless I opt out as described there.";

export const RENEWAL_CHECKBOX_LABEL =
  "I have read and agree to the updated Terms of Use, Privacy Policy, and AI Research and Election " +
  "Information Disclaimer, including the agreement to resolve disputes by binding individual arbitration " +
  "with a class-action waiver (Terms of Use Section 12), unless I opt out as described there.";

export const PRIVACY_NOTICE =
  "Privacy notice: we collect the address you enter, account information if you sign up, and device and " +
  "usage information, to generate results, operate and secure the Service, and comply with law — as " +
  "described in our Privacy Policy. Your address is used to find your districts and is not sold.";

// The browser may persist a pseudonymous evidence subject ID, but never an
// accepted/checked flag. Every consent screen starts unchecked.
