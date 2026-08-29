// Legal strings rendered VERBATIM from docs/legal/checkbox-copy.md.
// Do not edit here without updating that file (and vice versa); the version
// must track docs/legal/disclaimer.md and the backend's
// CURRENT_TERMS_VERSION in lockstep.

export const TERMS_VERSION = "1.3";

// The pre-search clickwrap is split in two: a one-sentence label that names
// every document and the arbitration clause, plus a dialog holding the detail
// that used to sit inline. Three paragraphs of small print above the Search
// button was skimmed past, which is the failure mode a clickwrap cannot
// afford; a summary the visitor actually reads, with the full text one click
// away and re-shown at the moment of agreement, is the pattern the clickwrap
// cases reward (Meyer v. Uber; Berman v. Freedom Financial; Sellers v.
// JustAnswer). Nothing was dropped — every sentence removed from the label
// appears in PRE_SEARCH_AGREEMENT_PARAGRAPHS below.
export const PRE_SEARCH_CHECKBOX_LABEL =
  "I have read and agree to the Terms of Use, Privacy Policy, and AI Research and Election Information " +
  "Disclaimer — including binding individual arbitration with a class-action waiver (Terms of Use " +
  "Section 12), unless I opt out as described there.";

/** Body of the "full agreement" dialog behind the pre-search checkbox. */
export const PRE_SEARCH_AGREEMENT_PARAGRAPHS = [
  "Elections Simplified provides AI-assisted informational research only. It is not an official election " +
    "source, and results may be inaccurate, incomplete, outdated, or misleading.",
  "You must verify voting, registration, ballot, district, polling-place, deadline, and election-result " +
    "information with official election authorities before relying on it.",
  "Disputes are resolved by binding individual arbitration with a class-action waiver, as described in " +
    "Section 12 of the Terms of Use, unless you opt out as described there.",
] as const;

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

/**
 * Sits beside the address field, where collection actually begins: the
 * autocomplete forwards what is typed after three characters, long before
 * anyone presses Search, and notice has to arrive at or before collection.
 * Keep this short and address-specific; the full PRIVACY_NOTICE belongs in
 * the consent dialog, while this line belongs beside every address field.
 */
export const ADDRESS_FIELD_PRIVACY_NOTE =
  "The address is only used to find voting districts. We don’t save it to your account.";

/**
 * Shown on results, where it reaches people who never passed the gate at all
 * — a shared computer, someone else's phone, a link from a text message. For
 * a reliance claim this line does more work than the agreement does, because
 * it does not depend on the reader having accepted anything.
 */
export const VERIFY_WITH_OFFICIALS_NOTE =
  "AI-assisted research. Verify voting information with official election authorities.";

// Acceptance IS remembered, per terms version, with an expiry — see
// frontend/src/lib/termsAcceptance.ts. What must never come back is the older
// behaviour that stored acceptance and then returned repeat visitors a
// PRE-TICKED box. Those are different things: a pre-ticked box shows assent
// that was never given, while skipping a gate somebody already passed is what
// every large site does. Remembering may therefore only ever decide whether
// the dialog OPENS. If a dialog opens, its checkbox starts empty.
