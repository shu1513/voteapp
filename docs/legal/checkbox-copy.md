<!--
The frontend must copy these strings verbatim. Version bumps to
disclaimer.md require re-review of this file too.

Clickwrap requirements (Meyer v. Uber; Nguyen v. Barnes & Noble; Berman v.
Freedom Financial Network):
- Checkbox UNCHECKED by default; the action button stays disabled until
  checked.
- Checkbox sits directly above the action button it gates.
- [Terms of Use] / [Privacy Policy] / [Disclaimer] render as clearly visible
  links right next to the checkbox — no tiny gray text, no footer-only links.
- Signup acceptance is recorded server-side: POST /api/auth/register requires
  accepted_terms_version matching CURRENT_TERMS_VERSION
  (backend/src/constants/legal.ts); stored on the user row with a timestamp.
  Registration always asks for its own acceptance: an anonymous acceptance on
  the same browser proves nothing about who owns the account.
- The users columns hold only the CURRENT version, and re-acceptance
  overwrites them, so every acceptance is also appended to
  user_terms_acceptances (migration 201). That table is the answer to "what
  has this account ever accepted"; the users columns answer "what is it on
  now", which is what the re-acceptance interstitial reads. Registration and
  renewal write their history row in the same statement or transaction as the
  users write, so an account can never claim a version with no history behind
  it. Rows are append-only: UPDATE is rejected, and DELETE is rejected unless
  the account itself is being deleted.
- Two limits on that table, stated here so nobody describes it as more than it
  is. It does not reach back before migration 201: accounts that accepted 1.0
  and later re-accepted 1.1 had the 1.0 acceptance overwritten in place on
  2026-07-18, and only a pre-bump backup can recover it. And acceptance rows
  are deleted with the account, so a closed account leaves no acceptance
  evidence at all — deliberate, because keeping it would be a retention
  practice the privacy policy does not describe.
- The wording behind a version is pinned by hash in
  packages/api-client/src/legalCopy.test.ts. A version string is only worth
  what the text behind it is, so editing terms-of-use.md, privacy-policy.md,
  or disclaimer.md fails CI until someone decides whether the edit keeps the
  version or needs a bump and re-acceptance.
- Registration and the re-acceptance interstitial keep their checkbox INLINE
  on the page: both gate an explicit account action the visitor came to take.
- The anonymous pre-search gate is DEFERRED instead: the home page carries no
  checkbox and no legal box, and pressing Search opens the terms dialog. Assent
  is asked for at the moment it gates something, which is where the clickwrap
  cases put it (Meyer v. Uber). Notice sitting apart from the action is the
  weak pattern — Nicosia v. Amazon turned on exactly that.
- Anonymous search acceptance is ENFORCED server-side but never stored: POST
  /api/address/resolve requires accepted_terms_version matching
  CURRENT_TERMS_VERSION and refuses the search otherwise, so the gate is real
  rather than a disabled button a direct API call walks around. Nothing about
  the acceptance is persisted — an anonymous visitor's IP and user agent are
  deliberately NOT collected, so the evidence is this file plus the deployed
  gate, not a row per search.
- Acceptance IS remembered on the device for 90 days, keyed to the terms
  version (frontend/src/lib/termsAcceptance.ts and the mobile port). Re-asking
  a returning visitor on every search teaches click-through, which weakens
  assent; the trigger that requires fresh consent is a version change, which
  the version check enforces. Remembering may ONLY decide whether the dialog
  opens. When a dialog opens its checkbox starts empty — a pre-ticked box shows
  assent nobody gave, and that is the thing that must never come back.
- The privacy notice must stay beside the address field, not only inside the
  dialog: the autocomplete forwards typed fragments after three characters, so
  collection begins before Search is ever pressed and notice has to arrive at
  or before collection.
- The pre-search checkbox label is a SUMMARY; the sentences it does not carry
  appear above it in the dialog. All of this copy lives in
  packages/api-client/src/legalCopy.ts, and legalCopy.test.ts asserts every
  string still appears in this file.
-->

# Checkbox and notice copy — Version 1.1

## Pre-search terms dialog (anonymous address search)

Opened by pressing **Search**. Heading: **Before we search**. Body, in order:
the paragraphs below, the full privacy notice, then the checkbox and its three
document links, then **Cancel** and **Agree and search**.

> [ ] I have read and agree to the [Terms of Use], [Privacy Policy], and
> [AI Research and Election Information Disclaimer] — including binding
> individual arbitration with a class-action waiver (Terms of Use Section 12),
> unless I opt out as described there.

Rules for the dialog: the box is empty every time it opens; **Agree and search**
stays disabled until it is ticked and names what it does rather than saying
"Continue"; the document links open in a new tab so reading one does not
discard the dialog or the address already typed; Cancel, Escape, and the
backdrop close it without agreeing and leave the typed address alone; no
forced scrolling through the documents.

### Dialog paragraphs

> Elections Simplified provides AI-assisted informational research only. It is
> not an official election source, and results may be inaccurate, incomplete,
> outdated, or misleading.
>
> You must verify voting, registration, ballot, district, polling-place,
> deadline, and election-result information with official election authorities
> before relying on it.
>
> Disputes are resolved by binding individual arbitration with a class-action
> waiver, as described in Section 12 of the Terms of Use, unless you opt out as
> described there.

## Signup checkbox (account registration)

> [ ] I am at least 18 years old, and I have read and agree to the
> [Terms of Use], [Privacy Policy], and [AI Research and Election Information
> Disclaimer]. I consent to enter this agreement electronically. I understand
> that Elections Simplified is not an official election source, does not register voters
> or cast ballots, and may display AI-assisted content that must be
> independently verified with official election authorities. I agree that
> disputes are resolved by binding individual arbitration with a class-action
> waiver as described in Section 12 of the Terms of Use, unless I opt out as
> described there.

## Re-acceptance checkbox (signed-in interstitial after a version bump)

> [ ] I have read and agree to the updated [Terms of Use], [Privacy Policy],
> and [AI Research and Election Information Disclaimer], including the
> agreement to resolve disputes by binding individual arbitration with a
> class-action waiver (Terms of Use Section 12), unless I opt out as
> described there.

## Privacy notice (full — shown inside the pre-search dialog)

> Privacy notice: we collect the address you enter, account information if
> you sign up, and device and usage information, to generate results, operate
> and secure the Service, and comply with law — as described in our
> [Privacy Policy]. Your address is used to find your districts and is not
> sold.

## Short privacy note (beside every address input)

This carries the address-specific points that matter at collection without
repeating the full consent-dialog disclosure beside the field. It carries no
Privacy Policy link of its own: the footer links the policy on every page, and
the explainer beside this note links it directly, so a second inline copy sat
next to the question people actually ask and crowded it out. The 14-day
lookup cache is described in the Privacy Policy and in the full notice above
rather than here, to keep this line to the two facts a visitor weighs while
typing — what it is used for, and that it is neither kept on their account nor
sold.

> Your address is only used to find your voting districts. We don’t save it to
> your account or sell it.

Beside the anonymous-search note, **Why do we need the full address?** opens
an informational dialog on web and mobile. It leads with what the address is
for — the ballot depends on which voting districts a home sits in — and then
explains why nothing shorter works: district lines are drawn street by street
and do not follow ZIP codes, so two homes in the same ZIP can vote in
different races. The dialog repeats the address-handling summary, links to the
Privacy Policy, and closes with **Got it**. It has no checkbox or agreement
button because it explains the field rather than requesting consent.

## Results verification line (ballot and results screens)

Non-blocking, and deliberately not gated on acceptance: it has to reach the
people the clickwrap never did — a shared computer, someone else's phone, a
link from a text message.

> AI-assisted research. Verify voting information with official election
> authorities. [Disclaimer]

## Per-record source line (candidate records, measures, results)

> Source: [link] · researched [date]

## Notification email footer line

> Information is AI-assisted research; verify with official sources before
> voting.
