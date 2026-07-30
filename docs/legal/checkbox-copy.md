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
- Anonymous search acceptance is ENFORCED server-side but never stored: POST
  /api/address/resolve requires accepted_terms_version matching
  CURRENT_TERMS_VERSION and refuses the search otherwise, so the checkbox is a
  real gate rather than a disabled button a direct API call walks around.
  Nothing about the acceptance is persisted — an anonymous visitor's IP and
  user agent are deliberately NOT collected, so the evidence is this file plus
  the deployed gate, not a row per search. Acceptance is also never remembered
  between visits — no localStorage key, no pre-ticked box.
- The pre-search checkbox label is a SUMMARY. The sentences it no longer
  carries live in the full-agreement dialog behind it, reachable from a "Read
  the full agreement" control beside the document links. Label, dialog
  paragraphs, and privacy notice all live in
  packages/api-client/src/legalCopy.ts, and legalCopy.test.ts asserts every one
  of them still appears in this file.
-->

# Checkbox and notice copy — Version 1.1

## Pre-search checkbox (anonymous address search)

> [ ] I have read and agree to the [Terms of Use], [Privacy Policy], and
> [AI Research and Election Information Disclaimer] — including binding
> individual arbitration with a class-action waiver (Terms of Use Section 12),
> unless I opt out as described there.

### Full-agreement dialog ("Read the full agreement", next to the checkbox)

Heading: **What you are agreeing to**. Body repeats the checkbox label, then:

> Elections Simplified provides AI-assisted informational research only. It is
> not an official election source, and results may be inaccurate, incomplete,
> outdated, or misleading.

> You must verify voting, registration, ballot, district, polling-place,
> deadline, and election-result information with official election authorities
> before relying on it.

> Disputes are resolved by binding individual arbitration with a class-action
> waiver, as described in Section 12 of the Terms of Use, unless you opt out as
> described there.

…followed by the privacy notice below, the three document links, and an
**I agree** button that ticks the checkbox. **Close** dismisses without
agreeing.

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

## Privacy notice (under the address input)

> Privacy notice: we collect the address you enter, account information if
> you sign up, and device and usage information, to generate results, operate
> and secure the Service, and comply with law — as described in our
> [Privacy Policy]. Your address is used to find your districts and is not
> sold.

## Per-record source line (candidate records, measures, results)

> Source: [link] · researched [date]

## Notification email footer line

> Information is AI-assisted research; verify with official sources before
> voting.
