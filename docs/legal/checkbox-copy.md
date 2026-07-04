<!--
DRAFT — pending attorney review before public launch.
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
- Anonymous search acceptance is frontend-only (nothing to bind server-side);
  localStorage may remember it per version.
-->

# Checkbox and notice copy — Version 1.0

## Pre-search checkbox (anonymous address search)

> [ ] I have read and agree to the [Terms of Use], [Privacy Policy], and
> [AI Research and Election Information Disclaimer]. I understand that
> VoteApp provides AI-assisted informational research only; it is not an
> official election source; results may be inaccurate, incomplete, outdated,
> or misleading; and I must verify voting, registration, ballot, district,
> polling-place, deadline, and election-result information with official
> election authorities before relying on it.

## Signup checkbox (account registration)

> [ ] I am at least 18 years old, and I have read and agree to the
> [Terms of Use], [Privacy Policy], and [AI Research and Election Information
> Disclaimer]. I consent to enter this agreement electronically. I understand
> that VoteApp is not an official election source, does not register voters
> or cast ballots, and may display AI-assisted content that must be
> independently verified with official election authorities.

## Privacy notice (under the address input)

> Privacy notice: we collect the address you enter, account information if
> you sign up, and device and usage information, to generate results, operate
> and secure the Service, and comply with law — as described in our
> [Privacy Policy]. Your address is used to find your districts and is not
> sold.

## Results-page banner (every ballot, election, and candidate view)

> AI-assisted research — verify before relying on it. This information may be
> inaccurate, incomplete, or outdated, and is not official election
> information.

## Per-record source line (candidate records, measures, results)

> Source: [link] · researched [date]

## Notification email footer line

> Information is AI-assisted research; verify with official sources before
> voting.
