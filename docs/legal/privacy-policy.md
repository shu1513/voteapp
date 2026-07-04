<!--
DRAFT — pending attorney review before public launch (CCPA/state privacy
law applicability in particular). Written to match the ACTUAL data practices
in this codebase as of July 2026 — if practices change (analytics,
advertising, new processors, address retention), this document must change
with them. Placeholders: [legal entity name], [contact email], [state].
-->

# VoteApp Privacy Policy

**Last updated:** July 3, 2026
**Version:** 1.0

This Privacy Policy describes how [legal entity name] ("VoteApp," "we," "us") collects, uses, and shares information when you use the VoteApp website and services (the "Service").

## 1. Information we collect

**Address and search input.** When you enter an address, we process it to determine your voting districts. The address is sent to geocoding services (see Section 3) and may be held briefly in a server-side cache to speed up repeat lookups. What we store on your account is the resulting **list of districts** — not your street address. We do not maintain a saved-address book.

**Account information.** If you create an account: your email address, first name, and a cryptographic hash of your password (we never store the password itself). We also record which version of our terms you accepted and when.

**Preferences and activity.** Settings you choose: candidates you follow, research-area interests, ballot ordering preferences, and email notification opt-ins. Because the Service is about elections, these choices may reflect your civic or political interests; we treat them as your private account data, use them only to provide the features you chose, and never sell them or use them for advertising.

**Technical information.** IP address, browser user agent, and request logs collected automatically for security, rate limiting, and abuse prevention. A session cookie (httpOnly) keeps you signed in. We do not use advertising or cross-site tracking cookies, and we do not run third-party analytics.

## 2. How we use information

To provide the Service (find your districts, show your ballot, send notification emails you opted into); to secure the Service (authentication, rate limiting, abuse prevention); to communicate with you about your account (verification, password reset, email-change confirmation); and to comply with law. We do **not** sell personal information, share it for cross-context behavioral advertising, or use it to build advertising profiles.

## 3. Service providers

We share information only with the processors needed to run the Service:

- **U.S. Census Bureau geocoder** — receives the address text you enter, to locate your districts.
- **Google Places (autocomplete)** — receives the address text you type when address suggestions are enabled; requests are proxied through our servers so Google does not receive your IP address from your browser.
- **Amazon Web Services (SES)** — receives your email address to deliver account and notification emails.
- **Infrastructure providers** — hosting, database, and cache providers that store Service data on our behalf.

Each receives only what its function requires. We do not share personal information with data brokers, advertisers, campaigns, or political organizations.

## 4. Retention and deletion

Account data is kept while your account is active. If you delete your account, it is deactivated immediately: sessions are destroyed, notification sending stops, and your email address is released for re-registration; residual records may persist in backups and security logs for a limited period before being purged. Cached address lookups expire automatically. Notification event records are pruned on a rolling schedule.

## 5. Your choices

You can view and update your name, email address, password, districts, follows, and email preferences in account settings; every notification email includes a working unsubscribe link; and you can delete your account entirely in settings. For access or deletion requests you cannot complete in the app, or questions about this policy, contact [contact email]. Depending on where you live, you may have additional privacy rights (such as access, correction, deletion, or portability); we honor valid requests as required by applicable law.

## 6. Security

Passwords are stored using a modern memory-hard hashing algorithm; sessions use httpOnly cookies; email-verification, password-reset, and email-change links are single-use and expire; and account-sensitive actions require your password and are rate limited. No system is perfectly secure — use a unique password.

## 7. Children

The Service is not directed to children and requires users to be at least 18. We do not knowingly collect personal information from children under 13; if you believe a child has provided us personal information, contact [contact email] and we will delete it.

## 8. Changes

We may update this Privacy Policy; the version and date above identify the current text. For material changes we will provide notice in the Service and may require renewed acceptance.

## 9. Contact

[legal entity name] — [contact email]
