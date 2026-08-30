<!--
Written to match the ACTUAL data practices
in this codebase as of July 2026 — if practices change (analytics,
advertising, new processors, address retention), this document must change
with them. The contact mailbox (contact@electionssimplified.com) must exist and
be monitored before launch. Analytics disclosure added 2026-07-05 ahead of
adoption; once a specific analytics provider is chosen, name it in Section 3
and confirm the "no advertising use" configuration claims hold for it.
Ask/OpenAI disclosure added 2026-08-12 (chatbot Phase 2): this version must be
LIVE before CHATBOT_LLM_ENABLED is ever turned on in production
(docs/plans/chatbot-rag.md). The Ask question-log description matches
backend/src/chatbot/ (anonymous, redacted, 90-day purge, low-count
suppression); the OpenAI description matches the adapter (store:false, no
raw identifiers).
Google Sign-In disclosure added 2026-08-12 (1.1 → 1.2,
docs/plans/google-sign-in.md): this version must be LIVE before
GOOGLE_OAUTH_CLIENT_ID is ever set in production. The description matches
backend/src/auth/googleIdToken.ts + loginWithGoogle: ID-token only (sub,
email, verified/hosted-domain status, optional first name), no Google
access/refresh tokens, no Google API calls, google_sub deleted with the
account. 1.2 is a clarifying addition for an OPTIONAL sign-in method —
CURRENT_TERMS_VERSION deliberately stays 1.1 (no forced re-acceptance).
Support-payments disclosure added 2026-08-21 (1.2 → 1.3,
docs/plans/membership-contributions.md): this version must be LIVE before
STRIPE_SECRET_KEY is ever set in production. The description matches the
plan's schema: Stripe holds card data, we hold amounts/dates/kind/refund
status/Stripe reference ids in billing_* tables, and those rows survive
account deletion unlinked from the account (billing_customers.user_id set
NULL) for accounting and legal compliance. The retained Stripe ids remain
resolvable to a person through the Stripe dashboard, so those rows are
pseudonymous business records, not anonymous data — the public text below
deliberately says "with the link to your deleted account removed" and never
claims anonymization. Unlike 1.2, this change ships together with
a Terms bump (payments section) — see terms-of-use.md.
Operator change 2026-08-29 (1.3 → 1.4): operator named as Elections
Simplified Inc., a Delaware corporation (previously impactperdollar).
Ships with the Terms 1.4 bump — see terms-of-use.md.
-->

# Elections Simplified Privacy Policy

**Last updated:** August 29, 2026
**Version:** 1.4

This Privacy Policy describes how Elections Simplified Inc., a Delaware corporation and the operator of the Elections Simplified service ("Elections Simplified," "we," "us"), collects, uses, and shares information when you use the Elections Simplified website and services (the "Service").

## 1. Information we collect

**Address and search input.** When you enter an address, we process it to determine your voting districts. The address is sent to geocoding services (see Section 3) and may be held in a server-side cache for up to 14 days to speed up repeat lookups; this cache is not linked to your account. What we store on your account is the resulting **list of districts** — not your street address. We do not maintain a saved-address book.

**Account information.** If you create an account: your email address, first name, and a cryptographic hash of your password (we never store the password itself). We also record which version of our terms you accepted and when.

**Sign in with Google (optional).** If you choose to sign up or sign in with your Google account, we receive from Google a signed identity token containing your stable Google account identifier, your Google account email address and its verification status (including whether it belongs to a Google Workspace organization), and optionally your first name. We store the identifier to recognize your sign-in and the email address and first name as your account information above. We receive **no** Google access or refresh tokens, cannot access your Google data (mail, contacts, files), and make no ongoing requests to your Google account. An account created with Google has no password until you set one (see Section 6).

**Support payments (optional).** If you choose to support the Service with a one-time payment or a monthly membership, the payment is processed by Stripe (see Section 3); you enter your card details on Stripe's payment pages, and your card number never reaches our servers. Stripe's payment notifications to us may include limited billing details (such as the name and email address you gave Stripe), which we do not store. What we store is the payment amount, date, type (one-time or monthly), refund status, and Stripe reference identifiers, linked to your account.

**Preferences and activity.** Settings you choose: candidates you follow, research-area interests, ballot ordering preferences, and email notification opt-ins. Because the Service is about elections, these choices may reflect your civic or political interests; we treat them as your private account data and never sell them or disclose them to third parties for advertising. We use them to provide the features you chose and to select relevant civic updates, information, and recommendations (occasionally including clearly labeled sponsored or promotional content) for the notification emails you control in settings; every such email includes a working unsubscribe link, and the underlying data never leaves us.

**Ask (chat) questions.** If you use the Ask feature, the questions you type are processed to find an answer in our own election database. Questions are logged **anonymously**: before a question is stored, email addresses, phone numbers, street addresses, and long digit sequences are removed, and the stored question is never linked to your account. Stored question text is deleted after 90 days; only aggregate statistics about commonly asked questions (never rare or unique ones) are kept longer. Please don't include personal information in your questions.

**Content reports.** If you report inaccurate or outdated content, we store the report message, the content item you identified, any optional source URL, any optional contact email you provide, and if you are signed in, your account identifier, so we can investigate and improve accuracy. Do not include sensitive personal information in content reports.

**Technical information.** IP address, browser user agent, and request logs collected automatically for security, rate limiting, and abuse prevention. A session cookie (httpOnly) keeps you signed in. We do not use advertising or cross-site tracking cookies.

**Usage analytics.** We may collect information about how the Service is used — such as pages viewed, features used, referring site, device and browser type, and approximate location derived from IP address — to understand usage and improve the Service. Analytics may use cookies or similar identifiers. Analytics data is not used to build advertising profiles, is not combined with your account's civic or political preferences (follows, research-area interests) for any third party's purposes, and is not shared with advertisers.

## 2. How we use information

To provide the Service (find your districts, show your ballot, send notification emails you opted into); to secure the Service (authentication, rate limiting, abuse prevention); to understand and improve how the Service is used (analytics); to communicate with you about your account (verification, password reset, email-change confirmation); and to comply with law. We do **not** sell personal information, share it for cross-context behavioral advertising, or use it to build advertising profiles.

## 3. Service providers

We share information only with the processors needed to run the Service:

- **U.S. Census Bureau geocoder** — receives the address text you enter, to locate your districts.
- **Google Places (autocomplete)** — receives the address text you type when address suggestions are enabled; requests are proxied through our servers so Google does not receive your IP address from your browser.
- **Google (Sign in with Google)** — when you choose to sign in with Google, your browser interacts with Google directly to complete the sign-in (Google's own privacy policy applies to that interaction), and we receive the identity token described in Section 1. We send Google nothing about your activity in the Service.
- **Amazon Web Services (SES)** — receives your email address to deliver account and notification emails.
- **Analytics provider** — when analytics is enabled, receives the usage information described in Section 1 ("Usage analytics") to help us understand and improve how the Service is used. We configure analytics so that this data is not used for the provider's own advertising purposes, and we will name the provider here before or when analytics is enabled.
- **OpenAI (AI answers in Ask)** — when AI-generated answers are enabled for the Ask feature, receives the text of your chat question and the snippets of our own election data used to answer it, together with a pseudonymous account identifier (a cryptographic hash used only for abuse prevention — never your email address, name, or address). We send requests with storage disabled, and under OpenAI's API terms this content is not used to train OpenAI's models. AI answers are labeled as AI-generated in the Service.
- **Stripe (support payments)** — if you choose to make a support payment, Stripe collects your card and billing details directly on its own payment pages (we never receive your card number) and processes the payment, any recurring membership billing, and any refund on our behalf. We receive the payment amount, status, and reference identifiers. Stripe also retains payment records under its own legal obligations; see Stripe's privacy policy at stripe.com/privacy.
- **Sentry (error monitoring)** — when error monitoring is enabled, receives reports about application errors (error type, stack trace, browser and device type, and the page path with its query string removed) so we can find and fix failures. We configure these reports to exclude your IP address, email address, address text, and the contents of your requests.
- **Infrastructure providers** — hosting, database, and cache providers that store Service data on our behalf.

Each receives only what its function requires. We do not share personal information with data brokers, advertisers, campaigns, or political organizations.

We may also disclose information if we believe in good faith that disclosure is required by law, subpoena, or other legal process, or is necessary to protect the rights, property, safety, or security of the Service, our users, or others, or to detect, prevent, or address fraud, abuse, or security issues. If the Service or its operator is involved in a merger, acquisition, reorganization, or sale of assets, account information may be transferred as part of that transaction; this Privacy Policy will continue to apply to it. The Service is operated from the United States, and information is processed and stored in the United States.

## 4. Retention and deletion

Account data is kept while your account is active. If you delete your account, your account record and its associated data — email address, name, the Google account identifier if you signed in with Google, saved districts, follows, preferences, and notification history — are deleted immediately: sessions are destroyed, notification sending stops, and your email address is released for re-registration. Content reports you submitted while signed in are kept for moderation purposes with your account identifier and contact email removed. If you made support payments, any active membership is canceled when you delete your account, and the payment records described in Section 1 (amounts, dates, type, refund status, and Stripe reference identifiers) are retained for accounting, tax, and legal-compliance purposes with the link to your deleted account removed; Stripe separately retains its payment records under its own legal obligations. Residual records may persist in backups and security logs for a limited period before being purged. Cached address lookups expire automatically. Notification event records are pruned on a rolling schedule. Usage analytics data is kept only as long as needed for the improvement purposes described above.

## 5. Your choices

You can view and update your name, email address, password, districts, follows, and email preferences in account settings; every notification email includes a working unsubscribe link; and you can delete your account entirely in settings. Where analytics uses optional cookies or identifiers, you can limit it through your browser's cookie controls, and we honor opt-out mechanisms where required by applicable law. For access or deletion requests you cannot complete in the app, or questions about this policy, contact contact@electionssimplified.com. Depending on where you live, you may have additional privacy rights (such as access, correction, deletion, or portability); we honor valid requests as required by applicable law.

## 6. Security

Passwords are stored using a modern memory-hard hashing algorithm; sessions use httpOnly cookies; email-verification, password-reset, and email-change links are single-use and expire; and account-sensitive actions require your password and are rate limited. An account created with Sign in with Google has no password until you set one; changing your email or deleting your account then requires setting a password first (account settings link to the emailed set-a-password flow). No system is perfectly secure — use a unique password.

## 7. Children

The Service is not directed to children and requires users to be at least 18. We do not knowingly collect personal information from children under 13; if you believe a child has provided us personal information, contact contact@electionssimplified.com and we will delete it.

## 8. Changes

We may update this Privacy Policy; the version and date above identify the current text. For material changes we will provide notice in the Service and may require renewed acceptance.

## 9. Contact

Elections Simplified (operated by Elections Simplified Inc.) — contact@electionssimplified.com
