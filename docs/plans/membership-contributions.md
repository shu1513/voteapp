# Membership & one-time support payments

Let signed-in users support the app with a custom monthly membership (minimum $5/month, any amount above) or a one-time payment (minimum $5). Every dollar is recorded in our own database: who paid, how much, when, and refunds. Stripe handles all card data; we store references only.

Not a nonprofit. UI copy must say the money supports operating the service — never candidates, campaigns, committees, parties, or charities — and must never claim charitable deductibility. Brand in UI copy is **Elections Simplified** (VoteApp is the internal repo name). Use "support" / "supporter" / "membership" in customer-facing copy — avoid "contribution"/"donation", which read as political/charitable in this product. No content is ever gated behind membership; perks are recognition only (out of scope for v1).

## Stripe facts this plan relies on (verified 2026-08-21)

- Checkout `custom_unit_amount` (pay-what-you-want prices) is **one-time only** ([docs](https://docs.stripe.com/payments/checkout/pay-what-you-want)). Custom monthly amounts instead use inline `price_data` with a dynamic `unit_amount`, which works in both `mode: "payment"` and `mode: "subscription"` (`recurring: {interval: "month"}`). One mechanism for both flows: user types a dollar amount in *our* UI, backend creates the session. Server enforces minimums.
- Webhook signature verification (`stripe.webhooks.constructEvent`) needs the **exact raw request body** — add an `express.raw` branch for the webhook path ahead of `createJsonBodyParser()` (already path-scoped).
- Stripe **does not guarantee webhook event order** and retries failed (non-2xx) deliveries for up to ~3 days ([docs](https://docs.stripe.com/webhooks)). Handlers must be idempotent and must not trust event arrival order for state.
- The SDK's pinned API version does **not** pin webhook payload shapes — the webhook endpoint's own configured API version controls those ([versioning](https://docs.stripe.com/api/versioning)). Pin the endpoint version when creating it (prod checklist).
- Stripe-hosted customer portal gives cancel + card-update UI; its **defaults allow customers to edit email/name and switch plans** — configure it explicitly ([portal config](https://docs.stripe.com/customer-management/configure-portal)).
- Fees 2.9% + 30¢ per domestic card charge → a $1 payment loses ~33% to fees. Hence $5 minimum on one-time too (~9% at $5).
- SDK: `stripe` npm package v22.x.

## Account-deletion constraint (drives the schema)

Account deletion is a **hard delete** — `DELETE FROM users` with `ON DELETE CASCADE` on every user-owned table ([authService.ts:1275](backend/src/auth/authService.ts#L1275)), promised by the privacy policy. So:
- Payment history must NOT reference `users.id` directly: a `NOT NULL` FK would block deletion; a cascade would erase accounting records we are legally required to keep.
- Solution: a `billing_customers` indirection row. On account deletion its `user_id` is set NULL — payment records survive as de-identified accounting data (amounts, dates, Stripe refs; no name/email — those live only in Stripe, which has its own retention).
- Deletion flow must **cancel any active subscription first** (Stripe would otherwise keep charging a card for a deleted account), then let `user_id` null out. Deletion must not fail if Stripe is unreachable: cancel is attempted; on failure, log loudly and continue the delete (manual cleanup from the Stripe dashboard; the orphaned subscription is visible there).

## Data model — migration 250 (next free number at write time)

```sql
-- Indirection that survives account deletion.
CREATE TABLE billing_customers (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id uuid UNIQUE REFERENCES users(id) ON DELETE SET NULL,  -- nullable
    stripe_customer_id text NOT NULL UNIQUE,
    created_at timestamptz NOT NULL DEFAULT now()
);

-- Ledger: one row per successful charge, monthly and one-time alike.
CREATE TABLE billing_payments (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    billing_customer_id uuid NOT NULL REFERENCES billing_customers(id),
    amount_cents integer NOT NULL CHECK (amount_cents > 0),
    currency text NOT NULL DEFAULT 'usd',
    kind text NOT NULL CHECK (kind IN ('one_time', 'monthly')),
    stripe_payment_ref text NOT NULL UNIQUE,  -- payment_intent id (one-time) or invoice id (monthly)
    refunded_amount_cents integer NOT NULL DEFAULT 0 CHECK (refunded_amount_cents >= 0),
    refunded_at timestamptz,
    paid_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX billing_payments_customer_paid_at_idx
    ON billing_payments (billing_customer_id, paid_at DESC);

-- Subscription state: at most one per billing customer (v1).
CREATE TABLE billing_subscriptions (
    billing_customer_id uuid PRIMARY KEY REFERENCES billing_customers(id),
    stripe_subscription_id text NOT NULL UNIQUE,
    monthly_amount_cents integer NOT NULL CHECK (monthly_amount_cents >= 500),
    stripe_status text NOT NULL,           -- raw Stripe status verbatim
    cancel_at_period_end boolean NOT NULL DEFAULT false,
    current_period_end timestamptz,
    started_at timestamptz NOT NULL,
    canceled_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);
```

- "Is this user a paid member" = join through `billing_customers` where `stripe_status IN ('active', 'past_due')` (`past_due` keeps member treatment during the retry window; everything else — `canceled`, `incomplete`, `unpaid`, etc. — is not a member). Raw status is stored verbatim so new Stripe statuses never break the schema.
- `stripe_payment_ref UNIQUE` = idempotent ledger writes under webhook retries (`ON CONFLICT DO NOTHING`).
- Totals shown to users = `SUM(amount_cents - refunded_amount_cents)` (net).
- All identifiers ≤ 63 chars.

## Backend

New module `backend/src/api/membership/` (service + helpers), wired through `AddressApiServerOptions` like existing features (see `getAuthenticatedEmailPreferences`: option injected in `runAddressApiServer.ts`, handler in `apiServer.ts`). Stripe client injected at the service boundary so tests mock it without network.

### Endpoints

First three: `requireVerifiedAuthenticatedUser` + JSON-parser path list. Webhook: public predicate list, signature-verified, raw body, **exempted from the global per-IP rate limiter** (`app.use(createRateLimitMiddleware…)` applies to every path and buckets by IP — Stripe's shared delivery IPs would 429 under the 60/min default).

1. `GET /api/me/membership` — `{ enabled, stripeStatus, monthlyAmountCents, cancelAtPeriodEnd, currentPeriodEnd, totalNetCents, payments: [...latest 50] }`. When Stripe isn't configured returns `{ enabled: false }` (frontend hides the whole section — no broken UI, no separate feature flag).
2. `POST /api/me/membership/checkout` — body `{ kind: 'monthly' | 'one_time', amountCents }`.
   - Validate: integer; ≥ 500 both kinds; ≤ 100_000 (=$1,000 card-testing cap — raise later if a real supporter asks).
   - Monthly while a subscription row is in a live status → 409 (amount change = cancel in portal, resubscribe; portal plan-switching is disabled).
   - Get-or-create `billing_customers` row + Stripe customer, then create the session:
     - `line_items[0].price_data`: `{currency: 'usd', product: STRIPE_MEMBERSHIP_PRODUCT_ID, unit_amount, recurring?}`; one dashboard-created Product shared by both flows.
     - `payment_method_types: ['card']` — cards only in v1, so no async-payment-method events (`checkout.session.async_payment_*`) to handle.
     - `customer`, `client_reference_id` = billing_customer_id; `metadata: {billingCustomerId, kind}` on the session, mirrored into `payment_intent_data.metadata` (one-time) / `subscription_data.metadata` (monthly). Internal billing id in metadata, never raw user id.
     - `success_url`/`cancel_url` → settings page `?membership=success|canceled`.
   - Frontend disables the button while the request is in flight (double-click guard); a rarer cross-tab race is closed by the webhook guard below.
3. `POST /api/me/membership/portal` — portal session, `return_url` → settings. 404 without a billing customer.
4. `POST /api/stripe/webhook` — verify signature; response policy:
   - `400`: bad signature/payload.
   - `2xx`: event ignored, or DB transaction committed.
   - `5xx`: transient DB/internal failure → Stripe retries (this is the delivery guarantee; "always 200" would silently drop payments).
   Events:
   - `checkout.session.completed` (`mode: payment`) → insert ledger row (`one_time`, ref = payment_intent).
   - `checkout.session.completed` (`mode: subscription`) → **fetch the subscription from Stripe and upsert `billing_subscriptions` from that current state** (no ledger row here — first month's row comes from `invoice.paid`; writing it in both places double-counts).
   - `customer.subscription.created` / `.updated` / `.deleted` → same treatment: the event is a poke, the fetched subscription object is the truth. This makes out-of-order delivery harmless — every handler writes current reality, not the event's snapshot.
   - `invoice.paid` (subscription invoices, amount > 0) → ledger row only (`monthly`, ref = invoice id). Never flips membership status — status recovery arrives via its own `subscription.updated` event.
   - `invoice.payment_failed` → nothing (status change arrives via `subscription.updated`; log it).
   - `refund.created` (and `charge.refunded` as belt-and-suspenders) → set `refunded_amount_cents`/`refunded_at` on the ledger row matching the payment ref. Partial refunds accumulate. Disputes: log + handle manually in the dashboard (out of code scope; volume will be ~0).
   - Duplicate-subscription guard: on subscription upsert, if the billing customer already has a **different** live subscription id, cancel the newly arrived one via API and log — closes the concurrent-checkout race without reservation machinery.
   - Unknown event types / unmatched refs → 200 + log.

### Account-deletion integration

In `authService` deleteAccount, before `DELETE FROM users`: look up the user's billing customer; if a live subscription exists, cancel it at Stripe (immediate, not period-end). Stripe failure → log loudly, proceed with delete anyway. `billing_customers.user_id` nulls via FK.

### Config

- `STRIPE_SECRET_KEY`, `STRIPE_WEBHOOK_SECRET`, `STRIPE_MEMBERSHIP_PRODUCT_ID` (`.env.example` + render.yaml `sync: false`).
- Presence of `STRIPE_SECRET_KEY` turns the feature on; startup then **fails fast if the other two are missing** (partial config = crash at boot, not 500s at runtime). All absent = feature cleanly off (per feature-flags policy: money features default off).
- New dependency: `stripe` (backend only). No frontend Stripe SDK — Checkout and portal are full-page redirects.

### Backend tests

- Checkout validation (mins, cap, kind, non-integer); 409 while live subscription.
- Webhook: bad signature → 400; DB error → 5xx; each event type; duplicate delivery no-ops; out-of-order subscription events converge (fetch-current makes this trivial); subscription-mode completion writes no ledger row; refund updates net totals; duplicate-subscription guard cancels the newcomer.
- Deletion: active member delete cancels subscription and nulls `user_id`; Stripe outage doesn't block delete.
- GET totals = net of refunds.

## Frontend

"Support Elections Simplified" section on [SettingsPage.tsx](frontend/src/pages/SettingsPage.tsx), existing useQuery/useMutation card pattern. Hidden entirely when `enabled: false`.

- Non-member: support pitch + two dollar inputs/buttons ("Become a monthly supporter — $5/month minimum", "One-time support"). Copy (final legal review in rollout phase): *"Elections Simplified is independently operated. Optional payments support operating the service — not any candidate, campaign, committee, party, or charity. Payments provide no additional content or influence and are not eligible for a charitable-contribution receipt."*
- Member: "Monthly supporter — $X/month since {date}"; if `cancel_at_period_end`, "ends {current_period_end}"; net total supported; "Manage membership" → portal. `past_due` → fix-payment nudge (portal link).
- Payment history (latest 50, no pagination v1).
- `?membership=success` → thank-you banner + refetch ("may take a moment to appear" — webhook lag; no polling).
- Whole-dollar input, cents conversion client-side, min enforced both ends; buttons disabled in flight.

Frontend tests: hidden-when-disabled, three states render, min validation, redirect on checkout response.

Mobile: out of scope v1. (Apple's external-purchase rules now vary by region — US allows external purchase links with conditions; don't add any mobile surface without a per-region guideline check.)

## Phases

1. **PR 0 — legal**: privacy policy (name Stripe as a third party in Section 3; amend Section 4: de-identified payment records are retained for accounting/legal compliance after account deletion) + terms of use (payments/subscription/refund/cancellation section). Small doc-only PR, merges first — the retention promise must change before any payment exists.
2. **PR 1 — backend**: migration 250, `stripe` dep, service, 4 endpoints, deletion integration, rate-limiter exemption, wiring, tests. Inert without env keys.
3. **PR 2 — frontend**: settings section + tests.
4. **Rollout** (no PR): Stripe live account — confirm account classification for voluntary support payments with Stripe ([their tips/donations requirements](https://support.stripe.com/questions/requirements-for-accepting-tips-or-donations)); create Product; restricted API key; webhook endpoint (5 event types, **pin its API version to the SDK's pinned version**); configure portal explicitly (payment-method update + invoice history + period-end cancel ON; email/name edits + plan switching OFF); enable Stripe email receipts for recurring charges (doubles as the recurring-payment reminder California's auto-renewal law expects; portal link in settings = the required easy cancellation); enable Stripe's failed-webhook email alerts; set 3 env vars in Render (manual sync approve); apply migration 250 (prod script-file pattern); test-mode E2E first, then live $5 → verify ledger row → refund it → verify net total drops to $0 → cancel → delete a test account while subscribed.

## Explicitly out of scope (v1)

Perks/badges/supporters page; dispute automation (manual); anonymous or logged-out payments; amount change without cancel+resubscribe; non-USD; mobile; pagination; a `stripe_webhook_events` inbox table and a scheduled reconciliation script (revisit if volume or audit needs grow — at launch scale, idempotent handlers + Stripe's dashboard event log + failed-delivery alerts cover it).

## References

- [Pay-what-you-want limits](https://docs.stripe.com/payments/checkout/pay-what-you-want) · [Webhooks: retries & ordering](https://docs.stripe.com/webhooks) · [Subscription webhooks](https://docs.stripe.com/billing/subscriptions/webhooks) · [API versioning](https://docs.stripe.com/api/versioning) · [Refunds](https://docs.stripe.com/refunds) · [Portal configuration](https://docs.stripe.com/customer-management/configure-portal) · [Idempotent requests](https://docs.stripe.com/api/idempotent_requests) · [stripe-node releases](https://github.com/stripe/stripe-node/releases)
