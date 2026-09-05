# Membership management page (`/me/membership`)

Follow-up to `membership-contributions.md`. Move membership management off the Settings page onto its own page, and let members change their monthly amount and cancel inside the app. Stripe's hosted portal stays only for updating the card.

Revised 2026-09-04 after an external review; the review's accepted and rejected points are folded in below.

## Why

Today Settings carries the whole "Support Elections Simplified" box (signup forms, plan line, portal button, running total, payment history), and the profile's "Manage membership" link only jumps down the same page. Members cannot change their amount at all: the portal offers cancel + card update only, and Stripe's portal cannot take a customer-typed amount (it only switches between pre-made prices, while our monthly amounts are ad-hoc prices created per checkout with `price_data`). The current answer, "cancel and start a new membership", pushes a paying member through a cancel flow to pay more.

## Decisions (user, 2026-09-04)

- **Amount changes never charge anything today.** No proration in either direction. The new amount bills at a future renewal.
- **Advance-notice rule (Reading B).** We treat every amount change as a fee change under CA BPC §17602(g)(2): the member gets a retainable notice of the new amount plus how to cancel **no less than 7 and no more than 30 days before the first charge at the new amount**. Member-initiated changes are not exempted (the statute says "including changes the consumer affirmatively consented to"). Applies to us: CA operator, contracts amended after 2025-07-01 (§17602(j)).
- **Payment history stays, collapsed** in a closed `<details>` at the bottom of the page, with the total inside it.
- Cancel stays **period-end**; reversible until then ("Keep membership").
- Portal only for **Update payment method**, deep-linked to the card screen and returning to `/me/membership`.

## Stripe facts this plan relies on

- `subscriptions.update(id, { items: [{ id: itemId, price_data: { currency: "usd", product, unit_amount, recurring: { interval: "month" } } }], proration_behavior: "none" })` swaps the item's price for a new ad-hoc price on the same product, leaves the billing-cycle anchor alone, and the next invoice at `current_period_end` bills the new amount. Each change creates one more Price object (harmless). Same mechanism Checkout uses.
- Cancel = `subscriptions.update(id, { cancel_at_period_end: true })`. Portal cancels on current API versions arrive as a scheduled `cancel_at` with `cancel_at_period_end` still false (existing test, `membershipService.test.ts` "records a portal period-end cancel"). **Resume must clear whichever is set**: retrieve first; `cancel_at_period_end` → `{ cancel_at_period_end: false }`; `cancel_at` → `{ cancel_at: "" }`; then verify the returned object has neither, else fail.
- `invoice.upcoming` fires N days before a renewal (Billing settings → "Upcoming renewal events"; set to **14 days**). It carries no invoice id; the subscription id is at `parent.subscription_details.subscription` on the endpoint's API version (2026-07-29.dahlia). This is our timing poke for advance notices — no scheduler needed.
- Stripe's Events API only retrieves the last 30 days, so Stripe events are **not** a durable change history. Our own change record is.
- Portal deep link: `billingPortal.sessions.create({ customer, return_url, flow_data: { type: "payment_method_update", after_completion: { type: "redirect", redirect: { return_url } } } })`. Without `after_completion` the flow ends on Stripe's confirmation page.
- Checkout sets `subscription.default_payment_method`, which can shadow the customer default. Rollout includes a sandbox check that a card updated through the portal is the card the subscription bills.
- Stripe **test clocks** let us advance a sandbox customer past a renewal and inspect the invoice — the only way to prove "no charge on save, new amount on the next invoice" for real.

## Amount-change flow (the core of the plan)

State lives in a new table (migration = next free number at write time):

```sql
CREATE TABLE billing_subscription_amount_changes (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  stripe_subscription_id text NOT NULL REFERENCES billing_subscriptions(stripe_subscription_id),
  previous_amount_cents integer NOT NULL,
  new_amount_cents integer NOT NULL CHECK (new_amount_cents >= 500),
  requested_at timestamptz NOT NULL DEFAULT now(),
  applied_at timestamptz,        -- Stripe price swapped
  effective_at timestamptz,      -- the renewal that first bills the new amount (set at apply)
  notice_sent_at timestamptz,    -- §17602(g)(2) notice emailed
  superseded_at timestamptz      -- replaced by a newer request, withdrawn, or subscription ended
);
CREATE UNIQUE INDEX ... ON billing_subscription_amount_changes (stripe_subscription_id)
  WHERE applied_at IS NULL AND superseded_at IS NULL;   -- one pending change per subscription
```

Rows are never deleted (consent/notice evidence; §17602(a)(6) wants 3 years). They hang off `billing_subscriptions`, which already survives account deletion.

**Request** (`POST /api/me/membership/amount`): find the live subscription; retrieve it fresh from Stripe; verify customer, membership product, one monthly USD item; require status `active` or `past_due`, not canceling. If `new == current` and no pending row → 200 no-op; if `new == current` and a pending row exists → supersede it (withdraw) and 200. Otherwise supersede any pending row, insert a new pending row, then run **apply-if-due**. Return the fresh status (includes the pending change).

**Apply-if-due** (one function, called from the request handler, from every subscription webhook poke, and from `invoice.upcoming`): for the subscription's pending row, let `days = current_period_end − now`.
- `7 ≤ days ≤ 30` and status active/past_due and not canceling → `subscriptions.update` with `price_data` + `proration_behavior: "none"`, set `applied_at`, `effective_at = current_period_end`, then send the notice email and set `notice_sent_at`.
- `days < 7` → do nothing; this renewal bills the old amount, and the poke after renewal re-evaluates with `days ≈ 28–31`.
- `days > 30` → do nothing; `invoice.upcoming` (14 days before) will apply it.
- Applied but `notice_sent_at IS NULL` (mail failed earlier) → resend only. A failed notice send returns a retryable 5xx to Stripe, exactly like the existing §17602 start acknowledgment — the notice is the legal requirement, so pokes keep retrying it.
- Subscription terminal, or `cancel_at`/`cancel_at_period_end` set → leave pending untouched (the member may resume); terminal → supersede.

Because the notice is always sent at apply time and apply only happens inside the window, every notice lands 7–30 days before the first charge at the new amount. No cron, no delayed-email queue.

**What the member sees** at request time: "Your new amount starts on <date>", where date = `current_period_end` if `days ≥ 7`, else the renewal after it (display = period end plus one month on the same anchor day; the real date is set at apply). After apply: the same line, now firm. A pending change is shown on the plan line ("$10 per month · $20 from October 4") and can be replaced by saving another amount or withdrawn by saving the current amount.

## Cancel / resume

- `POST /api/me/membership/cancel`: fresh retrieve + ownership check; `incomplete` → 409 `membership_pending` ("your first payment is still being confirmed — try again in a moment"; `incomplete` is a seconds-wide race by design and auto-expires, so no immediate-cancel path); already canceling → sync + 200; else `cancel_at_period_end: true`, upsert, email, return status.
- `POST /api/me/membership/resume`: fresh retrieve; not canceling → 200 no-op; else clear per the Stripe fact above, verify, upsert, email, return status. A pending amount change survives resume.
- Copy says "Your membership will not renew after <date>", never "no further charges" — an open `past_due` invoice can still be collected.

All three endpoints are **set-this-state** actions: if Stripe already has the requested state, sync and return 200 without mutating or re-emailing. That makes a lost-response retry harmless.

## Backend checklist

- `apiValidation.ts`: `ME_MEMBERSHIP_AMOUNT_PATH`, `ME_MEMBERSHIP_CANCEL_PATH`, `ME_MEMBERSHIP_RESUME_PATH`; `parseMembershipAmountBodyValue` reusing the checkout amount check (integer cents, 500–100,000 — no new whole-dollar rule; the UI already limits to whole dollars). Portal body gains optional `flow: "payment_method_update"`; `{}` remains the general portal so already-open old Settings tabs keep their cancel path during rollout.
- `apiServer.ts`: route recognition (the `pathname ===` list near line 173), the JSON-parser allowlist (near line 503) for all three POSTs, handlers mirroring checkout/portal (unwired → 404, wrong method → 405, `requireVerifiedAuthenticatedUser`).
- `apiErrors.ts`: map `membership_pending` → 409, `no_membership` → 404, ownership mismatch → 409; anything else keeps the existing 503/500 mapping.
- `membershipService.ts`:
  - Split `syncSubscription` into a pure `upsertSubscriptionRow(subscription)` and the existing wrapper that also sends the start acknowledgment. Endpoints call the pure upsert so a mail failure can never report a failed cancel that Stripe accepted.
  - Serialize retrieve→write per subscription with `pg_advisory_xact_lock(hashtext(stripe_subscription_id))` inside a transaction, in both the webhook path and the endpoints. Closes the stale-write race (old poke's write landing after our update) with no migration and no inbox.
  - `getMembership` adds `pending_amount_change: { new_amount_cents, starts_at, applied } | null`.
  - New methods: `changeMonthlyAmount`, `cancelMembership`, `resumeMembership`, `applyDueAmountChange`; `createPortalSession(userId, flow?)`.
  - Injected Stripe client type gains `subscriptions.update`.
  - `membership_exists` message → "You already have a monthly membership. Change the amount under Manage membership."
- Webhook: subscribe `invoice.upcoming` (endpoint event list becomes 7); handler resolves the subscription id and runs apply-if-due; unknown subscription → 200 ignore. Subscription pokes also run apply-if-due after the upsert.
- `membershipMailer.ts`: one `SendMembershipChangedEmail` with kinds `amount_notice` (new amount, "starting <date>", how to cancel, Terms link, not-a-charity sentence), `canceled` ("will not renew after <date>", Keep membership link), `resumed` (next charge date + amount, cancel path). SES + console variants. `manageMembershipUrl` → `${publicBaseUrl}/me/membership`; the started-email link text becomes "Manage membership" (it now links straight there).
- `runAddressApiServer.ts`: wire the three callbacks and the mailer; Stripe on + mailer off stays a boot failure.
- Terms: `docs/legal/terms-of-use.md` 14.3 "(via the Stripe billing portal)" → "under Manage membership". Re-pin the sha in `legalCopy.test.ts`; **no version bump** (navigation wording, no change in rights — a bump would force every user through the re-accept interstitial).

### Tests (backend)

- Amount: request inside window applies immediately (`price_data`, `proration_behavior: "none"`) and emails; `days < 7` stays pending and applies on the post-renewal poke; `days > 30` applies on `invoice.upcoming`; notice send failure → 5xx and a later poke resends without re-applying; same-amount request withdraws pending; product/customer mismatch never mutates; `incomplete`/`unpaid`/canceling rejected; lost-response retry is a no-op.
- Cancel/resume: happy paths; legacy `cancel_at` resume clears it and verifies; already-in-state → 200 no mutation; `incomplete` → 409.
- Concurrency: two interleaved syncs (stale poke vs. endpoint) end with the current Stripe amount.
- Portal: `{}` = general portal; `flow` = deep link with `after_completion` redirect to `/me/membership`.
- API: 404 unwired, 405, auth gates, JSON enforcement on empty-body POSTs, body validation, response shape.
- Mailer: subject/body per kind, HTML escaping.

## API client

- `MembershipMembership` gains `pending_amount_change`. The three POSTs return `MembershipStatus`.
- Membership `queryFn` passes the query's `signal` through `apiRequest` so an in-flight GET can be canceled before a mutation installs its result.

## Frontend

### `/me/membership` (`pages/MembershipPage.tsx`, route `me/membership`)

Auth-required like the other `/me/*` pages; same login-return and verified-email handling as Settings. Layout:

1. Heading **Your membership**; under it, for an active member, "Thank you. Because of supporters like you, Elections Simplified stays independent and free for every voter."
2. Plan line: "$10 per month · renews October 4, 2026". With a pending change: "$10 per month · $20 from October 4, 2026". Canceling: "$10 per month · will not renew after October 4, 2026" + **Keep membership**. Missing `current_period_end` → omit the date, never invent one.
3. Status notices: `incomplete` → first payment still confirming (existing copy); `past_due`/`unpaid` → "Your last payment didn't go through." placed directly above **Update payment method**.
4. **Change amount** — whole-dollar input (reuse `AmountForm` parsing, `MIN_DOLLARS`/`MAX_DOLLARS`), pre-filled with the current amount, button "Save new amount", helper "Your new amount starts on <computed date>. Nothing is charged today." Hidden for `incomplete`, `unpaid`, and while canceling (matches the backend). Disabled while any mutation is pending.
5. **Cancel membership** — secondary button → inline confirm "Your membership stays active until October 4, 2026 and will not renew after that." with **Cancel membership** / **Never mind**. Hidden for `incomplete`.
6. **Update payment method** — link; posts `{ flow: "payment_method_update" }`, redirect stays in flight until the browser leaves.
7. `<details>` **Recent payments** (closed): "Total support to date: $55.00" first, then the list (the list is the latest 50; the total covers everything).
8. Small disclaimer (existing `Disclaimer`).

Mutations: `cancelQueries(["me","membership"])` then `setQueryData` with the returned status. Inline `role="status"` confirmations: "Saved. $20 per month starts October 4, 2026." / "Your membership will not renew after October 4, 2026." / "Welcome back — your membership continues."

Other states on the same page: non-member with payments → history only plus "Become an honorary member" (`/support/member`) and "Support once" (`/support/once`); non-member without payments → those two links; `enabled: false` → "Payments are temporarily unavailable."

### Settings page

- Remove `<MembershipSection />` and the `#support` anchor.
- Profile line for **every nonterminal** subscription, `text-ink` (not soft): `active` → "Thank you for being a supporting member." + link **Manage membership**; `incomplete` → "Your membership is being set up." + link; `past_due`/`unpaid` → "Your last membership payment didn't go through." + link **Fix payment**. All links go to `/me/membership`.
- `membership === null` with `payments.length > 0` → "Payment history" link to `/me/membership`. No membership, no payments → the existing green **Become an honorary member** button.
- Delete `MembershipSection`; move `SupportCheckout` + `MembershipThanks` + shared hooks/forms to `components/SupportCheckout.tsx`; `MemberPlan` goes away.

### Other surfaces

- `/support/member`: existing `active`/`past_due` member → "You're already an honorary member — thank you." + link to `/me/membership`; `incomplete` → "Your membership is being set up." + link (never call an incomplete signup a member).
- `/mission` `MembershipThanks`: "Manage membership" becomes a link to `/me/membership` (no portal call).
- Checkout success banner: "Your payment may take a moment to appear in your Settings" → "…on your membership page."
- Prose on `/support` keeps the user-chosen "contribution" wording; only payment-form labels use "support".
- Sitemap / router worker: `/me/*` already excluded. Mobile: unchanged (informational row, 2026-08-28 decision).

### Tests (frontend)

`MembershipPage.test.tsx`: plan line + dates; pending-change line; amount form validation, posts `amount_cents`, shows "starts on"; cancel reveals confirm, posts, then shows will-not-renew + Keep; Keep posts resume; portal link posts `flow` and calls `navigateExternal`; forms hidden for `incomplete`/`unpaid`/canceling; failed-payment notice sits next to Update payment method; non-member with/without history; `enabled:false`; details closed by default; in-flight GET does not overwrite a mutation result. `SettingsPage.test.tsx`: box gone; link per status; history link. Retarget the surviving `MembershipSection` tests to `SupportCheckout`/`MembershipThanks`.

## Phases

Three PRs, each deployable on its own; nothing user-visible changes until PR 3.

- **PR 1 — backend plumbing (no migration). BUILT 2026-09-04.** `upsertSubscriptionRow` split, cancel/resume endpoints + confirmation emails, portal `flow` param + `after_completion` redirect, Terms 14.3 wording + sha re-pin, `membership_pending` / `membership_conflict` error codes. Settings keeps working unchanged. Deviations from the plan above, on purpose:
  - **Stale-write guard moved to PR 2.** An advisory lock needs one connection held across the Stripe call (pool-holding, and the routed-query test harness has no client), and a no-migration `updated_at` trick compares write-times, not retrieve-times — it can skip a *fresher* write. PR 2 adds a `stripe_synced_at` column with `WHERE stripe_synced_at <= $retrievedAt` guards alongside its migration.
  - **All email links and the general-portal return stay on `/me/settings`** until PR 3 ships the page; only the `flow` deep link (which nothing calls before PR 3) returns to `/me/membership`. PR 3 flips `manageMembershipUrl`, the started-email link label, and the `membership_exists` message ("change the amount under Manage membership" would be false until amount change exists).
  - Terms text is bundled into the frontend (`LegalDocumentPage` imports the markdown), so the 14.3 wording needs the **ssr deploy** too, not only api.
  - Verified against the sandbox subscription: a period-end cancel comes back from Stripe with BOTH `cancel_at` and `cancel_at_period_end: true` on API 2026-07-29; one `{ cancel_at: "" }` update clears both. Resume still checks the boolean afterwards for older representations.
- **PR 2 — amount change. BUILT 2026-09-04.** Migration 273 (`billing_subscription_amount_changes` + `billing_subscriptions.stripe_synced_at`), `changeMonthlyAmount` + apply-if-due, `invoice.upcoming` handler, `amount_notice` email, `pending_amount_change` in the status, `POST /api/me/membership/amount`. Verified against the sandbox subscription: request at ~30 days → applied at once, Stripe's upcoming-invoice preview bills the new amount at the unchanged period end, no invoice created; replace inside the window, revert, same-amount no-op, and cancel → 409 → resume all behave as below. Deviations and refinements, on purpose:
  - **Stale-write guard = `stripe_synced_at`, not an advisory lock.** Every writer carries the instant its Stripe state was known current (a retrieve's start, a mutation's return) and the upsert's `WHERE stripe_synced_at <= EXCLUDED.stripe_synced_at` drops older writes. A poke that read Stripe between a mutation's apply and its return can still lose to the mutation's equal-or-newer state; the mutation's own webhook re-syncs, so nothing stays wrong. Rows written before the column existed are NULL = always overwritable.
  - **`monthly_amount_cents` in the status = what the current period costs.** After a swap Stripe (and the row) already carry the new price, so the status reports the price the first applied, still-unbilled swap replaced. `pending_amount_change` is the latest un-superseded unbilled request; a request back to this period's amount (a revert inside the window) reports `null`.
  - **Replacing an applied change** is allowed while the window is still open (supersede, new swap, new notice — one more Price object). Once fewer than 7 days remain it is refused with 409 `membership_pending` ("already set for your next renewal"); there is no time to notice a replacement. The other refusals (`incomplete`, `unpaid`, scheduled to cancel) reuse `membership_pending` too — the message carries the specifics, no new error codes.
  - **Notice first, then the swap** (review 2026-09-04: with swap-first, a mail outage could leave the new price live with a notice landing inside 7 days, or with no notice at all). The notice is sent and stamped with the renewal it names (`notice_sent_at` + `effective_at`) before the Stripe price moves; a failed send leaves Stripe untouched and the request pending (the webhook path 5xxes; the request path answers the pending status and the next poke retries). A notice whose `effective_at` is no longer the current period end is stale and goes out again before the swap. No sender configured = no swap.
  - **Reconcile on every poke** (same review: a poke that read the previous request could finish its swap after a newer request had applied, leaving Stripe on the old amount with no later repair). Once a change is applied, each poke compares Stripe's item price with the noticed amount and swaps back on drift, and `applied_at` is only ever stamped on an un-superseded row. The older swap's own webhook triggers the repair within seconds. Not a lock: nothing is held across a Stripe call.
  - **Projected start uses the billing anchor day** (`started_at`'s day, which is Stripe's `billing_cycle_anchor` for Checkout subscriptions): a Jan 31 member whose Feb 28 renewal is under 7 days away is shown Mar 31, not Mar 28.
  - **`invoice.upcoming` runs the full subscription sync**, not a separate apply-only path: the same guards, upsert, and apply-if-due as any other poke.
  - **Window arithmetic is fractional days, both bounds.** Right after a renewal a 31-day month reads ~30.99 days out, so the post-renewal poke does not apply; `invoice.upcoming` at 14 days does. If that event is missing from the live endpoint, changes still apply at the next renewal poke of a ≤30-day month — a slip, never a wrong notice.
  - **Concurrent requests**: supersede + insert are not one transaction; the partial unique index plus `ON CONFLICT … DO NOTHING` drops the loser's row and its answer shows the winner's request.
  - The api-client `MembershipMembership` type gained `pending_amount_change` here (additive); the POST helper and `queryFn` signal are PR 3.
- **PR 3 — frontend.** `/me/membership` page, Settings cleanup, `/support/member` + `/mission` retargets, success-banner copy, the URL/label/message flips listed under PR 1, tests. Ships the feature.

## Rollout

1. Stripe dashboard, done ahead of PR 2 (safe now — the webhook handler logs and 200s unknown event types): add `invoice.upcoming` to the webhook endpoint (sandbox and live); set Billing → "Upcoming renewal events" to 14 days.
2. PR 1 → deploy api. PR 2 → apply the migration, deploy api. Old clients keep working (GET unchanged; `{}` portal unchanged).
3. After PR 2 is deployed, sandbox proof with a **test clock** customer: save a new amount → no charge today, renewal date unchanged; advance past `invoice.upcoming` → change applied + notice email; advance past renewal → invoice bills the new amount. Also: portal card update → the subscription's `default_payment_method` is the new card. Then: cancel → resume (legacy `cancel_at` variant included) → account delete still cancels.
4. PR 3 → deploy api + ssr.
5. Local worktree: the demo member is wired to a real sandbox subscription, so amount/cancel/resume/portal exercise real Stripe calls; `invoice.upcoming` cannot reach localhost, so the window path is covered by unit tests plus step 3.

## Out of scope

- Immediate (prorated) amount changes — decided against.
- Changing the renewal day, pausing, annual plans, admin-side changes, history export, custom card form, mobile expansion.
- A general webhook inbox, scheduler, or subscription schedules — the pending row + `invoice.upcoming` poke is the whole timing mechanism.
