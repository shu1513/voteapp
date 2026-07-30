# Address Autocomplete — Frontend Contract

The backend proxies Google Places (New) autocomplete behind two endpoints so
the frontend never talks to Google directly: the API key stays server-side, the
browser never calls Google (so the user's browser IP never reaches Google —
only the backend's does), and the provider is swappable server-side. Note the
typed address text **is** forwarded to Google to fetch suggestions; proxying
hides the key and the user's IP, not the address input itself.
This document is the complete contract for implementing the typeahead input in
the frontend repo.

## Endpoints

Both require `Content-Type: application/json`. Both return the standard error
envelope `{ "error": { "code", "message" } }` on failure.

### POST /api/address/autocomplete

Request:

```json
{ "input": "1600 Penn", "session_token": "0aa2ee7a-8f0f-4b3f-9c53-1b6f9d6a2f11" }
```

- `input`: 3–200 chars after trimming. Shorter input → 400; don't call until
  the user has typed 3+ characters.
- `session_token`: 8–128 chars of `[A-Za-z0-9_-]`. Use a UUIDv4
  (`crypto.randomUUID()`).

Response `200`:

```json
{
  "suggestions": [
    {
      "place_id": "ChIJGVtI4by3t4kRr51d_Qm_x58",
      "description": "1600 Pennsylvania Avenue NW, Washington, DC 20500, USA",
      "main_text": "1600 Pennsylvania Avenue NW",
      "secondary_text": "Washington, DC 20500, USA"
    }
  ]
}
```

`suggestions` may be empty. Render `main_text` bold with `secondary_text`
muted (or just `description`).

### POST /api/address/autocomplete/retrieve

Call once when the user picks a suggestion.

Request:

```json
{ "place_id": "ChIJGVtI4by3t4kRr51d_Qm_x58", "session_token": "0aa2ee7a-8f0f-4b3f-9c53-1b6f9d6a2f11" }
```

Response `200`:

```json
{ "address": "1600 Pennsylvania Avenue NW, Washington, DC 20500, USA" }
```

### Errors (both endpoints)

- `400 invalid_request` — bad input/token; treat as "no suggestions".
- `429 rate_limited` — honor the `retry-after` header; stop firing requests.
- `500 internal_error` "Address autocomplete is not configured" — the backend
  has no Google key; hide the dropdown entirely and fall back to plain input.
- `502 bad_upstream_response` / `503 upstream_unavailable` — Google hiccup;
  show no dropdown, let the user keep typing. Never block manual entry.

## Session token lifecycle (billing-relevant)

How Google bills a session that ends in a Place Details retrieve (the New
pricing model): the first 12 autocomplete requests in the session are each
billed (Autocomplete Requests SKU), any beyond 12 are free, and the terminating
Place Details Essentials request is also billed. Each SKU has its own monthly
free tier (~10k). A normal debounced address entry fires well under 12 requests,
so in practice you pay for each suggest request plus one retrieve — the
autocomplete free tier is the binding limit (~1–2k completed entries/month
before charges), not Place Details.

Still send a session token: without one, autocomplete requests bill the same way
but you forfeit the 13+-free tier and the correct session accounting, and reused
tokens are treated as no-session. It is a correctness/hygiene requirement, not a
large cost saver at our volume.

- Generate a fresh `crypto.randomUUID()` when the user **starts** an address
  entry (first keystroke that triggers a suggest call).
- Send the **same** token on every suggest call and the final retrieve for
  that entry.
- After a retrieve completes — or the user clears the field and starts over —
  the session is dead. Generate a new token for the next entry. Never reuse.

## Input behavior

- Debounce 250–300 ms after the last keystroke; minimum 3 characters.
- Cancel in-flight requests when a new one fires (`AbortController`) and
  ignore out-of-order responses.
- Selecting a suggestion: call retrieve, put the returned `address` string
  into the input, then run the **existing** flow unchanged:
  - anonymous: `POST /api/address/resolve` with
    `{ "address": ..., "accepted_terms_version": TERMS_VERSION }` — the
    clickwrap is enforced server-side, so the version is required
  - logged-in: `PUT /api/me/address` with `{ "address": ... }` (no clickwrap
    field: the account already carries its acceptance)
- Autocomplete failing must never block the form — the input stays a plain
  text field that submits to the same endpoints.
- Use an ARIA combobox pattern (`role="combobox"`, `aria-expanded`,
  `aria-activedescendant`, arrow-key navigation, Escape to close) or a
  maintained headless component — don't hand-roll keyboard handling.

## Compliance

- Show **"powered by Google"** attribution on the suggestions dropdown
  (required when predictions are displayed without a Google map).
- Do not store, cache, or log suggestions or retrieved addresses beyond the
  current entry session (Google ToS). Persist only the app's own output: the
  resolved districts from `/api/address/resolve` / `/api/me/address`.
