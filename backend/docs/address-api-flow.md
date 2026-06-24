# Address API Flow

This API supports an anonymous address lookup first, an initialize-only saved-district write after signup, and a saved ballot lookup for returning signed-in users.

## Anonymous Address Lookup

The address lookup endpoint is read-only:

```http
POST /api/address/resolve
content-type: application/json

{
  "address": "3921 Harlan Ave Baldwin Park CA 91706"
}
```

Response:

```json
{
  "matched_address": "3921 HARLAN AVE, BALDWIN PARK, CA, 91706",
  "districts": [
    {
      "id": "...",
      "district_type": "county",
      "geoid_compact": "06037",
      "name": "Los Angeles County",
      "state": "CA",
      "state_fips": "06",
      "population": 9876482,
      "representation_power_score": 12.3
    }
  ]
}
```

Frontend behavior:

1. Keep `districts.map((district) => district.id)` in temporary client state.
2. Use those IDs to fetch ballot summaries with `GET /api/ballot?district_ids=...`.
3. Do not write `user_districts` from this endpoint.

## Post-Signup District Initialization

After successful signup only, call:

```http
POST /api/me/districts/initialize
content-type: application/json

{
  "district_ids": ["..."]
}
```

This route requires an authenticated gateway to inject the configured `API_TRUSTED_USER_ID_HEADER`. Browsers must not set that header directly.

Successful first-time response:

```json
{
  "status": "initialized",
  "district_count": 7
}
```

If the user already has saved districts:

```json
{
  "status": "already_initialized",
  "district_count": 7
}
```

Frontend behavior:

1. Call this endpoint after signup if temporary address districts exist.
2. Retry on network failure; the operation is initialize-only and idempotent.
3. On normal login, do nothing. Do not overwrite existing saved districts.
4. Treat `district_count` as the count stored on the server, not necessarily the count submitted by the client.

## Returning User Ballot Lookup

When an already registered user opens the app, the frontend should fetch the user's saved ballot directly:

```http
GET /api/me/ballot
```

This route requires the same authenticated gateway boundary as `POST /api/me/districts/initialize`. The backend reads the trusted user ID, loads saved district IDs from `public.user_districts`, and returns the same response shape as `GET /api/ballot`.

Example response when the user has saved districts:

```json
{
  "district_ids": ["..."],
  "districts": [
    {
      "id": "...",
      "district_type": "county",
      "geoid_compact": "06037",
      "name": "Los Angeles County",
      "state": "CA",
      "state_fips": "06",
      "population": 9876482,
      "representation_power_score": 12.3
    }
  ],
  "elections": []
}
```

If the signed-in user has no saved districts yet, the route returns an empty ballot instead of an error:

```json
{
  "district_ids": [],
  "districts": [],
  "elections": []
}
```

Frontend behavior:

1. Signed-in returning user: call `GET /api/me/ballot` on app open.
2. If the response is empty, show the address-entry flow so the user can add districts.
3. Anonymous user: keep using `POST /api/address/resolve`, then `GET /api/ballot?district_ids=...`.
4. When a user clicks an election from either flow, keep using the election-detail endpoint.

## Security Boundary

`API_TRUSTED_USER_ID_HEADER` is trusted only when a gateway authenticates the user, injects the header, and strips any client-supplied copy.

If `API_TRUSTED_USER_ID_HEADER` is unset, authenticated routes fail closed with `401`.

## Integration Test

The row-lock behavior for signup initialization is covered by an opt-in Postgres integration test. Run it only against a disposable migrated database:

```bash
USER_DISTRICTS_INTEGRATION=true \
USER_DISTRICTS_INTEGRATION_DATABASE_URL=postgresql://localhost:5432/voteapp_user_districts_test \
npm run test -- tests/pipeline/users/userDistrictInitializer.integration.test.ts
```

The authenticated proxy boundary is covered by an opt-in real HTTP E2E test. It starts a local API server plus a mock auth proxy, verifies the proxy strips client-supplied user IDs, and verifies only the proxy-injected user ID reaches `POST /api/me/districts/initialize` and `GET /api/me/ballot`:

```bash
npm run test:e2e:proxy
```
