# Partial-address scope (ZIP ballot)

Status: planned 2026-08-25, revised same day after review (crosswalk stats
verified against the actual Census file; centroid bug confirmed in code). No
PRs yet. v1 is **ZIP only** — city input is deferred (see "Later research").

## Problem

The home page's only entry point is a full street address
([HomePage.tsx](../../frontend/src/pages/HomePage.tsx)). Some visitors distrust
the field and won't type where they live, even with
`ADDRESS_FIELD_PRIVACY_NOTE` and `FullAddressExplanation` doing their best.
A typed ZIP dies in the Census one-line geocoder with `not_found` → a 422 the
UI renders as "check the street, city, and state".

We can honestly serve a **partial ballot** for a ZIP: statewide races, plus
county races when the ZIP maps to exactly one county — with a banner saying
what's missing and inviting the street address. A conversion teaser instead of
a dead end.

## Correctness rules

1. **Never present a district guessed from a point as the visitor's
   district.** A ZIP/city centroid falls in *some* House / state-leg / school
   district, but the visitor may live in a different one.
2. **Don't claim more certainty than the data has.** ZCTAs are the Census's
   generalized, block-based approximation of ZIP delivery patterns (each
   block gets the ZIP with the most addresses) — not USPS boundaries
   ([methodology](https://www.census.gov/programs-surveys/geography/guidance/geo-areas/zctas.html)).
   So ZIP→county is only offered in the unambiguous case, and the UI always
   labels ZIP results partial.

Scope tiers:

| Input | Districts returned | How resolved |
|---|---|---|
| street address (today) | all 9 types | Census geocode — unchanged |
| ZIP | `statewide`; + `county` only when the ZCTA has **exactly one** county relationship | local ZCTA↔county crosswalk, **no geocoder call** |

Conservative county rule (verified against the file, 2026-08-25):

- Counties span >1 state → **error** ("that ZIP crosses state lines — enter
  your city and state or street address"). 137 ZCTAs nationally; 29 of them
  have a ≥97% dominant county, e.g. `02861` is 99.5% Providence County RI but
  intersects Bristol County MA — a land-share threshold would have quietly
  assigned some Massachusetts residents a Rhode Island ballot. No threshold.
- One state, exactly one county row → statewide + county. **23,605 of 33,791
  ZCTAs (70%)** — the common case gets county races.
- One state, multiple county rows → statewide only. 10,049 ZCTAs; the banner
  explains why county races need the address.
- County FIPS in a territory (72/78/66/69/60; 149 ZCTAs) → the existing
  "unsupported coverage" style error — the districts table covers 50 states
  + DC, so returning the key would just produce an empty ballot.
- ZIP with no ZCTA row (~7k ZIPs, largely unique/PO-box ones without their
  own ZCTA — though 2020 ZCTAs *can* represent PO-box-only ZIPs) → friendly
  error suggesting the street address.

## Why this stays small

The ballot page already takes plain district ids (`/ballot?d=<ids>`), and
`lookupAddressDistricts` ([addressDistrictLookup.ts](../../backend/src/pipeline/address/addressDistrictLookup.ts))
resolves `(district_type, geoid_compact)` keys against `public.districts`. A
ZIP resolution is just a *shorter key list* flowing through the existing pipe.
No ballot-query, ordering, or election-data changes.

## Pre-existing bug PR 1 must fix first

Google autocomplete has **no type restriction**
([googlePlacesAutocomplete.ts](../../backend/src/pipeline/address/googlePlacesAutocomplete.ts)
sends only `includedRegionCodes: ["us"]`), so it already suggests cities and
ZIPs. Selecting one returns a centroid `location`, and the resolver's
coordinate-first path
([addressResolverService.ts](../../backend/src/pipeline/address/addressResolverService.ts))
treats it as an exact point — today a visitor who picks "Austin, TX" gets a
full ballot, including House/state-leg/school races, for whatever district the
centroid happens to sit in. That violates rule 1 *now*, and makes it unsafe to
advertise coarse input before fixing classification.

Fix in the retrieve step (server-side — never trust the client to classify):

- Add `types,postalAddress` to `RETRIEVE_FIELD_MASK` (pass-through only;
  Google ToS forbids persisting).
- Classify server-side: if `types` intersects a region set (`postal_code`,
  `postal_code_prefix`, `locality`, `sublocality*`, `neighborhood`,
  `administrative_area_level_*`, `postal_town`, `country`), the selection is
  coarse — return `location: null` plus `granularity: "zip" | "region"` and,
  for `postal_code`, the 5-digit `postal_code` from `postalAddress`.
  Everything else keeps today's behavior (`granularity: "address"`).
- Old clients (shipped mobile builds) ignore the new fields, get a null
  location, submit the text string, and land on today's 422 — strictly safer
  than the current wrong-ballot behavior, with no forced upgrade.

## PR 1 — backend + data

**`allow_partial`.** `POST /api/address/resolve` body gains optional
`allow_partial: boolean` (default false). The ZIP branch runs **only when
true**. Updated web/mobile clients send it; old clients and the authenticated
saved-address path never do, so:

- [userAddressDistrictUpdater.ts](../../backend/src/pipeline/users/userAddressDistrictUpdater.ts)
  (which feeds saved ballots and notifications through the same resolver)
  can never silently replace a complete saved district set with a coarse one.
  A ZIP typed into the settings address form gets a clear
  "full address required" error instead.
- Shipped mobile builds see no behavior change until they update.

**ZIP detection.** In `resolveAddressToDistricts`, before the geocoder paths
and only under `allow_partial`: input matching `^\s*\d{5}(-\d{4})?\s*$` takes
the ZIP branch (ZIP+4 → first five digits). Zero external calls; skip the
redis cache (the crosswalk lookup is one indexed local query).

**Crosswalk table** (migration 255):

```sql
CREATE TABLE address_zcta_county (
  zcta5        text NOT NULL,
  county_geoid text NOT NULL,   -- 5-digit state+county FIPS
  PRIMARY KEY (zcta5, county_geoid)
);
```

No land-share column — the conservative rule doesn't use one.

**Importer** `npm run import:zcta-county-crosswalk` (new script), source
`https://www2.census.gov/geo/docs/maps-data/data/rel2020/zcta520/tab20_zcta520_county20_natl.txt`
(pipe-delimited; verified 2026-08-25: 47,863 data rows = 46,960 populated
ZCTA↔county relationships + 903 blank-ZCTA rows for county territory with no
ZCTA; UTF-8 BOM before the first header). The importer must:

- strip the BOM and validate the exact header row;
- skip blank-`GEOID_ZCTA5_20` records;
- validate 5-digit ZCTA and county GEOIDs;
- load into a staging table and swap inside one transaction;
- refuse to replace existing data if the parsed row count is implausible
  (guard band around ~47k).

One-time per decade; no scheduler. (If ZIP fidelity ever matters more, HUD's
quarterly USPS crosswalk uses address ratios and is the better source — a
deliberate later upgrade, not v1.)

**Contract.** `PublicAddressResolutionResult`
([addressApiResponses.ts](../../backend/src/api/addressApiResponses.ts)) gains
one field: `scope: "exact" | "zip"`. No `scope_label` — `matched_address`
already carries the display string (the ZIP itself). Internal
`AddressResolutionResult.coordinates` becomes nullable (or the ZIP branch uses
a separate constructor) — the public response never exposed coordinates, so
only internal types move. New failure modes get **distinct error codes** (ZIP
unknown / multi-state / territory) so clients can show accurate copy.

**Tests.** ZIP and ZIP+4 in/out of the regex; single-county, multi-county
same-state, multi-state (incl. a ≥97%-dominant one — must still error),
territory, unknown ZIP; `allow_partial` absent → full-address error;
authenticated updater rejects ZIP input; terms gate enforced on the ZIP
branch; retrieve classification (postal_code, locality, street address,
establishment); exact-address regression.

## PR 2 — web + mobile

Both clients share the endpoint and the matched-address handoff
(`mobile/src/app/(tabs)/index.tsx`, `mobile/src/app/ballot.tsx`), so this PR
covers **web, mobile, and the shared api-client types** together.

- Send `allow_partial: true` from the anonymous home flows; clear any stored
  coarse/granularity state on every keystroke (same rule as coordinates
  today).
- Selected ZIP suggestion (retrieve `granularity: "zip"`) → use the returned
  `postal_code` as the input; selected region (`"region"`) → inline guidance:
  "pick a street address, or enter just a ZIP code for partial results".
- Home copy: "Enter your full address for complete results — or just a ZIP
  code for partial results." Privacy note stays. Update
  [FullAddressExplanation.tsx](../../frontend/src/components/FullAddressExplanation.tsx)
  (and the mobile equivalent) to explain complete vs. partial rather than
  implying only full addresses work.
- Ballot banner when `scope === "zip"`: shown races + CTA, without promising
  specific missing races ("Enter your street address to check for additional
  congressional, legislative, local, and school races."). Scope rides router
  state like `matchedAddress`; additionally append `partial=1` to the ballot
  URL — no location data, but a shared or direct link still renders a generic
  partial notice when router state is absent.
- Error copy: `ErrorNotice`
  ([Status.tsx](../../frontend/src/components/Status.tsx)) currently replaces
  every 422 with street-address copy — branch on the new error codes (or
  render the API message for them).
- **Signup handoff: coarse results are never saved.** Skip
  `savePendingDistrictIds` for `scope !== "exact"` and clear any previously
  stored pending ids (last search wins — a stale exact set must not
  initialize an account the visitor thinks reflects their ZIP search).
  Persisting a partial account correctly would need a completeness flag plus
  banners across saved-ballot/settings/notifications — out of scope for v1;
  registered users get districts via the settings address form.

**Tests.** Banner per scope and via bare `partial=1` link; ZIP submit
navigates with returned ids; handoff skipped and cleared on coarse; mobile
banner; regression: exact flow unchanged.

## Rollout

1. PR 1 merge → migration 255 + importer locally; spot-check `78701` →
   Travis (single-county), `02861` → error, a 10049-class ZIP → statewide
   only.
2. PR 2 merge. No feature flag: invisible until someone types a ZIP with an
   updated client, and the exact path is regression-covered.
3. Prod: apply migration, run importer against prod DB, deploy api + ssr;
   mobile release through the normal Expo channel (old builds stay safe via
   `allow_partial`).

## Later research (explicitly not v1)

- **City input.** Needs *identity* verification, not point filtering: Google
  `locality` ≠ Census incorporated place (mailing-city names extend past
  municipal limits; CDPs aren't governments; a centroid can even fall outside
  a concave/multipart polygon). A future pass would match the selected
  locality+state to a Census place name/GEOID before returning `place` races.
  Until then, region selections get guidance, never results.
- ZCTA↔place crosswalk (city races for a ZIP); labeled multi-county display;
  county races for cities; HUD address-ratio crosswalk; any per-user coarse
  location storage.
