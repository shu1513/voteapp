# Pick district gate

Status: PR 1 (backend endpoint, #774) merged. PR 2 (frontend gate) implemented. PR 3 (tests) planned (2026-08-19).

## Problem

"Make my pick" renders for **everyone on every upcoming office race** — the only
gate is guest-or-logged-in ([CandidatePage.tsx](../../frontend/src/pages/CandidatePage.tsx)
`pickableElections`, [ElectionPage.tsx](../../frontend/src/pages/ElectionPage.tsx)
`showChoiceControls`). A visitor arriving from candidate search, the follows
list, or a shared candidate/election link can pick a race they cannot vote in.
Guest junk picks are worse: the localStorage draft flushes into the account at
signup (`flushBallotDraftToAccount`), so the junk becomes permanent. The
backend `PUT /api/me/election-choices` accepts any election id — no district
check anywhere.

Picking is meant to be a ballot-journey action ("add to cart" for MY ballot),
not a page-level action.

## Decided UX (with Shu, 2026-08-19)

Three states, per race, same rule on the candidate page and the election page:

1. **Race district ∈ my districts** → pick controls render, unchanged.
2. **My districts known, race not mine** → render **nothing**. No explanatory
   line, no link. Clean read-only page.
3. **My districts unknown** (guest who never looked up a ballot; logged-in
   user with no saved address; unverified account) → no pick controls, but a
   small conversion nudge in their place: *"Enter your address to see if this
   race is on your ballot"* linking to the address flow (`/` home form). This
   is the shared-link → new-user conversion moment; keep it.

**Safety valve (overrides all states):** if the viewer already has a decided
choice row for that election (account choice or draft), the controls render
regardless of the district check. An imperfect geocode must never lock someone
out of seeing/changing an existing pick. This also keeps My Picks / Draft /
saved-ballot pages fully functional with zero changes.

**Follow button: untouched.** Following anyone is fine; picking is not.

**No server-side enforcement.** A district gate on the PUT would break the
guest draft→account merge at signup (runs before an address exists) and
unverified users (can't read `/api/me/ballot`). Picks are private and
harmless; the client gate is the product fix.

## Where "my districts" comes from

| Viewer | Source | Notes |
| --- | --- | --- |
| Guest | `draft.district_ids` ([ballotDraft.ts](../../frontend/src/lib/ballotDraft.ts)) | Set by `setDraftBallotContext` on `/ballot?d=…` visit. Empty array = unknown (a real ballot always has ≥1 district). |
| Logged-in | new `GET /api/me/districts` | Thin wrapper over existing `listUserDistrictIds` (see `lookupAuthenticatedBallotSummaries` in [runAddressApiServer.ts](../../backend/src/scripts/runAddressApiServer.ts)). Returns `{ district_ids: string[] }`. Empty = no saved address = unknown. |

Why a new endpoint instead of reusing `GET /api/me/ballot`: the ballot summary
is heavy (full election list + previews) and every candidate/election page
would fetch it. District ids alone are cheap and cacheable.

Auth posture for the new endpoint: same verified-email gate as
`/api/me/ballot` (`requireVerifiedAuthenticatedUser`) — district ids are
personal location data. An unverified user therefore lands in state 3
(unknown → address nudge), which is acceptable: every address surface already
demands verification.

Race side: `CandidateElection.district.id` (candidate page) and
`data.district.id` (election page) already exist in the API payloads. No
backend read changes.

## Implementation steps

### PR 1 — backend endpoint

1. `ME_DISTRICTS_PATH = "/api/me/districts"` in
   [apiValidation.ts](../../backend/src/api/apiValidation.ts); GET only;
   verified gate; option hook `listAuthenticatedDistrictIds(userId)` in
   `addressApiTypes.ts`; wire in `runAddressApiServer.ts` via
   `listUserDistrictIds(pool, userId)`.
2. Response: `{ district_ids: string[] }`.
3. apiServer tests: 401 anon, 403 unverified, 200 empty (no address), 200 with
   ids, 405 non-GET.

### PR 2 — frontend gate

1. `frontend/src/lib/useMyDistricts.ts` (frontend, not `packages/api-client`:
   the guest half reads localStorage via `ballotDraft.ts`, which the shared
   package cannot depend on): `useMyDistricts()` hook — logged-in: query
   `["me", "districts"]` (staleTime ~60s, disabled while `me` undefined,
   treat 403 as unknown, invalidate on address save — `SavedAddressForm`
   already navigates after `PUT /api/me/address`, add invalidation there);
   guest: read `draft.district_ids`. Return
   `{ districtIds: Set<string> | undefined }` where `undefined` = unknown
   (loading, unverified, error, or no address/lookup). Loading follows the
   existing **no-flash rule**: render no pick UI until settled, like the
   follow button.
2. **CandidatePage**: `pickableElections = officeCandidacies.filter(e =>
   districtIds?.has(e.district.id) || isDecidedChoice(choiceForElection(e.election_id)))`.
   - State 3 (districts `undefined` after settle): render the address-nudge
     card where the sticky pick card / pick rows would go. Reuse sticky-card
     placement for the single-race case so the page's primary action slot
     stays consistent; plain inline nudge for multi-race pages.
   - State 2: those races simply drop out of `pickableElections`; nothing
     renders (the candidacy rows themselves stay — only the pick buttons go).
3. **ElectionPage**: `showChoiceControls` additionally requires
   `districtIds?.has(data.district.id) || isDecidedChoice(myChoice)`; state 3
   renders the same nudge near where the controls would sit.
4. Nudge component shared between both pages: one sentence + link to `/`
   (home address form). No dismissal state, no persistence.

### PR 3 — tests

- CandidatePage: button shows (race mine), hides (race not mine, known
  districts), nudge shows (unknown), safety valve (existing pick in foreign
  race keeps controls), guest variants via seeded draft.
- ElectionPage: same four.
- RTL gotcha: router-link walks leave the old page mounted — await an
  **arrival-only** element, never a shared nav element (see
  voteapp-rtl-router-walk-flake).

## Out of scope / follow-ups

- Mobile app: no election-choice UI found in `mobile/src` today; if picks land
  there later, apply the same gate.
- Server-side PUT validation: deliberately skipped (see above).
- Cleanup of junk picks already saved by real users: separate one-off audit if
  we care; they're private, so low priority.

## Risks

- District-id drift after redistricting: saved user districts vs. election
  district ids must stay the same id space — they already do (both come from
  the resolver), and the safety valve catches stragglers.
- Extra query per candidate/election page for logged-in users: one cheap
  indexed lookup, cached 60s client-side.
