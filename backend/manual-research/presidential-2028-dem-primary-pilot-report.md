# 2028 Democratic Presidential Primary Manual Pilot

Run date: 2026-06-30

## Target

- presidential_cycle_id: `54950967-294e-4169-a771-9dca92f58283`
- election_year: `2028`
- stage: `primary`
- party: `Democratic`

## Research Question

Which candidates should be imported for the 2028 Democratic presidential primary under the tightened presidential roster policy?

## Pilot Status

This was a non-production wrapper pilot, not a production-grade empty-roster determination. It proved that the manual presidential roster wrapper can accept an empty contract-shaped payload, run without AI provider calls, and leave the local cycle empty. It did not perform candidate-by-candidate primary-source rejection for every possible FEC filer or speculative 2028 Democratic name.

## Policy Applied

Import only candidates with both:

- presidential FEC candidate ID
- at least one non-FEC source-backed quality signal: official campaign website, public campaign launch, party-recognized candidate page, ballot access, or primary ballot listing

FEC filing alone is not enough.

## Evidence Table

| claim | source_url | source_type | supports_claim | confidence | rejected_alternatives |
|---|---|---|---|---|---|
| This pilot did not identify any candidate to import from the limited public overview pass; this is not sufficient evidence for a production empty roster. | https://en.wikipedia.org/wiki/2028_United_States_presidential_election | secondary election overview | partial | low | A production pass still needs candidate-by-candidate primary-source rejection for omitted names and FEC-only filers. |
| The local VoteApp 2028 Democratic presidential primary cycle exists and has no linked candidates before or after this pilot. | local Postgres `public.presidential_cycles`, `public.presidential_cycle_candidates` | local database | yes | high | Existing local data did not contain candidates to preserve or reconcile. |

## Payload

`manual-research/presidential-2028-dem-primary-roster.json`

```json
{
  "candidates": []
}
```

## Wrapper Runs

Dry-run:

```bash
npm run manual:presidential-roster:write -- --cycle-id 54950967-294e-4169-a771-9dca92f58283 --election-year 2028 --party Democratic --file manual-research/presidential-2028-dem-primary-roster.json --run-id phase7_presidential_2028_dem_primary --dry-run
```

Result:

- ok: true
- candidate_count: 0
- matched_count: 0
- ambiguous_count: 0
- unmatched_count: 0
- emitted_count: 0

Live:

```bash
npm run manual:presidential-roster:write -- --cycle-id 54950967-294e-4169-a771-9dca92f58283 --election-year 2028 --party Democratic --file manual-research/presidential-2028-dem-primary-roster.json --run-id phase7_presidential_2028_dem_primary
```

Result:

- ok: true
- candidate_count: 0
- matched_count: 0
- ambiguous_count: 0
- unmatched_count: 0
- emitted_count: 0

## Verification

DB verification:

```sql
select count(*) as cycle_candidate_count
from public.presidential_cycle_candidates
where cycle_id = '54950967-294e-4169-a771-9dca92f58283';
```

Result:

- cycle_candidate_count: 0

API/read path:

- Public candidate detail exists at `GET /api/candidates/:candidate_id`.
- No presidential-cycle roster API endpoint was found in current code.
- Candidate detail API verification was not applicable because this quality-filtered pilot imported zero candidates.

## Follow-On Passes

- Profile pass: skipped because roster imported zero candidates.
- Record pass: skipped because roster imported zero candidates.
- Low-signal FEC-only candidates: not imported by the empty fixture. This local environment does not have OpenFEC API keys, so this pilot did not enumerate every obscure FEC-only filer or prove that none exist.
- Production follow-up required: before treating this cycle as truly empty, run a roster-quality pass with primary sources/OpenFEC access and document candidate-by-candidate rejection reasons for every omitted filer or public candidate claim.
