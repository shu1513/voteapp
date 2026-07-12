# New York Campaign Finance

Outside-spending summaries for supported New York state candidates, built on
the official NY Open Data (Socrata) mirrors of NYSBOE campaign finance data.
Full background, verified data facts, and the phase plan live in
[plan-new-york-finance.md](../../plan-new-york-finance.md).

It is isolated behind New-York-specific feature flags and does not run unless
explicitly enabled.

## Scope

Supported:

- Outside support/oppose totals and top groups from Schedule R expense
  allocations filed by registry-verified Independent Expenditure Committees.
- Industries funding those outside groups, from the groups' own cycle-scoped
  itemized receipts (organization donors only).
- Direct-campaign receipts for the linked authorized committee (Phase 2):
  contribution totals, Schedule F disbursement totals, contribution-size
  buckets, contributor-type buckets, and organization donors classified into
  industries. Unitemized lumps count toward totals only.

Not supported:

- Donor occupation breakdowns — NYSBOE never collects occupation/employer, so
  `top_direct_donor_occupations` is permanently empty for New York.
- Cash on hand — the transaction dataset has no opening balances.
- Federal offices (FEC wins), county/local offices, and every NYC city office
  (Mayor, City Council, Public Advocate, NYC Comptroller, Borough President —
  NYC CFB territory).

## Eligible Offices

- Governor
- Lieutenant Governor
- Attorney General
- Comptroller (NYSBOE label "State Comptroller"; bare "Comptroller" in the
  registry is the county office and is excluded)
- State Senator (district required)
- State Lower Chamber Legislator (NYSBOE label "Member of Assembly"; district
  required)

## Data Source

`data.ny.gov` Socrata datasets, updated daily:

- `e9ss-239a` — disclosure transactions (Schedule R allocations, receipts)
- `7x2g-h32p` — filer registry (committee types, candidate rows)

The NYSBOE Public Reporting hosts (`publicreporting.elections.ny.gov`,
`cfapp.elections.ny.gov`) block backend clients behind a Cloudflare challenge
and must never be used. Queries are narrow SODA reads with stable ordering and
bounded paging; the ~18M-row dataset is never bulk-downloaded.

Set `NEW_YORK_SODA_APP_TOKEN` in production (free Socrata app token, sent as
`X-App-Token`) — unauthenticated calls share a small IP-based rate pool.

## Strict Schedule R Rules

An allocation row counts only when all of these hold; anything else is
skipped and counted, never guessed:

1. The filing committee's registry row says Independent Expenditure Committee
   (party committees also file Schedule R and must never appear as outside
   money).
2. `filing_sched_abbrev = 'R'` with explicit `r_support_oppose` S or O.
3. The target candidate name exact-normalizes to the linked candidate.
4. `office_desc` (and district, where the office has one) matches the
   eligible-office mapping, and `election_year_r` matches the election.
5. `filing_trans_id` is unique across the run.
6. `trans_mapping` resolves to exactly one same-filer Schedule F expenditure
   and the allocation does not exceed it. Roughly 10% of real mappings point
   at amended/superseded expenditures; those rows are dropped by design, so a
   modest undercount is expected.

## Committee Linking

The registry has no candidate→committee relationship, so auto-linking
requires (a) exactly one ACTIVE State-level CANDIDATE registry row matching
the candidate for the office/district and (b) exactly one ACTIVE Authorized
Single Candidate Committee whose name contains the candidate's first and last
name. Ambiguity skips. Hand-curated rows with `link_source = 'manual'` are
the escape hatch.

## Runtime Flow

1. Candidate profile enrichment links a candidate to an eligible future New
   York election and enqueues one deduped NY finance sync batch job for the
   day.
2. The batch job auto-links missing committees, then syncs due linked
   candidates: Schedule R outside groups, group funder breakdowns, and
   industry classifications into the `ny_candidate_finance_*` tables.
3. Ballot lookup reads those rows when `NEW_YORK_CAMPAIGN_FINANCE_ENABLED=true`
   (source `NEW_YORK_SODA`), including direct totals, contribution-size
   buckets, and classified direct-donor industries.

The sync window stops after election day plus a one-day grace period,
matching the other state finance modules.

## Feature Flags

```bash
NEW_YORK_CAMPAIGN_FINANCE_ENABLED=false
NEW_YORK_CAMPAIGN_FINANCE_SYNC_ENABLED=false
NEW_YORK_SODA_APP_TOKEN=
```

`NEW_YORK_CAMPAIGN_FINANCE_ENABLED` is the master switch. `--force` can bypass
the sync subflag, but it cannot bypass the master switch.

## Commands

Run from `backend/`.

Connectivity probe (no DB writes; run from the deployed backend before
trusting anything else):

```bash
npm run new-york-candidates:finance:soda-probe
```

Run due candidate finance sync manually:

```bash
npm run new-york-candidates:finance:sync-due
npm run new-york-candidates:finance:dry-run-sample
```

Scheduler:

```bash
npm run new-york-candidates:finance:scheduler:upsert
npm run new-york-candidates:finance:scheduler:worker
npm run new-york-candidates:finance:scheduler:trigger
```
