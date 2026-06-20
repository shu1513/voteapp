# Connecticut Campaign Finance

This module adds direct campaign-finance summaries for supported Connecticut state candidates using Connecticut SEEC eCRIS bulk receipt exports.

It is isolated behind Connecticut-specific feature flags and does not run unless explicitly enabled.

## Scope

Supported data in this module:

- Direct campaign receipts for matched candidate or exploratory committees.
- Top donor occupation categories.
- Contribution-size buckets.
- Contributor-state buckets.

Not supported in this module yet:

- Outside-spending industry summaries.
- PAC-to-candidate support/oppose attribution.
- Local offices such as mayor, school board, probate judge, or municipal clerk.

If Connecticut outside-spending data is added later, it should stay in this module and only write fields when support/oppose attribution is explicit and safe.

## Eligible Offices

The auto-linker only attempts exact/safe committee matching for these app offices:

- Governor
- Lieutenant Governor
- Secretary of State
- Attorney General
- Comptroller
- State Treasurer
- State Senator
- State Lower Chamber Legislator

The gate is explicit. The module does not run for every `statewide`, `state_upper`, or `state_lower` office.

## Runtime Flow

1. Raw-data refresh downloads/caches the eCRIS candidate/exploratory committee receipts CSV for the current election year.
2. Candidate profile enrichment links a candidate to an eligible future Connecticut election.
3. The enricher enqueues one deduped Connecticut finance sync batch job for the day.
4. The sync batch scans eligible Connecticut candidate-election links that do not already have active finance links.
5. The auto-linker resolves candidate name + office + district + election year against cached eCRIS receipt rows.
6. Only matched committees get a `ct_candidate_finance_links` row.
7. Due linked candidates sync direct receipt aggregates into Connecticut finance tables.
8. Ballot lookup reads those Connecticut rows when `CONNECTICUT_CAMPAIGN_FINANCE_ENABLED=true`.

The sync window stops after election day plus a one-day grace period, matching the other state finance modules.

## Feature Flags

```bash
CONNECTICUT_CAMPAIGN_FINANCE_ENABLED=false
CONNECTICUT_CAMPAIGN_FINANCE_SYNC_ENABLED=false
CONNECTICUT_ECRIS_RAW_DATA_REFRESH_ENABLED=false
```

`CONNECTICUT_CAMPAIGN_FINANCE_ENABLED` is the master switch. `--force` can bypass the sync or raw-refresh subflag, but it cannot bypass the master switch.

## Cache

Default cache directory:

```bash
scratch/connecticut-campaign-finance/ecris
```

Override with:

```bash
CONNECTICUT_ECRIS_CACHE_DIR=scratch/connecticut-campaign-finance/ecris
```

Candidate syncs read from this cache. They do not download eCRIS data per candidate.

## Commands

Run from `backend/`.

Refresh raw eCRIS receipts manually:

```bash
npm run connecticut-candidates:finance:raw:refresh -- --year=2026
```

Upsert the raw-data refresh scheduler:

```bash
npm run connecticut-candidates:finance:raw:scheduler:upsert
npm run connecticut-candidates:finance:raw:scheduler:worker
```

Trigger a raw-data refresh job:

```bash
npm run connecticut-candidates:finance:raw:scheduler:trigger -- --year=2026
```

Run due candidate finance sync manually:

```bash
npm run connecticut-candidates:finance:sync-due
```

Upsert the due-sync scheduler:

```bash
npm run connecticut-candidates:finance:scheduler:upsert
npm run connecticut-candidates:finance:scheduler:worker
```

Trigger a due-sync job:

```bash
npm run connecticut-candidates:finance:scheduler:trigger
```

## Operational Notes

- Auto-linking is conservative. Ambiguous or unmatched committee resolution is skipped rather than guessed.
- If the raw eCRIS cache is missing, auto-linking is skipped and already-linked candidates can still sync if their receipt data is available.
- The ballot response uses source `CONNECTICUT_ECRIS` and returns empty outside-spending arrays for Connecticut until safe outside-spending attribution is added.
