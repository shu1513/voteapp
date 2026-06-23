# Texas Campaign Finance

This module adds Texas TEC campaign-finance summaries for supported Texas state candidates using the official TEC bulk CSV database.

It is isolated behind Texas-specific feature flags and does not run unless explicitly enabled.

## Scope

Supported data in this module:

- Direct campaign receipts for safely matched candidate or officeholder committees.
- Top direct donor occupation categories.
- Contribution-size buckets.
- Outside-spending support and opposition totals from Texas direct campaign expenditure rows.
- Outside-spending group summaries when a spending committee can be linked safely to the candidate.
- Industry summaries behind outside-spending groups, based on organization donors to those groups.

Not supported in this module:

- Local offices filed outside TEC.
- Loose candidate-name-only outside-spending attribution.
- `ASSIST` committee relationships from `spacs.csv`.
- Individual donors as industry evidence.

If a Texas outside-spending relationship is ambiguous, missing support/oppose direction, or cannot be tied back to safe TEC rows, it is skipped rather than guessed.

## Eligible Offices

The auto-linker only attempts exact/safe committee matching for these app offices:

- Governor
- Lieutenant Governor
- Attorney General
- Comptroller
- Agriculture Commissioner
- Land Commissioner
- Railroad Commissioner
- State Senator
- State Lower Chamber Legislator

The gate is explicit. The module does not run for every `statewide`, `state_upper`, or `state_lower` office.

## Runtime Flow

1. Raw-data refresh downloads/caches the official TEC bulk CSV ZIP.
2. Candidate profile enrichment links a candidate to an eligible future Texas election.
3. The enricher enqueues one deduped Texas finance sync batch job for the day.
4. The sync batch scans eligible Texas candidate-election rows that do not already have active finance links.
5. The auto-linker resolves candidate name + office + district + election year against cached TEC filer rows.
6. Only matched committees get a `tx_candidate_finance_links` row.
7. Due linked candidates sync direct receipts, direct occupations, outside-spending groups, and outside-spending industry evidence into Texas finance tables.
8. Ballot lookup reads those Texas rows when `TEXAS_CAMPAIGN_FINANCE_ENABLED=true`.

The sync window stops after election day plus a one-day grace period, matching the other state finance modules.

## Data Source

Official TEC bulk CSV ZIP:

```text
https://prd.tecprd.ethicsefile.com/public/cf/public/TEC_CF_CSV.zip
```

Reference pages:

- `https://www.ethics.state.tx.us/search/cf/`
- `https://www.ethics.state.tx.us/data/search/cf/CFS-ReadMe.txt`
- `https://www.ethics.state.tx.us/data/search/cf/CFS-Codes.txt`
- `https://www.ethics.state.tx.us/search/cf/SuperPac.html`

## Feature Flags

```bash
TEXAS_CAMPAIGN_FINANCE_ENABLED=false
TEXAS_CAMPAIGN_FINANCE_SYNC_ENABLED=false
TEXAS_TEC_RAW_DATA_REFRESH_ENABLED=false
```

`TEXAS_CAMPAIGN_FINANCE_ENABLED` is the master switch. `--force` can bypass the sync or raw-refresh subflag, but it cannot bypass the master switch.

## Cache

Default cache directory:

```bash
scratch/texas-campaign-finance/tec
```

Override with:

```bash
TEXAS_TEC_CSV_DATABASE_CACHE_DIR=scratch/texas-campaign-finance/tec
```

Candidate syncs read from this cache. They do not download TEC data per candidate.

The refresh job uses TEC response metadata and reuses the cached ZIP when the remote file is unchanged.

## Commands

Run from `backend/`.

Refresh raw TEC data manually:

```bash
npm run texas-candidates:finance:raw:refresh
```

Upsert the raw-data refresh scheduler:

```bash
npm run texas-candidates:finance:raw:scheduler:upsert
npm run texas-candidates:finance:raw:scheduler:worker
```

Trigger a raw-data refresh job:

```bash
npm run texas-candidates:finance:raw:scheduler:trigger
```

Run due candidate finance sync manually:

```bash
npm run texas-candidates:finance:sync-due
```

Optionally classify unknown high-dollar outside-spending organization donors with the shared AI classifier:

```bash
npm run texas-candidates:finance:sync-due -- --ai-classify-industries --ai-min-amount=25000
```

Upsert the due-sync scheduler:

```bash
npm run texas-candidates:finance:scheduler:upsert
npm run texas-candidates:finance:scheduler:worker
```

Trigger a due-sync job:

```bash
npm run texas-candidates:finance:scheduler:trigger
```

```bash
npm run texas-candidates:finance:scheduler:trigger -- --ai-classify-industries --ai-min-amount=25000
```

## Operational Notes

- Enable and run the raw-data refresh before enabling the due-sync scheduler. The due sync depends on the cached TEC ZIP.
- Auto-linking is conservative. Ambiguous or unmatched committee resolution is skipped rather than guessed.
- The raw refresh should normally run before the candidate sync. The default schedulers follow that order: raw refresh at `25 8 * * *`, candidate sync at `10 9 * * *`.
- The ballot response uses source `TEXAS_TEC`.
- Direct donor summaries intentionally expose occupations and contribution-size buckets, not employers.
- Outside-spending industry explanations are generated by backend logic, not AI.
- Industry classification uses deterministic finance-label rules first. Unknown high-dollar organization donors can optionally use the shared AI classifier when sync jobs are run with `--ai-classify-industries`.
