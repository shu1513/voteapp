# Arizona Campaign Finance

This module syncs, probes, and surfaces Arizona state campaign-finance data from Arizona Secretary of State Spotlight.

It is isolated behind Arizona-specific feature flags and does not run unless explicitly enabled.

## Scope

Supported data in this module:

- Direct candidate committee income rows by Spotlight committee ID.
- Top direct donor occupation categories.
- Contribution-size buckets.
- Independent expenditures that explicitly support or oppose a candidate.
- Outside group/PAC summaries by spender committee ID and support/oppose direction.
- Donor and industry summaries behind outside groups, based on income rows for those spender committees.

Not supported in this module yet:

- Raw bulk export refresh scheduling.
- Guessing ambiguous candidate committee matches.
- Treating every outside group as a super PAC. The app stores these as outside groups/PACs.

If Spotlight returns multiple plausible candidate committees, the live probe reports an ambiguous resolution instead of choosing one.

## Runtime Flow

No-write probe flow:

1. The Spotlight client builds only the Advanced Search URLs/forms needed for income rows and independent expenditures.
2. The live probe resolves a candidate committee conservatively from candidate-name income search, or accepts `--committee-id` for manual live debugging.
3. The probe fetches direct candidate income rows.
4. The probe aggregates positive direct income rows by occupation and contribution-size bucket.
5. The probe fetches supporting and opposing independent expenditures for the candidate.
6. The probe groups outside spending by spender committee ID and support/oppose direction.
7. The probe fetches income rows for those outside groups and classifies organization donors into industries.

Database sync and ballot lookup flow:

1. The due sync scans eligible Arizona candidate-election rows.
2. Missing links are resolved conservatively by candidate name + office + cycle.
3. Active links sync direct receipts, outside groups, and outside group donor/industry breakdowns into `az_candidate_finance_links`, `az_candidate_finance_summaries`, `az_candidate_finance_direct_breakdowns`, `az_candidate_finance_outside_groups`, and `az_candidate_finance_outside_group_breakdowns`.
4. Ballot lookup reads those rows when `ARIZONA_CAMPAIGN_FINANCE_ENABLED=true`.
5. The ballot response uses source `ARIZONA_SOS` and matches the Texas state-finance output shape.

## Data Source

Arizona Secretary of State Spotlight:

```text
https://seethemoney.az.gov/Reporting/Explore
https://seethemoney.az.gov/Reporting/AdvancedSearch/
```

Reference guide:

```text
https://azsos.gov/sites/default/files/2023-10/cfs4_user_guide_2023march.pdf
```

## Feature Flags

```bash
ARIZONA_CAMPAIGN_FINANCE_ENABLED=false
ARIZONA_CAMPAIGN_FINANCE_SYNC_ENABLED=false
```

`ARIZONA_CAMPAIGN_FINANCE_ENABLED` is the master switch. `--force` can bypass the sync subflag for the no-write sync script, but it cannot bypass the master switch.

There is no Arizona raw-data refresh scheduler. Do not add one unless Spotlight proves export-based bulk caching is necessary.

## Commands

Run from `backend/`.

Run a no-write snapshot sync with known committee IDs:

```bash
npm run arizona-candidates:finance:sync -- \
  --candidate-name="Katie Hobbs" \
  --committee-id=201600105 \
  --candidate-filer-id=201600105 \
  --year=2024 \
  --force
```

Run due candidate finance sync manually:

```bash
npm run arizona-candidates:finance:sync-due -- --dry-run --force
```

Trigger a manual Arizona finance sync job:

```bash
npm run arizona-candidates:finance:scheduler:trigger -- --dry-run --force
```

Upsert the recurring Arizona finance sync job:

```bash
npm run arizona-candidates:finance:scheduler:upsert -- --dry-run --force
```

Run the Arizona finance sync scheduler worker:

```bash
npm run arizona-candidates:finance:scheduler:worker
```

Run the live probe with conservative committee resolution:

```bash
npm run arizona-candidates:finance:live-probe -- \
  --candidate-name="Katie Hobbs" \
  --office=Governor \
  --year=2024
```

Run the live probe with an explicit committee ID when name resolution is ambiguous:

```bash
npm run arizona-candidates:finance:live-probe -- \
  --candidate-name="Katie Hobbs" \
  --office=Governor \
  --year=2024 \
  --committee-id=201600105
```

Useful live-probe tuning flags:

```bash
--limit=5
--resolution-limit=100
--income-limit=500
--ie-limit=500
--outside-income-limit=500
--outside-max-groups=10
--min-industry-amount=25000
--timeout-ms=30000
```

## Operational Notes

- The live probe is intentionally no-write. It is for source validation and debugging.
- Spotlight is queried live by the probe. Keep limits low while validating candidates.
- The committee resolver only accepts a single matched committee ID from candidate-name income search. Ambiguous matches are returned in JSON.
- Due sync uses live Spotlight calls. Start with `--dry-run --max-candidates=1` while validating reliability.
- Direct donor summaries intentionally expose occupations and contribution-size buckets, not employers.
- Outside-spending industry evidence is derived from organization donors to outside groups/PACs, not from the outside expenditure row itself.
- Industry classification uses deterministic shared finance-label rules.
- Ballot lookup does not call Spotlight directly. It only reads persisted Arizona finance rows.
