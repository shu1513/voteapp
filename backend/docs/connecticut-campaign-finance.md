# Connecticut Campaign Finance

This module adds campaign-finance summaries for supported Connecticut state candidates using Connecticut SEEC eCRIS data: bulk receipt exports for direct money, and the eCRIS independent-expenditure search for outside spending.

It is isolated behind Connecticut-specific feature flags and does not run unless explicitly enabled.

## Scope

Supported data in this module:

- Direct campaign receipts for matched candidate or exploratory committees.
- Top donor occupation categories.
- Contribution-size buckets.
- Contributor-state buckets.
- Outside spending: support and oppose totals plus the top spending groups, from SEEC Form 40 independent-expenditure lines (see below).

Not supported in this module yet:

- Outside-spending industry summaries (eCRIS exposes no donors behind an independent-expenditure filer on this search).
- Local offices such as mayor, school board, probate judge, or municipal clerk.

## Outside spending rules

Source: the eCRIS "Search Independent Expenditures" page (`SearchingIndependentExpenditure.aspx`), an ASP.NET form. Each result line carries explicit "Supporting Candidates" and "Opposing Candidates" columns with the office named, so stance is never inferred. The page caps a search at 200 rows with no pager, so the client searches by received-date window and splits any full window until every window is under the cap. Only SEEC Form 40 documents are requested. Facts below were verified against the 2026 corpus and filed PDFs on 2026-09-01.

A line counts toward a candidate only when all of these hold:

- It names exactly one candidate on that stance side, and that candidate is this one (same name rules as committee matching: nicknames expand on the VoteApp side only, a contradicting middle initial rejects). Lines naming several candidates carry one amount for all of them, so they are skipped rather than split or duplicated.
- It names exactly one office, and that office is the candidate's office. eCRIS carries no district, so name plus office is the whole identity.
- Its form section is "Expenses Paid by Committee". "Expenses Incurred but Not Paid" lines reappear as paid lines once paid (verified: Impact CT / Landscape Media $500, Section I on the 01/20 report and Section G on the 01/23 report), so counting both would double-count. Reimbursement itemizations are excluded too.
- Its amount is positive and its file year is the election year.

Every remaining line is a distinct expenditure: same-amount lines with the same payee and date are separate Section G entries in the filed PDF, one per candidate. Groups are keyed by normalized committee name because this search exposes no committee id.

Other forms the search can return are excluded, checked against 2024 and 2026: SEEC Form 20 (party and PAC statements) candidate-tagged lines were all town-committee organization expenditures or contributions to committees, and the search row cannot tell a PAC's rare independent expenditure apart from those; SEEC Form 26 (spenders that are not committees) had two rows, both with blank amounts naming several candidates; SEEC Form 22 had none; SEEC Form 8 is registration. Revisit if a general-election check shows Form 22/26 rows with amounts.

Because eCRIS names no district, two candidates with the same name (after nickname expansion) running for the same office in the same year cannot be told apart. The batch sync checks the whole Connecticut roster for that year and leaves outside spending untouched for any such pair, with a warning, rather than guess.

A successful yearly fetch is authoritative: a candidate no line names gets zero totals and no groups, which clears superseded data. When the year has no cached expenditure artifact, the sync leaves stored outside-spending data untouched and only refreshes direct receipts. The ballot response carries an `outside_coverage_note` naming these exclusions.

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

1. Raw-data refresh downloads/caches the eCRIS candidate/exploratory committee receipts CSV for the current election year, then searches eCRIS for the year's SEEC Form 40 independent-expenditure lines and caches them as `<year>_independent_expenditures.json` in the same directory (a failed search is reported in the job result and does not undo the receipts refresh).
2. Candidate profile enrichment links a candidate to an eligible future Connecticut election.
3. The enricher enqueues one deduped Connecticut finance sync batch job for the day.
4. The sync batch scans eligible Connecticut candidate-election links that do not already have active finance links.
5. The auto-linker resolves candidate name + office + district + election year against cached eCRIS receipt rows.
6. Only matched committees get a `ct_candidate_finance_links` row.
7. Due linked candidates sync direct receipt aggregates into Connecticut finance tables, plus outside-spending totals and groups when the year's independent-expenditure artifact is cached.
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

Refresh the year's independent expenditures manually (same flag gate as the raw refresh; the daily raw-data refresh job also does this after the receipts CSV):

```bash
npm run connecticut-candidates:finance:ie:refresh -- --year=2026
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
- The ballot response uses source `CONNECTICUT_ECRIS`. Outside-spending totals are null for candidates never synced with an independent-expenditure artifact; industry arrays stay empty.
