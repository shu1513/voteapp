# California batch-06 — the one-chamber tail begins, on a refreshed dataset

**5 roll calls / 5 measures / 311 records.** Imported to local `voteapp` 2026-08-31. Prod untouched.

First batch drawn from the **one-chamber** pool: measures whose divided, enacted vote happened in
only one chamber. All five here are Assembly votes, which is deliberate — an Assembly roll reaches
~68 of our candidates, a Senate roll ~11.

## The refreshed dataset arrived, and it does not yet deliver

The standing plan was to wait for a refreshed LegiScan cut before working the pending seam. **The
refresh landed on 2026-08-30** (hash `c150cc01b198`, replacing `188f99053c12`) and was downloaded to
`/Users/shu/legiscan-data/ca-2172-0830/`. Measured against the old cut:

| | 08-23 cut | 08-30 cut |
| --- | --- | --- |
| roll calls | 19,942 | **21,158** (+1,216) |
| divided floor votes (kept types) | 972 | **1,153** (+181) |
| divided **and enacted** | 441 rolls / 227 measures | **445 / 229** (+4 rolls, +2 measures) |
| bills enrolled, awaiting the governor (status 3) | 232 | **728** |

So the session's final week is now in the data, but those bills are **enrolled, not signed**: status
3 nearly tripled while status 4 rose only 72. **The trigger for the big conversion is signing, not
the dataset refresh** — a distinction the earlier notes got wrong. Signing runs into the autumn;
re-measure then, and the check is the status-3 count falling, not the `dataset_hash` changing.

Batch-06 therefore works the one-chamber tail, which was available all along.

## What came through

| measure | area | yea | chamber | tally |
| --- | --- | --- | --- | --- |
| AB 263 Scott and Shasta river flows | environment_and_public_health | for | Assembly | 58-20 |
| SB 25 antitrust premerger notice to the AG | corporate_accountability | for | Assembly | 52-17 |
| AB 1084 gender-conforming name changes | civil_rights | for | Assembly | 58-16 |
| AB 435 child seat-belt readiness test | environment_and_public_health | for | Assembly | 49-13 |
| SB 709 self-storage price disclosure | corporate_accountability | for | Assembly | 57-17 |

## Dropped, and why

- **AB 863** (mandatory multilingual eviction summons) and **AB 483** (early termination fees) —
  both have exactly one divided vote, and both **predate the bill's last amendment** (AB 863 voted
  06-03, amended 08-18; AB 483 voted 04-07, amended 09-03). Under the SB 707 / SB 22 precedent a
  pre-amendment roll is dropped rather than caveated; for a one-chamber measure that drops the
  measure. Both are real losses — AB 483's 30% cap on early-termination fees is good voter material —
  but no roll in this dataset records a vote on the text that became law.
- **AB 1466** (groundwater adjudication procedure) — court-procedure mechanics, no defensible
  direction.
- **AB 154** — a Committee on Budget bill; the budget precedent, which its title does not reveal
  (the AB 138 lesson from batch-05).
- **SB 352, SB 744, SB 840, SB 830, AB 150, AB 136, AB 181, SB 499** — held for a later batch, not
  read in full here.

## Checks, run in the right order this time

- **Completeness audit BEFORE the import, not after.** Every change each bill's Legislative
  Counsel's Digest enumerates was extracted **untruncated**, then checked off against the drafted
  description one item at a time: 24 items across 5 measures, all covered. This is the batch-05
  lesson applied as a pre-flight rather than a post-mortem.
- **Version check**: the 5 kept picks all postdate their bill's last amendment; the 2 that failed
  were dropped (above).
- **Duplicate-date screen**: no pick has a twin.
- **Plain English from the first draft**, American-spelled: 0 lint warnings across 10 descriptions.
- Every label states `"nay": null` on purpose.

## Left for later

**83 one-chamber measures** remain actionable (17 Assembly-only minus the 5 judged and 4 dropped
here, plus 71 Senate-only). The Assembly-only ones are worth ~68 records each and should go first;
the Senate-only ones ~11 each. Plus the pending seam: **728 bills enrolled awaiting signature**, to
be re-measured once signing is done.
