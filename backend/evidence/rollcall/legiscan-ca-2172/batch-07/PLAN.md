# California batch-07 — selection

**8 roll calls / 8 measures / 507 records.** Imported to local `voteapp` 2026-08-31. Prod untouched.

First batch under the **final-text rule** and the first from the **one-chamber tail**.

## Why the rule changed

Batches 01-06 imported only bills that **became law**. That filter was wrong for what these
records are for. A legislator's vote is their own act; a veto is the governor's act, weeks later.
Someone who voted to ban a chemical in consumer products has taken that position whether or not
the governor signed the bill.

The rule is now **final text**, not enacted: a roll counts if the chamber voted on the text that
finished the legislative process. That takes in three bill statuses:

| status | meaning | usable |
| --- | --- | --- |
| chaptered | signed, now law | yes |
| enrolled | passed both chambers, on the governor's desk | yes — enrolled text cannot change |
| vetoed | passed both chambers, governor refused | yes |
| engrossed | passed one chamber, still moving | **no** — can still be amended |
| introduced / failed | never finished | no |

`engrossed` stays out for the same reason AB 863 and AB 483 were dropped: a vote on text that can
still change is not a vote on the final measure.

**This also removes the wait.** The plan had been to re-download in the autumn once the governor
signed. That was only necessary because the record was tied to the outcome. With the tie cut, the
110 enrolled measures are usable now.

## Dataset refreshed first

Re-fetched from the 2026-08-30 cut (hash `c150cc01b198`) before selecting, so the tail is measured
against final session data: 21,158 rolls, 6,395 floor votes, **6,454 stored rows** (was 5,328).
No `approved_conflict` — the store refuses to overwrite an approved row, and none was touched.

**One new unclassified roll**, recorded not fixed: roll 1726359 (AB 2524, Senate, 2026-08-26) has
the desc `AB2524 Gipson By Cabaldon` — a bill number and two names, with no question phrase at all.
It is 39-0, so it can never enter a batch. A single malformed desc is not a vocabulary to write a
pattern from; see `../CODE-FINDINGS.md` §3.

## The Assembly-only pool

Assembly rolls are worth about six times Senate rolls here — all 80 Assembly seats are on the
November ballot against 20 of 40 Senate seats — so Assembly-only measures were worked first.

| status | Assembly-only measures | worked | dropped |
| --- | --- | --- | --- |
| chaptered | 20 | 3 | 17 |
| enrolled | 9 | 3 | 6 |
| vetoed | 2 | 2 | 0 |

## What came through

| measure | status | area | yea | Assembly |
| --- | --- | --- | --- | --- |
| SB 352 community air monitoring, five-year minimum | chaptered | environment_and_public_health | for | 55-19 |
| SB 744 freezes recognized college accreditors to 2029 | chaptered | public_education_quality | for | 60-18 |
| SB 840 rewrites cap-and-trade spending; offsets to best science | chaptered | environment_and_public_health | for | 59-15 |
| SB 923 delete-my-data extended to third-party sources | enrolled | data_privacy | for | 49-14 |
| SB 1250 wildlife crossings in Caltrans planning | enrolled | environment_and_public_health | for | 59-15 |
| AB 2247 counseling for young gun-violence survivors (LA pilot) | enrolled | social_programs_and_welfare | for | 60-16 |
| SB 613 methane reduction priority; certified low-methane gas | vetoed | environment_and_public_health | for | 54-14 |
| SB 629 post-wildfire safety areas and wider hazard maps | vetoed | environment_and_public_health | for | 59-18 |

**`public_education_quality` gains its first California coverage** (SB 744), taking the state to
**14 of 27 research areas**.

## The version check did most of the dropping

**Six of the nine enrolled Assembly-only measures failed it**, and the reason is structural rather
than incidental. An Assembly bill's contested Assembly vote is usually its **May floor passage**;
the Senate then amends it over the summer, and the Assembly's concurrence in August is lopsided,
so it never reaches the divided gate. The May vote is therefore a vote on superseded text.

Dropped on that ground: **AB 1603, AB 1608, AB 1798, AB 1847, AB 2230, AB 2374** — PFAS in
pesticides, a high-speed rail inspector general, genetic testing in insurance, wildfire mortgage
forbearance, child daycare, and an AANHPI-serving institution designation. Real losses, and none
recoverable from this session.

**AB 2247 is the exception that proves the shape**: it has two divided Assembly rolls, and the
second (2026-08-26, concurrence) falls after the 08-13 Senate amendment, so that is the one used.

Senate bills behave the opposite way — the Assembly votes them late, after amendment — which is
why seven of the eight picks here are SB.

## Dropped under filter 5 after a full read

- **SB 499 (residential fees and charges)** — two directions in one bill. It *tightens* water and
  sewer connection fees to the reasonable-cost cap, and simultaneously *widens* what a local agency
  may collect before occupancy to include parkland and recreation facilities. The SB 642 / SB 786
  pattern.
- **SB 830 (regional transit measure ballot procedures)** — procedural and single-region: it fixes
  the ballot label for one Bay Area tax measure and sets how counties pick ballot arguments.
- **AB 100, AB 104, AB 108, AB 118, AB 121, AB 123, AB 130, AB 136, AB 149, AB 150, AB 154,
  AB 181** — the March 2025 budget package, all with the identical 53-17 vote on 2025-03-20.
  Budget and trailer bills, excluded as in every prior batch. AB 154 carries a substantive climate
  title in its chaptered form, but the divided vote is on the March budget shell, long before that
  content was amended in — a version-check failure as well as a budget drop.
- **AB 1466 (groundwater adjudication)** — court procedure, no voter-facing stance.
- **AB 863, AB 483** — permanently unavailable, as recorded in batch-06: their only divided votes
  predate the final amendment.

## Checks

- **Version check on all 8 picks**: every vote falls after its bill's last amendment. `rolls.json`
  records both dates per roll.
- **Completeness audit run BEFORE judging** — 49 untruncated digest items across the 8 measures.
- **Duplicate-date screen**: no pick has a twin (`../CODE-FINDINGS.md` §1).
- **Lint before any database write**: 16 descriptions, 0 warnings, longest sentence 41 words,
  no British spellings.
