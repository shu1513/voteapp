# PA batch-02 — the one-chamber-passage scope

Batch-01 gated on "divided AND became law", which in a split legislature hides
most of what legislators actually voted on. **On the user's direction this batch
opens the 256 divided rolls on measures that did NOT become law** — 193 House,
63 Senate, on 250 distinct measures.

That scope has never been taken in any prior state. The case for it: a recorded
floor vote is a position whether or not the bill survived, and in Pennsylvania
the House passes things the Senate never takes up, so the enacted-only gate was
systematically hiding where House members stand.

## Gate check

Verified from each bill's official page before anything was judged: **not one of
the 250 measures has an Act number or a governor's approval**, and only HB 1042
had both chambers vote. The pool is genuinely "passed one chamber, went no
further".

## This batch: 32 measures / 32 House rolls / 5,642 records / 180 candidates

| area | measures |
| --- | --- |
| healthcare_affordability (for) | HB 1088, HB 1234, HB 1104, HB 1445, HB 404, HB 535, HB 618, HB 755, HB 1828, HB 583, HB 282, HB 1127, HB 1881 |
| civil_rights (for) | HB 1800, HB 2103, HB 350, HB 630, HB 72, HB 1825 |
| gun_control (for) | HB 1593, **HB 1099**, **HB 1859**, **HB 1866** |
| social_programs_and_welfare (for) | HB 200, HB 145, HB 265, HB 1100 |
| womens_reproductive_rights (for) | HB 1140, HB 1957, HB 836 |
| corporate_accountability (for) | HB 111 |
| reduce_wealth_gap (for) | HB 1549 |

**Three of these are votes the measure LOST.** HB 1099 (undetectable firearms),
HB 1859 (extreme risk protection orders) and HB 1866 (machine gun conversion
devices) each failed the House 101-102 on 2025-09-30 and 2025-10-01. A failed
vote is still a recorded position, and a one-vote margin is the most
decision-relevant kind.

Notable content: HB 1549 is the first minimum-wage increase to clear a chamber
since 2007; HB 1800 would repeal the still-standing statutory bar on same-sex
marriage; HB 1957 is the first-session passage of a reproductive-liberty
constitutional amendment; HB 2103 would add sexual orientation and gender
identity to the Human Relations Act.

## Filters

Same five as batch-01, with filter 2 changed from "became law" to "passed at
least one chamber on a divided vote". Filter 5 (a defensible for/against
direction) does the real work and is why 65 of the 250 measures are dropped
outright.

## Disposition of all 256 rolls

`../survey/divided-not-enacted-worklist.tsv` carries one row per roll:

| disposition | rolls |
| --- | --- |
| batch-02 (imported here) | 32 |
| dropped, reason recorded | 65 |
| pending:batch-03 — screened in, analysis not yet verified | 115 |
| pending:needs-detail-read — no fiscal note at the voted printer number | 44 |

The 115 are already triaged to a proposed area and direction; they need their
fiscal-note analysis read and verified before descriptions can be written, which
is the same standard batch-02 met. **They are not a backlog of unknowns — they
are a queue of screened candidates.**

## Dropped outright (65 rolls), by class

- **Ceremonial or trivia** — the official state candy, a scenic byway
  designation, board quorums, oversize-load permits, preamble displays.
- **Local or narrow** — county fee schedules, township tax maximums, one
  district's collective bargaining.
- **Appropriations** — the House version of the General Appropriations Act.
- **School-choice financing** — HB 1500 cyber charter funding and HB 2632
  educational improvement tax credits, dropped on the same precedent as
  PA SB 315, TX SB 2 and GA SB 82.
- **No defensible direction** — economic development tax credits, a data centre
  framework that reads both as consumer protection and as industry
  accommodation, an omnibus Tax Reform Code bill.
- **User direction calls, deliberately held** — **HB 1200** (adult-use cannabis
  legalisation, passed the House 102-101) and **SB 49** (the Senate Cannabis
  Control Board, the only failed Senate vote in the pool, 23-27). No research
  area carries an honest direction for cannabis legalisation, and the Ohio
  HB 116 precedent says a divided vote with no honest direction is a user call,
  not mine. Expect to be asked.
