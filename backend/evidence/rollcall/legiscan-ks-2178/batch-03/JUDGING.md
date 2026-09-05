# Kansas batch-03 — ten measures read, two kept

## Result

Ten of the 64 measures left after batch-02 were read in full. **Two were kept and eight were
dropped.** 143 records, 143 area tags, 0 errors. 54 measures remain unread.

A two-in-ten yield is low, and it is the honest result. Kansas passes a large number of acts
that either bundle unrelated subjects or move both ways inside one policy area, and the campaign
rule is to drop those rather than file them under the nearest label.

| measure | area | direction | vote |
| --- | --- | --- | --- |
| HB 2635 pregnancy center autonomy act | womens_reproductive_rights | **yea = against**, nay = for | 87-35, veto override |
| SB 137 forfeited firearms to licensed dealers | gun_control | **yea = against**, nay = for | 90-28 |

Both carry a stance on **both sides**, which is why 143 records produce 143 tags rather than
fewer. These are the first Kansas measures in this campaign where a nay vote is itself evidence
of a position.

## Why these two directions are recorded this way

**HB 2635** bars state and local government from making a nonprofit pregnancy center perform,
provide, refer for or counsel toward abortion, from blocking its non-abortion services and
material aid, and from interfering with mission-based hiring. It creates a private lawsuit with
attorney fees. It creates **no duty on the centers, provides no funding, and changes nothing
about who may obtain an abortion** — the description says all three. A yes vote is a position
against expanding what those centers must do in the abortion context, so `yea` is recorded as
`against` and `nay` as `for`.

**SB 137** adds a fifth option for a forfeited firearm: sale or transfer to a licensed federal
firearms dealer. Destruction, agency use, trade to another agency and transfer to the state
bureau of investigation all remain, and the choice still rests with the seizing agency alone.
The practical effect is that forfeited guns may re-enter commerce, so `yea` is `against` under
`gun_control`.

## The eight dropped, and the pattern in them

Three failed for **want of a research area or a determinable direction**: SB 6 (ranked-choice
voting ban — `election_integrity` in this campaign means secure, accurate and auditable
elections, per the Alaska AB 123 precedent), SB 361 (Kansas electing into a *federal* tax credit
that costs the state nothing), and HB 2539 (library board selection).

Three moved **both ways inside one area**: SB 5 (bars federal election-administration money but
exempts election security), SB 114 (opens activities to nonpublic students while blocking those
who withdraw mid-year), and HB 2520 (raises the home plus cap from 12 to 16 while adding a
written-plan duty, with no staffing ratio either way).

Two were **bundles**: HB 2464 and SB 98.

## A third kind of title failure

Batch-02 recorded two: a title naming a different bill (HB 2183) and a title naming a minor
section while the act does something else entirely (HB 2372). This batch found a third, and it
matters because **the mechanical title check missed it**.

**HB 2464's worklist title says "tax credits… for contributions to graduate medical
education."** The enrolled act extends the angel investor credit, the Eisenhower Foundation
credit, the Friends of Cedar Crest Association credit, and the aerospace and aviation education
credits. Nothing in it concerns medical education. The title check passed it because both
strings share the words "extending" and "credits."

**The check is a first filter, never a substitute for reading the act.** Its results are cached
at `/Users/shu/legiscan-data/ks-work/titlecheck.json` and should be treated as a way to find
obvious swaps, not as clearance.

## Checks

| check | result |
| --- | --- |
| Plain-language lint | 4 descriptions, **0 warnings** |
| Reading-level and style checker | **0 problems** |
| Flesch-Kincaid grade | median **7.7**, worst **7.8** |
| Longest sentence | 25 words |
| Stated tallies against the stored vote row | **2 of 2 match** |

## Reconciliation

Predicted independently before touching the database: **143 records and 143 area tags**.
Dry run 143 insert, real run 143 insert with 0 errors, database 143 and 143, re-run all 143
unchanged. The dry-run stamp `2026-09-05T04:14:18.139Z` matched zero rows; the real stamp is
`2026-09-05T04:14:18.944Z`.

Kansas now holds **1,535 records across 21 rolls and 75 candidates**.

## What is left

**54 measures, none read.** All 76 enrolled PDFs are cached at
`/Users/shu/legiscan-data/ks-2178-docs/`, so a later session can start reading immediately.
