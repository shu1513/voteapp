# North Carolina findings, recorded and not fixed

## 1. LegiScan drops unaffiliated members from the 2026 House roll calls

On the three House veto-override rolls of 2026-06-24 (LegiScan rolls 1711513,
1711515 and 1711527) LegiScan lists 118 members and reports the vote as 71-46.
The official House roll-call transcripts for the same votes (RCS 738, 740 and
736) record 71-47, with 1 not voting and 1 excused absence, and they name the
two members LegiScan leaves out:

- Nasif Majeed, House District 99, listed by the House under "Noes
  (Unaffiliated)".
- Carla Cunningham, House District 106, listed under "Excused Absence
  (Unaffiliated)".

Both members were in LegiScan's lists for the same chamber in July 2025, when
every roll matched the official record exactly. Both changed their registration
between the two sittings, so the missing rows follow the party field, not the
date.

Effect on this campaign: no member is recorded on the wrong side, and neither of
the two is a mapped candidate, so no record was written about the wrong person.
The problem is the printed tally. The approval gate requires the record text to
quote the stored tally, so importing these rolls would have told about ninety
readers that the House voted 71-46 when North Carolina's own record says 71-47.

What was done: the three rolls were imported, then withdrawn the same day. Their
312 records were retired with a reason naming this finding, the rolls were
returned to the review queue as pending, and their evidence files were moved to
`batch-01/held-rolls/`. They can be imported once the pipeline can cite an
official tally that differs from the feed, in the same shape as the
`official_vote_date` override that Illinois needed for dates.

Checked against the official record: 11 of the 14 batch-01 rolls match exactly.

## 2. The House prints a materiality ruling in front of the question

Twenty-two House rolls carry a `R2 Ruled Mat&#x27;l` or `R3 Ruled Mat&#x27;l`
prefix, which records that the presiding officer ruled the matter material under
a House rule. The vote is still on the concurrence or the conference report that
follows the prefix. LegiScan leaves the apostrophe HTML-escaped, so the config
patterns match the escape rather than a normal apostrophe.

---

# Finding 3: the omission is systematic, not three rolls (batch-02)

Finding 1 was written from three rolls. Measured across all 1,452 fetched North
Carolina rolls, 248 have a member list smaller than the chamber, and the pattern
is not random.

**House.** Every 2026 House roll lists 117 or 118 members, never 120. Comparing
a 2025-07-29 roll against a 2026-06-30 roll, six members are gone and four
replacements appear: HD-047, HD-060, HD-090 and HD-119 each show a successor.
The two with no replacement listed are HD-099 Nasif Majeed and HD-106 Carla
Cunningham, the members from finding 1. Both were still serving. Rolls showing
117 are the same six dropped with only three successors seated yet, which is
legitimate.

**Senate.** Senate rolls in May 2026 list 49. Graig Meyer and Paul Newton show
successors; Terence Everitt of SD-018 does not. By late June 2026 the Senate
list is back to 50.

## What it costs

A short member list is not proof of a wrong tally. It is wrong only when a
dropped member actually voted. Checked against ncleg.gov transcripts:

| Bill  | Chamber, date     | Feed  | Official |
|-------|-------------------|-------|----------|
| H1089 | House, 2026-05-20 | 71-46 | 73-46    |
| S1080 | House, 2026-05-20 | 71-46 | 73-46    |
| S474  | House, 2026-07-01 | 72-40 | 74-40    |
| H834  | House, 2026-08-04 | 73-35 | 74-35    |
| S445  | House, 2026-08-04 | 81-28 | 84-26    |
| H376  | House, 2026-06-24 | 77-39 | 77-40    |
| S889  | House, 2026-06-10 | 70-41 | 69-43    |

Seven for seven wrong, on top of the three held in batch-01. Every 2025 roll
checked, and every 2026 Senate roll checked, matches.

## The rule this gives

No 2026 House roll may be imported. The judge's tally gate requires each
description to quote the stored tally, and the stored tally for these rolls is a
number North Carolina's own record contradicts. 17 such rolls are marked
`held:2026-house-tally-understated-by-legiscan` in the worklist, on top of the
three held in batch-01. They become importable when the feed is corrected or the
stored tallies are repaired from the transcripts.

2025 rolls and 2026 Senate rolls are usable, with the tally still checked
one by one against the official transcript before import.
