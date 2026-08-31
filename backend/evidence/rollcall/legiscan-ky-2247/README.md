# Kentucky roll-call import — 2026 Regular Session (LegiScan session 2247)

The 60-day session, adjourned in April 2026. The LegiScan dataset is dated
12 July 2026 and is complete. The 2025 Regular Session is a separate LegiScan
session with its own directory, `legiscan-ky-2179/`; read its README first, as
it carries the sources and the hazards common to both.

1,737 bills, 917 roll calls, 138 people. 917 raw descriptions fold to 19
families, listed in `survey/desc-families.tsv`. As in 2025 there are no
committee votes and every tally is a whole-chamber tally.

## This session's labels are not the 2025 labels

**LegiScan's Kentucky vocabulary flips between sessions, and the difference is
not cosmetic.** In 2026 the label `House: Veto Override` is the dominant House
family at 415 rolls — LegiScan's name for every substantive House floor vote,
passage and concurrence and genuine override alike — while `House: Third
Reading` falls to 29. Applying the 2025 rules here would drop 415 House votes
and keep 29 duplicates.

Checked against Kentucky's own record, all three of these arrive under the
single label `House: Veto Override`:

| Bill | Roll | What Kentucky says |
| --- | --- | --- |
| HB 398 | RCS# 46 | Pass |
| HB 398 | RCS# 373 | Final Passage |
| HB 2 | RCS# 455 | Veto Override |

So each Kentucky session must be surveyed on its own. Never carry a Kentucky
description rule from one session to another.

The Senate, unlike the House, does name its override question in this session:
36 rolls read `Senate: Veto Override`, and 30 of them sit on a bill whose
history records an override.

## Duplicate rolls

The 2026 feed holds **31 duplicate rolls**, which the 2025 feed does not: the
same chamber and sequence number appear under two different roll call ids with
an identical bill, date and tally. Each pair names `House: Veto Override` plus
one of `House: Third Reading` (29 pairs), `House: Adopt HFA 1` (1) or `House:
Co-Sponsor` (1).

The shared identity key includes the description, so it does **not** collapse
them. Excluding the three partner spellings resolves 29 of the 31 by rule. The
remaining two keep the `Veto Override` copy of what is really a floor amendment
and a co-sponsor vote — see `CODE-FINDINGS.md`.

## Ground truth

Same document as 2025, under the `26rs` path:
`https://apps.legislature.ky.gov/record/26rs/<bill>/vote_history.pdf`

## Scope

Under the campaign's Kentucky divided gate (nay votes at least 15 percent of
votes cast — the reasoning is in the 2179 README), this session yields **155
divided-and-enacted rolls on 52 measures**. 30 bills were vetoed and overridden.

## Layout

- `survey/` — the fetch survey report and the folded description histogram.
- `CODE-FINDINGS.md` — defects recorded but deliberately not fixed.
