# Kentucky 2025 session — findings recorded, not fixed

## 1. LegiScan's `desc` does not identify the question

Seven rolls are labelled `House: Veto Override` and none of them is a veto
override; the real overrides are labelled `Third Reading` like any other
passage. The table of what each of the seven actually decided is in `README.md`.

Nothing is fixed in code, because there is nothing in the feed to fix it from —
the correct question exists only in Kentucky's own vote record. The
configuration therefore excludes the label by rule, and the `questionClass`
stored on a kept Kentucky roll is treated as report metadata rather than a fact
about the vote. This is the Florida finding recurring: `exact_question` and the
question class on a Kentucky row are LegiScan's claim, never Kentucky's.

## 2. Two bills carry more than one kept roll on the same chamber and day

`SB 65` in the House and `SB 28` in the Senate. Both are genuine
reconsider-and-revote sequences, not duplicates. Kentucky's own record for SB 65
shows the order plainly: RCS# 331 Final Passage 72-15, RCS# 333 Reconsider
68-12, RCS# 334 Final Passage 75-18. The decisive roll is the last one, and the
importer's superseded-stage gate already refuses to approve an earlier peer
unless it is listed in `acknowledge_later_rolls`.

Recorded here only so a later reader does not mistake the pair for the Texas
duplicate-id defect. No code change is wanted.

## 3. The bill history's chamber label is inverted on override lines

HB 4's House override (79-19) is filed in the history under `S`, and its Senate
override (32-6) under `H`. The roll calls themselves carry the right chamber, so
no pipeline code reads the wrong value today. The hazard is for analysis
scripts: any script that reconciles rolls against the history must match on the
tally, not on the chamber.
