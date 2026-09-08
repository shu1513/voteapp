# North Dakota batches 02, 03 and 04 — judging notes

These three batches were worked in one pass, so the method notes are together. Batch-01's
notes stand on their own.

## Source and the correction that shaped this pass

Every description was written from the act or the print the chamber voted, read from
ndlegis.gov. North Dakota publishes no neutral prose analysis, so the text is the source.

The batch-01 review corrected a false claim in these notes: North Dakota's enrolled act
**does** carry change markup. A created section is underlined throughout; an amended
section shows the deleted words struck and the replacement underlined. `pdftotext` throws
both away, so a plain extract of an amended section shows repealed law as if it were live.

That correction is load-bearing here, because these batches are full of amended sections.
`/Users/shu/legiscan-data/nd_text.py` classifies each drawn rule by where it sits relative
to the baseline and prints deletions as `[[...]]` and additions as `<<...>>`.

**The tool was wrong when this pass started, and using it is what found the bug.** It
misread alternating lines, because the previous line's underline lands near the current
line's top and, measured from the current baseline, looks exactly like a strikethrough. It
was caught by checking it against a section the act creates outright, where every line must
read as an addition and half of them did not. After narrowing the band it was validated
both ways: a wholly new section now reads as all additions, and a known deletion is still
detected.

**SB 2159 is why the render rule exists.** Its title says only "Projects the state energy
research center is permitted to pursue". The flattened text reads `the state energy
research center may not conduct research`, which looks like a prohibition being preserved.
The markup shows `not` struck and an approval clause added, so the act lifts a flat ban on
high-level radioactive waste research. The page was rendered and read before the stance was
written, and it confirmed the tool exactly.

## Version check

Batch-02 rolls are each their chamber's vote on the text that became law; the scope was
built that way. For batches 03 and 04 there is no enrolled act, so each description was
written from the print that chamber voted, selected by date against the bill's text stack.
**HB 1145 is the case that matters**: it was amended before the House voted, moving the
required display from every classroom to a school cafeteria, and the description says
cafeteria.

## Conditional wording, enforced by the builder

Batches 03 and 04 cover measures that never became law, so the builder refuses to write a
body that lacks "would" or that asserts effect — it rejects "became law", "the act" and
"signed it into law" on any measure carrying a not-enacted tail. It was run against a
known-bad input first to prove the check fires.

North Dakota adjourned 2025-05-02, so tails state a completed fact rather than
Pennsylvania's time-stamped hedge, and each names what actually ended the bill: the other
chamber's rejection with its tally, or in SB 2307's case the veto and the failed override.

## Labels

All labels in these three batches take `nay: null`. In every case the realistic objection
runs on a different axis from the scored area — cost, local control, or the reach of
government — rather than being a position on the area itself. Two calls worth stating:

**HB 1496 is `corporate_accountability`, not `housing_affordability`.** A minimum-heat duty
binds landlords as a defined class of business. It changes neither housing supply nor cost,
which is what the Maryland HB 767 retraction put outside that area.

**SB 2159 and SB 2174 both score `environment_and_public_health` against.** One lifts a ban
on nuclear waste research; the other strips counties of power to set water quality, lagoon
and manure-plan rules for feedlots. Both remove a preventive constraint.

## Quality, measured before each import

| batch | lint | Flesch-Kincaid median | worst | longest sentence |
|---|---|---|---|---|
| 02 | 0 warnings over 20 | 8.9 | 10.0 | 38 words |
| 03 | 0 warnings over 12 | 8.3 | 10.7 | 28 words |
| 04 | 0 warnings over 18 | 8.2 | 9.7 | 35 words |

Every batch's first draft measured higher and was rewritten before anything was judged:
batch-02 from 10.1, batch-03 from 11.6, batch-04 from 9.6. The conditional voice the
not-enacted scope requires pushes reading level up, which is why batch-03's first draft was
the worst of the three. Grade 9 or so remains the honest floor for statutory text.

The builder's British-spelling check caught a British spelling of "color" in the batch-03
draft, which is the fourth time in this campaign that check has earned its keep. Note that
naming the caught word in these notes would itself trip the scan, which is the Arizona
lesson about re-reading any sentence that discusses spelling.

## Runs

| batch | judge | import | stamp |
|---|---|---|---|
| 02 | 10 updated | 244 inserts, 28 later retired | `2026-09-08T04:18:56.358Z` |
| 03 | 6 updated | 165 inserts, 28 later retired | `2026-09-08T04:24:22.385Z` |
| 04 | 9 updated | 227 inserts | `2026-09-08T04:27:29.293Z` |

Zero errors, zero notified, zero `related` flags and zero `ambiguous` outcomes throughout.
A convergence run after batch-04 reports all 970 records unchanged. Each batch's dry run
matched its real run exactly, and the proactive superseded check found no later or same-day
peer for any picked roll, so no judgment needed `acknowledge_later_rolls`.

**Batch-03's ledger was lost and is reconstructed, which the file says on its face.** The
run wrote to `import-rerun-report.json` and the batch-04 run overwrote it before it was
copied. `batch-03/import-db-reconciliation.json` is rebuilt by counting that run's stamp in
`candidate_records.origin_run_id`, which is the documented fallback. The lesson is the
campaign's existing one: copy a ledger out immediately after each run.

## Totals

North Dakota now holds **914 live records across 48 candidates and 37 approved rolls**.
Production still holds none.

## Review fixes, 2026-09-08 (#1244)

Eight findings; all eight were checked against the text and all eight were real. Two were
retractions, four were rewrites, two were bookkeeping.

**Retracted — SB 2174 (batch-02) and HB 1391 (batch-03).** Both are written up in their batch
plans. SB 2174's stance rested on provisions that were already law and merely renumbered;
HB 1391 defines "health status" as the right to refuse a vaccine or treatment and belongs to the
class the operator dropped on HB 1454. Recipe: `manual:records:retire --apply` on a manifest
built from the run stamp (56 records), a one-entry judge file with `review_status: pending`
for each roll, then the entry and its evidence file moved out of the batch into `retracted/`.

**Rewritten — 115 records on five rolls.** HB 1225 said reckless endangerment "was a class C
felony"; it was a class A misdemeanor, rising to class C only with extreme indifference, and the
act adds a class B tier for firearms. HB 1497 said the cutoff was September first; it is August
first. HB 1283 described "health plans" generally; chapter 54-52.1 is the public employees'
group insurance program, with a health savings account exception and a trial-and-report
structure. SB 2350 omitted that the exemption lasts three years or until the job ends, with
exceptions for internal investigations and police hiring. Each went judge → real import
(`rewrite` on the same row ids) → convergence (914 unchanged) → a direct comparison of every
live row against the committed files (914 checked, 0 disagree).

**Bookkeeping.** The `study-only` disposition had been applied by a regex that matched any
title naming a study, catching eight bills that also change law (SB 2261, for one, creates a
ten percent prison-industries tax credit and was vetoed). Those eight are now
`screened:title-and-description`, which says what actually happened to them; eleven pure
study bills remain. And the concurrent-resolution census was wrong: the description pattern
matches 15 measures carrying 21 kept rolls, every one a constitutional amendment, not 9. The
nine were the ones that drew a closely divided roll. Config comment and README corrected.

**What the two retractions have in common:** in both, the description was written from the
flattened text plus the title, and the markup or the definitions section would have shown the
problem. That is the rule this campaign keeps re-learning, and it is now stated in the
README's screening note as well.
