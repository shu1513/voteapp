# South Carolina roll-call votes — 126th General Assembly (LegiScan session 2194)

Source: the LegiScan bulk dataset for South Carolina's 126th General Assembly, the 2025-2026
regular session. Both years sit in one dataset. The copy this campaign used was cut on
2026-08-30 and holds 4,032 bills, 2,054 roll calls and 185 people.

## Layout

- `crosswalk.json` — the reviewed map from a LegiScan `people_id` to one of our candidates, or
  to nothing. 175 entries: 116 mapped, 59 deliberately left unmapped.
- `legiscan-people-sc-2194.json` — the people snapshot the importer reads, so a later run needs
  only committed files.
- `survey/fetch-survey-report.json` — the measured description histogram the state config was
  written from.
- `survey/fetch-report-summary.json` — the header of the fetch run that stored the votes. The
  2,006 per-roll rows are large and live outside the repo.
- `survey/divided-enacted-worklist.tsv` — every divided roll on a measure that became law, each
  with a disposition and a reason.
- `batch-01/` — the first batch: judgments, the roll evidence files, and the import ledgers.

## What the survey established

**The two chambers name their final vote differently.** The House prints `House: Passage Of Bill`
(377 rolls) and `House: Passage Of Joint Resolution` (18). The Senate's substantive vote is
**second** reading (`Senate: 2nd Reading`, 294 rolls); it also records a third reading (59). Both
Senate readings are kept as passage, and the judge's superseded-stage gate picks whichever came
last.

**The budget is voted section by section, and all of it is excluded.** The House votes each part
of the appropriations act on its own (`House: Adopt Section 5, Part 1B`, 211 rolls, and
`House: Passage Of Section 33, Part 1A`, 198) and the Senate votes each agency's section
(`Senate: To Adopt Section 22 - Corrections, Department Of`). None of those is a vote on the
measure.

**Nothing is left unclassified.** Every roll call in the session matches a kept or an excluded
pattern, and nothing is surfaced for review.

**Feed health is the cleanest tier**: 0 repeated roll call ids, 0 identity duplicates, 0
summary-only rolls, 0 tally mismatches, 0 committee votes. The one exception is a single Senate
second reading that failed 0-8 with only eight senators recorded, which the chamber-size cut
rejects.

South Carolina proposes constitutional amendments as joint resolutions, a type the pipeline
already keeps, so Georgia's resolution gap does not recur here.

## The fact that shapes this campaign

**The South Carolina Senate is not on the November 2026 ballot.** Senators serve four-year terms
and were last elected in 2024, so every Senate roll matches zero candidates and would write zero
records. House rolls carry the whole batch. All 698 Senate rolls in the crosswalk validation
returned zero matches, which is the expected result, not a gap.

House fan-out is a median of 101 matched candidates per roll, with a maximum of 113.

## Pool

902 kept floor votes (507 House, 395 Senate) → 70 divided → **37 divided and enacted on 21
measures** (23 House rolls, 14 Senate). Fifteen of the 21 measures have at least one House roll.

The standard divided gate (the losing side at least a quarter of the winning side) is well
calibrated for South Carolina and was kept. The House minority holds 37 of 128 seats in the
people file (29 percent) and the Senate minority 12 of 47 (26 percent), both above the threshold,
so a straight party-line vote still counts as divided. Kentucky's recalibration is not needed.

## Judging sources

**The bill page is the whole toolkit**: `https://www.scstatehouse.gov/sess126_2025-2026/bills/<n>.htm`
serves, on one page and to a plain request, the act number, the sponsors, the full action history
with each roll's tally, links to every printed version, and **the complete ratified act text**.
There is no legislative-analysis office writing a neutral summary the way Ohio, Maryland or
Connecticut do, and there is no sponsor statement of intent either. The act text is the source.

**The version check is exact and free.** Each printed version at
`.../prever/<bill>_<yyyymmdd>.htm` marks its changes in the page itself: text the bill deletes
carries the CSS class `scstrike` and text it adds carries `scinsert`. No rendering or image
reading is needed, unlike the struck-text hazards in Georgia, Maine, Montana and Kentucky.
`sc_docs.py` renders those two classes as `[[DEL: …]]` and `{{ADD: …}}`.

Two traps in that markup, both hit during this batch:

1. **The class attribute is sometimes unquoted** (`<span class=scstrike>`), and the spans nest.
   A regular expression looking for `class="scstrike"` silently finds nothing, and a plain-text
   read then shows repealed law as if it were live. The reader walks the spans instead.
2. **The printed pages split a section's first letter into its own word** ("S ection", "B e it
   enacted"), so a search for the enacting clause has to run on text with the spacing removed.

## Hazard: the House's `Passage Of Bill` covers two different readings

South Carolina's own history calls the House roll on S 171 and S 214 `Read second time`, while it
calls the roll on S 287 `Read third time and sent to Senate`. LegiScan prints the same
`House: Passage Of Bill` for all three. In each case the third reading, when it came, was given by
unanimous consent with no roll call, so the recorded roll is the chamber's only recorded vote on
the bill. Descriptions therefore say the House "passed it" and never name a reading.

## Helper scripts (outside the repo, so they survive a session)

- `/Users/shu/legiscan-data/sc_pool.py` — mirrors the pipeline's classification and measures the
  pool
- `/Users/shu/legiscan-data/sc_rolls.py` — every kept floor roll of a measure, in time order
- `/Users/shu/legiscan-data/sc_docs.py` — bill page and version reader, with change marking
- `/Users/shu/legiscan-data/sc_vercheck.py` — compares a printed version with the ratified act
- `/Users/shu/legiscan-data/sc_crosswalk.py` — builds the crosswalk from proposals plus review
- `/Users/shu/legiscan-data/sc-2194-STATE.md` — run state and where to resume
