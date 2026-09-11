# Iowa batch-07 — plan (first not-enacted batch)

## Scope
The divided-and-enacted pool closed in batch-06. This batch opens the not-enacted scope: bills
of the 91st General Assembly that did not become law. The General Assembly adjourned for good
on 2026-05-03, so every such bill is dead and each description states a completed fact.

## How the pool was measured
`../survey/not-enacted-worklist.json`. For every bill or joint resolution not at LegiScan
status 4, each chamber's final roll under a kept caption (the same caption families as the
config, plus the Senate joint-resolution caption) was found by date, journal page, and roll id.
The slot was kept only if that final roll was divided (the smaller side at least a quarter of
the larger). Result: 90 slots on 85 measures. All 90 passed their chamber; Iowa does not bring
losing bills to a floor vote. 7 slots are on vetoed bills; the rest died in the other chamber.

## Screening, all 85 measures
Every measure was read against the five filters from the Legislative Services Agency
explanation and then the text the chamber voted on. 25 measures were kept (26 rolls, batches
07-09); 60 were dropped, each with a written reason in the worklist. Three drops are the
"passed elsewhere" class: HF 2336 (student speech, enacted as SF 2231), HF 2716 (SNAP reports
and waivers, enacted as SF 2422), and SF 507 (city and county diversity offices, already
banned by HF 856 in 2025). Saying those "did not become law" would mislead.

## Selected for this batch
HF 2102, HF 2133, HF 269, HF 2487, HF 2488, HF 2513, HF 2529, HF 2557, HF 2584, HF 2624,
HF 2718, HF 546. All are House votes on bills the Senate never took up. Reasoning in
`JUDGING.md`.

## Version check
The text each chamber voted on is its reprint (LGR) when one exists, otherwise the introduced
text (LGI). Floor amendments adopted before the roll were checked against the history; for
HF 2133 the regulation amendment H-8168 lost 44-51, so the introduced text is what passed.

## Audits and import
`ia_build_ne.py` refuses a bill at status 4, a signed bill, prose that is not conditional,
a sentence over 45 words, a British spelling, and a held roll; it derives the fate sentence
from the dataset. `ia_finish_ne.py` checks each roll against Iowa's own journal tally line,
the date, division, and same-chamber text changes after the vote. Audit: none failed.
Lint: 0 warnings over 24 descriptions; longest sentence 43 words.
Judge 12 updated. Import dry run 12 rolls / 943 inserts, real run the same, no errors.
Stamp 2026-09-11T05:33:17.521Z: 943 records, 84 candidates, 670 tags.
Same-day sweep: 12 hand-written records share a date with these votes; each names a different
bill and tally (HF 602, HF 190, HF 2502, HF 2292, HF 2643, HF 2245, a sentencing bill), so none
is a duplicate and nothing was retired.
