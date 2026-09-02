# Montana roll-call import — LegiScan session 2159 (2025 Regular Session)

Montana's legislature meets only in odd years, so the 2025 Regular Session is
the whole dataset available to the November 2026 campaign. It convened on
January 6 2025 and adjourned sine die on April 30 2025, so nothing in it can
change.

## Dataset

LegiScan session **2159**, dataset dated 2025-12-07, hash
`a29791d2ff1a47ddd2f4c907dea35155`: 1,761 bills, 9,209 roll calls, 151 people.
The ZIP and the extracted tree live outside the repository at
`/Users/shu/legiscan-data/mt-2159{.zip,}`, with the 5,384 roll evidence files at
`/Users/shu/legiscan-data/mt-2159-evidence/`. This directory keeps only the
curated subset, as in every other state.

## What the survey measured

Montana separates floor votes from committee votes more cleanly than any state
already in the registry:

- Every description opens with the chamber in parentheses, `(H) ` or `(S) `.
- Every committee description joins the committee name to its question with a
  double hyphen, and no committee roll reports a total above 23.
- No floor description contains a double hyphen, and every floor roll reports
  the whole chamber (House 100, Senate 50), because Montana counts absent and
  excused members in the total.

So the pipeline's floor-versus-committee tally check rejects every committee
roll on its own, and none of the configured patterns has to name a committee.
The patterns leave nothing for a human to sort: 2,318 of the 9,167 parsed rolls
are kept and 3,250 are excluded by rule, with **zero floor descriptions
unmatched**.

Montana votes each measure twice on the floor. Second reading is the committee
of the whole, where floor amendments are taken; third reading is final passage.
Only third reading is kept, matching the call Texas, California and Missouri
made about their own pre-passage floor stages.

## Fetch

5,384 rows stored (2,306 floor, 3,078 excluded by question, **0 surfaced**),
dates 2025-01-08 to 2025-04-30. The run reconciles exactly:
5,384 stored + 42 parse errors + 235 excluded-measure + 3,483 committee +
65 identity duplicates = 9,209 dataset votes. Feed health is in the cleanest
tier apart from the parse errors described in `CODE-FINDINGS.md`: 0 repeated
roll_call_ids, 0 summary-only rolls, 0 tally mismatches among floor votes.

**902 divided floor votes** (house 449, senate 453); **633 of them are on
measures that became law**, across 373 measures. That is the largest
divided-and-enacted pool of any state in the campaign so far.

## Crosswalk

`crosswalk.json` — 151 entries: **87 mapped** (85 proposed, all accepted after
review, plus 2 hand-added) and 64 explicit nulls, each carrying its reason.
Validation over all 5,384 stored rolls: matched 233,318, unmatched_reviewed
168,420, **no_crosswalk 0, out_of_scope 0**, 0 file errors.

Seats in the crosswalk notes come from **Montana's own roster of the 69th
Legislature** (`https://api.legmt.gov/legislators/v1/legislators`), not from
LegiScan's `district` field — see `CODE-FINDINGS.md` §2 for why.

**Fan-out: house median 75 matched candidates per roll, senate 11.** All 100
House seats are on the 2026 ballot; the Senate is staggered, so only 25 of 50
seats are up and only 11 sitting senators are among those candidates. House
votes carry the value here, as in Texas.

## Judging sources

Montana publishes no neutral prose summary of a bill, so **the enrolled text is
both the ground truth and the primary source**, read top to bottom for every
measure. The state's own document service serves it without a key:

- Versions with posted dates —
  `https://api.legmt.gov/docs/v1/documents/getBillVersions?legislatureOrdinal=69&sessionOrdinal=20251&billType=HB&billNumber=121`
- The file itself — `.../getContent?documentId=<id>` (plain `curl`, extracts
  cleanly with `pdftotext -layout`)
- Fiscal notes and amendments-in-context — `getBillFiscalNotes`,
  `getBillAmendments` on the same query
- Official action trail, tallies and chapter number —
  `https://api.legmt.gov/bills/v1/bills/findBySessionIdAndDraftNumber?sessionId=2&draftNumber=<LC number>`
  (the LC number is the tail of LegiScan's `state_link`)

Version naming: `HB0121_1.pdf` introduced, `_2`/`_3`/`_4` amended,
`_X.pdf` enrolled. Helper scripts used for this batch are kept outside the
repository at `/Users/shu/legiscan-data/mt_doc.py` and `mt_diff.py`.

## Layout

- `crosswalk.json`, `legiscan-people-mt-2159.json`, `crosswalk-proposals-report.json`
- `CODE-FINDINGS.md` — five defects in LegiScan's Montana data, recorded not
  fixed, plus one judging hazard in Montana's own bill drafting
- `survey/` — the survey report, the fetch report,
  `divided-enacted-worklist.tsv` (one row per divided-and-enacted roll with its
  disposition), and `filter-5-drops.md` (why each dropped measure was dropped
  after a full read)
- `batch-01/` through `batch-06/` — each holds `PLAN.md`, `JUDGING.md`,
  `judgments.json`, the roll evidence files, and the import ledgers

## Status

Imported on the local `voteapp` database only. **Production has zero Montana
records.**

| Batch | Measures | Rolls | Records | Areas |
| --- | --- | --- | --- | --- |
| batch-01 | 9 | 18 | 764 | civil rights, immigration, income tax, reproductive rights, education, welfare, guns, elections, housing |
| batch-02 | 7 | 14 | 598 | environment, health care, corporate accountability |
| batch-03 | 2 | 4 | 172 | environment, both directions |
| batch-04 | 8 | 16 | 682 | environment, civil rights, immigration, anti-corruption |
| batch-05 | 7 | 14 | 597 | elections, courts, anti-corruption |
| batch-06 | 4 | 8 | 342 | property tax, civil rights, reproductive rights |
| **total** | **37** | **74** | **3,155** | 15 areas, 87 candidates, 1,827 tags |

533 divided-and-enacted rolls remain unbatched, 5 are held on HB 231 (below)
and 21 are dropped under filter 5;
`survey/divided-enacted-worklist.tsv` carries a disposition for every one, and
`survey/filter-5-drops.md` records why each drop was dropped after a full read.

Still held: **HB 231**, the property tax rewrite, was pulled out of batch-04
when its coordination instructions turned out to have fired. Sections 27, 29 and
31 void most of HB 231 because Senate Bill 542 also passed; the policy became
law as SB 542 instead. The two will be judged together in a later batch. **HB
807** and **SB 218** are vaccine-adjacent and wait on a direction call.
