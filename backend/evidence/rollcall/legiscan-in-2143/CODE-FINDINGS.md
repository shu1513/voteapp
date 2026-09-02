# Indiana code findings

Two defects found while surveying LegiScan session 2143. Both are recorded here and
handled by the recipe rather than by a code change, for the reasons given.

## 1. Ten House rolls carry a blank question, and no pattern can recover it

The description on these ten rolls is the literal string `House -`, with nothing after the
dash. Matching each one to its bill's action history by date and tally shows they are a
mixture of four different questions:

| Question | Rolls |
| --- | --- |
| Third reading (passage) | HB 1275, SB 159, SJR 17, SB 178, HB 1634 |
| House concurred with Senate amendments | HB 1391, HB 1460 |
| Floor amendment that failed | SB 197 |
| Appeal of the chair's ruling | HB 1115, SB 5 |

Because the question genuinely is not in the description, a pattern would have to guess,
and it would guess wrong on half of them. The config therefore leaves the blank
description unmatched, so the classifier returns `unknown_question` and the rolls surface
for a human instead of entering the review queue.

**What that costs:** two of the ten are divided votes we would otherwise keep — SB 178's
74-20 third reading and HB 1460's 59-18 concurrence. The other eight are either not divided
or are on questions the config excludes anyway.

**Why the naive fix is wrong:** classifying a blank description as passage would file two
chair appeals and a failed amendment as votes on the measure. Reaching the two divided
rolls properly means teaching the pipeline to read the bill's action history, which is a
larger change than this campaign needs, and the history is already how a human resolves
them by hand.

## 2. LegiScan's Indiana member lists disagree with the official journal on 30 rolls

Matching every one of the 1,010 rolls to its bill history line gives:

| Tally difference from the official journal | Rolls |
| --- | --- |
| none | 978 |
| one vote | 17 |
| two votes | 11 |
| four votes | 2 |

This was confirmed against Indiana's own roll-call PDF for **HB 1155, Roll Call 83**
(2025-02-04). The official record is Yea 88, Nay 3, Excused 6, Not Voting 3. LegiScan says
89-2 and lists **Rep. Jim Lucas as a yea; the official roll call lists him as a nay.**
Every other member agrees.

A wrong side is much worse than a wrong count: the fan-out would write a record saying a
named legislator voted the opposite of how they actually voted.

**How the recipe handles it.** Every roll selected for a batch has its member list compared
name by name against the official roll-call PDF before it is judged. All six batch-01 rolls, both batch-02 rolls and all
eight batch-03 rolls passed. The worklist in `survey/divided-enacted-worklist.tsv` marks the 12
divided-and-enacted rolls whose LegiScan tally has no exact match in the official history,
so a later batch knows to treat them with extra care rather than stumbling onto them.

**What batch-03 learned: a flagged roll is unusable either way.** Four of the twenty-three
rolls examined for batch-03 disagreed with the journal, and every one of them had already
been flagged in the worklist. Three were wrong-side errors — SJ 21 Roll Call 273 has
Rep. Mayfield as a nay where the journal has a yea, HB 1006 Roll Call 493 has Rep. Baird as
a nay where the journal has him excused, and SB 457 Roll Call 293 has Rep. Pryor as a nay
where the journal has a yea. Those cannot be imported at all.

The fourth looked survivable and is not. On SB 10 Roll Call 340 LegiScan records
Rep. Dvorak as not voting where the journal records a nay. No false record would be written
for him; he would simply get none. But the judge requires each description to cite its own
roll's tally, and LegiScan's tally is 66-25 where Indiana's journal says 66-26. Publishing
the smaller number would misstate the official record, so the roll is unusable too. **A
flagged roll that fails the member-list check is dropped, whichever direction the error
runs.** Both SB 10 and HB 1006 were dropped from batch-03 for this reason.

**Why this is not fixed in code.** The importer's evidence hash pins the LegiScan roll-call
element, which is the right behaviour: the stored evidence must be what the source served.
Correcting a member's side would mean carrying a second, official member list through the
pipeline, and the check is cheap and exact when done per selected roll.

## 3. The crosswalk proposer still cannot read `nickname` or `name`

Indiana reproduces the finding first recorded for Pennsylvania and Connecticut, and it
explains the exact-name misses Maryland left undiagnosed. `proposeLegiscanCrosswalk` reads
only `first_name` and `last_name`, so it misses:

- **Dave Hall, HD-062.** LegiScan's `name` field is `Dave Hall`, byte-for-byte identical to
  our candidate, but `first_name` is `David`, and `Dave` is not a prefix of `David`.
- **Bob Heaton (HD-046), Bob Morris (HD-084), Jim Tomes (SD-049).** LegiScan holds the legal
  first name with the working name in the unread `nickname` field.
- **David Heine, HD-085.** The same failure in reverse: LegiScan holds the short name and
  our roster holds the legal one.
- **Michael J. Aylesworth, HD-011.** A new sub-class — LegiScan misspells the first name as
  `Micheal`, so neither string is a prefix of the other.

The suggested fix is unchanged from the Pennsylvania note: also try `name`, `nickname`, and
the final token of a multi-part `last_name`. It is deliberately not made here because it
would change the proposals every already-committed state's crosswalk was reviewed against.

**A related case that is not a defect.** `John Bartlett, HD-095` was declined correctly.
Indiana has two John Bartletts on the 2026 ballot, John E. Bartlett in House District 33
and John L. Bartlett in House District 95, so the name alone is ambiguous and the
proposer's requirement that a match be unique in both directions is what stopped it. The
district settles the identity. This is the Maryland "two Mark Fishers" trap recurring, and
the uniqueness rule is what prevented it.

## 4. `pdftotext` hides what an Indiana bill adds and deletes; both are recoverable

Indiana prints an amendment in three styles, and says so on the first page of every enrolled
act: existing statute text in roman, additions in bold, and deletions in roman with a rule
struck through. `pdftotext` flattens all three into identical plain text. That is what made
the batch-01 description of SB 289 materially wrong — the words "meet the definition of a
minority" read as operative text when they were struck.

Both remaining styles are recoverable, and `tools/annot.py` now marks both. Additions carry
a bold font name. Deletions are not a font at all — the strike is drawn as a thin horizontal
rule, about 0.7 points tall, laid over the words — so they are found by intersecting those
rules with the word boxes they cross. The tool wraps additions in `<<...>>` and deletions in
`[[...]]`, which makes an Indiana amendment readable without rendering a single page as an
image.

Batch-03 used an earlier version of the tool that could see only the bold, and this document
then said the strike could not be recovered from the file at all. That was wrong: the strike
is vector geometry rather than text, so a text-only extractor misses it, but a library that
also reports page shapes finds it directly. Batch-04 corrected the tool and this note.

## 5. Bill versions should come from the dataset, not from the filename

Bill version PDFs live at
`iga.in.gov/pdf-documents/124/2025/<chamber>/bills/<BILL>/<BILL>.<nn>.<STAGE>.pdf`, and the
`<nn>` sequence number cannot be predicted. Probing for it wastes requests and misses files.
The LegiScan bill JSON already carries a dated `state_link` for every printed version, so the
whole version stack can be listed and fetched with no guessing, and the dates give an exact
mapping from a roll's date to the text that roll voted on. Joint resolutions sit under
`/resolutions/SJ00nn/` rather than `/bills/`, which the dataset links get right and a guessed
filename does not.

One transport detail: iga.in.gov returns its 691-byte JavaScript shell with a `200` status
for every PDF request that does not carry a browser `Accept` and `Referer` header. A size
check alone reads that as a missing file.
