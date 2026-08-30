# Pennsylvania roll-call import — LegiScan session 2192

Phase-4 LegiScan state #7 (after TX, GA, FL, IL, TN, CA). Plan doc:
`docs/plans/roll-call-vote-import.md` §5 phase 4.

Dataset: LegiScan **session 2192**, PA 2025-2026 Regular Session
(`state_id` 38). 4,935 bills / 5,038 roll calls / 260 people, dataset dated
2026-08-23. The session is **still live** (`sine_die 0`), so a later
download will hold more rolls than this run saw.

The extracted dataset and the full 1,624-file evidence set live outside the
repo at `/Users/shu/legiscan-data/pa-2192{,-evidence}/`. This directory keeps
the curated subset: crosswalk, people snapshot, survey, and the batches.

## Feed health

| check | result |
| --- | --- |
| duplicate `roll_call_id`s | 0 (the Texas 9.4% collapse is a verified no-op) |
| summary-only rolls (empty member list) | 0 |
| tally mismatches (`total` != yea+nay+nv+absent) | 0 |
| identity-duplicate rolls | 7 extras, collapsed by the shared identity key |
| parse errors | 0 |

## Desc vocabulary

**Pennsylvania names the venue in every desc.** A floor vote reads
`House Floor: HB 1431 PN 1746, FINAL PASSAGE`; a committee vote reads
`House Judiciary: Report Bill As Committed`. Every one of the 1,660
floor-sized rolls carries the literal `Floor:` and no committee-sized roll
does, so the config anchors on that token (the Tennessee `FLOOR VOTE:`
shape).

Two facts the survey settled that a guess would have missed:

- **The chamber word in the desc is not reliable.** Four Senate rolls are
  captioned `House Floor: PN1030, Concur in House Amendments`. No pattern
  reads the chamber word; the roll's own `chamber` field is the truth.
- **The desc carries the PRINTER'S NUMBER of the text being voted.** That
  makes the version check exact and free — no other phase-4 state gives it.
  `House Floor: PN1226, FINAL PASSAGE` names PN 1226 and nothing else.

Classification of the whole dataset with the committed config, nothing
surfaced:

| bucket | rolls |
| --- | --- |
| kept passage | 811 |
| kept concurrence | 50 |
| excluded (procedural) | 795 |
| committee-sized | 3,134 |
| floor-sized type `R` resolutions, dropped pre-config | 248 |

Fetch stored 1,624 rows (861 floor / 763 excluded-question), rejecting 2,701
committee and 713 excluded-measure votes before the queue. Dates
2025-01-27 .. 2026-07-23.

## The pool is thin, and divided government is why

**291 divided floor votes (house 225 / senate 66), but only 35 of them are
on measures that became law** — 28 measures, house 32 / senate 3. The other
252 divided rolls sit at LegiScan status 2: passed one chamber and died.
That is the signature of a split legislature (Democratic House, Republican
Senate, Democratic governor) — what passes is bipartisan, and what divides
does not pass.

Roughly half the 28 enacted measures are appropriations bills, excluded by
the federal and Georgia precedent that a fund-the-government vote has no
honest research area. **The user chose the standard gate** (divided AND
became law, appropriations excluded) over widening to one-chamber passage.

Every one of the 35 rolls carries a disposition in
`survey/divided-enacted-worklist.tsv`.

## Fan-out

**House median 176 matched candidates per roll (max 180); Senate 14.** The
best house reach of any phase-4 state (GA 149, TX 114, IL 92, FL 50, TN 13),
because Pennsylvania has our best state-legislative roster coverage. One
judged House roll writes ~176 records, so a five-measure batch is still
worth 882.

There are only 3 divided-and-enacted **Senate** rolls in the whole session
and all three fell out of the batch, so batch-01 is house-only by
arithmetic, not by choice.

## Crosswalk

260 entries = **170 proposed** (all accepted) + **24 hand-added** + 66
explicit null. Validation over all 1,624 rolls: matched 185,532,
unmatched_reviewed 39,826, `no_crosswalk` 0, `out_of_scope` 0, 0 file errors.

All 12 `first_prefix` proposals are clean nickname/legal-name pairs whose
seats agree; there are **no `seatAgrees:false` proposals** in Pennsylvania,
and no sitting legislator is running statewide in our pool (the local DB
holds only 9 PA Nov-2026 candidates outside the legislature).

The 24 hand-adds are all one class, and it is a **new** class — see
`CODE-FINDINGS.md` §1. Each was confirmed as a sitting `PA State
Representative` off `candidates.current_office`.

## Judging source

**The bill page at `palegis.us/legislation/bills/<year>/<bill>`** (plain
curl with a browser User-Agent; server-rendered). It gives the Act number,
the full action history with tallies, the dated printer's numbers, the
adopted amendments per PN, and — the important part — a **House
Appropriations fiscal note and a Senate fiscal note for each PN**.

The fiscal notes are the Ohio-LSC / Georgia-HBRO analog: a `SUMMARY` plus a
section-by-section `ANALYSIS`, with **no sponsor statement of intent
anywhere**, so the Texas advocacy-preamble hazard does not recur.

Two Pennsylvania-specific cautions:

- **The fiscal notes are partisan-staffed.** The House note is signed
  "House Appropriations Committee (D)" and the Senate note comes from the
  Republican Senate majority. Two independent analyses of the same printer's
  number is a bias control no other state offers — but neither is neutral by
  construction, so the enacted text is still the ground truth.
- **Bill titles are structurally uninformative.** Pennsylvania titles read
  "further providing for definitions and providing for presumed cost of
  doing business by retailer" and never name the policy. HB 1425's title
  says cigarette licensing; the act creates a vape-product directory. Read
  the text.

Version-check recipe: the printer's number is in the desc. Compare it to the
last PN on the bill page. Equal → the chamber voted the enacted text.
Different → diff the two PN texts at
`palegis.us/legislation/bills/text/HTM/<year>/0/<BILL>/PN<n>`.

## Campaign complete (2026-08-30)

Every one of the session's **291 divided floor votes** now carries a
disposition, and everything judgeable has been judged.

| | rolls |
| --- | --- |
| Divided floor votes in the session | 291 |
| **Judged and imported** | **178** |
| Dropped with a written reason | 109 |
| Superseded by a later vote in the same chamber | 4 |

Batches: **batch-01** 5 measures (divided and enacted), **batch-02** 32,
**batch-03** 112, **batch-04** 29. Local totals: **25,922 records across 194
candidates**.

The 570 kept floor votes that were not divided stay out of scope by the
campaign's gate everywhere — a near-unanimous vote says nothing about how
members differ. The session runs to 30 November 2026 and this dataset is
dated 23 August, so later votes exist that are not in it.

Two measures are still parked on a deliberate user direction call: **HB 1200**
(adult-use cannabis, passed the House 102-101) and **SB 49** (the Cannabis
Control Board, failed the Senate 23-27). No research area carries an honest
direction for legalisation.

Of the 30 divided-and-enacted rolls left pending in
`survey/divided-enacted-worklist.tsv`, 18 are appropriations, excluded on the
user's scope decision and on the federal and Georgia precedent that a
fund-the-government vote has no honest research area.
