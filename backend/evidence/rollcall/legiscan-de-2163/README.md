# Delaware roll-call import — LegiScan session 2163

Delaware's 153rd General Assembly (2025-2026). Both years sit in one LegiScan
session. Dataset dated 2026-08-30, hash `9cf41406c648af0c27594feaba0fce50`, still
live (`sine_die 0`): 1,296 bills, 2,044 roll calls, 65 people. Delaware's
`state_id` is 8.

## Layout

```
legiscan-de-2163/
  README.md                        this file
  CODE-FINDINGS.md                 findings recorded, not fixed
  crosswalk.json                   people_id -> candidate id, all 65 reviewed
  legiscan-people-de-2163.json     the people snapshot the crosswalk was built against
  survey/
    survey-report.json             the measured description histogram
    divided-worklist.tsv           all 158 divided rolls, each with a disposition
  batch-01/
    PLAN.md                        what was selected and why
    JUDGING.md                     how each measure was judged, and the checks run
    judgments.json                 the 25 judgments, the decision of record
    ls-de-*.json                   the 25 roll evidence files
    import-dry-run-report.json     the plan
    import-report.json             the insert ledger
    import-rerun-report.json       the convergence run
```

The full fetch (1,278 roll evidence files, 18 MB) and the dataset live outside the
repo at `/Users/shu/legiscan-data/de-2163{,-evidence,-docs,-work}/`, following the
Texas precedent that the repo keeps only the curated subset.

## What the feed looks like

Delaware's feed is the cleanest tier this campaign has seen. There are no committee
votes at all — every recorded roll reports a whole chamber (House 41, or 40 with a
seat vacant; Senate 21, or 19). There are no repeated roll call ids, no roll whose
parts fail to add up to its own total, no parse errors and no file errors. 350 House
rolls carry no member list, because Delaware records a voice vote as a roll with
every count zero, almost always on a concurrent resolution; the fetcher skips those.

Fetch reconciles exactly: 2,044 dataset votes = 1,278 stored floor votes + 416 on
excluded measure types (concurrent resolutions and simple resolutions) + 350
unrecorded, with 0 committee votes, 0 duplicates and 0 surfaced.

## ⚠ The description never names the question

**Every one of the 2,044 rolls reads exactly `House Third Reading` or `Senate Third
Reading`.** Final passage, a vote to adopt an amendment, the first chamber's later
vote on the second chamber's version, and a procedural motion all wear the same
words. Florida and Connecticut each had this defect in one chamber; Delaware has it
in both chambers, on every roll.

So the `questionClass` the pipeline reports for Delaware is the feed's claim, not
Delaware's. **The bill history is the ground truth**, and it spells the question out:

```
Passed By House. Votes: 31 YES 5 NO 5 ABSENT               -> passage
Amendment SA 3 to HB 445 - Passed By Senate. Votes: 21 YES  -> an amendment
Defeated By House. Votes: 15 YES 26 NO. Reason Taken:
  motion to recess to read amendment ...                    -> procedural
```

Matching a roll to its history line on date, chamber, yeas and nays resolves 1,574
of the 1,694 recorded rolls outright, and all but 8 of the 158 divided ones. That
match is a selection-time step, and `survey/divided-worklist.tsv` carries its result
in the `question` column. HB 445 is the worked example: its 2026-07-01 House roll
of 15-26 wears `House Third Reading` and is a motion to recess.

## ⭐ Delaware marks struck and new text in CSS, so the version check is mechanical

Delaware serves bill text as HTML whose classes carry `text-decoration: line-through`
for language a measure deletes and `text-decoration: underline` for language it adds.
Georgia, Maine, Montana and Kentucky all hid that distinction inside a PDF that
`pdftotext` flattens, and each needed a page render to recover it. In Delaware a
parser can read it. `/Users/shu/legiscan-data/de_doc.py` prints a document with
additions as `<<...>>` and deletions as `[[...]]`.

Two more things Delaware gives free:

- **The engrossed print names its own amendments in the header** — `HOUSE BILL NO. 445
  AS AMENDED BY HOUSE AMENDMENT NO. 1 AND SENATE AMENDMENT NO. 3`. That is the version
  check, stated by the document itself.
- **Every bill carries a SYNOPSIS written into the bill.** It is neutral and
  descriptive, with no sponsor statement of intent, so the Texas advocacy hazard does
  not recur. But see CODE-FINDINGS §2: the synopsis belongs to the introduced text and
  is never updated when the bill is amended.

## Which text became law

Delaware publishes only two text types, `Draft` and `Engrossed`, and **none of them
carries a date** — every one reads `0000-00-00`, so the usual "vote date after the
last amendment date" check is unavailable. A substitute is filed as another `Draft`,
so the last draft is the newest. The text that became law is the engrossed print when
the bill was amended, and otherwise the last draft. There is no enrolled text and no
session-law text in the feed.

Delaware also has no amendment records in the feed at all: `amendments[]` is empty on
every one of the 1,296 bills, and no HA or SA appears as its own bill. Amendments
exist only as lines in `history[]`.

## Selecting the right roll

**Delaware re-votes in the originating chamber after the other chamber amends**, so a
measure can carry three or four rolls. Taking the LAST kept floor vote per chamber
lands on the text that became law, which is the Maryland pattern. All 25 batch-01
rolls were checked against this rule and all 25 are their chamber's last.

## Crosswalk

65 entries: 12 proposed by the resolver (all reviewed and accepted) + 2 added by hand
+ 51 explicit nulls. Validated over all 1,278 stored rolls: matched 8,746
member-votes, `no_crosswalk` 0, `out_of_scope` 0, 0 file errors, 0 zero-match rolls.

The two hand-adds are both classes this campaign has met before:

- **Jack Walsh, SD-009** — LegiScan files the legal first name (`John`) in
  `first_name` and the working name in `name` and `nickname`. Our candidate row reads
  `Jack Walsh`, byte-identical to LegiScan's `name`, and the proposer reads neither
  field. The same finding as Pennsylvania, Connecticut, North Carolina, Indiana and
  Kentucky.
- **Gerald Hocker, SD-020** — an exact name match both ways, declined because our
  roster holds two candidate rows for him in the same race. See CODE-FINDINGS §3.

**Delaware's roster in this database is partial by design.** All 11 Senate districts
on the 2026 ballot are covered, but only 8 of the 41 House districts, and only 5 of
those 8 have the sitting member on the ballot. **Fan-out is therefore House median 5
per roll and Senate median 9** — the smallest of any state in this campaign, and a
roster limit rather than a feed limit. A roster campaign is filling the House in
parallel; re-running the import afterwards adds the new members without duplicating
anything, because the fan-out keys on the roll-call URL.

Two members are null for a reason worth stating: **Sarah McBride** (SD-001, left for
Delaware's US House seat) and **Kyle Gay** (SD-005, left to become Lieutenant
Governor) each cast only five votes in this dataset, all on 2024-12-16 organisational
resolutions that the pipeline excludes by measure type. Their successors, Daniel Cruce
and Raymond Seigfried, are mapped.

## Pool and what is left

158 divided rolls on kept bill types. **91 are on measures that became law**, across
60 measures (54 House, 37 Senate; 25 measures divided in both chambers). Every one of
the 158 carries a disposition in `survey/divided-worklist.tsv`:

| disposition | rolls |
| --- | --- |
| out-of-gate (measure did not become law) | 59 |
| candidate:batch-02 | 43 |
| batch-01 | 25 |
| dropped under filter 5 | 16 |
| excluded (procedural, or question not recoverable) | 8 |
| superseded by a later vote in the same chamber | 7 |

**The session is still open and Delaware's signing lags badly** — one bill that passed
on 24 June was signed on 20 August. 40 divided rolls sit on bills that had passed both
chambers but were not yet signed at the dataset cut, so the divided-and-enacted pool
will grow. Watch the count of bills at status 3 fall, the same signal California uses.
