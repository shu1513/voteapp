# South Dakota roll-call import — plan

## Goal

Give South Dakota voters a record of how the legislators on their November 2026
ballot actually voted, taken from the state's own roll calls rather than from
anyone's description of them.

South Dakota is a good fit. Every one of the 105 legislative seats is elected
every two years, so every member the crosswalk maps is a candidate. The local
database holds 160 South Dakota legislative candidates for 3 November 2026: 110
running for the House across 37 districts, and 50 running for the Senate across
35 districts. The House has 37 districts rather than 35 because three districts
are split into single-member subdistricts.

## Scope

Three LegiScan sessions, all surveyed, all registered:

    2170  2025 Regular Session        config key `SD`
    2231  2026 Regular Session        config key `SD-2231`
    2222  2025 First Special Session  config key `SD-2222`

Local database only, `postgresql://localhost:5432/voteapp`. Production is never
touched and holds zero South Dakota records. No AI provider is called at any
point in this work.

## The five filters

A vote is imported only if all five hold.

1. **The vote was closely divided.** The smaller side is at least a quarter of
   the larger. A vote nobody contested tells a reader nothing about a
   legislator.
2. **The measure became law.** South Dakota's bill history is the source: an
   action beginning `Signed by the Governor`, or the wording the state uses when
   a bill becomes law without a signature.
3. **The subject is one a voter would recognize.** Housekeeping, fee schedules
   for a single trade, and single-parcel land transfers are dropped.
4. **One vote per measure per chamber, and it must be that chamber's word on the
   text that became law.** The slot's vote is the last roll call in that chamber
   whose outcome the state's own history records as `Passed`. A closely divided
   vote on text a later action replaced is not imported.
5. **The measure carries a research area with a defensible direction.** A bill
   whose direction is genuinely arguable within the same area is dropped rather
   than filed under a label that misdescribes why members voted. The no-stance
   `general` label is barred here, so a measure with no honest direction is
   dropped, not neutered.

## The pool, measured

| Session | Slots after filter 4 | Measures |
|---|---|---|
| 2025 Regular | 72 | 53 |
| 2026 Regular | 62 | 45 |
| 2025 Special | 2 | 1 |
| **Total** | **136** | **99** |

Three of those slots, on two measures, are joint resolutions. In South Dakota a
joint resolution either proposes a constitutional amendment or grants
legislative approval to something; neither goes to the Governor. Both bills'
histories confirm it — there is no enactment action on either. They fail the
second filter and are dropped with that reason written down. **The pool that can
reach a batch is therefore 133 slots over 97 measures.**

## Hazards to check at every step

**The hoghouse amendment is South Dakota's signature trap.** A floor or
committee amendment can strike a bill's entire text and replace it with new
language, sometimes on an entirely different subject, while the bill keeps its
old number and often its old title. Every selected measure gets its amendment
history read before a description is written, and each chamber's vote is
checked against the text that chamber actually had in front of it. The dataset
carries an `amendments` array with an `adopted` flag and a link to each
amendment's own document, which is what makes this checkable.

**The vehicle-bill trap** is the same problem seen from the title's side: the
title describes a law that is not the one that passed. Reading the enrolled text
rather than the title settles it.

**LegiScan's `passed` flag cannot be trusted here.** It is wrong on 31 of the
1,255 floor roll calls, in both directions: 29 votes it calls passes were
defeats under a two-thirds requirement, and 2 votes it calls failures were
passed on the Lieutenant Governor's tie-breaking vote. Selection reads the
outcome word in the state's own bill history.

**Committee votes wear the same caption as floor votes.** `Do Pass` is both. The
tally separates them cleanly here, but nothing else does.

## Sources

Judgments are written from the Legislative Research Council's pages on
`sdlegislature.gov`: the bill history, every amendment, and the enrolled text.
Committee testimony is advocacy and is not a source. Sponsor material is not a
source. Where a description states what a law does, that statement traces to
words in the enrolled act.

## Steps

1. Survey all three datasets and read the caption histogram. **Done.**
2. Write the configuration entries from what the survey measured. **Done.**
3. Open the configuration pull request, based on `main`. **Done.**
4. Fetch the roll calls; build the crosswalk between LegiScan's people and the
   November 2026 candidates; review every mismatch by hand, trusting LegiScan's
   `district` field over its `role` field.
5. Select the first batch under all five filters.
6. Judge each measure, checking per roll which version of the text that chamber
   voted on.
7. Run the judge and the importer as a dry run, then for real, and reconcile the
   row counts three ways.
8. Commit the evidence with this plan and the judging notes, and open a data
   pull request based on `main`.

Steps 1 to 3 are this pull request. Steps 4 to 8 follow in a separate pull
request that shares no file with this one.
