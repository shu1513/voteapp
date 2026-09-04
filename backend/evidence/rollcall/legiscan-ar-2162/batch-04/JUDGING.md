# How batches 02, 03 and 04 were judged

These three batches were worked as one run that finished the Arkansas 2025 Regular Session, so
the method is recorded once here and referenced from each batch.

## Source

Every judgment was written from the enrolled Act, read top to bottom through the strikethrough
reader described in the campaign README. No summary, analysis or news report was used, and no AI
provider was called. Acts were fetched through LegiScan bulk `getBillText` with the dataset's own
`doc_id` and size-checked against what the API returned.

The two joint resolutions are the exception, and the reason matters: **a joint resolution
referring a constitutional amendment never becomes an Act, so LegiScan carries only its
superseded draft.** SJR 11 had three adopted amendments and SJR 15 one. Their adopted prints came
from the legislature directly, and the "As Engrossed" dates in each header match every adopted
amendment. The reader was extended to take a local file for exactly this case.

## How the reading was done, and checked

Sixty-four acts were read by eleven subagents, six acts each, each writing a structured file to
disk rather than reporting back in prose. Every one was told to run the marked reader over every
page carrying an operative section, to say who is bound by each rule, to carry every limit, and
to check for a contingent effective date.

**A subagent report is evidence, not testimony.** One report came back thin and its act was
re-read in full by hand. Every claim a description rests on was checked against the act before
importing. Kentucky's campaign had a subagent write a section it never read, which is why this
check is a step rather than a courtesy.

Two of the eleven flagged, unprompted, that they had placed the ballot-petition acts under
`civil_rights` only because the brief routes access questions there, and that
`election_integrity` was equally defensible. That disagreement is what produced the line drawn in
batch-02, and it is stated rather than buried.

## The line drawn on the ballot-initiative cluster

Batch-01 imported SB 207, SB 211 and HB 1713 as `election_integrity` for. Eight more acts in the
same family arrived in this run, and treating them all the same way would have been wrong.

**The rule applied: score `election_integrity` for where the act tests the honesty or accuracy of
the petition transaction — a fraud warning, a sworn statement, an identity check, disqualifying a
canvasser found to have broken the law, or extending those same rules to local petitions. Take no
stance where the act instead limits who may take part or how long a measure stays alive, because
there the direction is genuinely contested between protecting the process and restricting citizen
lawmaking.**

That puts SB 208, SB 209, SB 210, SB 551 and SB 584 with batch-01's three, and leaves HB 1221,
HB 1222 and HB 1574 without a stance. Each description carries the facts a reader needs either
way, including the ones that cut against the label: SB 209's finding is made by one official at
the lowest standard of proof in American law, with no hearing and no appeal written into the act,
and the description says so.

## Labels

Every stance label carries an explicit `nay: null`. None of these measures passes the test the
campaign settled on for authoring a no-side stance: single subject, whole operative content
inside the area's own mechanism, and a mainstream objection to that mechanism rather than to
cost, local control or a rider. Tags therefore sit on the yes side only, except on the no-stance
measures, where a `general` label tags both sides with no direction.

Directions follow the research area's own description, never the bill's framing.

## Writing

Reading level was measured for each batch, not assumed, and a first draft was rewritten before
anything was judged where it came in too high.

| Batch | Flesch-Kincaid median | Worst | Longest sentence | Lint warnings |
| --- | --- | --- | --- | --- |
| batch-02 | 7.5 | 9.0 | 31 words | 0 of 42 |
| batch-03 | 8.0 | 9.4 | 34 words | 0 of 24 |
| batch-04 | 7.8 | 9.8 | 36 words | 0 of 26 |

batch-02's first draft measured 8.8 median and 11.8 at worst and was rewritten. Descriptions run
five to nine short sentences rather than the two to four the standard suggests; cutting further
drops the statutory limits these acts turn on, and dropping exactly those limits has caused most
of this campaign's correction rounds.

Mechanical checks run before judging: the body joins its closing tally sentence with a period and
the builder asserts no comma splice; each description is asserted to cite its own roll's tally;
and a British-spelling scan runs over every description. **That scan earned its keep three times.**
It caught `licence` twice in batch-03, and it missed `neighbour` in batch-04 because the word list
had only `neighbouring` — the list was widened and every batch re-checked. The yes and no
descriptions are generated from one body behind different opening clauses, so the pair cannot
drift apart.

## Import

| Batch | Rolls | Inserts | Candidates | Stamp |
| --- | --- | --- | --- | --- |
| batch-02 | 21 | 893 | 96 | `2026-09-04T20:23:38.767Z` |
| batch-03 | 12 | 428 | 96 | `2026-09-04T20:26:21.951Z` |
| batch-04 | 13 | 575 | 96 | `2026-09-04T20:28:46.395Z` |

Every batch: dry run equalled the real run, 0 errors, 0 notified, 0 ambiguous, and a convergence
dry run reported every row `unchanged`. **Each batch's dry-run stamp matches zero rows**, which is
positive proof `--dry-run` wrote nothing.

⚠ **Reconciliation is by run stamp, never by table delta.** A parallel session was importing
Delaware and Minnesota roll calls into the same local database while these batches ran, so the
table total moved for reasons that have nothing to do with Arkansas.

Arkansas now holds **2,708 records across 96 candidates and 2,131 tags, on 61 approved rolls**.
96 is every member the crosswalk maps: Arkansas's Speaker votes, so there is no shortfall of the
kind Texas and Georgia show.

## Duplicate check

The importer flagged no related records in any batch. Because that scan misses a hand-written
record dated differently from the roll, a wider sweep also ran per batch over every Arkansas
record not written by this pipeline, matching each measure by bill number and by act number. It
found fourteen records, **every one a sponsorship or co-sponsorship claim that states no vote and
no tally**, so all are distinct claims and nothing was retired.

⚠ **That sweep also showed why the act number matters: Arkansas reuses bill numbers between
sessions.** Records naming HB 1365 (Act 264), HB 1428 (Act 325) and HB 1641 (Act 843) are about
different bills from other years than the Act 938, Act 855 and Act 600 imported here. Matching on
the bill number alone would have flagged them as duplicates.

## Production

Production was not touched and holds no Arkansas roll-call records.
