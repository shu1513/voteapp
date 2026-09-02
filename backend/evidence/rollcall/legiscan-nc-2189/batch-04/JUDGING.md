# How batch-04 was judged

## Sources

Only official, neutral sources were used:

- The bill page on ncleg.gov, for the bill's history, its printed editions and
  its recorded votes.
- The bill text itself, the edition that was actually before the chamber on the
  day of the vote.
- The official roll-call transcript on ncleg.gov, for every tally.

No sponsor statement, advocacy group, think tank or opinion piece was used.
Where a bill's short title is one side's framing — "Promoting Wholesome Content
for Students", "The Citizens Support Act", "Depoliticize Government Property
Act" — the description is written from the operative text, not the title.

## Tally verification

All 49 candidate rolls were checked against the transcript, not a sample.

48 matched. One did not:

| Bill | Chamber, date | LegiScan | Official |
|------|---------------|----------|----------|
| H244 | House, 2025-04-16 | 69-43 | 69-44 |

H244 is held and not imported. See finding 4 in `../CODE-FINDINGS.md`.

All 15 imported rolls match the official transcript exactly.

## The two short Senate rolls

S1082 (2026-05-20) and S808 (2026-05-05) list 49 senators, not 50. Both are
correct. Senate District 18 was vacant on both dates: Terence Everitt resigned
and Haseeb Fatmi was appointed 2026-05-21. The transcripts account for all 49
sitting senators as voting or excused, with nobody unaccounted for.

This matters beyond these two rolls. Finding 3 flagged the short May 2026 Senate
lists as unexplained. They are explained: a real vacancy, not a feed defect.

## Superseded-stage checks

Only three of the 49 have any later roll in the same chamber, and all three are
procedural motions the North Carolina config excludes: a previous-question
motion on H618 and on H636, and a motion to appeal the chair's ruling on S378.
None is a vote on the measure.

S1082 is the one roll needing an explicit acknowledgement. A constitutional
amendment requires a recorded three-fifths vote on both readings, so the Senate
took two roll calls the same afternoon, both 30-16, with the same members on each
side. Only the third reading is imported. Its judgment carries
`acknowledge_later_rolls: [1700901]` for the second reading, which is a kept vote
on the same day.

## Which measures were judged fit to import

The fifth filter asks whether a yes or no vote carries a defensible position in a
research area we track. Three rules were applied consistently:

**Contested worth is fine; contested direction is not.** Our labels record which
way a bill pushes, not whether that is good. So a bill can be politically
divisive and still be labelled, as long as everyone would agree on the direction
it pushes. H636 restricts what schools may put on a shelf; people disagree about
whether that is right, but not about which direction it moves. It is in. H618,
which would let pharmacists dispense ivermectin without a prescription, is out:
easier access and medical caution are both health goods, so the direction itself
is arguable.

**The area has to fit, not merely be reachable.** Several clean, single-purpose
bills were dropped because the closest available research area misdescribes why
members voted. Repealing certificate-of-need review (S370) could be filed under
government efficiency, but that is a health-system vote, and the label would
mislead. Same for S58, which bars the Attorney General from challenging
presidential executive orders: there is no separation-of-powers area, and forcing
it into another would be worse than leaving it out.

**A bill the chamber never saw cannot be attributed to anyone.** Five bills now
carry text adopted after the vote. They are out regardless of merit.

## Multi-strand measures

Four bills carry two strands. Each strand gets its own label, following the
batch-03 precedent set by H437.

Where a no vote does not clearly imply the opposite position on a secondary
strand, that label's `nay` is left null, so no-voters get no tag on it. H261 is
the clearest case: three quarters of the bill is a gang-related sentencing
enhancement with no immigration element, so a member voting no is not thereby
tagged as taking an immigration position.

## No description claims finality

The 2025-2026 biennium had not adjourned when the dataset was cut on
2026-08-30; the feed holds a House vote from 2026-08-04. So no bill here is
provably dead. H936 is in a conference committee that has not reported. S1082
is in House Rules and the election it names has not happened. Every description
uses the present tense — "has not voted", "is not law", "has not gone to
voters" — and states the position as of the dataset date. An earlier draft
said "died" and "did not become law"; that was a claim about the future and
was rewritten before review closed.

## Reading level

Every description was measured. All 30 fall between grade 5.1 and grade 6.9 on
the Flesch-Kincaid scale, against a target of seventh grade or below. No
sentence exceeds 45 words, and the plain-language lint returns zero warnings
across all 30.

## Reconciliation

Three counts were compared and all agree at 1,133:

- the importer's own report: 1,133 inserts across 15 rolls, no errors;
- the database: North Carolina's live roll-call records went from 2,004 to
  3,137, and each roll's row count matches its report line;
- a convergence dry run afterwards: 1,133 unchanged, nothing left to write.

A second real run was also made and reported 1,133 unchanged, confirming the
import is idempotent.

Records carry no duplicate-flag warnings: the `related` count is zero on every
roll.
