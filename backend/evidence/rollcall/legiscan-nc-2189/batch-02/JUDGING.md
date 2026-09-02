# How batch-02 was judged

## Sources

Every measure was read from the enacted session law text on ncleg.gov, not from
a summary and never from a sponsor. Where the General Assembly publishes a
neutral staff summary, it was used as an index into the law and each claim taken
from it was checked against the enacted text. For Senate Bill 249 the
legislature's own statistics packet, published with the map, is the source for
the partisan figure in the description.

Three bills needed the enacted text read page by page as images, because the
session law files resist plain text extraction.

## Version check

For every measure the question is which text the chamber actually voted on. It
matters here more than usual, because North Carolina moved a great deal of
content late.

- Both chambers' final votes were on the same text for every measure in the
  batch. Earlier votes on the same bills were often on very different text, and
  none of those earlier votes is in the batch.
- Senate Bill 249, Senate Bill 472, House Bill 694 and House Bill 307 all had
  their titles changed and their content expanded after one chamber had already
  voted. House Bill 694 passed the House 114-0 as a study-only bill; everything
  substantive was added by the Senate afterwards. The 114-0 vote is not in the
  batch. The 66-42 concurrence, which is a vote on the enacted text, is.
- House Bill 307 was filed as "Various Criminal Law Revisions" and became
  Iryna's Law in a Senate committee substitute. The death penalty language was
  added by floor amendment on the same day as the Senate's final vote, so both
  final votes did include it.

## Tally check

All 18 rolls considered for this batch were compared against the official
ncleg.gov roll-call transcripts, and all 18 match. The two rolls added late,
House Bill 1089 and Senate Bill 1080 in the Senate, were then checked a second
time directly against transcripts S-548 and S-535, because their member lists
are short by one seat.

Two traps the check caught, neither of which changed a number:

- Senate Bill 249 in the Senate on 2025-10-21 had two recorded votes and both
  were 26-20. Roll 496 was a motion to table an amendment; roll 498 was the vote
  on the bill. Matching on the tally alone would not have distinguished them.
  The imported roll is the third reading.
- House Bill 768 in the House on 2025-06-24 was voted on three times. A concur
  vote passed 68-40, a motion to reconsider passed 104-1, and the concur vote
  was then retaken and passed 67-44. The imported roll is the retaken vote,
  which is the one that stands. The earlier 68-40 roll is listed under
  `acknowledge_later_rolls` so the superseded-stage gate lets the real vote
  through on purpose.

`acknowledge_later_rolls` is also set on the two constitutional amendment rolls,
where second and third reading fall on the same day and the imported roll is the
third reading.

## Direction calls

A no vote gets a stance only where the whole act sits inside that one area:
Senate Bill 249, Senate Bill 442, Senate Bill 472 and House Bill 694. Everywhere
else a no voter gets no tag, because the act spans more than one area and a no
vote cannot be read as being about a particular one.

Two measures carry two labels, House Bill 307 and House Bill 768. In both cases
the two strands point the same way within their own areas, but neither strand
covers the whole act.

House Bill 1089 and Senate Bill 1080 are the only measures where a yes vote is
scored "for". Both are constitutional amendments headed to the November 3, 2026
ballot. A yes vote there is a vote to put the question to voters, which is not
the same as capping a tax. The descriptions say so, and say the property tax
amendment sets no limit itself.

## Descriptions

Reading level was measured, not assumed. Across the ten measures the
Flesch-Kincaid grade of the yes description runs from 5.1 to 7.8, mean 6.4. No
sentence runs over 45 words. Descriptions are five to seven sentences, which is
longer than the two to four the house style asks for. The same trade-off was
made in batch-01: short sentences at a seventh-grade reading level cost sentence
count, and the reading level was treated as the more important of the two.

## Results

- Judge: 16 approved, 0 pending, 0 errors. No superseded-stage failures.
- Import: 16 files, 1,075 records inserted, 0 errors, 0 notified.
- The database then held 1,075 rows under this run's stamp, matching the report.
- A dry run after the import reports 1,075 unchanged, so the batch converges.
- North Carolina now holds 1,788 live records across 147 candidates.
- No `related` flags and no `ambiguous` outcomes on any of the 16 rolls.
- Every imported roll's date matches the official record.
- Production was not touched. No AI provider was called.
