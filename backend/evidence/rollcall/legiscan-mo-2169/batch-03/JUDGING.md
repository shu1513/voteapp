# Judging notes, Missouri batch-03

Five divided House roll calls. **None of the five became law** — each passed the
House and the Senate never voted on it. Every description says so and uses
"would" throughout.

## Missouri's history carries the passage tally, and that settles which roll is which

Three of these bills have two divided House rolls on the same day with the
*same* desc, because the Missouri House perfects a bill and then takes it up for
third reading, and the feed's desc does not distinguish the two.

The bill history does. It prints the passage tally in the action line itself:
`Third Read and Passed (H) - AYES: 85 NOES: 72 PRESENT: 0`. Matching that tally
against the feed picked the right roll every time:

| measure | rolls that day | history passage | roll imported |
| --- | --- | --- | --- |
| HB 544 | 103-56 and 85-72 | 85-72 | 1495925 |
| HB 68 | 107-49 and 92-42 | 92-42 | 1495937 |
| HB 742 | one | 108-50 | 1495948 |
| HB 918 | one | 94-57 | 1554799 |
| HB 565 | one | 97-51 | 1544622 |

Every one of the five was checked this way, and all five matched. HB 544 and
HB 68 carry `acknowledge_later_rolls` for their perfection votes, which the gate
flags as same-day peers.

Had the higher tally been taken on HB 544, the record would have credited
members with a vote 103-56 that was never the vote to pass the bill.

## What each roll voted

Each description is written from that bill's engrossed or committee substitute
text, which is the text the House had before it at third reading. Texts were
downloaded through the LegiScan API with byte length and MD5 verified against
the dataset manifest. No vehicle-bill trap: each act is on the subject its title
names.

## Label reasoning

Every label uses `nay: null`.

- **HB 742**, `civil_rights`, against. No state department could spend on
  diversity, equity and inclusion programs, staffing or initiatives, or on
  anything giving preferential treatment by race, color, religion, sex, gender,
  sexual orientation or ethnicity.
- **HB 544**, `corporate_accountability`, against. Subsection 10 makes a label
  approved by the federal Environmental Protection Agency, or one consistent
  with that agency's carcinogenicity classification, sufficient to satisfy any
  Missouri cancer-warning requirement "under any other provision of current
  law". The effect is to foreclose a failure-to-warn claim about cancer that
  goes beyond the federal label.
- **HB 68**, `corporate_accountability`, against. It strikes "any other injury
  to the person" from the five-year limitations section and leaves those claims
  under the two-year one, so an injured person has less than half as long to
  file.
- **HB 918**, `corporate_accountability`, against. A plaintiff must prove the
  defendant designed, made, sold or leased the particular product used, "and not
  a similar or equivalent product", which forecloses market-share and
  alternative-liability theories.
- **HB 565**, `corporate_accountability`, against. It widens the existing
  immunity for equine and livestock activities.

All four liability measures point the same way, and the label says so rather
than reaching for variety.

## Descriptions

Each cites its own roll call's tally. Plain-language lint: 10 descriptions, 0
warnings, median Flesch-Kincaid grade 6.4, worst 9.7.

## Duplicates

0 found.
