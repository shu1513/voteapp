# Idaho 2025 (session 2168) — batch-01

Ten measures, twenty roll calls, every measure divided in both chambers.
872 candidate records across 88 candidates. First Idaho roll-call batch; the
local database held zero Idaho records before this run.

## How the ten were chosen

Taken from `survey/divided-enacted-worklist.tsv`, which already carries every
measure's disposition. Rows marked `candidate:unbatched` and divided in both
chambers were preferred, because Idaho puts all 35 Senate districts on the 2026
ballot and a both-chamber measure is worth about 87 records.

All five selection filters hold on every row:

1. Closely divided. The smaller side is at least a quarter of the larger on all
   twenty rolls. The narrowest is the Senate on House Bill 245, 21 to 14.
2. Became law. Each measure carries a `Session Law Chapter` line in Idaho's own
   history, listed below.
3. A subject a voter would recognize: marijuana fines, immigration and public
   benefits, foster care, public records, vaccine rules, pipeline penalties,
   lobbying, license plate readers, wildfire liability.
4. One roll per measure per chamber, the chamber's last kept roll.
5. Every measure carries a research area with a direction that can be defended.
   No measure in this batch lands on the barred `general` label.

## Version check

Eight of the ten measures have no engrossed print, so both chambers voted the
introduced text and that text became law. Two were amended in the Senate:

- **House Bill 253**, chapter 298. Senate passed the amended text 25-9 on
  April 1. The House then concurred and passed the engrossed text 45-24 on
  April 3. Both kept rolls sit on the enrolled text.
- **House Bill 271**, chapter 252. Senate passed the amended text 24-10 on
  March 26. The House passed the engrossed text 50-20 on March 31. Both kept
  rolls sit on the enrolled text.

Filter 4 keeps each chamber's last roll, which is why both landed on the text
that became law rather than the introduced text.

## The four rolls Idaho's feed truncates

`Rules Suspended:` lines lose their tally in LegiScan's copy (CODE-FINDINGS §4),
so four kept rolls had no tallied history line. Each was confirmed against the
bill page at legislature.idaho.gov before selection:

| measure | roll | feed | Idaho's page |
| --- | --- | --- | --- |
| House Bill 253 house | 1536751 | 45-24 | PASSED 45-24-1 |
| House Bill 398 house | 1525975 | 54-14 | PASSED 54-14-2 |
| Senate Bill 1180 house | 1536768 | 45-24 | PASSED 45-24-1 |
| Senate Bill 1183 house | 1529738 | 46-23 | PASSED 46-23-1 |

## The measures

| measure | chapter | house | senate | research area | a yes vote is |
| --- | --- | --- | --- | --- | --- |
| House Bill 7 | 7 | 54-14 | 27-8 | public safety and crime control | for |
| House Bill 135 | 275 | 46-22 | 26-9 | immigration; social programs and welfare | against; against |
| House Bill 245 | 142 | 53-17 | 21-14 | social programs and welfare | for |
| House Bill 253 | 298 | 45-24 | 25-9 | anti-corruption | against |
| House Bill 271 | 252 | 50-20 | 24-10 | public safety and crime control | for |
| House Bill 290 | 174 | 49-21 | 23-11 | environment and public health | against |
| House Bill 294 | 144 | 44-26 | 27-8 | corporate accountability | for |
| House Bill 398 | 280 | 54-14 | 27-8 | anti-corruption | for |
| Senate Bill 1180 | 316 | 45-24 | 25-10 | data privacy | for |
| Senate Bill 1183 | 249 | 46-23 | 24-11 | corporate accountability | against |

## What was not touched

House Bill 93, the parental choice tax credit, is the most newsworthy divided
measure of the session and is deliberately left out. The campaign's standing
school-choice line puts it on the barred `general` label, so it is a drop unless
the operator reopens that rule.
