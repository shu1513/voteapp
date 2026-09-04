# Arkansas batch-01 — selection

11 measures, 15 roll calls, 812 records across 96 candidates. Local database only;
production has no Arkansas roll-call records.

## How the batch was chosen

The campaign's five filters, in order:

1. **Divided.** The losing side is at least a quarter of the winning side. Arkansas's 2,449
   kept floor votes give 204 divided rolls.
2. **Became law.** 131 of those 204 are on measures that became law, across 95 measures.
3. **A nameable subject that maps to a research area.**
4. **One roll per measure per chamber — the chamber's LAST kept floor vote.** In Arkansas
   that is always the vote cast on the text that became law, because of how the two chambers
   handle amendments from the other side (see the campaign README). 31 of the 131 gated rolls
   are superseded by a later vote in the same chamber and are marked so in the worklist.
5. **A defensible direction.** A measure is taken only if it carries a research area with an
   honest for-or-against direction. One measure was dropped here after being read in full.

Within what survived, the batch was chosen House-first: a House roll reaches about 80
candidates, a Senate roll about 11, because Arkansas staggers its Senate and only 17 of 35
districts are on the November 2026 ballot.

## The measures

| Measure | Act | Rolls | Area and direction |
| --- | --- | --- | --- |
| SB 3 | 116 | House 65-27, Senate 24-6 | civil_rights, against |
| SB 520 | 747 | Senate 22-7, House 68-22 | civil_rights, against |
| SB 486 | 955 | House 60-22 | civil_rights, against |
| SB 426 | 654 | House 73-20 | immigration, against |
| HB 1974 | 948 | House 76-20 | immigration, against |
| SB 591 | 973 | House 63-17 | womens_reproductive_rights, against |
| SB 207 | 218 | House 67-26, Senate 25-9 | election_integrity, for |
| SB 211 | 241 | House 65-27, Senate 24-9 | election_integrity, for |
| HB 1713 | 602 | House 60-23 | election_integrity, for |
| HB 1150 | 624 | Senate 26-9 | corporate_accountability, for |
| HB 1017 | 904 | House 68-19 | social_programs_and_welfare for; civil_rights against |

Nine of the eleven score against the majority's direction on their area, which is the shape
every one-party-supermajority state in this campaign has produced: the divided-and-enacted set
is the majority's own agenda. Texas, Tennessee and North Carolina all read the same way, and
the Democratic-trifecta states read as its mirror image.

## Version check

Every roll was checked against the bill's own history before it was selected. Arkansas's
`amendments[]` records carry `adopted: 0` even for amendments the history says were adopted,
so the flag was not used — Maine and Alabama recorded the same defect. Two cases were worth
naming:

- **SB 591**: the Senate's first vote on 2025-04-08 FAILED 15-9. The Senate then adopted an
  amendment and passed the bill 29-6 the next day, which is not a divided vote, so the Senate
  drops out of the batch. Only the House roll is used, and it is on the enacted text.
- **SB 571** and several others: the House failed a vote, reconsidered, and passed. Selecting
  the chamber's last kept floor vote handles this without special cases.

## Date audit

All 15 roll dates match a passage or concurrence line in the bill's own history, on the same
date, in the same chamber. No `official_vote_date` override was needed. Arkansas's last voting
day in this session was 2025-04-16 and two batch rolls fall on it; both match the history.

## Dropped under filter 5, after the act was read in full

**SB 571, Act 1002.** Its title reads "To Amend The Law Concerning Municipal Building And
Zoning Regulations", which is why it was screened in. The enacted text is one new section: a
city may not enforce its building or zoning rules on county-owned property that is used for a
public purpose and sits inside the city limits, and the county's own rules apply instead. That
is a question about which government has authority, not about housing supply or housing cost,
and no research area fits it. The title-is-not-the-text rule again.

## What is left

131 gated rolls: 15 imported, 3 dropped, 31 superseded, 82 still to read. All are
dispositioned in `../survey/divided-enacted-worklist.tsv`. Arkansas's 2026 Fiscal Session
(LegiScan 2242) and 2026 First Special Session (LegiScan 2261) are downloaded and unsurveyed.
