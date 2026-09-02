# Checking LegiScan's Montana roll calls against Montana's own record

Run in batch-07, after one member's vote on SB 542 turned out to be wrong.

## What was compared

Every stored floor roll on all 335 bills in `divided-enacted-worklist.tsv` —
**1,826 roll calls** — was compared member by member against Montana's own vote
record.

Montana publishes it without a key:

    https://api.legmt.gov/bills/v1/votes/findByBillId?billId=<id>

The `billId` is the `id` field of the bill record already used for chapter
numbers, from
`findBySessionIdAndDraftNumber?sessionId=2&draftNumber=<LC>`. Each vote carries
a `legislatorVotes` array of legislator ids and vote types, which resolve
against the official roster at
`/Users/shu/legiscan-data/mt-legmt-legislators.json`. Vote types seen are `YES`,
`NO`, `ABSENT`, `EXCUSED`, `YES_EXCUSED` and `NO_EXCUSED`; the last three fold
into the first three.

Official votes are matched to LegiScan rolls on the same date and the same
member count, then compared name by name. Three members needed an alias because
the two sources name them differently: Julie Dooling / Darling, Robert / Bob
Carter, and Sidney / Chip Fitzpatrick. Every other name resolved on its own, and
**no name failed to resolve**.

The scripts live outside the repository with the other Montana helpers:
`/Users/shu/legiscan-data/mt_verify.py` does the comparison,
`mt_prefetch.py` warms the cache of official records with eight threads, and
`mt_audit_report.py` summarises the result.

## The result that matters

**None of the 81 roll calls this campaign has imported disagrees with Montana's
own record.** Every one was in scope, and every one matches member for member.
No Montana candidate record is wrong.

## The defect rate

| | Count | Share |
| --- | --- | --- |
| Roll calls compared | 1,826 | |
| Tallies that disagree | 63 | 3.5% |
| Rolls where at least one member's vote disagrees | 76 | 4.2% |
| Rolls where the tally matches but members still differ | 13 | 0.7% |

That last row is the reason a totals-only check is not enough. Thirteen rolls
would pass a tally comparison while still recording members on the wrong side.

By stage, the defect is far more common on second readings:

| Stage | Rolls | With a disagreement |
| --- | --- | --- |
| Second reading | 1,007 | 66 |
| Third reading | 812 | 10 |
| Other | 7 | 0 |

The largest disagreements are all second readings. SB 218's House second reading
differs on 86 of 100 members. HB 2's has 55. HB 231's Senate motion to
indefinitely postpone has 47 of 50. This campaign takes third readings, so most
of the damage falls outside what it uses.

## The eight rolls that would have been used

Ten third readings disagree, and eight of them are a chamber's last kept floor
vote — that is, exactly the roll this campaign would select.

| Measure | Roll | Date | LegiScan | Montana | Members differing |
| --- | --- | --- | --- | --- | --- |
| SB 542 | 1556679 | 2025-04-24 | 73-26 | 72-27 | 1 |
| HB 15 | 1481075 | 2025-01-27 | 78-20 | 77-21 | 1 |
| HB 76 | 1558903 | 2025-04-28 | 69-31 | 68-32 | 1 |
| HB 284 | 1551835 | 2025-04-17 | 58-41 | 59-40 | 1 |
| HB 636 | 1508554 | 2025-03-07 | 84-15 | 85-14 | 1 |
| HB 888 | 1558107 | 2025-04-25 | 59-41 | 58-42 | 1 |
| SB 243 | 1546282 | 2025-04-11 | 77-22 | 76-23 | 1 |
| SB 342 | 1546349 | 2025-04-11 | 58-41 | 57-42 | 1 |

Seven of these are marked `held:legiscan-vote-defect` in the worklist. HB 636's
roll never entered the worklist, because 84-15 is not divided.

The other two third-reading disagreements, HB 2 roll 1538411 and HB 492 roll
1503251, are superseded by later votes and so were never selectable.

## What happens next

The importer verifies the SHA-256 of each roll call payload against the value
approved at fetch time, and separately checks the evidence file's tally against
the approved row. Those guards make a hand correction impossible, which is
right: they exist to stop unreviewed editing of source data.

So an affected roll can only be held. Importing one needs a supported way to
record that an upstream source is wrong about a named member, which is a code
change and belongs in its own review. Until then the eight rolls above stay
held, and each affected measure is carried on its other chamber alone, or not at
all.

`legiscan-vote-audit.tsv` in this directory lists every one of the 76
disagreeing rolls, with the members and the direction of each disagreement.

## Whether this reaches other states

Unknown, and worth checking. The comparison was only possible because Montana
publishes member-level roll calls in a machine-readable form without a key. Any
state that does the same can be audited the same way, and any state already in
the registry that does should be, before its records are promoted to production.
