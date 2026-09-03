# New York feed findings

Recorded, not fixed. Each one is a property of LegiScan's New York data, and each is
handled at selection time rather than in code.

## 1. One roll call can never be stored: its tally disagrees with its own member list

Roll 1473007 — Senate Bill 824, Senate, 2025-01-22 — reports 35 yes votes while its member
list holds 36 yes votes. The parser refuses it, the fetch reports one `parse_error`, and
the fetch exits non-zero. That non-zero exit is expected for New York and does not mean the
run failed: all 5,459 valid rows were stored.

This is the same class of defect as Indiana's wrong-side member lists, and the same rule
applies: the stored evidence must pin what the source served, so nothing here is patched.
The cost is one chamber of one measure. Senate Bill 824 amends the Climate Change Superfund
Act and is divided in both chambers, but only its Assembly vote can ever be imported.

Every other roll in the session is internally consistent: 0 rolls where `total` differs
from yea + nay + not voting + absent, and 0 other member-list mismatches.

## 2. Constitutional amendments are ordinary bills, but Regents elections ride resolutions

New York proposes constitutional amendments as ordinary bills (type `B`), so the Georgia
problem — amendments filed as resolutions and dropped before the config is read — does not
happen here.

What the dataset does file as concurrent resolutions (type `CR`, 11 bills, 12 roll calls)
is the joint session that **elects Regents of the University of the State of New York**,
plus the resolution adjourning the session. Two of those votes are divided (106-44 and
94-48). They are not votes on a measure, and `CR` is not a kept bill type, so they are
dropped before the config is consulted. That is the right outcome, but it is an accident of
the bill-type filter rather than a decision the config makes, so it is written down here.

## 3. Chapter amendments create near-duplicate measures

New York's governor routinely signs a bill on the understanding that the legislature will
pass a second bill amending it. Both become law, both can be divided, and both appear in the
pool as separate measures with nearly the same subject:

| enacted act | chapter amendment |
| --- | --- |
| A 4040, disparate impact (Chap. 649) | S 8338 (Chap. 706) |
| S 36, abortion medication labels (Chap. 7) | A 5285 (Chap. 108) |
| S 8416, FAIR Business Practices Act (Chap. 708) | S 8811 (Chap. 94 of 2026) |

Importing both halves of a pair would give every member two records making nearly the same
claim, which is the Maryland cross-filed twin problem in a different shape. The rule used
here: **import the act, drop its chapter amendment with a written reason**. The
`SAME AS` line and the action history on the Assembly bill page are what identify a pair.
