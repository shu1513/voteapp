# North Carolina findings, recorded and not fixed

## 1. LegiScan drops unaffiliated members from the 2026 House roll calls

On the three House veto-override rolls of 2026-06-24 (LegiScan rolls 1711513,
1711515 and 1711527) LegiScan lists 118 members and reports the vote as 71-46.
The official House roll-call transcripts for the same votes (RCS 738, 740 and
736) record 71-47, with 1 not voting and 1 excused absence, and they name the
two members LegiScan leaves out:

- Nasif Majeed, House District 99, listed by the House under "Noes
  (Unaffiliated)".
- Carla Cunningham, House District 106, listed under "Excused Absence
  (Unaffiliated)".

Both members were in LegiScan's lists for the same chamber in July 2025, when
every roll matched the official record exactly. Both changed their registration
between the two sittings, so the missing rows follow the party field, not the
date.

Effect on this campaign: no member is recorded on the wrong side, and neither of
the two is a mapped candidate, so no record was written about the wrong person.
The problem is the printed tally. The approval gate requires the record text to
quote the stored tally, so importing these rolls would have told about ninety
readers that the House voted 71-46 when North Carolina's own record says 71-47.

What was done: the three rolls were imported, then withdrawn the same day. Their
312 records were retired with a reason naming this finding, the rolls were
returned to the review queue as pending, and their evidence files were moved to
`batch-01/held-rolls/`. They can be imported once the pipeline can cite an
official tally that differs from the feed, in the same shape as the
`official_vote_date` override that Illinois needed for dates.

Checked against the official record: 11 of the 14 batch-01 rolls match exactly.

## 2. The House prints a materiality ruling in front of the question

Twenty-two House rolls carry a `R2 Ruled Mat&#x27;l` or `R3 Ruled Mat&#x27;l`
prefix, which records that the presiding officer ruled the matter material under
a House rule. The vote is still on the concurrence or the conference report that
follows the prefix. LegiScan leaves the apostrophe HTML-escaped, so the config
patterns match the escape rather than a normal apostrophe.
