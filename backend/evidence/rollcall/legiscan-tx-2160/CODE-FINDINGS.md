# Texas 89R — findings from the data

Recorded during the narrowing pass (step 3). Neither is fixed in code yet;
both are stated here so the decision is deliberate rather than forgotten.

## 1. LegiScan issues several `roll_call_id`s for one senate action

**This is a real fan-out hazard and the narrowing works around it.**

Three roll calls on SB 2 differ in exactly one field:

```
roll_call_id 1557913 | 1592205 | 1586170
bill_id 1939845 | date 2025-04-24 | chamber S | chamber_id 92
desc "Senate concurs in House amendment(s)" | 19-12 | total 31
votes array: byte-identical across all three (31 members)
```

A full field-by-field diff of a pair shows `roll_call_id` as the **only**
difference. Every other field, including the complete member list with each
member's `vote_id`, is identical.

### Scale

Grouping all 6,824 stored roll calls by
`(chamber, bill_id, date, desc, yea, nay, sha1(member list))`:

| | |
|---|---|
| stored roll calls | 6,824 |
| distinct actions | 6,184 |
| actions carrying more than one id | 543 |
| redundant roll calls | **640 (9.4%)** |
| group sizes | 462×2, 77×3, 2×4, 1×5, 1×15 |
| chamber | **all 640 are Senate** |

The 15-id group is an SB 2 amendment vote on 2025-02-05
("Amendment fails of adoption", 11-20).

Within the 813 divided floor votes: **768 distinct actions, 45 redundant.**

### Why it matters

The fan-out dedupes by URL key, and the key is `ls:<roll_call_id>`. Three
ids give three different keys, so nothing recognizes them as one vote.
Judging all three would write three near-identical records on the same
senator for the same bill — the fan-out's multiplier working against us.

### How it is handled for now

Batch 01 collapses duplicates by member-list hash before selecting, and
takes one roll per (measure, chamber). 13 duplicate ids were collapsed out
of the 25 selected votes.

That is enough for a hand-picked batch and nothing more. **A larger Texas
run needs this in code**, because the collapse currently lives in the
selection script rather than in the importer. The natural place is the
fetch step: keep the lowest `roll_call_id` of each identical group and
record the rest as redundant, the same way unrecorded votes are counted but
not stored. Deferred until a batch actually needs it.

The Ohio pipeline is unaffected — the duplication is a LegiScan senate feed
artifact, and Ohio does not go through LegiScan.

## 2. `role` can contradict `district`

Phil King is `role: "Rep"` with `district: "SD-010"`, and he is a senator.
Seat logic reads `district`, never `role`. Already respected by
`parseLegiscanDistrict`; noted so it stays that way.
