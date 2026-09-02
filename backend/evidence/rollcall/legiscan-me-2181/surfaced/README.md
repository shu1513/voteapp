# Maine — the surfaced rolls, dispositioned

The classifier leaves **31 Maine rolls surfaced** (`is_floor_vote NULL`): the
`Accept Report` / `Acceptance Of Report` / `Acc Majority Report` families (19)
and the bare `Recede` family (12). Their descriptions do not say what the vote
decided, so the config refuses to guess. These sit outside the 433-roll
divided-and-enacted worklist and outside every batch.

Each one has now been matched to its action in the Maine journal, through the
bill history, to establish what it actually decided. `dispositions.json` holds
the per-roll finding. Nothing here is unreviewed.

| disposition | rolls |
|---|---|
| `out_of_gate` — not divided, or the measure never became law | 15 |
| **`substantive_unreachable`** — a genuine ought-to-pass report acceptance, divided, became law | **9** |
| `procedural` — a report accepted before referral, or a chamber receding from its own act | 4 |
| `failed_motion` — the motion to recede failed | 2 |
| `ambiguous_recede` | 1 |

## What the research established

**Maine writes `Accept Report` when the committee reports out UNANIMOUSLY.**
When the committee splits, the journal and the roll description both name the
side — `Accept Majority Ought To Pass As Amended Report`, which the config
keeps. When there is only one report there is no side to name, so the
description says only "Accept Report" and **the report's kind lives in the
committee's `Reported Out:` line in the bill history, not in the roll**.

That is why no pattern can rescue these. The nine substantive ones read
identically to the two procedural ones (LD 2225 and LD 2231), where the
"report" was a preliminary or study report and the bill was referred to
committee the same day. Only the history distinguishes them.

The bare `Recede` family is confirmed correctly excluded, which validates the
PR #934 review fix: of the five divided-and-enacted `Recede` rolls, **two are
motions that FAILED** (LD 1968 69-77, LD 297 49-94), one is a chamber receding
from its own engrossment to adopt an amendment (LD 2155), and one is the Senate
receding from its acceptance of an Ought Not To Pass report — undoing its own
kill of LD 613. None is a concurrence.

## Why the nine are not imported

`legislative_votes_approved_fields_check` requires `is_floor_vote = true` before
a row can be approved, and `is_floor_vote` is set by the classifier at fetch
time. Two paths exist and neither is taken:

1. **Widen the config.** Rejected: the distinguishing fact is not in the
   description, so any pattern that caught the nine would also catch the
   procedural report acceptances and would mislabel a future unanimous *ought
   not to pass* report as passage.
2. **Hand-edit `is_floor_vote` on the nine rows.** Rejected on the same grounds
   as the Illinois `official_vote_date` case: `upsertLegislativeVote` writes
   `is_floor_vote = $5` from the classifier on every fetch, so the edit would be
   silently reverted by the next re-fetch and nothing would flag it.

The honest resolution is a **parked design**, not a hand fix: a committed
per-roll disposition file that the fetcher consults for surfaced rolls, exactly
as `crosswalk.json` supplies identity the proposer cannot infer — human commits,
machine applies. That is a real feature and it is worth roughly **9 Senate rolls
≈ 216 records**, so it is recorded here rather than built at the tail of a
finished campaign.

If it is ever built, `dispositions.json` is the input: the nine
`substantive_unreachable` rolls are ready to judge, and the other 22 are already
screened out.
