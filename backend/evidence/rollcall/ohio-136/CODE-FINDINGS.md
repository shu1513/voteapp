# Code findings from the GA-136 survey and crosswalk sweep

Recorded, not fixed — this campaign's working session was asked to do the
data run and report anything that needs source changes. Both findings are
from real GA-136 data, not speculation. Neither wrote bad data: in each
case the pipeline declined to act, which is the designed failure mode.

## 1. Joint resolutions are ADOPTED, not passed — three action codes are unclassified

`classifyOhioVoteAction` knows `pass_300` (Passed) but not the codes Ohio
uses for joint resolutions. The survey found four vote-bearing actions it
could not classify, all on joint resolutions, so all four are stored with
`is_floor_vote = null` and can never be queued or approved:

| Bill | Chamber | Date | Code | Action text | Tally |
| --- | --- | --- | --- | --- | --- |
| SJR 10 | senate | 2026-06-03 | `pass_301` | Adopted | 22-9 |
| SJR 10 | house | 2026-06-10 | `pass_301` | Adopted | 62-30 |
| SJR 5 | senate | 2025-02-12 | `intro_108` | Adopted | 31-1 |
| SJR 5 | house | 2025-02-12 | `imm_consid_370` | Resolution for Immediate Adoption - Agreed To | 68-28 |

This matters: **SJR 10 is "CA: Require identification to vote"**, a
proposed constitutional amendment adopted on divided votes in both
chambers. It is exactly the kind of vote the campaign exists to record,
and today it cannot be judged. `hjr`/`sjr` are already in
`OHIO_KEPT_MEASURE_TYPES`, so only the code vocabulary is missing.

**But the obvious one-line fix is wrong.** `SJR 5` confirms the
appointment of James Tressel as Lieutenant Governor — a confirmation
vote, which the plan puts out of scope alongside nominations. It is
currently excluded by accident (unknown code) rather than by rule. Adding
`pass_301`/`intro_108`/`imm_consid_370` as kept classes without more
would make a confirmation vote queueable.

Suggested shape, for whoever picks this up:

- Add `pass_301` → `passage` (the joint-resolution analog of `pass_300`).
- Treat `intro_108` and `imm_consid_370` as their own class or leave them
  unknown; both appear here only on the confirmation resolution, and one
  sample is too thin to generalize from.
- Exclude confirmation resolutions explicitly rather than incidentally.
  The signal is in the title (`Confirm the appointment of …`), which the
  fetcher does not currently read; a title-based rule would be the first
  place this pipeline judges on free text, so it deserves its own think.

Until then these four rows sit `pending` with `is_floor_vote = null`,
which is safe — just incomplete.

## 2. The crosswalk proposer misses exactly the members most easily confused

Ohio's roster feed does not always put a plain surname in `lastname`:

- For the twelve members who **share a surname with a colleague**, it
  carries a disambiguated form — `"Hall, D."`, `"Hall, T."`, `"Miller,
  J."`, `"Thomas, C."`, `"White, A."`, and so on.
- One failure is on OUR side, not the roster's: Sarah Fowler Arthur's
  roster `lastname` is the full `"Fowler Arthur"` (display name "Sarah
  Fowler Arthur"), but our candidate row holds the shorter "Sarah
  Fowler", so the surname tail cannot match. That is an ordinary
  name-variant miss like Mike/Michael Dovilla — no lastname-cleanup fix
  reaches it.

`proposeOhioCrosswalk` requires the roster `lastName` tokens to be the
tail of the candidate's name tokens, so all thirteen fail to propose. The
2026-08-24 hand sweep mapped them by hand (see `crosswalk.json`).

The cost is proposal quality, not correctness — nothing wrong was
written, and the reviewer is supposed to be the authority. But the
matcher is silent precisely where two people are easiest to mix up
(Derrick vs Thomas Hall; four Millers; three Thomases), which is the
opposite of the help it should give.

Fix: strip a trailing `, X.` disambiguator from `lastname`, and/or match
against `displayname`, which is clean in every observed row. Keep the
unique-in-both-directions rule — with clean surnames it is what stops
"Rachel Baker" from being proposed for "Stacie Baker".

## Not findings (the code behaved correctly)

- **HB 184** has two `msg_507` House concurrence votes on 2025-11-19. The
  preflight rejected BOTH, stored neither, and exited 1 — the wild case
  the same-day collision rule was written for, confirmed on real data.
- 489 committee actions were rejected before the queue, including
  conference-report votes that carry a committee name, with zero
  misclassifications.
