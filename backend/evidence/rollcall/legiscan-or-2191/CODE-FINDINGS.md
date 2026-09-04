# Oregon (LegiScan 2191) — findings

Recorded, not fixed. Each one is a fact about the feed or the source that a
later Oregon batch, or another state, has to know.

## 1. `Repassed` is two different questions and the desc cannot tell them apart

Oregon prints `House Repassed` / `Senate Repassed` for two unrelated
questions:

- **Repassage after a conference report.** The chamber adopts the report and
  repasses the bill in one recorded vote. Oregon's own history spells it out:
  "Senate adopted Conference Committee Report and repassed bill" (SB 916,
  HB 3127, HB 2614, HB 3694).
- **A veto override.** SB 875's Senate roll of 2025-06-25, 21-6, is the motion
  to repass the bill notwithstanding the Governor's veto under Article V,
  section 15b of the Oregon Constitution.

Nothing in the description separates them; only the bill history does. The
config records the majority case as `conference_report`, which is safe here
because `questionClass` is report metadata and is never persisted, and
because SB 875 was vetoed and stayed vetoed, so it never reaches the
enacted gate. **Read the history before describing any Repassed roll.**

The House takes TWO rolls on a conference report — `House Adopted Conference
Comittee Report` and then `House Repassed`, the same day and often on the
same tally (SB 916 was 35-22 twice). The repass is the chamber's final action
on the enacted text, so selection prefers it and acknowledges the same-day
adopt roll. The Senate folds both into one `Senate Repassed`.

## 2. The feed misspells "Committee" in one floor question

`House Adopted Conference Comittee Report` — one `m`. Spelled correctly, the
pattern matches nothing. It is also why a naive `committee` exclusion does
not accidentally swallow the conference-report vote.

## 3. No constitutional amendment can be queued from this feed

Oregon refers constitutional amendments to the voters by JOINT resolution.
Type `JR` is in `LEGISCAN_KEPT_BILL_TYPES`, and the dataset holds 56 of them
— but **not one carries a recorded floor vote**. So the measures that reach
Oregon's ballot are unreachable here.

This is NOT the Georgia gap, where the resolution type itself was dropped
before the config was read. Here the type is kept and the rolls simply do not
exist in the dataset. Anyone wanting Oregon's referred amendments has to go
to the state's own record, not LegiScan.

## 4. `role` contradicts `district`, so seat logic must read `district`

Oregon's people file prints senators with `role` "Rep"/"house" beside an
`SD-` district (for example Mike McLane, `SD-030`, printed as house). The
campaign rule already says to trust `district` and never `role` — Texas found
the same thing with Phil King — and Oregon is another confirmation.

## 5. The crosswalk proposer misses names it has in hand

Six of the nine hand-adds are members whose LegiScan `name` or `nickname`
field already holds the ballot name, but `proposeLegiscanCrosswalk` reads
neither: it compares `first_name` (the legal name) against the candidate's
first token. Several of these pairs have a LegiScan `name` **byte-identical**
to the candidate name and were still missed.

`"thomas"` is not a prefix of `"tom"`, so the prefix rule cannot rescue them
either — the same shape as `"nick"`/`"nicholas"` in Connecticut and
`"thomas"`/`"tom"` in Montana. Pennsylvania, Connecticut, Kentucky, North
Carolina, Indiana, Montana and now Oregon have all paid for this. The fix
(read `name` and `nickname`, and the final token of a multi-part `last_name`)
is deliberately NOT made here, because it changes the proposals of every
committed state and those would all need re-measuring first.

## 6. LegiScan's Oregon text dates are all `0000-00-00`

Every entry in a bill's `texts[]` carries `"date": "0000-00-00"`. The version
stack is still exact, but it must be read from the version NAMES (Introduced,
House Amendments to Introduced, A-Engrossed, Senate Amendments to
A-Engrossed, B-Engrossed, Enrolled) and their array order, never from a date
comparison. A version check that compares a roll's date against a text date
would silently compare against the epoch.
