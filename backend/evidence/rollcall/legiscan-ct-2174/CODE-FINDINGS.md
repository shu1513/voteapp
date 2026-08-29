# Connecticut — code findings

## 1. Joint-committee chamber code — FIXED in the config PR

LegiScan prints `chamber: "J"` on Connecticut's joint standing committee tallies (1,774 of session
2174's 2,625 roll calls). `parseLegiscanRollCall` refused all of them ("chamber is not H, A or S"),
so two thirds of the feed surfaced as parse errors and the survey exited 1.

Fixed by `isLegiscanCommitteeChamberRollCall` / `LEGISCAN_COMMITTEE_CHAMBER_CODES`, checked BEFORE
parsing: the fetcher counts such rolls into `committeeVotes` and stores nothing, and the survey counts
them into a new `committeeChamberVotes` field while keeping them out of the desc histogram.
`parseLegiscanRollCall` deliberately still throws on `J` — recognizing the code is not trusting it.

## 2. The proposer ignores LegiScan's `nickname` field — RECORDED, NOT FIXED

`proposeLegiscanCrosswalk` matches `person.firstName` (LegiScan `first_name`, the legal name) against
the candidate's first token. LegiScan also ships a `nickname` field, and Connecticut rosters members
under the nickname: `first_name: "Kathleen", nickname: "Kathy"`, `"Lucia"/"Lucy"`,
`"Katherine"/"Tina"`, `"Mary"/"Renee"`, `"Michael"/"MJ"`, `"Christine"/"Cara"`, `"Emmanuel"/"Manny"`.
Seven of CT's 17 name-variant hand-adds are exactly this, and every one would have matched on
`nickname`.

The naive fix — try `nickname` as a second first-name candidate — is probably right but is NOT free:
the proposal rule requires uniqueness in both directions, so a second name per person can only create
new collisions, and the effect has to be measured on the states already registered (TX/FL/GA/IL/TN/CA
crosswalks are committed and must not silently change) before it is written. Not attempted here; the
17 hand-adds are cheaper than an unmeasured change to six live crosswalks.

The rest of the CT hand-adds are the known classes: not-a-prefix pairs (`nick` is not a prefix of
`nicholas`; `joe` is not a prefix of `joseph`) and multi-part surnames the roster shortens.

## 3. The Senate question is unrecoverable from the feed — a SELECTION rule, not a code fix

All 438 CT Senate rolls carry the same desc shape, so no config pattern can separate a passage from a
floor amendment (see README). This is Florida's finding recurring, and like Florida it belongs in
selection rather than in the classifier: the desc genuinely does not carry the fact.

Worth knowing if it is ever automated: within one chamber and bill, the printed vote numbers run in
the same order as that chamber's actions on the bill-status page, one-to-one — so the trail can be
aligned to the rolls mechanically. The decisive passage roll is the last `passed:1` roll by printed
vote number. Both were verified by hand on every roll in batch-01, and the alignment held on HB 7042's
19 Senate rolls (18 amendments A-R, then passage).

## 4. `roll_call_id` is not issued in time order in CT — no action needed

Within a same-day Senate batch, LegiScan issues ids in reverse of the printed vote numbers (SB 3:
id 1572265 = vote 182, id 1572263 = vote 184). The fetcher's duplicate-identity collapse keeps "the
lowest id of each identity group" on the stated assumption that ids ascend with time. CT has **zero**
duplicate identity groups, so nothing was affected here — but the assumption is not universal, and a
state with both re-issued ids and reversed ordering would keep the wrong member of a group.
