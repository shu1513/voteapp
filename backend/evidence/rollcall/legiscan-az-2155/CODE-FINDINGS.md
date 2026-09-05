# Arizona code findings

Recorded, not fixed. Each one is either a property of the feed that the pipeline cannot repair
or a change that would affect every state already committed.

## 1. Ballot referrals cannot be queued, because Arizona sends them as concurrent resolutions

Arizona refers a measure to the voters as a **concurrent resolution** (HCR or SCR), which
LegiScan types `CR`. `LEGISCAN_KEPT_BILL_TYPES` is `["B", "JR", "JRCA", "CA"]`, and the type
check runs before the state config is read, so every one is rejected as `excluded_measure`
before any pattern is consulted.

Measured cost in this session: **48 divided floor votes on 36 concurrent resolutions**,
including referrals headed for the November 2026 ballot — for example SCR 1004 (prohibit a tax
on vehicle mileage) and HCR 2021 (municipal food tax exemption), both of which reached the
ballot.

This matters more in Arizona than elsewhere. With a Republican legislature and a Democratic
governor who vetoed 174 measures in one session, a referral to the voters is the majority's
route around the veto, so the concurrent-resolution pool is where some of the session's most
consequential divided votes live.

The naive fix — adding `CR` to the kept types — is wrong for the same reason it was wrong in
Georgia. 102 of this session's 1,854 measures are concurrent resolutions and many are purely
ceremonial (`Law enforcement; first responders; honoring`, `Sovereign authority`), so keeping
the type wholesale would queue commendations. Arizona gives no separate wording to distinguish
them: a referral and a commendation both pass on a plain `House - Third Reading`.

A per-state opt-in for `CR`, combined with the existing requirement that a measure be divided
and consequential, would recover the pool. It would need re-measuring against Georgia before it
could ship.

## 2. Arizona publishes no member list for a concurrence vote

All 82 `House - Concurrence`, all 72 `Senate - Concurrence` and all 42
`House - Reconsider Third Reading` roll calls arrive with an empty `votes` array and a zero
tally, so `fetchLegiscanRollCallVotes` skips each as an unrecorded vote before classification.

This is not a pipeline defect — there is nothing to fan out — but it is not a feed gap either,
because the vote is public. Arizona's own bill overview page prints the tally under
`Senate FINAL`, and the House Bill Summary header reprints it. Only the member list is missing.

The effect is on selection rather than on code, and it is described in the README and in the
config comment: when the second chamber amends a bill, the originating chamber's vote on the
enacted text cannot be imported, so its earlier divided roll is dropped.

The config keeps both concurrence spellings as **kept** questions rather than excluding them,
so that if a later Arizona dataset fills in the voters those rolls enter the queue instead of
being dropped by a rule written when the feed was thin.

## 3. A failed third reading is stored under the same caption as a successful one

Arizona reconsiders a third reading that failed and re-votes it days later, and LegiScan files
both under the plain caption `House - Third Reading`. Nothing in the description separates
them; only the `passed` flag does.

Two measures in the divided-and-signed pool are affected, and both would have been imported as
passage votes by a selection rule that read only the caption and the tally:

- **HB 2518** — House third reading failed 21-35 on 2025-03-13, then passed 51-2 on 2025-03-20.
  Thirty members changed side.
- **SB 1661** — House third reading failed 19-39 on 2025-04-17, then passed 57-0 on 2025-05-05.

Both were dropped as superseded, following the federal 117-1 roll 160 retraction and the
Maryland SB 255 precedent, rather than imported with a caveat. **Check `passed` on every
selected Arizona roll**; the caption will not tell you.

## 4. `seatAgrees` is false for a routine chamber switch, because Arizona nests its districts

Arizona's House District N and Senate District N sit inside the same legislative district, and
its House districts elect two members each. A sitting Representative running for the Senate
therefore keeps the same seat number while changing chamber, and
`proposeLegiscanCrosswalk`'s seat check returns false for what is an ordinary and correct
match. Seven of the 53 proposals in this session were false for exactly that reason, and all
seven were confirmed against the candidate record's `current_office`.

This needs no fix — the flag is advisory and a human reviews every proposal — but a reviewer
who treats a false flag as a rejection would discard seven good matches in Arizona alone.

## 5. LegiScan's `first_name` can name a different real person

`proposeLegiscanCrosswalk` matches on `first_name` and `last_name` and never reads `name` or
`nickname`. In Arizona that produced the worst shape this failure has taken so far: not a
missed match, but a confident match on the **wrong human**.

LegiScan person 24498 is the sitting District 6 Representative. Its `name` is `Mae Peshlakai`,
its district is `HD-006`, its `first_name` is the legal `Jamescita` and its `nickname` is `Mae`.
Our database holds a different person, Jamescita Peshlakai, who holds no legislative seat
(current office: Arizona State Transportation Board) and is running for the District 6 Senate
seat. The matcher proposed her, uniquely and at `first_and_last` confidence, while the correct
candidate — rostered as `Mae Peshlakai` in House District 6 — sat in the unmatched list.

The only signal was the seat disagreement, which in Arizona is also produced by every ordinary
chamber switch (finding 4), so the two cannot be told apart mechanically. The suggested fix
recorded in Connecticut and Pennsylvania — read `name` and `nickname` as well — would have
matched the right person here, and is worth re-measuring across the committed states.

## 6. LegiScan's `passed` flag does not know Arizona needs a majority of the whole chamber

Arizona passes a bill on a majority of the seats, not of the votes cast: 31 of 60 in the House
and 16 of 30 in the Senate. LegiScan sets `passed` from a bare comparison of yeas to nays, so a
vote that Arizona's own history records as FAILED can arrive with `passed = 1`.

Measured across the session: **25 third-reading roll calls carry `passed = 1` while falling
short of a constitutional majority**, 16 in the Senate and 9 in the House, and every one is
recorded FAILED in the bill history. Examples: SB 1583 at 15-14, HB 2552 at 30-28, SB 1001's
first House vote at 29-26.

This is the same defect class recorded in Montana (two-thirds votes on constitutional
amendments) and Indiana (`Concurrence defeated`). It is a property of the feed, not something a
description pattern can fix, so nothing here changes it.

**None of the 149 roll calls approved across the four Arizona batches is affected, and the
reason is structural rather than luck.** Selection always takes the *last* divided roll call in
a chamber. A failed third reading in Arizona is followed either by a reconsidered vote that
passes — in which case the later roll is the one selected — or by the bill dying, in which case
it never enters a signed or vetoed pool at all. The check was still run explicitly over every
selected roll and over both worklists, and came back clean.

**A selection rule for any future Arizona session: do not rely on `passed`. Compare the yea
count to the chamber's seat majority.**

## 7. Arizona publishes documents whose filenames contain a space

At least two staff analyses are published under a name with a literal space before the
extension, for example `H.HB2576_020525_VETOED .DOCX.htm`. `curl` will not fetch that
unescaped, and the failure presents as a missing document rather than an error.

This cost a real decision. HB 2112's analysis failed to download during batch-02, the measure
was dropped on reasoning alone, and the recorded reason turned out to be wrong on its own terms
once the document was read — the act answers the privacy objection internally. The outcome did
not change, but the stated grounds did, and they have been corrected on the worklist.

`az_docs.py` now percent-encodes spaces before fetching.
