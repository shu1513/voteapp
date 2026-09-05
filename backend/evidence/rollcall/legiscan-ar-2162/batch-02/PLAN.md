# Arkansas batch-02 — elections, rights and the two ballot amendments

16 measures, 21 roll calls, 893 records across 96 candidates. Local database only;
production holds no Arkansas roll-call records.

Part of the run that finished the 2025 Regular Session. Every gated measure in the session is
accounted for in `../survey/DISPOSITIONS.md`.

## What is in it

**The citizen ballot-initiative fight.** Arkansas passed a cluster of acts in 2025 changing how
citizens put a measure on the ballot. Batch-01 took three of them; this batch takes the other
eight and splits them on a stated line.

Five score `election_integrity` for, because each tests the honesty or accuracy of the petition
transaction: SB 208 (the canvasser must check photo identification), SB 209 (a canvasser found to
have broken the canvassing or fraud laws has all their signatures thrown out), SB 210 (the signer
must read the ballot title in front of the canvasser), SB 551 (the same fraud rules for local
alcohol petitions), SB 584 (the statewide verification rules extended to county and city
petitions). That follows Florida HB 1205, where the same reasoning was tested from enacted text.

Three take no stance, because they limit who may take part or for how long rather than testing
the transaction: HB 1221 (signatures expire at the next general election), HB 1222 (the Attorney
General may reject a measure conflicting with federal law, and a sponsor may file only one
wording), HB 1574 (only Arkansas residents may be paid to canvass).

**Voting access, both directions in one batch.** HB 1878 requires early voting in cities over
fifteen thousand people (`civil_rights` for); SB 296 confines the county clerk's early voting site
to the county seat and makes any extra site need a fresh unanimous vote each election
(`civil_rights` against). Voting access goes to `civil_rights`, not `election_integrity`, which
is reserved for security and accuracy.

**Rights.** HB 1365 strips race and sex requirements from about twenty state boards
(`civil_rights` against, the companion to batch-01's SB 3). SB 433 requires the Ten Commandments
in every public school and college classroom (`civil_rights` against). SB 223 lists religious
rights students and staff keep at school, mostly on equal-treatment terms (`civil_rights` for —
the Texas SB 965 shape, not the Connecticut SB 11 shape, because no waiver of Establishment
Clause claims rides with it). HB 1428 caps bed height in accessible hotel rooms (`civil_rights`
for).

**The November 2026 ballot.** SJR 11 would widen the state constitutional right to keep and bear
arms (`gun_control` against). SJR 15 would create economic development districts (no stance).

## Two things a reader should know about these two amendments

They are **not law**. A joint resolution referring a constitutional amendment never becomes an
act, so LegiScan carries only the superseded draft; the adopted prints came from the legislature
directly and their "As Engrossed" headers match every adopted amendment. Their descriptions say
they go to voters and never say they became law.

They carry an **expiry**. Once Arkansas votes in November 2026, a description written in the
conditional stops being true. Both must be revisited after that election, the rule Missouri's
HJR 3 established.

## Version and date checks

Every roll used is its chamber's last kept floor vote, which in Arkansas is always the vote cast
on the text that became law. All 21 dates match a passage, concurrence or adoption line in the
bill's own history.

**HB 1878 is the one that needed an acknowledgment, and it is worth reading.** The Senate voted on
it three times on 2025-04-16. It failed 15-16, the Senate expunged that vote and passed it 18-17,
a sounding of the ballot then made it fail, the Senate expunged that vote too, and it finally
passed 18-16. The last of the three stands, and it is the one judged. The other two are listed in
`acknowledge_later_rolls` with that explanation, because the superseded-stage gate cannot order
votes taken on the same day.
