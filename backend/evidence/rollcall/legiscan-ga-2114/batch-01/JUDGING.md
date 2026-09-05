# Judging notes, Georgia 2023 special session, batch 01

Six roll calls, three measures, both chambers each. All three became law on
8 December 2023 as Acts 1EX, 2EX and 3EX.

## What each chamber actually voted

Checked before any description was written, because a roll call has to be
described from the document that chamber had in front of it.

- **HB 1**, the state House map. Texts in the feed: Introduced 2023-11-30 and
  Enrolled 2023-12-06. No substitute and no engrossed print, and the history
  records plain `Passed/Adopted` in both chambers, so both chambers voted the
  same text. Described from the Enrolled print (doc 2861155, 443,649 bytes,
  byte length and MD5 verified against the dataset manifest).
- **SB 1**, the state Senate map. The Senate engrossed it on 2023-12-01 and the
  House passed it unamended on 2023-12-05. The feed carries no Enrolled print
  for this bill, so the Engrossed print is the last text and is what both
  chambers voted (doc 2861538, 254,532 bytes, verified).
- **SB 3**, the congressional map. The Senate passed it by substitute on
  2023-12-05 and the House passed it unamended on 2023-12-07, after which it
  was enrolled the same day. Both chambers therefore voted the Senate
  substitute. Described from the Enrolled print (doc 2862161, 183,748 bytes,
  verified), which is 474 bytes smaller than the engrossed print it came from.

No vehicle-bill trap here: each of the three bills was introduced as a
redistricting map and stayed one.

## Supersession

The House vote on SB 3 (roll 1362081, House Vote #19, passage) shares its day
with roll 1362080, House Vote #18, `Agree To Committee Report`, 97-71. The
gate counts same-day peers and flagged it. Vote #18 precedes vote #19 on the
same day, so the passage vote is the final action, and 1362080 is listed under
`acknowledge_later_rolls` with that reason. No other roll tripped the gate.

## Labels

All six carry `civil_rights`, `yea: "for"`, `nay: null`.

The reasoning: a federal court had ruled the 2021 maps unlawful under federal
voting rights law, and these three bills are the replacement maps, each written
to apply only if the 2021 map cannot lawfully be used. Passing them is a
for-civil-rights action on the face of the record.

`nay` is deliberately null rather than `"against"`. The members who voted no
argued the new maps met the court's count while breaking up other districts
where minority voters had been electing their candidates of choice. Tagging
those members as against civil rights would misstate the record. Under the
explicit-nay contract, no voters get no tag on this slug.

## Descriptions

Each cites its own roll call's tally, states that the map replaced the 2021 map
after a federal court ruling, states the contingency, and notes that sitting
members keep their seats until the next regular election, which the bill
captions provide for. Plain-language lint: 12 descriptions, 0 warnings, median
Flesch-Kincaid grade 7.8, worst 8.2.

The descriptions do not state how many new majority-Black districts each map
drew. That number comes from the court order, not from the bill text in hand,
so it is left out rather than asserted.

## Duplicates

Swept the 211 candidates who received records for any non-roll-call record on
the same measure and date. 0 found.
