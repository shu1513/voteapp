# Washington batch-01

Five measures, ten roll calls, 530 records across 108 candidates. Local database only.
Production holds no Washington records.

## What is in the batch

| measure | chapter | area | direction | House | Senate |
| --- | --- | --- | --- | --- | --- |
| House Bill 1052 — hate crime | 249 L 25 | civil_rights | for | 59-38 | 30-19 |
| House Bill 1155 — noncompete ban | 149 L 26 | corporate_accountability | for | 62-33 | 30-19 |
| House Bill 1163 — permit to buy a firearm | 370 L 25 | gun_control | for | 57-39 | 29-19 |
| House Bill 1217 — rent increase limit | 209 L 25 | housing_affordability | for | 54-44 | 27-20 |
| House Bill 1604 — jail search standards | 17 L 26 | civil_rights | for | 56-39 | 30-19 |

All five are divided in BOTH chambers, none sits on a partially vetoed act, and none has
a same-day peer. Every measure is House-heavy by design: a House roll reaches 87
candidates and a Senate roll 19.

**Washington mirrors Texas, as expected of a trifecta.** House Bill 1163 is
`gun_control`/**for** under the same rule that made the Texas firearms bills score
against: direction follows the research area's own description, never the bill's framing.

## How the rolls were chosen

Filter 4 takes each chamber's LAST kept floor vote on the measure, ordered by DATE,
because Washington's roll call ids do not ascend with date.

Every choice was then confirmed against the Final Bill Report's own
"Votes on Final Passage" list, which prints the whole sequence. That is a third
independent check, after the feed and the bill history:

- **1052, 1155, 1163** — House passed, Senate amended and passed, House concurred. The
  concurrence is the House's vote on the enacted text, and the Senate's own vote is on the
  version it wrote, which is what became law. Both picks are correct.
- **1217 went to a conference committee.** The House passed it, the Senate amended it, the
  House REFUSED to concur, and a conference committee produced the version that became
  law. Both picks are the conference report votes, so both chambers are recorded on the
  enacted text. The earlier House vote of 53-42 is a different question and is not in the
  batch.
- **1604** took one vote in each chamber with no amendment, so there is nothing to choose.

No judgment needed `acknowledge_later_rolls`.

## Effective dates, which the descriptions carry

Two of the five are enacted but **not yet in force**, and saying otherwise would be
wrong: House Bill 1155 takes effect on 30 June 2027 and House Bill 1163 on 1 May 2027.
Their descriptions use "will require" and name the date. The other three are in force.

## 56 duplicates retired BEFORE the import

Washington already had hand-researched vote records covering these bills, and the
importer's URL-based duplicate check could not see them because they cite the bill page
rather than the roll call. A precise sweep — the record must name the bill number AND the
exact tally of one of the ten rolls — found **56 live hand-written records describing the
very votes this batch imports**, on House Bills 1217, 1052 and 1163. All 56 were retired
before the import, each with a reason naming the measure and the tally.

Records about a DIFFERENT vote on the same bill were deliberately left alone. The clearest
case is House Bill 1217, where several records describe the earlier 53-42 House passage,
which is a distinct question from the 54-44 conference vote this batch imports.

The 2026 measures (1155, 1604) had no hand-written coverage at all.

## Reconciliation

- Dry run planned 530 inserts; the real run inserted 530; the database holds 530 under the
  run stamp `2026-09-10T02:36:21.036Z`. Reconciled three ways.
- The dry run's own stamp matches **zero** rows, which is the positive proof that
  `--dry-run` writes nothing.
- Convergence re-run: all 530 `unchanged`.
- **108 candidates = every member the crosswalk maps.** Washington's Speaker votes, so
  there is no shortfall of the kind Texas and Georgia have.
- 375 area tags, and the arithmetic matches the label design exactly: `gun_control` is the
  only measure with an authored nay stance and the only area carrying `against` tags
  (66 for, 39 against). The four measures with `nay: null` tagged yea voters only.

## Writing

Built with one body per measure, with the yes and no sentences generated from it behind
different opening clauses, so the two cannot drift apart. The builder asserts the period
join, the absence of a comma splice, the tally in both sentences, a 45-word sentence cap
and an explicit British-spelling word list, and it was proved to REJECT a known-bad string
before being trusted.

Reading level was measured separately, because the 45-word lint is not a readability
check: **Flesch-Kincaid median 8.9, worst 9.8, longest sentence 36 words.** Grade 9 is the
honest floor for statutory text, as every other state in this campaign has found.

## Held for batch-02, with reasons

- **House Bill 1409, clean fuels.** The strongest candidate to read next, but it needs a
  full read rather than a quick one: it tightens the carbon intensity schedule, and in the
  same act replaces the Clean Air Act's civil and criminal penalties with a programme of
  its own and brakes further tightening until a biofuel facility is permitted. Whether
  those counter-strands are de minimis is a judgement that has to be made from the text.
- **House Bills 1462, 1296, 1491 and 1232** are read-ready and unread.
