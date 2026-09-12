# Wisconsin batch-12: the died pool, everything else

Judged 2026-09-11. Local database only; production holds no Wisconsin roll-call
records. No AI calls.

The last of three batches for the bills that passed one chamber on a closely
divided roll and never passed the other before the session's last floor period
ended. See batch-10 JUDGING.md for how the pool was defined. **With this batch,
all 46 measures in the died pool have a written disposition.**

## Kept: 10 of 18

| measure | what it would have done | area | yes means |
| --- | --- | --- | --- |
| AB 66 | judge approval to drop charges for listed crimes; no deferred prosecution for them | public_safety_and_crime_control | for |
| AB 377 | English as the official language; machine translation in place of interpreters | civil_rights | against |
| AB 380 | no pay for judges suspended over criminal misconduct | anti_corruption | for |
| AB 58 | only U.S., state and agency flags on public buildings | civil_rights | against |
| AB 617 | absentee ballot fixes, text alerts, earlier mail deadline, no central counting of polling-place ballots | election_integrity | for |
| AB 840 | rules for the largest data centers on utility costs, water and cleanup | environment_and_public_health | for |
| AB 840 | the same bill: data center costs kept off other customers' bills | cost_of_living_reduction | for |
| AB 963 | age checks, parental consent and privacy defaults for minors on social media | data_privacy | for |
| SB 94 | riot felonies and lawsuits against rioters and their backers | public_safety_and_crime_control | for |
| SB 384 | care and penalties for children born alive after an attempted abortion | womens_reproductive_rights | against |
| SB 394 | a felony for damaging historical monuments on public property | public_safety_and_crime_control | for |

Every no side is `null`.

**Earlier rulings followed.** AB 58 follows Idaho's flag act (civil rights,
against). SB 94 follows the Anti-Aggravated Riot Act (public safety, for).
SB 384 follows Montana HB 491, Gianna's Law (reproductive rights, against).
AB 840's water rules follow Utah SB 259 (environment, for).

**New calls, with the reason:**

- **AB 377** limits government communication in languages other than English
  and lets agencies use machine translation in place of an interpreter. For
  people who speak little English, that narrows access to fair treatment, so
  `civil_rights` yes = against. The bill's exceptions, for health, safety and a
  defendant's rights, are in the description.
- **AB 380** targets judges charged with criminal misconduct, which is the
  "enforcement" in `anti_corruption`. The amendments that added a probable-cause
  hearing and back pay for a cleared judge are in the description.
- **AB 617** is mostly about ballot tracking, fixing envelope errors and
  counting at the polls, which is the "accurate, auditable, and trusted" part of
  `election_integrity`. The earlier mail deadline is named so a reader sees it.
- **AB 840** carries two labels, as AB 1027 did in batch-06, because its two
  strands act on different things: water and cleanup for the environment, and a
  rule that no other customer pays for a data center's power.
- **AB 963** bans targeted ads and requires the most private settings for
  minors. That is the "limits on collection, sharing, and misuse" in
  `data_privacy`. Its age estimation relies on data the platform already holds.

## Dropped: 8

- **AB 625** would have held back part of each homelessness grant until the
  provider showed results. It reads both ways in `social_programs_and_welfare`:
  a push for results, or less money for shelters up front.
- **AB 640**, a maximum age of 75 for newly elected or appointed judges. No
  research area describes judicial terms of office.
- **AB 683**, as voted, joined a ban on telecom equipment from foreign
  adversaries to campaign finance rules. It bars foreign money in referendum
  campaigns, but also shields donors to tax-exempt groups from investigators.
  The defense part points one way and the campaign finance part points both
  ways, so no clean label exists.
- **AB 961**, warning labels on explicit online content. No research area
  describes it.
- **AB 962**, age checks and parental consent at app stores. It limits some
  data use but requires age verification for every account, so it reads both
  ways inside `data_privacy`.
- **SB 111** would have made counties transport minors held for emergency
  mental health care, using police only as a last resort. It is mostly a cost
  and duty assignment with no clear direction in any area.
- **SB 553** would have excluded some life-saving procedures, such as treating
  an ectopic pregnancy, from the definition of abortion. It can be read as
  protecting emergency care or as narrowing it with new conditions, so it reads
  both ways in `womens_reproductive_rights`.
- **SB 665**, a statewide wolf population goal. Wildlife management is not in
  the environment area's definition of air, water, climate and community health.

## Duplicate retired

A hand-written record, `f4d6e23e`, said a member "voted against Assembly Bill
840 on January 20, 2026; it regulated utility costs linked to data centers".
That is the same vote as roll 1614720, and it wrongly implies the bill became
law. It came from the same hand-written research run as the AB 226 duplicate in
batch-10, and that candidate's other hand-written records cover different bills.
It is retired in `duplicate-retirements.json`, which must be applied again at
promotion. The imported record `a493329b` replaces it.

## Reconciliation

- Plain-language lint over 20 descriptions: 0 warnings, longest sentence 40
  words.
- Judge: 10 updated. Import dry run: 634 inserts. Real run at
  `2026-09-11T21:20:36.675Z`: `outcomes {imported: 10}`, `actions {insert: 634}`,
  0 notified.
- 334 yes-side records carry 378 tags, because AB 840's yes side carries two.
  300 no-side records, no tags.
- All live Wisconsin roll-call records: 6,748 over 125 rolls and 99 candidates,
  with 4,136 tags. That includes batches 10 and 11, each on its own pull request.

## Review fixes, after the first import

Two findings from external review, both checked against the substitute
amendment each chamber voted, and both real.

- **AB 617 (Substitute Amendment 2).** The description said cities "could no
  longer have counted ballots at one central location instead of at the polls".
  The substitute repeals the s. 7.51 option to adjourn the polling-place canvass
  to a central count, but leaves s. 7.52, the separate central counting of
  absentee ballots, in place and still cross-references it. A reader who knows
  Milwaukee's absentee central count would have taken the old sentence to mean
  it ended. Now says polling-place ballots, and that absentee central counting
  could have continued.
- **AB 840 (Substitute Amendment 1).** The description said "any renewable
  power plant serving them" had to be on site. Both the introduced bill and the
  adopted substitute, s. 196.492 (2), restrict only a facility that "primarily
  serves the load" of a large data center. Now says a plant that mainly served
  them.

Fix path: `build-judgments.py` -> `judgments.json` -> `rollcall:judge` ->
`rollcall:legiscan:import`. Dry run predicted `rewrite 172, unchanged 462`; the
real run at `2026-09-12T03:21:33.442Z` did exactly that (86 records on each
bill), 0 errors. Convergence dry run afterwards: 634 unchanged
(`import-dry-run-rerun-report.json`). Lint over all 20 descriptions: 0
warnings; longest sentence still 40 words.

## The died pool, closed

| batch | strand | read | kept | rolls | records |
| --- | --- | --- | --- | --- | --- |
| batch-10 | education | 14 | 8 | 8 | 614 |
| batch-11 | economy and taxes | 14 | 4 | 4 | 264 |
| batch-12 | everything else | 18 | 10 | 10 | 634 |
| total | | 46 | 22 | 22 | 1,512 |
