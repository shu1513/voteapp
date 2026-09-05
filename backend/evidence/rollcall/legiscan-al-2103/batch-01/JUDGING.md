# Alabama 2024 batch-01 — judging notes

## Sources

Every measure was judged from its **enrolled Act**, read top to bottom, fetched through the LegiScan
bulk API and verified against the byte length and MD5 hash the dataset records for that document.
The Alabama site itself was returning SSL handshake timeouts for long stretches while this work ran,
and one direct download earlier in the campaign returned HTTP 200 with a truncated, unreadable PDF —
so the hash check is not a formality.

Alabama prints struck and inserted text together and `pdftotext` flattens both; the convention,
verified against several Acts, is struck text first and inserted text second.

## Roll-attribution check

All 29 divided rolls were checked against their own bill's history. **29 of 29 pass.** The 2026
session's misfiling defect does not exist here.

## The caption problem, and why it decides this batch

This session prints two naming systems at once, and one string means opposite things two sessions
apart: `Passed House Of Origin` is the **passage vote** in 2024 and a Budget Isolation Resolution in
2025. Reading it the 2025 way would have hidden 176 real passage votes and cut this pool from 10
divided-and-enacted rolls to 4 — losing the CHOOSE Act, the absentee voting law and the diversity
programme ban, three of the session's most contested measures. The full reasoning and the evidence
that settles it are in `../CODE-FINDINGS.md`.

## Date audit

All 9 rolls match the bill history line recording the same action: 9 of 9 exact.

## Version checks and supersession

- **SB 231**: the imported House roll is the vote on the conference committee report, which is the
  enacted text. The earlier House passage vote of 2024-04-23 is a different question and is
  dispositioned as superseded.
- **HB 129, HB 22, SB 1, SB 129, SB 186**: each imported roll is its chamber's final kept floor vote,
  with no later kept roll in that chamber. Nothing needs `acknowledge_later_rolls`.

## Label reasoning

Every stance label states `nay` explicitly, and every one is `null`.

- **SB 1 — election_integrity, yes = for.** The Act makes it a Class C felony to take payment for
  handling another voter's absentee ballot application, and a Class B felony to pay someone to do
  it; it also bars turning in another voter's completed application outside a medical emergency.
  These are the sharpest penalties of any absentee measure in this corpus, and the descriptions say
  so plainly. The **direction** follows what the campaign has already applied to Ohio SB 293,
  Montana HB 719 and Montana SB 105: tightening absentee handling is scored `for` election
  integrity, and the access objection — that it deters help for elderly, disabled and rural voters —
  runs on a different axis, so nay is null. The Act's own carve-out for voters who are blind,
  disabled or unable to read is stated in the description so a reader can weigh it.
- **SB 129 — civil_rights, yes = against.** The Act closes diversity, equity and inclusion offices
  and programmes at state agencies, school boards and public universities, bars compelling assent to
  a listed set of ideas about race, sex, religion and national origin, and requires public
  universities to label shared restrooms by biological sex. That is a withdrawal of
  anti-discrimination machinery, which is the area's own subject. The descriptions also state the
  Act's carve-outs — even-handed classroom teaching, research, accreditation, and student-run events
  without state money — because a reader should see the limits as well as the ban.
- **HB 22 — public_education_quality, yes = for.** The Act defines the assistant principal's role and
  says every public school shall have one where funding allows, and requires each local board to
  write and share a student discipline policy. Adding school leadership capacity and a written
  discipline standard sits inside "effective teaching, standards, funding, and accountability".
  The objection is cost, a different axis.

## The three no-stance imports

- **HB 129 (CHOOSE Act).** This is the flagship school-choice law: a refundable tax credit of $7,000
  per child at a participating private school, or up to $4,000 for home schooling, paid into an
  education savings account, with an income cap for 2025 and 2026 and none from 2027, and at least
  $100 million appropriated a year. It is high salience and squarely divided. It gets no stance for
  the same reason as HB 363 and SB 263 in the 2023 batch: `public_education_quality` is the only area
  that could hold it, and the direction there is contested. **The deciding point is what the Act can
  establish.** It creates a new fund and a new credit; it does not cut a public-school appropriation
  by its own terms. Whether it crowds out public-school funding depends on future budgets and cannot
  be read off the text, only argued. A contested direction gets no stance.
- **SB 186 (ranked-choice voting ban).** High salience and plainly an elections measure, but banning
  a counting method does not make elections more or less "secure, accurate, auditable". The case for
  scoring it `for` election integrity rests on the sponsors' framing that ranked counting is harder
  to follow, and this campaign takes stance direction from the research area's description, never
  from the bill's framing.
- **SB 231 (union recognition and business incentives).** An employer loses eligibility for state and
  local incentives if it recognises a union on signed cards alone, or shares a worker's contact
  details with a union without consent, and must repay incentives already taken. There is no labour
  area in the taxonomy. `reduce_wealth_gap` was considered and rejected: the Act does not touch
  wages, bargaining rights or union legality, only the conditions attached to a public subsidy, so
  reading it as a wealth-gap measure would be an inference about downstream effects rather than a
  reading of the text.

## Duplicates

The precise sweep found **18 true duplicates**, all retired before the import
(`duplicate-retirements.json`, to re-run at production promotion): hand-written records on HB 129,
SB 129 and SB 1 for Chip Brown, Chris Pringle, Margie Wilcox, Mark Shirey, Philip Ensler and Shane
Stringer.

Records describing a *different* vote on the same bill were left alone, as in the 2026 batch: one
for Marilyn Lands on SB 231's House passage of 2024-04-23, where this batch imports the conference
report vote of 2024-05-07.

## Import and reconciliation

- Dry run: 9 files, 0 errors, 589 planned inserts.
- Real run (stamp `2026-09-02T16:44:10.034Z`): **589 inserts, 0 errors, 0 notified.**
- Reconciled three ways: report totals (589); run-stamp predicate (589 rows, 117 distinct
  candidates); and the session total, 1,519 records carrying a 2103 run id, matching 589 + 930.
- Convergence: a follow-up dry run reports all 589 `unchanged`. **The first convergence run reported
  one error** — `citation URL fetch timed out` on SB 186 — which is the importer checking the
  citation URL over the network, not a data fault. A re-run came back clean.

## Writing checks run before import

`candidateRecordPlainLanguageLint`: 0 warnings over 18 descriptions, all 4 sentences, no sentence
over 45 words, British-spelling scan clean. Reading grade was measured and then acted on: a first
draft came in at a median Flesch-Kincaid 12.6 with SB 129 at 14.4, which was too heavy, so the
descriptions were rewritten with plainer words and shorter clauses. Final median 10.9, worst 12.3.
