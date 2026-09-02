# How North Carolina batch-01 was judged

## Sources

Every measure was judged from two documents:

1. The North Carolina General Assembly's own bill summary for the ratified
   version, written by the nonpartisan Legislative Analysis Division and stamped
   `Analysis of: Ratified` in its header. The summaries name a staff attorney,
   set out current law and then the bill section by section, and carry no
   sponsor statement of intent.
2. The enacted session law, which is the ground truth. Where the summary and the
   act could differ, the act wins.

Version check: an override is a vote on the ratified bill, so both chambers voted
the same text and no measure needed a per-chamber version split. This is the one
question class in the campaign where the version check is free.

Struck text: session law PDFs show deletions with a line through them, and
`pdftotext` renders struck and surviving text the same way. Senate Bill 266's
whole stance rests on what was deleted, so page 1 of Session Law 2025-78 was
rendered as an image and read. The struck words are "a seventy percent (70%)"
and "from 2005 levels by the year 2030 and"; "that result in" was inserted. The
2050 goal of carbon neutrality survives.

## Date check

Every roll date was checked against the official record. The session laws
themselves state the override date ("Became law notwithstanding the objections of
the Governor at 11:38 a.m. this 29th day of July, 2025"), and the bill pages
carry the date and time of each vote. All 11 imported rolls match. No
`official_vote_date` override was needed.

## Tally check, and what it caught

Every roll was compared against the official House and Senate roll-call records
on ncleg.gov. Eleven of the fourteen match exactly. The three House override
rolls of 2026-06-24 do not: LegiScan reports 71-46 and the official transcripts
report 71-47, because LegiScan leaves the chamber's unaffiliated members out of
its member list. Those three rolls were withdrawn rather than imported with a
number the state's own record contradicts. The full finding is in
`../CODE-FINDINGS.md`.

This is why `import-report.json` records 1,025 inserts across 14 files while the
live count is 713 across 11: the 312 records of the three held rolls were
retired the same day, with the reason naming the finding, and the rolls were
returned to the review queue. `held-rolls/retirements.json` is the retirement
file. `import-dry-run-rerun-report.json` is the convergence run after the hold:
11 files, 713 records, all unchanged.

## Labels

| Measure | Area | Yes means | No means |
| --- | --- | --- | --- |
| House Bill 193 | gun_control | against | not stated |
| House Bill 318 | immigration | against | for |
| House Bill 805 | civil_rights | against | not stated |
| House Bill 805 | corporate_accountability | for | not stated |
| Senate Bill 266 | environment_and_public_health | against | not stated |
| Senate Bill 153 | immigration | against | for |
| Senate Bill 227 | civil_rights | against | for |
| Senate Bill 558 | civil_rights | against | for |

Direction follows the area's own description, not the bill's framing. The
`immigration` area reads "Welcome immigration through a lawful, orderly, and
humane system", so enforcement bills score against it, exactly as Texas Senate
Bill 8 did and as the mirror-image bills in Illinois, Maryland and Connecticut
scored for it. Banning diversity, equity and inclusion offices scores against
`civil_rights`, following Ohio Senate Bill 1 and the Tennessee and Georgia
batches.

The no side is stated only where the whole act sits inside the area and there is
no other axis to object on:

- House Bills 318 and 153 are immigration statutes end to end, so a no vote is a
  vote against extending immigration enforcement.
- Senate Bills 227 and 558 are anti-discrimination statutes by their own terms,
  and both expressly protect speech the First Amendment covers, which removes
  the free-speech axis a no vote might otherwise be resting on. What is left is
  a civil-rights objection.
- House Bill 193 carries substantial non-gun content (penalties for assaulting
  officials, pretrial release rules), so a no vote is not only about guns.
- Senate Bill 266 is half emissions policy and half utility ratemaking, so a no
  vote can be about rates.
- House Bill 805 is an omnibus, so no single axis explains a no vote.

House Bill 805 carries one label per strand, following the Florida Senate Bill
700 pattern. The sex-definition, prison-care and school strands are the civil
rights label; the separate Prevent Sexual Exploitation of Women and Minors Act,
which puts age and consent duties on pornography websites and fines them up to
$10,000 a day, is the corporate accountability label.

## Wording

Descriptions were written in plain English from the first draft, not rewritten
later. Measured over the 14 drafted descriptions: mean Flesch-Kincaid grade 7.1,
worst 8.4, longest sentence 29 words, and `candidateRecordPlainLanguageLint`
reported zero warnings before the import ran. The body and the closing sentence
are joined with a period, and the builder asserts that the string ", The "
appears in no description.

One trade-off is worth recording. Holding these descriptions to a seventh-grade
reading level meant short sentences, which pushed them to five or six sentences
rather than the three or four a denser register would have used. Reading level
was treated as the binding constraint.

## Checks run

- Judge: 11 approved, 3 returned to pending. No superseded-stage errors, because
  an override roll is always the chamber's last floor vote on the measure, and no
  `acknowledge_later_rolls` entries were needed.
- Import dry run and real run both reported 1,025 inserts over 14 files; the
  database then held 1,025 rows under this run's stamp.
- After the hold: 713 live records, 145 candidates, 611 area tags.
- Convergence dry run after the hold: 11 files, 713 records, all unchanged.
- No `ambiguous` outcomes. Four `related` flags, all on Bryan Cohn (House
  District 32), pointing at two hand-written records from July 2026. Both cite
  the wrong roll: the H318 record quotes House roll 577 (71-49) and the S266
  record quotes roll 585 (72-48). Those were the previous-question motions.
  The overrides were rolls 578 (72-48) and 586 (74-46). The URL-based duplicate
  check could not catch them because the cited roll differs from the imported
  one. Both were retired on 2026-09-01 with reasons naming the replacing
  records; see `related-retirements.json`. A fresh convergence dry run after
  the retirement shows 713 unchanged and no `related` flags.
- Production was not touched.
