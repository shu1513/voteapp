# North Dakota batch-01 — judging notes

## Source

Every description was written from the **enrolled act** at ndlegis.gov, read top to bottom.
North Dakota publishes no neutral prose analysis — there is no analogue of the Ohio LSC,
Georgia HBRO, Connecticut OLR or Maryland DLS note — so the act is the source. That also
means the hazard those states carry, of an official summary that contradicts the act, does
not arise, and neither does the Texas sponsor-statement hazard: the enrolled act carries no
advocacy of any kind.

North Dakota's enrolled act is plain text with no strikethrough and no underline, so the
struck-text problem does not arise on the enrolled print. Where a description needed to say
what an act **changed**, the introduced print was read with
`/Users/shu/legiscan-data/nd_text.py`, which classifies each drawn rule by where it sits
relative to the baseline and prints deletions as `[[...]]` and additions as `<<...>>`. The
tool was verified against a known-bad input first, so a checker that never fires could not
be mistaken for one that passes.

## Version check

Every one of the 14 rolls was checked against the bill's dated text stack. In each case the
chamber voted the last text in force, and no text version other than `Enrolled` follows the
vote date. Four measures had an amendment shortly before the vote (HB 1216, HB 1318,
HB 1600, SB 2339) and in every case the roll falls **after** it.

This check matters more in North Dakota than elsewhere, because the state records no
conference-report or concurrence vote. A measure that went to conference has only
pre-conference votes on record, and 82 of the 194 divided-and-enacted rolls are out of the
pool for exactly that reason.

## Tally audit — three independent sources agree

North Dakota gives an unusually strong check, because the **enrolled act prints its own
vote counts** on the signature page. So each roll was checked three ways: the LegiScan
feed, the bill-history action line, and the enrolled act itself.

All 14 match on all three. That is not a formality here: a session-wide audit of all 2,087
second-reading rolls on kept bill types found 11 that do not match, in two classes — a
division or floor-amendment vote stored under the second-reading caption, and one roll's
member list copied onto three other bills. Both are written up in `CODE-FINDINGS.md`, and
all eleven sit in the config's `heldRollCallIds`.

## Labels

Seven measures take `nay: null` and two take an authored `nay`.

The two authored ones are **HB 1318** and **SB 2339**, and they meet the Connecticut test:
each is single-subject, its whole operative content is `corporate_accountability`'s own
mechanism — the extent to which a company can be held liable — and a no vote is a
recognizable position on that mechanism rather than an objection on some other axis. Both
score `yea: against, nay: for`.

The other seven take null because the realistic objection runs on a different axis from the
scored area. A no vote on HB 1600 may be about the four hundred thousand dollars rather
than about immigration; a no on HB 1217 may rest on criminal deterrence rather than on
civil rights; a no on SB 2352 may be about prison safety. Recording those as an area stance
would attribute a position the vote does not evidence.

Two label choices worth stating:

**HB 1178 is `civil_rights`, not `election_integrity`.** The area descriptions govern:
`election_integrity` is "secure, accurate, auditable" — the count itself — while who may
vote and how easily is voter access, which the campaign files under `civil_rights`. The
same rule sent California SB 1 and Arizona HB 2017 there.

**HB 1318 is `corporate_accountability`, not `environment_and_public_health`.** The act
changes no standard for air, water or health; it changes who can be sued and when. Every
operative word of it is about a company's exposure to a failure-to-warn claim, which is
`corporate_accountability`'s own subject. This follows the Montana HB 740 reasoning,
inverted: judged as public health the measure runs two ways, judged as corporate
accountability every provision points one way.

## Writing

Descriptions were written in plain English from the first draft, not rewritten afterwards.
One body per measure, with the yes and no descriptions generated from it behind different
opening clauses, so the two cannot drift apart — the Arkansas builder pattern.

The builder refuses to write the file if any check fails: sentence length over 45 words,
the `", The "` comma splice, a missing tally in either sentence, sentences joined without a
space, or a British spelling from an explicit word list. It was run against a known-bad
input first to prove the checks fire.

Measured before importing, never eyeballed:

- repository plain-language lint: **28 descriptions, 0 warnings**
- Flesch-Kincaid grade: **median 8.1, worst 10.1**
- longest sentence: **28 words**

A first draft measured median 9.8 and worst 12.9 and was rewritten before anything was
judged. Grade 9 or so remains the honest floor for statutory text: getting SB 2339 below it
would mean dropping "strict liability", which is the thing the act changes.

Descriptions run five to nine short sentences rather than the two-to-four guidance. That is
deliberate and is the same trade Indiana, Montana, North Carolina, Colorado and Delaware
recorded: four sentences means dropping the conditions and exceptions, and dropping exactly
those is what has caused most correction rounds in this campaign. So HB 1114 keeps both of
its limits (the high-deductible plan start point and the Medicare Part D carve-out),
HB 1216 keeps the two-part definition of which drugs it covers, and SB 2339 keeps the fact
that the wildfire plan it describes is voluntary.

## Run

| step | result |
|---|---|
| judge | 14 updated, 0 errors |
| import dry run | 334 planned inserts, 0 errors, stamp `2026-09-07T18:58:33.027Z` |
| import real run | **334 inserts**, 0 errors, 0 notified, stamp `2026-09-07T18:59:02.491Z` |
| convergence dry run | all 334 unchanged |

Reconciled three ways: the dry-run plan, the real run's ledger, and the database all say
334 records across 48 candidates. The dry-run stamp matches **zero** rows, which is the
positive proof that `--dry-run` wrote nothing.

**48 candidates is every member the crosswalk maps** — North Dakota's Speaker casts
recorded votes, so there is no gap of the kind Texas and Georgia have.

Tags come to 235, and the arithmetic was predicted from the labels before the database was
read: the two measures with an authored nay tag both sides (20 + 27 + 28 = 75) and the
seven with `nay: null` tag yea voters only (160).

Zero `related` flags and zero `ambiguous` outcomes. North Dakota had no pre-existing
hand-written vote records to collide with, so nothing was retired.

The importer names its own ledgers. `import-report.json` is the original insert run and the
convergence run wrote `import-dry-run-rerun-report.json`; neither was renamed by hand. All
three committed ledgers are filtered to this batch's 14 rolls, because the importer scans
every stored evidence file and the unfiltered report carries a row for all 2,131 North
Dakota rolls.
