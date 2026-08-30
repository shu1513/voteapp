# PA batch-01 — judging notes

Judged 2026-08-29 from the enacted text at
`palegis.us/legislation/bills/text/HTM/2025/0/<BILL>/PN<n>`, cross-read
against the House Appropriations fiscal note for the printer's number each
chamber actually voted. No AI provider call anywhere in this pipeline.

## Version check, per roll

Pennsylvania prints the printer's number inside the roll-call desc, so the
version check is exact rather than inferred.

| measure | desc | last PN on the bill page | verdict |
| --- | --- | --- | --- |
| HB 103 | `House Floor: HB 103 PN 1113, FINAL PASSAGE` | 1999 | **differs** — see below |
| HB 858 | `House Floor: HB 858 PN 1806, FINAL PASSAGE` | 1806 | voted the enacted text |
| SB 95 | `House Floor: SB 95 PN 1019, FINAL PASSAGE` | 1019 | voted the enacted text |
| HB 1425 | `House Floor: PN2675, CONCURRENCE` | 2675 | voted the enacted text |
| SB 375 | `House Floor: PN1539, FINAL PASSAGE` | 1539 | voted the enacted text |

**HB 103** is the one version split. The House passed PN 1113 148-55; the
Senate amended it to PN 1999, passed that 50-0, and the House concurred
201-2 (not divided, so not in the pool). Diffing the two texts: the scheme
is unchanged — the same at-risk / derelict / abandoned chapter, the same
cost recovery, the same summary-offense and third-degree-misdemeanor
penalties. The Senate added 48-hour thresholds, made the abandonment
presumption expressly rebuttable, and wrote out a nine-clause definition of
"law enforcement officer". The description therefore says the House passed
**this version** and names the Senate's tightening, rather than claiming
the House voted the Act.

**SB 375** needed one extra step: the fiscal note is written against PN
1538, and the House voted PN 1539. Diffing them, the only substantive
change is two reporting deadlines moving from 2026 to 2027. Everything the
description asserts was then re-verified against the PN 1539 text itself.

## Labels

| measure | area | yea | why |
| --- | --- | --- | --- |
| HB 103 | environment_and_public_health | for | removal of derelict vessels from public waters, with enforcement and polluter-pays cost recovery |
| HB 858 | corporate_accountability | for | forces business and LLC property owners to name a person answerable for code violations |
| SB 95 | healthcare_affordability | for | pharmacy price and cost-share disclosure on request, EMS naloxone, provisional licensing |
| HB 1425 | environment_and_public_health | for | certification, listing and enforcement regime for nicotine vape products |
| HB 1425 | cost_of_living_reduction | **against** | raises the presumed retailer cost of doing business on cigarettes — the statutory minimum retail price — from 7% to 8.5% and then 9.5% |
| SB 375 | environment_and_public_health | for | defibrillator availability, response plans and CPR training in schools |

Direction follows the **area description**, not the bill, per the standing
rule. `cost_of_living_reduction` reads "Lower household costs by improving
price stability, competition…", and a state-mandated markup floor on a
widely purchased product raises prices and suppresses price competition, so
a yes vote is `against` that area.

**HB 1425 is the multi-label case** (FL SB 700 precedent). Its two strands
genuinely pull opposite ways across two areas: the vape directory is
tighter nicotine regulation, the cigarette markup increase is a consumer
price floor. One label would flatten it and no label would waste it. The
markup is not de minimis — it is a phased statewide price change written
into the same act, not an incidental clause (the HB 351 de-minimis
principle does not reach it).

Counter-readings considered and **not** labelled:

- **HB 858 / data_privacy.** A county list of property-owner contact
  details invites a privacy objection. The act exempts owner-occupied
  property outright (§ 2504), makes the list confidential (§ 2508), and
  lets a municipality obtain an entry only on a showing of reasonable need
  plus an affirmation of no commercial use (§ 2506). Those safeguards make
  the counter-strand de minimis.
- **SB 375 / public_education_quality.** The area is student outcomes
  through teaching, standards, funding and accountability. Cardiac
  emergency preparedness is community-health prevention, which is what
  `environment_and_public_health` describes.

## Facts pinned to the enacted text, not the summary

The Georgia lesson (a summary can invert its own act) was applied to every
claim. Specifically checked against the statute rather than the note:

- **SB 95** — the price disclosure lives in new § 9.6 and is owed *upon
  request*, not posted; the pharmacy must post a notice of the right to ask.
  Non-compliance is a summary offense with a fine of up to $500. The EMS
  naloxone provision is expressly **voluntary**, creates no duty to stock,
  and carries no liability for declining.
- **SB 375** — § 1423.1(a) training is *offered as an option* to employees
  and volunteers; only § 1423.1(b)(2) makes it mandatory, and only for
  nurses, coaches, band directors, PE teachers and athletic trainers. The
  description says "must take that training" of exactly that group.
  Compliance deadline is three years (§ 6, which amended "five" to
  "THREE"), and the readily-accessible AED duty is § 1423.3(a).
- **HB 1425** — the markup section reads 7% today, "EIGHT AND ONE-HALF PER
  CENTUM" from the later of 60 days after the effective date or 2026-03-01,
  then "NINE AND ONE-HALF PER CENTUM" a year after that. The $50,000 bond
  floor, the 120-day sell-through and the escalating penalties up to
  revocation are all in the enacted article.
- **HB 858** — the chapter does not apply to owner-occupant real property
  (§ 2504), the 30-day filing duty attaches on purchase (§ 2503), and the
  $500 fine is for knowingly or intentionally false information (§ 2507).

## Import ledger

Dry run: 5 files, 0 errors, **882 planned inserts**, 0 notified.
Real run: 5 files all `imported`, 0 errors, **882 inserts**, 0 notified,
179 distinct candidates. `candidate_records` 78,177 → 79,059.

Batch stamp: `origin_run_id LIKE 'rollcall:PA:%:2026-08-29T05:31:20.098Z'`
returns 882 records / 179 candidates. The dry run's own stamp
`2026-08-29T05:30:47.915Z` matches **zero** rows — positive proof
`--dry-run` is inert. Tags: 1,058 = 882 + the 176 second labels on HB 1425.

Idempotency: a dry re-run reports all 882 `unchanged`
(`import-dry-run-rerun-report.json`); `import-report.json` is the original
insert ledger and was copied aside before the re-run.

0 `related` flags and 0 `ambiguous` records across all five rolls — no
pre-existing hand-written Pennsylvania record cites any of these votes, so
nothing was retired.

**PROD UNTOUCHED.** Promotion is a separate step.

## Fan-out note

176 of 203 members matched on the four 2025 rolls; SB 375 matched 178 of
200 voting members. The unmatched are members whose seats have no Nov-2026
candidate rows and members with an explicit `null` crosswalk entry. There
is no Speaker gap here — unlike Texas (Burrows) and Georgia (Burns), the
Pennsylvania Speaker votes on the floor and appears in the fan-out.

## Plain-language rewrite (2026-08-29)

Every description in this batch was rewritten in plain English, aimed at a
reader with no legal or legislative background. No fact, number, date, tally,
stance direction or label changed. A machine check compared every numeric
token in the old and new text; it found two deliberate differences, both
documented in these notes: HB 1445's wording simplification (batch-02) and
HB 103's added 201-2 final-concurrence tally (batch-01, required by the
superseded-stage gate). Mean sentence length across all 37 PA measures is 12.8 words,
longest 37; the plain-language lint reports 0 warnings.

What changed, in practice: terms of art were replaced with what they mean.
"Medical Assistance" became Medicaid, "cost sharing" became copay or
deductible, "postpartum" became after giving birth, "interscholastic
athletics" became school sports, an "automated external defibrillator" is now
introduced as the device that shocks a stopped heart, "extreme risk
protection orders" as what are often called red flag orders, "assisted
reproductive technology" as fertility treatment, "public accommodation" as
public places such as stores and restaurants. British spellings that had crept
in (sterilisation, programme, colour, misdemeanour) were corrected.

**HB 1445 is the one deliberate factual simplification.** "Individualized
education program or section 504 plan" became "already required by law to
provide it for a student with a disability", which covers both instruments
without naming a statute section a general reader cannot place.

## Two gates from main's PR #950, met during the rewrite

**Authored nay stances replace auto-inversion.** `rollcall:judge` now refuses a
judgment that does not state the nay side, because a no vote on one bill is not
automatically the opposite stance on a whole research area. Every PA label is
recorded as `nay: null` — nay voters get no tag. That is the system's own
default for rows judged before the field existed, and it is the honest reading
here: voting against one insurance mandate is not evidence a member opposes
affordable healthcare. Authoring a real nay stance per measure is a separate,
deliberate decision, not something to smuggle into a wording change. PA tags
fall from 6,700 to **4,081** as the old auto-inverted nay tags clear; the count
reconciles exactly against the labels on each approved roll.

**The superseded-stage gate caught a real defect in HB 103.** The House voted
148-55 on printer's number 1113, then voted again on the Senate's final text
and backed it 201-2 (roll 1596525, not divided, so it never entered the pool).
A record citing only the 148-55 vote would read as a member's final position
and would misrepresent the 55 no votes, most of which became yes. The
description now names the 201-2 vote, and the later roll is listed in
`acknowledge_later_rolls` so the approval is on purpose rather than by
oversight.

## Plain-language review fixes (2026-08-30)

Eight review findings, all verified against the official sources and all real.
The rewrite had introduced them; the fixes restore precision without giving up
plain wording:

- **HB 1549**: "After 2029" implied 2030; the fiscal note says the inflation
  adjustment begins January 1, 2029. Now "Starting in 2029".
- **HB 2103**: the bill covers gender identity **or expression**; the rewrite
  had dropped "or expression" in two places. Restored in both.
- **HB 755 / HB 1828**: "no copay or deductible" silently dropped coinsurance.
  Now "at no cost to the patient, with no copay, deductible or coinsurance".
- **HB 1127**: complaints cover dentists **and hygienists**, not "those
  dentists".
- **HB 1866**: the statute reaches making, repairing, selling, dealing in,
  using or possessing — and "owning" is not "possession". Full verb list
  restored.
- **HB 858**: "promise not to sell it" narrowed the statute's "not be used for
  commercial purposes". Restored.
- **SB 375**: "shocks a stopped heart" was medically wrong — an AED analyzes
  the rhythm and shocks only when that can help. Now "checks a person's heart
  rhythm and can give an electric shock to bring back a normal heartbeat".
- The audit statement above now names both deliberate numeric differences
  instead of claiming there was only one.

354 + 1,062 records rewritten in place; both convergence runs all unchanged.
`import-plain-language-report.json` is the final plain-language run;
`import-report.json` remains the original insert ledger.

## Plain-language pass 2 (2026-08-30) — the whole campaign measured, not assumed

Batches 03, 04 and 05 were written after the batch-01/02 rewrite and were
never held to the same standard, so every PA description was scored rather
than eyeballed: Flesch-Kincaid grade, longest sentence, and a scan for terms
of art left bare. 45 of the 179 measures came in at grade 8 or above or
carried bare jargon (worst 10.5); those 44 bodies were rewritten. Median
grade 6.8 -> 6.4, worst 10.5 -> 9.0, bare-jargon measures 20 -> 0. A machine
check compared every numeric token, roll number, date, chamber, review status
and label before and after: zero differences. 5,837 records rewritten in
place; all five convergence runs unchanged.

The pass-2 run ledger is `import-plain-language-2-report.json` (a snapshot of
the importer's re-run report). `import-report.json` is untouched: the
importer writes a real re-run's report to `import-rerun-report.json` and
never overwrites the insert ledger.

## Incident note (2026-08-30): this file was truncated and restored

The first push of pass 2 replaced this file with only the pass-2 note. The
cause was a Python one-liner used to append and to fix end-of-file newlines —
`open(p,'w').write(open(p).read()...)` — which truncates the file on opening
for write, before the read runs, so the read returns nothing. The same
one-liner had earlier truncated batch-04's and batch-05's JUDGING.md to a
single newline, and those truncations were merged to main unnoticed. All five
files are restored here from git history, byte-for-byte, with the notes
re-appended. Review caught it; nothing was lost, because every prior version
was in a commit.

## Review fixes on pass 2 (2026-08-30)

Four wording regressions the pass introduced, all verified and fixed:

- **HB 1866**: pass 2 wrote "owning" where the statute says possessing — the
  exact error an earlier review had already fixed once. Possession includes
  holding or controlling a device without owning it. Now "possessing" again.
- **HB 1262**: "a disability that makes online filing hard" broadened the
  bill's exemption, which requires a disability that prevents electronic
  filing. Now "prevents them filing online".
- **HB 316**: the rewrite framed every permit-denial ground as money owed,
  but an unfixed serious code violation is its own ground, not a debt. The
  sentence no longer says "owes money".
- **HB 660**: "sprinkler heads" is a different component from the regulated
  "spray sprinkler bodies" (the base holding the pressure regulator). The
  correct term is back, with a short explanation.
