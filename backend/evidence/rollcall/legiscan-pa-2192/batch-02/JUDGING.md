# PA batch-02 — judging notes

Judged 2026-08-29 from the House Appropriations fiscal-note ANALYSIS for the
printer's number each roll actually voted. No AI provider call anywhere.

## Wording rule for a bill that did not become law

Every body uses the conditional — "which would have required…" — the Ohio LSC
convention for provisions that never took effect. Descriptions never assert
that any of these bills changed the law, because none did.

The tail is time-stamped rather than absolute: "The Pennsylvania House passed it
143-60; as of August 2026 the Senate had not voted on it." The 2025-2026 session
runs to 30 November 2026, so a bill still sitting in the second chamber could
in principle still move. A flat "did not become law" would be a claim that could
later turn false; a statement about a past state cannot.

Failed votes take a different tail: "The measure failed in the Pennsylvania
House 101-102."

## Version check

Pennsylvania prints the printer's number in the roll-call desc, so each
description is written against the exact text that chamber voted. Because none
of these bills reached a second chamber, there is no later amendment for a
description to drift from — the version risk that produced the HB 103 split in
batch-01 does not arise here.

## Facts pinned to the analysis, with the qualifications carried

- **HB 1957 is a proposed constitutional amendment on FIRST consideration.** It
  did not create a right. A Pennsylvania amendment must pass both chambers in
  two consecutive sessions and then be approved by voters; the description says
  so, so no reader can take it as settled law. This is the Texas HJR 98 lesson.
- **HB 2103 carries its exemptions.** The description names the religious
  exercise clause for churches and other tax-exempt religious bodies, the
  carve-outs for rented rooms in a personal residence, landlord-occupied rooming
  houses and single-sex dormitory rooms, and the dress-and-grooming provision.
  Stating only the ban would flatten a qualified statute — the TX SB 2972 rule.
- **HB 1445** — insurers could still refuse an unlicensed or out-of-scope provider,
  a service not medically necessary or outside their administrative policies,
  or one a school must already provide under an IEP or section 504 plan. Named.
- **HB 583** — restores pre-September-2011 adult dental coverage, but the
  department may offer fewer services if funding falls short. Named.
- **HB 1593** deletes an existing exemption rather than creating a new check:
  private sales of long guns currently escape the background check that
  handguns already require. The description says that.
- **HB 1549** gives the actual county-class tiers ($15 Philadelphia; $12 rising to
  $15 by 2028 in the larger counties; $10 rising to $12 elsewhere) and the 60% tipped rate, not the
  headline number.
- **HB 111** is not a health-coverage bill; it stops LIFE insurers penalising
  someone for filling a naloxone prescription, so it is labelled
  corporate_accountability, not healthcare_affordability.
- **HB 1100** is food assistance, not health insurance, so it is labelled
  social_programs_and_welfare.

## Labels

Direction follows the area description, not the bill. All 32 are `for` their
area: each expands coverage, access, protection or rights that its area
describes. There is no Senate measure in this batch, so no `against` direction
appears — that is an artefact of which chamber's bills were verified first, not
a slant. The screened queue for batch-03 contains 8 Senate measures whose
direction is `against` (RGGI repeal, emissions-inspection repeal, transgender
sports restrictions, firearm preemption).

## Dropped after reading the analysis

- **HB 1077** — creates a Commission on Children's Vision and nothing else.
  Standing up an advisory body has no honest for/against on healthcare
  affordability.
- **SB 614** — no fiscal note exists at the voted printer number; moved to
  `pending:needs-detail-read` rather than judged from its title.

## Import ledger

Dry run: 32 files, 0 errors, **5,642 planned inserts**, 0 notified.
Real run: 32 files all `imported`, 0 errors, **5,642 inserts**, 180 candidates.

**Review fixes 2026-08-29 (six measures rewritten in place):** HB 535, HB 618
and HB 755 gained their federal-trigger condition (and HB 535 its $10,000
willful fine), HB 1445 its two missing exclusions, HB 1825 its narrow
second-fine waiver, HB 1549 its full wage ladder. Re-judge updated 6 rows;
re-import rewrote **1,053 records** (unchanged 4,589); convergence dry run =
all 5,642 unchanged. A rewrite re-stamps `origin_run_id` (the TX batch-02
mechanic), so the batch now spans two stamps: **4,589 @
`2026-08-29T06:47:30.145Z` + 1,053 @ `2026-08-29T23:00:27.999Z`**.
`import-report.json` stays the original insert ledger; the rewrite run is
`import-rewrite-report.json`.

Original batch stamp predicate before the rewrite returned 5,642 / 180. The dry run's own stamp `…T06:47:01.238Z` matches **zero**
rows. Batch-01's stamp still returns exactly 882, proving the per-run stamp
separates batches. PA totals: **6,524 records, 6,700 tags.** A dry re-run
reports all 5,642 `unchanged` (`import-dry-run-rerun-report.json`);
`import-report.json` is the original insert ledger, copied aside first.

`candidate_records` 81,744 → 87,386. The batch-01 baseline was 79,059, so the
local database gained rows from a concurrent session in between — the local
`voteapp` is shared. The run-stamp predicate, not the table delta, is the
authority, and it reconciles exactly.

**One `related` flag, correctly not a duplicate:** a hand-written record for Ann
Flood cites the same House Journal source but describes a different bill
(HB 1442, Morgan Rose's Law). Distinct claim, kept. 0 `ambiguous`.

**PROD UNTOUCHED.**

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
