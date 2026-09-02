# PA batch-04 — the rolls that needed a text read

29 rolls / 28 measures / **2,852 records**. These are the votes whose bill had
no fiscal note at the printer's number the chamber actually voted, so each was
judged from the enacted-style bill text at palegis.us instead of a summary.
Judged 2026-08-30. No AI provider call.

## Why this group existed

About 200 of the 256 not-enacted rolls had a fiscal note at the voted printer's
number. The other 56 did not, and most were Senate bills. Rather than judge
those from a title — Pennsylvania titles say almost nothing — they were parked
until their text could be read. Reading the text resolved 28 of them.

## The multi-vote measures live here

Four rolls are the second vote on a measure their chamber voted twice, and
each names the earlier roll in `acknowledge_later_rolls`:

- **SB 114** and **SB 1400** each got a final passage vote and then a
  reconsidered vote the same day on the same printer's number. The
  reconsidered vote is the operative one, so it is the one judged.
- **HB 1042** is the only measure in the whole pool both chambers passed. The
  House voted 149-50 in March and then 102-100 on the final version in July;
  the Senate voted 28-22 and then 30-20 on reconsideration. The decisive vote
  in each chamber is judged, and its tail says the bill had not become law as
  of August 2026 rather than claiming the other chamber never voted.

## Notable content

- **SB 1400** ends the automatic life sentence for second-degree (felony)
  murder for defendants 18 or older, letting courts set a minimum term. Its
  House companion **HB 1042** covers the same ground plus earned time for
  vocational training. Both labelled public_safety_and_crime_control/for,
  consistent with HB 150 (compassionate release) and HB 1936.
- **HB 846 and SB 908 point in opposite directions on the same statute.**
  HB 846 widens the prevailing wage law to cover duct cleaning and offsite
  fabrication; SB 908 narrows it, removing home rehabilitation and school
  safety work. corporate_accountability/for and /against respectively.
- **HB 2189** is a second minimum wage bill, on a flat statewide schedule
  ($11 / $13 / $15) rather than HB 1549's county tiers. Both are recorded.
- **SB 155 and SB 156** (death record and wage record cross-checks against
  Medicaid and food stamp rolls) are labelled government_efficiency/for
  rather than social_programs_and_welfare/against. They are eligibility
  verification, not a benefit cut; the counter-reading, that more frequent
  checks push eligible people off benefits, is recorded here rather than
  turned into a stance.
- **SB 490** (no release on recognizance for defendants found to threaten
  public safety) is labelled public_safety_and_crime_control/for. The
  counter-reading is that expanded money bail detains poor defendants.

## Eleven dropped after reading the text

HB 1532, HB 2650, SB 157, SB 403, SB 404, SB 472, SB 527, SB 682, SB 780,
SB 922 and SB 952 — reasons per roll in
`../survey/divided-not-enacted-worklist.tsv`. The recurring pattern is a
measure that reads two ways at once: SB 952 as both SEPTA accountability and
privatization, SB 403 and SB 404 as both permitting efficiency and weaker
waterway protection, SB 780 as both neighborhood relief and pressure on
unhoused people.

## Import ledger

Dry run 2,852 planned; real run 2,852 inserts, 0 errors, 0 notified, stamp
`2026-08-30T01:45:48.643Z`. Dry re-run all 2,852 `unchanged`. Every label
carries an explicit `nay: null`.

**PROD UNTOUCHED.**

## Review fixes (2026-08-30)

**SB 187 retracted.** The review was right: the bill creates a six-person,
$1.25 million-per-year Independent Energy Office that plans, analyzes and
reports on energy policy. Nothing in it improves service delivery, reduces
waste or modernizes administration, so the government_efficiency/for stance
was unsupported — and no other research area carries a defensible direction
for an energy policy-analysis office, which is exactly the fifth filter's
drop condition. All 14 fanned-out records were retired with a stated reason
(`/Users/shu/legiscan-data/pa-2192-sb187-retirements.json`), the roll was
returned to pending, and the entry and its evidence file were removed from
this batch. The retired records' tags are mooted by retirement (the GA SB 33
precedent). Batch-04 is now 28 rolls / 2,838 live records. The comparable
regulatory-reform bills SB 333 and SB 444 keep their government_efficiency
label: their subject is the machinery of regulation itself, where this bill's
subject is energy policy.

**HB 273 rewritten and relabelled.** The description had covered only the
county-code provision; it now names the inspectors and fines, the state grant
program (up to $100,000 a town, matching funds, for towns with no code
enforcement) and the mitigation fund that pays for demolition, cleanup and
repair of blighted property. On the label, the review's second point was
taken further than it asked: a property maintenance code binds all property
owners, not companies, so corporate_accountability was the wrong frame
entirely. The bill amends the Neighborhood Blight Reclamation chapter and its
machinery is blight remediation, so it is relabelled
**housing_affordability/for**, consistent with HB 743 (blighted property to
land banks), HB 1650 (home repair grants) and HB 734 (habitability).

**HB 1062 relabelled for the same reason.** The statewide registry of serious
property maintenance violations sits in the same chapter with the same
purpose and had the same wrong label; it moves to housing_affordability/for
as well. 176 HB 273 records rewritten in place, both relabels confirmed in
the tags table, convergence all 2,838 unchanged. `import-report.json` remains
the insert ledger; this run is `import-review-fix-report.json`.

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
