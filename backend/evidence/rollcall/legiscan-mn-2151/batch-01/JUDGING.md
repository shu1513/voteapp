# Minnesota batch-01 — judging

Two measures, three rolls. Both were read in full from the enacted session law at
revisor.mn.gov, which is the ground truth named in the campaign brief. Every claim below is
traceable to a numbered section of the act.

## Source

**The Revisor of Statutes publishes each session law chapter as marked-up text**, with new
language wrapped in `new text begin` / `new text end` and struck language in
`deleted text begin` / `deleted text end`. Minnesota therefore has **no struck-text hazard at
all** — the trap that forced page renders in Georgia, Maine, Montana, Kentucky and Indiana
simply does not arise here, because a plain text extraction keeps the markers.

Chapter URL: `https://www.revisor.mn.gov/laws/2025/<0 regular | 1 special>/Session+Law/Chapter/<n>/`

**⚠ The House Research Department bill summary is not a substitute for the act.** House
Research publishes an official, nonpartisan, section-by-section summary at
`https://www.house.mn.gov/hrd/bs/94/<BILL>.pdf` with no sponsor statement of intent, and it is
version-stamped in its own header. But for the 2025 bills that went to conference the file
stops at an engrossment — HF 2432's summary is headed "Third engrossment" and dated April 30,
while the enacted text is the May 18 conference report. Check the header before relying on one,
and read the chapter for anything a description asserts.

## HF 1 (2025 first special session, chapter 2) — both chambers

The whole act is one amendment to Minnesota Statutes section 256L.04, subdivision 10. It adds
paragraph (c): enrollment in MinnesotaCare for undocumented noncitizens aged 18 or older is
limited to those enrolled as of the effective date, and those adults are ineligible for
MinnesotaCare beginning January 1, 2026. Paragraphs (a) and (b), which make undocumented
noncitizens eligible, are otherwise untouched, so **people under 18 keep their coverage** — the
description says so, because the age limit is the whole point of the provision. Effective the
day after final enactment; signed June 14, 2025.

**Label: `immigration`, yea "against", nay "for".** Direction follows the area description
("Welcome immigration through a lawful, orderly, and humane system"), not the bill, which is the
rule that made Texas SB 8 an `against` and Maryland's Values Act a `for`. The nay side is stated
rather than left null because this measure passes the test the Connecticut repair set: the act
is single-subject, its entire operative content is who gets health coverage based on immigration
status, and the recognizable objection is to that mechanism itself. A no vote here is a vote to
keep the coverage.

Rolls: house 1588802 (68-65) and senate 1587239 (48-16), both 2025-06-09, both the chamber's
final action — the bill was introduced, passed and sent to the governor the same day, with one
text version and no adopted amendments, so there is no version question.

## SF 2200 (2025 regular session, chapter 24) — house only

Three sections, all read:

1. New section 13.891 makes government data identifying a restorative practice participant
   private data on individuals, with the same disclosure exceptions as section 3, and excludes
   personnel data and paid facilitators.
2. Section 142A.76, subdivision 8 adds a yearly grantee report to the director on recidivism,
   public safety, local investment, and payments to participants.
3. New section 595.02, subdivision 1b makes statements made and documents offered in the course
   of a restorative practice neither discoverable nor admissible in a civil or criminal
   proceeding. **Three exceptions**, all named in the description: statements or documents that
   are the subject of a maltreatment report under section 626.557 or chapter 260E; disclosure a
   participant reasonably believed necessary to prevent reasonably certain death, great bodily
   harm, or commission of a crime; and evidence of professional misconduct by a participant
   acting under a professional or occupational license. Two savings clauses follow: a court that
   ordered participation may be told whether the person took part, and evidence otherwise
   admissible does not become inadmissible because it was discussed in a restorative practice.

**Label: `public_safety_and_crime_control`, yea "for", nay null.** The area covers "prevention
... and justice system performance", and protecting the confidentiality of a court-connected
restorative process is squarely that. The nay side is null on purpose: the recognizable
objection — that courts and victims lose access to evidence — sits inside the same research
area on the other side, which is the case the Connecticut SB 1367 note describes.

Roll: house 1570373 (98-36), 2025-05-12. The bill was never amended after that vote and went to
the governor as passed, so no version question arises. The Senate's vote was not divided, so
this measure is house-only by the gate, not by choice.

## Checks run

- **Plain-language lint before importing**, not after: 6 descriptions, 0 warnings, longest
  sentence 25 words.
- **Reading grade measured separately**, because the 45-word lint is not a readability check:
  Flesch-Kincaid 8.7 and 8.9 for HF 1, 9.9 for SF 2200. SF 2200 sits higher because "restorative
  justice", "criminal" and "professional" cannot be replaced without losing the statute's terms;
  its meaning is glossed in a following sentence instead.
- **Sentence count**: HF 1 is 4 sentences, SF 2200 is 6. SF 2200 exceeds the 2-4 guidance
  deliberately — it names three statutory exceptions, and dropping them is the omission defect
  that forced correction rounds in Maryland, Pennsylvania and Connecticut.
- Body and tail joined **with a period**; the builder asserts `", The "` appears in no
  description.
- American spelling checked over the descriptions and these notes.
- Every roll date matches Minnesota's own history exactly; no `official_vote_date` override is
  needed.
- Tally sentences carry each roll's own numbers, which the approval gate requires.

## Import

Two runs, one per session, because the importer selects evidence by session id.

| run | ledger | rolls | records | stamp |
|---|---|---|---|---|
| regular session 2151 | `import-2151-report.json` | 1 | 3 | 2026-09-04T20:22:26.399Z |
| special session 2217 | `import-2217-report.json` | 2 | 25 | 2026-09-04T20:22:27.242Z |

Reconciled three ways: the dry run planned 3 and 25; the real runs imported 3 and 25; the
database holds **28 live records across 25 candidates with 28 tags**. Both convergence dry runs
report every record unchanged (`import-2151-convergence-report.json`,
`import-2217-convergence-report.json`). 0 errors, 0 notified, 0 related flags, 0 ambiguous.

Tag arithmetic checks out per roll: HF 1's senate roll tagged all 22 mapped members (12 yea
voters "against", 10 nay voters "for") because its nay side is stated; HF 1's house roll tagged
3 the same way; SF 2200 tagged its 3 mapped members "for", all of whom voted yes, and no nay
tags because that label's nay is null.

**Production has zero Minnesota records.** Everything here is on the local database only.
