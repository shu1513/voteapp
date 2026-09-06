# Judging notes, South Carolina batch-02

Six divided House roll calls on six measures. **None of the six became law.**
Every description says so explicitly and says what stage the bill reached.

## Reading South Carolina texts

South Carolina's LegiScan documents are `text/html`, not PDF. Fetching them as
PDF and running a PDF extractor fails with hex-string and trailer-dictionary
errors, which is how this was found. The API response's `mime` field is the
thing to branch on. All six texts here were fetched, saved as HTML, stripped of
tags and read.

## Which text each roll voted

South Carolina takes a second-reading and a third-reading vote on most bills,
often on consecutive days, and the second reading is frequently the divided one.
For each measure the text read is the version printed for the House vote being
imported:

- **H 3927** amended print of 2 April 2025, the day of the roll.
- **H 4760** amended print of 5 February 2026, the day of the roll.
- **H 4764** amended print of 1 April 2026; the imported roll is the third
  reading the next day on that same text.
- **H 4767** committee substitute of 10 February 2026; the later committee
  substitute of 5 May 2026 is the Senate committee's version, produced after the
  House had finished with the bill, and is not what the House voted.
- **H 3645** amended print of 29 April 2025, the day of the roll.
- **H 3045** amended print of 2 April 2025, the day of the roll.

## Supersession

Two rolls needed `acknowledge_later_rolls`, both because the third reading was
lopsided where the second reading had been close:

- **H 3045** — second reading 76-20 (divided), third reading 91-7 (not divided).
- **H 3645** — second reading 80-31 (divided), third reading 86-18 (not
  divided).

In both the later vote is on the same text, so the imported roll is the one that
records a real division.

## Label reasoning

Every label uses `nay: null`.

- **H 3927**, `civil_rights`, against. Section 1-1-1920 defines DEI as
  programs that "constitute illegal discrimination", and 1-1-1930 opens with
  "Except as required by federal law". The description carries both
  qualifications. The label stays: the operative sections shut every DEI
  office, DEI statement and mandatory DEI training in every public entity,
  and Section 2 repeals S.C. Code 1-13-110, the 1978 requirement that each
  state agency keep an affirmative action plan approved by the Human Affairs
  Commission. Those are concrete cuts to anti-discrimination machinery
  regardless of how the definition is read, and every other DEI-office ban
  in the run (NC SB 129, TN SB 227 and SB 558, AL) carries the same label.
- **H 4760**, `womens_reproductive_rights`, against. Section 44-41-820 makes
  it a felony to deliver, dispense, distribute or provide abortion-inducing
  drugs to a pregnant woman; subsection (C)(2)-(3) exempts the woman's own
  acts and her possession for her own use. Section 3 lists mifepristone and
  misoprostol as Schedule IV and makes possession a felony unless obtained
  by valid prescription, again exempting a pregnant woman's possession for
  her own consumption. The description names the supplier offense, the
  patient exemption and the prescription exemption directly instead of
  "using" and "some exceptions".
- **H 4764**, `immigration`, against. Subsection (B) requires every law
  enforcement agency operating a correctional facility to enter a 287(g)
  agreement; (D) turns that into an annual request when no agreement can be
  had; (E) excuses an agency whose governing body formally finds compliance
  fiscally or operationally impractical, subject to the Attorney General
  finding the written findings sufficient under (J)(4). The description
  carries all three tiers rather than a bare mandate.
- **H 4767**, `corporate_accountability`, **for**. Noncompete clauses in
  physician employment contracts would be void and against public policy, and a
  departing doctor could keep treating existing patients. It limits a term an
  employer can impose, which is what this area measures. At 58-53 it is the
  closest recorded vote in the South Carolina run.
- **H 3645**, `social_programs_and_welfare`, **for**. More weeks of paid
  parental leave for eligible state employees on the birth or adoption of a
  child.
- **H 3045**, `public_safety_and_crime_control`, for. A new offense of obscene
  visual representations of child sexual abuse, with penalties, added to the sex
  offender registry.

## Descriptions

Each cites its own roll call's tally and ends by saying the Senate had not voted
on the bill, so it has not become law — the wording follows the Pennsylvania
batch-02 precedent. Every description uses "would" rather than the present
tense, because none of these is law. Plain-language lint after the review
rewrite: 12 descriptions, 0 warnings, longest sentence 29 words.

## Review rewrite

PR review flagged H 3927, H 4760 and H 4764 as overstating the voted text.
All three descriptions were rewritten against the amended prints named above,
the judgments re-applied with `rollcall:judge` (3 updated, 3 unchanged), and
the batch re-imported: 304 records rewritten in place, 297 unchanged, no new
rows. Labels unchanged. See `import-rerun-report.json`.

## Duplicates

0 found.
