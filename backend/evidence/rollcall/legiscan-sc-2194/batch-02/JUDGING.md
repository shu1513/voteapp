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

- **H 3927**, `civil_rights`, against. The bill's own title is the Ending
  Illegal Discrimination and Restoring Merit-Based Opportunity Act; it would bar
  every state office, division or unit from operating diversity, equity and
  inclusion programs.
- **H 4760**, `womens_reproductive_rights`, against. New crimes and penalties
  for abortion-inducing drugs, and mifepristone and misoprostol added to
  Schedule IV so that possession alone becomes a criminal offense, with
  exceptions.
- **H 4764**, `immigration`, against. Every law enforcement agency operating a
  correctional facility would have to sign an agreement with federal immigration
  authorities to help enforce federal immigration law.
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
tense, because none of these is law. Plain-language lint: 12 descriptions, 0
warnings, median Flesch-Kincaid grade 7.6, worst 10.7.

## Duplicates

0 found.
