# New York batch-01 — judging notes

## Source

Every description was written from the enacted text on the Assembly bill page
(`nyassembly.gov/leg/?bn=<BILL>&term=2025&Summary=Y&Actions=Y&Text=Y`), read in full with
added language marked. The one-line summary on the same page was used as an index only.

**The sponsor's memo was never opened.** New York publishes one for every bill, and it is
advocacy in the same class as the Texas author's statement of intent. It is a separate
query parameter on the same page, so avoiding it is a deliberate choice, not an accident.

**Reading a New York act requires the bold-and-underline markup.** New York prints added
language in bold and underline and deletions in brackets, and the plain text of the page
loses the first of those. The reader marks added text `{NEW ... NEW}`, which is what makes
it possible to say what an act changes rather than describing statute it merely reprints.
This is the Kentucky bold-font lesson in a different format.

## Labels

| measure | area | yea | nay |
| --- | --- | --- | --- |
| S 1985-A | gun_control | for | null |
| S 743 | gun_control | for | null |
| S 744 | gun_control | for | null |
| A 4040-A | civil_rights | for | **against** |
| S 36-A | womens_reproductive_rights | for | null |
| S 3072 | civil_rights | for | null |
| S 8416 | corporate_accountability | for | null |
| S 7882 | housing_affordability | for | null |
| S 952-B | housing_affordability | for | null |
| S 801 | environment_and_public_health | for | null |
| S 8417 | environment_and_public_health | for | null |

The test for stating a nay side is the one Connecticut and Maryland settled on: state it
only when the act is single-subject, its whole operative content is the area's own
mechanism, and the realistic objection is to that mechanism rather than to cost, liability,
or something sitting in a different area.

**A 4040 is the one that passes it.** The area is "protect equal rights, anti-discrimination
enforcement, and fair treatment under law", and the act is nothing but a rule about how
discrimination is proved. A no vote is a position on that enforcement tool, so nay is
`against`.

The other ten take `nay: null`, each for a reason on a different axis from the area:

- S 1985, S 743, S 744 — the objections run on gun owners' due process and on burdens on
  dealers, not on whether gun violence should fall.
- S 36 — a no can rest on wanting the dispensing pharmacist named on a label, which is a
  labeling question rather than a position on reproductive health access.
- S 3072 — the objection is an employer's interest in screening for theft or fraud risk.
- S 8416 — the objection is exposure to the attorney general and to litigation.
- S 7882, S 952 — the objections are landlord compliance costs and liability.
- S 801, S 8417 — the objections are construction and hookup costs, which is a housing cost
  axis, not an environmental one.

Tag arithmetic was predicted before it was checked: 335 yes-side records across all eleven
measures, plus the 19 no votes on A 4040's Assembly roll, is 354. The database holds 354.

## Wording checks run before importing

- **Plain-language lint** (`listPlainLanguageWarnings`, 45-word cap): 44 descriptions,
  **0 warnings**.
- **Reading level measured separately**, because the lint only counts sentence length:
  Flesch-Kincaid **median grade 9.2, worst 9.6**, longest sentence 33 words. A first draft
  measured median 10.9 and worst 12.6 and was rewritten before anything was imported.
  Around grade 9 is the honest floor for statutory text; going lower means dropping the
  limits the acts actually carry, which is what caused correction rounds in other states.
- **Comma splice**: the builder joins each body to its tally sentence with a period and
  asserts the string `", The "` appears in no description.
- **British spellings**: scanned and clean in the descriptions. (This file and PLAN.md
  use American spellings throughout.)
- Every description contains its own roll's tally, which the approval gate requires.

## Qualifications that are deliberately in the text

These are the limits a shorter description would have dropped, and each one is in the
record:

- **A 4040 is housing discrimination only**, not all discrimination, and the practice can
  still be lawful with a proved business justification that no less harmful practice would
  serve.
- **S 8416's private right of action still covers deceptive acts only** — the new unfair
  and abusive categories are the attorney general's to enforce — and a business following
  federal trade rules has a complete defense.
- **S 3072 has eight exceptions**, all named, and local laws that protect workers more
  still apply.
- **S 952 covers rent stabilized apartments only**, and lists exactly what a landlord may
  keep.
- **S 7882 does not cover** software that sets rent or income limits under rent regulation
  or a government affordable housing program.
- **S 801 does not touch existing buildings**, and lets the state board ask a one- or
  two-family home for wiring alone.
- **S 8417 leaves the 100-foot cost sharing rule in place for electric service** and only
  ends it for gas.
- **S 743** also drops the old line telling local police to enforce the warning rule, which
  is a counter-strand and is stated rather than hidden.

## Related records and duplicates

The import flagged 8 related records, all on one candidate, Assembly member Brian Maher,
who has hand-written vote records in the database. Reading them:

- **3 are true duplicates** of votes this batch imports (S 952, S 7882, S 1985). They cite
  the Assembly bill page rather than the roll call, so the importer's URL check could not
  fold them. All three were retired with `manual:records:retire`, each reason naming the
  record that replaces it; the file is `duplicate-retirements.json` and must be re-run
  against production when New York is promoted.
- **5 are false positives** — records about other bills that happen to share a date with a
  roll in this batch (S 752, S 745, A 2581, S 4914).

A wider sweep over every New York candidate record that is not from this pipeline, matching
on bill number and on the bill-page URL, found the same three and nothing else.

## Counts and reconciliation

- Dry run: 22 files, **570 planned inserts**, 0 errors, 0 notified.
- Real run: 22 files, **570 inserts**, 0 errors, 0 notified, stamp
  `2026-09-03T01:53:37.791Z`.
- Convergence dry run afterwards: 570 `unchanged`.
- The dry run's own stamp `2026-09-03T01:52:55.648Z` matches **zero** rows, which is the
  proof that `--dry-run` writes nothing.
- Database: 570 live records, 53 candidates, 354 tags, 22 approved rolls.

**53 candidates, not the 55 the crosswalk maps.** Michael Cashman (Assembly District 115)
and Diana Moreno (Assembly District 36) both took their seats during the session and cast
their first floor votes on 2026-01-07 and 2026-02-04. Every measure in this batch was voted
in 2025, so neither of them could have voted on one. This is the same shape as Florida's
Nathan Boyles, not a gap in the crosswalk.

## Gates

No `acknowledge_later_rolls` anywhere. Before judging, every selected roll was checked for a
later same-chamber floor vote on the same measure and for a same-day peer: there are none,
because New York votes each measure once per chamber. Senate Bill 1985's superseded
2025-05-13 vote is earlier than the roll judged, so the superseded-stage gate never fires.
