# SC batch-02 — measures the House passed that did not become law

**6 divided House rolls / 6 measures / 601 records.**

Batch-01 exhausted South Carolina's divided-and-enacted pool: all 37 rolls in
`../survey/divided-enacted-worklist.tsv` carry a disposition, and nothing there
is left unjudged on its merits. This batch opens the scope batch-01 recorded as
untouched — measures the House passed that the Senate never voted on — following
the Pennsylvania batch-02 precedent in `../../legiscan-pa-2192/batch-02/`.

Why this is worth doing here in particular: **the South Carolina Senate is not
on the November 2026 ballot**, so every Senate roll fans out to zero candidates
and the House record is the entire reachable story. A House member's recorded
vote on a bill the Senate then sat on is still that member's position, and the
House fan-out here is about 100 candidates per roll.

| measure | area | yea | roll | House |
| --- | --- | --- | --- | --- |
| H 3927 Ending Illegal Discrimination and Restoring Merit-Based Opportunity Act | civil_rights | against | 1534869 | 82-32 |
| H 4760 abortion-inducing drug crimes and scheduling | womens_reproductive_rights | against | 1625316 | 76-28 |
| H 4764 jail agreements with federal immigration authorities | immigration | against | 1675715 | 84-26 |
| H 4767 physician noncompete ban | corporate_accountability | for | 1670890 | 58-53 |
| H 3645 more paid parental leave for state employees | social_programs_and_welfare | for | 1561513 | 80-31 |
| H 3045 obscene visual representations of child sexual abuse | public_safety_and_crime_control | for | 1535156 | 76-20 |

**H 4767 at 58-53 is the closest recorded vote in the South Carolina run.**

## Not taken from this scope

- **H 5683**, the congressional redistricting bill the House passed 74-36 and
  74-37 on 20 May 2026. Dropped under filter 5 for the same reason as
  Tennessee's HB 7003: a mid-decade redraw with no court order behind it, where
  any direction would be an assertion about who the map favors rather than
  something the text settles. Flagged for the operator.
- **H 4762** school volunteer chaplains and historical displays, **H 4755**
  judicial selection reform, **H 4151** juveniles, **H 3876** accommodations,
  **H 3869** a sales tax exemption, **H 3924** hemp-derived ingestibles,
  **H 3570** disclosure of economic interests — each reads two ways on its own
  text, or has no nameable single subject.
- Rolls on bills that never left the introduced stage, and the House Rules and
  Sine Die procedural votes.

## Still true from batch-01

The `general` label rule closes the no-stance route, which continues to cost
S 933 (legislative pay, 59-48), S 508 (monuments) and H 3558 (Article V
commissioners). Those remain open questions for the operator.
