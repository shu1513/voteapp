# Wisconsin batch-10: the died pool, education strand

Judged 2026-09-11. Local database only; production holds no Wisconsin roll-call
records. No AI calls.

## The died pool

The operator asked for the 46 measures that passed one chamber on a closely
divided roll and never passed the other. Every one ends with the same history
line: "Failed to concur in pursuant to Senate Joint Resolution 1". Senate Joint
Resolution 1 set the session schedule, and the last floor period for general
business has ended. So each bill is dead, and the tail says so plainly:
"...but the Senate did not vote on it before the session's last floor period
ended, so it never became law."

The pool is split into three batches: education (this one), economy and taxes
(batch-11), and everything else (batch-12). AB 625, homelessness grants, moved
from this strand to batch-12.

## What was read

Only Introduced prints exist for these bills. Each description is written from
the introduced text, read with the markup resolver, plus any amendment the
chamber adopted before its vote. Amendments were fetched with LegiScan's
`getAmendment` call and checked against the dataset's md5 hash.

Amendments that changed what was voted:

- **AB 91**: Assembly Amendment 1 changed the cost split from 75/25 to 50/50.
- **AB 226**: Assembly Amendment 2 widened the ban from free and reduced-price
  meals at public and charter schools to all school meals, including private
  voucher schools.
- **AB 644**: Assembly Amendment 1 moved the years to 2027-2029 and changed the
  aid from $2,000 in one year to $1,500 and then $650.
- **SB 41**: Senate Amendment 1 deleted the $30 million appropriation. The
  description says the grants would have had no money set aside.
- **AB 6**, **AB 486** and **AB 613** also had amendments. All three are dropped
  for other reasons.

## Kept: 8 measures, 8 roll calls

| measure | what it would have done | area | yes means |
| --- | --- | --- | --- |
| AB 3 | cursive in the model standards and elementary curricula | public_education_quality | for |
| AB 4 | civics instruction and a half-credit graduation requirement | public_education_quality | for |
| AB 91 | Milwaukee school resource officer costs split 50/50, with aid penalties | public_safety_and_crime_control | for |
| AB 226 | five food additives barred from school meals | environment_and_public_health | for |
| AB 644 | higher aid for merged school districts | government_efficiency | for |
| AB 647 | grants for districts that share whole grades | government_efficiency | for |
| AB 648 | new aid for merged districts whose tax rate rises | government_efficiency | for |
| SB 41 | school safety grants, with the money removed | public_safety_and_crime_control | for |

Every no side is `null`.

**The school-merger bills use `government_efficiency`.** They were one package
to encourage districts to merge or share grades, and AB 646's own study language
asks for mergers "that promote efficiency". That is the area's "reduce waste" in
plain terms.

**AB 3 and AB 4 add standards**, which is the area's own word. They follow AB 2
(phones out of class) and AB 592 (science teacher training) in batch-01.

## Dropped: 6 measures

- **AB 6** would have required districts to spend 70 percent of operating costs
  in the classroom, cut state aid from districts that fall short, and capped
  administrator raises at the teachers' average. Inside
  `public_education_quality` it reads both ways: more money in classrooms, but
  aid cuts to the districts that miss the line.
- **AB 486** would have limited colleges from cutting a student's aid because of
  a private scholarship. College costs fit no research area;
  `cost_of_living_reduction` lists housing, energy, drugs, food and goods.
- **AB 613** would have required a written notice to every parent in a class
  each time a pupil is removed from it. It could mean more openness or fewer
  removals of disruptive pupils, and no area names classroom discipline.
- **AB 646** is a study of school district boundaries. A study is not a policy,
  the Connecticut lesson.
- **AB 649** is a supplemental appropriation to the Joint Committee on Finance.
  Appropriations are excluded by standing rule.
- **SB 371** would have required ultrasound video and fetal development content
  in human growth and development classes. It does not change access to
  reproductive care, so `womens_reproductive_rights` does not describe it, and
  inside `public_education_quality` a content mandate reads both ways.

## Duplicate retired

A hand-written record, `83e85ea1`, said a member "voted against Assembly Bill
226 on January 15, 2026; it restricted certain food additives". That is the
same vote as roll 1614194, and it wrongly implies the bill became law. It is
retired in `duplicate-retirements.json`, which must be applied again at
promotion. The imported record `d1faf97d` replaces it.

## Reconciliation

- Plain-language lint over 16 descriptions: 0 warnings, longest sentence 44
  words.
- Judge: 8 updated. Import dry run: 614 inserts. Real run at
  `2026-09-11T21:11:17.343Z`: `outcomes {imported: 8}`, `actions {insert: 614}`,
  0 notified.
- 313 yes-side records, each with one tag. 301 no-side records, no tags.
- All live Wisconsin roll-call records: 5,850 over 111 rolls and 99 candidates,
  with 3,604 tags.
