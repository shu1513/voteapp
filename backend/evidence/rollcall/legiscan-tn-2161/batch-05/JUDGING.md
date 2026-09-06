# Judging notes, Tennessee batch-05

Eleven roll calls on ten measures, all enacted. Every chaptered act was
downloaded through the LegiScan API with byte length and MD5 verified, extracted
and read. Scope `--scope-from 2026-08-01`.

## Version check

No roll tripped the supersession gate. HB 132's earlier House passage (roll
1553242, 69-22) is superseded by the House concurrence the next day (1554657,
72-23), which is the imported roll and the final House action; the earlier roll
is marked `superseded` in the worklist.

Every imported roll was on the text that became law. No vehicle-bill trap.

## One reading corrected

**HB 612** looked from its LegiScan summary like an expansion — it "expands from
wetlands to all areas that an aquatic resource alteration permit may apply".
Reading the act reverses that impression. Section 1 says that where the *only*
thing disqualifying an applicant from a general permit is the acreage of wetland
impact or length of stream impact, the department **shall exempt** that acreage
or length from compensatory mitigation. The act reduces what a developer must do
to offset harm, so the direction is `against`, not `for`. This is why the
summary field is used for triage and never for the label.

## Label reasoning

Every stance label uses `nay: null`. HB 1332 carries the neutral `general`
label after PR review (below).

- **SB 218**, **SB 1840**, **HB 120** — `public_safety_and_crime_control`, for.
  A presumption against release on personal recognizance for a felony involving
  a firearm or serious bodily injury or death; a ten-year limitations period for
  vehicular homicide; and a new offense of intentionally impeding a member of
  the division of protective services.
- **HB 1332**, `general`. Sections 2 and 3 lower the handgun carry permit age
  from 21 to 18; section 1 tightens enhanced permit eligibility (no
  driving-under-the-influence conviction within five years, and not two or more
  within ten). The two pull in opposite directions on `gun_control`, so the
  act carries no stance. The first pass read only section 1 and labeled it
  `gun_control` for; fixed on PR review.
- **HB 1665**, `civil_rights`, against. Insurers, managed care organizations and
  other payers may not require or request that a provider ask a minor whether
  the minor feels normal in their body, believes they are the correct gender, or
  identifies as a gender different from their sex, and may not condition
  payment, credentialing, quality scoring or participation on asking.
- **HB 132**, `anti_corruption`, for. The General Assembly gains power to end a
  governor's declared or extended state of emergency by joint resolution. The
  direction rests on the plain fact that a unilateral executive power now has a
  legislative check on it.
- **SB 1868** and **SB 1788**, `social_programs_and_welfare`, for. SB 1868
  requires that treatment justifying a child's state custody be evidence-based
  and delivered by a qualified provider, caps custody at six months and allows
  one six-month extension only after a hearing. SB 1788 bars a local government
  from relocating a homeless person to another jurisdiction without that
  jurisdiction's written consent, and only through a program to reunite the
  person with a place they have substantial ties to.
- **HB 612**, `environment_and_public_health`, against, for the reason above.
- **HB 1060**, `cost_of_living_reduction`, against. The agent fee on a hunting
  or fishing license rises from $1 to $3.

## Descriptions

Each cites its own roll call's tally. Plain-language lint: 22 descriptions, 0
warnings, median Flesch-Kincaid grade 7.2, worst 11.0 — the worst is HB 1665,
where the payer terms it regulates cannot be shortened without losing who the
rule binds.

## Duplicates

0 found.

## Post-import review (PR #1184)

- **HB 120, House roll 1509575.** Official tally 73-20; LegiScan lists 73-19.
  LegiScan omits **Shaundelle Brooks (HD-060)** from this roll — she is absent
from the member list, not recorded as not voting — as it does on three other
early-2025 House rolls in this run. Tennessee's own bill pages list her voting No on each.
Every other name on the roll matches the official list, so the roll is kept and
her vote is added by hand (the Alabama HB 95 precedent: a member the feed does
not cover is not a member who abstained). Her four records cite the official
bill page and carry the neutral `general` label, matching the `nay: null` side
of each roll. The descriptions now state the official tally and, in
parentheses, the LegiScan tally the judge's tally gate requires.
- **HB 1332.** Both descriptions now state the age change and the DUI bar; the
  `gun_control` for label is replaced by `general`, and the 75 records' tags
  were re-synced by the import.

Re-import after the corrections: 11 files, 151 rewritten, 623 unchanged, 0
errors (`import-rerun-report.json`).
