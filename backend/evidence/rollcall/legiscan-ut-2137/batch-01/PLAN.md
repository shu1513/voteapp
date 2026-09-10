# Utah 2025 General Session, batch-01

12 roll calls on 7 measures. 433 candidate records across 69 candidates.
Local database only. Production holds no Utah records.

## How the batch was chosen

The five standing filters, in order, applied to the 1,557 stored floor rolls:

1. **Closely divided** — the smaller side is at least a quarter of the larger.
2. **Became law** — read from each bill's own history, not from LegiScan's
   status field, because Utah leaves a bill that became law without the
   Governor's signature at status 3.
3. **A subject a voter would recognize.**
4. **One roll per measure per chamber**, taking the chamber's last kept floor
   vote.
5. **A research area with a defensible direction.** `general` is barred on a
   roll-call record, so a measure with no honest direction is dropped.

That left 120 candidate rolls on 95 measures. This batch takes the 7 measures
whose enrolled text was read in full and whose direction is clean, leading with
House rolls because a House roll reaches about 56 of our candidates and a
Senate roll about 8.

## What is in the batch

| measure | rolls | area | direction |
| --- | --- | --- | --- |
| HB 402 Foods Additives in Schools | House 38-34, Senate 16-6 | environment_and_public_health | for |
| HB 403 SNAP Funds | House 54-14, Senate 15-13 | social_programs_and_welfare | against |
| HB 477 School Trespass | House 49-19, Senate 17-7 | civil_rights | for |
| SB 78 Homeless Individuals Protection | House 43-28, Senate 18-8 | social_programs_and_welfare | for |
| HB 263 Election Record | House 47-23, Senate 19-6 | election_integrity | for |
| HB 100 Food Security | House 47-24 | social_programs_and_welfare | for |
| SB 284 Medicaid Doula Services | House 55-16 | healthcare_affordability | for |

Every label states `nay` explicitly, and every one is null. On each of these
measures the realistic objection runs on a different axis from the area
scored — cost, administrative burden, parental choice, local control — so a no
vote is not evidence of a position on the area itself. The reasoning per
measure is in `JUDGING.md`.

## Checks run before importing

- **Version, per roll.** All 12 were cast on the text that was enrolled. This
  was checked rather than assumed: across the session, 321 of 1,916 floor votes
  were cast on some other substitute.
- **Superseded stage.** No selected roll has a later kept floor vote on the
  same measure and chamber, and no measure has a same-day peer, so no judgment
  needs `acknowledge_later_rolls`.
- **Governor action.** All 7 measures carry a `Governor Signed` line, so every
  tail reads "was signed into law".
- **Tally and members.** Every roll matches Utah's own record on the tally, and
  every roll was compared name by name against Utah's per-roll vote sheet: no
  member on the wrong side, no name missing on either side.
- **Later sessions.** Each measure's code sections were checked against the two
  2025 special sessions and the 2026 General Session for a later amendment or
  repeal. None of the seven was repealed.
- **Prose.** The builder refuses to write unless the body joins the tail with a
  period, every description states its own tally, no sentence exceeds 45 words,
  and no British spelling appears. It refused once, on a 46-word sentence in
  HB 477, which was split. Flesch-Kincaid grade median 7.6, worst 9.4, longest
  sentence 36 words. The repository's own lint reports 0 warnings over all 24
  descriptions.
- **Duplicates.** The importer flagged 5 related records and a wider sweep by
  bill number over every Utah record not written by this pipeline found the
  same set. One was a true duplicate and is retired in
  `duplicate-retirements.json`; the rest are sponsorship claims or records
  about other bills, which are distinct claims.

## What was dropped, and why

- **HB 69 Government Records and Information** — filter 5. It widens voter
  privacy, but its other half requires a court to find bad faith before a
  records requester can be paid attorney fees, bars fees where the requester's
  interest is commercial, and lets a court charge fees to a requester. Two
  directions of comparable weight.
- **SB 86 Workplace Protection** — filter 3. The official summary says it lowers
  the employee threshold for the Antidiscrimination Act; the enrolled text
  strikes "15" and re-inserts "15". What remains is a change to the definition
  of sexual harassment, which is not a subject a voter would recognize as the
  bill they were told about.

## Held for a direction call from the operator

None of these is dropped on the merits; each needs a decision.

- **HB 81 Fluoride Amendments** — bars fluoride in public water systems. The
  standing instruction is that the mainstream-consensus tiebreak on fluoride
  does not apply here, so the direction must be decided rather than assumed.
- **HB 77 Flag Display** — bars most flags on government property. Reads as a
  restriction on expression and as government neutrality; `general` is barred.
- **HB 233 School Curriculum** and **HB 281 Health Curriculum and Procedures**
  — area and direction both arguable.
- **SB 73 Statewide Initiatives** — adds requirements to initiative petitions.
  The standing line is that a measure about who may take part in lawmaking, or
  how hard lawmaking is, carries no honest direction.
- **HB 267 Public Sector Labor Union** and **SB 327 Public Sector Labor
  Organization** — no research area covers labor or union rights, and the
  `corporate_accountability` workaround does not reach a public employer. Both
  were also overtaken by the 2025 Second Special Session, which passed HB 2001
  amending the same law.

## Left for batch-02

93 rolls still marked `candidate` in `survey/dispositions.tsv`, including three
this batch deliberately deferred rather than rushed: HB 37 housing (18 code
sections), HB 300 election law (68 sections, 10,303 words of change), and
SB 181 parking, whose reach turns on a defined term this session could not
verify from Utah's code site.
