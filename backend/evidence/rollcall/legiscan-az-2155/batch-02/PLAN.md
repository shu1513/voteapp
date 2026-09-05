# Arizona batch-02 — selection

21 measures, 26 rolls, 682 records across 54 candidates. This batch **closes the Arizona 2025
divided-and-signed pool**: every one of the 184 rows in `../survey/divided-signed-worklist.tsv`
now carries a disposition and none is left open.

## The whole remaining pool was read

Batch-01 left 113 rows on 93 measures marked `candidate:batch-02`. Sixteen were the 2025-2026
budget bills (SB 1735 to SB 1750), dropped by the standing appropriations rule without a read.
**The other 77 measures were each read in full** from Arizona's enacted-stage staff analysis,
with the chaptered text consulted wherever the stance turned on a specific change.

| outcome | measures |
| --- | --- |
| judged and imported | 21 |
| dropped after a full read | 56 |
| dropped by the appropriations rule | 16 |

A 73% drop rate is higher than California's 47%, and two Arizona-specific rules explain it.
First, **Arizona records nothing without a stance**, because `general` is not a user-selectable
research area — the escape hatch other states used for a divided vote with no honest direction
does not exist here. Second, a striking number of Arizona's divided-and-signed measures are
subjects the research areas simply do not cover: animal welfare, foreign adversary land
ownership, cryptocurrency, stadium financing, specialty license plates.

## What is in

| measure | area | yes means | rolls |
| --- | --- | --- | --- |
| HB 2074 school safety program | public_safety_and_crime_control | for | House 35-24, Senate 17-12 |
| HB 2173 mental health questions on license applications | civil_rights | for | House 34-25 |
| HB 2195 mature ads in children's apps | corporate_accountability | for | Senate 16-9 |
| HB 2374 transnational repression | public_safety_and_crime_control | for | House 35-25, Senate 16-12 |
| HB 2386 state police pay benchmarks | public_safety_and_crime_control | for | Senate 22-6 |
| HB 2447 administrative approval of development plans | housing_affordability | for | House 38-22, Senate 19-8 |
| HB 2514 student directory information | data_privacy | for | Senate 18-11 |
| HB 2611 Preston's Law, group assault | public_safety_and_crime_control | for | Senate 18-10 |
| HB 2653 victim and witness name redaction | public_safety_and_crime_control | for | Senate 22-7 |
| HB 2880 campus encampments | civil_rights | **against** | House 41-17, Senate 18-10 |
| SB 1060 materials before an internal police interview | public_safety_and_crime_control | **against** | House 38-20, Senate 20-9 |
| SB 1295 AI impersonation to defraud | public_safety_and_crime_control | for | House 40-17 |
| SB 1307 advanced air mobility plan | public_infrastructure | for | House 44-12 |
| SB 1308 sober living homes | environment_and_public_health | for | Senate 20-7 |
| SB 1316 maternal mortality review | environment_and_public_health | for | House 42-14 |
| SB 1333 foster placement | social_programs_and_welfare | for | House 44-11 |
| SB 1378 political flags and homeowners associations | civil_rights | for | House 32-25 |
| SB 1437 mandatory reporting of child abuse | public_safety_and_crime_control | for | House 38-20 |
| SB 1461 dismissal of a promoted officer on probation | public_safety_and_crime_control | **against** | House 42-17 |
| SB 1462 explicit images made by computer | public_safety_and_crime_control | for | House 33-24 |
| SB 1537 transitional housing after prison | social_programs_and_welfare | for | House 31-25 |

Eight areas, three of them new to Arizona: `data_privacy`, `public_infrastructure` and
`social_programs_and_welfare`. Every nay is stated and every nay is `null`.

**Three measures score `against`, and two of them are the same argument.** SB 1060 and SB 1461
both strengthen a police officer's position against their own employer — one by handing over
the investigation materials 24 hours before the interview, the other by barring dismissal of a
promoted officer who fails probation. `public_safety_and_crime_control` names accountability
among its goals, so both cut against the area. That sits deliberately beside the seven crime
measures in this batch that score `for`.

## What was dropped, grouped by reason

Every drop has a written reason on its worklist row. The patterns:

**No research area covers the subject** (11), the largest group and an honest gap rather than a
judgment call: SB 1658 animal cruelty, HB 2109 forced organ harvesting, HB 2704 stadium
financing, HB 2749 a state Bitcoin reserve, HB 2009 a military vehicle tax break, the specialty
license plates.

**The direction runs both ways** (14): HB 2112 age verification for adult sites (child
protection against the privacy cost of identity checks), HB 2540 swapping a national test for
the state assessment, HB 2728 religious programs and therapy as DUI alternatives, SB 1348
faster permits paired with dropping the fire marshal experience requirement, SB 1496 narrowing
which charities qualify while widening the services that count.

**Scope of practice** (4), all following Montana HB 218: SB 1395 international medical
graduates, SB 1124 oral preventive assistants, HB 2583 imaging ordered by physical therapists,
HB 2001 practice before licensure. In each the objection is patient safety, and
`healthcare_affordability` is defined as access to "affordable, quality care", so the
counter-reading sits inside the same area.

**Technical, narrow or trivia** (24): pension mechanics, tax deed procedures, specialty plates,
liquor on boats, a single-site zoning carve-out, sunset extensions and two study committees.

**Two China measures dropped together** (SB 1082 land ownership, SB 1221 public fund
divestment). Both are aimed at security, `foreign_trade` is about trade rather than investment,
and splitting the pair would have made two members' records disagree about the same question.

**The most consequential drop is SB 1611**, the groundwater savings credit measure. Arizona
water is the state's defining policy fight, and this act lets a landowner in the Phoenix or
Pinal management areas give up an irrigation right permanently in exchange for credits. Whether
that conserves water or frees credits for new building is exactly what is disputed, so no
direction is defensible. **Expect to be asked about this one.**

## Version and vote checks

All 26 rolls were confirmed to be on the text that became law, using the rule established in
batch-01: a `Final Read` line in the bill history means the second chamber amended and the
originating chamber's earlier roll is on a superseded draft.

**HB 2447 hit the reconsidered-vote trap for a third time.** Its Senate third reading failed
15-12 on 2025-03-26, the Senate moved to reconsider, and it passed 19-8 the same day. Both are
stored under the plain `Senate - Third Reading` caption, so only the `passed` flag and the
ascending roll id separate them. The passing roll is judged and the failed one is named in
`acknowledge_later_rolls` with a note, because the superseded-stage gate cannot order two votes
taken on one day.

## What remains in Arizona

Nothing in this pool. The remaining Arizona work is listed in `../README.md`: the 374 divided
roll calls on 174 **vetoed** measures, the 48 divided votes on ballot referrals that the
concurrent-resolution gap makes unreachable, the 2026 session, and promotion to production.
