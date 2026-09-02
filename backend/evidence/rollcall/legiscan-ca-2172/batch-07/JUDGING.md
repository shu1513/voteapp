# California batch-07 — judging notes

Source unchanged: the **Legislative Counsel's Digest and the operative sections** on leginfo, with
every qualification read out of the section rather than the digest's paraphrase. For the two vetoed
bills and the three enrolled ones there is no chaptered version, so the **enrolled text** is the
final text and the one read.

## Writing votes on bills that are not law

Five of the eight measures here never became law — three are still on the governor's desk, two were
vetoed. The descriptions say so plainly and in the right tense:

- enrolled: *"The bill has passed the Legislature and is awaiting the governor's decision."*
- vetoed: *"…but the governor vetoed it, so it did not become law."*

and the body of a vetoed bill's description is written in the conditional ("would have required")
throughout, so nothing implies a rule that does not exist. This is the whole cost of the wider
rule, and it is a writing cost, not an accuracy one.

## The pre-flight completeness audit — 49 items, five real gaps

Run before judging, as in batch-06. Every "This bill would…" clause in each digest was listed
**untruncated** and checked against the drafted description. Five gaps were found and closed
before any database write:

| measure | what the draft had left out |
| --- | --- |
| SB 352 | the board chair and air district officers must **appear before** legislative policy committees, not just file a report |
| SB 840 | the Legislature's **intent** on revenue shares; the **2034** five-year re-review of offset protocols; the **urgency** clause |
| SB 1250 | the public's right to **submit data and comments** into the connectivity review |
| SB 629 | the **urban conflagration and farmland** hazard factors; the **safety element** update duty; the **exemption from normal rulemaking** |

The SB 629 rulemaking exemption is the one worth naming. A bill that lets an agency draw regulatory
maps outside the Administrative Procedure Act is making a real trade — speed against notice and
comment — and a voter is entitled to see it. It sat behind a flat digest clause.

## Label calls

| measure | label | why this direction |
| --- | --- | --- |
| SB 352 | `environment_and_public_health`/for | Community air monitoring that must stay in place for at least five years, with published data — "standards, enforcement, and prevention". |
| SB 744 | `public_education_quality`/for | The area names **standards and accountability**. Accreditation is the standard that makes a degree count for state licensing; freezing recognized accreditors keeps students' degrees usable while federal recognition is in flux. |
| SB 840 | `environment_and_public_health`/for | Keeps cap-and-trade proceeds flowing to air protection, forest health and clean transportation, and forces offset rules onto the best available science. |
| SB 923 | `data_privacy`/for | "Clear limits on collection, sharing, and misuse" — the deletion right stops being defeated by buying the data from a third party. |
| SB 1250 | `environment_and_public_health`/for | Wildlife connectivity becomes a standing objective of highway planning rather than a stated hope. |
| AB 2247 | `social_programs_and_welfare`/for | "Support vulnerable populations through effective safety-net programs." `gun_control` was considered and **rejected**: the bill regulates no firearm, it pays for counseling after the fact. |
| SB 613 | `environment_and_public_health`/for | The operative goal is cutting methane, including from imported gas. |
| SB 629 | `environment_and_public_health`/for | Fire hazard mapping and building standards in burned areas — prevention. |

## Traps caught while reading

- **SB 352 is funding-conditional throughout.** "Subject to available funding" governs the
  five-year monitoring minimum, the plan update *and* the district's duties — three separate
  subdivisions. A flat "monitoring must run five years" would have overstated all three.
- **SB 840's percentages are intent, its dollars are law.** The digest leads with intent to direct
  "specific percentages" to dedicated funds; the operative section allocates **fixed dollar sums in
  priority order** ($1 billion to high-speed rail, $800 million to affordable housing, $250 million
  to community air protection, and so on). Describing the percentages as the rule would have been
  the digest trap again. Both are stated, and marked for what they are.
- **SB 613 mandates less than it appears to.** Agencies "shall prioritize" methane strategies only
  "where feasible and cost effective"; the board "shall encourage" certified-gas buying subject to
  the PUC's ratepayer judgment; the protocol powers are "may". The description uses the verbs the
  statute uses. Its subdivision (f) also disclaims requiring any new gas purchases, which is stated
  — without it the bill reads as promoting gas.
- **SB 629's clock is two-staged.** The Fire Marshal transmits the map within 90 days of full
  containment or by May 1, 2026, whichever is later; standards then apply **30 days after
  transmission**; the local agency posts notice within **10 business days**. Only the last two are
  in the description, because they are the ones a property owner feels.
- **AB 2247 does nothing until it is funded.** "Contingent upon appropriation" is stated, along
  with the January 1, 2032 sunset, so no one reads it as money already flowing.
- **SB 744 is conditional on the accreditor not changing.** Recognition is retained only while the
  agency "continues to operate in substantially the same manner" as on January 1, 2025.

## The `nay` side

**Every label states `"nay": null` deliberately**, as in batches 04-06. Nay voters keep a record
saying exactly what they voted against and take no stance tag. None of these eight is a referendum
on the area itself: voting against a Los Angeles counseling pilot is not evidence of opposing the
safety net, and voting against one methane bill is not opposing clean air.

## Runs

| step | result |
| --- | --- |
| plain-language lint | 16 descriptions, **0 warnings**, longest sentence 41 words, no British spellings |
| `rollcall:judge --dry-run` | 8 `dry_run` |
| `rollcall:judge` | 8 `updated` |
| `rollcall:legiscan:import --dry-run` | **507 planned inserts**, 3,859 unchanged, 0 errors |
| `rollcall:legiscan:import` | **507 inserts**, 3,859 unchanged, 0 errors, 0 notified |
| re-run `--dry-run` | **4,366 unchanged**, 0 errors |

**Reconciled three ways.** `candidate_records` went 120,295 → 120,802 (+507); the run's predicate
`origin_run_id LIKE 'rollcall:CA:%:2172:%:2026-08-31T18:11:03.299Z'` returns 507 rows across 69
candidates; the DRY RUN's stamp `2026-08-31T18:10:30.959Z` matches **zero** rows.

69 candidates, not 80: absences differ per roll, and no single roll reaches every member.

California now holds **4,366 roll-call records across 80 candidates**, 62 measures. Prod untouched.

## Review response (2026-09-01) — enrolled bills rewritten into the conditional

The section above says enrolled bills get a pending closing sentence "in the right tense" — but the
**bodies** of SB 923, SB 1250 and AB 2247 were written as law: "requires", "creates", "must". A
reviewer caught it. All three now use "would" throughout, matching the vetoed bills. 185 records
rewritten (55 + 64 + 66), re-run 4,560 `unchanged`, row count unchanged. When the governor acts,
each needs a second rewrite; the README keeps the list.
