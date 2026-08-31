# California batch-09 — judging notes

Method unchanged: digest plus operative sections, "as specified" chased, completeness audit before
judging.

## The audit found gaps in five of eight

| measure | what the draft had left out |
| --- | --- |
| SB 294 | the annual notice also goes to a worker's **chosen representative**; a **public prosecutor** may enforce the act alongside the Labor Commissioner |
| SB 1125 | the **Attorney General** may sue to stop practices that break the program's rules |
| AB 1108 | "in-custody death" reaches **federal correctional and immigration detention** facilities, not only county jails and state prisons |
| AB 507 | the **owner-occupied** affordability tiers; a city's option to write its **own** adaptive reuse ordinance; the **30-year property-tax incentive** |
| AB 260 | if federal regulators **withdraw approval** of these drugs, state drug labeling law stops applying to them |

Two of those matter more than the rest. **AB 1108's federal reach** is the difference between a bill
about county jails and a bill that also covers deaths in immigration detention. **AB 260's
FDA-withdrawal clause** is the bill's insurance policy — the provision designed to keep the drug
lawfully dispensable in California if federal approval disappears — and the digest buried it in a
clause about labeling law.

## Label calls

| measure | label | why this direction |
| --- | --- | --- |
| AB 1050 | `housing_affordability`/for | "Increase housing supply" — private covenants that cap the number of homes stop blocking commercial-to-residential conversions. |
| SB 294 | `civil_rights`/for | "Fair treatment under law": written notice of rights, including rights when law enforcement comes to a workplace, plus an emergency-contact duty if a worker is detained. `immigration` was considered and passed over — the act protects every worker, not only immigrants. |
| SB 1125 | `social_programs_and_welfare`/for | Means-tested help with an essential household bill. `cost_of_living_reduction` was considered and **rejected** although it is an uncovered area: that area is written around prices, competition and trade, and this is a safety-net program. Correctness beats a new area. |
| SB 655 | `environment_and_public_health`/for | Indoor heat as a health standard. |
| AB 1108 | `public_safety_and_crime_control`/for | The area names **accountability** — the sheriff no longer rules on deaths in their own custody. |
| SB 464 | `corporate_accountability`/for | More granular pay reporting and a penalty a court must impose rather than may. |
| AB 507 | `housing_affordability`/for | By-right conversion of existing buildings with mandatory affordable set-asides. |
| AB 260 | `womens_reproductive_rights`/for | Direct: protects access to medication abortion and the people who provide it. |

## Traps caught while reading

- **SB 655 sets a goal, not a standard.** Agencies must *consider* the policy; no building is
  required to install cooling. The description says so, because "all homes must be able to stay
  cool" would read as a mandate on landlords.
- **AB 1050 does not override zoning.** It clears private covenants only; a project must still be
  consistent with state housing law, the general plan and zoning. Stated.
- **AB 507 cuts both ways for local government**, and the description says both: cities lose
  discretionary review and CEQA on these projects, but may write their own ordinance with different
  procedures so long as they do not close the route off.
- **SB 1125 does nothing until it is funded**, and its steps differ depending on whether the
  appropriation covers a partial or a statewide rollout.
- **AB 260 repeals dead law as well as writing new law.** The parental-consent and criminal
  advertising provisions had already been held unconstitutional; removing them changes no one's
  rights today, so the description leads with the operative half.
- **SB 464's real change is the word "must".** A court previously *may* impose the penalty for a
  missing pay data report; now it must.

## The `nay` side

Every label states `"nay": null`.

## Runs

| step | result |
| --- | --- |
| plain-language lint | 16 descriptions, **0 warnings**, longest sentence 40 words |
| `rollcall:judge --dry-run` / real | 8 `dry_run` / 8 `updated` |
| `rollcall:legiscan:import --dry-run` | **87 planned inserts**, 4,473 unchanged, 0 errors |
| `rollcall:legiscan:import` | **87 inserts**, 4,473 unchanged, 0 errors, 0 notified |
| re-run `--dry-run` | **4,560 unchanged**, 0 errors |

**Reconciled three ways.** `candidate_records` went 120,909 → 120,996 (+87); the run's predicate
`origin_run_id LIKE 'rollcall:CA:%:2172:%:2026-08-31T18:27:54.458Z'` returns 87 rows across 11
candidates; the DRY RUN's stamp `2026-08-31T18:27:27.830Z` matches **zero** rows.

California now holds **4,560 roll-call records across 80 candidates**, 80 measures, **18 of 27
research areas**. Prod untouched.
