# Wisconsin batch-06 — how each measure was judged

Judged 2026-09-10. Local database only; production holds no Wisconsin roll-call
records.

Every description was written from the enrolled print.

## Result

**9 measures, 17 roll calls, 840 candidate records, 99 candidates, 557 area
tags.** Four measures were dropped. One hand-written record was retired as a
duplicate of a record this batch imported.

## ⭐ The three unemployment insurance bills get two different labels, on purpose

AB 167, AB 168 and AB 169 were drafted as one package and passed the Assembly
53-42 on the same day. They are not labeled alike, because they do not act on
the same thing.

- **AB 167** and **AB 169** act on **claimants**: more conduct counts as
  misconduct that ends benefits, claimants must list every job offer and recall
  each week, re-employment workshops become mandatory, and recovering money paid
  by mistake changes from "may" to "shall". Each narrows who receives the safety
  net or how easily, so `social_programs_and_welfare` yes = **against**.
- **AB 168** acts on **the agency**: identity checks to a federal standard,
  weekly matching against death, prison, new-hire and immigration records,
  notice to the Legislature before any fraud check is scaled back, longer phone
  hours during a surge, and free training for employers. Those are fraud
  controls and service standards, so `government_efficiency` yes = for.

Labeling all three the same way because they moved together would describe
the package, not the bills. The standing rule is to label what each act does.

AB 167 also contains a separate strand: any new federal unemployment benefit
would need approval from the Legislature's budget committee before the state
could pay it. It is described and not given its own label, because a legislative
approval requirement is a separation-of-powers mechanism no area describes, and
its practical effect is already carried by the `social_programs_and_welfare`
label.

## ⚠ AB 1027's LegiScan title describes a data request; the act is a benefits bill

LegiScan's title is *"Requiring information relating to the food stamp program
to be compiled and provided to the U.S. Department of Agriculture."* The enrolled
title adds *"and food stamp program work requirements, eligibility requirements,
and administration,"* and the added sections are the larger part of the act:

- the work requirement would stop applying at age 65 instead of 50
- the exemption for parents would cover a child under 14 instead of under 18
- noncitizens other than qualified aliens would be barred, with status checked
  at every enrollment

That makes AB 1027 the third Wisconsin measure in this pool whose title
describes less than its enrolled print, after SB 7 and SB 799.

It carries two labels. `social_programs_and_welfare` yes = against, for the work
and eligibility changes. `data_privacy` yes = against, because the act would
have given the U.S. Department of Agriculture the identity of every recipient
and all records back to 2020 that the department asked for — a new transfer of
personal data outside the state program.

## AB 165 carries the batch's only stated no-side position

It would have barred any city, village, town or county from spending its own
money on guaranteed income, meaning regular cash payments not tied to work or
training. It is single-subject, and its whole content is an anti-poverty cash
program, so a no vote evidences a position for letting local governments run
one. `social_programs_and_welfare` yes = against, `nay: for`. Same test as SB 389
and SB 431.

## The tax measures

- **AB 461** and **SB 36** would have copied the new federal deductions for
  overtime pay and for tips into Wisconsin income tax. `personal_income_tax_reduction`
  yes = for. AB 461 would also have kept the overtime subtraction after the
  federal deduction ends in 2028; the description says so.
- **SB 176** would have given a company a state tax credit of 6.32 percent of
  the broadband expansion grants and federal high-cost funding it receives,
  2026 through 2030. `public_infrastructure` yes = for. Its LegiScan title calls
  it an exemption; the enrolled act is a credit. Both its slots voted the
  Assembly's substitute, which is the enrolled text, so no version drop was
  needed. The Senate slot's tail says the Senate accepted the Assembly's rewrite,
  because that is what the Senate's 20-13 vote was.

## AB 162

It would have required every state agency running a job training or placement
program — including Wisconsin Works and the food stamp employment program — to
track and publish outcomes: employment six months and a year out, median
earnings, credentials earned. `government_efficiency` yes = for. Performance
measurement of service delivery is what the area names.

## The drops

- **AB 146** would have given employers at least 12 business days to answer the
  state's first request for information about a claim. An administrative timing
  rule with no direction in any area.
- **AB 241** would have allowed two apprentices per journeyworker in non-union
  construction apprenticeships, instead of one. No area describes apprenticeship
  ratios.
- **AB 269** would have declared app-based drivers and couriers not to be
  employees for workers' compensation, unemployment insurance and minimum wage
  law, and in the same act created portable benefit accounts companies could pay
  into for them. It reads two ways inside `social_programs_and_welfare`: a new
  benefit channel on one side, removal from three worker protections on the
  other. It also passed with bipartisan crossover, 56-36 and 17-15, which is
  consistent with a vote that did not divide on the area's axis.
- **SB 291** would have let businesses count child care they provide or reserve
  for employees toward the business development tax credit. A business tax credit
  that no area describes.

## The labels

| measure | area | yes means | no means |
| --- | --- | --- | --- |
| AB 162 | government_efficiency | for | — |
| AB 165 | social_programs_and_welfare | against | **for** |
| AB 167 | social_programs_and_welfare | against | — |
| AB 168 | government_efficiency | for | — |
| AB 169 | social_programs_and_welfare | against | — |
| AB 461 | personal_income_tax_reduction | for | — |
| SB 36 | personal_income_tax_reduction | for | — |
| SB 176 | public_infrastructure | for | — |
| AB 1027 | social_programs_and_welfare | against | — |
| AB 1027 | data_privacy | against | — |

## Quality, measured before importing

- Plain-language lint over all 34 descriptions: **0 warnings**. Longest sentence
  44 words.
- Every roll number, chamber, date and tally checked against `legislative_votes`
  before judging.
- Version check on all 17 slots: every one voted the enrolled text.
- British-spelling scan over the descriptions and these documents: clean.

## Reconciliation

- Plan: 840 inserts over 17 rolls.
- Real run: `outcomes {imported: 17}`, `actions {insert: 840}`, 0 errors,
  0 notified. Run stamp `2026-09-10T06:41:44.255Z`.
- Run-stamp predicate: 840. All Wisconsin roll-call records: 4,648, which is
  3,808 before this batch plus 840.
- Tags predicted independently and confirmed: 466 records on the yes side, plus
  46 second tags on AB 1027's yes side, plus 45 records on AB 165's no side,
  giving 557. The only tagged no-side records are AB 165's.
- 99 distinct candidates.

## ⚠ One duplicate retired

The sweep found a hand-written record, dated 9 September 2026, saying a member
"voted against passage of Wisconsin Assembly Bill 165 ... in the Assembly roll
call." AB 165 had exactly one Assembly vote, roll 1554274 on 22 April 2025, and
this batch imported a record for the same member on that roll. Same member, same
roll, same side: a true duplicate.

The hand-written record was retired, and the imported one kept, because the
imported record cites the roll call and states that the governor vetoed the bill.
The hand-written one does neither. It carried one area tag, which leaves with it.

- retired: `7d48b016-235f-44a0-9c77-4b0e40c2a1fa`
- kept: `601d368a-5a5f-431d-99cf-7f21397bb867`
- the reason is in `duplicate-retirements.json`, in the same shape Alabama used,
  so the same file can be applied at production promotion.
