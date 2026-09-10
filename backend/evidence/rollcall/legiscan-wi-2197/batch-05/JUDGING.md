# Wisconsin batch-05 — how each measure was judged

Judged 2026-09-10. Local database only; production holds no Wisconsin roll-call
records.

Every description was written from the enrolled print.

## Result

**7 measures, 10 roll calls, 550 candidate records, 99 candidates, 329 area
tags.** Four measures were dropped and one roll call was dropped on the version
rule.

## ⚠ Why AB 100 and AB 102 are dropped and AB 103, AB 104 and SB 405 are not

Wyoming SF0044 was dropped because `civil_rights` reads two ways **of comparable
weight** on a sports bill: fair competition for women, and excluding a class of
students. Both of those are equal-treatment claims, and the area names equal
rights. Neither side's claim is foreign to the area, so the area cannot say which
way a yes vote points. The operator has not yet ruled.

AB 100 and AB 102 carry exactly that structure. They designate teams by
biological sex and then give a female student a right to sue if a male student
plays on a women's team. They are dropped on SF0044's reasoning, pending the
same decision.

The other three gender-related measures have a different structure. In each, the
claim on the other side of the scale is **not an equal-treatment claim**:

- **AB 103** requires notarized parental permission before school staff may use
  a name or pronouns that do not match a minor's biological sex. The competing
  claim is parental authority, which is real but is not an equal-rights claim
  and is not named in the area.
- **AB 104** bans puberty blockers, cross-sex hormones and gender surgery for
  minors, with mandatory and, for physicians and nurses, permanent license
  revocation. The competing claim is child medical safety, which is a health
  claim, not an equal-rights one.
- **SB 405** creates a right to sue a provider for injury from a gender
  transition procedure performed on a minor, with punitive damages and until
  age 33. The competing claim is again medical safety.

With only one equal-treatment claim present — the measure treats one class of
people differently — `civil_rights` reads one way, and yes = **against**. All
three are `nay: null`, because a no vote could rest on medical judgment, parental
rights or federal law rather than on the area.

If the operator rules that SF0044 is a yes = against vote, AB 100 and AB 102
join these three. If the operator rules that it stays dropped, these three are
unaffected, because the reasoning that distinguishes them does not depend on the
ruling.

## ⚠ SB 799 carries a section its LegiScan title does not mention

LegiScan's title is *"Parental access to health records of minors."* The Assembly
replaced the whole bill with a substitute, and the enrolled print adds a second,
unrelated section: Milwaukee's Common Council could change a police or fire
department policy only by unanimous vote, instead of two-thirds.

That is the second Wisconsin measure where the title describes less than the
enrolled act, after SB 7 in batch-04.

**The Senate slot is dropped.** The Senate passed SB 799 18-15 on 18 February
2026, and the Assembly substituted the whole bill the next day. The Senate then
accepted the substitute without a recorded divided vote. Only the Assembly's
54-41 vote on the substitute survives.

SB 799 is labeled `data_privacy`, yes = **against**. The act removes a
developmentally disabled 14-year-old's right to object to a parent seeing their
treatment records, gives parents the minor's HIV test results from age 14, and
gives parents access at any time to the minor's online records. Each is a loosened
limit on who may see a minor's health data, which is the area's own subject. The
Milwaukee section is described but not labeled: no area describes a city
council's voting threshold.

## SB 431 carries the batch's only stated no-side position

Wisconsin's fair employment law treats an arrest record as protected, with an
exception for a pending **criminal** charge that is substantially related to the
job. SB 431 would have deleted the word "criminal", so the exception would reach
any pending charge, including a non-criminal one.

It is `civil_rights`, yes = against. It is single-subject and its whole content
is an anti-discrimination protection, so a no vote evidences a position for
keeping that protection as it stands: `nay: for`. That is the same test used for
SB 389 in batch-02 and SB 825 in batch-01.

## The two health measures

- **SB 4** would have set rules for direct primary care, where a patient or
  employer pays a subscription fee instead of per visit. `healthcare_affordability`
  yes = for: it adds a way to pay for primary care, and it bars a provider from
  refusing a patient or ending an agreement because of the patient's health.
  The standard objection is that it is not insurance; the act requires the
  agreement to say so in writing.
- **SB 214** would have let an out-of-state provider treat Wisconsin patients by
  telehealth after registering, with malpractice cover and a five-year clean
  discipline record. `healthcare_affordability` yes = for, because it widens
  access to care.

## The drops

- **AB 100** and **AB 102** — see above. Pending the SF0044 decision.
- **SB 652** would have rewritten the University of Wisconsin's and technical
  colleges' minority student grant and recruitment programs into programs for
  disadvantaged students. Dropped on filter 5. Inside `civil_rights` it reads
  both ways: ending race-specific programs is an equal-treatment claim, and so is
  keeping them as remedies. Same structure as SF0044.
- **SB 417** would have required nursing homes, assisted living facilities and
  hospitals to admit at least one visitor during a disease outbreak. Dropped on
  filter 5. No area describes a visitation right, and reading it under
  `environment_and_public_health` as a vote against infection control would
  misstate the vote.

## The labels

| measure | area | yes means | no means |
| --- | --- | --- | --- |
| AB 103 | civil_rights | against | — |
| AB 104 | civil_rights | against | — |
| SB 405 | civil_rights | against | — |
| SB 431 | civil_rights | against | **for** |
| SB 799 | data_privacy | against | — |
| SB 4 | healthcare_affordability | for | — |
| SB 214 | healthcare_affordability | for | — |

## Quality, measured before importing

- Plain-language lint over all 20 descriptions: **0 warnings**. Longest sentence
  43 words. One SB 405 sentence measured 48 words on the first run and was split.
- Every roll number, chamber, date and tally checked against `legislative_votes`
  before judging.
- British-spelling scan over the descriptions and these documents: clean.

## Reconciliation

- Plan: 550 inserts over 10 rolls.
- Real run: `outcomes {imported: 10}`, `actions {insert: 550}`, 0 errors,
  0 notified. Run stamp `2026-09-10T06:35:56.344Z`.
- Run-stamp predicate: 550. All Wisconsin roll-call records: 3,808, which is
  3,258 before this batch plus 550.
- Tags predicted independently and confirmed: 285 records on the yes side each
  carry one tag, plus the 44 records on SB 431's no side, giving 329. A check
  confirmed SB 431's are the only tagged no-side records.
- 99 distinct candidates.
- **Duplicate sweep: nothing to retire.** No record outside the roll-call runs
  mentions any of these seven Wisconsin measures.
