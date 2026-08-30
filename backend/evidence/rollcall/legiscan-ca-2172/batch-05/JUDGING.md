# California batch-05 — judging notes

Same method as batch-04: chaptered text on leginfo, every qualification read from the enacted
section rather than the digest, plain English in the first draft, `nay` stated on purpose.

## Label calls

| measure | label | why this direction |
| --- | --- | --- |
| AB 1079 | `civil_rights`/for | The California Voting Rights Act is anti-discrimination law about vote dilution of protected classes; this stops an appeal automatically freezing the remedy. "Anti-discrimination enforcement" is the area's own wording. |
| SB 22 | `corporate_accountability`/for | Consumer protection: a customer can take the money back instead of a small balance quietly staying with the retailer. |
| AB 931 | `corporate_accountability`/for | Disclosure, a cancellation right, and a bar on the funder steering the case — consumer protection against predatory lawsuit lending. |
| AB 1487 | `social_programs_and_welfare`/for | "Support vulnerable populations through effective safety-net" — job training, resettlement, and youth diversion grants for a specific population. |
| SB 635 | `data_privacy`/for | "Clear limits on collection, sharing, and misuse": the mechanism is a bar on handing over identifying records without a subpoena or warrant. |
| SB 805 | `public_safety_and_crime_control`/for | Accountability, the SB 627 / AB 572 / AB 847 reading — this is the identification companion to SB 627's mask rule. |
| SB 358 | `housing_affordability`/for | Impact fees are a direct cost of building; this makes it harder to deny the lower rate to housing designed to generate fewer car trips. |

## Traps caught while reading

- **SB 805's ID duty has five narrow exemptions**, all from the enacted policy section: undercover
  and investigative work; named plainclothes roles inside listed state agencies and their federal
  equivalents; protective equipment that prevents display; urgent danger to people or property or an
  escaping suspect; and a specific, stated reason to believe identification would endanger the
  officer. The descriptions carry them rather than claiming a flat rule.
- **AB 931's economics are the point.** What the funder is owed is a set amount fixed by how long
  the case runs, explicitly **not** a percentage of the recovery, and the company may not pay
  referral fees to attorneys. Describing it only as "disclosure rules" would have missed the
  substance.
- **AB 1079 is not absolute.** The trial court may still pause its own order, and must consider it
  when the Secretary of State certifies a pause is needed for orderly elections; cases begun on or
  before 2026-01-01 are untouched.
- **AB 1487 is contingent on an appropriation** — it widens what the fund *may* pay for, and the
  fund makes grants only when the Legislature sets money aside.
- **SB 635 yields to law.** The bar on disclosure gives way where state or federal law requires
  disclosure; it stops *voluntary* cooperation, not compelled process.
- **SB 22 has an exemption and a start date** — donated cards are out, and it begins 2026-04-01.

## A self-check worth recording

The first draft slipped into British spellings — "misdemeanour", "itemised", "programmes",
"licence", "organisation" — which no lint or jargon check looks for, and which would have been the
only five such spellings in a 3,548-record American-spelled corpus. Caught before the import by
diffing the new vocabulary against the existing records. **A spelling-register check belongs
alongside the jargon check when writing for an established corpus.**

## Runs

| step | result |
| --- | --- |
| plain-language lint | 26 descriptions, **0 warnings** (one 52-word sentence split pre-import) |
| `rollcall:judge --dry-run` | 13 `dry_run` |
| `rollcall:judge` | 13 `updated` → queue 96 approved / 5,232 pending |
| `rollcall:legiscan:import --dry-run` | **509 planned inserts**, 3,039 unchanged, 0 errors, 0 notified |
| `rollcall:legiscan:import` | 96 `imported`, **509 inserts**, 3,039 unchanged, 0 errors, 0 notified |
| re-run `--dry-run` after the import | **3,548 unchanged**, 0 errors |

**Reconciled three ways.** `candidate_records` went 110,517 → 111,026 (+509); the run's predicate
`origin_run_id LIKE 'rollcall:CA:%:2172:%:2026-08-30T05:34:14.517Z'` returns 509 rows across 80
candidates; and the DRY RUN's stamp `2026-08-30T05:33:40.939Z` matches **zero** rows.

California now holds **3,548 roll-call records across 80 candidates** (729 + 859 + 645 + 806 + 509)
across 49 measures. All runs clean first time. Prod untouched.
