# California batch-04 — judging notes

Source unchanged: the **chaptered text and its Legislative Counsel's Digest** on leginfo, with every
qualification read out of the enacted section rather than the digest's paraphrase (the AB 572
lesson). Written in plain English from the first draft, not rewritten into it afterwards.

## The `nay` side is now stated on purpose

Between batch-03 and this batch, `parseRollCallLabels` gained a `nay` field and `rollcall:judge`
gained a gate that **refuses a new judgment which leaves `nay` unstated on a stance area**. The
contract's reasoning is in `rollCallFanOut.ts`: a no vote is not automatically the opposite stance on
the area's whole goal — "a no on one election bill is not 'Opposes Election Integrity'".

**Every label in this batch states `"nay": null` deliberately.** Nay voters keep their record, which
describes exactly what they voted against, and take no stance tag. That is the honest reading for all
eleven: each is a specific bill, and opposing it is not evidence of opposing housing affordability,
data privacy, or reproductive rights as goals. A future batch may well state an explicit nay stance
where a vote really is a referendum on the area itself; none here is.

Batches 01-03 were judged before the field existed. Their rows carry no `nay` key, which the parser
reads as null — the same behaviour, so nothing about them changed.

## Label calls

| measure | label | why this direction |
| --- | --- | --- |
| SB 497 | `womens_reproductive_rights`/for | The area reads "protect legal access to reproductive healthcare **and individual bodily autonomy**". California's "legally protected health care activity" and "sensitive services" cover reproductive and gender-affirming care alike, and this is a shield against out-of-state prosecution of both. |
| SB 59 | `data_privacy`/for | "Clear limits on collection, sharing, and misuse" — the mechanism is sealing court records and barring their publication. |
| AB 847 | `public_safety_and_crime_control`/for | Accountability, the SB 627 / AB 572 / SB 524 reading: civilian oversight boards get the records they need to investigate officer conduct. |
| AB 1312 | `healthcare_affordability`/for | Shifts the burden of finding charity care from patient to hospital, which is "reduce out-of-pocket costs". |
| SB 707 | `anti_corruption`/for | The area names **transparency** first; this is open-meeting access for people who cannot attend in person. |
| AB 1362 | `corporate_accountability`/for | Brings farmworker recruiters under the registration regime that already covers non-farm recruiters — the AB 692 / AB 858 worker-protection reading. |
| AB 1340 | `corporate_accountability`/for | Requires the companies to bargain in good faith with a certified driver organization. |
| AB 454 | `environment_and_public_health`/for | Pins California's migratory bird protection to the federal list as of 2025-01-01 so a federal rollback cannot narrow it. |
| AB 309 | `environment_and_public_health`/for | Harm reduction; makes permanent a rule that would otherwise have lapsed at the end of 2025. |
| SB 262 | `housing_affordability`/for | Widens what earns a prohousing rating, and with it preference for state housing money. |
| AB 727 | `environment_and_public_health`/for | Suicide prevention is "community health through … prevention". `public_education_quality` was considered and rejected: that area is about teaching, standards, and outcomes, and a phone number printed on an ID card is not that. |

## Traps caught while reading

- **SB 497 is bounded by federal law.** Its bar on cooperating with a *federal* law enforcement
  agency applies only "to the extent permitted by federal law" — the descriptions say "as far as
  federal law allows" rather than implying California can override federal process.
- **SB 707 is temporary and does not reach every local body.** It runs 2026-07-01 to 2030-01-01 and
  covers city councils and county boards in jurisdictions of 30,000+, city councils in counties of
  600,000+, and larger special districts. A flat "local governments must offer remote access" would
  have been wrong on both counts.
- **AB 1362 starts later than it reads.** H-2B recruiters were already covered; the H-2A
  agricultural extension begins 2027-07-01.
- **AB 454 does not displace federal permissions.** Federal rules allowing certain takings still
  apply unless they conflict with California's fish and game law; and what it protects is the
  federal list *as it stood before 2025-01-01*, plus later additions — the point of the bill.
- **AB 1340 does not change employment status.** Drivers remain independent contractors under
  Proposition 22; the bill adds bargaining machinery, and the descriptions say so.
- **AB 309 is a sunset removal**, not a new right; the underlying rule already existed.
- **AB 727 adds to what is already required** — the 988 line is already on those cards; this adds an
  LGBTQ+ line, phone and text.

## Runs

| step | result |
| --- | --- |
| plain-language lint | 42 descriptions, **0 warnings**, longest sentence 39 words |
| `rollcall:judge --dry-run` | 21 `dry_run` |
| `rollcall:judge` | 21 `updated` → queue 83 approved / 5,245 pending |
| `rollcall:legiscan:import --dry-run` | **806 planned inserts**, 2,233 unchanged, 0 errors, 0 notified |
| `rollcall:legiscan:import` | 83 `imported`, **806 inserts**, 2,233 unchanged, 0 errors, 0 notified |
| re-run `--dry-run` after the import | **3,039 unchanged**, 0 errors |

**Reconciled three ways.** `candidate_records` went 108,712 → 109,518 (+806); the run's predicate
`origin_run_id LIKE 'rollcall:CA:%:2172:%:2026-08-30T02:27:37.306Z'` returns 806 rows across 80
candidates; and the DRY RUN's stamp `2026-08-30T02:27:09.903Z` matches **zero** rows.

California now holds **3,039 roll-call records across 80 candidates** (729 + 859 + 645 + 806). All
runs clean first time — no legiscan.com timeout this round. Prod untouched.
