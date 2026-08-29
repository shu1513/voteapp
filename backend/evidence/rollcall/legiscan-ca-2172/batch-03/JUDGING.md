# California batch-03 — judging notes

Source unchanged: the **chaptered text and its Legislative Counsel's Digest** on leginfo. What
changed after batch-02's review is how the digest is used — see the next section.

## The digest is a map, not the territory

Batch-02's review found that AB 572's chaptered digest paraphrased an operative exception loosely
(it said "custodial interrogation" where the statute says a family member who already received
substantially equivalent or Miranda advisements). So in this batch **every qualification that
appears in a description was read out of the enacted section**, with the digest used only to find
where to look:

- **AB 1127's nine exemptions** come from Penal Code 27595(c), not from the digest's "except as
  specified": pistols delivered to the dealer before 2026-01-01, sales to named law enforcement
  agencies, state agencies and the military for official duties, private-party transfers run
  through a dealer, transfers to gunsmiths, to other licensed dealers and out-of-state federal
  licensees, returns after safekeeping or a temporary prohibition, forensic laboratories, and
  active peace officers.
- **SB 634's carve-out** for plywood and heavy construction materials, and its list of what "basic
  survival" covers, come from Government Code 53069.44(b).
- **SB 524's vendor rule** has a second half the digest compresses into "except as provided": a
  vendor may still access the data to troubleshoot, mitigate bias, improve accuracy, or refine the
  system.
- **AB 628's exclusions** (permanent supportive housing and other named dwellings) and its
  preservation of a tenant's existing remedies come from the enacted section.

## Label calls

| measure | label | why this direction |
| --- | --- | --- |
| AB 1127 | `gun_control`/for | A dealer sales ban on pistols convertible to automatic fire — "regulate firearm access" is the area's wording. |
| SB 524 | `public_safety_and_crime_control`/for | The area names accountability; this puts disclosure, signature, draft retention, and an audit trail on AI-written police reports. The SB 627 / AB 572 reading. |
| AB 1036 | `public_safety_and_crime_control`/for | "Justice system performance": it widens access to exculpatory material after conviction, from 15-year serious or violent felonies to any state prison felony. |
| SB 518 | `civil_rights`/for | Creates a bureau inside the Civil Rights Department to verify descendant status and administer descendant benefits. |
| AB 246 | `social_programs_and_welfare`/for | "Support vulnerable populations through effective safety-net" — the defense exists only for households whose Social Security was interrupted by federal action. |
| SB 634 | `social_programs_and_welfare`/for | Stops local governments criminalizing the people who feed, clothe, and shelter the homeless. |
| AB 628 | `housing_affordability`/for | Raises the floor of what a renter is entitled to and puts the cost of a recalled appliance on the landlord. |
| AB 1056 | `environment_and_public_health`/for | Shrinks a gill net fishery the state already closed to new permits, by ending inheritance of permits outside the family. |
| SB 825 | `corporate_accountability`/for | Confirms the regulator may enforce the deceptive-practices ban against escrow agents and finance lenders. |

**Two `public_safety_and_crime_control` and two `social_programs_and_welfare` measures in one
batch** is deliberate: the area description fits both members of each pair squarely, and batch-02
set the precedent with three `corporate_accountability` measures.

## Traps caught while reading

- **AB 1127 is a DEALER ban, not a possession ban.** It bars licensed dealers from selling or
  transferring these pistols; it does not make owning one a crime. The descriptions say "a licensed
  firearms dealer".
- **AB 1127's penalties escalate** — up to $1,000, then up to $5,000 with possible licence loss,
  then a misdemeanor with revocation. All three are maxima, stated as "up to" (the SB 763 lesson
  from batch-02's review, where fixed amounts misstated statutory maxima).
- **AB 246 does not forgive rent, and the stay is capped.** Civil Code 1946.3(d) ends the stay at
  the EARLIER of 14 days after benefits are restored or six months after it issues — the first pass
  omitted the six-month maximum, implying indefinite protection if benefits never resume (fixed on
  review, see below). The tenant must pay everything past due, or agree a payment plan, within 14
  days of benefits resuming.
- **SB 518 is contingent on an appropriation** — the bureau exists on paper until the Legislature
  funds it, which the descriptions state.
- **AB 1056 is about inheriting permits**, not about banning gill nets. The fishery was already
  closed to new permits; the bill ends transfer on death or disability and limits transfers to
  family from 2027.
- **SB 825 is narrow and the description says so** — it confirms an existing enforcement power
  reaches two licensee categories, nothing more. This is the measure batch-01 held back as thin;
  stated precisely, it is honest rather than inflated.
- **The Assembly is called the Assembly**, as in both prior batches.

## Runs

| step | result |
| --- | --- |
| plain-language lint | 36 descriptions, **0 warnings** (one flagged at 46 words pre-import, split) |
| `rollcall:judge --dry-run` | 18 `dry_run` |
| `rollcall:judge` | 18 `updated` → queue 62 approved / 5,266 pending |
| `rollcall:legiscan:import --dry-run` | **645 planned inserts**, 1,588 unchanged, 0 errors, 0 notified |
| `rollcall:legiscan:import` | 62 `imported`, **645 inserts**, 1,588 unchanged, 0 errors, 0 notified |
| re-run `--dry-run` after the import | **2,233 unchanged**, 0 errors |

**Reconciled three ways.** `candidate_records` went 81,099 → 81,744 (+645); the run's predicate
`origin_run_id LIKE 'rollcall:CA:%:2172:%:2026-08-29T05:37:51.085Z'` returns 645 rows across 80
candidates; and the DRY RUN's stamp `2026-08-29T05:37:26.038Z` matches **zero** rows.

California now holds **2,233 roll-call records across 80 candidates** (batch-01 729 + batch-02 859 +
batch-03 645). No `citation URL fetch timed out` flake this time — all three runs were clean first
time. Prod untouched.

## Review response (2026-08-29) — AB 246 stay cap; a cross-pipeline collision surfaced

**The finding (P2, true).** Civil Code 1946.3(d) stays the eviction "until the earlier of" 14 days
after benefits are restored OR **six months after the stay is issued**; the first-pass descriptions
carried only the benefits-resume half, implying indefinite protection when benefits never resume.
Both descriptions now carry the cap. Judge: 2 `updated` / 16 `unchanged`; import rewrote AB 246's
67 records (57 Assembly + 10 Senate); re-run 2,233 `unchanged` / 0 errors; lint still 0 warnings.
Row count unchanged at 2,233 / 80 candidates.

**⚠ The same import run also rewrote 75 records whose judgments had NOT changed** — and the
investigation matters more than the fix. A **plain-language backfill sweep**
(`plainLanguageBackfill.ts`, transition reason `plain_language_rewrite`) ran earlier the same day
and rewrote 952 candidate-record descriptions in place in cursor order, including roll-call records
in NINE jurisdictions (GA 525, TX 65, PA 58, US 27, OH 16, IL 7, ME 6, FL 3, TN 2 surviving, plus
78 CA). The roll-call importer treats `judgments.json` as canonical and reverts any drift, so this
run **reverted the 78 CA records the sweep had reached** (proven by joining the sweep's
`new_record_identity_key` to this run's `old_record_identity_key`: 78 matches). The sweep's texts
are not recoverable — identity transitions store key hashes, not prior text.

The two pipelines have incompatible ownership models: the backfill rewrites descriptions in place
(AI rewrite + independent-provider verify, identity key recomputed); the importer enforces
byte-identity with the approved, sha-pinned judgments. Every future re-import of ANY LegiScan/OH/US
jurisdiction will revert whatever the sweep has rewritten there, and the sweep will re-rewrite on
its next pass — a permanent fight unless one side yields. Resolving that (skip
`origin='rollcall_import'` in the backfill, or fold the backfill's style into judgments) is an
operator decision recorded here for the roll-call side; this batch takes no position beyond
restoring judgment text, which is what the importer is built to do.
