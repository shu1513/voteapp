# Illinois batch-02 — judging notes

Every judgment was written from the **Legislative Reference Bureau synopsis of
the version the voting chamber actually had in front of it**, read from
`https://ftp.ilga.gov/Legislation/104/BillStatus/XML/10400<BILL>.xml`. Source
reading was fanned out to eight Opus subagents, four measures each, each
returning a structured version-check, an official-date check, and a flagged
`runs_both_ways` field; the labels and descriptions here are mine. No AI
provider inside the VoteApp pipeline was called at any point.

## ⚠ The find that matters most: a roll can be a vote on a different bill

**H.B. 2568's house roll 1545195 (75-38, 2025-04-10) was cast on the trust code
and unclaimed property text** — Illinois Trust Code recordkeeping, a 20-year
abandonment presumption, a licensing scheme for unclaimed-property finders. The
Senate then replaced everything after the enacting clause with the **Equality
for Every Family Act**, and none of the trust provisions survive in the law.

Importing that roll under the enacted subject would have told ~92
representatives' constituents that they voted on family-parentage equality when
they voted on unclaimed property. **Only the senate roll was imported.**

This is a level beyond the version-split hazard batch-01 recorded. There the
chambers voted different drafts of the same bill; here they voted *different
bills sharing a number*. The check that catches it is the same one — read the
synopsis stack and the amendment trail for the date of each roll — but the
failure mode is far worse, so it is now the first thing to verify on any
Illinois measure whose chambers voted months apart.

The same shape appears three more times in this batch and is handled correctly
because the divided-vote gate happened to exclude the stale roll: H.B. 1312's
first house vote (106-0, on a POW/MIA commemorative day), H.B. 5090's first
house vote (101-1, on construction procurement) and H.B. 1836's first house
vote (107-6, on eavesdropping and grand juries) were all unanimous or nearly
so, so they never entered the divided pool. **That is luck, not a safeguard.**

## Date skew: it is per-roll, and it recurs

Batch-01 found one skewed roll (S.B. 3777). This batch audited all 54 selected
rolls against the ILGA action trail and found **three more**:

| roll | LegiScan | official ILGA |
|---|---|---|
| H.B. 5024 senate 1716945 | 2026-05-31 | **2026-06-01** |
| S.B. 2437 senate 1582772 | 2025-05-31 | **2025-06-01** |
| H.B. 5090 house 1719024 | 2026-05-31 | **2026-06-01** |

Critically, **five other rolls on those same two calendar days did not skew**
(H.B. 3247 S, H.B. 4379 S, H.B. 3772 S, H.B. 4571 S, S.B. 2437 H). The feed
dates the *legislative day*, and only the votes actually taken after midnight
land on the wrong calendar date — so the audit cannot be shortcut to "check
sine-die days" and must be done per roll.

Unlike batch-01, these three were **held out of the import** rather than
imported with a known-wrong date, and are marked `pending:date-skew` in the
worklist. That is the more conservative call, taken because review flagged the
batch-01 skew twice.

**The override now exists locally.** While this batch was being judged, the
background session implementing `CODE-FINDINGS.md` §1 added
`legislative_votes.official_vote_date`, set it for S.B. 3777's house roll, and
rewrote those 91 records to 2026-06-01 (visible as run stamp
`2026-08-27T20:17:53.243Z`). These three rolls are its natural next customers
once that work merges. **Note the cross-branch hazard this creates: an importer
without the override code, run against a database that has it, would look for
existing records on the raw `vote_date`, find none, and insert duplicates.** Do
not re-run batch-01's import from a branch lacking the override.

## Descriptions that had to be pulled back from the title

Illinois gut-and-replace hit five of the 29 measures. Beyond H.B. 2568 above:

- **H.B. 1312 "POW MIA RECOGNITION DAY"** is the Illinois Bivens Act plus a
  courthouse civil-arrest privilege and immigration-status confidentiality
  duties for hospitals, public colleges and day care centers.
- **H.B. 5090 "PROCUREMENT-CONSTRUCTION"** is the Transportation Network Driver
  Labor Relations Act — rideshare collective bargaining — inserted on the last
  night of session.
- **H.B. 1836 "EAVESDROP-STATEWIDE GRAND JURY"** is the Clean Slate Act; the
  eavesdropping and grand jury provisions are not law.
- **H.B. 460 and S.B. 405, both "EDUCATION-TECH"**, are a RISE Act student-aid
  change and a school-counseling mandate respectively.

## Where the description had to be narrower than the bill's reputation

The batch-01 rule — when a statute qualifies a provision, the description must
carry the qualifier — did real work here:

- **H.B. 4758** regulates **job postings only**. The introduced bill would have
  barred refusing to hire someone for lacking a driver's licence; that was
  stripped before either chamber voted. The description says so.
- **S.B. 191**'s seat-belt mandate starts **2031**, exempts leased buses, and
  expressly imposes no duty on anyone to ensure a belt is fastened.
- **H.B. 2425** does not reach people **actively incarcerated** on a felony.
- **H.B. 4571**'s affordability ceiling is **140%** of area median income — the
  as-introduced 150% figure would have been wrong.
- **S.B. 2164**'s misdemeanor and felony provisions are **pre-existing law
  carried forward**, not new penalties, and are not attributed to the bill.
- **H.B. 5093**'s senate description names the nonimmigrant-status exclusion
  the house version did not contain.

## Version splits named per chamber

Three measures, following batch-01's precedent: **H.B. 5093** (senate added a
nonimmigrant carve-out, RISE Act aid eligibility, and delayed dates),
**S.B. 405** (senate voted permissive "may include", house made it mandatory
"shall address"), and **H.B. 4379** (senate reworked which buildings count).

## Label calls worth reusing

- **`immigration` is the batch's largest area at 13 rolls, all `for`** — the
  exact mirror of Texas, where the same area scored `against`. The direction
  follows the area description ("Welcome immigration through a lawful, orderly,
  and humane system"), not the party of the sponsor.
- **H.B. 4834 is `data_privacy`, not an identity area.** It exempts testosterone,
  estrogen and GnRH analogues (gender-affirming care) *and* mifepristone and
  misoprostol (abortion medication) from the Prescription Monitoring Program.
  No single identity area covers both; the mechanism — keeping the prescriptions
  out of a searchable state database — is what the bill actually does.
- **H.B. 1189 is `reduce_wealth_gap`, not `corporate_accountability`.** The
  operative effect is higher wages for workers, not a compliance duty on firms.
- **H.B. 2517 is `healthcare_affordability`** on the area's "quality care"
  clause, not `womens_reproductive_rights`: it is a licensure training mandate,
  not access to reproductive care.
- **H.B. 4844 is `corporate_accountability`** (an employer wage mandate), not
  `cost_of_living_reduction`.

## Runs

| step | result |
|---|---|
| `rollcall:judge --dry-run` | 54 rows, all `approved` |
| `rollcall:judge` | `{"updated": 54}` — queue to 76 approved / 8,299 pending |
| `rollcall:legiscan:import --dry-run` | 54 files, **3,319 planned inserts**, 0 errors, 0 notified |
| `rollcall:legiscan:import` | 54 files all `imported`, **3,319 inserts**, 0 errors |
| re-run | all **3,319 unchanged** (`import-verify-report.json`) |

**Reconciled three ways.** `candidate_records` went 68,634 → 71,953 (+3,319);
the run's provenance predicate

```sql
origin_run_id LIKE 'rollcall:IL:%:2176:%:2026-08-27T20:26:44.466Z'
```

returns exactly **3,319** rows over **132 distinct candidates**. Illinois now
holds **4,683** roll-call records, which reconciles against batch-01 exactly:
1,271 still on batch-01's original stamp + 91 rewritten by the override session
+ 2 rewritten by the batch-01 punctuation repair + 3,319 from this batch.

**Prod untouched.**
