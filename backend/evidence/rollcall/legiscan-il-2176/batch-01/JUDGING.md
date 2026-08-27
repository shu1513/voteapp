# Illinois batch-01 — judging notes

Every judgment was written from the **Legislative Reference Bureau synopsis
of the version the voting chamber actually had in front of it**, read from
`https://ftp.ilga.gov/Legislation/104/BillStatus/XML/10400<BILL>.xml`. No AI
provider was called at any point.

## Version check — done for all 22 rolls, and it mattered three times

The BillStatus XML's action trail states outright which amendments were
`Adopted` and which were `Tabled`, `Postponed` or `Lost`, so the check is
exact rather than inferred.

**Eight measures: both chambers voted the same text.** S.B. 8 (Senate adopted
CA1 + FA3; the House tabled its only amendment), H.B. 1373 (House adopted
CA1 by voice; the Senate tabled both of its filed amendments), S.B. 3341
(Senate adopted FA2; the House tabled FA1), H.B. 5095, S.B. 1976 and S.B.
3772 (one chamber adopted a replace-everything amendment, the other passed
the engrossed text unchanged), and H.B. 3489 and H.B. 4339, where no
amendment was ever filed.

**Three measures diverged between chambers, and each roll got its own
description saying so:**

- **S.B. 3777** is the big one. The Senate passed the *introduced* text,
  which redefined "unlawful discrimination" itself to mean discrimination by
  purpose **or effect**. The House then replaced everything after the
  enacting clause with a narrower, explicit burden-shifting test — job
  relatedness and business necessity, with a less-discriminatory-alternative
  rebuttal — and named citizenship status, family responsibilities, work
  authorization status, arrest record and conviction record as covered bases.
  The Senate concurred 41-14. The senate roll's description says it passed
  "this version" and states what the House later did; the house roll
  describes the enacted framework.
- **S.B. 2339.** The Senate text both restricted employers' use of
  employment eligibility verification systems *and* added the new employee
  protections. House Floor Amendment 2 **repealed the two restriction
  sections** and removed the petty-offense penalty; the Senate concurred in
  that narrower version. Both descriptions name the repeal.
- **H.B. 5295.** The House voted the text as changed by House Floor Amendment
  1; the Senate then adopted Committee Amendment 1, which reworked the
  definitions and the conditions under which a covered entity may receive
  segregated information, and the House concurred.

This is the Texas S.B. 379 lesson recurring: when a chamber votes
pre-amendment text, that roll needs its own description naming what later
changed.

## Stance directions follow the AREA DESCRIPTION, not the bill's framing

The recorded rule, and Illinois mirror-images Texas on almost every one:

- `gun_control` reads "Regulate firearm access through background checks,
  licensing, and **safe-storage requirements** to reduce gun violence" — so
  Illinois' Safe Gun Storage Act is `gun_control` / **for**, where Texas'
  firearm bills were `gun_control` / against. Same area, opposite direction,
  because the bills point opposite ways.
- `immigration` reads "Welcome immigration through a lawful, orderly, and
  humane system" — so Texas' S.B. 8 enforcement mandate was **against** and
  Illinois' S.B. 2339, which caps how far an employer may push work
  authorization re-verification and gives the employee notice and response
  rights, is **for**.
- `corporate_accountability` for S.B. 1976 follows the Georgia S.B. 50
  precedent in reverse: loosening worker protections scored *against*, so a
  floor under worker-safety rules scores **for**.
- `civil_rights` for H.B. 5095 and S.B. 3777 follows the Ohio S.B. 1 and
  Texas S.B. 12 precedent, again inverted — those restricted, these extend.

All eleven measures carry a stance. Only `general` and
`integrity_and_ethics` may omit one, and nothing here needed that escape.

## Label calls worth reusing

- **Three contraception/abortion measures, one area.** H.B. 5295 is as much a
  data-privacy bill as a reproductive one, and H.B. 3489 is as much a
  healthcare-access bill; `womens_reproductive_rights` is the more specific
  area in each case and it is the one that carries the direction honestly.
- **H.B. 1373 is `gun_control`, not `public_safety_and_crime_control`.** It
  mandates ATF eTrace submission and reciprocal trace-data sharing for every
  crime gun — firearm-trafficking detection, which the area's own purpose
  clause ("to reduce gun violence") covers.
- **H.B. 4339 is `election_integrity` / for.** Registering eligible
  graduating students goes to accuracy of the rolls and public trust, the
  area's stated aims, and follows the Texas H.B. 493 / H.B. 5115 / S.B. 510
  precedent of mapping election-administration bills there.
- **S.B. 3772 is `environment_and_public_health` / for**, not a
  government-efficiency bill, even though half of it stands up an Office of
  Environmental Justice: the operative half conditions air-pollution
  construction permits in overburdened communities.

## Descriptions

They end **"and it became law"**, not "was signed into law" — LegiScan status
4 and the ILGA `Public Act` action both record enactment, not whether the
governor signed. This follows the Texas batch-02 convention.

S.B. 8's description carries the statute's three exceptions (firearm under
the owner's control, lawful self-defense, unlawful entry) because the statute
states them as conditions on the ban. That is the Texas S.B. 2972 rule:
**when a statute qualifies a prohibition, the description must carry the
qualifier.**

## Runs

| step | result |
|---|---|
| `rollcall:judge --dry-run` | 22 rows, all `approved` |
| `rollcall:judge` | `{"updated": 22}` — queue went to 22 approved / 8,353 pending |
| `rollcall:legiscan:import --dry-run` | 22 files, **1,364 planned inserts**, 0 errors, 0 notified |
| `rollcall:legiscan:import` | 22 files all `imported`, **1,364 inserts**, 0 errors, 0 notified |
| re-run | all **1,364 unchanged** |

**Reconciled three ways.** `candidate_records` went 66,606 → 67,970 (+1,364);
the run's provenance predicate

```sql
origin_run_id LIKE 'rollcall:IL:%:2176:%:2026-08-27T01:40:23.513Z'
```

returns exactly **1,364** rows over **132 distinct candidates** — which is
every candidate the crosswalk maps, so there is no fan-out gap (Illinois'
Speaker does cast recorded votes, unlike Texas' Burrows and Georgia's Burns).
The dry run's own stamp `2026-08-27T01:40:04.557Z` matches **zero** rows,
which is positive proof `--dry-run` is inert.

The per-run `startedAt` stamp is the only batch key — scoping to the session
will not work, because batch-02 will be session 2176 too. Note that a rewrite
re-stamps `origin_run_id` with the rewriting run's timestamp, so if this
batch is ever rewritten in place, use the grouped-stamp form.

**Prod untouched.** Promotion is a separate step.

## Review response (2026-08-27)

**Comma splice in every description — FIXED, and the fix path is worth
recording.** Every yea/nay description joined its two sentences with a comma
("…from 72 to 48 hours, The Illinois Senate passed it…"). The fix looked
routine — edit `judgments.json`, re-judge, re-import — but the re-import
reported **1,362 `unchanged`**: `buildCandidateRecordIdentityKey` normalizes
text with `[^a-z0-9]+ → " "`, so a punctuation-only edit is
**identity-invariant** and the fan-out correctly sees the old rows as already
matching. (Texas batch-02's description edits rewrote 361 records because
they changed *words*, which changes the key.) Since the pipeline's own
identity model declares the two texts the same record, the repair was applied
directly in SQL — `replace(description, ', The Illinois', '. The Illinois')`
on the 1,362 rows, which preserves every `record_identity_key` byte-for-byte
— after the judge run had updated the 22 `legislative_votes` sentences. A
final import run converged: **22 files, all 1,364 `unchanged`**
(`import-verify-report.json`). Rule for next time: **a punctuation-only
description fix never propagates through re-import; fix the judgments file
and the live rows together.**

The 2 records the 18:51 run counted as `rewrite` (Paul Jacobs and Regan
Deering, H.B. 4339 nay side) were collateral from a concurrent
`source_url_repair` run in another session against the shared local DB — it
had re-keyed those two rows, and the import restored the canonical key,
picking up the fixed text in the same write.

**S.B. 3777 house vote date — LegiScan says 2026-05-31, the official record
says June 1; kept, documented.** The ILGA BillStatus XML dates the 72-38
House third reading **6/1/2026**; the whole LegiScan dataset contains **no
June dates at all** — the House ran its sine-die session past midnight and
LegiScan stamped the legislative day. A full audit of all 22 rolls against
the ILGA XML found **21/22 exact matches**; this is the only skew. The date
is kept as LegiScan's because the row's evidence sha pins the roll_call
element (date field included) and a re-fetch would silently revert any
hand-edit; the record's `source_url` is the LegiScan page, which displays
the same date it asserts, so the citation stays self-consistent; and no
description states a date. The skew is recorded here and in the README —
promotion-side consumers wanting journal-exact dates should prefer the ILGA
BillStatus XML. Auditing every selected roll's date against the ILGA XML is
now part of the batch recipe.

A second review pass asked for "an explicit reviewed override for
`event_date`". That mechanism does not exist, and every hand-edit that would
simulate it is provably unsafe — a records-only SQL fix makes the next
import re-run insert 91 duplicates (the duplicate scan is date-scoped), and
a row-plus-judgment fix makes batch-01 permanently fail the importer's
evidence-date cross-check. The override is parked as a designed code
follow-up in `../CODE-FINDINGS.md` §1, to be built after the parallel state
campaigns land; 31 pending 2026-05-31 rolls in the worklist are the
remaining suspect pool.

## Operational notes

1. **A real re-run overwrites `import-report.json`.** A *dry* re-run writes
   `import-dry-run-rerun-report.json` and leaves the ledger alone, but a real
   one does not — the idempotency re-run here replaced the ledger's
   `{"insert": 1364}` with `{"unchanged": 1364}`, and it was restored from
   the run's stdout. Capture the ledger before re-running. The committed
   `import-report.json` is the original insert ledger;
   `import-verify-report.json` is the post-review convergence run.
2. The shared local database is also in use by the parallel state sessions —
   a concurrent `manual:candidate-records:` write and a `source_url_repair`
   run both landed during this batch's verification steps. The import's own
   accounting was unaffected each time.


## Official-date override applied (2026-08-27)

The House roll on S.B. 3777 (roll_call_id 1721227) carries
`official_vote_date: 2026-06-01` in `judgments.json`, applied with the
override mechanism from CODE-FINDINGS.md §1 once it was built (migration
257). LegiScan stamps the vote 2026-05-31 — the legislative day of the
overnight sine-die session — while the official record dates the House's
Third Reading passage 6/1/2026: the ILGA BillStatus XML action trail,
https://ftp.ilga.gov/Legislation/104/BillStatus/XML/10400SB3777.xml
("Third Reading - Short Debate - Passed 072-038-000", 6/1/2026). The row's
`vote_date` stays the LegiScan value the sha-pinned evidence asserts; the
re-run import rewrote the 91 fanned-out records onto 2026-06-01 in place
(same row ids, 91 `rollcall_normalization` identity transitions logged).

Ledgers: `import-report.json` stays the original insert ledger; the override
re-run is `import-override-rerun-report.json` (`{"unchanged": 1273,
"rewrite": 91}`, no inserts, no notifications), and
`import-dry-run-rerun-report.json` is the post-override convergence check
(`{"unchanged": 1364}`).
