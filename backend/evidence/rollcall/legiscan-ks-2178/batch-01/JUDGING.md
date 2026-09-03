# Kansas batch-01 — how these were judged

## Sources

Every description was written from the **Kansas Legislative Research Department
Summary of Legislation** entry for the enacted act, checked against the **enrolled
act** itself. The summary is official, nonpartisan, section by section, and — unlike
Texas's bill analyses or Missouri's committee summaries — it carries **no sponsor
statement of intent and no proponent or opponent testimony at all**, so the advocacy
hazard that has bitten other states does not arise here.

The **Supplemental Note** on each bill is the second Kansas document. Its Brief is
equally neutral and it is **version-stamped in its own header** ("As Amended by
Senate Committee on Public Health and Welfare"), which makes it useful for the
version check. ⚠ It does carry a labelled proponent and opponent testimony digest
near the end. Read the Brief; never write from the testimony section.

Documents are fetched with `/Users/shu/legiscan-data/ks_docs.py list|get <BILL>`.
⚠ `kslegislature.gov/li/b2025_26/measures/<bill>/` now redirects to
`kslegislature.gov/b2025_26/bills/<bill>/`, and every document is served through
`/b2025_26/bills/download/?apn=<path>` with a browser User-Agent.

## Version check, per roll

All twelve rolls are on the text that became law.

- Nine are **veto overrides**, which Kansas votes on the enrolled act with no
  amendment possible, so the version check is free. This is the Kentucky finding
  recurring: an override habit makes version drift almost impossible.
- **HB 2054** is a conference report and **HB 2109** a concurrence — in both, the
  question is the final text.
- **HB 2101**'s House vote is its only one, and the Senate passed the bill without
  amending it.
- **SB 63** is the one that needed work; see below.

⚠ **SB 244 is a House Substitute and HB 2382 a Senate Substitute.** In each case the
other chamber's earlier vote was on a different text, so only the House roll is
imported and the descriptions describe what the House actually voted on.

⚠ **SB 269 and SB 30 both passed unanimously early** (Senate 40-0 and House 123-0)
and only divided at the conference report. That is the shape a vehicle bill makes, so
both enrolled acts were read in full. Neither is a vehicle bill: each conference
report is a worked-out version of the bill's own subject.

## ⚠ Why SB 63 uses its passage roll, not its override

SB 63's House override on 2025-02-18 is **held out of this batch because LegiScan has
it wrong**. LegiScan reports 84-35; the Kansas bill history and the state's own
published roll call both report **85-34**, and diffing the member lists shows
LegiScan puts **Rep. Bob Lewis on the nay side when he voted yea**. Importing it would
publish a false claim about a named legislator, and the approval gate forces the stored
tally into the record text, so there is no way to import it with the right number.

The House's passage vote on 2025-01-31 (83-35) is clean: it matches the bill history
exactly, the House adopted no amendments, and the text it passed is the text enrolled
five days later. That roll is imported instead, with the override named in the
description and listed in `acknowledge_later_rolls`. This follows the Pennsylvania
HB 103 rule — when a later vote exists, say so rather than letting the earlier vote
read as the member's final position.

Ten more rolls across the session fail the same tally check and are listed in
CODE-FINDINGS.md finding 5.

## Labels

Every stance label carries `nay: null`. On each of these measures the realistic
objection runs on a different axis from the area being scored — cost, agency
expertise, ballot access, litigation risk — so a no vote is not evidence that the
member opposes the area's whole goal.

Two calls worth recording:

- **SB 4 is `election_integrity`/for**, not a civil rights measure, following the
  Ohio SB 293, Montana HB 719 and Alabama precedents that tightening absentee-ballot
  handling scores in that area. The access objection is real but sits in a different
  area, which is exactly why the nay side is null.
- **SB 30 is `government_efficiency`/for.** The Pennsylvania SB 187 rule says an
  office or process is only "efficiency" when its subject is the machinery of
  regulation itself. SB 30's whole subject is how occupational licences get made, and
  the review it requires asks whether each licence is the least restrictive means. It
  fits. The separation-of-powers objection is a different axis, hence `nay: null`.

## Dropped or held under filter 5

- **HB 2240** (legislative approval before certain agency action) — its House override
  is one of the eleven rolls whose LegiScan tally is wrong, and it has no clean
  alternative roll.
- Seven measures are **superseded**: their only divided House roll was followed by a
  later, undivided vote in the same chamber, so attributing the divided one would
  misstate where the House ended up. Dropped rather than acknowledged, following the
  Maryland SB 255 decision. They include SB 418 (by-right housing) and SB 45
  (education), both of which would otherwise have been strong picks.

## Checks run before importing

- **Plain-language lint**: 24 descriptions, **0 warnings**.
- **Reading level, measured separately** because the lint only counts sentence
  length: median Flesch-Kincaid grade **9.0**, worst 10.5, mean sentence 19.0 words,
  longest 34. A first draft measured median **13.2** and was rewritten three times
  before importing. ⚠ Grade 9 is the honest floor here, not the 7th-grade target: the
  remaining difficulty is in words the statutes cannot do without — citizenship,
  registration, legislature, reproductive. Reaching grade 7 would mean dropping the
  qualifications that this campaign's correction rounds keep proving load-bearing.
- **Comma splice**: the builder joins each body to its closing sentence with a period
  and asserts `", The "` appears in no description. 0 occurrences.
- **Tally in every sentence pair**: asserted, all 24.
- **British spellings**: scanned, none.
- **Superseded and same-day peers**: checked over all 12 rolls up front rather than
  by iterating on gate errors. One acknowledgement, SB 63's.
- **Shortened legal lists**: a review pass restored two that the readability rewrite
  had dropped — SB 244's "stop a serious threat to safety or order" exception, and the
  offices HB 2054's $1,000 cap covers (district judge, district attorney, and local
  races under 50,000 people). It also bounded SB 244's definition of gender to the
  four Kansas acts it actually amends. This is the recurring failure shape: plain
  rewriting quietly narrows or widens a statutory class.

## Duplicates

The importer flagged 7 related records. Six are false positives — hand-written rows
about **other** bills that happen to share a date, plus one about a different vote on
HB 2437. One was real: **Barbara Ballard's hand-written row for the SB 4 override**
made exactly the claim this batch imports. Retired before the import, reason naming
the replacing evidence, file `duplicate-retirements.json`.

A wider sweep by bill number (the related scan only catches rows on the same date)
found no others among the 75 candidates in the fan-out.

## Run ledger

Real run stamp `2026-09-03T01:59:08.140Z`; 12 files, all `imported`, **880 inserts**,
0 errors, 0 notified.

Reconciled three ways:

1. the ledger says 880 inserts across 12 files;
2. `origin_run_id LIKE 'rollcall:KS:%:2178:%:2026-09-03T01:59:08.140Z'` returns
   **880 records across 75 candidates**;
3. `candidate_records` with `origin='rollcall_import'` moved 115,260 → 116,140.

The dry run's own stamp `…01:57:36.727Z` matches **zero** rows, which is the positive
proof that `--dry-run` wrote nothing. A convergence dry run reports all 880
`unchanged`.

**Tags: 546.** Predicted independently from the evidence files before checking the
database — every label has `nay: null`, so the count should equal the matched yea
voters across the twelve rolls. It does, exactly.

**75 candidates is every member the crosswalk maps.** The Kansas Speaker votes, so
there is no shortfall of the kind Texas and Georgia have.

Production holds **zero** Kansas roll-call records. This run touched local `voteapp`
only.
