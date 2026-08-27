# FL 2135 — batch-02 judging notes

4 votes judged and imported on local `voteapp`. **Production untouched.**

## Sources

- **HB 1205:** the House **Final Bill Analysis** (`h1205z.GOS.PDF`, describes
  the enacted ch. 2025-21) plus the Senate's **House Message summary**
  (`2025h01205.hms.ee.PDF`), which pins what House amendment 258567 changed
  (possession cap 5 → 25 forms). The full floor-amendment history on the bill
  page established that rolls 1563815 and 1564585 are votes on the exact
  enacted text (Senate delete-all 476344 + House 258567).
- **SB 700:** the **enrolled text** (`2025700er`, 92 sections / 111 pages) —
  the last Senate staff analysis (`.fp`, dated 4/09) predates adopted floor
  amendment 340838, so the enrolled act is the only document that describes
  what both chambers voted. The fp analysis grounded the fluoride mechanism
  (additives limited to those meeting/surpassing drinking-water standards,
  removing contaminants, or improving water quality; fluoride for dental
  health falls outside; ~29 counties then fluoridated). Failed Senate floor
  amendment 765612 — the divided 13-21 vote LegiScan mislabels as a third
  reading — was verified to be the fluoride-preservation attempt, which is
  why the description leads with that provision.

## Version check

- HB 1205: both selected rolls are on the enacted text by construction (the
  House passage came after concurring in the Senate rewrite as amended; the
  Senate roll IS the concurrence in the last change). The earlier passage
  rolls (1536108, 1563439) were on superseded texts and were not selected.
- SB 700: the Senate's 27-9 third reading was on the engrossed text
  (post-340838); the House passed the identical text 88-27 after both of its
  floor amendments (649901, 910335) failed; enrolled = that text. Same text
  both chambers → one shared description.

## Label calls

- **HB 1205 → `election_integrity`, yea = for.** Direction follows the area
  description ("secure, accurate, auditable, and trusted"): every enacted
  provision adds verification, audit, enforcement, or process control, and
  none runs against the area. The burden-on-initiatives objection maps to no
  research area; per the standing rule, political contestedness alone is not
  the two-directions test. The description names the restrictive provisions
  in full — third-degree felony for unregistered possession of >25 forms,
  30→10-day deadline, resident/citizen/felon eligibility bars, one-initiative-per-committee cap and the three-election petition expiry — so a nay-leaning reader is not misled about
  what yes meant.
- **SB 700 → `housing_affordability`/for only** — revised twice: first
  from `general` to per-strand stances on the user's suggestion, then the
  `environment_and_public_health`/against label was DROPPED on the user's
  explicit direction call (the user credits the fluoride-harm evidence, so
  the fluoride provision's direction is contested and carries no stance
  either way; the description keeps the fluoride facts). Per-area tests and
  the corporate_accountability skip are in `PLAN.md`. Final verified state:
  each SB 700 record carries exactly one tag, `housing_affordability` (60
  tags); the interim environment tags and the original `general` tags are
  both gone, and the queue rows' `labels_json` matches.
  ⚠ Importer mechanic learned during the relabels: a labels-only change
  reports every record `unchanged` — tag-sync runs separately from the
  record-row compare — so verify tag flips in `candidate_record_area_tags`,
  never in the import report.

## Import ledger

Real run on local `voteapp`, `startedAt` **2026-08-27T19:51:43.245Z**:

- 4 files, all `imported`, **0 errors, 120 inserts** (51+51 house, 9+9
  senate), 0 rewrites, 0 notified.
- Reconciled three ways: report 120; `candidate_records` 68,514 → 68,634
  (+120); the stamp predicate returns 120. Convergence dry run: all 120
  `unchanged`. The dry-run stamp (`…T19:51:15.436Z`) matches zero rows.
- FL batch stamps now: `01:26:41.127Z` = 333, `17:24:02.253Z` = 52 (batch-01
  + its review fix), `19:51:43.245Z` = 60 + `20:05:21.743Z` = 60 (batch-02:
  the SB 700 inserts keep the insert-run stamp; the HB 1205 records carry the
  review-fix rewrite stamp). FL total **505 records / 62 candidates**.

## Review fix (2026-08-27): the three-cycle rule and a chapter typo

External review caught two errors, both verified against Florida's own
Ethics and Elections Committee bill summary
(`flsenate.gov/Committees/billsummaries/2025/html/1205`):

1. **The three-election limit attaches to the PETITION, not the committee.**
   The descriptions said sponsoring committees were "limited to one amendment
   at a time and three election cycles"; the enacted rule is that a proposed
   amendment expires if it goes three general elections without gathering at
   least 25 percent of the required signatures (it may be refiled anew). All
   four descriptions corrected; rewrite run `20:05:21.743Z` restored the 60
   imported HB 1205 records, 0 errors, convergence dry run all 120
   `unchanged`. The committed `import-report.json` is the rewrite run
   (batch-01 convention); the insert run's numbers stand above.
2. **HB 1205 is chapter 2025-21, not 2025-2.** The Final Bill Analysis PDF
   itself carries the typo ("ch. 2025-2, L.O.F.") and it was copied into
   PLAN.md and this file; the bill history and the committee summary both
   say 2025-21. Both documents corrected. ⚠ A lesson for FL sourcing: even
   the Final Bill Analysis needs its citations cross-checked against the
   bill-history page — SB 700's `2025-22` was verified correct.

## Duplicate retired

The dry run's one `related` flag was real: **Colleen Burton** carried a
hand-researched VoteSmart vote claim describing this same SB 700 senate vote
(record `085bd48c-6c3f-4c5a-b74e-1272c7f61006`, source
justfacts.votesmart.org). Retired by hand after the import, reason naming the
replacing canonical record (`c0f175ff-7edb-4d31-ae6f-c99d4b667101`) — the
Ohio Bird / federal Husted pattern: the record citing the roll call itself
supersedes a second-hand vote claim.
